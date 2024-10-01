# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import annotations

__all__ = (
    "QueryPlan",
    "QueryJoinsPlan",
    "QueryProjectionPlan",
    "QueryFindFirstPlan",
    "ResolvedDatasetSearch",
)

import dataclasses
from collections.abc import Iterator

from ..dimensions import DimensionElement, DimensionGroup
from ..queries import tree as qt
from ..registry.interfaces import CollectionRecord


@dataclasses.dataclass
class ResolvedDatasetSearch:
    """A struct describing a dataset search joined into a query, after
    resolving its collection search path.
    """

    name: str
    """Name of the dataset type."""

    dimensions: DimensionGroup
    """Dimensions of the dataset type."""

    collection_records: list[CollectionRecord] = dataclasses.field(default_factory=list)
    """Records of the collections to search for this dataset, in order, after
    removing collections inconsistent with the dataset type or the query's
    data ID constraint.
    """

    messages: list[str] = dataclasses.field(default_factory=list)
    """Diagnostic messages about collections that were filtered out of
    collection records.
    """

    is_calibration_search: bool = False
    """`True` if any of the collections to be searched is a
    `~CollectionType.CALIBRATION` collection, `False` otherwise.

    Since only calibration datasets can be present in
    `~CollectionType.CALIBRATION` collections, this also
    """


@dataclasses.dataclass
class QueryJoinsPlan:
    """A struct describing the "joins" section of a butler query.

    See `QueryPlan` and `QueryPlan.joins` for additional information.
    """

    predicate: qt.Predicate
    """Boolean expression to apply to rows."""

    columns: qt.ColumnSet
    """All columns whose tables need to be joined into the query.

    This is updated after construction to include all columns required by
    `predicate`.
    """

    materializations: dict[qt.MaterializationKey, DimensionGroup] = dataclasses.field(default_factory=dict)
    """Materializations to join into the query."""

    datasets: dict[str, ResolvedDatasetSearch] = dataclasses.field(default_factory=dict)
    """Dataset searches to join into the query."""

    data_coordinate_uploads: dict[qt.DataCoordinateUploadKey, DimensionGroup] = dataclasses.field(
        default_factory=dict
    )
    """Data coordinate uploads to join into the query."""

    messages: list[str] = dataclasses.field(default_factory=list)
    """Diagnostic messages that report reasons the query may not return any
    rows.
    """

    def __post_init__(self) -> None:
        self.predicate.gather_required_columns(self.columns)

    def iter_mandatory(self) -> Iterator[DimensionElement]:
        """Return an iterator over the dimension elements that must be joined
        into the query.

        These elements either provide "field" (non-key) columns or define
        relationships that result rows must be consistent with.  They do not
        necessarily include all dimension keys in `columns`, since each of
        those can typically be included in a query in multiple different ways.
        """
        for element_name in self.columns.dimensions.elements:
            element = self.columns.dimensions.universe[element_name]
            if self.columns.dimension_fields[element_name]:
                # We need to get dimension record fields for this element, and
                # its table is the only place to get those.
                yield element
            elif element.defines_relationships:
                # We also need to join in DimensionElement tables that define
                # one-to-many and many-to-many relationships, but data
                # coordinate uploads, materializations, and datasets can also
                # provide these relationships. Data coordinate uploads and
                # dataset tables only have required dimensions, and can hence
                # only provide relationships involving those.
                if any(
                    element.minimal_group.names <= upload_dimensions.required
                    for upload_dimensions in self.data_coordinate_uploads.values()
                ):
                    continue
                if any(
                    element.minimal_group.names <= dataset_spec.dimensions.required
                    for dataset_spec in self.datasets.values()
                ):
                    continue
                # Materializations have all key columns for their dimensions.
                if any(
                    element in materialization_dimensions.names
                    for materialization_dimensions in self.materializations.values()
                ):
                    continue
                yield element


@dataclasses.dataclass
class QueryProjectionPlan:
    """A struct describing the "projection" stage of a butler query.

    This struct evaluates to `True` in boolean contexts if either
    `needs_dimension_distinct` or `needs_dataset_distinct` are `True`.  In
    other cases the projection is effectively a no-op, because the
    "joins"-stage rows are already unique.

    See `QueryPlan` and `QueryPlan.projection` for additional information.
    """

    columns: qt.ColumnSet
    """The columns present in the query after the projection is applied.

    This is always a subset of `QueryJoinsPlan.columns`.
    """

    datasets: dict[str, ResolvedDatasetSearch]
    """Dataset searches to join into the query."""

    needs_dimension_distinct: bool = False
    """If `True`, the projection's dimensions do not include all dimensions in
    the "joins" stage, and hence a SELECT DISTINCT [ON] or GROUP BY must be
    used to make post-projection rows unique.
    """

    needs_dataset_distinct: bool = False
    """If `True`, the projection columns do not include collection-specific
    dataset fields that were present in the "joins" stage, and hence a SELECT
    DISTINCT [ON] or GROUP BY must be added to make post-projection rows
    unique.
    """

    def __bool__(self) -> bool:
        return self.needs_dimension_distinct or self.needs_dataset_distinct

    find_first_dataset: str | None = None
    """If not `None`, this is a find-first query for this dataset.

    This is set even if the find-first search is trivial because there is only
    one resolved collection.
    """


@dataclasses.dataclass
class QueryFindFirstPlan:
    """A struct describing the "find-first" stage of a butler query.

    See `QueryPlan` and `QueryPlan.find_first` for additional information.
    """

    search: ResolvedDatasetSearch
    """Information about the dataset being searched for."""

    @property
    def dataset_type(self) -> str:
        """Name of the dataset type."""
        return self.search.name

    def __bool__(self) -> bool:
        return len(self.search.collection_records) > 1


@dataclasses.dataclass
class QueryPlan:
    """A struct that aggregates information about a complete butler query.

    Notes
    -----
    Butler queries are transformed into a combination of SQL and Python-side
    postprocessing in three stages, with each corresponding to an attributes of
    this class and a method of `DirectQueryDriver`

    - In the `joins` stage (`~DirectQueryDriver.apply_query_joins`), we define
      the main SQL FROM and WHERE clauses, by joining all tables needed to
      bring in any columns, or constrain the keys of its rows.

    - In the `projection` stage (`~DirectQueryDriver.apply_query_projection`),
      we select only the columns needed for the query's result rows (including
      columns needed only by postprocessing and ORDER BY, as well those needed
      by the objects returned to users).  If the result rows are not naturally
      unique given what went into the query in the "joins" stage, the
      projection involves a SELECT DISTINCT [ON] or GROUP BY to make them
      unique, and in a few rare cases uses aggregate functions with GROUP BY.

    - In the `find_first` stage (`~DirectQueryDriver.apply_query_find_first`),
      we use a window function (PARTITION BY) subquery to find only the first
      dataset in the collection search path for each data ID.  This stage does
      nothing if there is no find-first dataset search, or if the search is
      trivial because there is only one collection.

    In `DirectQueryDriver.build_query`, a `QueryPlan` instance is constructed
    via `DirectQueryDriver.analyze_query`, which also returns an initial
    `QueryBuilder`.  After this point the plans are considered frozen, and the
    nested plan attributes are then passed to each of the corresponding
    `DirectQueryDriver` methods along with the builder, which is mutated (and
    occasionally replaced) into the complete SQL/postprocessing form of the
    query.
    """

    joins: QueryJoinsPlan
    """Description of the "joins" stage of query construction."""

    projection: QueryProjectionPlan
    """Description of the "projection" stage of query construction."""

    find_first: QueryFindFirstPlan | None
    """Description of the "find_first" stage of query construction.

    This attribute is `None` if there is no find-first search at all, and
    `False` in boolean contexts if the search is trivial because there is only
    one collection after the collections have been resolved.
    """

    final_columns: qt.ColumnSet
    """The columns included in the SELECT clause of the complete SQL query
    that is actually executed.

    This is a subset of `QueryProjectionPlan.columns` that differs only in
    columns used by the `find_first` stage or an ORDER BY expression.

    Like all other `.queries.tree.ColumnSet` attributes, it does not include
    fields added directly to `QueryBuilder.special`, which may also be added
    to the SELECT clause.
    """

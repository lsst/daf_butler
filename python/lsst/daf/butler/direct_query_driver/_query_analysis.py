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
    "QueryCollectionAnalysis",
    "QueryFindFirstAnalysis",
    "QueryJoinsAnalysis",
    "ResolvedDatasetSearch",
)

import dataclasses
from collections.abc import Iterator, Mapping
from typing import TYPE_CHECKING, Generic, TypeVar

from ..dimensions import DimensionElement, DimensionGroup
from ..queries import tree as qt
from ..registry import CollectionSummary
from ..registry.interfaces import CollectionRecord

if TYPE_CHECKING:
    from ._postprocessing import Postprocessing
    from ._sql_builders import SqlSelectBuilder

_T = TypeVar("_T")


@dataclasses.dataclass
class ResolvedDatasetSearch(Generic[_T]):
    """A struct describing a dataset search joined into a query, after
    resolving its collection search path.
    """

    name: _T
    """Name or names of the dataset type(s)."""

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
    `~CollectionType.CALIBRATION` collections, this also indicates that the
    dataset type is a calibration.
    """


@dataclasses.dataclass
class QueryJoinsAnalysis:
    """A struct describing the "joins" section of a butler query.

    See `DirectQueryDriver.build_query` for an overview of how queries are
    transformed into SQL, and the role this object plays in that.
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

    datasets: dict[str, ResolvedDatasetSearch[str]] = dataclasses.field(default_factory=dict)
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

    def iter_mandatory(self, union_dataset_dimensions: DimensionGroup | None) -> Iterator[DimensionElement]:
        """Return an iterator over the dimension elements that must be joined
        into the query.

        These elements either provide "field" (non-key) columns or define
        relationships that result rows must be consistent with.  They do not
        necessarily include all dimension keys in `columns`, since each of
        those can typically be included in a query in multiple different ways.

        Parameters
        ----------
        union_dataset_dimensions : `DimensionGroup` or `None`
            Dimensions of the union dataset types, or `None` if this is not
            a union dataset query.
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
                if (
                    union_dataset_dimensions is not None
                    and element.minimal_group.names <= union_dataset_dimensions.required
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
class QueryFindFirstAnalysis(Generic[_T]):
    """A struct describing the "find-first" stage of a butler query.

    See `DirectQueryDriver.build_query` for an overview of how queries are
    transformed into SQL, and the role this object plays in that.
    """

    search: ResolvedDatasetSearch[_T]
    """Information about the dataset type or types being searched for."""

    @property
    def dataset_type(self) -> _T:
        """Name(s) of the dataset type(s)."""
        return self.search.name

    def __bool__(self) -> bool:
        return len(self.search.collection_records) > 1


@dataclasses.dataclass
class QueryCollectionAnalysis:
    """A struct containing information about all of the collections that appear
    in a butler query.
    """

    collection_records: Mapping[str, CollectionRecord]
    """All collection records, keyed by collection name.

    This includes CHAINED collections.
    """

    calibration_dataset_types: set[str | qt.AnyDatasetType] = dataclasses.field(default_factory=set)
    """A set of the anmes of all calibration dataset types.

    If ``ANY_DATASET`` appears in the set, the dataset type union includes at
    least one calibration dataset type.
    """

    summaries_by_dataset_type: dict[
        str | qt.AnyDatasetType, list[tuple[CollectionRecord, CollectionSummary]]
    ] = dataclasses.field(default_factory=dict)
    """Collection records and summaries, in search order, keyed by dataset type
    name.

    CHAINED collections are flattened out in the nested lists.  Lists have been
    filtered to be consistent with the dataset types in the summaries, but not
    necessarily the governor dimensions in the summaries.
    """


@dataclasses.dataclass
class QueryTreeAnalysis:
    """A struct aggregating all analysis results derived from the query tree.

    See `DirectQueryDriver.build_query` for an overview of how queries are
    transformed into SQL, and the role this object plays in that.
    """

    joins: QueryJoinsAnalysis
    """Analysis of the "joins" stage, including all joins and columns needed by
    ``tree``.  Additional columns will be added to this plan later.
    """

    union_datasets: list[ResolvedDatasetSearch[list[str]]]
    """Resolved dataset searches that expand `QueryTree.any_dataset` out
    into groups of dataset types with the same collection search path.
    """

    initial_select_builder: SqlSelectBuilder
    """In-progress SQL query builder, initialized with just spatial and
    temporal overlaps."""

    postprocessing: Postprocessing
    """Struct representing post-query processing to be done in Python."""

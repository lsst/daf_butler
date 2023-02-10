# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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

__all__ = ["QuerySummary"]  # other classes here are local to subpackage

import dataclasses
from collections.abc import Iterable, Mapping, Set
from typing import Any

import astropy.time
from lsst.daf.relation import ColumnExpression, ColumnTag, Predicate, SortTerm
from lsst.sphgeom import IntersectionRegion, Region
from lsst.utils.classes import cached_getter, immutable

from ...core import (
    DataCoordinate,
    DatasetType,
    DimensionElement,
    DimensionGraph,
    DimensionKeyColumnTag,
    DimensionRecordColumnTag,
    DimensionUniverse,
    NamedValueAbstractSet,
    NamedValueSet,
    SkyPixDimension,
)

# We're not trying to add typing to the lex/yacc parser code, so MyPy
# doesn't know about some of these imports.
from .expressions import make_string_expression_predicate
from .expressions.categorize import categorizeElementOrderByName, categorizeOrderByName


@dataclasses.dataclass(frozen=True, eq=False)
class QueryWhereClause:
    """Structure holding various contributions to a query's WHERE clause.

    Instances of this class should only be created by
    `QueryWhereExpression.combine`, which guarantees the consistency of its
    attributes.
    """

    @classmethod
    def combine(
        cls,
        dimensions: DimensionGraph,
        expression: str = "",
        *,
        bind: Mapping[str, Any] | None = None,
        data_id: DataCoordinate | None = None,
        region: Region | None = None,
        defaults: DataCoordinate | None = None,
        dataset_type_name: str | None = None,
        allow_orphans: bool = False,
    ) -> QueryWhereClause:
        """Construct from various components.

        Parameters
        ----------
        dimensions : `DimensionGraph`
            The dimensions that would be included in the query in the absence
            of the WHERE clause.
        expression : `str`, optional
            A user-provided string expression.
        bind : `Mapping` [ `str`, `object` ], optional
            Mapping containing literal values that should be injected into the
            query expression, keyed by the identifiers they replace.
        data_id : `DataCoordinate`, optional
            A data ID identifying dimensions known in advance.  If not
            provided, will be set to an empty data ID.
        region : `lsst.sphgeom.Region`, optional
            A spatial constraint that all rows must overlap.
        defaults : `DataCoordinate`, optional
            A data ID containing default for governor dimensions.
        dataset_type_name : `str` or `None`, optional
            The name of the dataset type to assume for unqualified dataset
            columns, or `None` if there are no such identifiers.
        allow_orphans : `bool`, optional
            If `True`, permit expressions to refer to dimensions without
            providing a value for their governor dimensions (e.g. referring to
            a visit without an instrument).  Should be left to default to
            `False` in essentially all new code.

        Returns
        -------
        where : `QueryWhereClause`
            An object representing the WHERE clause for a query.
        """
        if data_id is None:
            data_id = DataCoordinate.makeEmpty(dimensions.universe)
        if defaults is None:
            defaults = DataCoordinate.makeEmpty(dimensions.universe)
        expression_predicate, governor_constraints = make_string_expression_predicate(
            expression,
            dimensions,
            bind=bind,
            data_id=data_id,
            defaults=defaults,
            dataset_type_name=dataset_type_name,
            allow_orphans=allow_orphans,
        )
        return QueryWhereClause(
            expression_predicate,
            data_id,
            region=region,
            governor_constraints=governor_constraints,
        )

    expression_predicate: Predicate | None
    """A predicate that evaluates a string expression from the user
    (`expressions.Predicate` or `None`).
    """

    data_id: DataCoordinate
    """A data ID identifying dimensions known before query construction
    (`DataCoordinate`).
    """

    region: Region | None
    """A spatial region that all result rows must overlap
    (`lsst.sphgeom.Region` or `None`).
    """

    governor_constraints: Mapping[str, Set[str]]
    """Restrictions on the values governor dimensions can take in this query,
    imposed by the string expression and/or data ID
    (`Mapping` [ `set`,  `~collections.abc.Set` [ `str` ] ]).

    Governor dimensions not present in this mapping are not constrained at all.
    """


@dataclasses.dataclass(frozen=True)
class OrderByClauseColumn:
    """Information about single column in ORDER BY clause."""

    element: DimensionElement
    """Dimension element for data in this column (`DimensionElement`)."""

    column: str | None
    """Name of the column or `None` for primary key (`str` or `None`)"""

    ordering: bool
    """True for ascending order, False for descending (`bool`)."""


@dataclasses.dataclass(frozen=True, eq=False)
class OrderByClause:
    """Class for information about columns in ORDER BY clause"""

    @classmethod
    def parse_general(cls, order_by: Iterable[str], graph: DimensionGraph) -> OrderByClause:
        """Parse an iterable of strings in the context of a multi-dimension
        query.

        Parameters
        ----------
        order_by : `Iterable` [ `str` ]
            Sequence of names to use for ordering with optional "-" prefix.
        graph : `DimensionGraph`
            Dimensions used by a query.

        Returns
        -------
        clause : `OrderByClause`
            New order-by clause representing the given string columns.
        """
        terms = []
        for name in order_by:
            if not name or name == "-":
                raise ValueError("Empty dimension name in ORDER BY")
            ascending = True
            if name[0] == "-":
                ascending = False
                name = name[1:]
            element, column = categorizeOrderByName(graph, name)
            term = cls._make_term(element, column, ascending)
            terms.append(term)
        return cls(terms)

    @classmethod
    def parse_element(cls, order_by: Iterable[str], element: DimensionElement) -> OrderByClause:
        """Parse an iterable of strings in the context of a single dimension
        element query.

        Parameters
        ----------
        order_by : `Iterable` [ `str` ]
            Sequence of names to use for ordering with optional "-" prefix.
        element : `DimensionElement`
            Single or primary dimension element in the query

        Returns
        -------
        clause : `OrderByClause`
            New order-by clause representing the given string columns.
        """
        terms = []
        for name in order_by:
            if not name or name == "-":
                raise ValueError("Empty dimension name in ORDER BY")
            ascending = True
            if name[0] == "-":
                ascending = False
                name = name[1:]
            column = categorizeElementOrderByName(element, name)
            term = cls._make_term(element, column, ascending)
            terms.append(term)
        return cls(terms)

    @classmethod
    def _make_term(cls, element: DimensionElement, column: str | None, ascending: bool) -> SortTerm:
        """Make a single sort term from parsed user expression values.

        Parameters
        ----------
        element : `DimensionElement`
            Dimension element the sort term references.
        column : `str` or `None`
            DimensionRecord field name, or `None` if ``element`` is a
            `Dimension` and the sort term is on is key value.
        ascending : `bool`
            Whether to sort ascending (`True`) or descending (`False`).

        Returns
        -------
        term : `lsst.daf.relation.SortTerm`
            Sort term struct.
        """
        tag: ColumnTag
        expression: ColumnExpression
        if column is None:
            tag = DimensionKeyColumnTag(element.name)
            expression = ColumnExpression.reference(tag)
        elif column in ("timespan.begin", "timespan.end"):
            base_column, _, subfield = column.partition(".")
            tag = DimensionRecordColumnTag(element.name, base_column)
            expression = ColumnExpression.reference(tag).method(
                "lower" if subfield == "begin" else "upper", dtype=astropy.time.Time
            )
        else:
            tag = DimensionRecordColumnTag(element.name, column)
            expression = ColumnExpression.reference(tag)
        return SortTerm(expression, ascending)

    terms: Iterable[SortTerm]
    """Columns that appear in the ORDER BY
    (`Iterable` [ `OrderByClauseColumn` ]).
    """

    @property
    @cached_getter
    def columns_required(self) -> Set[ColumnTag]:
        """Set of column tags for all columns referenced by the ORDER BY clause
        (`~collections.abc.Set` [ `ColumnTag` ]).
        """
        tags: set[ColumnTag] = set()
        for term in self.terms:
            tags.update(term.expression.columns_required)
        return tags


@immutable
class ElementOrderByClause:
    """Class for information about columns in ORDER BY clause for one element.

    Parameters
    ----------
    order_by : `Iterable` [ `str` ]
        Sequence of names to use for ordering with optional "-" prefix.
    element : `DimensionElement`
        Dimensions used by a query.
    """

    def __init__(self, order_by: Iterable[str], element: DimensionElement):
        self.order_by_columns = []
        for name in order_by:
            if not name or name == "-":
                raise ValueError("Empty dimension name in ORDER BY")
            ascending = True
            if name[0] == "-":
                ascending = False
                name = name[1:]
            column = categorizeElementOrderByName(element, name)
            self.order_by_columns.append(
                OrderByClauseColumn(element=element, column=column, ordering=ascending)
            )

    order_by_columns: Iterable[OrderByClauseColumn]
    """Columns that appear in the ORDER BY
    (`Iterable` [ `OrderByClauseColumn` ]).
    """


@immutable
class QuerySummary:
    """A struct that holds and categorizes the dimensions involved in a query.

    A `QuerySummary` instance is necessary to construct a `QueryBuilder`, and
    it needs to include all of the dimensions that will be included in the
    query (including any needed for querying datasets).

    Parameters
    ----------
    requested : `DimensionGraph`
        The dimensions whose primary keys should be included in the result rows
        of the query.
    data_id : `DataCoordinate`, optional
        A fully-expanded data ID identifying dimensions known in advance.  If
        not provided, will be set to an empty data ID.
    expression : `str`, optional
        A user-provided string WHERE expression.
    region : `lsst.sphgeom.Region`, optional
        A spatial constraint that all rows must overlap.
    timespan : `Timespan`, optional
        A temporal constraint that all rows must overlap.
    bind : `Mapping` [ `str`, `object` ], optional
        Mapping containing literal values that should be injected into the
        query expression, keyed by the identifiers they replace.
    defaults : `DataCoordinate`, optional
        A data ID containing default for governor dimensions.
    datasets : `Iterable` [ `DatasetType` ], optional
        Dataset types whose searches may be joined into the query.  Callers
        must still call `QueryBuilder.joinDataset` explicitly to control how
        that join happens (e.g. which collections are searched), but by
        declaring them here first we can ensure that the query includes the
        right dimensions for those joins.
    order_by : `Iterable` [ `str` ]
        Sequence of names to use for ordering with optional "-" prefix.
    limit : `Tuple`, optional
        Limit on the number of returned rows and optional offset.
    check : `bool`, optional
        If `False`, permit expressions to refer to dimensions without providing
        a value for their governor dimensions (e.g. referring to a visit
        without an instrument).  Should be left to default to `True` in
        essentially all new code.
    """

    def __init__(
        self,
        requested: DimensionGraph,
        *,
        data_id: DataCoordinate | None = None,
        expression: str = "",
        region: Region | None = None,
        bind: Mapping[str, Any] | None = None,
        defaults: DataCoordinate | None = None,
        datasets: Iterable[DatasetType] = (),
        order_by: Iterable[str] | None = None,
        limit: tuple[int, int | None] | None = None,
        check: bool = True,
    ):
        self.requested = requested
        self.datasets = NamedValueSet(datasets).freeze()
        if len(self.datasets) == 1:
            (dataset_type_name,) = self.datasets.names
        else:
            dataset_type_name = None
        self.where = QueryWhereClause.combine(
            self.requested,
            expression=expression,
            bind=bind,
            data_id=data_id,
            region=region,
            defaults=defaults,
            dataset_type_name=dataset_type_name,
            allow_orphans=not check,
        )
        self.order_by = None if order_by is None else OrderByClause.parse_general(order_by, requested)
        self.limit = limit
        self.columns_required, self.dimensions, self.region = self._compute_columns_required()

    requested: DimensionGraph
    """Dimensions whose primary keys should be included in the result rows of
    the query (`DimensionGraph`).
    """

    where: QueryWhereClause
    """Structure containing objects that contribute to the WHERE clause of the
    query (`QueryWhereClause`).
    """

    datasets: NamedValueAbstractSet[DatasetType]
    """Dataset types whose searches may be joined into the query
    (`NamedValueAbstractSet` [ `DatasetType` ]).
    """

    order_by: OrderByClause | None
    """Object that manages how the query results should be sorted
    (`OrderByClause` or `None`).
    """

    limit: tuple[int, int | None] | None
    """Integer offset and maximum number of rows returned (prior to
    postprocessing filters), respectively.
    """

    dimensions: DimensionGraph
    """All dimensions in the query in any form (`DimensionGraph`).
    """

    region: Region | None
    """Region that bounds all query results (`lsst.sphgeom.Region`).

    While `QueryWhereClause.region` and the ``region`` constructor argument
    represent an external region given directly by the caller, this represents
    the region actually used directly as a constraint on the query results,
    which can also come from the data ID passed by the caller.
    """

    columns_required: Set[ColumnTag]
    """All columns that must be included directly in the query.

    This does not include columns that only need to be included in the result
    rows, and hence could be provided by postprocessors.
    """

    @property
    def universe(self) -> DimensionUniverse:
        """All known dimensions (`DimensionUniverse`)."""
        return self.requested.universe

    def _compute_columns_required(
        self,
    ) -> tuple[set[ColumnTag], DimensionGraph, Region | None]:
        """Compute the columns that must be provided by the relations joined
        into this query in order to obtain the right *set* of result rows in
        the right order.

        This does not include columns that only need to be included in the
        result rows, and hence could be provided by postprocessors.
        """
        tags: set[ColumnTag] = set(DimensionKeyColumnTag.generate(self.requested.names))
        for dataset_type in self.datasets:
            tags.update(DimensionKeyColumnTag.generate(dataset_type.dimensions.names))
        if self.where.expression_predicate is not None:
            tags.update(self.where.expression_predicate.columns_required)
        if self.order_by is not None:
            tags.update(self.order_by.columns_required)
        region = self.where.region
        for dimension in self.where.data_id.graph:
            dimension_tag = DimensionKeyColumnTag(dimension.name)
            if dimension_tag in tags:
                continue
            if dimension == self.universe.commonSkyPix or not isinstance(dimension, SkyPixDimension):
                # If a dimension in the data ID is available from dimension
                # tables or dimension spatial-join tables in the database,
                # include it in the set of dimensions whose tables should be
                # joined.  This makes these data ID constraints work just like
                # simple 'where' constraints, which is good.
                tags.add(dimension_tag)
            else:
                # This is a SkyPixDimension other than the common one.  If it's
                # not already present in the query (e.g. from a dataset join),
                # this is a pure spatial constraint, which we can only apply by
                # modifying the 'region' for the query.  That will also require
                # that we join in the common skypix dimension.
                pixel = dimension.pixelization.pixel(self.where.data_id[dimension])
                if region is None:
                    region = pixel
                else:
                    region = IntersectionRegion(region, pixel)
        # Make sure the dimension keys are expanded self-consistently in what
        # we return by passing them through DimensionGraph.
        dimensions = DimensionGraph(
            self.universe, names={tag.dimension for tag in DimensionKeyColumnTag.filter_from(tags)}
        )
        # If we have a region constraint, ensure region columns and the common
        # skypix dimension are included.
        missing_common_skypix = False
        if region is not None:
            for family in dimensions.spatial:
                element = family.choose(dimensions.elements)
                tags.add(DimensionRecordColumnTag(element.name, "region"))
                if not isinstance(element, SkyPixDimension) and self.universe.commonSkyPix not in dimensions:
                    missing_common_skypix = True
        if missing_common_skypix:
            dimensions = dimensions.union(self.universe.commonSkyPix.graph)
        tags.update(DimensionKeyColumnTag.generate(dimensions.names))
        return (tags, dimensions, region)

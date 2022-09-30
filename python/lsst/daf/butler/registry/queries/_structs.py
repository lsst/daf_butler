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

from collections.abc import Iterable, Iterator, Mapping, Set
from dataclasses import dataclass
from typing import Any, cast

from lsst.sphgeom import Region
from lsst.utils.classes import cached_getter, immutable
from sqlalchemy.sql import ColumnElement

from ...core import (
    DataCoordinate,
    DatasetType,
    Dimension,
    DimensionElement,
    DimensionGraph,
    DimensionUniverse,
    NamedKeyDict,
    NamedKeyMapping,
    NamedValueAbstractSet,
    NamedValueSet,
    SkyPixDimension,
    TimespanDatabaseRepresentation,
)
from .._exceptions import UserExpressionSyntaxError

# We're not trying to add typing to the lex/yacc parser code, so MyPy
# doesn't know about some of these imports.
from .expressions import Node, NormalForm, NormalFormExpression, ParserYacc  # type: ignore
from .expressions.categorize import categorizeElementOrderByName, categorizeOrderByName


@immutable
class QueryWhereExpression:
    """A struct representing a parsed user-provided WHERE expression.

    Parameters
    ----------
    expression : `str`, optional
        The string expression to parse.  If `None`, a where expression that
        always evaluates to `True` is implied.
    bind : `Mapping` [ `str`, `object` ], optional
        Mapping containing literal values that should be injected into the
        query expression, keyed by the identifiers they replace.
    """

    def __init__(self, expression: str | None = None, bind: Mapping[str, Any] | None = None):
        if expression:
            try:
                parser = ParserYacc()
                self._tree = parser.parse(expression)
            except Exception as exc:
                raise UserExpressionSyntaxError(f"Failed to parse user expression `{expression}'.") from exc
            assert self._tree is not None
        else:
            self._tree = None
        if bind is None:
            bind = {}
        self._bind = bind

    def attach(
        self,
        graph: DimensionGraph,
        dataId: DataCoordinate | None = None,
        region: Region | None = None,
        defaults: DataCoordinate | None = None,
        check: bool = True,
    ) -> QueryWhereClause:
        """Allow this expression to be attached to a `QuerySummary` by
        transforming it into a `QueryWhereClause`, while checking it for both
        internal consistency and consistency with the rest of the query.

        Parameters
        ----------
        graph : `DimensionGraph`
            The dimensions the query would include in the absence of this
            WHERE expression.
        dataId : `DataCoordinate`, optional
            A fully-expanded data ID identifying dimensions known in advance.
            If not provided, will be set to an empty data ID.
        region : `lsst.sphgeom.Region`, optional
            A spatial constraint that all rows must overlap.  If `None` and
            ``dataId`` is an expanded data ID, ``dataId.region`` will be used
            to construct one.
        defaults : `DataCoordinate`, optional
            A data ID containing default for governor dimensions.  Ignored
            unless ``check=True``.
        check : `bool`
            If `True` (default) check the query for consistency and inject
            default values into the data ID when needed.  This may
            reject some valid queries that resemble common mistakes (e.g.
            queries for visits without specifying an instrument).
        """
        if dataId is not None and dataId.hasRecords():
            if region is None and dataId.region is not None:
                region = dataId.region
        if dataId is None:
            dataId = DataCoordinate.makeEmpty(graph.universe)
        if defaults is None:
            defaults = DataCoordinate.makeEmpty(graph.universe)
        if self._bind and check:
            for identifier in self._bind:
                if identifier in graph.universe.getStaticElements().names:
                    raise RuntimeError(
                        f"Bind parameter key {identifier!r} conflicts with a dimension element."
                    )
                table, sep, column = identifier.partition(".")
                if column and table in graph.universe.getStaticElements().names:
                    raise RuntimeError(f"Bind parameter key {identifier!r} looks like a dimension column.")
        governor_constraints: dict[str, Set[str]] = {}
        summary: InspectionSummary
        if self._tree is not None:
            if check:
                # Convert the expression to disjunctive normal form (ORs of
                # ANDs).  That's potentially super expensive in the general
                # case (where there's a ton of nesting of ANDs and ORs).  That
                # won't be the case for the expressions we expect, and we
                # actually use disjunctive normal instead of conjunctive (i.e.
                # ANDs of ORs) because I think the worst-case is a long list
                # of OR'd-together data IDs, which is already in or very close
                # to disjunctive normal form.
                expr = NormalFormExpression.fromTree(self._tree, NormalForm.DISJUNCTIVE)
                from .expressions import CheckVisitor

                # Check the expression for consistency and completeness.
                visitor = CheckVisitor(dataId, graph, self._bind, defaults)
                try:
                    summary = expr.visit(visitor)
                except RuntimeError as err:
                    exprOriginal = str(self._tree)
                    exprNormal = str(expr.toTree())
                    if exprNormal == exprOriginal:
                        msg = f'Error in query expression "{exprOriginal}": {err}'
                    else:
                        msg = (
                            f'Error in query expression "{exprOriginal}" '
                            f'(normalized to "{exprNormal}"): {err}'
                        )
                    raise RuntimeError(msg) from None
                for dimension_name, values in summary.dimension_constraints.items():
                    if dimension_name in graph.universe.getGovernorDimensions().names:
                        governor_constraints[dimension_name] = cast(Set[str], values)
                dataId = visitor.dataId
            else:
                from .expressions import InspectionVisitor

                summary = self._tree.visit(InspectionVisitor(graph.universe, self._bind))
        else:
            from .expressions import InspectionSummary

            summary = InspectionSummary()
        return QueryWhereClause(
            self._tree,
            dataId,
            dimensions=summary.dimensions,
            columns=summary.columns,
            bind=self._bind,
            governor_constraints=governor_constraints,
            region=region,
        )


@dataclass(frozen=True)
class QueryWhereClause:
    """Structure holding various contributions to a query's WHERE clause.

    Instances of this class should only be created by
    `QueryWhereExpression.attach`, which guarantees the consistency of its
    attributes.
    """

    tree: Node | None
    """A parsed string expression tree., or `None` if there was no string
    expression.
    """

    dataId: DataCoordinate
    """A data ID identifying dimensions known before query construction
    (`DataCoordinate`).

    ``dataId.hasRecords()`` is guaranteed to return `True`.
    """

    dimensions: NamedValueAbstractSet[Dimension]
    """Dimensions whose primary keys or dependencies were referenced anywhere
    in the string expression (`NamedValueAbstractSet` [ `Dimension` ]).
    """

    columns: NamedKeyMapping[DimensionElement, Set[str]]
    """Dimension element tables whose non-key columns were referenced anywhere
    in the string expression
    (`NamedKeyMapping` [ `DimensionElement`, `Set` [ `str` ] ]).
    """

    bind: Mapping[str, Any]
    """Mapping containing literal values that should be injected into the
    query expression, keyed by the identifiers they replace (`Mapping`).
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

    @property
    @cached_getter
    def temporal(self) -> NamedValueAbstractSet[DimensionElement]:
        """Dimension elements whose timespans are referenced by this
        expression (`NamedValueAbstractSet` [ `DimensionElement` ])
        """
        return NamedValueSet(
            e for e, c in self.columns.items() if TimespanDatabaseRepresentation.NAME in c
        ).freeze()


@dataclass(frozen=True)
class OrderByClauseColumn:
    """Information about single column in ORDER BY clause."""

    element: DimensionElement
    """Dimension element for data in this column (`DimensionElement`)."""

    column: str | None
    """Name of the column or `None` for primary key (`str` or `None`)"""

    ordering: bool
    """True for ascending order, False for descending (`bool`)."""


@immutable
class OrderByClause:
    """Class for information about columns in ORDER BY clause

    Parameters
    ----------
    order_by : `Iterable` [ `str` ]
        Sequence of names to use for ordering with optional "-" prefix.
    graph : `DimensionGraph`
        Dimensions used by a query.
    """

    def __init__(self, order_by: Iterable[str], graph: DimensionGraph):

        self.order_by_columns = []
        for name in order_by:
            if not name or name == "-":
                raise ValueError("Empty dimension name in ORDER BY")
            ascending = True
            if name[0] == "-":
                ascending = False
                name = name[1:]
            element, column = categorizeOrderByName(graph, name)
            self.order_by_columns.append(
                OrderByClauseColumn(element=element, column=column, ordering=ascending)
            )

        self.elements = NamedValueSet(
            column.element for column in self.order_by_columns if column.column is not None
        )

    order_by_columns: Iterable[OrderByClauseColumn]
    """Columns that appear in the ORDER BY
    (`Iterable` [ `OrderByClauseColumn` ]).
    """

    elements: NamedValueSet[DimensionElement]
    """Dimension elements whose non-key columns were referenced by order_by
    (`NamedValueSet` [ `DimensionElement` ]).
    """


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
    dataId : `DataCoordinate`, optional
        A fully-expanded data ID identifying dimensions known in advance.  If
        not provided, will be set to an empty data ID.
    expression : `str` or `QueryWhereExpression`, optional
        A user-provided string WHERE expression.
    whereRegion : `lsst.sphgeom.Region`, optional
        If `None` and ``dataId`` is an expanded data ID, ``dataId.region`` will
        be used to construct one.
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
    check : `bool`
        If `True` (default) check the query for consistency.  This may reject
        some valid queries that resemble common mistakes (e.g. queries for
        visits without specifying an instrument).
    """

    def __init__(
        self,
        requested: DimensionGraph,
        *,
        dataId: DataCoordinate | None = None,
        expression: str | QueryWhereExpression | None = None,
        whereRegion: Region | None = None,
        bind: Mapping[str, Any] | None = None,
        defaults: DataCoordinate | None = None,
        datasets: Iterable[DatasetType] = (),
        order_by: Iterable[str] | None = None,
        limit: tuple[int, int | None] | None = None,
        check: bool = True,
    ):
        self.requested = requested
        if expression is None:
            expression = QueryWhereExpression(None, bind)
        elif isinstance(expression, str):
            expression = QueryWhereExpression(expression, bind)
        elif bind is not None:
            raise TypeError("New bind parameters passed, but expression is already a QueryWhereExpression.")
        self.where = expression.attach(
            self.requested, dataId=dataId, region=whereRegion, defaults=defaults, check=check
        )
        self.datasets = NamedValueSet(datasets).freeze()
        self.order_by = None if order_by is None else OrderByClause(order_by, requested)
        self.limit = limit

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

    @property
    def universe(self) -> DimensionUniverse:
        """All known dimensions (`DimensionUniverse`)."""
        return self.requested.universe

    @property
    @cached_getter
    def spatial(self) -> NamedValueAbstractSet[DimensionElement]:
        """Dimension elements whose regions and skypix IDs should be included
        in the query (`NamedValueAbstractSet` of `DimensionElement`).
        """
        # An element may participate spatially in the query if:
        # - it's the most precise spatial element for its system in the
        #   requested dimensions (i.e. in `self.requested.spatial`);
        # - it isn't also given at query construction time.
        result: NamedValueSet[DimensionElement] = NamedValueSet()
        for family in self.mustHaveKeysJoined.spatial:
            element = family.choose(self.mustHaveKeysJoined.elements)
            assert isinstance(element, DimensionElement)
            if element not in self.where.dataId.graph.elements:
                result.add(element)
        if len(result) == 1:
            # There's no spatial join, but there might be a WHERE filter based
            # on a given region.
            if self.where.dataId.graph.spatial:
                # We can only perform those filters against SkyPix dimensions,
                # so if what we have isn't one, add the common SkyPix dimension
                # to the query; the element we have will be joined to that.
                (element,) = result
                if not isinstance(element, SkyPixDimension):
                    result.add(self.universe.commonSkyPix)
            else:
                # There is no spatial join or filter in this query.  Even
                # if this element might be associated with spatial
                # information, we don't need it for this query.
                return NamedValueSet().freeze()
        elif len(result) > 1:
            # There's a spatial join.  Those require the common SkyPix
            # system to be included in the query in order to connect them.
            result.add(self.universe.commonSkyPix)
        return result.freeze()

    @property
    @cached_getter
    def temporal(self) -> NamedValueAbstractSet[DimensionElement]:
        """Dimension elements whose timespans should be included in the
        query (`NamedValueSet` of `DimensionElement`).
        """
        if len(self.mustHaveKeysJoined.temporal) > 1:
            # We don't actually have multiple temporal families in our current
            # dimension configuration, so this limitation should be harmless.
            raise NotImplementedError("Queries that should involve temporal joins are not yet supported.")
        return self.where.temporal

    @property
    @cached_getter
    def mustHaveKeysJoined(self) -> DimensionGraph:
        """Dimensions whose primary keys must be used in the JOIN ON clauses
        of the query, even if their tables do not appear (`DimensionGraph`).

        A `Dimension` primary key can appear in a join clause without its table
        via a foreign key column in table of a dependent dimension element or
        dataset.
        """
        names = set(self.requested.names | self.where.dimensions.names)
        for dataset_type in self.datasets:
            names.update(dataset_type.dimensions.names)
        return DimensionGraph(self.universe, names=names)

    @property
    @cached_getter
    def mustHaveTableJoined(self) -> NamedValueAbstractSet[DimensionElement]:
        """Dimension elements whose associated tables must appear in the
        query's FROM clause (`NamedValueSet` of `DimensionElement`).
        """
        result = NamedValueSet(self.spatial | self.temporal | self.where.columns.keys())
        if self.order_by is not None:
            result.update(self.order_by.elements)
        for dimension in self.mustHaveKeysJoined:
            if dimension.implied:
                result.add(dimension)
        for element in self.mustHaveKeysJoined.union(self.where.dataId.graph).elements:
            if element.alwaysJoin:
                result.add(element)
        return result.freeze()


@dataclass
class DatasetQueryColumns:
    """A struct containing the columns used to reconstruct `DatasetRef`
    instances from query results.
    """

    datasetType: DatasetType
    """The dataset type being queried (`DatasetType`).
    """

    id: ColumnElement
    """Column containing the unique integer ID for this dataset.
    """

    runKey: ColumnElement
    """Foreign key column to the `~CollectionType.RUN` collection that holds
    this dataset.
    """

    ingestDate: ColumnElement | None
    """Column containing the ingest timestamp, this is not a part of
    `DatasetRef` but it comes from the same table.
    """

    def __iter__(self) -> Iterator[ColumnElement]:
        yield self.id
        yield self.runKey


@dataclass
class QueryColumns:
    """A struct organizing the columns in an under-construction or currently-
    executing query.

    Takes no parameters at construction, as expected usage is to add elements
    to its container attributes incrementally.
    """

    def __init__(self) -> None:
        self.keys = NamedKeyDict()
        self.timespans = NamedKeyDict()
        self.regions = NamedKeyDict()
        self.datasets = None

    keys: NamedKeyDict[Dimension, list[ColumnElement]]
    """Columns that correspond to the primary key values of dimensions
    (`NamedKeyDict` mapping `Dimension` to a `list` of `ColumnElement`).

    Each value list contains columns from multiple tables corresponding to the
    same dimension, and the query should constrain the values of those columns
    to be the same.

    In a `Query`, the keys of this dictionary must include at least the
    dimensions in `QuerySummary.requested` and `QuerySummary.dataId.graph`.
    """

    timespans: NamedKeyDict[DimensionElement, TimespanDatabaseRepresentation]
    """Columns that correspond to timespans for elements that participate in a
    temporal join or filter in the query (`NamedKeyDict` mapping
    `DimensionElement` to `TimespanDatabaseRepresentation`).

    In a `Query`, the keys of this dictionary must be exactly the elements
    in `QuerySummary.temporal`.
    """

    regions: NamedKeyDict[DimensionElement, ColumnElement]
    """Columns that correspond to regions for elements that participate in a
    spatial join or filter in the query (`NamedKeyDict` mapping
    `DimensionElement` to `sqlalchemy.sql.ColumnElement`).

    In a `Query`, the keys of this dictionary must be exactly the elements
    in `QuerySummary.spatial`.
    """

    datasets: DatasetQueryColumns | None
    """Columns that can be used to construct `DatasetRef` instances from query
    results.
    (`DatasetQueryColumns` or `None`).
    """

    def isEmpty(self) -> bool:
        """Return `True` if this query has no columns at all."""
        return not (self.keys or self.timespans or self.regions or self.datasets is not None)

    def getKeyColumn(self, dimension: Dimension | str) -> ColumnElement:
        """Return one of the columns in self.keys for the given dimension.

        The column selected is an implentation detail but is guaranteed to
        be deterministic and consistent across multiple calls.

        Parameters
        ----------
        dimension : `Dimension` or `str`
            Dimension for which to obtain a key column.

        Returns
        -------
        column : `sqlalchemy.sql.ColumnElement`
            SQLAlchemy column object.
        """
        # Choosing the last element here is entirely for human readers of the
        # query (e.g. developers debugging things); it makes it more likely a
        # dimension key will be provided by the dimension's own table, or
        # failing that, some closely related dimension, which might be less
        # surprising to see than e.g. some dataset subquery.  From the
        # database's perspective this is entirely arbitrary, because the query
        # guarantees they all have equal values.
        return self.keys[dimension][-1]

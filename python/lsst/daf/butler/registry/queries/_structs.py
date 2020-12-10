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

__all__ = ["QuerySummary", "RegistryManagers"]  # other classes here are local to subpackage

from dataclasses import dataclass
from typing import AbstractSet, Any, Iterator, List, Mapping, Optional, Type, Union

from sqlalchemy.sql import ColumnElement

from lsst.sphgeom import Region
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
    SpatialRegionDatabaseRepresentation,
    TimespanDatabaseRepresentation,
)
from ...core.utils import cached_getter, immutable
from ..interfaces import (
    CollectionManager,
    DatasetRecordStorageManager,
    DimensionRecordStorageManager,
)
from ..wildcards import GovernorDimensionRestriction
# We're not trying to add typing to the lex/yacc parser code, so MyPy
# doesn't know about some of these imports.
from .expressions import Node, NormalForm, NormalFormExpression, ParserYacc  # type: ignore


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
    def __init__(self, expression: Optional[str] = None, bind: Optional[Mapping[str, Any]] = None):
        if expression:
            try:
                parser = ParserYacc()
                self._tree = parser.parse(expression)
            except Exception as exc:
                raise RuntimeError(f"Failed to parse user expression `{expression}'.") from exc
            assert self._tree is not None
        else:
            self._tree = None
        if bind is None:
            bind = {}
        self._bind = bind

    def attach(
        self,
        graph: DimensionGraph,
        dataId: Optional[DataCoordinate] = None,
        region: Optional[Region] = None,
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
            ``dataId.hasRecords()`` must return `True`.
        region : `lsst.sphgeom.Region`, optional
            A spatial region that all rows must overlap.  If `None` and
            ``dataId`` is not `None`, ``dataId.region`` will be used.
        check : `bool`
            If `True` (default) check the query for consistency.  This may
            reject some valid queries that resemble common mistakes (e.g.
            queries for visits without specifying an instrument).
        """
        if region is None and dataId is not None:
            region = dataId.region
        if dataId is None:
            dataId = DataCoordinate.makeEmpty(graph.universe)
        if self._bind and check:
            for identifier in self._bind:
                if identifier in graph.universe.getStaticElements().names:
                    raise RuntimeError(
                        f"Bind parameter key {identifier!r} conflicts with a dimension element."
                    )
                table, sep, column = identifier.partition('.')
                if column and table in graph.universe.getStaticElements().names:
                    raise RuntimeError(
                        f"Bind parameter key {identifier!r} looks like a dimension column."
                    )
        restriction = GovernorDimensionRestriction(graph.universe)
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
                try:
                    summary = expr.visit(CheckVisitor(dataId, graph, self._bind.keys()))
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
                restriction = GovernorDimensionRestriction(
                    graph.universe,
                    **summary.governors.byName(),
                )
            else:
                from .expressions import InspectionVisitor
                summary = self._tree.visit(InspectionVisitor(graph.universe, self._bind.keys()))
        else:
            from .expressions import InspectionSummary
            summary = InspectionSummary()
        return QueryWhereClause(
            self._tree,
            dataId,
            dimensions=summary.dimensions,
            columns=summary.columns,
            bind=self._bind,
            restriction=restriction,
            region=region,
        )


@dataclass(frozen=True)
class QueryWhereClause:
    """Structure holding various contributions to a query's WHERE clause.

    Instances of this class should only be created by
    `QueryWhereExpression.attach`, which guarantees the consistency of its
    attributes.
    """

    tree: Optional[Node]
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

    columns: NamedKeyMapping[DimensionElement, AbstractSet[str]]
    """Dimension element tables whose non-key columns were referenced anywhere
    in the string expression
    (`NamedKeyMapping` [ `DimensionElement`, `Set` [ `str` ] ]).
    """

    bind: Mapping[str, Any]
    """Mapping containing literal values that should be injected into the
    query expression, keyed by the identifiers they replace (`Mapping`).
    """

    region: Optional[Region]
    """A spatial region that all result rows must overlap
    (`lsst.sphgeom.Region` or `None`).
    """

    restriction: GovernorDimensionRestriction
    """Restrictions on the values governor dimensions can take in this query,
    imposed by the string expression or data ID
    (`GovernorDimensionRestriction`).
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
        not provided, will be set to an empty data ID.  ``dataId.hasRecords()``
        must return `True`.
    expression : `str` or `QueryWhereExpression`, optional
        A user-provided string WHERE expression.
    whereRegion : `lsst.sphgeom.Region`, optional
        A spatial region that all rows must overlap.  If `None` and ``dataId``
        is not `None`, ``dataId.region`` will be used.
    bind : `Mapping` [ `str`, `object` ], optional
        Mapping containing literal values that should be injected into the
        query expression, keyed by the identifiers they replace.
    check : `bool`
        If `True` (default) check the query for consistency.  This may reject
        some valid queries that resemble common mistakes (e.g. queries for
        visits without specifying an instrument).
    """
    def __init__(self, requested: DimensionGraph, *,
                 dataId: Optional[DataCoordinate] = None,
                 expression: Optional[Union[str, QueryWhereExpression]] = None,
                 whereRegion: Optional[Region] = None,
                 bind: Optional[Mapping[str, Any]] = None,
                 check: bool = True):
        self.requested = requested
        if expression is None:
            expression = QueryWhereExpression(None, bind)
        elif isinstance(expression, str):
            expression = QueryWhereExpression(expression, bind)
        elif bind is not None:
            raise TypeError("New bind parameters passed, but expression is already a QueryWhereExpression.")
        self.where = expression.attach(self.requested, dataId=dataId, region=whereRegion, check=check)

    requested: DimensionGraph
    """Dimensions whose primary keys should be included in the result rows of
    the query (`DimensionGraph`).
    """

    where: QueryWhereClause
    """Structure containing objects that contribute to the WHERE clause of the
    query (`QueryWhereClause`).
    """

    @property
    def universe(self) -> DimensionUniverse:
        """All known dimensions (`DimensionUniverse`).
        """
        return self.requested.universe

    @property  # type: ignore
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
                element, = result
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

    @property  # type: ignore
    @cached_getter
    def temporal(self) -> NamedValueAbstractSet[DimensionElement]:
        """Dimension elements whose timespans should be included in the
        query (`NamedValueSet` of `DimensionElement`).
        """
        # An element may participate temporally in the query if:
        # - it's the most precise temporal element for its system in the
        #   requested dimensions (i.e. in `self.requested.temporal`);
        # - it isn't also given at query construction time.
        result: NamedValueSet[DimensionElement] = NamedValueSet()
        for family in self.mustHaveKeysJoined.temporal:
            element = family.choose(self.mustHaveKeysJoined.elements)
            assert isinstance(element, DimensionElement)
            if element not in self.where.dataId.graph.elements:
                result.add(element)
        if len(result) == 1 and not self.where.dataId.graph.temporal:
            # No temporal join or filter.  Even if this element might be
            # associated with temporal information, we don't need it for this
            # query.
            return NamedValueSet().freeze()
        return result.freeze()

    @property  # type: ignore
    @cached_getter
    def mustHaveKeysJoined(self) -> DimensionGraph:
        """Dimensions whose primary keys must be used in the JOIN ON clauses
        of the query, even if their tables do not appear (`DimensionGraph`).

        A `Dimension` primary key can appear in a join clause without its table
        via a foreign key column in table of a dependent dimension element or
        dataset.
        """
        names = set(self.requested.names | self.where.dimensions.names)
        return DimensionGraph(self.universe, names=names)

    @property  # type: ignore
    @cached_getter
    def mustHaveTableJoined(self) -> NamedValueAbstractSet[DimensionElement]:
        """Dimension elements whose associated tables must appear in the
        query's FROM clause (`NamedValueSet` of `DimensionElement`).
        """
        result = NamedValueSet(self.spatial | self.temporal | self.where.columns.keys())
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

    ingestDate: Optional[ColumnElement]
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

    keys: NamedKeyDict[Dimension, List[ColumnElement]]
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

    regions: NamedKeyDict[DimensionElement, SpatialRegionDatabaseRepresentation]
    """Columns that correspond to regions for elements that participate in a
    spatial join or filter in the query (`NamedKeyDict` mapping
    `DimensionElement` to `SpatialRegionDatabaseRepresentation`).

    In a `Query`, the keys of this dictionary must be exactly the elements
    in `QuerySummary.spatial`.
    """

    datasets: Optional[DatasetQueryColumns]
    """Columns that can be used to construct `DatasetRef` instances from query
    results.
    (`DatasetQueryColumns` or `None`).
    """

    def isEmpty(self) -> bool:
        """Return `True` if this query has no columns at all.
        """
        return not (self.keys or self.timespans or self.regions or self.datasets is not None)

    def getKeyColumn(self, dimension: Union[Dimension, str]) -> ColumnElement:
        """ Return one of the columns in self.keys for the given dimension.

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


@dataclass
class RegistryManagers:
    """Struct used to pass around the manager objects that back a `Registry`
    and are used internally by the query system.
    """

    collections: CollectionManager
    """Manager for collections (`CollectionManager`).
    """

    datasets: DatasetRecordStorageManager
    """Manager for datasets and dataset types (`DatasetRecordStorageManager`).
    """

    dimensions: DimensionRecordStorageManager
    """Manager for dimensions (`DimensionRecordStorageManager`).
    """

    tsRepr: Type[TimespanDatabaseRepresentation]
    """Type that encapsulates how timespans are represented in this database
    (`type`; subclass of `TimespanDatabaseRepresentation`).
    """

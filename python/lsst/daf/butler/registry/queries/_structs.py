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

from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Iterable, Iterator, List, Mapping, Optional, Tuple, Union

from lsst.utils.classes import cached_getter, immutable
from lsst.utils.sets.unboundable import MutableUnboundableSet, UnboundableSet
from sqlalchemy.sql import ColumnElement

from ...core import (
    ColumnTag,
    DataCoordinate,
    DataIdValue,
    DatasetColumnTag,
    DatasetType,
    Dimension,
    DimensionElement,
    DimensionGraph,
    DimensionKeyColumnTag,
    DimensionRecordColumnTag,
    DimensionUniverse,
    LogicalColumn,
    NamedKeyDict,
    NamedValueAbstractSet,
    NamedValueSet,
    SkyPixDimension,
    SpatialConstraint,
    SpatialRegionDatabaseRepresentation,
    TemporalConstraint,
    TimespanDatabaseRepresentation,
)

# We're not trying to add typing to the lex/yacc parser code, so MyPy
# doesn't know about some of these imports.
from .expressions import ExpressionPredicate
from .expressions.categorize import categorizeElementOrderByName, categorizeOrderByName


@dataclass(frozen=True, eq=False)
class QueryWhereClause:
    """Structure holding various contributions to a query's WHERE clause.

    Instances of this class should only be created by
    `QueryWhereExpression.attach`, which guarantees the consistency of its
    attributes.
    """

    @classmethod
    def combine(
        cls,
        dimensions: DimensionGraph,
        expression: str = "",
        *,
        bind: Optional[Mapping[str, Any]] = None,
        data_id: Optional[DataCoordinate] = None,
        spatial_constraint: Optional[SpatialConstraint] = None,
        temporal_constraint: Optional[TemporalConstraint] = None,
        defaults: Optional[DataCoordinate] = None,
        dataset_type_name: Optional[str] = None,
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
            A fully-expanded data ID identifying dimensions known in advance.
            If not provided, will be set to an empty data ID.
            ``data_id.hasRecords()`` must return `True`.
        spatial_constraint : `SpatialConstraint`, optional
            A spatial constraint that all rows must overlap.  If `None` and
            ``data_id`` is not `None`, ``data_id.region`` will be used to
            construct one.
        temporal_constraint : `TemporalConstraint`, optional
            A temporal constraint that all rows must overlap.  If `None` and
            ``data_id`` is not `None`, ``data_id.timespan`` will be used to
            construct one.
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
        if spatial_constraint is None and data_id is not None and data_id.region is not None:
            spatial_constraint = SpatialConstraint(data_id.region)
        if temporal_constraint is None and data_id is not None and data_id.timespan is not None:
            temporal_constraint = TemporalConstraint.from_timespan(data_id.timespan)
        if data_id is None:
            data_id = DataCoordinate.makeEmpty(dimensions.universe)
        if defaults is None:
            defaults = DataCoordinate.makeEmpty(dimensions.universe)
        expression_predicate: Optional[ExpressionPredicate] = None
        if expression is not None:
            expression_predicate = ExpressionPredicate.parse(
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
            spatial_constraint=spatial_constraint,
            temporal_constraint=temporal_constraint,
        )

    expression_predicate: ExpressionPredicate | None
    """A predicate that evaluates a string expression from the user
    (`ExpressionPredicate` or `None`).
    """

    data_id: DataCoordinate
    """A data ID identifying dimensions known before query construction
    (`DataCoordinate`).

    ``dataId.hasRecords()`` is guaranteed to return `True`.
    """

    spatial_constraint: SpatialConstraint | None
    """A spatial constraint that all result rows must overlap
    (`SpatialConstraint` or `None`).
    """

    temporal_constraint: TemporalConstraint | None
    """A temporal constraint that all result rows must overlap
    (`TemporalConstraint` or `None`).
    """

    @property  # type: ignore
    @cached_getter
    def dimension_constraints(self) -> Mapping[str, UnboundableSet[DataIdValue]]:
        """Combined pre-execution dimension constraints from all attributes of
        this object (`Mapping` [ `str`, `UnboundableSet` ]).
        """
        result: defaultdict[str, MutableUnboundableSet[DataIdValue]] = defaultdict(
            MutableUnboundableSet.make_full
        )
        for dimension, value in self.data_id.full.items():
            result[dimension.name].intersection_update({value})
        if self.expression_predicate is not None:
            for dimension_name, values in self.expression_predicate.dimension_constraints.items():
                result[dimension_name].intersection_update(values)
        return result


@dataclass(frozen=True)
class OrderByClauseColumn:
    """Information about single column in ORDER BY clause."""

    element: DimensionElement
    """Dimension element for data in this column (`DimensionElement`)."""

    column: Optional[str]
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
    data_id : `DataCoordinate`, optional
        A fully-expanded data ID identifying dimensions known in advance.  If
        not provided, will be set to an empty data ID.
        ``data_id.hasRecords()`` must return `True`.
    expression : `str` or `QueryWhereExpression`, optional
        A user-provided string WHERE expression.
    spatial_constraint : `SpatialConstraint`, optional
        A spatial constraint that all rows must overlap.  If `None` and
        ``data_id`` is not `None`, ``data_id.region`` will be used to construct
        one.
    temporal_constraint : `TemporalConstraint`, optional
        A temporal constraint that all rows must overlap.  If `None` and
        ``data_id`` is not `None`, ``data_id.timespan`` will be used to
        construct one.
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
        data_id: Optional[DataCoordinate] = None,
        expression: str = "",
        spatial_constraint: Optional[SpatialConstraint] = None,
        temporal_constraint: Optional[TemporalConstraint] = None,
        bind: Optional[Mapping[str, Any]] = None,
        defaults: Optional[DataCoordinate] = None,
        datasets: Iterable[DatasetType] = (),
        order_by: Optional[Iterable[str]] = None,
        limit: Optional[Tuple[int, Optional[int]]] = None,
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
            spatial_constraint=spatial_constraint,
            temporal_constraint=temporal_constraint,
            defaults=defaults,
            dataset_type_name=dataset_type_name,
            allow_orphans=not check,
        )
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
            if element not in self.where.data_id.graph.elements:
                result.add(element)
        if len(result) == 1:
            # There's no spatial join, but there might be a WHERE filter based
            # on a given region.
            if self.where.data_id.graph.spatial:
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

    @property  # type: ignore
    @cached_getter
    def temporal(self) -> NamedValueAbstractSet[DimensionElement]:
        """Dimension elements whose timespans should be included in the
        query (`NamedValueSet` of `DimensionElement`).
        """
        if len(self.mustHaveKeysJoined.temporal) > 1:
            # We don't actually have multiple temporal families in our current
            # dimension configuration, so this limitation should be harmless.
            raise NotImplementedError("Queries that should involve temporal joins are not yet supported.")
        result = NamedValueSet[DimensionElement]()
        if self.where.expression_predicate is not None:
            for tag in DimensionRecordColumnTag.filter_from(self.where.expression_predicate.columns_required):
                if tag.column == "timespan":
                    result.add(self.requested.universe[tag.element])
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
        names = set(self.requested.names)
        if self.where.expression_predicate is not None:
            names.update(
                tag.dimension
                for tag in DimensionKeyColumnTag.filter_from(self.where.expression_predicate.columns_required)
            )
        for dataset_type in self.datasets:
            names.update(dataset_type.dimensions.names)
        return DimensionGraph(self.universe, names=names)

    @property  # type: ignore
    @cached_getter
    def mustHaveTableJoined(self) -> NamedValueAbstractSet[DimensionElement]:
        """Dimension elements whose associated tables must appear in the
        query's FROM clause (`NamedValueSet` of `DimensionElement`).
        """
        result = NamedValueSet(self.spatial | self.temporal)
        if self.where.expression_predicate is not None:
            for tag in DimensionRecordColumnTag.filter_from(self.where.expression_predicate.columns_required):
                result.add(self.requested.universe[tag.element])
        if self.order_by is not None:
            result.update(self.order_by.elements)
        for dimension in self.mustHaveKeysJoined:
            if dimension.implied:
                result.add(dimension)
        for element in self.mustHaveKeysJoined.union(self.where.data_id.graph).elements:
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
        """Return `True` if this query has no columns at all."""
        return not (self.keys or self.timespans or self.regions or self.datasets is not None)

    def getKeyColumn(self, dimension: Union[Dimension, str]) -> ColumnElement:
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

    def make_logical_column_mapping(self) -> dict[ColumnTag, LogicalColumn]:
        """Create a dictionary with `ColumnTag` keys for the columns
        tracked by this object.

        This is a transitional method that converts from the old way of
        representing columns in queries (this class) to the new one (containers
        involving `ColumnTag`).
        """
        result: dict[ColumnTag, LogicalColumn] = {}
        for dimension in self.keys:
            result[DimensionKeyColumnTag(dimension.name)] = self.getKeyColumn(dimension)
        for element, region_column in self.regions.items():
            result[DimensionRecordColumnTag(element.name, "region")] = region_column
        for element, timespan_column in self.timespans.items():
            result[DimensionRecordColumnTag(element.name, "timespan")] = timespan_column
        if self.datasets is not None:
            dataset_type_name = self.datasets.datasetType.name
            result[DatasetColumnTag(dataset_type_name, "dataset_id")] = self.datasets.id
            result[DatasetColumnTag(dataset_type_name, "run")] = self.datasets.runKey
            if self.datasets.ingestDate is not None:
                result[DatasetColumnTag(dataset_type_name, "ingest_date")] = self.datasets.ingestDate
        return result

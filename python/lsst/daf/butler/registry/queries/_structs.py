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

from dataclasses import dataclass
from typing import Iterator, List, Optional, Union

from sqlalchemy.sql import ColumnElement

from ...core import (
    DatasetType,
    Dimension,
    DimensionElement,
    DimensionGraph,
    DimensionUniverse,
    ExpandedDataCoordinate,
    NamedKeyDict,
    NamedValueSet,
    SkyPixDimension,
    Timespan,
)
# We're not trying to add typing to the lex/yacc parser code, so MyPy
# doesn't know about some of these imports.
from .exprParser import Node, ParserYacc  # type: ignore


@dataclass
class QueryWhereExpression:
    """A struct representing a parsed user-provided WHERE expression.

    Parameters
    ----------
    universe : `DimensionUniverse`
        All known dimensions.
    expression : `str`, optional
        The string expression to parse.
    """
    def __init__(self, universe: DimensionUniverse, expression: Optional[str] = None):
        if expression:
            from .expressions import InspectionVisitor
            try:
                parser = ParserYacc()
                self.tree = parser.parse(expression)
            except Exception as exc:
                raise RuntimeError(f"Failed to parse user expression `{expression}'.") from exc
            visitor = InspectionVisitor(universe)
            assert self.tree is not None
            self.tree.visit(visitor)
            self.keys = visitor.keys
            self.metadata = visitor.metadata
        else:
            self.tree = None
            self.keys = NamedValueSet()
            self.metadata = NamedKeyDict()

    tree: Optional[Node]
    """The parsed user expression tree, if present (`Node` or `None`).
    """

    keys: NamedValueSet[Dimension]
    """All dimensions whose keys are referenced by the expression
    (`NamedValueSet` of `Dimension`).
    """

    metadata: NamedKeyDict[DimensionElement, List[str]]
    """All dimension elements metadata fields referenced by the expression
    (`NamedKeyDict` mapping `DimensionElement` to a `set` of field names).
    """


@dataclass
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
    dataId : `ExpandedDataCoordinate`, optional
        A fully-expanded data ID identifying dimensions known in advance.  If
        not provided, will be set to an empty data ID.
    expression : `str` or `QueryWhereExpression`, optional
        A user-provided string WHERE expression.
    """
    def __init__(self, requested: DimensionGraph, *,
                 dataId: Optional[ExpandedDataCoordinate] = None,
                 expression: Optional[Union[str, QueryWhereExpression]] = None):
        self.requested = requested
        self.dataId = dataId if dataId is not None else ExpandedDataCoordinate(requested.universe.empty, (),
                                                                               records=NamedKeyDict())
        self.expression = (expression if isinstance(expression, QueryWhereExpression)
                           else QueryWhereExpression(requested.universe, expression))

    requested: DimensionGraph
    """Dimensions whose primary keys should be included in the result rows of
    the query (`DimensionGraph`).
    """

    dataId: ExpandedDataCoordinate
    """A data ID identifying dimensions known before query construction
    (`ExpandedDataCoordinate`).
    """

    expression: QueryWhereExpression
    """Information about any parsed user WHERE expression
    (`QueryWhereExpression`).
    """

    @property
    def universe(self) -> DimensionUniverse:
        """All known dimensions (`DimensionUniverse`).
        """
        return self.requested.universe

    @property
    def spatial(self) -> NamedValueSet[DimensionElement]:
        """Dimension elements whose regions and skypix IDs should be included
        in the query (`NamedValueSet` of `DimensionElement`).
        """
        # An element may participate spatially in the query if:
        # - it's the most precise spatial element for its system in the
        #   requested dimensions (i.e. in `self.requested.spatial`);
        # - it isn't also given at query construction time.
        result = NamedValueSet(self.mustHaveKeysJoined.spatial - self.dataId.graph.elements)
        if len(result) == 1:
            # There's no spatial join, but there might be a WHERE filter based
            # on a given region.
            if self.dataId.graph.spatial:
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
                return NamedValueSet()
        elif len(result) > 1:
            # There's a spatial join.  Those require the common SkyPix
            # system to be included in the query in order to connect them.
            result.add(self.universe.commonSkyPix)
        return result

    @property
    def temporal(self) -> NamedValueSet[DimensionElement]:
        """Dimension elements whose timespans should be included in the
        query (`NamedValueSet` of `DimensionElement`).
        """
        # An element may participate temporally in the query if:
        # - it's the most precise temporal element for its system in the
        #   requested dimensions (i.e. in `self.requested.temporal`);
        # - it isn't also given at query construction time.
        result = NamedValueSet(self.mustHaveKeysJoined.temporal - self.dataId.graph.elements)
        if len(result) == 1 and not self.dataId.graph.temporal:
            # No temporal join or filter.  Even if this element might be
            # associated with temporal information, we don't need it for this
            # query.
            return NamedValueSet()
        return result

    @property
    def mustHaveKeysJoined(self) -> DimensionGraph:
        """Dimensions whose primary keys must be used in the JOIN ON clauses
        of the query, even if their tables do not appear (`DimensionGraph`).

        A `Dimension` primary key can appear in a join clause without its table
        via a foreign key column in table of a dependent dimension element or
        dataset.
        """
        names = set(self.requested.names | self.expression.keys.names)
        return DimensionGraph(self.universe, names=names)

    @property
    def mustHaveTableJoined(self) -> NamedValueSet[DimensionElement]:
        """Dimension elements whose associated tables must appear in the
        query's FROM clause (`NamedValueSet` of `DimensionElement`).
        """
        result = NamedValueSet(self.spatial | self.temporal | self.expression.metadata.keys())
        for dimension in self.mustHaveKeysJoined:
            if dimension.implied:
                result.add(dimension)
        for element in self.mustHaveKeysJoined.union(self.dataId.graph).elements:
            if element.alwaysJoin:
                result.add(element)
        return result


@dataclass
class DatasetQueryColumns:
    """A struct containing the columns used to reconstruct `DatasetRef`
    instances from query results.
    """

    id: ColumnElement
    """Column containing the unique integer ID for this dataset.
    """

    runKey: ColumnElement
    """Foreign key column to the `~CollectionType.RUN` collection that holds
    this dataset.
    """

    rank: Optional[ColumnElement] = None
    """Column containing the index into the ordered sequence of given
    collections for the collection in which this dataset was found.
    """

    def __iter__(self) -> Iterator[ColumnElement]:
        yield self.id
        yield self.runKey
        if self.rank is not None:
            yield self.rank


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
        self.datasets = NamedKeyDict()

    keys: NamedKeyDict[Dimension, List[ColumnElement]]
    """Columns that correspond to the primary key values of dimensions
    (`NamedKeyDict` mapping `Dimension` to a `list` of `ColumnElement`).

    Each value list contains columns from multiple tables corresponding to the
    same dimension, and the query should constrain the values of those columns
    to be the same.

    In a `Query`, the keys of this dictionary must include at least the
    dimensions in `QuerySummary.requested` and `QuerySummary.dataId.graph`.
    """

    timespans: NamedKeyDict[DimensionElement, Timespan[ColumnElement]]
    """Columns that correspond to timespans for elements that participate in a
    temporal join or filter in the query (`NamedKeyDict` mapping
    `DimensionElement` to `Timespan` of `ColumnElement`).

    In a `Query`, the keys of this dictionary must be exactly the elements
    in `QuerySummary.temporal`.
    """

    regions: NamedKeyDict[DimensionElement, ColumnElement]
    """Columns that correspond to regions for elements that participate in a
    spatial join or filter in the query (`NamedKeyDict` mapping
    `DimensionElement` to `ColumnElement`).

    In a `Query`, the keys of this dictionary must be exactly the elements
    in `QuerySummary.spatial`.
    """

    datasets: NamedKeyDict[DatasetType, DatasetQueryColumns]
    """Columns that can be used to construct `DatasetRef` instances from query
    results, for each `DatasetType` included in the query
    (`NamedKeyDict` [ `DatasetType`, `DatasetQueryColumns` ] ).
    """

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

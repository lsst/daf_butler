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

__all__ = ("QueryBuilder",)

import logging
from abc import ABC, abstractmethod
from sqlalchemy.sql import and_, or_, not_, literal

from ..core import DimensionJoin, DimensionSet
from ..exprParser import ParserYacc, TreeVisitor
from .resultColumnsManager import ResultColumnsManager

_LOG = logging.getLogger(__name__)


class _ClauseVisitor(TreeVisitor):
    """Implement TreeVisitor to convert user expression into SQLAlchemy
    clause.

    Parameters
    ----------
    queryBuilder: `QueryBuilder`
        Query builder object the new visitor is to be a helper for.
    """

    unaryOps = {"NOT": lambda x: not_(x),
                "+": lambda x: +x,
                "-": lambda x: -x}
    """Mapping or unary operator names to corresponding functions"""

    binaryOps = {"OR": lambda x, y: or_(x, y),
                 "AND": lambda x, y: and_(x, y),
                 "=": lambda x, y: x == y,
                 "!=": lambda x, y: x != y,
                 "<": lambda x, y: x < y,
                 "<=": lambda x, y: x <= y,
                 ">": lambda x, y: x > y,
                 ">=": lambda x, y: x >= y,
                 "+": lambda x, y: x + y,
                 "-": lambda x, y: x - y,
                 "*": lambda x, y: x * y,
                 "/": lambda x, y: x / y,
                 "%": lambda x, y: x % y}
    """Mapping or binary operator names to corresponding functions"""

    def __init__(self, queryBuilder):
        self.queryBuilder = queryBuilder

    def visitNumericLiteral(self, value, node):
        # Docstring inherited from TreeVisitor.visitNumericLiteral
        # Convert string value into float or int
        if value.isdigit():
            value = int(value)
        else:
            value = float(value)
        return literal(value)

    def visitStringLiteral(self, value, node):
        # Docstring inherited from TreeVisitor.visitStringLiteral
        return literal(value)

    def visitIdentifier(self, name, node):
        # Docstring inherited from TreeVisitor.visitIdentifier
        table, sep, column = name.partition('.')
        if column:
            selectable = self.queryBuilder.findSelectableByName(table)
            if selectable is None:
                raise ValueError(f"No table or clause with name '{table}' in this query.")
        else:
            column = table
            selectable = self.queryBuilder.findSelectableForLink(column)
            if selectable is None:
                raise ValueError(f"No table or clause providing link '{column}' in this query.")
        try:
            return selectable.columns[column]
        except KeyError as err:
            raise ValueError(f"No column '{column}' found.") from err

    def visitUnaryOp(self, operator, operand, node):
        # Docstring inherited from TreeVisitor.visitUnaryOp
        func = self.unaryOps.get(operator)
        if func:
            return func(operand)
        else:
            raise ValueError(f"Unexpected unary operator `{operator}' in `{node}'.")

    def visitBinaryOp(self, operator, lhs, rhs, node):
        # Docstring inherited from TreeVisitor.visitBinaryOp
        func = self.binaryOps.get(operator)
        if func:
            return func(lhs, rhs)
        else:
            raise ValueError(f"Unexpected binary operator `{operator}' in `{node}'.")

    def visitIsIn(self, lhs, values, not_in, node):
        # Docstring inherited from TreeVisitor.visitIsIn
        if not_in:
            return lhs.notin_(values)
        else:
            return lhs.in_(values)

    def visitParens(self, expression, node):
        # Docstring inherited from TreeVisitor.visitParens
        return expression.self_group()


class QueryBuilder(ABC):
    """Helper class for constructing a query that joins together several
    related `Dimension` and `DimensionJoin` tables.

    `QueryBuilder` and its subclasses are responsible for ensuring that tables
    and views that represent `Dimension` and `DatasetType` objects are
    properly joined into and selected from in a query.  They do not in general
    take responsibility for ensuring that a self-consistent or useful set of
    `Dimension` and `DatasetType` tables/views are included in the query, but
    do provide special `classmethod` constructors that guarantee this for
    certain common cases.

    Parameters
    ----------
    registry : `SqlRegistry`
        Registry instance the query is being run against.
    fromClause : `sqlalchemy.sql.expression.FromClause`, optional
        Initial FROM clause for the query.
    whereClause : SQLAlchemy boolean expression, optional
        Expression to use as the initial WHERE clause.
    """

    def __init__(self, registry, *, fromClause=None, whereClause=None):
        self.registry = registry
        self._resultColumns = ResultColumnsManager(self.registry)
        self._fromClause = fromClause
        self._whereClause = whereClause
        self._selectablesForDimensionElements = {}
        self._elementsCache = None

    @classmethod
    def fromDimensions(cls, registry, dimensions, addResultColumns=True):
        """Construct a query for a complete `DimensionGraph`.

        This ensures that all `DimensionJoin` tables that relate the given
        dimensions are correctly included.

        Parameters
        ----------
        registry : `SqlRegistry`
            Registry instance the query is being run against.
        dimensions : `DimensionGraph`
            The full set of dimensions to be included in the query.
        addResultColumns : `bool`
            If `True` (default), automatically include all link columns for
            the given dimensions in the SELECT clause for the query.
        """
        self = cls(registry)
        for dimension in dimensions:
            self.joinDimensionElement(dimension)
        for join in dimensions.joins(summaries=False):
            if not join.asNeeded:
                self.joinDimensionElement(join)
        if addResultColumns:
            for link in dimensions.links():
                self.selectDimensionLink(link)
        return self

    @property
    def fromClause(self):
        """The current FROM clause for the in-progress query
        (`sqlalchemy.sql.FromClause`).

        The FROM clause cannot be modified via this property; use `join`
        instead.
        """
        return self._fromClause

    @property
    def whereClause(self):
        """The current WHERE clause for the in-progress query (SQLAlchemy
        column expression).

        The WHERE clause cannot be modified via this property; use
        `whereSqlExpressoin` instead.
        """
        return self._whereClause

    @property
    def resultColumns(self):
        """The helper object that manages the SELECT clause of the query
        (`ResultColumnsManager`).
        """
        return self._resultColumns

    def join(self, selectable, links, isOuter=False):
        """Join a new selectable (table, view, or subquery) into the query.

        Parameters
        ----------
        selectable : `sqlalchemy.sql.FromClause`
            SQLAlchemy representation of a table, view, or subquery to be
            joined into the the query.
        links : iterable of `str`
            The dimension link values (i.e. data ID keys) that are columns
            for ``selectable``; any of these that are already present on any
            other selectable in the query will be used in the ON expression
            for this join.
        isOuter : `bool`
            If `True`, perform a LEFT OUTER JOIN instead of a regular (INNER)
            JOIN.
        """
        if self.fromClause is None:
            self._fromClause = selectable
            return
        joinOn = []
        for link in links:
            selectableAlreadyIncluded = self.findSelectableForLink(link)
            if selectableAlreadyIncluded is not None:
                joinOn.append(selectableAlreadyIncluded.columns[link] == selectable.columns[link])
        if joinOn:
            self._fromClause = self.fromClause.join(selectable, and_(*joinOn), isouter=isOuter)
        else:
            # New table is completely unrelated to all already-included
            # tables. We need a cross join here but SQLAlchemy does not have
            # specific method for that. Using join() without `onclause` will
            # try to join on FK and will raise an exception for unrelated
            # tables, so we have to use `onclause` which is always true.
            self._fromClause = self.fromClause.join(selectable, literal(True) == literal(True),
                                                    isouter=isOuter)

    def whereSqlExpression(self, sqlExpression, op=and_):
        """Add a SQL expression to the query's WHERE clause.

        Parameters
        ----------
        sqlExpression: `sqlalchemy.sql.ColumnElement`
            SQLAlchemy boolean column expression.
        op : `sqlalchemy.sql.operator`
            Binary operator to use if a WHERE expression already exists.
        """
        if self.whereClause is None:
            self._whereClause = sqlExpression
        else:
            self._whereClause = op(self.whereClause, sqlExpression)

    def whereParsedExpression(self, expression, op=and_):
        """Add a parsed dimension expression to the query's WHERE clause.

        Parameters
        ----------
        expression : `str`
            String expression involving dimensions.
        op : `sqlalchemy.sql.operator`
            Binary operator to use if a WHERE expression already exists.
        """
        try:
            parser = ParserYacc()
            expression = parser.parse(expression)
        except Exception as exc:
            raise ValueError(f"Failed to parse user expression `{expression}'") from exc
        if expression:
            visitor = _ClauseVisitor(self)
            sqlExpression = expression.visit(visitor)
            _LOG.debug("where from expression: %s", sqlExpression)
            self.whereSqlExpression(sqlExpression, op=op)

    def whereDataId(self, dataId, op=and_):
        """Add a dimension constraint from a data ID to the query's WHERE
        clause.

        All data ID values that correspond to dimension values already included
        in the query will be used in the constraint; any others will be
        ignored.

        Parameters
        ----------
        dataId : `DataId` or `dict`
            Data ID to require (at least partial) dimension equality with.
        op : `sqlalchemy.sql.operator`
            Binary operator to use if a WHERE expression already exists.

        Returns
        -------
        links : `set` of `str`
            The data ID keys actually used in the constraint.
        """
        links = set()
        terms = []
        for key, value in dataId.items():
            selectable = self.findSelectableForLink(key)
            if selectable is not None:
                links.add(key)
                terms.append(selectable.columns[key] == value)
        if terms:
            self.whereSqlExpression(and_(*terms), op=op)
        return links

    def selectDimensionLink(self, link):
        """Add a dimension link column to the SELECT clause of the query.

        Parameters
        ----------
        link : `str`
            Dimension link name (i.e. data ID key) to select.
        """
        selectable = self.findSelectableForLink(link)
        if selectable is None:
            raise ValueError(f"No table involving link {link} in query.")
        self.resultColumns.addDimensionLink(selectable, link)

    def build(self, whereSql=None):
        """Return the full SELECT query.

        Parameters
        ----------
        whereSql
            An additional SQLAlchemy boolean column expression to include
            in the query.  Unlike the `whereSqlExpression` method, this
            does not modify the builder itself.

        Returns
        -------
        query: `sqlalchemy.sql.Select`
            Completed query that combines the FROM clause represented by
            `fromClause`, the WHERE clause represented by `whereClause`,
            and the SELECT clause managed by `resultColumns`.
        """
        query = self.resultColumns.selectFrom(self.fromClause)
        if self._whereClause is not None:
            query = query.where(self._whereClause)
        if whereSql is not None:
            query = query.where(whereSql)
        if _LOG.isEnabledFor(logging.DEBUG):
            try:
                compiled = query.compile(bind=self.registry._connection.engine,
                                         compile_kwargs={"literal_binds": True})
            except AttributeError:
                # Workaround apparent SQLAlchemy bug that sometimes treats a
                # list as if it were a string.
                compiled = str(query)
            _LOG.debug("building query: %s", compiled)
        return query

    def __str__(self):
        query = self.build()
        try:
            compiled = str(query.compile(bind=self.registry._connection.engine,
                                         compile_kwargs={"literal_binds": True}))
        except AttributeError:
            # Workaround apparent SQLAlchemy bug that sometimes treats a
            # list as if it were a string.
            compiled = str(query)
        return compiled

    def execute(self, whereSql=None, **kwds):
        """Build and execute the query, iterating over result rows.

        Parameters
        ----------
        whereSql
            An additional SQLAlchemy boolean column expression to include
            in the query.  Unlike the `whereSqlExpression` method, this
            does not modify the builder itself.
        kwds
            Additional keyword arguments forwarded to `convertResultRow`.

        Yields
        ------
        result
            Objects of the type returned by `convertResultRow`.

        Notes
        -----
        Query rows that include disjoint regions are automatically filtered
        out.
        """
        query = self.build(whereSql=whereSql)
        results = self.registry._connection.execute(query)
        total = 0
        count = 0
        for row in results:
            total += 1
            managed = self.resultColumns.manageRow(row=row)
            if managed.areRegionsDisjoint():
                continue
            count += 1
            yield self.convertResultRow(managed, **kwds)
        _LOG.debug("Total %d rows in result set, %d after region filtering", total, count)

    def executeOne(self, whereSql=None, **kwds):
        """Build and execute the query, returning a single result row.

        Parameters
        ----------
        whereSql
            An additional SQLAlchemy boolean column expression to include
            in the query.  Unlike the `whereSqlExpression` method, this
            does not modify the builder itself.
        kwds
            Additional keyword arguments forwarded to `convertResultRow`.

        Returns
        -------
        result
            A single object of the type returned by `convertResultRow`, or
            `None` if the query generated no results.

        Notes
        -----
        A query row that include disjoint regions is filtered out, resulting
        in `None` being returned.
        """
        query = self.build(whereSql=whereSql)
        results = self.registry._connection.execute(query)
        for row in results:
            managed = self.resultColumns.manageRow(row)
            if managed is None or managed.areRegionsDisjoint():
                continue
            return self.convertResultRow(managed, **kwds)
        return None

    def getIncludedDimensionElements(self):
        """Return the set of all `DimensionElement` objects explicitly
        included in the query.

        Returns
        -------
        dimensions : `DimensionSet`
            All `Dimension` and `DimensionJoin` objects represented in the
            query.
        """
        if self._elementsCache is None:
            self._elementsCache = DimensionSet(self.registry.dimensions,
                                               self._selectablesForDimensionElements.keys())
        return self._elementsCache

    def joinDimensionElement(self, element):
        """Add a table or view for a `DimensionElement` to the query.

        Tables can be added in any order, and repeated calls with the same
        element are safe no-ops.  Tables are automatically joined to any other
        tables already in the query with which they share any dependency
        relationship, but callers are responsible for making sure any such
        tables are added (and in the right order, in contexts where that
        matters).

        Attempting to add a table for a `DimensionElement` that does not have
        a direct in-database representation (such as the "skypix" `Dimension`)
        is also a safe no-op.

        Parameters
        ----------
        element : `DimensionElement`
            `Dimension` or `DimensionJoin` associated with the table or view
            to be added.

        Returns
        -------
        table : `sqlalchemy.FromClause` or `None`
            SQLAlchemy object representing the table or view added, or
            `None` if there is no table for the given element.
        """
        table = self._selectablesForDimensionElements.get(element)
        if table is None:
            # Table isn't already in the output query, see if we can add it.
            table = self.registry._schema.tables.get(element.name)
            if table is None:
                # This element doesn't have an associated table.
                return None
            # Invalidate accessor cache, since we're about to mutate self.
            self._elementsCache = None
            # We found a table, add it to the FROM clause.
            self.join(table, element.links(implied=True))
            # Now that we've updated self.fromClause, record that we have a
            # table for this element in the query.
            self._selectablesForDimensionElements[element] = table
            # Spatial joins need extra refinement; see if this is one.
            if isinstance(element, DimensionJoin):
                lhsHolder = self.registry.dimensions.extract(element.lhs).getRegionHolder()
                rhsHolder = self.registry.dimensions.extract(element.rhs).getRegionHolder()
                if lhsHolder is not None and rhsHolder is not None:
                    # This is a spatial join, but the database only knows
                    # about dimension entries that *might* overlap, and can't
                    # conclusively state when they do.  That means we need to
                    # filter the query in Python later, and to do that we need
                    # to include the regions of those dimensions in the query
                    # results. First we make sure the tables that provide
                    # those regions are in the query.  Note that this does
                    # nothing and returns None for skypix, but that's okay
                    # because ResultColumnsManager knows to handle that case
                    # specially.
                    lhsTable = self.joinDimensionElement(lhsHolder)
                    rhsTable = self.joinDimensionElement(rhsHolder)
                    self.resultColumns.addRegion(lhsTable, lhsHolder)
                    self.resultColumns.addRegion(rhsTable, rhsHolder)
        return table

    def findSelectableForLink(self, link):
        """Return a selectable (table, view, or subquery) already in the query
        that contains a column corresponding to the given dimension link, or
        `None` if no such selectable exists.

        If multiple selectables can provide the given link, which one is
        returned is unspecified, but the returned selectable is guaranteed
        to not to have been included with an outer join.

        This method should be reimplemented by subclasses by delegating to
        ``super()`` first and returning its result if not `None`.

        Parameters
        ----------
        link : `str`
            Dimension link name (i.e. data ID key).

        Returns
        -------
        selectable : `sqlalchemy.sql.FromClause` or `None`
            A table, view, or subquery that contains the given link column, or
            `None` if one was not found.
        """
        for element in self.registry.dimensions.withLink(link):
            selectable = self._selectablesForDimensionElements.get(element)
            if selectable is not None:
                return selectable
        return None

    def findSelectableByName(self, name):
        """Return the selectable already in the query with the given name, or
        `None` if no such selectable exists.

        This method should be reimplemented by subclasses by delegating to
        ``super()`` first and returning its result if not `None`.

        Returns
        -------
        selectable : `sqlalchemy.sql.FromClause` or `None`
            A table, view, or subquery with the given name, or `None` if one
            was not found.
        """
        element = self.getIncludedDimensionElements().get(name)
        if element is None:
            return None
        return self._selectablesForDimensionElements[element]

    @abstractmethod
    def convertResultRow(self, managed, **kwds):
        """Convert a query result row to the type appropriate for this
        `QueryBuilder`.

        This method is a customization point for `execute` that must be
        implemented by subclasses.  Other code should generally not need
        to call it directly.

        Parameters
        ----------
        managed : `ResultColumnsManager.ManagedRow`
            Intermediate row object to convert.
        kwds :
            Additional keyword arguments defined by subclasses.

        Returns
        -------
        result
            An object that corresponds to a single query result row,
            with type defined by the subclass implementation.
        """
        raise NotImplementedError("Must be implemented by subclasses.")

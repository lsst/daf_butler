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

__all__ = ()  # all symbols intentionally private; for internal package use.


from sqlalchemy.sql import not_, or_, and_, literal, FromClause

from ...core.utils import NamedValueSet, NamedKeyDict
from ...core import DimensionUniverse, Dimension, DimensionElement
from .exprParser import TreeVisitor
from ._structs import QueryColumns


def categorizeIdentifier(universe: DimensionUniverse, name: str):
    """Categorize an identifier in a parsed expression as either a `Dimension`
    name (indicating the primary key for that dimension) or a non-primary-key
    column in a `DimensionElement` table.

    Parameters
    ----------
    universe : `DimensionUniverse`
        All known dimensions.
    name : `str`
        Identifier to categorize.

    Returns
    -------
    element : `DimensionElement`
        The `DimensionElement` the identifier refers to.
    column : `str` or `None`
        The name of a column in the table for ``element``, or `None` if
        ``element`` is a `Dimension` and the requested column is its primary
        key.

    Raises
    ------
    LookupError
        Raised if the identifier refers to a nonexistent `DimensionElement`
        or column.
    RuntimeError
        Raised if the expression refers to a primary key in an illegal way.
        This exception includes a suggestion for how to rewrite the expression,
        so at least its message should generally be propagated up to a context
        where the error can be interpreted by a human.
    """
    table, sep, column = name.partition('.')
    if column:
        try:
            element = universe[table]
        except KeyError as err:
            raise LookupError(f"No dimension element with name '{table}'.") from err
        if isinstance(element, Dimension) and column == element.primaryKey.name:
            # Allow e.g. "visit.id = x" instead of just "visit = x"; this
            # can be clearer.
            return element, None
        elif column in element.graph.names:
            # User said something like "patch.tract = x" or
            # "tract.tract = x" instead of just "tract = x" or
            # "tract.id = x", which is at least needlessly confusing and
            # possibly not actually a column name, though we can guess
            # what they were trying to do.
            # Encourage them to clean that up and try again.
            raise RuntimeError(
                f"Invalid reference to '{table}.{column}' in expression; please use "
                f"'{column}' or '{column}.{universe[column].primaryKey.name}' instead."
            )
        else:
            if column not in element.RecordClass.__slots__:
                raise LookupError(f"Column '{column} not found in table for {element}.")
            return element, column
    else:
        try:
            dimension = universe.dimensions[table]
        except KeyError as err:
            raise LookupError(f"No dimension with name '{table}.") from err
        return dimension, None


class InspectionVisitor(TreeVisitor):
    """Implements TreeVisitor to identify dimension elements that need
    to be included in a query, prior to actually constructing a SQLAlchemy
    WHERE clause from it.

    Parameters
    ----------
    universe : `DimensionUniverse`
        All known dimensions.
    """

    def __init__(self, universe: DimensionUniverse):
        self.universe = universe
        self.keys = NamedValueSet()
        self.metadata = NamedKeyDict()

    def visitNumericLiteral(self, value, node):
        # Docstring inherited from TreeVisitor.visitNumericLiteral
        pass

    def visitStringLiteral(self, value, node):
        # Docstring inherited from TreeVisitor.visitStringLiteral
        pass

    def visitTimeLiteral(self, value, node):
        # Docstring inherited from TreeVisitor.visitTimeLiteral
        pass

    def visitIdentifier(self, name, node):
        # Docstring inherited from TreeVisitor.visitIdentifier
        element, column = categorizeIdentifier(self.universe, name)
        if column is not None:
            self.metadata.setdefault(element, []).append(column)
        else:
            self.keys.add(element)

    def visitUnaryOp(self, operator, operand, node):
        # Docstring inherited from TreeVisitor.visitUnaryOp
        pass

    def visitBinaryOp(self, operator, lhs, rhs, node):
        # Docstring inherited from TreeVisitor.visitBinaryOp
        pass

    def visitIsIn(self, lhs, values, not_in, node):
        # Docstring inherited from TreeVisitor.visitIsIn
        pass

    def visitParens(self, expression, node):
        # Docstring inherited from TreeVisitor.visitParens
        pass

    def visitRangeLiteral(self, start, stop, stride, node):
        # Docstring inherited from TreeVisitor.visitRangeLiteral
        pass


class ClauseVisitor(TreeVisitor):
    """Implements TreeVisitor to convert the tree into a SQLAlchemy WHERE
    clause.

    Parameters
    ----------
    universe : `DimensionUniverse`
        All known dimensions.
    columns: `QueryColumns`
        Struct that organizes the special columns known to the query
        under construction.
    elements: `NamedKeyDict`
        `DimensionElement` instances and their associated tables.
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

    def __init__(self, universe: DimensionUniverse,
                 columns: QueryColumns, elements: NamedKeyDict[DimensionElement, FromClause]):
        self.universe = universe
        self.columns = columns
        self.elements = elements

    def visitNumericLiteral(self, value, node):
        # Docstring inherited from TreeVisitor.visitNumericLiteral
        # Convert string value into float or int
        try:
            value = int(value)
        except ValueError:
            value = float(value)
        return literal(value)

    def visitStringLiteral(self, value, node):
        # Docstring inherited from TreeVisitor.visitStringLiteral
        return literal(value)

    def visitTimeLiteral(self, value, node):
        # Docstring inherited from TreeVisitor.visitTimeLiteral
        return literal(value)

    def visitIdentifier(self, name, node):
        # Docstring inherited from TreeVisitor.visitIdentifier
        element, column = categorizeIdentifier(self.universe, name)
        if column is not None:
            return self.elements[element].columns[column]
        else:
            return self.columns.getKeyColumn(element)

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

        # `values` is a list of literals and ranges, range is represented
        # by a tuple (start, stop, stride). We need to transform range into
        # some SQL construct, simplest would be to generate a set of literals
        # and add it to the same list but it could become too long. What we
        # do here is to introduce some large limit on the total number of
        # items in IN() and if range exceeds that limit then we do something
        # like:
        #
        #    X IN (1, 2, 3)
        #    OR
        #    (X BETWEEN START AND STOP AND MOD(X, STRIDE) = MOD(START, STRIDE))
        #
        # or for NOT IN case
        #
        #    NOT (X IN (1, 2, 3)
        #         OR
        #         (X BETWEEN START AND STOP
        #          AND MOD(X, STRIDE) = MOD(START, STRIDE)))

        max_in_items = 1000

        # split the list into literals and ranges
        literals, ranges = [], []
        for item in values:
            if isinstance(item, tuple):
                ranges.append(item)
            else:
                literals.append(item)

        clauses = []
        for start, stop, stride in ranges:
            count = (stop - start + 1) // stride
            if len(literals) + count > max_in_items:
                # X BETWEEN START AND STOP
                #    AND MOD(X, STRIDE) = MOD(START, STRIDE)
                expr = lhs.between(start, stop)
                if stride != 1:
                    expr = and_(expr, (lhs % stride) == (start % stride))
                clauses.append(expr)
            else:
                # add all values to literal list, stop is inclusive
                literals += [literal(value) for value in range(start, stop+1, stride)]

        if literals:
            # add IN() in front of BETWEENs
            clauses.insert(0, lhs.in_(literals))

        expr = or_(*clauses)
        if not_in:
            expr = not_(expr)

        return expr

    def visitParens(self, expression, node):
        # Docstring inherited from TreeVisitor.visitParens
        return expression.self_group()

    def visitRangeLiteral(self, start, stop, stride, node):
        # Docstring inherited from TreeVisitor.visitRangeLiteral

        # Just return a triple and let parent clauses handle it,
        # stride can be None which means the same as 1.
        return (start, stop, stride or 1)

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

import dataclasses
from typing import (
    Any,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
    TYPE_CHECKING,
    Union,
)

import sqlalchemy
from sqlalchemy.ext.compiler import compiles

from ...core import (
    DimensionUniverse,
    Dimension,
    DimensionElement,
    NamedKeyDict,
    NamedValueSet,
)
from ...core.ddl import AstropyTimeNsecTai
from .exprParser import Node, TreeVisitor
from ._structs import QueryColumns

if TYPE_CHECKING:
    import astropy.time


class _TimestampColumnElement(sqlalchemy.sql.ColumnElement):
    """Special ColumnElement type used for TIMESTAMP columns in expressions.

    TIMESTAMP columns in expressions are usually compared to time literals
    which are `astropy.time.Time` instances that are converted to integer
    nanoseconds since Epoch. For comparison we need to convert TIMESTAMP
    column value to the same type. This type is a wrapper for actual column
    that has special dialect-specific compilation methods defined below
    transforming column in that common type.

    This mechanism is only used for expressions in WHERE clause, values of the
    TIMESTAMP columns returned from queries are still handled by standard
    mechanism and they are converted to `datetime` instances.
    """
    def __init__(self, column: sqlalchemy.sql.ColumnElement):
        super().__init__()
        self._column = column


@compiles(_TimestampColumnElement, "sqlite")
def compile_timestamp_sqlite(element: Any, compiler: Any, **kw: Mapping[str, Any]) -> str:
    """Compilation of TIMESTAMP column for SQLite.

    SQLite defines ``strftime`` function that can be used to convert timestamp
    value to Unix seconds.
    """
    return f"STRFTIME('%s', {element._column.name})*1000000000"


@compiles(_TimestampColumnElement, "postgresql")
def compile_timestamp_pg(element: Any, compiler: Any, **kw: Mapping[str, Any]) -> str:
    """Compilation of TIMESTAMP column for PostgreSQL.

    PostgreSQL can use `EXTRACT(epoch FROM timestamp)` function.
    """
    return f"EXTRACT(epoch FROM {element._column.name})*1000000000"


def categorizeIngestDateId(name: str) -> bool:
    """Categorize an identifier in a parsed expression as an ingest_date
    attribute of a dataset table.

    Parameters
    ----------
    name : `str`
        Identifier to categorize.

    Returns
    -------
    isIngestDate : `bool`
        True is returned if identifier name is ``ingest_date``.
    """
    # TODO: this is hardcoded for now, may be better to extract it from schema
    # but I do not know how to do it yet.
    return name == "ingest_date"


def categorizeElementId(universe: DimensionUniverse, name: str) -> Tuple[DimensionElement, Optional[str]]:
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
                f"Invalid reference to '{table}.{column}' "  # type: ignore
                f"in expression; please use '{column}' or "
                f"'{column}.{universe[column].primaryKey.name}' instead."
            )
        else:
            if column not in element.RecordClass.fields.standard.names:
                raise LookupError(f"Column '{column} not found in table for {element}.")
            return element, column
    else:
        try:
            dimension = universe[table]
        except KeyError as err:
            raise LookupError(f"No dimension with name '{table}.") from err
        return dimension, None


@dataclasses.dataclass
class InspectionSummary:
    """Result object used by `InspectionVisitor` to gather information about
    a parsed expression.
    """

    def merge(self, other: InspectionSummary) -> InspectionSummary:
        """Merge ``other`` into ``self``, making ``self`` a summary of both
        expression tree branches.

        Parameters
        ----------
        other : `InspectionSummary`
            The other summary object.

        Returns
        -------
        self : `InspectionSummary`
            The merged summary (updated in-place).
        """
        self.dimensions.update(other.dimensions)
        for element, columns in other.columns.items():
            self.columns.setdefault(element, set()).update(columns)
        self.hasIngestDate = self.hasIngestDate or other.hasIngestDate
        return self

    dimensions: NamedValueSet[Dimension] = dataclasses.field(default_factory=NamedValueSet)
    """Dimensions whose primary keys were referenced anywhere in this branch
    (`NamedValueSet` [ `Dimension` ]).
    """

    columns: NamedKeyDict[DimensionElement, Set[str]] = dataclasses.field(default_factory=NamedKeyDict)
    """Dimension element tables whose columns were referenced anywhere in this
    branch (`NamedKeyDict` [ `DimensionElement`, `set` [ `str` ] ]).
    """

    hasIngestDate: bool = False
    """Whether this expression includes the special dataset ingest date
    identifier (`bool`).
    """


class InspectionVisitor(TreeVisitor[InspectionSummary]):
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

    def visitNumericLiteral(self, value: str, node: Node) -> InspectionSummary:
        # Docstring inherited from TreeVisitor.visitNumericLiteral
        return InspectionSummary()

    def visitStringLiteral(self, value: str, node: Node) -> InspectionSummary:
        # Docstring inherited from TreeVisitor.visitStringLiteral
        return InspectionSummary()

    def visitTimeLiteral(self, value: astropy.time.Time, node: Node) -> InspectionSummary:
        # Docstring inherited from TreeVisitor.visitTimeLiteral
        return InspectionSummary()

    def visitIdentifier(self, name: str, node: Node) -> InspectionSummary:
        # Docstring inherited from TreeVisitor.visitIdentifier
        if categorizeIngestDateId(name):
            self.hasIngestDate = True
            return InspectionSummary(
                hasIngestDate=True,
            )
        element, column = categorizeElementId(self.universe, name)
        if column is None:
            assert isinstance(element, Dimension)
            return InspectionSummary(
                dimensions=NamedValueSet({element}),
            )
        else:
            return InspectionSummary(columns=NamedKeyDict({element: {column}}))

    def visitUnaryOp(self, operator: str, operand: InspectionSummary, node: Node
                     ) -> InspectionSummary:
        # Docstring inherited from TreeVisitor.visitUnaryOp
        return operand

    def visitBinaryOp(self, operator: str, lhs: InspectionSummary, rhs: InspectionSummary, node: Node
                      ) -> InspectionSummary:
        # Docstring inherited from TreeVisitor.visitBinaryOp
        return lhs.merge(rhs)

    def visitIsIn(self, lhs: InspectionSummary, values: List[InspectionSummary], not_in: bool, node: Node
                  ) -> InspectionSummary:
        # Docstring inherited from TreeVisitor.visitIsIn
        for v in values:
            lhs.merge(v)
        return lhs

    def visitParens(self, expression: InspectionSummary, node: Node) -> InspectionSummary:
        # Docstring inherited from TreeVisitor.visitParens
        return expression

    def visitTupleNode(self, items: Tuple[InspectionSummary, ...], node: Node) -> InspectionSummary:
        # Docstring inherited from base class
        result = InspectionSummary()
        for i in items:
            result.merge(i)
        return result

    def visitRangeLiteral(self, start: int, stop: int, stride: Optional[int], node: Node
                          ) -> InspectionSummary:
        # Docstring inherited from TreeVisitor.visitRangeLiteral
        return InspectionSummary()

    def visitPointNode(self, ra: InspectionSummary, dec: InspectionSummary, node: Node) -> InspectionSummary:
        # Docstring inherited from base class
        return InspectionSummary()


class ClauseVisitor(TreeVisitor[sqlalchemy.sql.ColumnElement]):
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

    unaryOps = {"NOT": lambda x: sqlalchemy.sql.not_(x),
                "+": lambda x: +x,
                "-": lambda x: -x}
    """Mapping or unary operator names to corresponding functions"""

    binaryOps = {"OR": lambda x, y: sqlalchemy.sql.or_(x, y),
                 "AND": lambda x, y: sqlalchemy.sql.and_(x, y),
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
                 columns: QueryColumns, elements: NamedKeyDict[DimensionElement, sqlalchemy.sql.FromClause]):
        self.universe = universe
        self.columns = columns
        self.elements = elements
        self.hasIngestDate: bool = False

    def visitNumericLiteral(self, value: str, node: Node) -> sqlalchemy.sql.ColumnElement:
        # Docstring inherited from TreeVisitor.visitNumericLiteral
        # Convert string value into float or int
        coerced: Union[int, float]
        try:
            coerced = int(value)
        except ValueError:
            coerced = float(value)
        return sqlalchemy.sql.literal(coerced)

    def visitStringLiteral(self, value: str, node: Node) -> sqlalchemy.sql.ColumnElement:
        # Docstring inherited from TreeVisitor.visitStringLiteral
        return sqlalchemy.sql.literal(value)

    def visitTimeLiteral(self, value: astropy.time.Time, node: Node) -> sqlalchemy.sql.ColumnElement:
        # Docstring inherited from TreeVisitor.visitTimeLiteral
        return sqlalchemy.sql.literal(value, type_=AstropyTimeNsecTai)

    def visitIdentifier(self, name: str, node: Node) -> sqlalchemy.sql.ColumnElement:
        # Docstring inherited from TreeVisitor.visitIdentifier
        if categorizeIngestDateId(name):
            self.hasIngestDate = True
            assert self.columns.datasets is not None
            assert self.columns.datasets.ingestDate is not None, "dataset.ingest_date is not in the query"
            return _TimestampColumnElement(self.columns.datasets.ingestDate)
        element, column = categorizeElementId(self.universe, name)
        if column is not None:
            return self.elements[element].columns[column]
        else:
            assert isinstance(element, Dimension)
            return self.columns.getKeyColumn(element)

    def visitUnaryOp(self, operator: str, operand: sqlalchemy.sql.ColumnElement, node: Node
                     ) -> sqlalchemy.sql.ColumnElement:
        # Docstring inherited from TreeVisitor.visitUnaryOp
        func = self.unaryOps.get(operator)
        if func:
            return func(operand)
        else:
            raise ValueError(f"Unexpected unary operator `{operator}' in `{node}'.")

    def visitBinaryOp(self, operator: str, lhs: sqlalchemy.sql.ColumnElement,
                      rhs: sqlalchemy.sql.ColumnElement, node: Node) -> sqlalchemy.sql.ColumnElement:
        # Docstring inherited from TreeVisitor.visitBinaryOp
        func = self.binaryOps.get(operator)
        if func:
            return func(lhs, rhs)
        else:
            raise ValueError(f"Unexpected binary operator `{operator}' in `{node}'.")

    def visitIsIn(self, lhs: sqlalchemy.sql.ColumnElement, values: List[sqlalchemy.sql.ColumnElement],
                  not_in: bool, node: Node) -> sqlalchemy.sql.ColumnElement:
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
                    expr = sqlalchemy.sql.and_(expr, (lhs % stride) == (start % stride))
                clauses.append(expr)
            else:
                # add all values to literal list, stop is inclusive
                literals += [sqlalchemy.sql.literal(value) for value in range(start, stop+1, stride)]

        if literals:
            # add IN() in front of BETWEENs
            clauses.insert(0, lhs.in_(literals))

        expr = sqlalchemy.sql.or_(*clauses)
        if not_in:
            expr = sqlalchemy.sql.not_(expr)

        return expr

    def visitParens(self, expression: sqlalchemy.sql.ColumnElement, node: Node
                    ) -> sqlalchemy.sql.ColumnElement:
        # Docstring inherited from TreeVisitor.visitParens
        return expression.self_group()

    def visitTupleNode(self, items: Tuple[sqlalchemy.sql.ColumnElement, ...], node: Node
                       ) -> sqlalchemy.sql.ColumnElement:
        # Docstring inherited from base class
        return sqlalchemy.sql.expression.Tuple(*items)

    def visitRangeLiteral(self, start: int, stop: int, stride: Optional[int], node: Node
                          ) -> sqlalchemy.sql.ColumnElement:
        # Docstring inherited from TreeVisitor.visitRangeLiteral

        # Just return a triple and let parent clauses handle it,
        # stride can be None which means the same as 1.
        return (start, stop, stride or 1)

    def visitPointNode(self, ra: Any, dec: Any, node: Node) -> None:
        # Docstring inherited from base class

        # this is a placeholder for future extension, we enabled syntax but
        # do not support actual use just yet.
        raise NotImplementedError("POINT() function is not supported yet")

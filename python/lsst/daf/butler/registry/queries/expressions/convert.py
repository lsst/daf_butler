# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

__all__ = (
    "convertExpressionToSql",
    "ExpressionTypeError",
)

from abc import ABC, abstractmethod
import operator
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Tuple,
    Type,
    TYPE_CHECKING,
    TypeVar,
    Union,
)

from astropy.time import Time
import sqlalchemy
from sqlalchemy.ext.compiler import compiles

from ....core import (
    ddl,
    Dimension,
    DimensionElement,
    DimensionUniverse,
    NamedKeyMapping,
    Timespan,
    TimespanDatabaseRepresentation,
)
from ....core.utils import iterable
from .parser import Node, TreeVisitor
from .categorize import categorizeElementId, categorizeConstant, ExpressionConstant

if TYPE_CHECKING:
    from .._structs import QueryColumns


def convertExpressionToSql(
    tree: Node,
    universe: DimensionUniverse,
    columns: QueryColumns,
    elements: NamedKeyMapping[DimensionElement, sqlalchemy.sql.FromClause],
    bind: Mapping[str, Any],
    TimespanReprClass: Type[TimespanDatabaseRepresentation],
) -> sqlalchemy.sql.ColumnElement:
    """Convert a query expression tree into a SQLAlchemy expression object.

    Parameters
    ----------
    tree : `Node`
        Root node of the query expression tree.
    universe : `DimensionUniverse`
        All known dimensions.
    columns : `QueryColumns`
        Struct that organizes the special columns known to the query
        under construction.
    elements : `NamedKeyMapping`
        `DimensionElement` instances and their associated tables.
    bind : `Mapping`
        Mapping from string names to literal values that should be subsituted
        for those names when they appear (as identifiers) in the expression.
    TimespanReprClass : `type`; subclass of `TimespanDatabaseRepresentation`
        Class that encapsulates the representation of `Timespan` objects in
        the database.

    Returns
    -------
    sql : `sqlalchemy.sql.ColumnElement`
        A boolean SQLAlchemy column expression.

    Raises
    ------
    ExpressionTypeError
        Raised if the operands in a query expression operation are incompatible
        with the operator, or if the expression does not evaluate to a boolean.
    """
    visitor = WhereClauseConverterVisitor(universe, columns, elements, bind, TimespanReprClass)
    converter = tree.visit(visitor)
    return converter.finish(tree)


class ExpressionTypeError(TypeError):
    """Exception raised when the types in a query expression are not
    compatible with the operators or other syntax.
    """


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


class WhereClauseConverter(ABC):
    """Abstract base class for the objects used to transform a butler query
    expression tree into SQLAlchemy expression objects.

    WhereClauseConverter instances are created and consumed by
    `WhereClauseConverterVisitor`, which is in turn created and used only by
    the `convertExpressionToSql` function.
    """

    def finish(self, node: Node) -> sqlalchemy.sql.ColumnElement:
        """Finish converting this [boolean] expression, returning a SQLAlchemy
        expression object.

        Parameters
        ----------
        node : `Node`
            Original expression tree  nodethis converter represents; used only
            for error reporting.

        Returns
        -------
        sql : `sqlalchemy.sql.ColumnElement`
            A boolean SQLAlchemy column expression.

        Raises
        ------
        ExpressionTypeError
            Raised if this node does not represent a boolean expression.  The
            default implementation always raises this exception; subclasses
            that may actually represent a boolean should override.
        """
        raise ExpressionTypeError(f'Expression "{node}" has type {self.dtype}, not bool.')

    @property
    @abstractmethod
    def dtype(self) -> type:
        """The Python type of the expression tree node associated with this
        converter (`type`).

        This should be the exact type of any literal or bind object, and the
        type produced by SQLAlchemy's converter mechanism when returning rows
        from the database in the case of expressions that map to database
        entities or expressions.
        """
        raise NotImplementedError()

    @abstractmethod
    def categorizeForIn(
        self,
        literals: List[sqlalchemy.sql.ColumnElement],
        ranges: List[Tuple[int, int, int]],
        dtype: type,
        node: Node,
    ) -> None:
        """Visit this expression when it appears as an element in the
        right-hand side of an IN expression.

        Implementations must either:

         - append or extend to ``literals``
         - append or extend to ``ranges``
         - raise `ExpressionTypeError`.

        Parameters
        ----------
        literals : `list` [ `sqlalchemy.sql.ColumnElement` ]
            List of SQL expression objects that the left-hand side of the IN
            operation may match exactly.
        ranges : `list` of `tuple`
            List of (start, stop, step) tuples that represent ranges that the
            left-hand side of the IN operation may match.
        dtype : `type`
            Type of the left-hand side operand for the IN expression.  Literals
            should only be appended to if ``self.dtype is dtype``, and
            ``ranges`` should only be appended to if ``dtype is int``.
        node : `Node`
            Original expression tree node this converter represents; for use
            only in error reporting.

        Raises
        ------
        ExpressionTypeError
            Raised if this node can never appear on the right-hand side of an
            IN expression, or if it is incompatible with the left-hand side
            type.
        """
        raise NotImplementedError()


class ScalarWhereClauseConverter(WhereClauseConverter):
    """Primary implementation of WhereClauseConverter, for expressions that can
    always be represented directly by a single `sqlalchemy.sql.ColumnElement`
    instance.

    Should be constructed by calling either `fromExpression` or `fromLiteral`.

    Parameters
    ----------
    column : `sqlalchemy.sql.ColumnElement`
        A SQLAlchemy column expression.
    value
        The Python literal this expression was constructed from, or `None` if
        it was not constructed from a literal.  Note that this is also `None`
        this object corresponds to the literal `None`, in which case
        ``dtype is type(None)``.
    dtype : `type`
        Python type this expression maps to.
    """
    def __init__(self, column: sqlalchemy.sql.ColumnElement, value: Any, dtype: type):
        self.column = column
        self.value = value
        self._dtype = dtype

    @classmethod
    def fromExpression(cls, column: sqlalchemy.sql.ColumnElement, dtype: type) -> ScalarWhereClauseConverter:
        """Construct from an existing SQLAlchemy column expression and type.

        Parameters
        ----------
        column : `sqlalchemy.sql.ColumnElement`
            A SQLAlchemy column expression.
        dtype : `type`
            Python type this expression maps to.

        Returns
        -------
        converter : `ScalarWhereClauseConverter`
            Converter instance that wraps ``column``.
        """
        return cls(column, None, dtype)

    @classmethod
    def fromLiteral(cls, value: Any) -> ScalarWhereClauseConverter:
        """Construct from a Python literal.

        Parameters
        ----------
        value
            The Python literal to wrap.

        Returns
        -------
        converter : `ScalarWhereClauseConverter`
            Converter instance that wraps ``value``.
        """
        dtype = type(value)
        column = sqlalchemy.sql.literal(value, type_=ddl.AstropyTimeNsecTai if dtype is Time else None)
        return cls(column, value, dtype)

    def finish(self, node: Node) -> sqlalchemy.sql.ColumnElement:
        # Docstring inherited.
        if self.dtype is not bool:
            return super().finish(node)  # will raise; just avoids duplicate error message
        return self.column

    @property
    def dtype(self) -> type:
        # Docstring inherited.
        return self._dtype

    def categorizeForIn(
        self,
        literals: List[sqlalchemy.sql.ColumnElement],
        ranges: List[Tuple[int, int, int]],
        dtype: type,
        node: Node,
    ) -> None:
        # Docstring inherited.
        if dtype is not self.dtype:
            raise ExpressionTypeError(
                f'Error in IN expression "{node}": left hand side has type '
                f'{dtype.__name__}, but item has type {self.dtype.__name__}.'
            )
        literals.append(self.column)


class TimespanWhereClauseConverter(WhereClauseConverter):
    """Implementation of WhereClauseConverter for `Timespan` expressions.

    Parameters
    ----------
    timespan : `TimespanDatabaseRepresentation`
        Object that represents a logical timespan column or column expression
        (which may or may not be backed by multiple real columns).
    """
    def __init__(self, timespan: TimespanDatabaseRepresentation):
        self.timespan = timespan

    @classmethod
    def fromPair(
        cls,
        begin: ScalarWhereClauseConverter,
        end: ScalarWhereClauseConverter,
        TimespanReprClass: Type[TimespanDatabaseRepresentation],
    ) -> TimespanWhereClauseConverter:
        """Construct from a pair of literal expressions.

        Parameters
        ----------
        begin : `ScalarWhereClauseConverter`
            Converter object associated with an expression of type
            `astropy.time.Time` or `None` (for a timespan that is unbounded
            from below).
        end : `ScalarWhereClauseConverter`
            Converter object associated with an expression of type
            `astropy.time.Time` or `None` (for a timespan that is unbounded
            from above).
        TimespanReprClass : `type`; `TimespanDatabaseRepresentation` subclass
            Class that encapsulates the representation of `Timespan` objects in
            the database.

        Returns
        -------
        converter : `TimespanWhereClauseConverter`
            Converter instance that represents a `Timespan` literal.

        Raises
        ------
        ExpressionTypeError
            Raised if begin or end is a time column from the database or other
            time expression, not a literal or bind time value.
        """
        assert begin.dtype in (Time, type(None)), "Guaranteed by dispatch table rules."
        assert end.dtype in (Time, type(None)), "Guaranteed by dispatch table rules."
        if (begin.value is None and begin.dtype is Time) or (end.value is None and end.dtype is Time):
            raise ExpressionTypeError("Time pairs in expressions must be literals or bind values.")
        return cls(TimespanReprClass.fromLiteral(Timespan(begin.value, end.value)))

    @property
    def dtype(self) -> type:
        # Docstring inherited.
        return Timespan

    def overlaps(self, other: TimespanWhereClauseConverter) -> ScalarWhereClauseConverter:
        """Construct a boolean converter expression that represents the overlap
        of this timespan with another.

        Parameters
        ----------
        other : `TimespanWhereClauseConverter`
            RHS operand for the overlap operation.

        Returns
        -------
        overlaps : `ScalarWhereClauseConverter`
            Converter that wraps the boolean overlaps expression.
        """
        assert other.dtype is Timespan, "Guaranteed by dispatch table rules"
        return ScalarWhereClauseConverter.fromExpression(self.timespan.overlaps(other.timespan), bool)

    def contains(self, other: ScalarWhereClauseConverter) -> ScalarWhereClauseConverter:
        """Construct a boolean converter expression that represents whether
        this timespans contains a scalar time.

        Parameters
        ----------
        other : `ScalarWhereClauseConverter`
            RHS operand for the overlap operation.
        TimespanReprClass : `type`; `TimespanDatabaseRepresentation` subclass
            Ignored; provided for signature compatibility with `DispatchTable`.

        Returns
        -------
        overlaps : `ScalarWhereClauseConverter`
            Converter that wraps the boolean overlaps expression.
        """
        assert other.dtype is Time, "Guaranteed by dispatch table rules"
        return ScalarWhereClauseConverter.fromExpression(self.timespan.contains(other.column), bool)

    def categorizeForIn(
        self,
        literals: List[sqlalchemy.sql.ColumnElement],
        ranges: List[Tuple[int, int, int]],
        dtype: type,
        node: Node,
    ) -> None:
        # Docstring inherited.
        raise ExpressionTypeError(
            f'Invalid element on right side of IN expression "{node}": '
            'Timespans are not allowed in this context.'
        )


class RangeWhereClauseConverter(WhereClauseConverter):
    """Implementation of WhereClauseConverters for integer range literals.

    Range literals may only appear on the right-hand side of IN operations
    where the left-hand side operand is of type `int`.

    Parameters
    ----------
    start : `int`
        Starting point (inclusive) for the range.
    stop : `int`
        Stopping point (exclusive) for the range.
    step : `int`
        Step size for the range.
    """
    def __init__(self, start: int, stop: int, step: int):
        self.start = start
        self.stop = stop
        self.step = step

    @property
    def dtype(self) -> type:
        # Docstring inherited.
        return range

    def categorizeForIn(
        self,
        literals: List[sqlalchemy.sql.ColumnElement],
        ranges: List[Tuple[int, int, int]],
        dtype: type,
        node: Node,
    ) -> None:
        # Docstring inherited.
        if dtype is not int:
            raise ExpressionTypeError(
                f'Error in IN expression "{node}": range expressions '
                f'are only supported for int operands, not {dtype.__name__}.'
            )
        ranges.append((self.start, self.stop, self.step))


UnaryFunc = Callable[[WhereClauseConverter], WhereClauseConverter]
"""Signature of unary-operation callables directly stored in `DispatchTable`.
"""

BinaryFunc = Callable[[WhereClauseConverter, WhereClauseConverter], WhereClauseConverter]
"""Signature of binary-operation callables directly stored in `DispatchTable`.
"""

UnaryColumnFunc = Callable[[sqlalchemy.sql.ColumnElement], sqlalchemy.sql.ColumnElement]
"""Signature for unary-operation callables that can work directly on SQLAlchemy
column expressions.
"""

BinaryColumnFunc = Callable[[sqlalchemy.sql.ColumnElement, sqlalchemy.sql.ColumnElement],
                            sqlalchemy.sql.ColumnElement]
"""Signature for binary-operation callables that can work directly on
SQLAlchemy column expressions.
"""

_F = TypeVar("_F")


def adaptIdentity(func: _F, result: Optional[type]) -> _F:
    """An adapter function for `DispatchTable.registerUnary` and
    `DispatchTable.registerBinary` that just returns this original function.
    """
    return func


def adaptUnaryColumnFunc(func: UnaryColumnFunc, result: type) -> UnaryFunc:
    """An adapter function for `DispatchTable.registerUnary` that converts a
    `UnaryColumnFunc` into a `UnaryFunc`, requiring the operand to be a
    `ScalarWhereClauseConverter`.
    """
    def adapted(operand: WhereClauseConverter) -> WhereClauseConverter:
        assert isinstance(operand, ScalarWhereClauseConverter)
        return ScalarWhereClauseConverter.fromExpression(func(operand.column), dtype=result)
    return adapted


def adaptBinaryColumnFunc(func: BinaryColumnFunc, result: type) -> BinaryFunc:
    """An adapter function for `DispatchTable.registerBinary` that converts a
    `BinaryColumnFunc` into a `BinaryFunc`, requiring the operands to be
    `ScalarWhereClauseConverter` instances.
    """
    def adapted(lhs: WhereClauseConverter, rhs: WhereClauseConverter) -> WhereClauseConverter:
        assert isinstance(lhs, ScalarWhereClauseConverter)
        assert isinstance(rhs, ScalarWhereClauseConverter)
        return ScalarWhereClauseConverter.fromExpression(func(lhs.column, rhs.column), dtype=result)
    return adapted


class DispatchTable:
    """An object that manages unary- and binary-operator type-dispatch tables
    for `WhereClauseConverter`.

    Notes
    -----
    A lot of the machinery in this class (and in the preceding function
    signature type aliases) is all in service of making the actual dispatch
    rules in the `build` method concise and easy to read, because that's where
    all of the important high-level logic lives.

    Double-dispatch is a pain in Python, as it is in most languages; it's worth
    noting that I first tried the traditional visitor-pattern approach here,
    and it was *definitely* much harder to see the actual behavior.
    """
    def __init__(self) -> None:
        self._unary: Dict[Tuple[str, type], UnaryFunc] = {}
        self._binary: Dict[Tuple[str, type, type], BinaryFunc] = {}

    def registerUnary(
        self,
        operator: str,
        operand: Union[type, Iterable[type]],
        func: _F,
        *,
        result: Optional[type] = None,
        adapt: Any = True,
    ) -> None:
        """Register a unary operation for one or more types.

        Parameters
        ----------
        operator : `str`
            Operator as it appears in the string expression language.  Unary
            operations that are not mapped to operators may use their own
            arbitrary strings, as long as these are used consistently in
            `build` and `applyUnary`.
        operand : `type` or `Iterable` [ `type` ]
            Type or types for which this operation is implemented by the given
            ``func``.
        func : `Callable`
            Callable that implements the unary operation.  If
            ``adapt is True``, this should be a `UnaryColumnFunc`.  If
            ``adapt is False``, this should be a `UnaryFunc`.  Otherwise,
            this is whatever type is accepted as the first argument to
            ``adapt``.
        result : `type`, optional
            Type of the expression returned by this operation.  If not
            provided, the type of the operand is assumed.
        adapt : `bool` or `Callable`
            A callable that wraps ``func`` (the first argument) and ``result``
            (the second argument), returning a new callable with the
            signature of `UnaryFunc`.  `True` (default) and `False` invoke a
            default adapter or no adapter (see ``func`` docs).
        """
        if adapt is True:
            adapt = adaptUnaryColumnFunc
        elif adapt is False:
            adapt = adaptIdentity
        for item in iterable(operand):
            self._unary[operator, item] = adapt(
                func,
                result if result is not None else item
            )

    def registerBinary(
        self,
        operator: str,
        lhs: Union[type, Iterable[type]],
        func: _F,
        *,
        rhs: Optional[Union[type, Iterable[type]]] = None,
        result: Optional[type] = None,
        adapt: Any = True,
    ) -> None:
        """Register a binary operation for one or more types.

        Parameters
        ----------
        operator : `str`
            Operator as it appears in the string expression language.  Binary
            operations that are not mapped to operators may use their own
            arbitrary strings, as long as these are used consistently in
            `build` and `applyBinary`.
        lhs : `type` or `Iterable` [ `type` ]
            Left-hand side type or types for which this operation is
            implemented by the given ``func``.
        func : `Callable`
            Callable that implements the binary operation.  If
            ``adapt is True``, this should be a `BinaryColumnFunc`.  If
            ``adapt is False``, this should be a `BinaryFunc`.  Otherwise,
            this is whatever type is accepted as the first argument to
            ``adapt``.
        rhs : `type` or `Iterable` [ `type` ]
            Right-hand side type or types for which this operation is
            implemented by the given ``func``.  If multiple types, all
            combinations of ``lhs`` and ``rhs`` are registered.  If not
            provided, each element of ``lhs`` is assumed to be paired with
            itself, but mixed-type combinations are not registered.
        result : `type`, optional
            Type of the expression returned by this operation.  If not
            provided and ``rhs`` is also not provided, the type of the operand
            (``lhs``) is assumed.  If not provided and ``rhs`` *is* provided,
            then ``result=None`` will be forwarded to ``adapt``.
        adapt : `bool` or `Callable`
            A callable that wraps ``func`` (the first argument) and ``result``
            (the second argument), returning a new callable with the
            signature of `BinaryFunc`.  `True` (default) and `False` invoke a
            default adapter or no adapter (see ``func`` docs).
        """
        if adapt is True:
            adapt = adaptBinaryColumnFunc
        elif adapt is False:
            adapt = adaptIdentity
        for lh in iterable(lhs):
            if rhs is None:
                self._binary[operator, lh, lh] = adapt(func, result if result is not None else lh)
            else:
                for rh in iterable(rhs):
                    self._binary[operator, lh, rh] = adapt(func, result)

    def applyUnary(
        self,
        operator: str,
        operand: WhereClauseConverter,
    ) -> WhereClauseConverter:
        """Look up and apply the appropriate function for a registered unary
        operation.

        Parameters
        ----------
        operator : `str`
            Operator for the operation to apply.
        operand : `WhereClauseConverter`
            Operand, with ``operand.dtype`` and ``operator`` used to look up
            the appropriate function.

        Returns
        -------
        expression : `WhereClauseConverter`
            Converter instance that represents the operation, created by
            calling the registered function.

        Raises
        ------
        KeyError
            Raised if the operator and operand type combination is not
            recognized.
        """
        return self._unary[operator, operand.dtype](operand)

    def applyBinary(
        self,
        operator: str,
        lhs: WhereClauseConverter,
        rhs: WhereClauseConverter,
    ) -> WhereClauseConverter:
        """Look up and apply the appropriate function for a registered binary
        operation.

        Parameters
        ----------
        operator : `str`
            Operator for the operation to apply.
        lhs : `WhereClauseConverter`
            Left-hand side operand.
        rhs : `WhereClauseConverter`
            Right-hand side operand.

        Returns
        -------
        expression : `WhereClauseConverter`
            Converter instance that represents the operation, created by
            calling the registered function.

        Raises
        ------
        KeyError
            Raised if the operator and operand type combination is not
            recognized.
        """
        return self._binary[operator, lhs.dtype, rhs.dtype](lhs, rhs)

    @classmethod
    def build(cls, TimespanReprClass: Type[TimespanDatabaseRepresentation]) -> DispatchTable:
        table = DispatchTable()
        # Standard scalar unary and binary operators: just delegate to
        # SQLAlchemy operators.
        table.registerUnary("NOT", bool, sqlalchemy.sql.not_)
        table.registerUnary("+", (int, float), operator.__pos__)
        table.registerUnary("-", (int, float), operator.__neg__)
        table.registerBinary("AND", bool, sqlalchemy.sql.and_)
        table.registerBinary("OR", bool, sqlalchemy.sql.or_)
        table.registerBinary("=", (int, float, str, Time), operator.__eq__, result=bool)
        table.registerBinary("!=", (int, float, str, Time), operator.__ne__, result=bool)
        table.registerBinary("<", (int, float, str, Time), operator.__lt__, result=bool)
        table.registerBinary(">", (int, float, str, Time), operator.__gt__, result=bool)
        table.registerBinary("<=", (int, float, str, Time), operator.__le__, result=bool)
        table.registerBinary(">=", (int, float, str, Time), operator.__ge__, result=bool)
        table.registerBinary("+", (int, float), operator.__add__)
        table.registerBinary("-", (int, float), operator.__sub__)
        table.registerBinary("*", (int, float), operator.__mul__)
        table.registerBinary("/", (int, float), operator.__truediv__)
        table.registerBinary("%", (int, float), operator.__mod__)
        table.registerBinary(
            "=",
            lhs=(int, float, str, Time, type(None)),
            rhs=(type(None),),
            func=sqlalchemy.sql.expression.ColumnOperators.is_,
            result=bool,
        )
        table.registerBinary(
            "=",
            lhs=(type(None),),
            rhs=(int, float, str, Time, type(None)),
            func=sqlalchemy.sql.expression.ColumnOperators.is_,
            result=bool,
        )
        table.registerBinary(
            "!=",
            lhs=(int, float, str, Time, type(None)),
            rhs=(type(None),),
            func=sqlalchemy.sql.expression.ColumnOperators.is_not,
            result=bool,
        )
        table.registerBinary(
            "!=",
            lhs=(type(None),),
            rhs=(int, float, str, Time, type(None)),
            func=sqlalchemy.sql.expression.ColumnOperators.is_not,
            result=bool,
        )
        # Construct Timespan literals from 2-element tuples (A, B), where A and
        # B are each either Time or None.
        table.registerBinary(
            "PAIR",
            lhs=(Time, type(None)),
            rhs=(Time, type(None)),
            func=lambda lhs, rhs: TimespanWhereClauseConverter.fromPair(lhs, rhs, TimespanReprClass),
            adapt=False,
        )
        # Less-than and greater-than between Timespans.
        table.registerBinary(
            "<",
            lhs=Timespan,
            func=lambda a, b: ScalarWhereClauseConverter.fromExpression(a.timespan < b.timespan, dtype=bool),
            adapt=False,
        )
        table.registerBinary(
            ">",
            lhs=Timespan,
            func=lambda a, b: ScalarWhereClauseConverter.fromExpression(a.timespan > b.timespan, dtype=bool),
            adapt=False,
        )
        # Less-than and greater-than between Timespans and Times.
        table.registerBinary(
            "<",
            lhs=Timespan,
            rhs=Time,
            func=lambda a, b: ScalarWhereClauseConverter.fromExpression(a.timespan < b.column, dtype=bool),
            adapt=False,
        )
        table.registerBinary(
            ">",
            lhs=Timespan,
            rhs=Time,
            func=lambda a, b: ScalarWhereClauseConverter.fromExpression(a.timespan > b.column, dtype=bool),
            adapt=False,
        )
        table.registerBinary(
            "<",
            lhs=Time,
            rhs=Timespan,
            func=lambda a, b: ScalarWhereClauseConverter.fromExpression(b.timespan > a.column, dtype=bool),
            adapt=False,
        )
        table.registerBinary(
            ">",
            lhs=Time,
            rhs=Timespan,
            func=lambda a, b: ScalarWhereClauseConverter.fromExpression(b.timespan < a.column, dtype=bool),
            adapt=False,
        )
        # OVERLAPS operator between Timespans.
        table.registerBinary(
            "OVERLAPS",
            lhs=Timespan,
            func=TimespanWhereClauseConverter.overlaps,
            adapt=False,
        )
        # OVERLAPS operator between Timespans and Time is equivalent to
        # "contains", but expression language only has OVERLAPS to keep it
        # simple.
        table.registerBinary(
            "OVERLAPS",
            lhs=Timespan,
            rhs=Time,
            func=TimespanWhereClauseConverter.contains,
            adapt=False,
        )
        table.registerBinary(
            "OVERLAPS",
            lhs=Time,
            rhs=Timespan,
            func=lambda a, b: TimespanWhereClauseConverter.contains(b, a),
            adapt=False,
        )
        return table


class WhereClauseConverterVisitor(TreeVisitor[WhereClauseConverter]):
    """Implements TreeVisitor to convert the tree into
    `WhereClauseConverter` objects.

    This class should be used only by the `convertExpressionToSql` function;
    external code should just call that function.

    Parameters
    ----------
    universe : `DimensionUniverse`
        All known dimensions.
    columns: `QueryColumns`
        Struct that organizes the special columns known to the query
        under construction.
    elements: `NamedKeyMapping`
        `DimensionElement` instances and their associated tables.
    bind: `Mapping`
        Mapping from string names to literal values that should be subsituted
        for those names when they appear (as identifiers) in the expression.
    TimespanReprClass: `type`; subclass of `TimespanDatabaseRepresentation`
        Class that encapsulates the representation of `Timespan` objects in
        the database.
    """
    def __init__(
        self,
        universe: DimensionUniverse,
        columns: QueryColumns,
        elements: NamedKeyMapping[DimensionElement, sqlalchemy.sql.FromClause],
        bind: Mapping[str, Any],
        TimespanReprClass: Type[TimespanDatabaseRepresentation],
    ):
        self.universe = universe
        self.columns = columns
        self.elements = elements
        self.bind = bind
        self._TimespanReprClass = TimespanReprClass
        self._dispatch = DispatchTable.build(TimespanReprClass)

    def visitNumericLiteral(self, value: str, node: Node) -> WhereClauseConverter:
        # Docstring inherited from TreeVisitor.visitNumericLiteral
        # Convert string value into float or int
        coerced: Union[int, float]
        try:
            coerced = int(value)
        except ValueError:
            coerced = float(value)
        return ScalarWhereClauseConverter.fromLiteral(coerced)

    def visitStringLiteral(self, value: str, node: Node) -> WhereClauseConverter:
        # Docstring inherited from TreeVisitor.visitStringLiteral
        return ScalarWhereClauseConverter.fromLiteral(value)

    def visitTimeLiteral(self, value: Time, node: Node) -> WhereClauseConverter:
        # Docstring inherited from TreeVisitor.visitTimeLiteral
        return ScalarWhereClauseConverter.fromLiteral(value)

    def visitIdentifier(self, name: str, node: Node) -> WhereClauseConverter:
        # Docstring inherited from TreeVisitor.visitIdentifier
        if name in self.bind:
            value = self.bind[name]
            if isinstance(value, Timespan):
                return TimespanWhereClauseConverter(self._TimespanReprClass.fromLiteral(value))
            return ScalarWhereClauseConverter.fromLiteral(value)
        constant = categorizeConstant(name)
        if constant is ExpressionConstant.INGEST_DATE:
            assert self.columns.datasets is not None
            assert self.columns.datasets.ingestDate is not None, "dataset.ingest_date is not in the query"
            return ScalarWhereClauseConverter.fromExpression(
                _TimestampColumnElement(self.columns.datasets.ingestDate),
                Time,
            )
        elif constant is ExpressionConstant.NULL:
            return ScalarWhereClauseConverter.fromLiteral(None)
        assert constant is None, "Check for enum values should be exhaustive."
        element, column = categorizeElementId(self.universe, name)
        if column is not None:
            if column == TimespanDatabaseRepresentation.NAME:
                if element.temporal is None:
                    raise ExpressionTypeError(
                        f"No timespan column exists for non-temporal element '{element.name}'."
                    )
                return TimespanWhereClauseConverter(self.columns.timespans[element])
            else:
                if column not in element.RecordClass.fields.standard.names:
                    raise ExpressionTypeError(f"No column '{column}' in dimension table '{element.name}'.")
                return ScalarWhereClauseConverter.fromExpression(
                    self.elements[element].columns[column],
                    element.RecordClass.fields.standard[column].getPythonType(),
                )
        else:
            assert isinstance(element, Dimension)
            return ScalarWhereClauseConverter.fromExpression(
                self.columns.getKeyColumn(element),
                element.primaryKey.getPythonType()
            )

    def visitUnaryOp(self, operator: str, operand: WhereClauseConverter, node: Node) -> WhereClauseConverter:
        # Docstring inherited from TreeVisitor.visitUnaryOp
        try:
            return self._dispatch.applyUnary(operator, operand)
        except KeyError:
            raise ExpressionTypeError(
                f'Invalid operand of type {operand.dtype} for unary operator {operator} in "{node}".'
            ) from None

    def visitBinaryOp(
        self, operator: str, lhs: WhereClauseConverter, rhs: WhereClauseConverter, node: Node
    ) -> WhereClauseConverter:
        # Docstring inherited from TreeVisitor.visitBinaryOp
        try:
            return self._dispatch.applyBinary(operator, lhs, rhs)
        except KeyError:
            raise ExpressionTypeError(
                f'Invalid operand types ({lhs.dtype}, {rhs.dtype}) for binary '
                f'operator {operator} in "{node}".'
            ) from None

    def visitIsIn(
        self,
        lhs: WhereClauseConverter,
        values: List[WhereClauseConverter],
        not_in: bool,
        node: Node,
    ) -> WhereClauseConverter:
        if not isinstance(lhs, ScalarWhereClauseConverter):
            raise ExpressionTypeError(
                f'Invalid LHS operand of type {lhs.dtype} for IN operator in "{node}".'
            )
        # Docstring inherited from TreeVisitor.visitIsIn
        #
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
        #
        max_in_items = 1000
        clauses: List[sqlalchemy.sql.ColumnElement] = []
        # Split the list into literals and ranges
        literals: List[sqlalchemy.sql.ColumnElement] = []
        ranges: List[Tuple[int, int, int]] = []
        for value in values:
            value.categorizeForIn(literals, ranges, lhs.dtype, node)
        # Handle ranges (maybe by converting them to literals).
        for start, stop, stride in ranges:
            count = (stop - start + 1) // stride
            if len(literals) + count > max_in_items:
                # X BETWEEN START AND STOP
                #    AND MOD(X, STRIDE) = MOD(START, STRIDE)
                expr = lhs.column.between(start, stop)
                if stride != 1:
                    expr = sqlalchemy.sql.and_(expr, (lhs.column % stride) == (start % stride))
                clauses.append(expr)
            else:
                # add all values to literal list, stop is inclusive
                literals += [sqlalchemy.sql.literal(value) for value in range(start, stop + 1, stride)]
        # Handle literals.
        if literals:
            # add IN() in front of BETWEENs
            clauses.insert(0, lhs.column.in_(literals))
        # Assemble the full expression.
        expr = sqlalchemy.sql.or_(*clauses)
        if not_in:
            expr = sqlalchemy.sql.not_(expr)
        return ScalarWhereClauseConverter.fromExpression(expr, bool)

    def visitParens(self, expression: WhereClauseConverter, node: Node) -> WhereClauseConverter:
        # Docstring inherited from TreeVisitor.visitParens
        return expression

    def visitTupleNode(self, items: Tuple[WhereClauseConverter, ...], node: Node) -> WhereClauseConverter:
        # Docstring inherited from base class
        if len(items) != 2:
            raise ExpressionTypeError(f'Unrecognized {len(items)}-element tuple "{node}".')
        try:
            return self._dispatch.applyBinary("PAIR", items[0], items[1])
        except KeyError:
            raise ExpressionTypeError(
                f'Invalid type(s) ({items[0].dtype}, {items[1].dtype}) in timespan tuple "{node}" '
                '(Note that date/time strings must be preceded by "T" to be recognized).'
            )

    def visitRangeLiteral(
        self, start: int, stop: int, stride: Optional[int], node: Node
    ) -> WhereClauseConverter:
        # Docstring inherited from TreeVisitor.visitRangeLiteral
        # stride can be None which means the same as 1.
        return RangeWhereClauseConverter(start, stop, stride or 1)

    def visitPointNode(
        self, ra: WhereClauseConverter, dec: WhereClauseConverter, node: Node
    ) -> WhereClauseConverter:
        # Docstring inherited from base class

        # this is a placeholder for future extension, we enabled syntax but
        # do not support actual use just yet.
        raise NotImplementedError("POINT() function is not supported yet")

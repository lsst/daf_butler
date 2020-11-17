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
    Sequence,
    Set,
    Tuple,
    TYPE_CHECKING,
    Union,
)

import sqlalchemy
from sqlalchemy.ext.compiler import compiles

from ...core import (
    DataCoordinate,
    DimensionUniverse,
    Dimension,
    DimensionElement,
    DimensionGraph,
    GovernorDimension,
    NamedKeyDict,
    NamedValueSet,
)
from ...core.ddl import AstropyTimeNsecTai
from ..wildcards import EllipsisType, Ellipsis
from .exprParser import Node, NormalForm, NormalFormVisitor, TreeVisitor
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
                raise LookupError(f"Column '{column}' not found in table for {element}.")
            return element, column
    else:
        try:
            dimension = universe[table]
        except KeyError as err:
            raise LookupError(f"No dimension with name '{table}'.") from err
        return dimension, None


@dataclasses.dataclass
class InspectionSummary:
    """Base class for objects used by `CheckVisitor` and `InspectionVisitor`
    to gather information about a parsed expression.
    """

    def update(self, other: InspectionSummary) -> None:
        """Update ``self`` with all dimensions and columns from ``other``.

        Parameters
        ----------
        other : `InspectionSummary`
            The other summary object.
        """
        self.dimensions.update(other.dimensions)
        for element, columns in other.columns.items():
            self.columns.setdefault(element, set()).update(columns)
        self.hasIngestDate = self.hasIngestDate or other.hasIngestDate

    dimensions: NamedValueSet[Dimension] = dataclasses.field(default_factory=NamedValueSet)
    """Dimensions whose primary keys or dependencies were referenced anywhere
    in this branch (`NamedValueSet` [ `Dimension` ]).
    """

    columns: NamedKeyDict[DimensionElement, Set[str]] = dataclasses.field(default_factory=NamedKeyDict)
    """Dimension element tables whose columns were referenced anywhere in this
    branch (`NamedKeyDict` [ `DimensionElement`, `set` [ `str` ] ]).
    """

    hasIngestDate: bool = False
    """Whether this expression includes the special dataset ingest date
    identifier (`bool`).
    """


@dataclasses.dataclass
class TreeSummary(InspectionSummary):
    """Result object used by `InspectionVisitor` to gather information about
    a parsed expression.

    Notes
    -----
    TreeSummary adds attributes that allow dimension equivalence expressions
    (e.g. "tract=4") to be recognized when they appear in simple contexts
    (surrounded only by ANDs and ORs).  When `InspectionVisitor` is used on its
    own (i.e. when ``check=False`` in the query code), these don't do anything,
    but they don't cost much, either.  They are used by `CheckVisitor` when it
    delegates to `InspectionVisitor` to see what governor dimension values are
    set in a branch of the normal-form expression.
    """

    def merge(self, other: TreeSummary, isEq: bool = False) -> TreeSummary:
        """Merge ``other`` into ``self``, making ``self`` a summary of both
        expression tree branches.

        Parameters
        ----------
        other : `TreeSummary`
            The other summary object.
        isEq : `bool`, optional
            If `True` (`False` is default), these summaries are being combined
            via the equality operator.

        Returns
        -------
        self : `TreeSummary`
            The merged summary (updated in-place).
        """
        self.update(other)
        if isEq and self.isDataIdKeyOnly() and other.isDataIdValueOnly():
            self.dataIdValue = other.dataIdValue
        elif isEq and self.isDataIdValueOnly() and other.isDataIdKeyOnly():
            self.dataIdKey = other.dataIdKey
        else:
            self.dataIdKey = None
            self.dataIdValue = None
        return self

    def isDataIdKeyOnly(self) -> bool:
        """Test whether this branch is _just_ a data ID key identifier.
        """
        return self.dataIdKey is not None and self.dataIdValue is None

    def isDataIdValueOnly(self) -> bool:
        """Test whether this branch is _just_ a literal value that may be
        used as the value in a data ID key-value pair.
        """
        return self.dataIdKey is None and self.dataIdValue is not None

    dataIdKey: Optional[Dimension] = None
    """A `Dimension` that is (if `dataIdValue` is not `None`) or may be
    (if `dataIdValue` is `None`) fully identified by a literal value in this
    branch.
    """

    dataIdValue: Optional[str] = None
    """A literal value that constrains (if `dataIdKey` is not `None`) or may
    constrain (if `dataIdKey` is `None`) a dimension in this branch.

    This is always a `str` or `None`, but it may need to be coerced to `int`
    to reflect the actual user intent.
    """


class InspectionVisitor(TreeVisitor[TreeSummary]):
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

    def visitNumericLiteral(self, value: str, node: Node) -> TreeSummary:
        # Docstring inherited from TreeVisitor.visitNumericLiteral
        return TreeSummary(dataIdValue=value)

    def visitStringLiteral(self, value: str, node: Node) -> TreeSummary:
        # Docstring inherited from TreeVisitor.visitStringLiteral
        return TreeSummary(dataIdValue=value)

    def visitTimeLiteral(self, value: astropy.time.Time, node: Node) -> TreeSummary:
        # Docstring inherited from TreeVisitor.visitTimeLiteral
        return TreeSummary()

    def visitIdentifier(self, name: str, node: Node) -> TreeSummary:
        # Docstring inherited from TreeVisitor.visitIdentifier
        if categorizeIngestDateId(name):
            return TreeSummary(
                hasIngestDate=True,
            )
        element, column = categorizeElementId(self.universe, name)
        if column is None:
            assert isinstance(element, Dimension)
            return TreeSummary(
                dimensions=NamedValueSet(element.graph.dimensions),
                dataIdKey=element,
            )
        else:
            return TreeSummary(
                dimensions=NamedValueSet(element.graph.dimensions),
                columns=NamedKeyDict({element: {column}})
            )

    def visitUnaryOp(self, operator: str, operand: TreeSummary, node: Node
                     ) -> TreeSummary:
        # Docstring inherited from TreeVisitor.visitUnaryOp
        return operand

    def visitBinaryOp(self, operator: str, lhs: TreeSummary, rhs: TreeSummary,
                      node: Node) -> TreeSummary:
        # Docstring inherited from TreeVisitor.visitBinaryOp
        return lhs.merge(rhs, isEq=(operator == "="))

    def visitIsIn(self, lhs: TreeSummary, values: List[TreeSummary], not_in: bool,
                  node: Node) -> TreeSummary:
        # Docstring inherited from TreeVisitor.visitIsIn
        for v in values:
            lhs.merge(v)
        return lhs

    def visitParens(self, expression: TreeSummary, node: Node) -> TreeSummary:
        # Docstring inherited from TreeVisitor.visitParens
        return expression

    def visitTupleNode(self, items: Tuple[TreeSummary, ...], node: Node) -> TreeSummary:
        # Docstring inherited from base class
        result = TreeSummary()
        for i in items:
            result.merge(i)
        return result

    def visitRangeLiteral(self, start: int, stop: int, stride: Optional[int], node: Node
                          ) -> TreeSummary:
        # Docstring inherited from TreeVisitor.visitRangeLiteral
        return TreeSummary()

    def visitPointNode(self, ra: TreeSummary, dec: TreeSummary, node: Node) -> TreeSummary:
        # Docstring inherited from base class
        return TreeSummary()


@dataclasses.dataclass
class InnerSummary(InspectionSummary):
    """Result object used by `CheckVisitor` to gather referenced dimensions
    and tables from an inner group of AND'd together expression branches, and
    check them for consistency and completeness.
    """

    governors: NamedKeyDict[GovernorDimension, str] = dataclasses.field(default_factory=NamedKeyDict)
    """Mapping containing the values of all governor dimensions that are
    equated with literal values in this expression branch.
    """


@dataclasses.dataclass
class OuterSummary(InspectionSummary):
    """Result object used by `CheckVisitor` to gather referenced dimensions,
    tables, and governor dimension values from the entire expression.
    """

    governors: NamedKeyDict[GovernorDimension, Union[Set[str], EllipsisType]] \
        = dataclasses.field(default_factory=NamedKeyDict)
    """Mapping containing all values that appear in this expression for any
    governor dimension relevant to the query.

    Mapping values may be a `set` of `str` to indicate that only these values
    are permitted for a dimension, or ``...`` indicate that the values for
    that governor are not fully constrained by this expression.
    """


class CheckVisitor(NormalFormVisitor[TreeSummary, InnerSummary, OuterSummary]):
    """An implementation of `NormalFormVisitor` that identifies the dimensions
    and tables that need to be included in a query while performing some checks
    for completeness and consistency.

    Parameters
    ----------
    dataId : `DataCoordinate`
        Dimension values that are fully known in advance.
    graph : `DimensionGraph`
        The dimensions the query would include in the absence of this
        expression.
    """
    def __init__(self, dataId: DataCoordinate, graph: DimensionGraph):
        self.dataId = dataId
        self.graph = graph
        self._branchVisitor = InspectionVisitor(dataId.universe)

    def visitBranch(self, node: Node) -> TreeSummary:
        # Docstring inherited from NormalFormVisitor.
        return node.visit(self._branchVisitor)

    def visitInner(self, branches: Sequence[TreeSummary], form: NormalForm) -> InnerSummary:
        # Docstring inherited from NormalFormVisitor.
        # Disjunctive normal form means inner branches are AND'd together...
        assert form is NormalForm.DISJUNCTIVE
        # ...and that means each branch we iterate over together below
        # constrains the others, and they all need to be consistent.  Moreover,
        # because outer branches are OR'd together, we also know that if
        # something is missing from one of these branches (like a governor
        # dimension value like the instrument or skymap needed to interpret a
        # visit or tract number), it really is missing, because there's no way
        # some other inner branch can constraint it.
        #
        # That is, except the data ID the visitor was passed at construction;
        # that's AND'd to the entire expression later, and thus it affects all
        # branches.  To take care of that, we add any governor values it
        # contains to the summary in advance.
        summary = InnerSummary()
        summary.governors.update((k, self.dataId[k]) for k in self.dataId.graph.governors)  # type: ignore
        # Finally, we loop over those branches.
        for branch in branches:
            # Update the sets of dimensions and columns we've seen anywhere in
            # the expression in any context.
            summary.update(branch)
            # Test whether this branch has a form like '<dimension>=<value'
            # (or equivalent; categorizeIdentifier is smart enough to see that
            # e.g. 'detector.id=4' is equivalent to 'detector=4').
            # If so, and it's a governor dimension, remember that we've
            # constrained it on this branch, and make sure it's consistent
            # with any other constraints on any other branches its AND'd with.
            if isinstance(branch.dataIdKey, GovernorDimension) and branch.dataIdValue is not None:
                governor = branch.dataIdKey
                value = summary.governors.setdefault(governor, branch.dataIdValue)
                if value != branch.dataIdValue:
                    # Expression says something like "instrument='HSC' AND
                    # instrument='DECam'", or data ID has one and expression
                    # has the other.
                    if governor in self.dataId:
                        raise RuntimeError(
                            f"Conflict between expression containing {governor.name}={branch.dataIdValue!r} "
                            f"and data ID with {governor.name}={value!r}."
                        )
                    else:
                        raise RuntimeError(
                            f"Conflicting literal values for {governor.name} in expression: "
                            f"{value!r} != {branch.dataIdValue!r}."
                        )
        # Now that we know which governor values we've constrained, see if any
        # are missing, i.e. if the expression contains something like "visit=X"
        # without saying what instrument that visit corresponds to.  This rules
        # out a lot of accidents, but it also rules out possibly-legitimate
        # multi-instrument queries like "visit.seeing < 0.7".  But it's not
        # unreasonable to ask the user to be explicit about the instruments
        # they want to consider to work around this restriction, and that's
        # what we do.  Note that if someone does write an expression like
        #
        #  (instrument='HSC' OR instrument='DECam') AND visit.seeing < 0.7
        #
        # then in disjunctive normal form that will become
        #
        #  (instrument='HSC' AND visit.seeing < 0.7)
        #    OR (instrument='DECam' AND visit.seeing < 0.7)
        #
        # i.e. each instrument will get its own outer branch and the logic here
        # still works (that sort of thing is why we convert to normal form,
        # after all).
        governorsNeededInBranch: NamedValueSet[GovernorDimension] = NamedValueSet()
        for dimension in summary.dimensions:
            governorsNeededInBranch.update(dimension.graph.governors)
        if not governorsNeededInBranch.issubset(summary.governors.keys()):
            missing = NamedValueSet(governorsNeededInBranch - summary.governors.keys())
            raise RuntimeError(
                f"No value(s) for governor dimensions {missing} in expression that references dependent "
                "dimensions. 'Governor' dimensions must always be specified completely in either the "
                "query expression (via simple 'name=<value>' terms, not 'IN' terms) or in a data ID passed "
                "to the query method."
            )
        return summary

    def visitOuter(self, branches: Sequence[InnerSummary], form: NormalForm) -> OuterSummary:
        # Docstring inherited from NormalFormVisitor.
        # Disjunctive normal form means outer branches are OR'd together.
        assert form is NormalForm.DISJUNCTIVE
        # Iterate over branches in first pass to gather all dimensions and
        # columns referenced.  This aggregation is for the full query, so we
        # don't care whether things are joined by AND or OR (or + or -, etc).
        summary = OuterSummary()
        for branch in branches:
            summary.update(branch)
        # See if we've referenced any dimensions that weren't in the original
        # query graph; if so, we update that to include them.  This is what
        # lets a user say "tract=X" on the command line (well, "skymap=Y AND
        # tract=X" - logic in visitInner checks for that) when running a task
        # like ISR that has nothing to do with skymaps.
        if not summary.dimensions.issubset(self.graph.dimensions):
            self.graph = DimensionGraph(
                self.graph.universe,
                dimensions=(summary.dimensions | self.graph.dimensions),
            )
        # Set up a dict of empty sets, with all of the governors this query
        # involves as keys.
        summary.governors.update((k, set()) for k in self.graph.governors)
        # Iterate over branches again to see if there are any branches that
        # don't constraint a particular governor (because these branches are
        # OR'd together, that means there is no constraint on that governor at
        # all); if that's the case, we set the dict value to None.  If a
        # governor is constrained by all branches, we update the set with the
        # values that governor can have.
        for branch in branches:
            for governor in summary.governors:
                currentValues = summary.governors[governor]
                if currentValues is not Ellipsis:
                    branchValue = branch.governors.get(governor)
                    if branchValue is None:
                        # This governor is unconstrained in this branch, so
                        # no other branch can constrain it.
                        summary.governors[governor] = Ellipsis
                    else:
                        currentValues.add(branchValue)
        return summary


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

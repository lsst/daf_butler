# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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

"""Module which defines classes for intermediate representation of the
expression tree produced by parser.

The purpose of the intermediate representation is to be able to generate
same expression as a part of SQL statement with the minimal changes. We
will need to be able to replace identifiers in original expression with
database-specific identifiers but everything else will probably be sent
to database directly.
"""

from __future__ import annotations

__all__ = [
    "BinaryOp",
    "FunctionCall",
    "Identifier",
    "IsIn",
    "Node",
    "NumericLiteral",
    "Parens",
    "PointNode",
    "RangeLiteral",
    "StringLiteral",
    "TimeLiteral",
    "TupleNode",
    "UnaryOp",
    "function_call",
]

# -------------------------------
#  Imports of standard modules --
# -------------------------------
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

# -----------------------------
#  Imports for other modules --
# -----------------------------

# ----------------------------------
#  Local non-exported definitions --
# ----------------------------------

if TYPE_CHECKING:
    import astropy.time

    from .treeVisitor import TreeVisitor

# ------------------------
#  Exported definitions --
# ------------------------


class Node(ABC):
    """Base class of IR node in expression tree.

    The purpose of this class is to simplify visiting of the
    all nodes in a tree. It has a list of sub-nodes of this
    node so that visiting code can navigate whole tree without
    knowing exact types of each node.

    Parameters
    ----------
    children : tuple of :py:class:`Node`
        Possibly empty list of sub-nodes.
    """

    def __init__(self, children: tuple[Node, ...] | None = None):
        self.children = tuple(children or ())

    @abstractmethod
    def visit(self, visitor: TreeVisitor) -> Any:
        """Implement Visitor pattern for parsed tree.

        Parameters
        ----------
        visitor : `TreeVisitor`
            Instance of visitor type.
        """


class BinaryOp(Node):
    """Node representing binary operator.

    This class is used for representing all binary operators including
    arithmetic and boolean operations.

    Parameters
    ----------
    lhs : `Node`
        Left-hand side of the operation.
    op : `str`
        Operator name, e.g. '+', 'OR'.
    rhs : `Node`
        Right-hand side of the operation.
    """

    def __init__(self, lhs: Node, op: str, rhs: Node):
        Node.__init__(self, (lhs, rhs))
        self.lhs = lhs
        self.op = op
        self.rhs = rhs

    def visit(self, visitor: TreeVisitor) -> Any:
        # Docstring inherited from Node.visit
        lhs = self.lhs.visit(visitor)
        rhs = self.rhs.visit(visitor)
        return visitor.visitBinaryOp(self.op, lhs, rhs, self)

    def __str__(self) -> str:
        return "{lhs} {op} {rhs}".format(**vars(self))


class UnaryOp(Node):
    """Node representing unary operator.

    This class is used for representing all unary operators including
    arithmetic and boolean operations.

    Parameters
    ----------
    op : `str`
        Operator name, e.g. '+', 'NOT'.
    operand : `Node`
        Operand.
    """

    def __init__(self, op: str, operand: Node):
        Node.__init__(self, (operand,))
        self.op = op
        self.operand = operand

    def visit(self, visitor: TreeVisitor) -> Any:
        # Docstring inherited from Node.visit
        operand = self.operand.visit(visitor)
        return visitor.visitUnaryOp(self.op, operand, self)

    def __str__(self) -> str:
        return "{op} {operand}".format(**vars(self))


class StringLiteral(Node):
    """Node representing string literal.

    Parameters
    ----------
    value : `str`
        Literal value.
    """

    def __init__(self, value: str):
        Node.__init__(self)
        self.value = value

    def visit(self, visitor: TreeVisitor) -> Any:
        # Docstring inherited from Node.visit
        return visitor.visitStringLiteral(self.value, self)

    def __str__(self) -> str:
        return "'{value}'".format(**vars(self))


class TimeLiteral(Node):
    """Node representing time literal.

    Parameters
    ----------
    value : `astropy.time.Time`
        Literal string value.
    """

    def __init__(self, value: astropy.time.Time):
        Node.__init__(self)
        self.value = value

    def visit(self, visitor: TreeVisitor) -> Any:
        # Docstring inherited from Node.visit
        return visitor.visitTimeLiteral(self.value, self)

    def __str__(self) -> str:
        return "'{value}'".format(**vars(self))


class NumericLiteral(Node):
    """Node representing string literal.

    We do not convert literals to numbers, their text representation
    is stored literally.

    Parameters
    ----------
    value : str
        Literal value.
    """

    def __init__(self, value: str):
        Node.__init__(self)
        self.value = value

    def visit(self, visitor: TreeVisitor) -> Any:
        # Docstring inherited from Node.visit
        return visitor.visitNumericLiteral(self.value, self)

    def __str__(self) -> str:
        return "{value}".format(**vars(self))


class Identifier(Node):
    """Node representing identifier.

    Value of the identifier is its name, it may contain zero or one dot
    character.

    Parameters
    ----------
    name : str
        Identifier name.
    """

    def __init__(self, name: str):
        Node.__init__(self)
        self.name = name

    def visit(self, visitor: TreeVisitor) -> Any:
        # Docstring inherited from Node.visit
        return visitor.visitIdentifier(self.name, self)

    def __str__(self) -> str:
        return "{name}".format(**vars(self))


class RangeLiteral(Node):
    """Node representing range literal appearing in `IN` list.

    Range literal defines a range of integer numbers with start and
    end of the range (with inclusive end) and optional stride value
    (default is 1).

    Parameters
    ----------
    start : `int`
        Start value of a range.
    stop : `int`
        End value of a range, inclusive, same or higher than ``start``.
    stride : `int` or `None`, optional
        Stride value, must be positive, can be `None` which means that stride
        was not specified. Consumers are supposed to treat `None` the same way
        as stride=1 but for some consumers it may be useful to know that
        stride was missing from literal.
    """

    def __init__(self, start: int, stop: int, stride: int | None = None):
        self.start = start
        self.stop = stop
        self.stride = stride

    def visit(self, visitor: TreeVisitor) -> Any:
        # Docstring inherited from Node.visit
        return visitor.visitRangeLiteral(self.start, self.stop, self.stride, self)

    def __str__(self) -> str:
        res = f"{self.start}..{self.stop}" + (f":{self.stride}" if self.stride else "")
        return res


class IsIn(Node):
    """Node representing IN or NOT IN expression.

    Parameters
    ----------
    lhs : `Node`
        Left-hand side of the operation.
    values : `list` of `Node`
        List of values on the right side.
    not_in : `bool`
        If `True` then it is NOT IN expression, otherwise it is IN expression.
    """

    def __init__(self, lhs: Node, values: list[Node], not_in: bool = False):
        Node.__init__(self, (lhs,) + tuple(values))
        self.lhs = lhs
        self.values = values
        self.not_in = not_in

    def visit(self, visitor: TreeVisitor) -> Any:
        # Docstring inherited from Node.visit
        lhs = self.lhs.visit(visitor)
        values = [value.visit(visitor) for value in self.values]
        return visitor.visitIsIn(lhs, values, self.not_in, self)

    def __str__(self) -> str:
        values = ", ".join(str(x) for x in self.values)
        not_in = ""
        if self.not_in:
            not_in = "NOT "
        return f"{self.lhs} {not_in}IN ({values})"


class Parens(Node):
    """Node representing parenthesized expression.

    Parameters
    ----------
    expr : `Node`
        Expression inside parentheses.
    """

    def __init__(self, expr: Node):
        Node.__init__(self, (expr,))
        self.expr = expr

    def visit(self, visitor: TreeVisitor) -> Any:
        # Docstring inherited from Node.visit
        expr = self.expr.visit(visitor)
        return visitor.visitParens(expr, self)

    def __str__(self) -> str:
        return "({expr})".format(**vars(self))


class TupleNode(Node):
    """Node representing a tuple, sequence of parenthesized expressions.

    Tuple is used to represent time ranges, for now parser supports tuples
    with two items, though this class can be used to represent different
    number of items in sequence.

    Parameters
    ----------
    items : `tuple` of `Node`
        Expressions inside parentheses.
    """

    def __init__(self, items: tuple[Node, ...]):
        Node.__init__(self, items)
        self.items = items

    def visit(self, visitor: TreeVisitor) -> Any:
        # Docstring inherited from Node.visit
        items = tuple(item.visit(visitor) for item in self.items)
        return visitor.visitTupleNode(items, self)

    def __str__(self) -> str:
        items = ", ".join(str(item) for item in self.items)
        return f"({items})"


class FunctionCall(Node):
    """Node representing a function call.

    Parameters
    ----------
    function : `str`
        Name of the function.
    args : `list` [ `Node` ]
        Arguments passed to function.
    """

    def __init__(self, function: str, args: list[Node]):
        Node.__init__(self, tuple(args))
        self.name = function
        self.args = args[:]

    def visit(self, visitor: TreeVisitor) -> Any:
        # Docstring inherited from Node.visit
        args = [arg.visit(visitor) for arg in self.args]
        return visitor.visitFunctionCall(self.name, args, self)

    def __str__(self) -> str:
        args = ", ".join(str(arg) for arg in self.args)
        return f"{self.name}({args})"


class PointNode(Node):
    """Node representing a point, (ra, dec) pair.

    Parameters
    ----------
    ra : `Node`
        Node representing ra value.
    dec : `Node`
        Node representing dec value.
    """

    def __init__(self, ra: Node, dec: Node):
        Node.__init__(self, (ra, dec))
        self.ra = ra
        self.dec = dec

    def visit(self, visitor: TreeVisitor) -> Any:
        # Docstring inherited from Node.visit
        ra = self.ra.visit(visitor)
        dec = self.dec.visit(visitor)
        return visitor.visitPointNode(ra, dec, self)

    def __str__(self) -> str:
        return f"POINT({self.ra}, {self.dec})"


def function_call(function: str, args: list[Node]) -> Node:
    """Return node representing function calls.

    Parameters
    ----------
    function : `str`
        Name of the function.
    args : `list` [ `Node` ]
        Arguments passed to function.

    Notes
    -----
    Our parser supports arbitrary functions with arbitrary list of parameters.
    For now the main purpose of the syntax is to support POINT(ra, dec)
    construct, and to simplify implementation of visitors we define special
    type of node for that. This method makes `PointNode` instance for that
    special function call and generic `FunctionCall` instance for all other
    functions.
    """
    if function.upper() == "POINT":
        if len(args) != 2:
            raise ValueError("POINT requires two arguments (ra, dec)")
        return PointNode(*args)
    else:
        # generic function call
        return FunctionCall(function, args)

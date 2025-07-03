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
    "BoxNode",
    "CircleNode",
    "FunctionCall",
    "Identifier",
    "IsIn",
    "Node",
    "NumericLiteral",
    "Parens",
    "PointNode",
    "PolygonNode",
    "RangeLiteral",
    "RegionNode",
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
from uuid import UUID

# -----------------------------
#  Imports for other modules --
# -----------------------------

# ----------------------------------
#  Local non-exported definitions --
# ----------------------------------

if TYPE_CHECKING:
    import astropy.time

    from .treeVisitor import TreeVisitor


def _strip_parens(expression: Node) -> Node:
    """Strip all parentheses from an expression."""
    if isinstance(expression, Parens):
        return _strip_parens(expression.expr)
    return expression


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


class LiteralNode(Node):
    """Intermediate base class for nodes representing literals of any knid."""


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
        super().__init__((lhs, rhs))
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
        super().__init__((operand,))
        self.op = op
        self.operand = operand

    def visit(self, visitor: TreeVisitor) -> Any:
        # Docstring inherited from Node.visit
        operand = self.operand.visit(visitor)
        return visitor.visitUnaryOp(self.op, operand, self)

    def __str__(self) -> str:
        return "{op} {operand}".format(**vars(self))


class StringLiteral(LiteralNode):
    """Node representing string literal.

    Parameters
    ----------
    value : `str`
        Literal value.
    """

    def __init__(self, value: str):
        super().__init__()
        self.value = value

    def visit(self, visitor: TreeVisitor) -> Any:
        # Docstring inherited from Node.visit
        return visitor.visitStringLiteral(self.value, self)

    def __str__(self) -> str:
        return "'{value}'".format(**vars(self))


class TimeLiteral(LiteralNode):
    """Node representing time literal.

    Parameters
    ----------
    value : `astropy.time.Time`
        Literal string value.
    """

    def __init__(self, value: astropy.time.Time):
        super().__init__()
        self.value = value

    def visit(self, visitor: TreeVisitor) -> Any:
        # Docstring inherited from Node.visit
        return visitor.visitTimeLiteral(self.value, self)

    def __str__(self) -> str:
        return "T'{value}'".format(**vars(self))


class NumericLiteral(LiteralNode):
    """Node representing a numeric literal.

    We do not convert literals to numbers, their text representation
    is stored literally.

    Parameters
    ----------
    value : str
        Literal value.
    """

    def __init__(self, value: str):
        super().__init__()
        self.value = value

    def visit(self, visitor: TreeVisitor) -> Any:
        # Docstring inherited from Node.visit
        return visitor.visitNumericLiteral(self.value, self)

    def __str__(self) -> str:
        return "{value}".format(**vars(self))


class UuidLiteral(LiteralNode):
    """Node representing a UUID literal.

    Parameters
    ----------
    value : `UUID`
        Literal value.
    """

    def __init__(self, value: UUID):
        super().__init__()
        self.value = value

    def visit(self, visitor: TreeVisitor) -> Any:
        # Docstring inherited from Node.visit
        return visitor.visitUuidLiteral(self.value, self)

    def __str__(self) -> str:
        return f"UUID('{self.value}')"


class Identifier(Node):
    """Node representing identifier.

    Value of the identifier is its name, it may contain zero, one, or two dot
    characters.

    Parameters
    ----------
    name : str
        Identifier name.
    """

    def __init__(self, name: str):
        super().__init__()
        self.name = name

    def visit(self, visitor: TreeVisitor) -> Any:
        # Docstring inherited from Node.visit
        return visitor.visitIdentifier(self.name, self)

    def __str__(self) -> str:
        return "{name}".format(**vars(self))


class BindName(Node):
    """Node representing a bind name.

    Value of the bind is its name, which is a simple identifier.

    Parameters
    ----------
    name : str
        Bind name.
    """

    def __init__(self, name: str):
        super().__init__()
        self.name = name

    def visit(self, visitor: TreeVisitor) -> Any:
        # Docstring inherited from Node.visit
        return visitor.visitBind(self.name, self)

    def __str__(self) -> str:
        return ":{name}".format(**vars(self))


class RangeLiteral(LiteralNode):
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
        super().__init__()
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
        # All values must be literals or binds (and we allow simple identifiers
        # as binds).
        for node in values:
            node = _strip_parens(node)
            if not isinstance(node, LiteralNode | BindName | Identifier):
                raise TypeError(f"Unsupported type of expression in IN operator: {node}")
        super().__init__((lhs,) + tuple(values))
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
        super().__init__((expr,))
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
        super().__init__(items)
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
        super().__init__(tuple(args))
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
        super().__init__((ra, dec))
        self.ra = ra
        self.dec = dec

    def visit(self, visitor: TreeVisitor) -> Any:
        # Docstring inherited from Node.visit
        ra = self.ra.visit(visitor)
        dec = self.dec.visit(visitor)
        return visitor.visitPointNode(ra, dec, self)

    def __str__(self) -> str:
        return f"POINT({self.ra}, {self.dec})"


class CircleNode(Node):
    """Node representing a circle, (ra, dec, radius) pair.

    Parameters
    ----------
    ra : `Node`
        Node representing circle center ra value.
    dec : `Node`
        Node representing circle center dec value.
    radius : `Node`
        Node representing circle radius value.
    """

    def __init__(self, ra: Node, dec: Node, radius: Node):
        super().__init__((ra, dec, radius))
        self.ra = ra
        self.dec = dec
        self.radius = radius

    def visit(self, visitor: TreeVisitor) -> Any:
        # Docstring inherited from Node.visit
        ra = self.ra.visit(visitor)
        dec = self.dec.visit(visitor)
        radius = self.radius.visit(visitor)
        return visitor.visitCircleNode(ra, dec, radius, self)

    def __str__(self) -> str:
        return f"CIRCLE({self.ra}, {self.dec}, {self.radius})"


class BoxNode(Node):
    """Node representing box region in ADQL notation (ra, dec, width, height).

    Parameters
    ----------
    ra : `Node`
        Node representing box center ra value.
    dec : `Node`
        Node representing box center dec value.
    width : `Node`
        Node representing box ra width value.
    height : `Node`
        Node representing box dec height value.
    """

    def __init__(self, ra: Node, dec: Node, width: Node, height: Node):
        super().__init__((ra, dec, width, height))
        self.ra = ra
        self.dec = dec
        self.width = width
        self.height = height

    def visit(self, visitor: TreeVisitor) -> Any:
        # Docstring inherited from Node.visit
        ra = self.ra.visit(visitor)
        dec = self.dec.visit(visitor)
        width = self.width.visit(visitor)
        height = self.height.visit(visitor)
        return visitor.visitBoxNode(ra, dec, width, height, self)

    def __str__(self) -> str:
        return f"BOX({self.ra}, {self.dec}, {self.width}, {self.height})"


class PolygonNode(Node):
    """Node representing polygon region in ADQL notation.

    Parameters
    ----------
    vertices : `list`[`tuple`[`Node`, `Node`]]
        Node representing vertices of polygon.
    """

    def __init__(self, vertices: list[tuple[Node, Node]]):
        super().__init__(sum(vertices, start=()))
        self.vertices = vertices

    def visit(self, visitor: TreeVisitor) -> Any:
        # Docstring inherited from Node.visit
        vertices = [(ra.visit(visitor), dec.visit(visitor)) for ra, dec in self.vertices]
        return visitor.visitPolygonNode(vertices, self)

    def __str__(self) -> str:
        params = ", ".join(str(param) for param in self.children)
        return f"POLYGON({params})"


class RegionNode(Node):
    """Node representing region using IVOA SIAv2 POS notation.

    Parameters
    ----------
    pos : `Node`
        IVOA SIAv2 POS string representation of a region.
    """

    def __init__(self, pos: Node):
        super().__init__((pos,))
        self.pos = pos

    def visit(self, visitor: TreeVisitor) -> Any:
        # Docstring inherited from Node.visit
        pos = self.pos.visit(visitor)
        return visitor.visitRegionNode(pos, self)

    def __str__(self) -> str:
        return f"REGION({self.pos})"


class GlobNode(Node):
    """Node representing a call to GLOB(pattern, expression) function.

    Parameters
    ----------
    expression : `Node`
        Node representing expression matched against pattern, typically a
        column-like thing.
    pattern : `Node`
        Node representing a pattern, this must be either a `StringLiteral` or
        `BindName`.
    """

    def __init__(self, expression: Identifier, pattern: StringLiteral | BindName):
        super().__init__((expression, pattern))
        self.expression = expression
        self.pattern = pattern

    def visit(self, visitor: TreeVisitor) -> Any:
        # Docstring inherited from Node.visit
        expression = self.expression.visit(visitor)
        pattern = self.pattern.visit(visitor)
        return visitor.visitGlobNode(expression, pattern, self)

    def __str__(self) -> str:
        return f"GLOB({self.expression}, {self.pattern})"


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
    For now we only need a small set of functions, and to simplify
    implementation of visitors we define special type of node for each
    supported function. This method makes a special `Node` instance for those
    supported functions, and generic `FunctionCall` instance for all other
    functions. Tree visitors will most likely raise an error when visiting
    `FunctionCall` nodes.
    """
    function_name = function.upper()
    if function_name == "POINT":
        if len(args) != 2:
            raise ValueError("POINT requires two arguments (ra, dec)")
        return PointNode(*args)
    elif function_name == "CIRCLE":
        if len(args) != 3:
            raise ValueError("CIRCLE requires three arguments (ra, dec, radius)")
        # Check types of arguments, we want to support expressions too.
        for name, arg in zip(("ra", "dec", "radius"), args, strict=True):
            if not isinstance(arg, NumericLiteral | BindName | Identifier | BinaryOp | UnaryOp):
                raise ValueError(f"CIRCLE {name} argument must be either numeric expression or bind value.")
        return CircleNode(*args)
    elif function_name == "BOX":
        if len(args) != 4:
            raise ValueError("CIRCLE requires four arguments (ra, dec, width, height)")
        # Check types of arguments, we want to support expressions too.
        for name, arg in zip(("ra", "dec", "width", "height"), args, strict=True):
            if not isinstance(arg, NumericLiteral | BindName | Identifier | BinaryOp | UnaryOp):
                raise ValueError(f"BOX {name} argument must be either numeric expression or bind value.")
        return BoxNode(*args)
    elif function_name == "POLYGON":
        if len(args) % 2 != 0:
            raise ValueError("POLYGON requires even number of arguments")
        if len(args) < 6:
            raise ValueError("POLYGON requires at least three vertices")
        # Check types of arguments, we want to support expressions too.
        for arg in args:
            if not isinstance(arg, NumericLiteral | BindName | Identifier | BinaryOp | UnaryOp):
                raise ValueError("POLYGON argument must be either numeric expression or bind value.")
        vertices = list(zip(args[::2], args[1::2]))
        return PolygonNode(vertices)
    elif function_name == "REGION":
        if len(args) != 1:
            raise ValueError("REGION requires a single string argument")
        if not isinstance(args[0], StringLiteral | BindName | Identifier):
            raise ValueError("REGION argument must be either a string or a bind value")
        return RegionNode(args[0])
    elif function_name == "GLOB":
        if len(args) != 2:
            raise ValueError("GLOB requires two arguments (pattern, expression)")
        expression, pattern = (_strip_parens(arg) for arg in args)
        if not isinstance(expression, Identifier):
            raise TypeError("glob() first argument must be an identifier")
        if not isinstance(pattern, StringLiteral | BindName):
            raise TypeError("glob() second argument must be a string or a bind name (prefixed with colon)")
        return GlobNode(expression, pattern)
    elif function_name == "UUID":
        if len(args) != 1:
            raise ValueError("UUID() requires a single arguments (uuid-string)")
        argument = _strip_parens(args[0])
        # Potentially we could allow BindName as argument but it's too
        # complicated and people should use bind with UUID instead.
        if not isinstance(argument, StringLiteral):
            raise TypeError("UUID() argument must be a string literal")
        # This will raise ValueError if string is not good.
        uuid = UUID(argument.value)
        return UuidLiteral(uuid)
    else:
        # generic function call
        return FunctionCall(function, args)

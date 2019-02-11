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

"""Module which defines classes for intermediate representation of the
expression tree produced by parser.

The purpose of the intermediate representation is to be able to generate
same expression as a part of SQL statement with the minimal changes. We
will need to be able to replace identifiers in original expression with
database-specific identifiers but everything else will probably be sent
to database directly.
"""

__all__ = ['Node', 'BinaryOp', 'Identifier', 'IsIn', 'NumericLiteral',
           'Parens', 'StringLiteral', 'UnaryOp']

# -------------------------------
#  Imports of standard modules --
# -------------------------------
from abc import ABC, abstractmethod

# -----------------------------
#  Imports for other modules --
# -----------------------------

# ----------------------------------
#  Local non-exported definitions --
# ----------------------------------

# ------------------------
#  Exported definitions --
# ------------------------


class Node(ABC):
    """Base class of IR node in expression tree.

    The purpose of this class is to simplify visiting of the
    all nodes in a tree. It has a list of sub-nodes of this
    node so that visiting code can navigate whole tree without
    knowing exct types of each node.

    Attributes
    ----------
    children : tuple of :py:class:`Node`
        Possibly empty list of sub-nodes.
    """
    def __init__(self, children=None):
        self.children = tuple(children or ())

    @abstractmethod
    def visit(self, visitor):
        """Implement Visitor pattern for parsed tree.

        Parameters
        ----------
        visitor : `TreeVisitor`
            Instance of vistor type.
        """


class BinaryOp(Node):
    """Node representing binary operator.

    This class is used for representing all binary operators including
    arithmetic and boolean operations.

    Attributes
    ----------
    lhs : Node
        Left-hand side of the operation
    rhs : Node
        Right-hand side of the operation
    op : str
        Operator name, e.g. '+', 'OR'
    """
    def __init__(self, lhs, op, rhs):
        Node.__init__(self, (lhs, rhs))
        self.lhs = lhs
        self.op = op
        self.rhs = rhs

    def visit(self, visitor):
        # Docstring inherited from Node.visit
        lhs = self.lhs.visit(visitor)
        rhs = self.rhs.visit(visitor)
        return visitor.visitBinaryOp(self.op, lhs, rhs, self)

    def __str__(self):
        return "{lhs} {op} {rhs}".format(**vars(self))


class UnaryOp(Node):
    """Node representing unary operator.

    This class is used for representing all unary operators including
    arithmetic and boolean operations.

    Attributes
    ----------
    op : str
        Operator name, e.g. '+', 'NOT'
    operand : Node
        Operand.
    """
    def __init__(self, op, operand):
        Node.__init__(self, (operand,))
        self.op = op
        self.operand = operand

    def visit(self, visitor):
        # Docstring inherited from Node.visit
        operand = self.operand.visit(visitor)
        return visitor.visitUnaryOp(self.op, operand, self)

    def __str__(self):
        return "{op} {operand}".format(**vars(self))


class StringLiteral(Node):
    """Node representing string literal.

    Attributes
    ----------
    value : str
        Literal value.
    """
    def __init__(self, value):
        Node.__init__(self)
        self.value = value

    def visit(self, visitor):
        # Docstring inherited from Node.visit
        return visitor.visitStringLiteral(self.value, self)

    def __str__(self):
        return "'{value}'".format(**vars(self))


class NumericLiteral(Node):
    """Node representing string literal.

    We do not convert literals to numbers, their text representation
    is stored literally.

    Attributes
    ----------
    value : str
        Literal value.
    """
    def __init__(self, value):
        Node.__init__(self)
        self.value = value

    def visit(self, visitor):
        # Docstring inherited from Node.visit
        return visitor.visitNumericLiteral(self.value, self)

    def __str__(self):
        return "{value}".format(**vars(self))


class Identifier(Node):
    """Node representing identifier.

    Value of the identifier is its name, it may contain zero or one dot
    character.

    Attributes
    ----------
    name : str
        Identifier name.
    """
    def __init__(self, name):
        Node.__init__(self)
        self.name = name

    def visit(self, visitor):
        # Docstring inherited from Node.visit
        return visitor.visitIdentifier(self.name, self)

    def __str__(self):
        return "{name}".format(**vars(self))


class IsIn(Node):
    """Node representing IN or NOT IN expression.

    Attributes
    ----------
    lhs : Node
        Left-hand side of the operation
    values : list of Node
        List of values on the right side.
    not_in : bool
        If `True` then it is NOT IN expression, otherwise it is IN expression.
    """
    def __init__(self, lhs, values, not_in=False):
        Node.__init__(self, (lhs,) + tuple(values))
        self.lhs = lhs
        self.values = values
        self.not_in = not_in

    def visit(self, visitor):
        # Docstring inherited from Node.visit
        lhs = self.lhs.visit(visitor)
        values = [value.visit(visitor) for value in self.values]
        return visitor.visitIsIn(lhs, values, self.not_in, self)

    def __str__(self):
        values = ", ".join(str(x) for x in self.values)
        not_in = ""
        if self.not_in:
            not_in = "NOT "
        return "{lhs} {not_in}IN ({values})".format(lhs=self.lhs,
                                                    not_in=not_in,
                                                    values=values)


class Parens(Node):
    """Node representing parenthesized expression.

    Attributes
    ----------
    expr : Node
        Expression inside parentheses.
    """
    def __init__(self, expr):
        Node.__init__(self, (expr,))
        self.expr = expr

    def visit(self, visitor):
        # Docstring inherited from Node.visit
        expr = self.expr.visit(visitor)
        return visitor.visitParens(expr, self)

    def __str__(self):
        return "({expr})".format(**vars(self))

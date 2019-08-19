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

__all__ = ['TreeVisitor']

from abc import ABC, abstractmethod


class TreeVisitor(ABC):
    """Definition of interface for visitor classes.

    Visitors and tree node classes implement Visitor pattern for tree
    traversal. Typical use case is to generate different representation
    of the tree, e.g. transforming parsed tree into SQLAlchemy clause.

    All methods of the class can (and most likely should) return the
    "transformed" value of the visited node. This value will be returned
    from the `Node.visit` method and it will also be passed as an argument
    to other methods of the visitor.
    """
    @abstractmethod
    def visitNumericLiteral(self, value, node):
        """Visit NumericLiteral node.

        Parameters
        ----------
        value : `str`
            The value associated with the visited node, the value is string,
            exactly as it appears in the original expression. Depending on
            use case it may need to be converted to `int` or `float`.
        node : `Node`
            Corresponding tree node, mostly useful for diagnostics.
        """

    @abstractmethod
    def visitStringLiteral(self, value, node):
        """Visit StringLiteral node.

        Parameters
        ----------
        value : `str`
            The value associated with the visited node.
        node : `Node`
            Corresponding tree node, mostly useful for diagnostics.
        """

    @abstractmethod
    def visitRangeLiteral(self, start, stop, stride, node):
        """Visit RangeLiteral node.

        Parameters
        ----------
        start : `int`
            Range starting value.
        stop : `int`
            Range final value.
        stride : `int`
            Stride, can be `None` if not specified (should be treated same
            as 1).
        node : `Node`
            Corresponding tree node, mostly useful for diagnostics.
        """

    @abstractmethod
    def visitIdentifier(self, name, node):
        """Visit Identifier node.

        Parameters
        ----------
        name : `str`
            Identifier name.
        node : `Node`
            Corresponding tree node, mostly useful for diagnostics.
        """

    @abstractmethod
    def visitUnaryOp(self, operator, operand, node):
        """Visit UnaryOp node.

        Parameters
        ----------
        operator : `str`
            Operator name, e.g. "NOT" or "+".
        operand : `object`
            Operand, this object is returned by one of the methods of this
            class as a result of transformation of some other tree node.
        node : `Node`
            Corresponding tree node, mostly useful for diagnostics.
        """

    @abstractmethod
    def visitBinaryOp(self, operator, lhs, rhs, node):
        """Visit BinaryOp node.

        Parameters
        ----------
        operator : `str`
            Operator name, e.g. "NOT" or "+".
        lhs : `object`
            Left hand side operand, this object is returned by one of the
            methods of this class as a result of transformation of some other
            tree node.
        rhs : `object`
            Right hand side operand, this object is returned by one of the
            methods of this class as a result of transformation of some other
            tree node.
        node : `Node`
            Corresponding tree node, mostly useful for diagnostics.
        """

    @abstractmethod
    def visitIsIn(self, lhs, values, not_in, node):
        """Visit IsIn node.

        Parameters
        ----------
        lhs : `object`
            Left hand side operand, this object is returned by one of the
            methods of this class as a result of transformation of some other
            tree node.
        values : `list` of `object`
            Right hand side operand, list of objects returned by methods of
            this class as a result of transformation of some other tree nodes.
        not_in : `bool`
            `True` for "NOT IN" expression.
        node : `Node`
            Corresponding tree node, mostly useful for diagnostics.
        """

    @abstractmethod
    def visitParens(self, expression, node):
        """Visit Parens node.

        Parameters
        ----------
        expression : `object`
            Expression inside parentheses, this object is returned by one of
            the methods of this class as a result of transformation of some
            other tree node.
        node : `Node`
            Corresponding tree node, mostly useful for diagnostics.
        """

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

from __future__ import annotations

__all__ = ["TreeVisitor"]

from abc import abstractmethod
from typing import TYPE_CHECKING, Generic, TypeVar

if TYPE_CHECKING:
    import astropy.time

    from .exprTree import Node, PointNode, RangeLiteral


T = TypeVar("T")


class TreeVisitor(Generic[T]):
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
    def visitNumericLiteral(self, value: str, node: Node) -> T:
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
    def visitStringLiteral(self, value: str, node: Node) -> T:
        """Visit StringLiteral node.

        Parameters
        ----------
        value : `str`
            The value associated with the visited node.
        node : `Node`
            Corresponding tree node, mostly useful for diagnostics.
        """

    @abstractmethod
    def visitTimeLiteral(self, value: astropy.time.Time, node: Node) -> T:
        """Visit TimeLiteral node.

        Parameters
        ----------
        value : `astropy.time.Time`
            The value associated with the visited node.
        node : `Node`
            Corresponding tree node, mostly useful for diagnostics.
        """

    @abstractmethod
    def visitRangeLiteral(self, start: int, stop: int, stride: int | None, node: RangeLiteral) -> T:
        """Visit RangeLiteral node.

        Parameters
        ----------
        start : `int`
            Range starting value.
        stop : `int`
            Range final value.
        stride : `int` or `None`
            Stride, can be `None` if not specified (should be treated same
            as 1).
        node : `Node`
            Corresponding tree node, mostly useful for diagnostics.
        """

    @abstractmethod
    def visitIdentifier(self, name: str, node: Node) -> T:
        """Visit Identifier node.

        Parameters
        ----------
        name : `str`
            Identifier name.
        node : `Node`
            Corresponding tree node, mostly useful for diagnostics.
        """

    @abstractmethod
    def visitUnaryOp(self, operator: str, operand: T, node: Node) -> T:
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
    def visitBinaryOp(self, operator: str, lhs: T, rhs: T, node: Node) -> T:
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
    def visitIsIn(self, lhs: T, values: list[T], not_in: bool, node: Node) -> T:
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
    def visitParens(self, expression: T, node: Node) -> T:
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

    @abstractmethod
    def visitTupleNode(self, items: tuple[T, ...], node: Node) -> T:
        """Visit TupleNode node.

        Parameters
        ----------
        items : `tuple` of `object`
            Expressions inside parentheses, tuple of objects returned by one
            of the methods of this class as a result of transformation of
            tuple items.
        node : `Node`
            Corresponding tree node, mostly useful for diagnostics.
        """

    def visitFunctionCall(self, name: str, args: list[T], node: Node) -> T:
        """Visit FunctionCall node.

        Parameters
        ----------
        name : `str`
            Name of the function.
        args : `list` of `object`
            Arguments to function, list of objects returned by methods of
            this class as a result of transformation of function arguments.
        node : `Node`
            Corresponding tree node, mostly useful for diagnostics.

        Notes
        -----
        For now we only have to support one specific function ``POINT()``
        and for that function we define special node type `PointNode`.
        `FunctionCall` node type represents a generic function and regular
        visitors do not handle generic function. This non-abstract method
        is a common implementation for those visitors which raises an
        exception.
        """
        raise ValueError(f"Unknown function '{name}' in expression")

    @abstractmethod
    def visitPointNode(self, ra: T, dec: T, node: PointNode) -> T:
        """Visit PointNode node.

        Parameters
        ----------
        ra, dec : `object`
            Representation of 'ra' and 'dec' values, objects returned by
            methods of this class as a result of transformation of function
            arguments.
        node : `Node`
            Corresponding tree node, mostly useful for diagnostics.
        """

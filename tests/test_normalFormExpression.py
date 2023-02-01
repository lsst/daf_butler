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


import random
import unittest
from typing import List, Optional, Union

import astropy.time
from lsst.daf.butler.registry.queries.expressions.normalForm import (
    NormalForm,
    NormalFormExpression,
    TransformationVisitor,
)
from lsst.daf.butler.registry.queries.expressions.parser import Node, ParserYacc, TreeVisitor


class BooleanEvaluationTreeVisitor(TreeVisitor[bool]):
    """A `TreeVisitor` implementation that evaluates boolean expressions given
    boolean values for identifiers.
    """

    def __init__(self, **kwargs: bool):
        self.values = kwargs

    def init(self, form: NormalForm) -> None:
        self.form = form

    def visitNumericLiteral(self, value: str, node: Node) -> bool:
        # Docstring inherited from TreeVisitor.visitNumericLiteral
        raise NotImplementedError()

    def visitStringLiteral(self, value: str, node: Node) -> bool:
        # Docstring inherited from TreeVisitor.visitStringLiteral
        raise NotImplementedError()

    def visitTimeLiteral(self, value: astropy.time.Time, node: Node) -> bool:
        # Docstring inherited from TreeVisitor.visitTimeLiteral
        raise NotImplementedError()

    def visitIdentifier(self, name: str, node: Node) -> bool:
        # Docstring inherited from TreeVisitor.visitIdentifier
        return self.values[name]

    def visitUnaryOp(
        self,
        operator: str,
        operand: bool,
        node: Node,
    ) -> bool:
        # Docstring inherited from TreeVisitor.visitUnaryOp
        if operator == "NOT":
            return not operand
        raise NotImplementedError()

    def visitBinaryOp(
        self,
        operator: str,
        lhs: bool,
        rhs: bool,
        node: Node,
    ) -> bool:
        # Docstring inherited from TreeVisitor.visitBinaryOp
        if operator == "AND":
            return lhs and rhs
        if operator == "OR":
            return lhs or rhs
        raise NotImplementedError()

    def visitIsIn(
        self,
        lhs: bool,
        values: List[bool],
        not_in: bool,
        node: Node,
    ) -> bool:
        # Docstring inherited from TreeVisitor.visitIsIn
        raise NotImplementedError()

    def visitParens(self, expression: bool, node: Node) -> bool:
        # Docstring inherited from TreeVisitor.visitParens
        return expression

    def visitRangeLiteral(self, start: int, stop: int, stride: Optional[int], node: Node) -> bool:
        # Docstring inherited from TreeVisitor.visitRangeLiteral
        raise NotImplementedError()


class NormalFormExpressionTestCase(unittest.TestCase):
    """Tests for `NormalFormExpression` and its helper classes."""

    def check(
        self,
        expression: str,
        conjunctive: Union[str, bool] = False,
        disjunctive: Union[str, bool] = False,
    ) -> None:
        """Compare the results of transforming an expression to normal form
        against given expected values.

        Parameters
        ----------
        expression : `str`
            Expression to parse and transform.
        conjunctive : `str` or `bool`
            The expected string form of the expression after transformation
            to conjunctive normal form, or `True` to indicate that
            ``expression`` should already be in that form.  `False` to skip
            tests on this form.
        disjunctive : `str` or `bool`
            Same as ``conjunctive``, but for disjunctive normal form.
        """
        with self.subTest(expression):
            parser = ParserYacc()
            originalTree = parser.parse(expression)
            wrapper = originalTree.visit(TransformationVisitor())
            trees = {form: NormalFormExpression.fromTree(originalTree, form).toTree() for form in NormalForm}
            if conjunctive is True:  # expected to be conjunctive already
                self.assertTrue(wrapper.satisfies(NormalForm.CONJUNCTIVE), msg=str(wrapper))
            elif conjunctive:  # str to expect after normalization to conjunctive
                self.assertEqual(str(trees[NormalForm.CONJUNCTIVE]), conjunctive)
            if disjunctive is True:  # expected to be disjunctive already
                self.assertTrue(wrapper.satisfies(NormalForm.DISJUNCTIVE), msg=str(wrapper))
            elif disjunctive:  # str to expect after normalization to disjunctive
                self.assertEqual(str(trees[NormalForm.DISJUNCTIVE]), disjunctive)
            for i in range(10):
                values = {k: bool(random.randint(0, 1)) for k in "ABCDEF"}
                visitor = BooleanEvaluationTreeVisitor(**values)
                expected = originalTree.visit(visitor)
                for name, tree in trees.items():
                    self.assertEqual(expected, tree.visit(visitor), msg=name)

    def testNormalize(self):
        self.check("A AND B", conjunctive=True, disjunctive=True)
        self.check("A OR B", conjunctive=True, disjunctive=True)
        self.check("NOT (A OR B)", conjunctive="NOT A AND NOT B", disjunctive="NOT A AND NOT B")
        self.check("NOT (A AND B)", conjunctive="NOT A OR NOT B", disjunctive="NOT A OR NOT B")
        self.check(
            "NOT (A AND (NOT B OR C))",
            conjunctive="(NOT A OR B) AND (NOT A OR NOT C)",
            disjunctive=True,
        )
        self.check(
            "NOT (A OR (B AND NOT C))",
            conjunctive=True,
            disjunctive="(NOT A AND NOT B) OR (NOT A AND C)",
        )
        self.check(
            "A AND (B OR C OR D)",
            conjunctive=True,
            disjunctive="(A AND B) OR (A AND C) OR (A AND D)",
        )
        self.check(
            "A OR (B AND C AND D)",
            disjunctive=True,
            conjunctive="(A OR B) AND (A OR C) AND (A OR D)",
        )
        self.check(
            "A AND (B OR NOT C) AND (NOT D OR E OR F)",
            conjunctive=True,
            disjunctive=(
                "(A AND B AND NOT D) OR (A AND B AND E) OR (A AND B AND F) "
                "OR (A AND NOT C AND NOT D) OR (A AND NOT C AND E) OR (A AND NOT C AND F)"
            ),
        )
        self.check(
            "A OR (B AND NOT C) OR (NOT D AND E AND F)",
            conjunctive=(
                "(A OR B OR NOT D) AND (A OR B OR E) AND (A OR B OR F) "
                "AND (A OR NOT C OR NOT D) AND (A OR NOT C OR E) AND (A OR NOT C OR F)"
            ),
            disjunctive=True,
        )
        self.check(
            "(A OR (B AND NOT (C OR (NOT D AND E)))) OR F",
            conjunctive="(A OR B OR F) AND (A OR NOT C OR F) AND (A OR D OR NOT E OR F)",
            disjunctive="A OR (B AND NOT C AND D) OR (B AND NOT C AND NOT E) OR F",
        )


if __name__ == "__main__":
    unittest.main()

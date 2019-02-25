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

"""Simple unit test for expr_parser/parserYacc module.
"""

import unittest

from lsst.daf.butler.exprParser import exprTree, TreeVisitor, ParserYacc, ParseError


class _Visitor(TreeVisitor):
    """Trivial implementation of TreeVisitor.
    """
    def visitNumericLiteral(self, value, node):
        return f"N({value})"

    def visitStringLiteral(self, value, node):
        return f"S({value})"

    def visitIdentifier(self, name, node):
        return f"ID({name})"

    def visitUnaryOp(self, operator, operand, node):
        return f"U({operator} {operand})"

    def visitBinaryOp(self, operator, lhs, rhs, node):
        return f"B({lhs} {operator} {rhs})"

    def visitIsIn(self, lhs, values, not_in, node):
        values = ", ".join([str(val) for val in values])
        if not_in:
            return f"!IN({lhs} ({values}))"
        else:
            return f"IN({lhs} ({values}))"

    def visitParens(self, expression, node):
        return f"P({expression})"


class ParserLexTestCase(unittest.TestCase):
    """A test case for ParserYacc
    """

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testInstantiate(self):
        """Tests for making ParserLex instances
        """
        parser = ParserYacc()  # noqa: F841

    def testEmpty(self):
        """Tests for empty expression
        """
        parser = ParserYacc()

        # empty expression is allowed, returns None
        tree = parser.parse("")
        self.assertIsNone(tree)

    def testParseLiteral(self):
        """Tests for literals (strings/numbers)
        """
        parser = ParserYacc()

        tree = parser.parse('1')
        self.assertIsInstance(tree, exprTree.NumericLiteral)
        self.assertEqual(tree.value, '1')

        tree = parser.parse('.5e-2')
        self.assertIsInstance(tree, exprTree.NumericLiteral)
        self.assertEqual(tree.value, '.5e-2')

        tree = parser.parse("'string'")
        self.assertIsInstance(tree, exprTree.StringLiteral)
        self.assertEqual(tree.value, 'string')

    def testParseIdentifiers(self):
        """Tests for identifiers
        """
        parser = ParserYacc()

        tree = parser.parse('a')
        self.assertIsInstance(tree, exprTree.Identifier)
        self.assertEqual(tree.name, 'a')

        tree = parser.parse('a.b')
        self.assertIsInstance(tree, exprTree.Identifier)
        self.assertEqual(tree.name, 'a.b')

    def testParseParens(self):
        """Tests for identifiers
        """
        parser = ParserYacc()

        tree = parser.parse('(a)')
        self.assertIsInstance(tree, exprTree.Parens)
        self.assertIsInstance(tree.expr, exprTree.Identifier)
        self.assertEqual(tree.expr.name, 'a')

    def testUnaryOps(self):
        """Tests for unary plus and minus
        """
        parser = ParserYacc()

        tree = parser.parse('+a')
        self.assertIsInstance(tree, exprTree.UnaryOp)
        self.assertEqual(tree.op, '+')
        self.assertIsInstance(tree.operand, exprTree.Identifier)
        self.assertEqual(tree.operand.name, 'a')

        tree = parser.parse('- x.y')
        self.assertIsInstance(tree, exprTree.UnaryOp)
        self.assertEqual(tree.op, '-')
        self.assertIsInstance(tree.operand, exprTree.Identifier)
        self.assertEqual(tree.operand.name, 'x.y')

    def testBinaryOps(self):
        """Tests for binary operators
        """
        parser = ParserYacc()

        tree = parser.parse('a + b')
        self.assertIsInstance(tree, exprTree.BinaryOp)
        self.assertEqual(tree.op, '+')
        self.assertIsInstance(tree.lhs, exprTree.Identifier)
        self.assertIsInstance(tree.rhs, exprTree.Identifier)
        self.assertEqual(tree.lhs.name, 'a')
        self.assertEqual(tree.rhs.name, 'b')

        tree = parser.parse('a - 2')
        self.assertIsInstance(tree, exprTree.BinaryOp)
        self.assertEqual(tree.op, '-')
        self.assertIsInstance(tree.lhs, exprTree.Identifier)
        self.assertIsInstance(tree.rhs, exprTree.NumericLiteral)
        self.assertEqual(tree.lhs.name, 'a')
        self.assertEqual(tree.rhs.value, '2')

        tree = parser.parse('2 * 2')
        self.assertIsInstance(tree, exprTree.BinaryOp)
        self.assertEqual(tree.op, '*')
        self.assertIsInstance(tree.lhs, exprTree.NumericLiteral)
        self.assertIsInstance(tree.rhs, exprTree.NumericLiteral)
        self.assertEqual(tree.lhs.value, '2')
        self.assertEqual(tree.rhs.value, '2')

        tree = parser.parse('1.e5/2')
        self.assertIsInstance(tree, exprTree.BinaryOp)
        self.assertEqual(tree.op, '/')
        self.assertIsInstance(tree.lhs, exprTree.NumericLiteral)
        self.assertIsInstance(tree.rhs, exprTree.NumericLiteral)
        self.assertEqual(tree.lhs.value, '1.e5')
        self.assertEqual(tree.rhs.value, '2')

        tree = parser.parse('333%76')
        self.assertIsInstance(tree, exprTree.BinaryOp)
        self.assertEqual(tree.op, '%')
        self.assertIsInstance(tree.lhs, exprTree.NumericLiteral)
        self.assertIsInstance(tree.rhs, exprTree.NumericLiteral)
        self.assertEqual(tree.lhs.value, '333')
        self.assertEqual(tree.rhs.value, '76')

    def testIsIn(self):
        """Tests for IN
        """
        parser = ParserYacc()

        tree = parser.parse("a in (1,2,'X')")
        self.assertIsInstance(tree, exprTree.IsIn)
        self.assertFalse(tree.not_in)
        self.assertIsInstance(tree.lhs, exprTree.Identifier)
        self.assertEqual(tree.lhs.name, 'a')
        self.assertIsInstance(tree.values, list)
        self.assertEqual(len(tree.values), 3)
        self.assertIsInstance(tree.values[0], exprTree.NumericLiteral)
        self.assertEqual(tree.values[0].value, '1')
        self.assertIsInstance(tree.values[1], exprTree.NumericLiteral)
        self.assertEqual(tree.values[1].value, '2')
        self.assertIsInstance(tree.values[2], exprTree.StringLiteral)
        self.assertEqual(tree.values[2].value, 'X')

        tree = parser.parse("10 not in (1000)")
        self.assertIsInstance(tree, exprTree.IsIn)
        self.assertTrue(tree.not_in)
        self.assertIsInstance(tree.lhs, exprTree.NumericLiteral)
        self.assertEqual(tree.lhs.value, '10')
        self.assertIsInstance(tree.values, list)
        self.assertEqual(len(tree.values), 1)
        self.assertIsInstance(tree.values[0], exprTree.NumericLiteral)
        self.assertEqual(tree.values[0].value, '1000')

    def testCompareOps(self):
        """Tests for comparison operators
        """
        parser = ParserYacc()

        for op in ('=', '!=', '<', '<=', '>', '>='):
            tree = parser.parse('a {} 10'.format(op))
            self.assertIsInstance(tree, exprTree.BinaryOp)
            self.assertEqual(tree.op, op)
            self.assertIsInstance(tree.lhs, exprTree.Identifier)
            self.assertIsInstance(tree.rhs, exprTree.NumericLiteral)
            self.assertEqual(tree.lhs.name, 'a')
            self.assertEqual(tree.rhs.value, '10')

    def testBoolOps(self):
        """Tests for boolean operators
        """
        parser = ParserYacc()

        for op in ('OR', 'AND'):
            tree = parser.parse('a {} b'.format(op))
            self.assertIsInstance(tree, exprTree.BinaryOp)
            self.assertEqual(tree.op, op)
            self.assertIsInstance(tree.lhs, exprTree.Identifier)
            self.assertIsInstance(tree.rhs, exprTree.Identifier)
            self.assertEqual(tree.lhs.name, 'a')
            self.assertEqual(tree.rhs.name, 'b')

        tree = parser.parse('NOT b')
        self.assertIsInstance(tree, exprTree.UnaryOp)
        self.assertEqual(tree.op, 'NOT')
        self.assertIsInstance(tree.operand, exprTree.Identifier)
        self.assertEqual(tree.operand.name, 'b')

    def testExpression(self):
        """Test for more or less complete expression"""
        parser = ParserYacc()

        expression = ("((instrument='HSC' AND detector != 9) OR instrument='CFHT') "
                      "AND tract=8766 AND patch.cell_x > 5 AND "
                      "patch.cell_y < 4 AND abstract_filter='i'")

        tree = parser.parse(expression)
        self.assertIsInstance(tree, exprTree.BinaryOp)
        self.assertEqual(tree.op, 'AND')
        self.assertIsInstance(tree.lhs, exprTree.BinaryOp)
        # AND is left-associative, so rhs operand will be the
        # last sub-expressions
        self.assertIsInstance(tree.rhs, exprTree.BinaryOp)
        self.assertEqual(tree.rhs.op, '=')
        self.assertIsInstance(tree.rhs.lhs, exprTree.Identifier)
        self.assertEqual(tree.rhs.lhs.name, 'abstract_filter')
        self.assertIsInstance(tree.rhs.rhs, exprTree.StringLiteral)
        self.assertEqual(tree.rhs.rhs.value, 'i')

    def testException(self):
        """Test for exceptional cases"""

        def _assertExc(exc, expr, token, pos, lineno, posInLine):
            """Check exception attribute values"""
            self.assertEqual(exc.expression, expr)
            self.assertEqual(exc.token, token)
            self.assertEqual(exc.pos, pos)
            self.assertEqual(exc.lineno, lineno)
            self.assertEqual(exc.posInLine, posInLine)

        parser = ParserYacc()

        expression = "(1, 2, 3)"
        with self.assertRaises(ParseError) as catcher:
            parser.parse(expression)
        _assertExc(catcher.exception, expression, ",", 2, 1, 2)

        expression = "\n(1\n,\n 2, 3)"
        with self.assertRaises(ParseError) as catcher:
            parser.parse(expression)
        _assertExc(catcher.exception, expression, ",", 4, 3, 0)

    def testStr(self):
        """Test for formatting"""
        parser = ParserYacc()

        tree = parser.parse("(a+b)")
        self.assertEqual(str(tree), '(a + b)')

        tree = parser.parse("1 in (1,'x',3)")
        self.assertEqual(str(tree), "1 IN (1, 'x', 3)")

        tree = parser.parse("a not   in (1,'x',3)")
        self.assertEqual(str(tree), "a NOT IN (1, 'x', 3)")

        tree = parser.parse("(A or B) And NoT (x+3 > y)")
        self.assertEqual(str(tree), "(A OR B) AND NOT (x + 3 > y)")

    def testVisit(self):
        """Test for visitor methods"""

        # test should cover all visit* methods
        parser = ParserYacc()
        visitor = _Visitor()

        tree = parser.parse("(a+b)")
        result = tree.visit(visitor)
        self.assertEqual(result, "P(B(ID(a) + ID(b)))")

        tree = parser.parse("(A or B) and not (x + 3 > y)")
        result = tree.visit(visitor)
        self.assertEqual(result, "B(P(B(ID(A) OR ID(B))) AND U(NOT P(B(B(ID(x) + N(3)) > ID(y)))))")

        tree = parser.parse("x in (1,2) AND y NOT IN (1.1, .25, 1e2) OR z in ('a', 'b')")
        result = tree.visit(visitor)
        self.assertEqual(result, "B(B(IN(ID(x) (N(1), N(2))) AND !IN(ID(y) (N(1.1), N(.25), N(1e2))))"
                         " OR IN(ID(z) (S(a), S(b))))")


if __name__ == "__main__":
    unittest.main()

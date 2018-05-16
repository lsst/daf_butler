#
# LSST Data Management System
# Copyright 2018 AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
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
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#

"""Simple unit test for expr_parser/parserLex module.
"""

import unittest

from lsst.pipe.supertask.expr_parser import exprTree
from lsst.pipe.supertask.expr_parser import parserYacc
import lsst.utils.tests


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
        parser = parserYacc.ParserYacc()  # noqa: F841

    def testParseLiteral(self):
        """Tests for literals (strings/numbers)
        """
        parser = parserYacc.ParserYacc()

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
        parser = parserYacc.ParserYacc()

        tree = parser.parse('a')
        self.assertIsInstance(tree, exprTree.Identifier)
        self.assertEqual(tree.name, 'a')

        tree = parser.parse('a.b')
        self.assertIsInstance(tree, exprTree.Identifier)
        self.assertEqual(tree.name, 'a.b')

    def testParseParens(self):
        """Tests for identifiers
        """
        parser = parserYacc.ParserYacc()

        tree = parser.parse('(a)')
        self.assertIsInstance(tree, exprTree.Parens)
        self.assertIsInstance(tree.expr, exprTree.Identifier)
        self.assertEqual(tree.expr.name, 'a')

    def testUnaryOps(self):
        """Tests for unary plus and minus
        """
        parser = parserYacc.ParserYacc()

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
        parser = parserYacc.ParserYacc()

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
        parser = parserYacc.ParserYacc()

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
        parser = parserYacc.ParserYacc()

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
        parser = parserYacc.ParserYacc()

        for op in ('OR', 'XOR', 'AND'):
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
        parser = parserYacc.ParserYacc()

        expression = ("((camera='HSC' AND sensor != 9) OR camera='CFHT') "
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

        parser = parserYacc.ParserYacc()

        with self.assertRaises(parserYacc.ParserEOFError):
            parser.parse("")

        expression = "(1, 2, 3)"
        with self.assertRaises(parserYacc.ParseError) as catcher:
            parser.parse(expression)
        _assertExc(catcher.exception, expression, ",", 2, 1, 2)

        expression = "\n(1\n,\n 2, 3)"
        with self.assertRaises(parserYacc.ParseError) as catcher:
            parser.parse(expression)
        _assertExc(catcher.exception, expression, ",", 4, 3, 0)

    def testStr(self):
        """Test for formatting"""
        parser = parserYacc.ParserYacc()

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

        def _visitor(node, nodes):
            nodes.append(node)

        parser = parserYacc.ParserYacc()

        tree = parser.parse("(a+b)")

        # collect all nodes into a list, depth first
        nodes = []
        tree.visitDepthFirst(_visitor, nodes)
        self.assertEqual(len(nodes), 4)
        self.assertIsInstance(nodes[0], exprTree.Identifier)
        self.assertEqual(nodes[0].name, 'a')
        self.assertIsInstance(nodes[1], exprTree.Identifier)
        self.assertEqual(nodes[1].name, 'b')
        self.assertIsInstance(nodes[2], exprTree.BinaryOp)
        self.assertIsInstance(nodes[3], exprTree.Parens)

        # collect all nodes into a list, breadth first
        nodes = []
        tree.visitBreadthFirst(_visitor, nodes)
        self.assertEqual(len(nodes), 4)
        self.assertIsInstance(nodes[0], exprTree.Parens)
        self.assertIsInstance(nodes[1], exprTree.BinaryOp)
        self.assertIsInstance(nodes[2], exprTree.Identifier)
        self.assertEqual(nodes[2].name, 'a')
        self.assertIsInstance(nodes[3], exprTree.Identifier)
        self.assertEqual(nodes[3].name, 'b')


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()

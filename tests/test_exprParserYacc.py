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

"""Simple unit test for exprParser subpackage module."""

import unittest
from itertools import chain

import astropy.time

from lsst.daf.butler.queries.expressions.parser import (
    ParseError,
    ParserYacc,
    ParserYaccError,
    TreeVisitor,
    exprTree,
)
from lsst.daf.butler.queries.expressions.parser.parserYacc import _parseTimeString


class _Visitor(TreeVisitor):
    """Trivial implementation of TreeVisitor."""

    def visitNumericLiteral(self, value, node):
        return f"N({value})"

    def visitStringLiteral(self, value, node):
        return f"S({value})"

    def visitTimeLiteral(self, value, node):
        return f"T({value})"

    def visitUuidLiteral(self, value, node):
        return f"UUID({value})"

    def visitRangeLiteral(self, start, stop, stride, node):
        if stride is None:
            return f"R({start}..{stop})"
        else:
            return f"R({start}..{stop}:{stride})"

    def visitIdentifier(self, name, node):
        return f"ID({name})"

    def visitBind(self, name, node):
        return f":({name})"

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

    def visitPointNode(self, ra, dec, node):
        return f"POINT({ra}, {dec})"

    def visitCircleNode(self, ra, dec, radius, node):
        return f"CIRCLE({ra}, {dec}, {radius})"

    def visitBoxNode(self, ra, dec, width, height, node):
        return f"BOX({ra}, {dec}, {width}, {height})"

    def visitPolygonNode(self, vertices, node):
        params = ", ".join(str(param) for param in chain.from_iterable(vertices))
        return f"POLYGON({params})"

    def visitRegionNode(self, pos, node):
        return f"REGION({pos})"

    def visitTupleNode(self, expression, node):
        return f"TUPLE({expression})"

    def visitGlobNode(self, expression, pattern, node):
        return f"GLOB({expression}, {pattern})"


class ParserYaccTestCase(unittest.TestCase):
    """A test case for ParserYacc"""

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testInstantiate(self):
        """Tests for making ParserLex instances"""
        parser = ParserYacc()  # noqa: F841

    def testEmpty(self):
        """Tests for empty expression"""
        parser = ParserYacc()

        # empty expression is allowed, returns None
        tree = parser.parse("")
        self.assertIsNone(tree)

    def testParseLiteral(self):
        """Tests for literals (strings/numbers)"""
        parser = ParserYacc()

        tree = parser.parse("1")
        self.assertIsInstance(tree, exprTree.NumericLiteral)
        self.assertEqual(tree.value, "1")
        self.assertEqual(str(tree), "1")

        tree = parser.parse(".5e-2")
        self.assertIsInstance(tree, exprTree.NumericLiteral)
        self.assertEqual(tree.value, ".5e-2")
        self.assertEqual(str(tree), ".5e-2")

        tree = parser.parse("'string'")
        self.assertIsInstance(tree, exprTree.StringLiteral)
        self.assertEqual(tree.value, "string")
        self.assertEqual(str(tree), "'string'")

        tree = parser.parse("10..20")
        self.assertIsInstance(tree, exprTree.RangeLiteral)
        self.assertEqual(tree.start, 10)
        self.assertEqual(tree.stop, 20)
        self.assertEqual(tree.stride, None)
        self.assertEqual(str(tree), "10..20")

        tree = parser.parse("-10 .. 10:5")
        self.assertIsInstance(tree, exprTree.RangeLiteral)
        self.assertEqual(tree.start, -10)
        self.assertEqual(tree.stop, 10)
        self.assertEqual(tree.stride, 5)
        self.assertEqual(str(tree), "-10..10:5")

        # more extensive tests of time parsing is below
        tree = parser.parse("T'51544.0'")
        self.assertIsInstance(tree, exprTree.TimeLiteral)
        self.assertEqual(tree.value, astropy.time.Time(51544.0, format="mjd", scale="tai"))
        self.assertEqual(str(tree), "T'51544.0'")

        tree = parser.parse("T'2020-03-30T12:20:33'")
        self.assertIsInstance(tree, exprTree.TimeLiteral)
        self.assertEqual(tree.value, astropy.time.Time("2020-03-30T12:20:33", format="isot", scale="utc"))
        self.assertEqual(str(tree), "T'2020-03-30T12:20:33.000'")

    def testParseIdentifiers(self):
        """Tests for identifiers"""
        parser = ParserYacc()

        tree = parser.parse("a")
        self.assertIsInstance(tree, exprTree.Identifier)
        self.assertEqual(tree.name, "a")
        self.assertEqual(str(tree), "a")

        tree = parser.parse("a.b")
        self.assertIsInstance(tree, exprTree.Identifier)
        self.assertEqual(tree.name, "a.b")
        self.assertEqual(str(tree), "a.b")

    def testParseBind(self):
        """Tests for bind name"""
        parser = ParserYacc()

        tree = parser.parse(":a")
        self.assertIsInstance(tree, exprTree.BindName)
        self.assertEqual(tree.name, "a")
        self.assertEqual(str(tree), ":a")

        with self.assertRaises(ParserYaccError):
            tree = parser.parse(":1")

    def testParseParens(self):
        """Tests for identifiers"""
        parser = ParserYacc()

        tree = parser.parse("(a)")
        self.assertIsInstance(tree, exprTree.Parens)
        self.assertIsInstance(tree.expr, exprTree.Identifier)
        self.assertEqual(tree.expr.name, "a")
        self.assertEqual(str(tree), "(a)")

        tree = parser.parse("(:a)")
        self.assertIsInstance(tree, exprTree.Parens)
        self.assertIsInstance(tree.expr, exprTree.BindName)
        self.assertEqual(tree.expr.name, "a")
        self.assertEqual(str(tree), "(:a)")

    def testUnaryOps(self):
        """Tests for unary plus and minus"""
        parser = ParserYacc()

        tree = parser.parse("+a")
        self.assertIsInstance(tree, exprTree.UnaryOp)
        self.assertEqual(tree.op, "+")
        self.assertIsInstance(tree.operand, exprTree.Identifier)
        self.assertEqual(tree.operand.name, "a")
        self.assertEqual(str(tree), "+ a")

        tree = parser.parse("- x.y")
        self.assertIsInstance(tree, exprTree.UnaryOp)
        self.assertEqual(tree.op, "-")
        self.assertIsInstance(tree.operand, exprTree.Identifier)
        self.assertEqual(tree.operand.name, "x.y")
        self.assertEqual(str(tree), "- x.y")

    def testBinaryOps(self):
        """Tests for binary operators"""
        parser = ParserYacc()

        tree = parser.parse("a + b")
        self.assertIsInstance(tree, exprTree.BinaryOp)
        self.assertEqual(tree.op, "+")
        self.assertIsInstance(tree.lhs, exprTree.Identifier)
        self.assertIsInstance(tree.rhs, exprTree.Identifier)
        self.assertEqual(tree.lhs.name, "a")
        self.assertEqual(tree.rhs.name, "b")
        self.assertEqual(str(tree), "a + b")

        tree = parser.parse("a - 2")
        self.assertIsInstance(tree, exprTree.BinaryOp)
        self.assertEqual(tree.op, "-")
        self.assertIsInstance(tree.lhs, exprTree.Identifier)
        self.assertIsInstance(tree.rhs, exprTree.NumericLiteral)
        self.assertEqual(tree.lhs.name, "a")
        self.assertEqual(tree.rhs.value, "2")
        self.assertEqual(str(tree), "a - 2")

        tree = parser.parse("2 * 2")
        self.assertIsInstance(tree, exprTree.BinaryOp)
        self.assertEqual(tree.op, "*")
        self.assertIsInstance(tree.lhs, exprTree.NumericLiteral)
        self.assertIsInstance(tree.rhs, exprTree.NumericLiteral)
        self.assertEqual(tree.lhs.value, "2")
        self.assertEqual(tree.rhs.value, "2")
        self.assertEqual(str(tree), "2 * 2")

        tree = parser.parse("1.e5/2")
        self.assertIsInstance(tree, exprTree.BinaryOp)
        self.assertEqual(tree.op, "/")
        self.assertIsInstance(tree.lhs, exprTree.NumericLiteral)
        self.assertIsInstance(tree.rhs, exprTree.NumericLiteral)
        self.assertEqual(tree.lhs.value, "1.e5")
        self.assertEqual(tree.rhs.value, "2")
        self.assertEqual(str(tree), "1.e5 / 2")

        tree = parser.parse("333%76")
        self.assertIsInstance(tree, exprTree.BinaryOp)
        self.assertEqual(tree.op, "%")
        self.assertIsInstance(tree.lhs, exprTree.NumericLiteral)
        self.assertIsInstance(tree.rhs, exprTree.NumericLiteral)
        self.assertEqual(tree.lhs.value, "333")
        self.assertEqual(tree.rhs.value, "76")
        self.assertEqual(str(tree), "333 % 76")

        # tests for overlaps operator
        tree = parser.parse("region1 OVERLAPS region2")
        self.assertIsInstance(tree, exprTree.BinaryOp)
        self.assertEqual(tree.op, "OVERLAPS")
        self.assertIsInstance(tree.lhs, exprTree.Identifier)
        self.assertIsInstance(tree.rhs, exprTree.Identifier)
        self.assertEqual(str(tree), "region1 OVERLAPS region2")

        # time ranges with literals
        tree = parser.parse("(T'2020-01-01', T'2020-01-02') overlaps (T'2020-01-01', T'2020-01-02')")
        self.assertIsInstance(tree, exprTree.BinaryOp)
        self.assertEqual(tree.op, "OVERLAPS")
        self.assertIsInstance(tree.lhs, exprTree.TupleNode)
        self.assertIsInstance(tree.rhs, exprTree.TupleNode)
        self.assertEqual(
            str(tree),
            (
                "(T'2020-01-01 00:00:00.000', T'2020-01-02 00:00:00.000') "
                "OVERLAPS (T'2020-01-01 00:00:00.000', T'2020-01-02 00:00:00.000')"
            ),
        )

        # but syntax allows anything, it's visitor responsibility to decide
        # what are the right operands
        tree = parser.parse("x+y Overlaps function(x-y)")
        self.assertIsInstance(tree, exprTree.BinaryOp)
        self.assertEqual(tree.op, "OVERLAPS")
        self.assertIsInstance(tree.lhs, exprTree.BinaryOp)
        self.assertIsInstance(tree.rhs, exprTree.FunctionCall)
        self.assertEqual(str(tree), "x + y OVERLAPS function(x - y)")

    def testIsIn(self):
        """Tests for IN"""
        parser = ParserYacc()

        tree = parser.parse("a in (1,2,'X')")
        self.assertIsInstance(tree, exprTree.IsIn)
        self.assertFalse(tree.not_in)
        self.assertIsInstance(tree.lhs, exprTree.Identifier)
        self.assertEqual(tree.lhs.name, "a")
        self.assertIsInstance(tree.values, list)
        self.assertEqual(len(tree.values), 3)
        self.assertIsInstance(tree.values[0], exprTree.NumericLiteral)
        self.assertEqual(tree.values[0].value, "1")
        self.assertIsInstance(tree.values[1], exprTree.NumericLiteral)
        self.assertEqual(tree.values[1].value, "2")
        self.assertIsInstance(tree.values[2], exprTree.StringLiteral)
        self.assertEqual(tree.values[2].value, "X")
        self.assertEqual(str(tree), "a IN (1, 2, 'X')")

        tree = parser.parse("10 not in (1000, 2000..3000:100)")
        self.assertIsInstance(tree, exprTree.IsIn)
        self.assertTrue(tree.not_in)
        self.assertIsInstance(tree.lhs, exprTree.NumericLiteral)
        self.assertEqual(tree.lhs.value, "10")
        self.assertIsInstance(tree.values, list)
        self.assertEqual(len(tree.values), 2)
        self.assertIsInstance(tree.values[0], exprTree.NumericLiteral)
        self.assertEqual(tree.values[0].value, "1000")
        self.assertIsInstance(tree.values[1], exprTree.RangeLiteral)
        self.assertEqual(tree.values[1].start, 2000)
        self.assertEqual(tree.values[1].stop, 3000)
        self.assertEqual(tree.values[1].stride, 100)
        self.assertEqual(str(tree), "10 NOT IN (1000, 2000..3000:100)")

        tree = parser.parse("10 in (-1000, -2000)")
        self.assertIsInstance(tree, exprTree.IsIn)
        self.assertFalse(tree.not_in)
        self.assertIsInstance(tree.lhs, exprTree.NumericLiteral)
        self.assertEqual(tree.lhs.value, "10")
        self.assertIsInstance(tree.values, list)
        self.assertEqual(len(tree.values), 2)
        self.assertIsInstance(tree.values[0], exprTree.NumericLiteral)
        self.assertEqual(tree.values[0].value, "-1000")
        self.assertIsInstance(tree.values[1], exprTree.NumericLiteral)
        self.assertEqual(tree.values[1].value, "-2000")
        self.assertEqual(str(tree), "10 IN (-1000, -2000)")

        # test for time contained in time range, all literals
        tree = parser.parse("T'2020-01-01' in (T'2020-01-01', T'2020-01-02')")
        self.assertIsInstance(tree, exprTree.IsIn)
        self.assertFalse(tree.not_in)
        self.assertIsInstance(tree.lhs, exprTree.TimeLiteral)
        self.assertEqual(len(tree.values), 2)
        self.assertIsInstance(tree.values[0], exprTree.TimeLiteral)
        self.assertIsInstance(tree.values[1], exprTree.TimeLiteral)
        self.assertEqual(
            str(tree),
            "T'2020-01-01 00:00:00.000' IN (T'2020-01-01 00:00:00.000', T'2020-01-02 00:00:00.000')",
        )

        # test for time range contained in time range
        tree = parser.parse("(T'2020-01-01', t1) in (T'2020-01-01', t2)")
        self.assertIsInstance(tree, exprTree.IsIn)
        self.assertFalse(tree.not_in)
        self.assertIsInstance(tree.lhs, exprTree.TupleNode)
        self.assertEqual(len(tree.values), 2)
        self.assertIsInstance(tree.values[0], exprTree.TimeLiteral)
        self.assertIsInstance(tree.values[1], exprTree.Identifier)
        self.assertEqual(str(tree), "(T'2020-01-01 00:00:00.000', t1) IN (T'2020-01-01 00:00:00.000', t2)")

        # test for point in region (we don't have region syntax yet, use
        # identifier)
        tree = parser.parse("point(1, 2) in (region1)")
        self.assertIsInstance(tree, exprTree.IsIn)
        self.assertFalse(tree.not_in)
        self.assertIsInstance(tree.lhs, exprTree.PointNode)
        self.assertEqual(len(tree.values), 1)
        self.assertIsInstance(tree.values[0], exprTree.Identifier)
        self.assertEqual(str(tree), "POINT(1, 2) IN (region1)")

        # Test that bind can appear in RHS.
        tree = parser.parse("a in (:in)")
        self.assertIsInstance(tree, exprTree.IsIn)
        self.assertIsInstance(tree.lhs, exprTree.Identifier)
        self.assertEqual(len(tree.values), 1)
        self.assertIsInstance(tree.values[0], exprTree.BindName)
        self.assertEqual(str(tree), "a IN (:in)")

        tree = parser.parse("a not in (:a, :b, c, 1)")
        self.assertIsInstance(tree, exprTree.IsIn)
        self.assertIsInstance(tree.lhs, exprTree.Identifier)
        self.assertEqual(len(tree.values), 4)
        self.assertIsInstance(tree.values[0], exprTree.BindName)
        self.assertIsInstance(tree.values[1], exprTree.BindName)
        self.assertIsInstance(tree.values[2], exprTree.Identifier)
        self.assertIsInstance(tree.values[3], exprTree.NumericLiteral)
        self.assertEqual(str(tree), "a NOT IN (:a, :b, c, 1)")

        expr = (
            "id in ("
            "UUID('38a42b54-0822-4dce-93b7-47e9b0d8ad66'), "
            "UUID('782fb690-281a-4787-9b6e-f5324a9b6369'), "
            ":uuid"
            ")"
        )
        tree = parser.parse(expr)
        self.assertIsInstance(tree, exprTree.IsIn)
        self.assertIsInstance(tree.lhs, exprTree.Identifier)
        self.assertEqual(len(tree.values), 3)
        self.assertIsInstance(tree.values[0], exprTree.UuidLiteral)
        self.assertIsInstance(tree.values[1], exprTree.UuidLiteral)
        self.assertIsInstance(tree.values[2], exprTree.BindName)
        self.assertEqual(
            str(tree),
            (
                "id IN ("
                "UUID('38a42b54-0822-4dce-93b7-47e9b0d8ad66'), "
                "UUID('782fb690-281a-4787-9b6e-f5324a9b6369'), "
                ":uuid"
                ")"
            ),
        )

        # parens on right hand side are required
        with self.assertRaises(ParseError):
            parser.parse("point(1, 2) in region1")

        # and we don't support full expressions in RHS list
        with self.assertRaises(ParseError):
            parser.parse("point(1, 2) in (x + y)")

    def testCompareOps(self):
        """Tests for comparison operators"""
        parser = ParserYacc()

        for op in ("=", "!=", "<", "<=", ">", ">="):
            tree = parser.parse(f"a {op} 10")
            self.assertIsInstance(tree, exprTree.BinaryOp)
            self.assertEqual(tree.op, op)
            self.assertIsInstance(tree.lhs, exprTree.Identifier)
            self.assertIsInstance(tree.rhs, exprTree.NumericLiteral)
            self.assertEqual(tree.lhs.name, "a")
            self.assertEqual(tree.rhs.value, "10")
            self.assertEqual(str(tree), f"a {op} 10")

            tree = parser.parse(f"a {op} :b")
            self.assertIsInstance(tree, exprTree.BinaryOp)
            self.assertEqual(tree.op, op)
            self.assertIsInstance(tree.lhs, exprTree.Identifier)
            self.assertIsInstance(tree.rhs, exprTree.BindName)
            self.assertEqual(tree.lhs.name, "a")
            self.assertEqual(tree.rhs.name, "b")
            self.assertEqual(str(tree), f"a {op} :b")

            tree = parser.parse(f":b {op} a")
            self.assertIsInstance(tree, exprTree.BinaryOp)
            self.assertEqual(tree.op, op)
            self.assertIsInstance(tree.lhs, exprTree.BindName)
            self.assertIsInstance(tree.rhs, exprTree.Identifier)
            self.assertEqual(tree.lhs.name, "b")
            self.assertEqual(tree.rhs.name, "a")
            self.assertEqual(str(tree), f":b {op} a")

    def testBoolOps(self):
        """Tests for boolean operators"""
        parser = ParserYacc()

        for op in ("OR", "AND"):
            tree = parser.parse(f"a {op} b")
            self.assertIsInstance(tree, exprTree.BinaryOp)
            self.assertEqual(tree.op, op)
            self.assertIsInstance(tree.lhs, exprTree.Identifier)
            self.assertIsInstance(tree.rhs, exprTree.Identifier)
            self.assertEqual(tree.lhs.name, "a")
            self.assertEqual(tree.rhs.name, "b")
            self.assertEqual(str(tree), f"a {op} b")

        tree = parser.parse("NOT b")
        self.assertIsInstance(tree, exprTree.UnaryOp)
        self.assertEqual(tree.op, "NOT")
        self.assertIsInstance(tree.operand, exprTree.Identifier)
        self.assertEqual(tree.operand.name, "b")
        self.assertEqual(str(tree), "NOT b")

    def testFunctionCall(self):
        """Tests for function calls"""
        parser = ParserYacc()

        tree = parser.parse("f()")
        self.assertIsInstance(tree, exprTree.FunctionCall)
        self.assertEqual(tree.name, "f")
        self.assertEqual(tree.args, [])
        self.assertEqual(str(tree), "f()")

        tree = parser.parse("f1(a)")
        self.assertIsInstance(tree, exprTree.FunctionCall)
        self.assertEqual(tree.name, "f1")
        self.assertEqual(len(tree.args), 1)
        self.assertIsInstance(tree.args[0], exprTree.Identifier)
        self.assertEqual(tree.args[0].name, "a")
        self.assertEqual(str(tree), "f1(a)")

        tree = parser.parse("f2(:a, :b)")
        self.assertIsInstance(tree, exprTree.FunctionCall)
        self.assertEqual(tree.name, "f2")
        self.assertEqual(len(tree.args), 2)
        self.assertIsInstance(tree.args[0], exprTree.BindName)
        self.assertEqual(tree.args[0].name, "a")
        self.assertIsInstance(tree.args[1], exprTree.BindName)
        self.assertEqual(tree.args[1].name, "b")
        self.assertEqual(str(tree), "f2(:a, :b)")

        tree = parser.parse("anything_goes('a', x+y, ((a AND b) or (C = D)), NOT T < 42., Z IN (1,2,3,4))")
        self.assertIsInstance(tree, exprTree.FunctionCall)
        self.assertEqual(tree.name, "anything_goes")
        self.assertEqual(len(tree.args), 5)
        self.assertIsInstance(tree.args[0], exprTree.StringLiteral)
        self.assertIsInstance(tree.args[1], exprTree.BinaryOp)
        self.assertIsInstance(tree.args[2], exprTree.Parens)
        self.assertIsInstance(tree.args[3], exprTree.UnaryOp)
        self.assertIsInstance(tree.args[4], exprTree.IsIn)
        self.assertEqual(
            str(tree), "anything_goes('a', x + y, ((a AND b) OR (C = D)), NOT T < 42., Z IN (1, 2, 3, 4))"
        )

        with self.assertRaises(ParseError):
            parser.parse("f.ff()")

    def testPointNode(self):
        """Tests for POINT() function"""
        parser = ParserYacc()

        # POINT function makes special node type
        tree = parser.parse("POINT(Object.ra, 0.0)")
        self.assertIsInstance(tree, exprTree.PointNode)
        self.assertIsInstance(tree.ra, exprTree.Identifier)
        self.assertEqual(tree.ra.name, "Object.ra")
        self.assertIsInstance(tree.dec, exprTree.NumericLiteral)
        self.assertEqual(tree.dec.value, "0.0")
        self.assertEqual(str(tree), "POINT(Object.ra, 0.0)")

        # it is not case sensitive
        tree = parser.parse("Point(1, 1)")
        self.assertIsInstance(tree, exprTree.PointNode)
        self.assertEqual(str(tree), "POINT(1, 1)")

    def testCircleNode(self):
        """Tests for CIRCLE() function"""
        parser = ParserYacc()

        tree = parser.parse("CIRCLE(Object.ra, Object.dec, 0.1)")
        self.assertIsInstance(tree, exprTree.CircleNode)
        self.assertIsInstance(tree.ra, exprTree.Identifier)
        self.assertEqual(tree.ra.name, "Object.ra")
        self.assertIsInstance(tree.dec, exprTree.Identifier)
        self.assertEqual(tree.dec.name, "Object.dec")
        self.assertIsInstance(tree.radius, exprTree.NumericLiteral)
        self.assertEqual(tree.radius.value, "0.1")
        self.assertEqual(str(tree), "CIRCLE(Object.ra, Object.dec, 0.1)")

        tree = parser.parse("Circle(0.5 + 0.1, -1 * :bind_name, 1 / 10)")
        self.assertIsInstance(tree, exprTree.CircleNode)
        self.assertEqual(str(tree), "CIRCLE(0.5 + 0.1, - 1 * :bind_name, 1 / 10)")

        with self.assertRaises(ValueError):
            tree = parser.parse("Circle()")
        with self.assertRaises(ValueError):
            tree = parser.parse("Circle(0., 0.)")
        with self.assertRaises(ValueError):
            tree = parser.parse("Circle(0., 0., 0., 0.)")
        with self.assertRaises(ValueError):
            tree = parser.parse("Circle('1', 1, 1)")

    def testBoxNode(self):
        """Tests for BOX() function"""
        parser = ParserYacc()

        tree = parser.parse("BOX(Object.ra, Object.dec, 0.1, 2.)")
        self.assertIsInstance(tree, exprTree.BoxNode)
        self.assertIsInstance(tree.ra, exprTree.Identifier)
        self.assertEqual(tree.ra.name, "Object.ra")
        self.assertIsInstance(tree.dec, exprTree.Identifier)
        self.assertEqual(tree.dec.name, "Object.dec")
        self.assertIsInstance(tree.width, exprTree.NumericLiteral)
        self.assertEqual(tree.width.value, "0.1")
        self.assertIsInstance(tree.height, exprTree.NumericLiteral)
        self.assertEqual(tree.height.value, "2.")
        self.assertEqual(str(tree), "BOX(Object.ra, Object.dec, 0.1, 2.)")

        tree = parser.parse("box(0.5 + 0.1, -1 * :bind_name, 1 / 10, 42.)")
        self.assertIsInstance(tree, exprTree.BoxNode)
        self.assertEqual(str(tree), "BOX(0.5 + 0.1, - 1 * :bind_name, 1 / 10, 42.)")

        with self.assertRaises(ValueError):
            tree = parser.parse("box()")
        with self.assertRaises(ValueError):
            tree = parser.parse("box(0., 0.)")
        with self.assertRaises(ValueError):
            tree = parser.parse("box(0., 0., 0., 0., 1)")
        with self.assertRaises(ValueError):
            tree = parser.parse("box(:a IN (100), 1, 1, 1)")

    def testPolygonNode(self):
        """Tests for POLYGON() function"""
        parser = ParserYacc()

        tree = parser.parse("POLYGON(0, 0, 1, 0, 1, 1)")
        self.assertIsInstance(tree, exprTree.PolygonNode)
        self.assertEqual(len(tree.vertices), 3)
        for ra, dec in tree.vertices:
            self.assertIsInstance(ra, exprTree.NumericLiteral)
            self.assertIsInstance(dec, exprTree.NumericLiteral)
        self.assertEqual([ra.value for ra, dec in tree.vertices], ["0", "1", "1"])
        self.assertEqual([dec.value for ra, dec in tree.vertices], ["0", "0", "1"])
        self.assertEqual(str(tree), "POLYGON(0, 0, 1, 0, 1, 1)")

        with self.assertRaisesRegex(ValueError, "POLYGON requires at least three vertices"):
            tree = parser.parse("POLYGON()")
        with self.assertRaisesRegex(ValueError, "POLYGON requires at least three vertices"):
            tree = parser.parse("POLYGON(0., 0., 1., 1.)")
        with self.assertRaisesRegex(ValueError, "POLYGON requires even number of arguments"):
            tree = parser.parse("polygon(0, 0, 1, 0, 1, 1, 2)")
        with self.assertRaisesRegex(
            ValueError, "POLYGON argument must be either numeric expression or bind value"
        ):
            tree = parser.parse("Polygon(:a IN (100), 1, 1, 1, 2, 2)")

    def testRegionNode(self):
        """Tests for REGION() function"""
        parser = ParserYacc()

        tree = parser.parse("region('CIRCLE 0 0 1')")
        self.assertIsInstance(tree, exprTree.RegionNode)
        self.assertIsInstance(tree.pos, exprTree.StringLiteral)
        self.assertEqual(tree.pos.value, "CIRCLE 0 0 1")
        self.assertEqual(str(tree), "REGION('CIRCLE 0 0 1')")

        with self.assertRaisesRegex(ValueError, "REGION requires a single string argument"):
            tree = parser.parse("REGION()")
        with self.assertRaisesRegex(ValueError, "REGION requires a single string argument"):
            tree = parser.parse("REGION('CIRCLE', '0 1 1')")
        with self.assertRaisesRegex(ValueError, "REGION argument must be either a string or a bind value"):
            tree = parser.parse("region(a = b)")

    def testGlobNode(self):
        """Tests for GLOB() function"""
        parser = ParserYacc()

        # Literal pattern and simple identifier.
        tree = parser.parse("GLOB(group, '*')")
        self.assertIsInstance(tree, exprTree.GlobNode)
        self.assertIsInstance(tree.expression, exprTree.Identifier)
        self.assertEqual(tree.expression.name, "group")
        self.assertIsInstance(tree.pattern, exprTree.StringLiteral)
        self.assertEqual(tree.pattern.value, "*")
        self.assertEqual(str(tree), "GLOB(group, '*')")

        # Bind name for pattern, dotted name for identifier, all in parens.
        tree = parser.parse("glob((instrument.name), (:pattern))")
        self.assertIsInstance(tree, exprTree.GlobNode)
        self.assertIsInstance(tree.expression, exprTree.Identifier)
        self.assertEqual(tree.expression.name, "instrument.name")
        self.assertIsInstance(tree.pattern, exprTree.BindName)
        self.assertEqual(tree.pattern.name, "pattern")
        self.assertEqual(str(tree), "GLOB(instrument.name, :pattern)")

        # Invalid argument types
        with self.assertRaisesRegex(TypeError, r"glob\(\) first argument must be an identifier"):
            parser.parse("glob('string', '*')")
        with self.assertRaisesRegex(TypeError, r"glob\(\) second argument must be a string or a bind name"):
            parser.parse("glob(group, id)")

    def testTupleNode(self):
        """Tests for tuple"""
        parser = ParserYacc()

        # test with simple identifier and literal
        tree = parser.parse("(Object.ra, 0.0)")
        self.assertIsInstance(tree, exprTree.TupleNode)
        self.assertEqual(len(tree.items), 2)
        self.assertIsInstance(tree.items[0], exprTree.Identifier)
        self.assertEqual(tree.items[0].name, "Object.ra")
        self.assertIsInstance(tree.items[1], exprTree.NumericLiteral)
        self.assertEqual(tree.items[1].value, "0.0")
        self.assertEqual(str(tree), "(Object.ra, 0.0)")

        # any expression can appear in tuple
        tree = parser.parse("(x+y, ((a AND :b) or (C = D)))")
        self.assertIsInstance(tree, exprTree.TupleNode)
        self.assertEqual(len(tree.items), 2)
        self.assertIsInstance(tree.items[0], exprTree.BinaryOp)
        self.assertIsInstance(tree.items[1], exprTree.Parens)
        self.assertEqual(str(tree), "(x + y, ((a AND :b) OR (C = D)))")

        # only two items can appear in a tuple
        with self.assertRaises(ParseError):
            parser.parse("(1, 2, 3)")

    def testExpression(self):
        """Test for more or less complete expression"""
        parser = ParserYacc()

        expression = (
            "((instrument='HSC' AND detector != 9) OR instrument='CFHT') "
            "AND tract=8766 AND patch.cell_x > 5 AND "
            "patch.cell_y < 4 AND band=:band"
        )

        tree = parser.parse(expression)
        self.assertIsInstance(tree, exprTree.BinaryOp)
        self.assertEqual(tree.op, "AND")
        self.assertIsInstance(tree.lhs, exprTree.BinaryOp)
        # AND is left-associative, so rhs operand will be the
        # last sub-expressions
        self.assertIsInstance(tree.rhs, exprTree.BinaryOp)
        self.assertEqual(tree.rhs.op, "=")
        self.assertIsInstance(tree.rhs.lhs, exprTree.Identifier)
        self.assertEqual(tree.rhs.lhs.name, "band")
        self.assertIsInstance(tree.rhs.rhs, exprTree.BindName)
        self.assertEqual(tree.rhs.rhs.name, "band")
        self.assertEqual(
            str(tree),
            (
                "((instrument = 'HSC' AND detector != 9) OR instrument = 'CFHT') "
                "AND tract = 8766 AND patch.cell_x > 5 AND "
                "patch.cell_y < 4 AND band = :band"
            ),
        )

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
        _assertExc(catcher.exception, expression, ",", 5, 1, 5)

        expression = "\n(1\n,\n 2, 3)"
        with self.assertRaises(ParseError) as catcher:
            parser.parse(expression)
        _assertExc(catcher.exception, expression, ",", 8, 4, 2)

        expression = "T'not-a-time'"
        with self.assertRaises(ParseError) as catcher:
            parser.parse(expression)
        _assertExc(catcher.exception, expression, "not-a-time", 0, 1, 0)

    def testStr(self):
        """Test for formatting"""
        parser = ParserYacc()

        tree = parser.parse("(a+b)")
        self.assertEqual(str(tree), "(a + b)")

        tree = parser.parse("1 in (1,'x',3)")
        self.assertEqual(str(tree), "1 IN (1, 'x', 3)")

        tree = parser.parse("a not   in (1,'x',3)")
        self.assertEqual(str(tree), "a NOT IN (1, 'x', 3)")

        tree = parser.parse("(A or B) And NoT (x+3 > y)")
        self.assertEqual(str(tree), "(A OR B) AND NOT (x + 3 > y)")

        tree = parser.parse("A in (100, 200..300:50)")
        self.assertEqual(str(tree), "A IN (100, 200..300:50)")

    def testVisit(self):
        """Test for visitor methods"""
        # test should cover all visit* methods
        parser = ParserYacc()
        visitor = _Visitor()

        tree = parser.parse("(a+b)")
        result = tree.visit(visitor)
        self.assertEqual(result, "P(B(ID(a) + ID(b)))")

        tree = parser.parse("(A or B) and not (x + 3 > :y)")
        result = tree.visit(visitor)
        self.assertEqual(result, "B(P(B(ID(A) OR ID(B))) AND U(NOT P(B(B(ID(x) + N(3)) > :(y)))))")

        tree = parser.parse("x in (1,2) AND y NOT IN (1.1, .25, 1e2) OR :z in ('a', 'b')")
        result = tree.visit(visitor)
        self.assertEqual(
            result,
            "B(B(IN(ID(x) (N(1), N(2))) AND !IN(ID(y) (N(1.1), N(.25), N(1e2)))) OR IN(:(z) (S(a), S(b))))",
        )

        tree = parser.parse("x in (1,2,5..15) AND y NOT IN (-100..100:10)")
        result = tree.visit(visitor)
        self.assertEqual(result, "B(IN(ID(x) (N(1), N(2), R(5..15))) AND !IN(ID(y) (R(-100..100:10))))")

        tree = parser.parse("time > T'2020-03-30'")
        result = tree.visit(visitor)
        self.assertEqual(result, "B(ID(time) > T(2020-03-30 00:00:00.000))")

        tree = parser.parse("point(ra, :dec)")
        result = tree.visit(visitor)
        self.assertEqual(result, "POINT(ID(ra), :(dec))")

        tree = parser.parse("circle(ra, :dec, 1.5)")
        result = tree.visit(visitor)
        self.assertEqual(result, "CIRCLE(ID(ra), :(dec), N(1.5))")

        tree = parser.parse("box(ra, :dec, 1.5, 10)")
        result = tree.visit(visitor)
        self.assertEqual(result, "BOX(ID(ra), :(dec), N(1.5), N(10))")

        tree = parser.parse("Polygon(ra, :dec, 0, 0, 180, 0)")
        result = tree.visit(visitor)
        self.assertEqual(result, "POLYGON(ID(ra), :(dec), N(0), N(0), N(180), N(0))")

        tree = parser.parse("region('CIRCLE 0 0 1.')")
        result = tree.visit(visitor)
        self.assertEqual(result, "REGION(S(CIRCLE 0 0 1.))")

        tree = parser.parse("glob(group, 'prefix#*')")
        result = tree.visit(visitor)
        self.assertEqual(result, "GLOB(ID(group), S(prefix#*))")

    def testParseTimeStr(self):
        """Test for _parseTimeString method"""
        # few expected failures
        bad_times = [
            "",
            " ",
            "123.456e10",  # no exponents
            "mjd-dj/123.456",  # format can only have word chars
            "123.456/mai-tai",  # scale can only have word chars
            "2020-03-01 00",  # iso needs minutes if hour is given
            "2020-03-01T",  # isot needs hour:minute
            "2020:100:12",  # yday needs minutes if hour is given
            "format/123456.00",  # unknown format
            "123456.00/unscale",  # unknown scale
        ]
        for bad_time in bad_times:
            with self.assertRaises(ValueError):
                _parseTimeString(bad_time)

        # each tuple is (string, value, format, scale)
        tests = [
            ("51544.0", 51544.0, "mjd", "tai"),
            ("mjd/51544.0", 51544.0, "mjd", "tai"),
            ("51544.0/tai", 51544.0, "mjd", "tai"),
            ("mjd/51544.0/tai", 51544.0, "mjd", "tai"),
            ("MJd/51544.0/TAi", 51544.0, "mjd", "tai"),
            ("jd/2451544.5", 2451544.5, "jd", "tai"),
            ("jd/2451544.5", 2451544.5, "jd", "tai"),
            ("51544.0/utc", 51544.0, "mjd", "utc"),
            ("unix/946684800.0", 946684800.0, "unix", "utc"),
            ("cxcsec/63072064.184", 63072064.184, "cxcsec", "tt"),
            ("2020-03-30", "2020-03-30 00:00:00.000", "iso", "utc"),
            ("2020-03-30 12:20", "2020-03-30 12:20:00.000", "iso", "utc"),
            ("2020-03-30 12:20:33.456789", "2020-03-30 12:20:33.457", "iso", "utc"),
            ("2020-03-30T12:20", "2020-03-30T12:20:00.000", "isot", "utc"),
            ("2020-03-30T12:20:33.456789", "2020-03-30T12:20:33.457", "isot", "utc"),
            ("isot/2020-03-30", "2020-03-30T00:00:00.000", "isot", "utc"),
            ("2020-03-30/tai", "2020-03-30 00:00:00.000", "iso", "tai"),
            ("+02020-03-30", "2020-03-30T00:00:00.000", "fits", "utc"),
            ("+02020-03-30T12:20:33", "2020-03-30T12:20:33.000", "fits", "utc"),
            ("+02020-03-30T12:20:33.456789", "2020-03-30T12:20:33.457", "fits", "utc"),
            ("fits/2020-03-30", "2020-03-30T00:00:00.000", "fits", "utc"),
            ("2020:123", "2020:123:00:00:00.000", "yday", "utc"),
            ("2020:123:12:20", "2020:123:12:20:00.000", "yday", "utc"),
            ("2020:123:12:20:33.456789", "2020:123:12:20:33.457", "yday", "utc"),
            ("yday/2020:123:12:20/tai", "2020:123:12:20:00.000", "yday", "tai"),
        ]
        for time_str, value, fmt, scale in tests:
            time = _parseTimeString(time_str)
            self.assertEqual(time.value, value)
            self.assertEqual(time.format, fmt)
            self.assertEqual(time.scale, scale)


if __name__ == "__main__":
    unittest.main()

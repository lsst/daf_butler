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

"""Simple unit test for expr_parser/parserLex module."""

import re
import unittest

from lsst.daf.butler.registry.queries.expressions.parser import ParserLex, ParserLexError


class ParserLexTestCase(unittest.TestCase):
    """A test case for ParserLex"""

    def _assertToken(self, token, type, value, lineno=None, lexpos=None):
        self.assertIsNotNone(token)
        self.assertEqual(token.type, type)
        self.assertEqual(token.value, value)
        if lineno is not None:
            self.assertEqual(token.lineno, lineno)
        if lexpos is not None:
            self.assertEqual(token.lexpos, lexpos)

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testInstantiate(self):
        """Tests for making ParserLex instances"""
        default_reflags = re.IGNORECASE | re.VERBOSE
        lexer = ParserLex.make_lexer()
        self.assertEqual(lexer.lexreflags, default_reflags)

        lexer = ParserLex.make_lexer(reflags=re.DOTALL)
        self.assertEqual(lexer.lexreflags, re.DOTALL | default_reflags)

    def testSimpleTokens(self):
        """Test for simple tokens"""
        lexer = ParserLex.make_lexer()

        lexer.input("=!= <<= >>= +-*/()")
        self._assertToken(lexer.token(), "EQ", "=")
        self._assertToken(lexer.token(), "NE", "!=")
        self._assertToken(lexer.token(), "LT", "<")
        self._assertToken(lexer.token(), "LE", "<=")
        self._assertToken(lexer.token(), "GT", ">")
        self._assertToken(lexer.token(), "GE", ">=")
        self._assertToken(lexer.token(), "ADD", "+")
        self._assertToken(lexer.token(), "SUB", "-")
        self._assertToken(lexer.token(), "MUL", "*")
        self._assertToken(lexer.token(), "DIV", "/")
        self._assertToken(lexer.token(), "LPAREN", "(")
        self._assertToken(lexer.token(), "RPAREN", ")")
        self.assertIsNone(lexer.token())

    def testReservedTokens(self):
        """Test for reserved words"""
        lexer = ParserLex.make_lexer()

        tokens = "NOT IN OR AND OVERLAPS"
        lexer.input(tokens)
        for token in tokens.split():
            self._assertToken(lexer.token(), token, token)
        self.assertIsNone(lexer.token())

        tokens = "not in or and overlaps"
        lexer.input(tokens)
        for token in tokens.split():
            self._assertToken(lexer.token(), token.upper(), token.upper())
        self.assertIsNone(lexer.token())

        # not reserved
        token = "NOTIN"
        lexer.input(token)
        self._assertToken(lexer.token(), "SIMPLE_IDENTIFIER", token)
        self.assertIsNone(lexer.token())

    def testStringLiteral(self):
        """Test for string literals"""
        lexer = ParserLex.make_lexer()

        lexer.input("''")
        self._assertToken(lexer.token(), "STRING_LITERAL", "")
        self.assertIsNone(lexer.token())

        lexer.input("'string'")
        self._assertToken(lexer.token(), "STRING_LITERAL", "string")
        self.assertIsNone(lexer.token())

        lexer.input("'string' 'string'\n'string'")
        self._assertToken(lexer.token(), "STRING_LITERAL", "string")
        self._assertToken(lexer.token(), "STRING_LITERAL", "string")
        self._assertToken(lexer.token(), "STRING_LITERAL", "string")
        self.assertIsNone(lexer.token())

        # odd newline inside string
        lexer.input("'string\nstring'")
        with self.assertRaises(ParserLexError):
            lexer.token()

        lexer.input("'string")
        with self.assertRaises(ParserLexError):
            lexer.token()

    def testNumericLiteral(self):
        """Test for numeric literals"""
        lexer = ParserLex.make_lexer()

        lexer.input("0 100 999. 100.1 1e10 1e-10 1.e+20 .2E5")
        self._assertToken(lexer.token(), "NUMERIC_LITERAL", "0")
        self._assertToken(lexer.token(), "NUMERIC_LITERAL", "100")
        self._assertToken(lexer.token(), "NUMERIC_LITERAL", "999.")
        self._assertToken(lexer.token(), "NUMERIC_LITERAL", "100.1")
        self._assertToken(lexer.token(), "NUMERIC_LITERAL", "1e10")
        self._assertToken(lexer.token(), "NUMERIC_LITERAL", "1e-10")
        self._assertToken(lexer.token(), "NUMERIC_LITERAL", "1.e+20")
        self._assertToken(lexer.token(), "NUMERIC_LITERAL", ".2E5")
        self.assertIsNone(lexer.token())

    def testRangeLiteral(self):
        """Test for range literals"""
        lexer = ParserLex.make_lexer()

        lexer.input("0..10 -10..-1 -10..10:2 0 .. 10 0 .. 10 : 2 ")
        self._assertToken(lexer.token(), "RANGE_LITERAL", (0, 10, None))
        self._assertToken(lexer.token(), "RANGE_LITERAL", (-10, -1, None))
        self._assertToken(lexer.token(), "RANGE_LITERAL", (-10, 10, 2))
        self._assertToken(lexer.token(), "RANGE_LITERAL", (0, 10, None))
        self._assertToken(lexer.token(), "RANGE_LITERAL", (0, 10, 2))
        self.assertIsNone(lexer.token())

    def testTimeLiteral(self):
        """Test for time literals"""
        lexer = ParserLex.make_lexer()

        # string can contain anything, lexer does not check it
        lexer.input("T'2020-03-30' T'2020-03-30 00:00:00' T'2020-03-30T00:00:00' T'123.456' T'time'")
        self._assertToken(lexer.token(), "TIME_LITERAL", "2020-03-30")
        self._assertToken(lexer.token(), "TIME_LITERAL", "2020-03-30 00:00:00")
        self._assertToken(lexer.token(), "TIME_LITERAL", "2020-03-30T00:00:00")
        self._assertToken(lexer.token(), "TIME_LITERAL", "123.456")
        self._assertToken(lexer.token(), "TIME_LITERAL", "time")
        self.assertIsNone(lexer.token())

    def testIdentifier(self):
        """Test for numeric literals"""
        lexer = ParserLex.make_lexer()

        lexer.input("ID id _012 a_b_C")
        self._assertToken(lexer.token(), "SIMPLE_IDENTIFIER", "ID")
        self._assertToken(lexer.token(), "SIMPLE_IDENTIFIER", "id")
        self._assertToken(lexer.token(), "SIMPLE_IDENTIFIER", "_012")
        self._assertToken(lexer.token(), "SIMPLE_IDENTIFIER", "a_b_C")
        self.assertIsNone(lexer.token())

        lexer.input("a.b a.b.c _._ _._._")
        self._assertToken(lexer.token(), "QUALIFIED_IDENTIFIER", "a.b")
        self._assertToken(lexer.token(), "QUALIFIED_IDENTIFIER", "a.b.c")
        self._assertToken(lexer.token(), "QUALIFIED_IDENTIFIER", "_._")
        self._assertToken(lexer.token(), "QUALIFIED_IDENTIFIER", "_._._")
        self.assertIsNone(lexer.token())

        lexer.input(".id")
        with self.assertRaises(ParserLexError):
            lexer.token()

        lexer.input("id.")
        self._assertToken(lexer.token(), "SIMPLE_IDENTIFIER", "id")
        with self.assertRaises(ParserLexError):
            lexer.token()

        lexer.input("id.id.id.id")
        self._assertToken(lexer.token(), "QUALIFIED_IDENTIFIER", "id.id.id")
        with self.assertRaises(ParserLexError):
            lexer.token()

    def testExpression(self):
        """Test for more or less complete expression"""
        lexer = ParserLex.make_lexer()

        expr = (
            "((instrument='HSC' AND detector != 9) OR instrument='CFHT') "
            "AND tract=8766 AND patch.cell_x > 5 AND "
            "patch.cell_y < 4 AND band='i' "
            "or visit IN (1..50:2)"
        )
        tokens = (
            ("LPAREN", "("),
            ("LPAREN", "("),
            ("SIMPLE_IDENTIFIER", "instrument"),
            ("EQ", "="),
            ("STRING_LITERAL", "HSC"),
            ("AND", "AND"),
            ("SIMPLE_IDENTIFIER", "detector"),
            ("NE", "!="),
            ("NUMERIC_LITERAL", "9"),
            ("RPAREN", ")"),
            ("OR", "OR"),
            ("SIMPLE_IDENTIFIER", "instrument"),
            ("EQ", "="),
            ("STRING_LITERAL", "CFHT"),
            ("RPAREN", ")"),
            ("AND", "AND"),
            ("SIMPLE_IDENTIFIER", "tract"),
            ("EQ", "="),
            ("NUMERIC_LITERAL", "8766"),
            ("AND", "AND"),
            ("QUALIFIED_IDENTIFIER", "patch.cell_x"),
            ("GT", ">"),
            ("NUMERIC_LITERAL", "5"),
            ("AND", "AND"),
            ("QUALIFIED_IDENTIFIER", "patch.cell_y"),
            ("LT", "<"),
            ("NUMERIC_LITERAL", "4"),
            ("AND", "AND"),
            ("SIMPLE_IDENTIFIER", "band"),
            ("EQ", "="),
            ("STRING_LITERAL", "i"),
            ("OR", "OR"),
            ("SIMPLE_IDENTIFIER", "visit"),
            ("IN", "IN"),
            ("LPAREN", "("),
            ("RANGE_LITERAL", (1, 50, 2)),
            ("RPAREN", ")"),
        )
        lexer.input(expr)
        for type, value in tokens:
            self._assertToken(lexer.token(), type, value)
        self.assertIsNone(lexer.token())

    def testExceptions(self):
        """Test for exception contents"""

        def _assertExc(exc, expr, remain, pos, lineno):
            """Check exception attribute values"""
            self.assertEqual(exc.expression, expr)
            self.assertEqual(exc.remain, remain)
            self.assertEqual(exc.pos, pos)
            self.assertEqual(exc.lineno, lineno)

        lexer = ParserLex.make_lexer()
        expr = "a.b.c.d"
        lexer.input(expr)
        self._assertToken(lexer.token(), "QUALIFIED_IDENTIFIER", "a.b.c")
        with self.assertRaises(ParserLexError) as catcher:
            lexer.token()
        _assertExc(catcher.exception, expr, ".d", 5, 1)

        lexer = ParserLex.make_lexer()
        expr = "a \n& b"
        lexer.input(expr)
        self._assertToken(lexer.token(), "SIMPLE_IDENTIFIER", "a")
        with self.assertRaises(ParserLexError) as catcher:
            lexer.token()
        _assertExc(catcher.exception, expr, "& b", 3, 2)

        lexer = ParserLex.make_lexer()
        expr = "a\n=\n1e5.e2"
        lexer.input(expr)
        self._assertToken(lexer.token(), "SIMPLE_IDENTIFIER", "a")
        self._assertToken(lexer.token(), "EQ", "=")
        self._assertToken(lexer.token(), "NUMERIC_LITERAL", "1e5")
        with self.assertRaises(ParserLexError) as catcher:
            lexer.token()
        _assertExc(catcher.exception, expr, ".e2", 7, 3)

        # zero stride in range literal
        lexer = ParserLex.make_lexer()
        expr = "1..2:0"
        lexer.input(expr)
        self._assertToken(lexer.token(), "RANGE_LITERAL", (1, 2, None))
        with self.assertRaises(ParserLexError) as catcher:
            lexer.token()
        _assertExc(catcher.exception, expr, ":0", 4, 1)

        # negative stride in range literal
        lexer = ParserLex.make_lexer()
        expr = "1..2:-10"
        lexer.input(expr)
        self._assertToken(lexer.token(), "RANGE_LITERAL", (1, 2, None))
        with self.assertRaises(ParserLexError) as catcher:
            lexer.token()
        _assertExc(catcher.exception, expr, ":-10", 4, 1)


if __name__ == "__main__":
    unittest.main()

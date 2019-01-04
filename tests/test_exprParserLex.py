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

"""Simple unit test for expr_parser/parserLex module.
"""

import re
import unittest

from lsst.daf.butler.exprParser import ParserLex, ParserLexError
import lsst.utils.tests


class ParserLexTestCase(unittest.TestCase):
    """A test case for ParserLex
    """

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
        """Tests for making ParserLex instances
        """

        default_reflags = re.IGNORECASE | re.VERBOSE
        lexer = ParserLex.make_lexer()
        self.assertEqual(lexer.lexreflags, default_reflags)

        lexer = ParserLex.make_lexer(reflags=re.DOTALL)
        self.assertEqual(lexer.lexreflags, re.DOTALL | default_reflags)

    def testSimpleTokens(self):
        """Test for simple tokens"""
        lexer = ParserLex.make_lexer()

        lexer.input("=!= <<= >>= +-*/()")
        self._assertToken(lexer.token(), 'EQ', '=')
        self._assertToken(lexer.token(), 'NE', '!=')
        self._assertToken(lexer.token(), 'LT', '<')
        self._assertToken(lexer.token(), 'LE', '<=')
        self._assertToken(lexer.token(), 'GT', '>')
        self._assertToken(lexer.token(), 'GE', '>=')
        self._assertToken(lexer.token(), 'ADD', '+')
        self._assertToken(lexer.token(), 'SUB', '-')
        self._assertToken(lexer.token(), 'MUL', '*')
        self._assertToken(lexer.token(), 'DIV', '/')
        self._assertToken(lexer.token(), 'LPAREN', '(')
        self._assertToken(lexer.token(), 'RPAREN', ')')
        self.assertIsNone(lexer.token())

    def testReservedTokens(self):
        """Test for reserved words"""
        lexer = ParserLex.make_lexer()

#         tokens = "IS NOT IN NULL OR XOR AND BETWEEN LIKE ESCAPE REGEXP"
        tokens = "NOT IN OR XOR AND"
        lexer.input(tokens)
        for token in tokens.split():
            self._assertToken(lexer.token(), token, token)
        self.assertIsNone(lexer.token())

#         tokens = "is not in null or xor and between like escape regexp"
        tokens = "not in or xor and"
        lexer.input(tokens)
        for token in tokens.split():
            self._assertToken(lexer.token(), token.upper(), token)
        self.assertIsNone(lexer.token())

        # not reserved
        token = "ISNOTIN"
        lexer.input(token)
        self._assertToken(lexer.token(), "IDENTIFIER", token)
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

    def testIdentifier(self):
        """Test for numeric literals"""
        lexer = ParserLex.make_lexer()

        lexer.input("ID id _012 a_b_C")
        self._assertToken(lexer.token(), "IDENTIFIER", "ID")
        self._assertToken(lexer.token(), "IDENTIFIER", "id")
        self._assertToken(lexer.token(), "IDENTIFIER", "_012")
        self._assertToken(lexer.token(), "IDENTIFIER", "a_b_C")
        self.assertIsNone(lexer.token())

        lexer.input("a.b _._")
        self._assertToken(lexer.token(), "IDENTIFIER", "a.b")
        self._assertToken(lexer.token(), "IDENTIFIER", "_._")
        self.assertIsNone(lexer.token())

        lexer.input(".id")
        with self.assertRaises(ParserLexError):
            lexer.token()

        lexer.input("id.")
        self._assertToken(lexer.token(), "IDENTIFIER", "id")
        with self.assertRaises(ParserLexError):
            lexer.token()

        lexer.input("id.id.id")
        self._assertToken(lexer.token(), "IDENTIFIER", "id.id")
        with self.assertRaises(ParserLexError):
            lexer.token()

    def testExpression(self):
        """Test for more or less complete expression"""
        lexer = ParserLex.make_lexer()

        expr = ("((instrument='HSC' AND detector != 9) OR instrument='CFHT') "
                "AND tract=8766 AND patch.cell_x > 5 AND "
                "patch.cell_y < 4 AND abstract_filter='i'")
        tokens = (("LPAREN", "("),
                  ("LPAREN", "("),
                  ("IDENTIFIER", "instrument"),
                  ("EQ", "="),
                  ("STRING_LITERAL", "HSC"),
                  ("AND", "AND"),
                  ("IDENTIFIER", "detector"),
                  ("NE", "!="),
                  ("NUMERIC_LITERAL", "9"),
                  ("RPAREN", ")"),
                  ("OR", "OR"),
                  ("IDENTIFIER", "instrument"),
                  ("EQ", "="),
                  ("STRING_LITERAL", "CFHT"),
                  ("RPAREN", ")"),
                  ("AND", "AND"),
                  ("IDENTIFIER", "tract"),
                  ("EQ", "="),
                  ("NUMERIC_LITERAL", "8766"),
                  ("AND", "AND"),
                  ("IDENTIFIER", "patch.cell_x"),
                  ("GT", ">"),
                  ("NUMERIC_LITERAL", "5"),
                  ("AND", "AND"),
                  ("IDENTIFIER", "patch.cell_y"),
                  ("LT", "<"),
                  ("NUMERIC_LITERAL", "4"),
                  ("AND", "AND"),
                  ("IDENTIFIER", "abstract_filter"),
                  ("EQ", "="),
                  ("STRING_LITERAL", "i"))
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
        expr = "a.b.c"
        lexer.input(expr)
        self._assertToken(lexer.token(), "IDENTIFIER", "a.b")
        with self.assertRaises(ParserLexError) as catcher:
            lexer.token()
        _assertExc(catcher.exception, expr, ".c", 3, 1)

        lexer = ParserLex.make_lexer()
        expr = "a \n& b"
        lexer.input(expr)
        self._assertToken(lexer.token(), "IDENTIFIER", "a")
        with self.assertRaises(ParserLexError) as catcher:
            lexer.token()
        _assertExc(catcher.exception, expr, "& b", 3, 2)

        lexer = ParserLex.make_lexer()
        expr = "a\n=\n1e5.e2"
        lexer.input(expr)
        self._assertToken(lexer.token(), "IDENTIFIER", "a")
        self._assertToken(lexer.token(), "EQ", "=")
        self._assertToken(lexer.token(), "NUMERIC_LITERAL", "1e5")
        with self.assertRaises(ParserLexError) as catcher:
            lexer.token()
        _assertExc(catcher.exception, expr, ".e2", 7, 3)


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()

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

# type: ignore

"""Module which defines PLY lexer for user expressions parsed by pre-flight.
"""

__all__ = ["ParserLex", "ParserLexError"]

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import re

# -----------------------------
#  Imports for other modules --
# -----------------------------
from .ply import lex

# ----------------------------------
#  Local non-exported definitions --
# ----------------------------------

_RE_RANGE = r"(?P<start>-?\d+)\s*\.\.\s*(?P<stop>-?\d+)(\s*:\s*(?P<stride>[1-9]\d*))?"
"""Regular expression to match range literal in the form NUM..NUM[:NUM],
this must match t_RANGE_LITERAL docstring.
"""

# ------------------------
#  Exported definitions --
# ------------------------


class ParserLexError(Exception):
    """Exception raised for lex-phase errors.

    Attributes
    ----------
    expression : str
        Full initial expression being parsed
    remain : str
        Remaining non-parsed part of the expression
    pos : int
        Current parsing position, offset from beginning of expression in
        characters
    lineno : int
        Current line number in the expression
    """

    def __init__(self, expression, remain, pos, lineno):
        Exception.__init__(self, "Unexpected character at position {}".format(pos))
        self.expression = expression
        self.remain = remain
        self.pos = pos
        self.lineno = lineno


class ParserLex:
    """Class which defines PLY lexer."""

    @classmethod
    def make_lexer(cls, reflags=0, **kwargs):
        """Factory for lexers.

        Returns
        -------
        `ply.lex.Lexer` instance.
        """

        # make sure that flags that we need are there
        kw = dict(reflags=reflags | re.IGNORECASE | re.VERBOSE)
        kw.update(kwargs)

        return lex.lex(object=cls(), **kw)

    # literals = ""

    # reserved words in a grammar.
    # SQL has reserved words which we could potentially make reserved in our
    # grammar too, for now try to pretend we don't care about SQL
    reserved = dict(
        # IS="IS",
        IN="IN",
        # NULL="NULL",
        OR="OR",
        AND="AND",
        NOT="NOT",
        OVERLAPS="OVERLAPS",
        # BETWEEN="BETWEEN",
        # LIKE="LIKE",
        # ESCAPE="ESCAPE",
        # REGEXP="REGEXP"
    )

    # List of token names.
    tokens = (
        "NUMERIC_LITERAL",
        "TIME_LITERAL",
        "STRING_LITERAL",
        "RANGE_LITERAL",
        # 'DURATION_LITERAL',
        "QUALIFIED_IDENTIFIER",
        "SIMPLE_IDENTIFIER",
        "LPAREN",
        "RPAREN",
        "EQ",
        "NE",
        "LT",
        "LE",
        "GT",
        "GE",
        "ADD",
        "SUB",
        "MUL",
        "DIV",
        "MOD",
        "COMMA",
    ) + tuple(reserved.values())

    # Regular expression rules for simple tokens
    t_LPAREN = r"\("
    t_RPAREN = r"\)"
    t_EQ = "="
    t_NE = "!="
    t_LT = "<"
    t_LE = "<="
    t_GT = ">"
    t_GE = ">="
    t_ADD = r"\+"
    t_SUB = "-"
    t_MUL = r"\*"
    t_DIV = "/"
    t_MOD = "%"
    t_COMMA = ","

    # A string containing ignored characters (spaces and tabs)
    t_ignore = " \t"

    # Define a rule so we can track line numbers
    def t_newline(self, t):
        r"\n+"
        t.lexer.lineno += len(t.value)

    # quoted string prefixed with 'T'
    def t_TIME_LITERAL(self, t):
        r"T'.*?'"
        # strip quotes
        t.value = t.value[2:-1]
        return t

    # quoted string
    def t_STRING_LITERAL(self, t):
        r"'.*?'"
        # strip quotes
        t.value = t.value[1:-1]
        return t

    # range literal in format N..M[:S], spaces allowed, see _RE_RANGE
    @lex.TOKEN(_RE_RANGE)
    def t_RANGE_LITERAL(self, t):
        match = re.match(_RE_RANGE, t.value)
        start = int(match.group("start"))
        stop = int(match.group("stop"))
        stride = match.group("stride")
        if stride is not None:
            stride = int(stride)
        t.value = (start, stop, stride)
        return t

    # numbers are used as strings by parser, do not convert
    def t_NUMERIC_LITERAL(self, t):
        r"""\d+(\.\d*)?(e[-+]?\d+)?   #  1, 1., 1.1, 1e10, 1.1e-10, etc.
        |
        \.\d+(e[-+]?\d+)?         #  .1, .1e10, .1e+10
        """
        return t

    # qualified identifiers have one or two dots
    def t_QUALIFIED_IDENTIFIER(self, t):
        r"[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*){1,2}"
        t.type = "QUALIFIED_IDENTIFIER"
        return t

    # we only support ASCII in identifier names
    def t_SIMPLE_IDENTIFIER(self, t):
        r"[a-zA-Z_][a-zA-Z0-9_]*"
        # Check for reserved words and make sure they are upper case
        reserved = self.reserved.get(t.value.upper())
        if reserved is not None:
            t.type = reserved
            t.value = reserved
        else:
            t.type = "SIMPLE_IDENTIFIER"
        return t

    def t_error(self, t):
        "Error handling rule"
        lexer = t.lexer
        raise ParserLexError(lexer.lexdata, t.value, lexer.lexpos, lexer.lineno)

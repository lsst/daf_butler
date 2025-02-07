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

"""Syntax definition for user expression parser."""

from __future__ import annotations

__all__ = ["ParseError", "ParserEOFError", "ParserYacc", "ParserYaccError"]

import functools
import re
import warnings
from typing import Any, Protocol

import astropy.time

# As of astropy 4.2, the erfa interface is shipped independently and
# ErfaWarning is no longer an AstropyWarning
try:
    import erfa
except ImportError:
    erfa = None

from .exprTree import (
    BinaryOp,
    Identifier,
    IsIn,
    Node,
    NumericLiteral,
    Parens,
    RangeLiteral,
    StringLiteral,
    TimeLiteral,
    TupleNode,
    UnaryOp,
    function_call,
)
from .parserLex import LexToken, ParserLex
from .ply import yacc


class YaccProduction(Protocol):
    """Protocol for YaccProduction defined in ``ply.yacc``."""

    lexer: Any

    def __getitem__(self, n: int) -> Any: ...
    def __setitem__(self, n: int, v: Any) -> None: ...
    def __len__(self) -> int: ...
    def lineno(self, n: int) -> int: ...
    def lexpos(self, n: int) -> int: ...


# The purpose of this regex is to guess time format if it is not explicitly
# provided in the string itself
_re_time_str = re.compile(
    r"""
    ((?P<format>\w+)/)?             # optionally prefixed by "format/"
    (?P<value>
        (?P<number>-?(\d+(\.\d*)|(\.\d+)))   # floating point number
        |
        (?P<iso>\d+-\d+-\d+([ T]\d+:\d+(:\d+([.]\d*)?)?)?)   # iso(t) [no timezone]
        |
        (?P<fits>[+]\d+-\d+-\d+(T\d+:\d+:\d+([.]\d*)?)?)   # fits
        |
        (?P<yday>\d+:\d+(:\d+:\d+(:\d+([.]\d*)?)?)?)         # yday
    )
    (/(?P<scale>\w+))?              # optionally followed by "/scale"
    $
""",
    re.VERBOSE | re.IGNORECASE,
)


def _parseTimeString(time_str: str) -> astropy.time.Time:
    """Try to convert time string into astropy.Time.

    Parameters
    ----------
    time_str : `str`
        Input string.

    Returns
    -------
    time : `astropy.time.Time`
       The parsed time.

    Raises
    ------
    ValueError
        Raised if input string has unexpected format.
    """
    # Check for time zone. Python datetime objects can be timezone-aware
    # and if one has been stringified then there will be a +00:00 on the end.
    # Special case UTC. Fail for other timezones.
    time_str = time_str.replace("+00:00", "")

    match = _re_time_str.match(time_str)
    if not match:
        raise ValueError(f'Time string "{time_str}" does not match known formats')

    value, fmt, scale = match.group("value", "format", "scale")
    if fmt is not None:
        fmt = fmt.lower()
        if fmt not in astropy.time.Time.FORMATS:
            raise ValueError(f'Time string "{time_str}" specifies unknown time format "{fmt}"')
    if scale is not None:
        scale = scale.lower()
        if scale not in astropy.time.Time.SCALES:
            raise ValueError(f'Time string "{time_str}" specifies unknown time scale "{scale}"')

    # convert number string to floating point
    if match.group("number") is not None:
        value = float(value)

    # guess format if not given
    if fmt is None:
        if match.group("number") is not None:
            fmt = "mjd"
        elif match.group("iso") is not None:
            if "T" in value or "t" in value:
                fmt = "isot"
            else:
                fmt = "iso"
        elif match.group("fits") is not None:
            fmt = "fits"
        elif match.group("yday") is not None:
            fmt = "yday"
    assert fmt is not None

    # guess scale if not given
    if scale is None:
        if fmt in ("iso", "isot", "fits", "yday", "unix"):
            scale = "utc"
        elif fmt == "cxcsec":
            scale = "tt"
        else:
            scale = "tai"

    try:
        # Hide warnings about future dates
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=astropy.utils.exceptions.AstropyWarning)
            if erfa is not None:
                warnings.simplefilter("ignore", category=erfa.ErfaWarning)
            value = astropy.time.Time(value, format=fmt, scale=scale)
    except ValueError:
        # astropy makes very verbose exception that is not super-useful in
        # many context, just say we don't like it.
        raise ValueError(f'Time string "{time_str}" does not match format "{fmt}"') from None

    return value


# ------------------------
#  Exported definitions --
# ------------------------


class ParserYaccError(Exception):
    """Base class for exceptions generated by parser."""

    pass


class ParseError(ParserYaccError):
    """Exception raised for parsing errors.

    Parameters
    ----------
    expression : `str`
        Full initial expression being parsed.
    token : `str`
        Current token at parsing position.
    pos : `int`
        Current parsing position, offset from beginning of expression in
        characters.
    lineno : `int`
        Current line number in the expression.

    Attributes
    ----------
    expression : `str`
        Full initial expression being parsed.
    token : `str`
        Current token at parsing position.
    pos : `int`
        Current parsing position, offset from beginning of expression in
        characters.
    lineno : `int`
        Current line number in the expression.
    posInLine : `int`
        Parsing position in current line, 0-based.
    """

    def __init__(self, expression: str, token: str, pos: int, lineno: int):
        self.expression = expression
        self.token = token
        self.pos = pos
        self.lineno = lineno
        self.posInLine = self._posInLine()
        msg = "Syntax error at or near '{0}' (line: {1}, pos: {2})"
        msg = msg.format(token, lineno, self.posInLine + 1)
        ParserYaccError.__init__(self, msg)

    def _posInLine(self) -> int:
        """Return position in current line"""
        lines = self.expression.split("\n")
        pos = self.pos
        for line in lines[: self.lineno - 1]:
            # +1 for newline
            pos -= len(line) + 1
        return pos


class ParserEOFError(ParserYaccError):
    """Exception raised for EOF-during-parser."""

    def __init__(self) -> None:
        Exception.__init__(self, "End of input reached while expecting further input")


class ParserYacc:
    """Class which defines PLY grammar.

    Based on MySQL grammar for expressions
    (https://dev.mysql.com/doc/refman/5.7/en/expressions.html).

    Parameters
    ----------
    **kwargs
        Optional keyword arguments that are passed to `yacc.yacc` constructor.
    """

    def __init__(self, **kwargs: Any):
        kw = dict(write_tables=0, debug=False)
        kw.update(kwargs)
        self.parser = self._parser_factory(**kw)

    @staticmethod
    @functools.cache
    def _parser_factory(**kwarg: Any) -> Any:
        """Make parser instance."""
        return yacc.yacc(module=ParserYacc, **kwarg)

    def parse(self, input: str, lexer: Any = None, debug: bool = False, tracking: bool = False) -> Node:
        """Parse input expression ad return parsed tree object.

        This is a trivial wrapper for yacc.LRParser.parse method which
        provides lexer if not given in arguments.

        Parameters
        ----------
        input : `str`
            Expression to parse.
        lexer : `object`, optional
            Lexer instance, if not given then ParserLex.make_lexer() is
            called to create one.
        debug : `bool`, optional
            Set to True for debugging output.
        tracking : `bool`, optional
            Set to True for tracking line numbers in parser.
        """
        # make lexer
        if lexer is None:
            lexer = ParserLex.make_lexer()
        tree = self.parser.parse(input=input, lexer=lexer, debug=debug, tracking=tracking)
        return tree

    tokens = ParserLex.tokens[:]

    precedence = (
        ("left", "OR"),
        ("left", "AND"),
        ("nonassoc", "OVERLAPS"),  # Nonassociative operators
        ("nonassoc", "EQ", "NE"),  # Nonassociative operators
        ("nonassoc", "LT", "LE", "GT", "GE"),  # Nonassociative operators
        ("left", "ADD", "SUB"),
        ("left", "MUL", "DIV", "MOD"),
        ("right", "UPLUS", "UMINUS", "NOT"),  # unary plus and minus
    )

    # this is the starting rule
    @classmethod
    def p_input(cls, p: YaccProduction) -> None:
        """input : expr
        | empty
        """
        p[0] = p[1]

    @classmethod
    def p_empty(cls, p: YaccProduction) -> None:
        """empty :"""
        p[0] = None

    @classmethod
    def p_expr(cls, p: YaccProduction) -> None:
        """expr : expr OR expr
        | expr AND expr
        | NOT expr
        | bool_primary
        """
        if len(p) == 4:
            p[0] = BinaryOp(lhs=p[1], op=p[2].upper(), rhs=p[3])
        elif len(p) == 3:
            p[0] = UnaryOp(op=p[1].upper(), operand=p[2])
        else:
            p[0] = p[1]

    @classmethod
    def p_bool_primary(cls, p: YaccProduction) -> None:
        """bool_primary : bool_primary EQ predicate
        | bool_primary NE predicate
        | bool_primary LT predicate
        | bool_primary LE predicate
        | bool_primary GE predicate
        | bool_primary GT predicate
        | bool_primary OVERLAPS predicate
        | predicate
        """
        if len(p) == 2:
            p[0] = p[1]
        else:
            p[0] = BinaryOp(lhs=p[1], op=p[2], rhs=p[3])

    @classmethod
    def p_predicate(cls, p: YaccProduction) -> None:
        """predicate : bit_expr IN LPAREN literal_or_id_list RPAREN
        | bit_expr NOT IN LPAREN literal_or_id_list RPAREN
        | bit_expr
        """
        if len(p) == 6:
            p[0] = IsIn(lhs=p[1], values=p[4])
        elif len(p) == 7:
            p[0] = IsIn(lhs=p[1], values=p[5], not_in=True)
        else:
            p[0] = p[1]

    @classmethod
    def p_identifier(cls, p: YaccProduction) -> None:
        """identifier : SIMPLE_IDENTIFIER
        | QUALIFIED_IDENTIFIER
        """
        p[0] = Identifier(p[1])

    @classmethod
    def p_literal_or_id_list(cls, p: YaccProduction) -> None:
        """literal_or_id_list : literal_or_id_list COMMA literal
        | literal_or_id_list COMMA identifier
        | literal
        | identifier
        """
        if len(p) == 2:
            p[0] = [p[1]]
        else:
            p[0] = p[1] + [p[3]]

    @classmethod
    def p_bit_expr(cls, p: YaccProduction) -> None:
        """bit_expr : bit_expr ADD bit_expr
        | bit_expr SUB bit_expr
        | bit_expr MUL bit_expr
        | bit_expr DIV bit_expr
        | bit_expr MOD bit_expr
        | simple_expr
        """
        if len(p) == 2:
            p[0] = p[1]
        else:
            p[0] = BinaryOp(lhs=p[1], op=p[2], rhs=p[3])

    @classmethod
    def p_simple_expr_lit(cls, p: YaccProduction) -> None:
        """simple_expr : literal"""
        p[0] = p[1]

    @classmethod
    def p_simple_expr_id(cls, p: YaccProduction) -> None:
        """simple_expr : identifier"""
        p[0] = p[1]

    @classmethod
    def p_simple_expr_function_call(cls, p: YaccProduction) -> None:
        """simple_expr : function_call"""
        p[0] = p[1]

    @classmethod
    def p_simple_expr_unary(cls, p: YaccProduction) -> None:
        """simple_expr : ADD simple_expr %prec UPLUS
        | SUB simple_expr %prec UMINUS
        """
        p[0] = UnaryOp(op=p[1], operand=p[2])

    @classmethod
    def p_simple_expr_paren(cls, p: YaccProduction) -> None:
        """simple_expr : LPAREN expr RPAREN"""
        p[0] = Parens(p[2])

    @classmethod
    def p_simple_expr_tuple(cls, p: YaccProduction) -> None:
        """simple_expr : LPAREN expr COMMA expr RPAREN"""
        # For now we only support tuples with two items,
        # these are used for time ranges.
        p[0] = TupleNode((p[2], p[4]))

    @classmethod
    def p_literal_num(cls, p: YaccProduction) -> None:
        """literal : NUMERIC_LITERAL"""
        p[0] = NumericLiteral(p[1])

    @classmethod
    def p_literal_num_signed(cls, p: YaccProduction) -> None:
        """literal : ADD NUMERIC_LITERAL %prec UPLUS
        | SUB NUMERIC_LITERAL %prec UMINUS
        """
        p[0] = NumericLiteral(p[1] + p[2])

    @classmethod
    def p_literal_str(cls, p: YaccProduction) -> None:
        """literal : STRING_LITERAL"""
        p[0] = StringLiteral(p[1])

    @classmethod
    def p_literal_time(cls, p: YaccProduction) -> None:
        """literal : TIME_LITERAL"""
        try:
            value = _parseTimeString(p[1])
        except ValueError as e:
            raise ParseError(p.lexer.lexdata, p[1], p.lexpos(1), p.lineno(1)) from e
        p[0] = TimeLiteral(value)

    @classmethod
    def p_literal_range(cls, p: YaccProduction) -> None:
        """literal : RANGE_LITERAL"""
        # RANGE_LITERAL value is tuple of three numbers
        start, stop, stride = p[1]
        p[0] = RangeLiteral(start, stop, stride)

    @classmethod
    def p_function_call(cls, p: YaccProduction) -> None:
        """function_call : SIMPLE_IDENTIFIER LPAREN expr_list RPAREN"""
        p[0] = function_call(p[1], p[3])

    @classmethod
    def p_expr_list(cls, p: YaccProduction) -> None:
        """expr_list : expr_list COMMA expr
        | expr
        | empty
        """
        if len(p) == 2:
            if p[1] is None:
                p[0] = []
            else:
                p[0] = [p[1]]
        else:
            p[0] = p[1] + [p[3]]

    # ---------- end of all grammar rules ----------

    # Error rule for syntax errors
    @classmethod
    def p_error(cls, p: LexToken | None) -> None:
        if p is None:
            raise ParserEOFError()
        else:
            raise ParseError(p.lexer.lexdata, p.value, p.lexpos, p.lineno)

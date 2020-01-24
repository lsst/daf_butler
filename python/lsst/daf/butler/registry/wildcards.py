# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
from __future__ import annotations

__all__ = ["Like", "WildcardExpression", "CategorizedWildcard"]


from dataclasses import dataclass
from typing import Any, List, Optional, Sequence, Union

import sqlalchemy

from ..core.utils import iterable


@dataclass(frozen=True)
class Like:
    """Simple wrapper around a string pattern used to indicate that a string is
    a pattern to be used with the SQL ``LIKE`` operator rather than a complete
    name.
    """

    pattern: str
    """The string pattern, in SQL ``LIKE`` syntax.
    """


WildcardExpression = Union[str, Like, Sequence[Union[str, Like]], type(...)]
"""Type annotation alias for the types accepted to describe a search for
entities identified by strings.

The interpretation of the allowed values is:
 - a single `str` indicates an exact match;
 - a `Like` instance provides a pattern to be matched with SQL's ``LIKE``
   operator;
 - a `list` of `str` or `Like` indicates a match against any of those;
 - `...` indicates that any string will match.
"""


@dataclass
class CategorizedWildcard:
    """The results of preprocessing a wildcard expression to separate match
    patterns from strings.

    Should be constructed by calling `categorize` rather than invoking the
    constructor directly.
    """

    @classmethod
    def categorize(cls, expression: Any) -> Optional[CategorizedWildcard]:
        """Categorize a wildcard expression.

        Parameters
        ----------
        expression
            The expression to parse.  If this is one of the types included in
            `WildcardExpression`, the `strings` and `patterns` attributes will
            be populated.  If it is (or is a sequence that contains) other
            types, these will be added to `other`.

        Returns
        -------
        categorized : `CategorizedWildcard` or `None`.
            The struct describing the wildcard.  If ``expression is ...`,
            `None` is returned.
        """
        if expression is ...:
            return None
        self = cls(strings=[], patterns=[], other=[])
        for item in iterable(expression):
            if isinstance(item, Like):
                self.patterns.append(Like.pattern)
            elif isinstance(item, str):
                self.strings.append(item)
            else:
                self.other.append(item)
        return self

    def makeWhereExpression(self, column: sqlalchemy.sql.ColumnElement
                            ) -> Optional[sqlalchemy.sql.ColumnElement]:
        """Transform the wildcard into a SQLAlchemy boolean expression suitable
        for use in a WHERE clause.

        This only makes use of the items in `strings` and `patterns`; anything
        in `others` must be handled separately.

        Parameters
        ----------
        column : `sqlalchemy.sql.ColumnElement`
            A string column in a table or query that should be compared to the
            wildcard expression.

        Returns
        -------
        where : `sqlalchemy.sql.ColumnElement` or `None`
            A boolean SQL expression that evaluates to true if and only if
            the value of ``column`` matches the wildcard.  `None` is returned
            if both `strings` and `patterns` are empty, and hence no match is
            possible.
        """
        terms = []
        if len(self.strings) == 1:
            terms.append(column == self.strings[0])
        elif len(self.strings) > 1:
            terms.append(column.in_(self.strings))
        terms.extend(column.like(pattern) for pattern in self.patterns)
        if not terms:
            return None
        return sqlalchemy.sql.or_(*terms)

    strings: List[str]
    """Explicit string values found in the wildcard, either because it is a
    scalar string or a sequence that contains one or more strings.
    """

    patterns: List[str]
    """The `Like.pattern` attributes of any `Like` instances found in the
    wildcard, either because it is a scalar `Like` or a sequence that contains
    one or more `Like` instances.
    """

    other: List[Any]
    """Any objects in the wildcard that are not `str` or `Like` instances
    or iterables.
    """

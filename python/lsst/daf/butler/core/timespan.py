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

__all__ = ("Timespan", "TIMESPAN_FIELD_SPECS", "TIMESPAN_MIN", "TIMESPAN_MAX")

import operator
from typing import Any, Generic, Optional, TypeVar

from . import ddl, time_utils


TIMESPAN_MIN = time_utils.EPOCH
TIMESPAN_MAX = time_utils.MAX_TIME

T = TypeVar("T")


class BaseTimespan(Generic[T], tuple):
    """A generic 2-element named tuple for time intervals.

    For cases where either or both bounds may be `None` to represent
    infinite, the `Timespan` specialization should be preferred over using
    this class directly.

    Parameters
    ----------
    begin : ``T``
        Minimum timestamp in the interval (inclusive).  `None` is interpreted
        as -infinity (if allowed by ``T``).
    end : ``T``
        Maximum timestamp in the interval (inclusive).  `None` is interpreted
        as +infinity (if allowed by ``T``).

    Notes
    -----
    This class is generic because it is used for both Python literals (with
    ``T == astropy.time.Time``) and timestamps in SQLAlchemy expressions
    (with ``T == sqlalchemy.sql.ColumnElement``), including operations between
    those.

    Python's built-in `collections.namedtuple` is not actually a type (just
    a factory for types), and `typing.NamedTuple` doesn't support generics,
    so neither can be used here (but also wouldn't add much even if they
    could).
    """

    def __new__(cls, begin: T, end: T) -> BaseTimespan:
        return tuple.__new__(cls, (begin, end))

    @property
    def begin(self) -> T:
        """Minimum timestamp in the interval (inclusive).

        `None` should be interpreted as -infinity.
        """
        return self[0]

    @property
    def end(self) -> T:
        """Maximum timestamp in the interval (inclusive).

        `None` should be interpreted as +infinity.
        """
        return self[1]

    def __getnewargs__(self) -> tuple:
        return (self.begin, self.end)


class Timespan(BaseTimespan[Optional[T]]):
    """A generic 2-element named tuple for time intervals.

    `Timespan` explicitly marks both its start and end bounds as possibly
    `None` (signifying infinite bounds), and provides operations that take
    that into account.
    """

    def overlaps(self, other: Timespan[Any], ops: Any = operator) -> Any:
        """Test whether this timespan overlaps another.

        Parameters
        ----------
        other : `Timespan`
            Another timespan whose begin and end values can be compared with
            those of ``self`` with the ``>=`` operator, yielding values
            that can be passed to ``ops.or_`` and/or ``ops.and_``.
        ops : `Any`, optional
            Any object with ``and_`` and ``or_`` boolean operators.  Defaults
            to the Python built-in `operator` module, which is appropriate when
            ``T`` is a Python literal like `astropy.time.Time`.  When either
            operand contains SQLAlchemy column expressions, the
            `sqlalchemy.sql` module should be used instead.

        Returns
        -------
        overlaps : `Any`
            The result of the overlap.  When ``ops`` is `operator`, this will
            be a `bool`.  If ``ops`` is `sqlachemy.sql`, it will be a boolean
            column expression.
        """
        # Silence flake8 below because we use "== None" to invoke SQLAlchemy
        # operator overloads.
        # Silence mypy below because this whole method is very much dynamically
        # typed.
        return ops.and_(
            ops.or_(
                self.end == None,  # noqa: E711
                other.begin == None,  # noqa: E711
                self.end >= other.begin,  # type: ignore
            ),
            ops.or_(
                self.begin == None,  # noqa: E711
                other.end == None,  # noqa: E711
                other.end >= self.begin,  # type: ignore
            ),
        )

    def intersection(*args: Timespan[Any]) -> Optional[Timespan]:
        """Return a new `Timespan` that is contained by all of the given ones.

        Parameters
        ----------
        *args
            All positional arguments are `Timespan` instances.

        Returns
        -------
        intersection : `Timespan` or `None`
            The intersection timespan, or `None`, if there is no intersection
            or no arguments.

        Notes
        -----
        Unlike `overlaps`, this method does not support SQLAlchemy column
        expressions as operands.
        """
        if len(args) == 0:
            return None
        elif len(args) == 1:
            return args[0]
        else:
            begins = [ts.begin for ts in args if ts.begin is not None]
            ends = [ts.end for ts in args if ts.end is not None]
            if not begins:
                begin = None
            elif len(begins) == 1:
                begin = begins[0]
            else:
                begin = max(*begins)
            if not ends:
                end = None
            elif len(ends) == 1:
                end = ends[0]
            else:
                end = min(*ends) if ends else None
            if begin is not None and end is not None and begin > end:
                return None
            return Timespan(begin=begin, end=end)


# For timestamps we use Unix time in nanoseconds in TAI scale which need
# 64-bit integer,
TIMESPAN_FIELD_SPECS: BaseTimespan[ddl.FieldSpec] = BaseTimespan(
    begin=ddl.FieldSpec(name="datetime_begin", dtype=ddl.AstropyTimeNsecTai),
    end=ddl.FieldSpec(name="datetime_end", dtype=ddl.AstropyTimeNsecTai),
)

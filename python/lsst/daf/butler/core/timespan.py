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

__all__ = (
    "Timespan",
    "DatabaseTimespanRepresentation"
)

from abc import ABC, abstractmethod
from typing import Any, ClassVar, Dict, Iterator, Mapping, NamedTuple, Optional, Tuple, Type, TypeVar, Union

import astropy.time
import sqlalchemy

from . import ddl
from .time_utils import astropy_to_nsec, EPOCH, MAX_TIME, times_equal


S = TypeVar("S", bound="DatabaseTimespanRepresentation")


class Timespan(NamedTuple):
    """A 2-element named tuple for time intervals.

    Parameters
    ----------
    begin : ``Timespan``
        Minimum timestamp in the interval (inclusive).  `None` is interpreted
        as -infinity.
    end : ``Timespan``
        Maximum timestamp in the interval (exclusive).  `None` is interpreted
        as +infinity.
    """

    begin: Optional[astropy.time.Time]
    """Minimum timestamp in the interval (inclusive).

    `None` should be interpreted as -infinity.
    """

    end: Optional[astropy.time.Time]
    """Maximum timestamp in the interval (exclusive).

    `None` should be interpreted as +infinity.
    """

    def __str__(self) -> str:
        if self.begin is None:
            head = "(-∞, "
        else:
            head = f"[{self.begin}, "
        if self.end is None:
            tail = "∞)"
        else:
            tail = f"{self.end})"
        return head + tail

    def __repr__(self) -> str:
        # astropy.time.Time doesn't have an eval-friendly __repr__, so we
        # simulate our own here to make Timespan's __repr__ eval-friendly.
        tmpl = "astropy.time.Time('{t}', scale='{t.scale}', format='{t.format}')"
        begin = tmpl.format(t=self.begin) if self.begin is not None else None
        end = tmpl.format(t=self.end) if self.end is not None else None
        return f"Timespan(begin={begin}, end={end})"

    def overlaps(self, other: Timespan) -> Any:
        """Test whether this timespan overlaps another.

        Parameters
        ----------
        other : `Timespan`
            Another timespan whose begin and end values can be compared with
            those of ``self`` with the ``>=`` operator, yielding values
            that can be passed to ``ops.or_`` and/or ``ops.and_``.

        Returns
        -------
        overlaps : `Any`
            The result of the overlap.  When ``ops`` is `operator`, this will
            be a `bool`.  If ``ops`` is `sqlachemy.sql`, it will be a boolean
            column expression.
        """
        return (
            (self.end is None or other.begin is None or self.end > other.begin)
            and (self.begin is None or other.end is None or other.end > self.begin)
        )

    def intersection(*args: Timespan) -> Optional[Timespan]:
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
            if begin is not None and end is not None and begin >= end:
                return None
            return Timespan(begin=begin, end=end)

    def difference(self, other: Timespan) -> Iterator[Timespan]:
        """Return the one or two timespans that cover the interval(s) that are
        in ``self`` but not ``other``.

        This is implemented as an iterator because the result may be zero, one,
        or two `Timespan` objects, depending on the relationship between the
        operands.

        Parameters
        ----------
        other : `Timespan`
            Timespan to subtract.

        Yields
        ------
        result : `Timespan`
            A `Timespan` that is contained by ``self`` but does not overlap
            ``other``.
        """
        if other.begin is not None:
            if self.begin is None or self.begin < other.begin:
                if self.end is not None and self.end < other.begin:
                    yield self
                else:
                    yield Timespan(begin=self.begin, end=other.begin)
        if other.end is not None:
            if self.end is None or self.end > other.end:
                if self.begin is not None and self.begin > other.end:
                    yield self
                else:
                    yield Timespan(begin=other.end, end=self.end)


class DatabaseTimespanRepresentation(ABC):
    """An interface that encapsulates how timespans are represented in a
    database engine.

    Most of this class's interface is comprised of classmethods.  Instances
    can be constructed via the `fromSelectable` method as a way to include
    timespan overlap operations in query JOIN or WHERE clauses.
    """

    NAME: ClassVar[str] = "timespan"
    """Base name for all timespan fields in the database (`str`).

    Actual field names may be derived from this, rather than exactly this.
    """

    Compound: ClassVar[Type[DatabaseTimespanRepresentation]]
    """A concrete subclass of `DatabaseTimespanRepresentation` that simply
    uses two separate fields for the begin (inclusive) and end (excusive)
    endpoints.

    This implementation should be compatibly with any SQL database, and should
    generally be used when a database-specific implementation is not available.
    """

    __slots__ = ()

    @classmethod
    @abstractmethod
    def makeFieldSpecs(cls, nullable: bool, **kwargs: Any) -> Tuple[ddl.FieldSpec, ...]:
        """Make one or more `ddl.FieldSpec` objects that reflect the fields
        that must be added to a table for this representation.

        Parameters
        ----------
        nullable : `bool`
            If `True`, the timespan is permitted to be logically ``NULL``
            (mapped to `None` in Python), though the correspoding value(s) in
            the database are implementation-defined.  Nullable timespan fields
            default to NULL, while others default to (-∞, ∞).
        **kwargs
            Keyword arguments are forwarded to the `ddl.FieldSpec` constructor
            for all fields; implementations only provide the ``name``,
            ``dtype``, and ``default`` arguments themselves.

        Returns
        -------
        specs : `tuple` [ `ddl.FieldSpec` ]
            Field specification objects; length of the tuple is
            subclass-dependent, but is guaranteed to match the length of the
            return values of `getFieldNames` and `update`.
        """
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def getFieldNames(cls) -> Tuple[str, ...]:
        """Return the actual field names used by this representation.

        Returns
        -------
        names : `tuple` [ `str` ]
            Field name(s).  Guaranteed to be the same as the names of the field
            specifications returned by `makeFieldSpecs`.
        """
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def update(cls, timespan: Optional[Timespan], *,
               result: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Add a `Timespan` to a dictionary that represents a database row
        in this representation.

        Parameters
        ----------
        timespan : `Timespan`, optional
            A concrete timespan.
        result : `dict` [ `str`, `Any` ], optional
            A dictionary representing a database row that fields should be
            added to, or `None` to create and return a new one.

        Returns
        -------
        result : `dict` [ `str`, `Any` ]
            A dictionary containing this representation of a timespan.  Exactly
            the `dict` passed as ``result`` if that is not `None`.
        """
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def extract(cls, mapping: Mapping[str, Any]) -> Optional[Timespan]:
        """Extract a `Timespan` instance from a dictionary that represents a
        database row in this representation.

        Parameters
        ----------
        mapping : `Mapping` [ `str`, `Any` ]
            A dictionary representing a database row containing a `Timespan`
            in this representation.  Should have key(s) equal to the return
            value of `getFieldNames`.

        Returns
        -------
        timespan : `Timespan` or `None`
            Python representation of the timespan.
        """
        raise NotImplementedError()

    @classmethod
    def hasExclusionConstraint(cls) -> bool:
        """Return `True` if this representation supports exclusion constraints.

        Returns
        -------
        supported : `bool`
            If `True`, defining a constraint via `ddl.TableSpec.exclusion` that
            includes the fields of this representation is allowed.
        """
        return False

    @classmethod
    @abstractmethod
    def fromSelectable(cls: Type[S], selectable: sqlalchemy.sql.FromClause) -> S:
        """Construct an instance of this class that proxies the columns of
        this representation in a table or SELECT query.

        Parameters
        ----------
        selectable : `sqlalchemy.sql.FromClause`
            SQLAlchemy object representing a table or SELECT query that has
            columns in this representation.

        Returns
        -------
        instance : `DatabaseTimespanRepresentation`
            An instance of this representation subclass.
        """
        raise NotImplementedError()

    @abstractmethod
    def isNull(self) -> sqlalchemy.sql.ColumnElement:
        """Return a SQLAlchemy expression that tests whether this timespan is
        logically ``NULL``.

        Returns
        -------
        isnull : `sqlalchemy.sql.ColumnElement`
            A boolean SQLAlchemy expression object.
        """
        raise NotImplementedError()

    @abstractmethod
    def overlaps(self: S, other: Union[Timespan, S]) -> sqlalchemy.sql.ColumnElement:
        """Return a SQLAlchemy expression representing an overlap operation on
        timespans.

        Parameters
        ----------
        other : `Timespan` or `DatabaseTimespanRepresentation`
            The timespan to overlap ``self`` with; either a Python `Timespan`
            literal or an instance of the same `DatabaseTimespanRepresentation`
            as ``self``, representing a timespan in some other table or query
            within the same database.

        Returns
        -------
        overlap : `sqlalchemy.sql.ColumnElement`
            A boolean SQLAlchemy expression object.
        """
        raise NotImplementedError()


class _CompoundDatabaseTimespanRepresentation(DatabaseTimespanRepresentation):
    """An implementation of `DatabaseTimespanRepresentation` that simply stores
    the endpoints in two separate fields.

    This type should generally be accessed via
    `DatabaseTimespanRepresentation.Compound`, and should be constructed only
    via the `fromSelectable` method.

    Parameters
    ----------
    begin : `sqlalchemy.sql.ColumnElement`
        SQLAlchemy object representing the begin (inclusive) endpoint.
    end : `sqlalchemy.sql.ColumnElement`
        SQLAlchemy object representing the end (exclusive) endpoint.

    Notes
    -----
    ``NULL`` timespans are represented by having both fields set to ``NULL``;
    setting only one to ``NULL`` is considered a corrupted state that should
    only be possible if this interface is circumvented.  `Timespan` instances
    with one or both of `~Timespan.begin` and `~Timespan.end` set to `None`
    are set to fields mapped to the minimum and maximum value constants used
    by our integer-time mapping.
    """
    def __init__(self, begin: sqlalchemy.sql.ColumnElement, end: sqlalchemy.sql.ColumnElement):
        self.begin = begin
        self.end = end

    __slots__ = ("begin", "end")

    @classmethod
    def makeFieldSpecs(cls, nullable: bool, **kwargs: Any) -> Tuple[ddl.FieldSpec, ...]:
        # Docstring inherited.
        return (
            ddl.FieldSpec(
                f"{cls.NAME}_begin", dtype=ddl.AstropyTimeNsecTai, nullable=nullable,
                default=(None if nullable else sqlalchemy.sql.text(str(astropy_to_nsec(EPOCH)))),
                **kwargs,
            ),
            ddl.FieldSpec(
                f"{cls.NAME}_end", dtype=ddl.AstropyTimeNsecTai, nullable=nullable,
                default=(None if nullable else sqlalchemy.sql.text(str(astropy_to_nsec(MAX_TIME)))),
                **kwargs,
            ),
        )

    @classmethod
    def getFieldNames(cls) -> Tuple[str, ...]:
        # Docstring inherited.
        return (f"{cls.NAME}_begin", f"{cls.NAME}_end")

    @classmethod
    def update(cls, timespan: Optional[Timespan], *,
               result: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        # Docstring inherited.
        if result is None:
            result = {}
        if timespan is None:
            begin = None
            end = None
        else:
            if timespan.begin is None or timespan.begin < EPOCH:
                begin = EPOCH
            else:
                begin = timespan.begin
            if timespan.end is None or timespan.end > MAX_TIME:
                end = MAX_TIME
            else:
                end = timespan.end
        result[f"{cls.NAME}_begin"] = begin
        result[f"{cls.NAME}_end"] = end
        return result

    @classmethod
    def extract(cls, mapping: Mapping[str, Any]) -> Optional[Timespan]:
        # Docstring inherited.
        begin = mapping[f"{cls.NAME}_begin"]
        end = mapping[f"{cls.NAME}_end"]
        if begin is None:
            if end is not None:
                raise RuntimeError(
                    f"Corrupted timespan extracted: begin is NULL, but end is {end}."
                )
            return None
        elif end is None:
            raise RuntimeError(
                f"Corrupted timespan extracted: end is NULL, but begin is {begin}."
            )
        if times_equal(begin, EPOCH):
            begin = None
        elif begin < EPOCH:
            raise RuntimeError(
                f"Corrupted timespan extracted: begin ({begin}) is before {EPOCH}."
            )
        if times_equal(end, MAX_TIME):
            end = None
        elif end > MAX_TIME:
            raise RuntimeError(
                f"Corrupted timespan extracted: end ({end}) is after {MAX_TIME}."
            )
        return Timespan(begin=begin, end=end)

    @classmethod
    def fromSelectable(cls, selectable: sqlalchemy.sql.FromClause) -> _CompoundDatabaseTimespanRepresentation:
        # Docstring inherited.
        return cls(begin=selectable.columns[f"{cls.NAME}_begin"],
                   end=selectable.columns[f"{cls.NAME}_end"])

    def isNull(self) -> sqlalchemy.sql.ColumnElement:
        # Docstring inherited.
        return self.begin.is_(None)

    def overlaps(self, other: Union[Timespan, _CompoundDatabaseTimespanRepresentation]
                 ) -> sqlalchemy.sql.ColumnElement:
        # Docstring inherited.
        if isinstance(other, Timespan):
            begin = EPOCH if other.begin is None else other.begin
            end = MAX_TIME if other.end is None else other.end
        elif isinstance(other, _CompoundDatabaseTimespanRepresentation):
            begin = other.begin
            end = other.begin
        else:
            raise TypeError(f"Unexpected argument to overlaps: {other!r}.")
        return sqlalchemy.sql.and_(self.end > begin, end > self.begin)


DatabaseTimespanRepresentation.Compound = _CompoundDatabaseTimespanRepresentation

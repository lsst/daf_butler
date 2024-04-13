# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
from __future__ import annotations

__all__ = ("TimespanDatabaseRepresentation",)

from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, ClassVar, TypeVar

import sqlalchemy

# As of astropy 4.2, the erfa interface is shipped independently and
# ErfaWarning is no longer an AstropyWarning
try:
    import erfa
except ImportError:
    erfa = None


from . import ddl
from ._timespan import Timespan
from .time_utils import TimeConverter

if TYPE_CHECKING:  # Imports needed only for type annotations; may be circular.
    pass


_S = TypeVar("_S", bound="TimespanDatabaseRepresentation")


class TimespanDatabaseRepresentation(ABC):
    """An interface for representing a timespan in a database.

    Notes
    -----
    Much of this class's interface is comprised of classmethods.  Instances
    can be constructed via the `from_columns` or `fromLiteral` methods as a
    way to include timespan overlap operations in query JOIN or WHERE clauses.

    `TimespanDatabaseRepresentation` implementations are guaranteed to use the
    same interval definitions and edge-case behavior as the `Timespan` class.
    They are also guaranteed to round-trip `Timespan` instances exactly.
    """

    NAME: ClassVar[str] = "timespan"

    Compound: ClassVar[type[TimespanDatabaseRepresentation]]
    """A concrete subclass of `TimespanDatabaseRepresentation` that simply
    uses two separate fields for the begin (inclusive) and end (exclusive)
    endpoints.

    This implementation should be compatible with any SQL database, and should
    generally be used when a database-specific implementation is not available.
    """

    __slots__ = ()

    @classmethod
    @abstractmethod
    def makeFieldSpecs(
        cls, nullable: bool, name: str | None = None, **kwargs: Any
    ) -> tuple[ddl.FieldSpec, ...]:
        """Make objects that reflect the fields that must be added to table.

        Makes one or more `ddl.FieldSpec` objects that reflect the fields
        that must be added to a table for this representation.

        Parameters
        ----------
        nullable : `bool`
            If `True`, the timespan is permitted to be logically ``NULL``
            (mapped to `None` in Python), though the corresponding value(s) in
            the database are implementation-defined.  Nullable timespan fields
            default to NULL, while others default to (-∞, ∞).
        name : `str`, optional
            Name for the logical column; a part of the name for multi-column
            representations.  Defaults to ``cls.NAME``.
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
    def getFieldNames(cls, name: str | None = None) -> tuple[str, ...]:
        """Return the actual field names used by this representation.

        Parameters
        ----------
        name : `str`, optional
            Name for the logical column; a part of the name for multi-column
            representations.  Defaults to ``cls.NAME``.

        Returns
        -------
        names : `tuple` [ `str` ]
            Field name(s).  Guaranteed to be the same as the names of the field
            specifications returned by `makeFieldSpecs`.
        """
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def fromLiteral(cls: type[_S], timespan: Timespan | None) -> _S:
        """Construct a database timespan from a literal `Timespan` instance.

        Parameters
        ----------
        timespan : `Timespan` or `None`
            Literal timespan to convert, or `None` to make logically ``NULL``
            timespan.

        Returns
        -------
        tsRepr : `TimespanDatabaseRepresentation`
            A timespan expression object backed by `sqlalchemy.sql.literal`
            column expressions.
        """
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def from_columns(cls: type[_S], columns: sqlalchemy.sql.ColumnCollection, name: str | None = None) -> _S:
        """Construct a database timespan from the columns of a table or
        subquery.

        Parameters
        ----------
        columns : `sqlalchemy.sql.ColumnCollections`
            SQLAlchemy container for raw columns.
        name : `str`, optional
            Name for the logical column; a part of the name for multi-column
            representations.  Defaults to ``cls.NAME``.

        Returns
        -------
        tsRepr : `TimespanDatabaseRepresentation`
            A timespan expression object backed by `sqlalchemy.sql.literal`
            column expressions.
        """
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def update(
        cls, timespan: Timespan | None, name: str | None = None, result: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Add a timespan value to a dictionary that represents a database row.

        Parameters
        ----------
        timespan : `Timespan` or `None`
            A timespan literal, or `None` for ``NULL``.
        name : `str`, optional
            Name for the logical column; a part of the name for multi-column
            representations.  Defaults to ``cls.NAME``.
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
    def extract(cls, mapping: Mapping[Any, Any], name: str | None = None) -> Timespan | None:
        """Extract a timespan from a dictionary that represents a database row.

        Parameters
        ----------
        mapping : `~collections.abc.Mapping` [ `Any`, `Any` ]
            A dictionary representing a database row containing a `Timespan`
            in this representation.  Should have key(s) equal to the return
            value of `getFieldNames`.
        name : `str`, optional
            Name for the logical column; a part of the name for multi-column
            representations.  Defaults to ``cls.NAME``.

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

    @property
    @abstractmethod
    def name(self) -> str:
        """Return base logical name for the timespan column or expression
        (`str`).

        If the representation uses only one actual column, this should be the
        full name of the column.  In other cases it is an unspecified
        common subset of the column names.
        """
        raise NotImplementedError()

    @abstractmethod
    def isNull(self) -> sqlalchemy.sql.ColumnElement:
        """Return expression that tests whether the timespan is ``NULL``.

        Returns a SQLAlchemy expression that tests whether this region is
        logically ``NULL``.

        Returns
        -------
        isnull : `sqlalchemy.sql.ColumnElement`
            A boolean SQLAlchemy expression object.
        """
        raise NotImplementedError()

    @abstractmethod
    def flatten(self, name: str | None = None) -> tuple[sqlalchemy.sql.ColumnElement, ...]:
        """Return the actual column(s) that comprise this logical column.

        Parameters
        ----------
        name : `str`, optional
            If provided, a name for the logical column that should be used to
            label the columns.  If not provided, the columns' native names will
            be used.

        Returns
        -------
        columns : `tuple` [ `sqlalchemy.sql.ColumnElement` ]
            The true column or columns that back this object.
        """
        raise NotImplementedError()

    @abstractmethod
    def isEmpty(self) -> sqlalchemy.sql.ColumnElement:
        """Return a boolean SQLAlchemy expression for testing empty timespans.

        Returns
        -------
        empty : `sqlalchemy.sql.ColumnElement`
            A boolean SQLAlchemy expression object.
        """
        raise NotImplementedError()

    @abstractmethod
    def __lt__(self: _S, other: _S | sqlalchemy.sql.ColumnElement) -> sqlalchemy.sql.ColumnElement:
        """Return SQLAlchemy expression for testing less than.

        Returns a SQLAlchemy expression representing a test for whether an
        in-database timespan is strictly less than another timespan or a time
        point.

        Parameters
        ----------
        other : ``type(self)`` or `sqlalchemy.sql.ColumnElement`
            The timespan or time to relate to ``self``; either an instance of
            the same `TimespanDatabaseRepresentation` subclass as ``self``, or
            a SQL column expression representing an `astropy.time.Time`.

        Returns
        -------
        less : `sqlalchemy.sql.ColumnElement`
            A boolean SQLAlchemy expression object.

        Notes
        -----
        See `Timespan.__lt__` for edge-case behavior.
        """
        raise NotImplementedError()

    @abstractmethod
    def __gt__(self: _S, other: _S | sqlalchemy.sql.ColumnElement) -> sqlalchemy.sql.ColumnElement:
        """Return a SQLAlchemy expression for testing greater than.

        Returns a SQLAlchemy expression representing a test for whether an
        in-database timespan is strictly greater than another timespan or a
        time point.

        Parameters
        ----------
        other : ``type(self)`` or `sqlalchemy.sql.ColumnElement`
            The timespan or time to relate to ``self``; either an instance of
            the same `TimespanDatabaseRepresentation` subclass as ``self``, or
            a SQL column expression representing an `astropy.time.Time`.

        Returns
        -------
        greater : `sqlalchemy.sql.ColumnElement`
            A boolean SQLAlchemy expression object.

        Notes
        -----
        See `Timespan.__gt__` for edge-case behavior.
        """
        raise NotImplementedError()

    @abstractmethod
    def overlaps(self: _S, other: _S | sqlalchemy.sql.ColumnElement) -> sqlalchemy.sql.ColumnElement:
        """Return a SQLAlchemy expression representing timespan overlaps.

        Parameters
        ----------
        other : ``type(self)``
            The timespan or time to overlap ``self`` with.  If a single time,
            this is a synonym for `contains`.

        Returns
        -------
        overlap : `sqlalchemy.sql.ColumnElement`
            A boolean SQLAlchemy expression object.

        Notes
        -----
        See `Timespan.overlaps` for edge-case behavior.
        """
        raise NotImplementedError()

    @abstractmethod
    def contains(self: _S, other: _S | sqlalchemy.sql.ColumnElement) -> sqlalchemy.sql.ColumnElement:
        """Return a SQLAlchemy expression representing containment.

        Returns a test for whether an in-database timespan contains another
        timespan or a time point.

        Parameters
        ----------
        other : ``type(self)`` or `sqlalchemy.sql.ColumnElement`
            The timespan or time to relate to ``self``; either an instance of
            the same `TimespanDatabaseRepresentation` subclass as ``self``, or
            a SQL column expression representing an `astropy.time.Time`.

        Returns
        -------
        contains : `sqlalchemy.sql.ColumnElement`
            A boolean SQLAlchemy expression object.

        Notes
        -----
        See `Timespan.contains` for edge-case behavior.
        """
        raise NotImplementedError()

    @abstractmethod
    def lower(self: _S) -> sqlalchemy.sql.ColumnElement:
        """Return a SQLAlchemy expression representing a lower bound of a
        timespan.

        Returns
        -------
        lower : `sqlalchemy.sql.ColumnElement`
            A SQLAlchemy expression for a lower bound.

        Notes
        -----
        If database holds ``NULL`` for a timespan then the returned expression
        should evaluate to 0. Main purpose of this and `upper` method is to use
        them in generating SQL, in particular ORDER BY clause, to guarantee a
        predictable ordering. It may potentially be used for transforming
        boolean user expressions into SQL, but it will likely require extra
        attention to ordering issues.
        """
        raise NotImplementedError()

    @abstractmethod
    def upper(self: _S) -> sqlalchemy.sql.ColumnElement:
        """Return a SQLAlchemy expression representing an upper bound of a
        timespan.

        Returns
        -------
        upper : `sqlalchemy.sql.ColumnElement`
            A SQLAlchemy expression for an upper bound.

        Notes
        -----
        If database holds ``NULL`` for a timespan then the returned expression
        should evaluate to 0. Also see notes for `lower` method.
        """
        raise NotImplementedError()

    @abstractmethod
    def apply_any_aggregate(
        self, func: Callable[[sqlalchemy.ColumnElement[Any]], sqlalchemy.ColumnElement[Any]]
    ) -> TimespanDatabaseRepresentation:
        """Apply the given ANY_VALUE aggregate function (or equivalent) to
        the timespan column(s).

        Parameters
        ----------
        func : `~collections.abc.Callable`
            Callable that takes a `sqlalchemy.ColumnElement` and returns a
            `sqlalchemy.ColumnElement`.

        Returns
        -------
        timespan : `TimespanDatabaseRepresentation`
            A timespan database representation usable in the SELECT clause of
            a query with GROUP BY where it does not matter which of the grouped
            values is selected.
        """
        raise NotImplementedError()


class _CompoundTimespanDatabaseRepresentation(TimespanDatabaseRepresentation):
    """Representation of a time span as two separate fields.

    An implementation of `TimespanDatabaseRepresentation` that simply stores
    the endpoints in two separate fields.

    This type should generally be accessed via
    `TimespanDatabaseRepresentation.Compound`, and should be constructed only
    via the `from_columns` and `fromLiteral` methods.

    Parameters
    ----------
    nsec : `tuple` of `sqlalchemy.sql.ColumnElement`
        Tuple of SQLAlchemy objects representing the lower (inclusive) and
        upper (exclusive) bounds, as 64-bit integer columns containing
        nanoseconds.
    name : `str`, optional
        Name for the logical column; a part of the name for multi-column
        representations.  Defaults to ``cls.NAME``.

    Notes
    -----
    ``NULL`` timespans are represented by having both fields set to ``NULL``;
    setting only one to ``NULL`` is considered a corrupted state that should
    only be possible if this interface is circumvented.  `Timespan` instances
    with one or both of `~Timespan.begin` and `~Timespan.end` set to `None`
    are set to fields mapped to the minimum and maximum value constants used
    by our integer-time mapping.
    """

    def __init__(self, nsec: tuple[sqlalchemy.sql.ColumnElement, sqlalchemy.sql.ColumnElement], name: str):
        self._nsec = nsec
        self._name = name

    __slots__ = ("_nsec", "_name")

    @classmethod
    def makeFieldSpecs(
        cls, nullable: bool, name: str | None = None, **kwargs: Any
    ) -> tuple[ddl.FieldSpec, ...]:
        # Docstring inherited.
        if name is None:
            name = cls.NAME
        return (
            ddl.FieldSpec(
                f"{name}_begin",
                dtype=sqlalchemy.BigInteger,
                nullable=nullable,
                default=(None if nullable else sqlalchemy.sql.text(str(TimeConverter().min_nsec))),
                **kwargs,
            ),
            ddl.FieldSpec(
                f"{name}_end",
                dtype=sqlalchemy.BigInteger,
                nullable=nullable,
                default=(None if nullable else sqlalchemy.sql.text(str(TimeConverter().max_nsec))),
                **kwargs,
            ),
        )

    @classmethod
    def getFieldNames(cls, name: str | None = None) -> tuple[str, ...]:
        # Docstring inherited.
        if name is None:
            name = cls.NAME
        return (f"{name}_begin", f"{name}_end")

    @classmethod
    def update(
        cls, extent: Timespan | None, name: str | None = None, result: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        # Docstring inherited.
        if name is None:
            name = cls.NAME
        if result is None:
            result = {}
        if extent is None:
            begin_nsec = None
            end_nsec = None
        else:
            begin_nsec = extent.nsec[0]
            end_nsec = extent.nsec[1]
        result[f"{name}_begin"] = begin_nsec
        result[f"{name}_end"] = end_nsec
        return result

    @classmethod
    def extract(cls, mapping: Mapping[str, Any], name: str | None = None) -> Timespan | None:
        # Docstring inherited.
        if name is None:
            name = cls.NAME
        begin_nsec = mapping[f"{name}_begin"]
        end_nsec = mapping[f"{name}_end"]
        if begin_nsec is None:
            if end_nsec is not None:
                raise RuntimeError(
                    f"Corrupted timespan extracted: begin is NULL, but end is {end_nsec}ns -> "
                    f"{TimeConverter().nsec_to_astropy(end_nsec).tai.isot}."
                )
            return None
        elif end_nsec is None:
            raise RuntimeError(
                f"Corrupted timespan extracted: end is NULL, but begin is {begin_nsec}ns -> "
                f"{TimeConverter().nsec_to_astropy(begin_nsec).tai.isot}."
            )
        return Timespan(None, None, _nsec=(begin_nsec, end_nsec))

    @classmethod
    def from_columns(
        cls, columns: sqlalchemy.sql.ColumnCollection, name: str | None = None
    ) -> _CompoundTimespanDatabaseRepresentation:
        # Docstring inherited.
        if name is None:
            name = cls.NAME
        return cls(nsec=(columns[f"{name}_begin"], columns[f"{name}_end"]), name=name)

    @classmethod
    def fromLiteral(cls, timespan: Timespan | None) -> _CompoundTimespanDatabaseRepresentation:
        # Docstring inherited.
        if timespan is None:
            return cls(nsec=(sqlalchemy.sql.null(), sqlalchemy.sql.null()), name=cls.NAME)
        return cls(
            nsec=(sqlalchemy.sql.literal(timespan.nsec[0]), sqlalchemy.sql.literal(timespan.nsec[1])),
            name=cls.NAME,
        )

    @property
    def name(self) -> str:
        # Docstring inherited.
        return self._name

    def isNull(self) -> sqlalchemy.sql.ColumnElement:
        # Docstring inherited.
        return self._nsec[0].is_(None)

    def isEmpty(self) -> sqlalchemy.sql.ColumnElement:
        # Docstring inherited.
        return self._nsec[0] >= self._nsec[1]

    def __lt__(
        self, other: _CompoundTimespanDatabaseRepresentation | sqlalchemy.sql.ColumnElement
    ) -> sqlalchemy.sql.ColumnElement:
        # Docstring inherited.
        # See comments in Timespan.__lt__ for why we use these exact
        # expressions.
        if isinstance(other, sqlalchemy.sql.ColumnElement):
            return sqlalchemy.sql.and_(self._nsec[1] <= other, self._nsec[0] < other)
        else:
            return sqlalchemy.sql.and_(self._nsec[1] <= other._nsec[0], self._nsec[0] < other._nsec[1])

    def __gt__(
        self, other: _CompoundTimespanDatabaseRepresentation | sqlalchemy.sql.ColumnElement
    ) -> sqlalchemy.sql.ColumnElement:
        # Docstring inherited.
        # See comments in Timespan.__gt__ for why we use these exact
        # expressions.
        if isinstance(other, sqlalchemy.sql.ColumnElement):
            return sqlalchemy.sql.and_(self._nsec[0] > other, self._nsec[1] > other)
        else:
            return sqlalchemy.sql.and_(self._nsec[0] >= other._nsec[1], self._nsec[1] > other._nsec[0])

    def overlaps(
        self, other: _CompoundTimespanDatabaseRepresentation | sqlalchemy.sql.ColumnElement
    ) -> sqlalchemy.sql.ColumnElement:
        # Docstring inherited.
        if isinstance(other, sqlalchemy.sql.ColumnElement):
            return self.contains(other)
        return sqlalchemy.sql.and_(self._nsec[1] > other._nsec[0], other._nsec[1] > self._nsec[0])

    def contains(
        self, other: _CompoundTimespanDatabaseRepresentation | sqlalchemy.sql.ColumnElement
    ) -> sqlalchemy.sql.ColumnElement:
        # Docstring inherited.
        if isinstance(other, sqlalchemy.sql.ColumnElement):
            return sqlalchemy.sql.and_(self._nsec[0] <= other, self._nsec[1] > other)
        else:
            return sqlalchemy.sql.and_(self._nsec[0] <= other._nsec[0], self._nsec[1] >= other._nsec[1])

    def lower(self) -> sqlalchemy.sql.ColumnElement:
        # Docstring inherited.
        return sqlalchemy.sql.functions.coalesce(self._nsec[0], sqlalchemy.sql.literal(0))

    def upper(self) -> sqlalchemy.sql.ColumnElement:
        # Docstring inherited.
        return sqlalchemy.sql.functions.coalesce(self._nsec[1], sqlalchemy.sql.literal(0))

    def flatten(self, name: str | None = None) -> tuple[sqlalchemy.sql.ColumnElement, ...]:
        # Docstring inherited.
        if name is None:
            return self._nsec
        else:
            return (
                self._nsec[0].label(f"{name}_begin"),
                self._nsec[1].label(f"{name}_end"),
            )

    def apply_any_aggregate(
        self,
        func: Callable[[sqlalchemy.ColumnElement[Any]], sqlalchemy.ColumnElement[Any]],
    ) -> TimespanDatabaseRepresentation:
        # Docstring inherited.
        return _CompoundTimespanDatabaseRepresentation(
            nsec=(func(self._nsec[0]), func(self._nsec[1])), name=self._name
        )


TimespanDatabaseRepresentation.Compound = _CompoundTimespanDatabaseRepresentation

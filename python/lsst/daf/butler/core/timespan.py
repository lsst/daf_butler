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
    "TimespanDatabaseRepresentation",
)

import enum
import warnings
from abc import ABC, abstractmethod
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    Generator,
    List,
    Mapping,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import astropy.time
import astropy.utils.exceptions
import sqlalchemy
import yaml

# As of astropy 4.2, the erfa interface is shipped independently and
# ErfaWarning is no longer an AstropyWarning
try:
    import erfa
except ImportError:
    erfa = None

from lsst.utils.classes import cached_getter

from . import ddl
from .json import from_json_generic, to_json_generic
from .time_utils import TimeConverter

if TYPE_CHECKING:  # Imports needed only for type annotations; may be circular.
    from ..registry import Registry
    from .dimensions import DimensionUniverse


class _SpecialTimespanBound(enum.Enum):
    """Enumeration to provide a singleton value for empty timespan bounds.

    This enum's only member should generally be accessed via the
    `Timespan.EMPTY` alias.
    """

    EMPTY = enum.auto()
    """The value used for both `Timespan.begin` and `Timespan.end` for empty
    Timespans that contain no points.
    """


TimespanBound = Union[astropy.time.Time, _SpecialTimespanBound, None]


class Timespan:
    """A half-open time interval with nanosecond precision.

    Parameters
    ----------
    begin : `astropy.time.Time`, `Timespan.EMPTY`, or `None`
        Minimum timestamp in the interval (inclusive).  `None` indicates that
        the timespan has no lower bound.  `Timespan.EMPTY` indicates that the
        timespan contains no times; if this is used as either bound, the other
        bound is ignored.
    end : `astropy.time.Time`, `SpecialTimespanBound`, or `None`
        Maximum timestamp in the interval (exclusive). `None` indicates that
        the timespan has no upper bound.  As with ``begin``, `Timespan.EMPTY`
        creates an empty timespan.
    padInstantaneous : `bool`, optional
        If `True` (default) and ``begin == end`` *after discretization to
        integer nanoseconds*, extend ``end`` by one nanosecond to yield a
        finite-duration timespan.  If `False`, ``begin == end`` evaluates to
        the empty timespan.
    _nsec : `tuple` of `int`, optional
        Integer nanosecond representation, for internal use by `Timespan` and
        `TimespanDatabaseRepresentation` implementation only.  If provided,
        all other arguments are are ignored.

    Raises
    ------
    TypeError
        Raised if ``begin`` or ``end`` has a type other than
        `astropy.time.Time`, and is not `None` or `Timespan.EMPTY`.
    ValueError
        Raised if ``begin`` or ``end`` exceeds the minimum or maximum times
        supported by this class.

    Notes
    -----
    Timespans are half-open intervals, i.e. ``[begin, end)``.

    Any timespan with ``begin > end`` after nanosecond discretization
    (``begin >= end`` if ``padInstantaneous`` is `False`), or with either bound
    set to `Timespan.EMPTY`, is transformed into the empty timespan, with both
    bounds set to `Timespan.EMPTY`.  The empty timespan is equal to itself, and
    contained by all other timespans (including itself).  It is also disjoint
    with all timespans (including itself), and hence does not overlap any
    timespan - this is the only case where ``contains`` does not imply
    ``overlaps``.

    Finite timespan bounds are represented internally as integer nanoseconds,
    and hence construction from `astropy.time.Time` (which has picosecond
    accuracy) can involve a loss of precision.  This is of course
    deterministic, so any `astropy.time.Time` value is always mapped
    to the exact same timespan bound, but if ``padInstantaneous`` is `True`,
    timespans that are empty at full precision (``begin > end``,
    ``begin - end < 1ns``) may be finite after discretization.  In all other
    cases, the relationships between full-precision timespans should be
    preserved even if the values are not.

    The `astropy.time.Time` bounds that can be obtained after construction from
    `Timespan.begin` and `Timespan.end` are also guaranteed to round-trip
    exactly when used to construct other `Timespan` instances.
    """

    def __init__(
        self,
        begin: TimespanBound,
        end: TimespanBound,
        padInstantaneous: bool = True,
        _nsec: Optional[Tuple[int, int]] = None,
    ):
        converter = TimeConverter()
        if _nsec is None:
            begin_nsec: int
            if begin is None:
                begin_nsec = converter.min_nsec
            elif begin is self.EMPTY:
                begin_nsec = converter.max_nsec
            elif isinstance(begin, astropy.time.Time):
                begin_nsec = converter.astropy_to_nsec(begin)
            else:
                raise TypeError(
                    f"Unexpected value of type {type(begin).__name__} for Timespan.begin: {begin!r}."
                )
            end_nsec: int
            if end is None:
                end_nsec = converter.max_nsec
            elif end is self.EMPTY:
                end_nsec = converter.min_nsec
            elif isinstance(end, astropy.time.Time):
                end_nsec = converter.astropy_to_nsec(end)
            else:
                raise TypeError(f"Unexpected value of type {type(end).__name__} for Timespan.end: {end!r}.")
            if begin_nsec == end_nsec:
                if begin_nsec == converter.max_nsec or end_nsec == converter.min_nsec:
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore", category=astropy.utils.exceptions.AstropyWarning)
                        if erfa is not None:
                            warnings.simplefilter("ignore", category=erfa.ErfaWarning)
                        if begin is not None and begin < converter.epoch:
                            raise ValueError(f"Timespan.begin may not be earlier than {converter.epoch}.")
                        if end is not None and end > converter.max_time:
                            raise ValueError(f"Timespan.end may not be later than {converter.max_time}.")
                    raise ValueError("Infinite instantaneous timespans are not supported.")
                elif padInstantaneous:
                    end_nsec += 1
                    if end_nsec == converter.max_nsec:
                        raise ValueError(
                            f"Cannot construct near-instantaneous timespan at {end}; "
                            "within one ns of maximum time."
                        )
            _nsec = (begin_nsec, end_nsec)
        if _nsec[0] >= _nsec[1]:
            # Standardizing all empty timespans to the same underlying values
            # here simplifies all other operations (including interactions
            # with TimespanDatabaseRepresentation implementations).
            _nsec = (converter.max_nsec, converter.min_nsec)
        self._nsec = _nsec

    __slots__ = ("_nsec", "_cached_begin", "_cached_end")

    EMPTY: ClassVar[_SpecialTimespanBound] = _SpecialTimespanBound.EMPTY

    # YAML tag name for Timespan
    yaml_tag = "!lsst.daf.butler.Timespan"

    @classmethod
    def makeEmpty(cls) -> Timespan:
        """Construct an empty timespan.

        Returns
        -------
        empty : `Timespan`
            A timespan that is contained by all timespans (including itself)
            and overlaps no other timespans (including itself).
        """
        converter = TimeConverter()
        return Timespan(None, None, _nsec=(converter.max_nsec, converter.min_nsec))

    @classmethod
    def fromInstant(cls, time: astropy.time.Time) -> Timespan:
        """Construct a timespan that approximates an instant in time.

        This is done by constructing a minimum-possible (1 ns) duration
        timespan.

        This is equivalent to ``Timespan(time, time, padInstantaneous=True)``,
        but may be slightly more efficient.

        Parameters
        ----------
        time : `astropy.time.Time`
            Time to use for the lower bound.

        Returns
        -------
        instant : `Timespan`
            A ``[time, time + 1ns)`` timespan.
        """
        converter = TimeConverter()
        nsec = converter.astropy_to_nsec(time)
        if nsec == converter.max_nsec - 1:
            raise ValueError(
                f"Cannot construct near-instantaneous timespan at {time}; within one ns of maximum time."
            )
        return Timespan(None, None, _nsec=(nsec, nsec + 1))

    @property
    @cached_getter
    def begin(self) -> TimespanBound:
        """Minimum timestamp in the interval, inclusive.

        If this bound is finite, this is an `astropy.time.Time` instance.
        If the timespan is unbounded from below, this is `None`.
        If the timespan is empty, this is the special value `Timespan.EMPTY`.
        """
        if self.isEmpty():
            return self.EMPTY
        elif self._nsec[0] == TimeConverter().min_nsec:
            return None
        else:
            return TimeConverter().nsec_to_astropy(self._nsec[0])

    @property
    @cached_getter
    def end(self) -> TimespanBound:
        """Maximum timestamp in the interval, exclusive.

        If this bound is finite, this is an `astropy.time.Time` instance.
        If the timespan is unbounded from above, this is `None`.
        If the timespan is empty, this is the special value `Timespan.EMPTY`.
        """
        if self.isEmpty():
            return self.EMPTY
        elif self._nsec[1] == TimeConverter().max_nsec:
            return None
        else:
            return TimeConverter().nsec_to_astropy(self._nsec[1])

    def isEmpty(self) -> bool:
        """Test whether ``self`` is the empty timespan (`bool`)."""
        return self._nsec[0] >= self._nsec[1]

    def __str__(self) -> str:
        if self.isEmpty():
            return "(empty)"
        # Trap dubious year warnings in case we have timespans from
        # simulated data in the future
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=astropy.utils.exceptions.AstropyWarning)
            if erfa is not None:
                warnings.simplefilter("ignore", category=erfa.ErfaWarning)
            if self.begin is None:
                head = "(-∞, "
            else:
                assert isinstance(self.begin, astropy.time.Time), "guaranteed by earlier checks and ctor"
                head = f"[{self.begin.tai.isot}, "
            if self.end is None:
                tail = "∞)"
            else:
                assert isinstance(self.end, astropy.time.Time), "guaranteed by earlier checks and ctor"
                tail = f"{self.end.tai.isot})"
        return head + tail

    def __repr__(self) -> str:
        # astropy.time.Time doesn't have an eval-friendly __repr__, so we
        # simulate our own here to make Timespan's __repr__ eval-friendly.
        # Interestingly, enum.Enum has an eval-friendly __str__, but not an
        # eval-friendly __repr__.
        tmpl = "astropy.time.Time('{t}', scale='{t.scale}', format='{t.format}')"
        begin = tmpl.format(t=self.begin) if isinstance(self.begin, astropy.time.Time) else str(self.begin)
        end = tmpl.format(t=self.end) if isinstance(self.end, astropy.time.Time) else str(self.end)
        return f"Timespan(begin={begin}, end={end})"

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Timespan):
            return False
        # Correctness of this simple implementation depends on __init__
        # standardizing all empty timespans to a single value.
        return self._nsec == other._nsec

    def __hash__(self) -> int:
        # Correctness of this simple implementation depends on __init__
        # standardizing all empty timespans to a single value.
        return hash(self._nsec)

    def __reduce__(self) -> tuple:
        return (Timespan, (None, None, False, self._nsec))

    def __lt__(self, other: Union[astropy.time.Time, Timespan]) -> bool:
        """Test if a Timespan's bounds are strictly less than the given time.

        Parameters
        ----------
        other : `Timespan` or `astropy.time.Time`.
            Timespan or instant in time to relate to ``self``.

        Returns
        -------
        less : `bool`
            The result of the less-than test.  `False` if either operand is
            empty.
        """
        # First term in each expression below is the "normal" one; the second
        # ensures correct behavior for empty timespans.  It's important that
        # the second uses a strict inequality to make sure inf == inf isn't in
        # play, and it's okay for the second to use a strict inequality only
        # because we know non-empty Timespans have nonzero duration, and hence
        # the second term is never false for non-empty timespans unless the
        # first term is also false.
        if isinstance(other, astropy.time.Time):
            nsec = TimeConverter().astropy_to_nsec(other)
            return self._nsec[1] <= nsec and self._nsec[0] < nsec
        else:
            return self._nsec[1] <= other._nsec[0] and self._nsec[0] < other._nsec[1]

    def __gt__(self, other: Union[astropy.time.Time, Timespan]) -> bool:
        """Test if a Timespan's bounds are strictly greater than given time.

        Parameters
        ----------
        other : `Timespan` or `astropy.time.Time`.
            Timespan or instant in time to relate to ``self``.

        Returns
        -------
        greater : `bool`
            The result of the greater-than test.  `False` if either operand is
            empty.
        """
        # First term in each expression below is the "normal" one; the second
        # ensures correct behavior for empty timespans.  It's important that
        # the second uses a strict inequality to make sure inf == inf isn't in
        # play, and it's okay for the second to use a strict inequality only
        # because we know non-empty Timespans have nonzero duration, and hence
        # the second term is never false for non-empty timespans unless the
        # first term is also false.
        if isinstance(other, astropy.time.Time):
            nsec = TimeConverter().astropy_to_nsec(other)
            return self._nsec[0] > nsec and self._nsec[1] > nsec
        else:
            return self._nsec[0] >= other._nsec[1] and self._nsec[1] > other._nsec[0]

    def overlaps(self, other: Timespan | astropy.time.Time) -> bool:
        """Test if the intersection of this Timespan with another is empty.

        Parameters
        ----------
        other : `Timespan` or `astropy.time.Time`
            Timespan or time to relate to ``self``.  If a single time, this is
            a synonym for `contains`.

        Returns
        -------
        overlaps : `bool`
            The result of the overlap test.

        Notes
        -----
        If either ``self`` or ``other`` is empty, the result is always `False`.
        In all other cases, ``self.contains(other)`` being `True` implies that
        ``self.overlaps(other)`` is also `True`.
        """
        if isinstance(other, astropy.time.Time):
            return self.contains(other)
        return self._nsec[1] > other._nsec[0] and other._nsec[1] > self._nsec[0]

    def contains(self, other: Union[astropy.time.Time, Timespan]) -> bool:
        """Test if the supplied timespan is within this one.

        Tests whether the intersection of this timespan with another timespan
        or point is equal to the other one.

        Parameters
        ----------
        other : `Timespan` or `astropy.time.Time`.
            Timespan or instant in time to relate to ``self``.

        Returns
        -------
        overlaps : `bool`
            The result of the contains test.

        Notes
        -----
        If ``other`` is empty, `True` is always returned.  In all other cases,
        ``self.contains(other)`` being `True` implies that
        ``self.overlaps(other)`` is also `True`.

        Testing whether an instantaneous `astropy.time.Time` value is contained
        in a timespan is not equivalent to testing a timespan constructed via
        `Timespan.fromInstant`, because Timespan cannot exactly represent
        zero-duration intervals.  In particular, ``[a, b)`` contains the time
        ``b``, but not the timespan ``[b, b + 1ns)`` that would be returned
        by `Timespan.fromInstant(b)``.
        """
        if isinstance(other, astropy.time.Time):
            nsec = TimeConverter().astropy_to_nsec(other)
            return self._nsec[0] <= nsec and self._nsec[1] > nsec
        else:
            return self._nsec[0] <= other._nsec[0] and self._nsec[1] >= other._nsec[1]

    def intersection(self, *args: Timespan) -> Timespan:
        """Return a new `Timespan` that is contained by all of the given ones.

        Parameters
        ----------
        *args
            All positional arguments are `Timespan` instances.

        Returns
        -------
        intersection : `Timespan`
            The intersection timespan.
        """
        if not args:
            return self
        lowers = [self._nsec[0]]
        lowers.extend(ts._nsec[0] for ts in args)
        uppers = [self._nsec[1]]
        uppers.extend(ts._nsec[1] for ts in args)
        nsec = (max(*lowers), min(*uppers))
        return Timespan(begin=None, end=None, _nsec=nsec)

    def difference(self, other: Timespan) -> Generator[Timespan, None, None]:
        """Return the one or two timespans that cover the interval(s).

        The interval is defined as one that is in ``self`` but not ``other``.

        This is implemented as a generator because the result may be zero, one,
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
            ``other``.  Guaranteed not to be empty.
        """
        intersection = self.intersection(other)
        if intersection.isEmpty():
            yield self
        elif intersection == self:
            yield from ()
        else:
            if intersection._nsec[0] > self._nsec[0]:
                yield Timespan(None, None, _nsec=(self._nsec[0], intersection._nsec[0]))
            if intersection._nsec[1] < self._nsec[1]:
                yield Timespan(None, None, _nsec=(intersection._nsec[1], self._nsec[1]))

    def to_simple(self, minimal: bool = False) -> List[int]:
        """Return simple python type form suitable for serialization.

        Parameters
        ----------
        minimal : `bool`, optional
            Use minimal serialization. Has no effect on for this class.

        Returns
        -------
        simple : `list` of `int`
            The internal span as integer nanoseconds.
        """
        # Return the internal nanosecond form rather than astropy ISO string
        return list(self._nsec)

    @classmethod
    def from_simple(
        cls,
        simple: List[int],
        universe: Optional[DimensionUniverse] = None,
        registry: Optional[Registry] = None,
    ) -> Timespan:
        """Construct a new object from simplified form.

        Designed to use the data returned from the `to_simple` method.

        Parameters
        ----------
        simple : `list` of `int`
            The values returned by `to_simple()`.
        universe : `DimensionUniverse`, optional
            Unused.
        registry : `lsst.daf.butler.Registry`, optional
            Unused.

        Returns
        -------
        result : `Timespan`
            Newly-constructed object.
        """
        nsec1, nsec2 = simple  # for mypy
        return cls(begin=None, end=None, _nsec=(nsec1, nsec2))

    to_json = to_json_generic
    from_json = classmethod(from_json_generic)

    @classmethod
    def to_yaml(cls, dumper: yaml.Dumper, timespan: Timespan) -> Any:
        """Convert Timespan into YAML format.

        This produces a scalar node with a tag "!_SpecialTimespanBound" and
        value being a name of _SpecialTimespanBound enum.

        Parameters
        ----------
        dumper : `yaml.Dumper`
            YAML dumper instance.
        timespan : `Timespan`
            Data to be converted.
        """
        if timespan.isEmpty():
            return dumper.represent_scalar(cls.yaml_tag, "EMPTY")
        else:
            return dumper.represent_mapping(
                cls.yaml_tag,
                dict(begin=timespan.begin, end=timespan.end),
            )

    @classmethod
    def from_yaml(cls, loader: yaml.SafeLoader, node: yaml.MappingNode) -> Optional[Timespan]:
        """Convert YAML node into _SpecialTimespanBound.

        Parameters
        ----------
        loader : `yaml.SafeLoader`
            Instance of YAML loader class.
        node : `yaml.ScalarNode`
            YAML node.

        Returns
        -------
        value : `Timespan`
            Timespan instance, can be ``None``.
        """
        if node.value is None:
            return None
        elif node.value == "EMPTY":
            return Timespan.makeEmpty()
        else:
            d = loader.construct_mapping(node)
            return Timespan(d["begin"], d["end"])


# Register Timespan -> YAML conversion method with Dumper class
yaml.Dumper.add_representer(Timespan, Timespan.to_yaml)

# Register YAML -> Timespan conversion method with Loader, for our use case we
# only need SafeLoader.
yaml.SafeLoader.add_constructor(Timespan.yaml_tag, Timespan.from_yaml)


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

    Compound: ClassVar[Type[TimespanDatabaseRepresentation]]
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
        cls, nullable: bool, name: Optional[str] = None, **kwargs: Any
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
    def getFieldNames(cls, name: Optional[str] = None) -> tuple[str, ...]:
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
    def fromLiteral(cls: Type[_S], timespan: Optional[Timespan]) -> _S:
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
    def from_columns(
        cls: Type[_S], columns: sqlalchemy.sql.ColumnCollection, name: Optional[str] = None
    ) -> _S:
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
        cls, timespan: Optional[Timespan], name: Optional[str] = None, result: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Add a timespan value to a dictionary that represents a database row.

        Parameters
        ----------
        timespan
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
    def extract(cls, mapping: Mapping[str, Any], name: Optional[str] = None) -> Timespan | None:
        """Extract a timespan from a dictionary that represents a database row.

        Parameters
        ----------
        mapping : `Mapping` [ `str`, `Any` ]
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
    def flatten(self, name: Optional[str] = None) -> Tuple[sqlalchemy.sql.ColumnElement, ...]:
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
    def __lt__(self: _S, other: Union[_S, sqlalchemy.sql.ColumnElement]) -> sqlalchemy.sql.ColumnElement:
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
    def __gt__(self: _S, other: Union[_S, sqlalchemy.sql.ColumnElement]) -> sqlalchemy.sql.ColumnElement:
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
    def contains(self: _S, other: Union[_S, sqlalchemy.sql.ColumnElement]) -> sqlalchemy.sql.ColumnElement:
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
        cls, nullable: bool, name: Optional[str] = None, **kwargs: Any
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
    def getFieldNames(cls, name: Optional[str] = None) -> tuple[str, ...]:
        # Docstring inherited.
        if name is None:
            name = cls.NAME
        return (f"{name}_begin", f"{name}_end")

    @classmethod
    def update(
        cls, extent: Optional[Timespan], name: Optional[str] = None, result: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        # Docstring inherited.
        if name is None:
            name = cls.NAME
        if result is None:
            result = {}
        if extent is None:
            begin_nsec = None
            end_nsec = None
        else:
            begin_nsec = extent._nsec[0]
            end_nsec = extent._nsec[1]
        result[f"{name}_begin"] = begin_nsec
        result[f"{name}_end"] = end_nsec
        return result

    @classmethod
    def extract(cls, mapping: Mapping[str, Any], name: Optional[str] = None) -> Optional[Timespan]:
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
        cls, columns: sqlalchemy.sql.ColumnCollection, name: Optional[str] = None
    ) -> _CompoundTimespanDatabaseRepresentation:
        # Docstring inherited.
        if name is None:
            name = cls.NAME
        return cls(nsec=(columns[f"{name}_begin"], columns[f"{name}_end"]), name=name)

    @classmethod
    def fromLiteral(cls, timespan: Optional[Timespan]) -> _CompoundTimespanDatabaseRepresentation:
        # Docstring inherited.
        if timespan is None:
            return cls(nsec=(sqlalchemy.sql.null(), sqlalchemy.sql.null()), name=cls.NAME)
        return cls(
            nsec=(sqlalchemy.sql.literal(timespan._nsec[0]), sqlalchemy.sql.literal(timespan._nsec[1])),
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
        self, other: Union[_CompoundTimespanDatabaseRepresentation, sqlalchemy.sql.ColumnElement]
    ) -> sqlalchemy.sql.ColumnElement:
        # Docstring inherited.
        # See comments in Timespan.__lt__ for why we use these exact
        # expressions.
        if isinstance(other, sqlalchemy.sql.ColumnElement):
            return sqlalchemy.sql.and_(self._nsec[1] <= other, self._nsec[0] < other)
        else:
            return sqlalchemy.sql.and_(self._nsec[1] <= other._nsec[0], self._nsec[0] < other._nsec[1])

    def __gt__(
        self, other: Union[_CompoundTimespanDatabaseRepresentation, sqlalchemy.sql.ColumnElement]
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
        self, other: Union[_CompoundTimespanDatabaseRepresentation, sqlalchemy.sql.ColumnElement]
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

    def flatten(self, name: Optional[str] = None) -> Tuple[sqlalchemy.sql.ColumnElement, ...]:
        # Docstring inherited.
        if name is None:
            return self._nsec
        else:
            return (
                self._nsec[0].label(f"{name}_begin"),
                self._nsec[1].label(f"{name}_end"),
            )


TimespanDatabaseRepresentation.Compound = _CompoundTimespanDatabaseRepresentation

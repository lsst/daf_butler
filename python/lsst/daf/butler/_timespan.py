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

__all__ = ("Timespan",)

import enum
import warnings
from collections.abc import Generator
from typing import Any, ClassVar, TypeAlias

import astropy.time
import astropy.utils.exceptions
import pydantic
import yaml

# As of astropy 4.2, the erfa interface is shipped independently and
# ErfaWarning is no longer an AstropyWarning
try:
    import erfa
except ImportError:
    erfa = None

from lsst.utils.classes import cached_getter

from .time_utils import TimeConverter

_ONE_DAY = astropy.time.TimeDelta("1d", scale="tai")


class _SpecialTimespanBound(enum.Enum):
    """Enumeration to provide a singleton value for empty timespan bounds.

    This enum's only member should generally be accessed via the
    `Timespan.EMPTY` alias.
    """

    EMPTY = enum.auto()
    """The value used for both `Timespan.begin` and `Timespan.end` for empty
    Timespans that contain no points.
    """


TimespanBound: TypeAlias = astropy.time.Time | _SpecialTimespanBound | None


class Timespan(pydantic.BaseModel):
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
        _nsec: tuple[int, int] | None = None,
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
        super().__init__(nsec=_nsec)

    nsec: tuple[int, int] = pydantic.Field(frozen=True)

    model_config = pydantic.ConfigDict(
        json_schema_extra={
            "description": (
                "A [begin, end) TAI timespan with bounds as integer nanoseconds since 1970-01-01 00:00:00."
            )
        }
    )

    EMPTY: ClassVar[_SpecialTimespanBound] = _SpecialTimespanBound.EMPTY

    # YAML tag name for Timespan
    yaml_tag: ClassVar[str] = "!lsst.daf.butler.Timespan"

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

    @classmethod
    def from_day_obs(cls, day_obs: int, offset: int = 0) -> Timespan:
        """Construct a timespan for a 24-hour period based on the day of
        observation.

        Parameters
        ----------
        day_obs : `int`
            The day of observation as an integer of the form YYYYMMDD.
            The year must be at least 1970 since these are converted to TAI.
        offset : `int`, optional
            Offset in seconds from TAI midnight to be applied.

        Returns
        -------
        day_span : `Timespan`
            A timespan corresponding to a full day of observing.

        Notes
        -----
        If the observing day is 20240229 and the offset is 12 hours the
        resulting time span will be 2024-02-29T12:00 to 2024-03-01T12:00.
        """
        if day_obs < 1970_00_00 or day_obs > 1_0000_00_00:
            raise ValueError(f"day_obs must be in form yyyyMMDD and be newer than 1970, not {day_obs}.")

        ymd = str(day_obs)
        t1 = astropy.time.Time(f"{ymd[0:4]}-{ymd[4:6]}-{ymd[6:8]}T00:00:00", format="isot", scale="tai")

        if offset != 0:
            t_delta = astropy.time.TimeDelta(offset, format="sec", scale="tai")
            t1 += t_delta

        t2 = t1 + _ONE_DAY

        return Timespan(t1, t2)

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
        elif self.nsec[0] == TimeConverter().min_nsec:
            return None
        else:
            return TimeConverter().nsec_to_astropy(self.nsec[0])

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
        elif self.nsec[1] == TimeConverter().max_nsec:
            return None
        else:
            return TimeConverter().nsec_to_astropy(self.nsec[1])

    def isEmpty(self) -> bool:
        """Test whether ``self`` is the empty timespan (`bool`)."""
        return self.nsec[0] >= self.nsec[1]

    def __str__(self) -> str:
        if self.isEmpty():
            return "(empty)"
        fmt = "%Y-%m-%dT%H:%M:%S"
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
                head = f"[{self.begin.tai.strftime(fmt)}, "
            if self.end is None:
                tail = "∞)"
            else:
                assert isinstance(self.end, astropy.time.Time), "guaranteed by earlier checks and ctor"
                tail = f"{self.end.tai.strftime(fmt)})"
        return head + tail

    def __repr_astropy__(self, t: astropy.time.Time | None) -> str:
        # Provide our own repr for astropy time.
        # For JD times we want to use jd1 and jd2 to maintain precision.
        if isinstance(t, astropy.time.Time):
            if t.format == "jd":
                return f"astropy.time.Time({t.jd1}, {t.jd2}, scale='{t.scale}', format='{t.format}')"
            else:
                return f"astropy.time.Time('{t}', scale='{t.scale}', format='{t.format}')"
        return str(t)

    def __repr__(self) -> str:
        # astropy.time.Time doesn't have an eval-friendly __repr__, so we
        # simulate our own here to make Timespan's __repr__ eval-friendly.
        # Interestingly, enum.Enum has an eval-friendly __str__, but not an
        # eval-friendly __repr__.
        begin = self.__repr_astropy__(self.begin)
        end = self.__repr_astropy__(self.end)
        return f"Timespan(begin={begin}, end={end})"

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Timespan):
            return False
        # Correctness of this simple implementation depends on __init__
        # standardizing all empty timespans to a single value.
        return self.nsec == other.nsec

    def __hash__(self) -> int:
        # Correctness of this simple implementation depends on __init__
        # standardizing all empty timespans to a single value.
        return hash(self.nsec)

    def __reduce__(self) -> tuple:
        return (Timespan, (None, None, False, self.nsec))

    def __lt__(self, other: astropy.time.Time | Timespan) -> bool:
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
            return self.nsec[1] <= nsec and self.nsec[0] < nsec
        else:
            return self.nsec[1] <= other.nsec[0] and self.nsec[0] < other.nsec[1]

    def __gt__(self, other: astropy.time.Time | Timespan) -> bool:
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
            return self.nsec[0] > nsec and self.nsec[1] > nsec
        else:
            return self.nsec[0] >= other.nsec[1] and self.nsec[1] > other.nsec[0]

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
        return self.nsec[1] > other.nsec[0] and other.nsec[1] > self.nsec[0]

    def contains(self, other: astropy.time.Time | Timespan) -> bool:
        """Test if the supplied timespan is within this one.

        Tests whether the intersection of this timespan with another timespan
        or point is equal to the other one.

        Parameters
        ----------
        other : `Timespan` or `astropy.time.Time`
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
            return self.nsec[0] <= nsec and self.nsec[1] > nsec
        else:
            return self.nsec[0] <= other.nsec[0] and self.nsec[1] >= other.nsec[1]

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
        lowers = [self.nsec[0]]
        lowers.extend(ts.nsec[0] for ts in args)
        uppers = [self.nsec[1]]
        uppers.extend(ts.nsec[1] for ts in args)
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
            if intersection.nsec[0] > self.nsec[0]:
                yield Timespan(None, None, _nsec=(self.nsec[0], intersection.nsec[0]))
            if intersection.nsec[1] < self.nsec[1]:
                yield Timespan(None, None, _nsec=(intersection.nsec[1], self.nsec[1]))

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
    def from_yaml(cls, loader: yaml.SafeLoader, node: yaml.MappingNode) -> Timespan | None:
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

    @pydantic.model_validator(mode="before")
    @classmethod
    def _validate(cls, value: Any) -> Any:
        if isinstance(value, Timespan):
            return value
        if isinstance(value, dict):
            return value
        return {"nsec": value}

    @pydantic.model_serializer(mode="plain")
    def _serialize(self) -> tuple[int, int]:
        return self.nsec


# Register Timespan -> YAML conversion method with Dumper class
yaml.Dumper.add_representer(Timespan, Timespan.to_yaml)

# Register YAML -> Timespan conversion method with Loader, for our use case we
# only need SafeLoader.
yaml.SafeLoader.add_constructor(Timespan.yaml_tag, Timespan.from_yaml)

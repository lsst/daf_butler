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

#
# Design notes for this module are in
# doc/lsst.daf.butler/dev/dataCoordinate.py.
#

from __future__ import annotations

__all__ = (
    "DataCoordinate",
    "DataId",
    "DataIdKey",
    "DataIdValue",
    "SerializedDataCoordinate",
    "SerializedDataId",
)

import numbers
from abc import abstractmethod
from collections.abc import Iterable, Iterator, Mapping
from typing import TYPE_CHECKING, Any, ClassVar, TypeAlias, overload

import pydantic

from lsst.sphgeom import IntersectionRegion, Region

from .._exceptions import DimensionNameError
from .._timespan import Timespan
from ..json import from_json_pydantic, to_json_pydantic
from ..persistence_context import PersistenceContextVars
from ._group import DimensionGroup
from ._records import DataIdKey, DataIdValue, DimensionRecord, SerializedDimensionRecord

if TYPE_CHECKING:  # Imports needed only for type annotations; may be circular.
    from ..registry import Registry
    from ._universe import DimensionUniverse

SerializedDataId: TypeAlias = dict[str, DataIdValue]
"""Simplified model for serializing the ``mapping`` property of
`DataCoordinate`.
"""


class SerializedDataCoordinate(pydantic.BaseModel):
    """Simplified model for serializing a `DataCoordinate`."""

    dataId: SerializedDataId
    records: dict[str, SerializedDimensionRecord] | None = None

    @classmethod
    def direct(cls, *, dataId: SerializedDataId, records: dict[str, dict] | None) -> SerializedDataCoordinate:
        """Construct a `SerializedDataCoordinate` directly without validators.

        Parameters
        ----------
        dataId : `SerializedDataId`
            The data ID.
        records : `dict` or `None`
            The dimension records.

        Notes
        -----
        This differs from the pydantic "construct" method in that the arguments
        are explicitly what the model requires, and it will recurse through
        members, constructing them from their corresponding `direct` methods.

        This method should only be called when the inputs are trusted.
        """
        key = (frozenset(dataId.items()), records is not None)
        cache = PersistenceContextVars.serializedDataCoordinateMapping.get()
        if cache is not None and (result := cache.get(key)) is not None:
            return result

        if records is None:
            serialized_records = None
        else:
            serialized_records = {k: SerializedDimensionRecord.direct(**v) for k, v in records.items()}

        node = cls.model_construct(dataId=dataId, records=serialized_records)

        if cache is not None:
            cache[key] = node
        return node


def _intersectRegions(*args: Region) -> Region | None:
    """Return the intersection of several regions.

    For internal use by `ExpandedDataCoordinate` only.

    If no regions are provided, returns `None`.
    """
    if len(args) == 0:
        return None
    else:
        result = args[0]
        for n in range(1, len(args)):
            result = IntersectionRegion(result, args[n])
        return result


class DataCoordinate:
    """A validated data ID.

    DataCoordinate guarantees that its key-value pairs identify at least all
    required dimensions in a `DimensionGroup`.

    Notes
    -----
    `DataCoordinate` is an ABC, but it provides `staticmethod` factory
    functions for private concrete implementations that should be sufficient
    for most purposes.  `standardize` is the most flexible and safe of these;
    the others (`make_empty`, `from_required_values`, and `from_full_values`)
    are more specialized and perform little or no checking of inputs.

    Lookups for implied dimensions (those in ``self.dimensions.implied``) are
    supported if and only if `has_full_values` is `True`.  This also sets the
    keys of the `mapping` attribute.  This means that `DataCoordinate` equality
    is not the same as testing for equality on the `mapping` attribute
    (instead, it is the same as testing for equality on the `required`
    attribute).

    See also :ref:`lsst.daf.butler-dimensions_data_ids`
    """

    __slots__ = ()

    _serializedType: ClassVar[type[pydantic.BaseModel]] = SerializedDataCoordinate

    @staticmethod
    def standardize(
        mapping: Mapping[str, DataIdValue] | DataCoordinate | None = None,
        *,
        dimensions: Iterable[str] | DimensionGroup | None = None,
        universe: DimensionUniverse | None = None,
        defaults: DataCoordinate | None = None,
        **kwargs: Any,
    ) -> DataCoordinate:
        """Standardize the supplied dataId.

        Adapts an arbitrary mapping and/or additional arguments into a true
        `DataCoordinate`, or augment an existing one.

        Parameters
        ----------
        mapping : `~collections.abc.Mapping` or `DataCoordinate`, optional
            An informal data ID that maps dimensions or dimension names to
            their primary key values (may also be a true `DataCoordinate`).
        dimensions : `~collections.abc.Iterable` [ `str` ], `DimensionGroup`, \
                optional
            The dimensions to be identified by the new `DataCoordinate`. If not
            provided, will be inferred from the keys of ``mapping`` and
            ``**kwargs``, and ``universe`` must be provided unless ``mapping``
            is already a `DataCoordinate`.
        universe : `DimensionUniverse`
            All known dimensions and their relationships; used to expand and
            validate dependencies when ``dimensions`` is not provided.
        defaults : `DataCoordinate`, optional
            Default dimension key-value pairs to use when needed.  These are
            never used to infer ``group``, and are ignored if a different value
            is provided for the same key in ``mapping`` or `**kwargs``.
        **kwargs
            Additional keyword arguments are treated like additional key-value
            pairs in ``mapping``.

        Returns
        -------
        coordinate : `DataCoordinate`
            A validated `DataCoordinate` instance.

        Raises
        ------
        TypeError
            Raised if the set of optional arguments provided is not supported.
        DimensionNameError
            Raised if a key-value pair for a required dimension is missing.
        """
        universe = universe or getattr(dimensions, "universe", None) or getattr(mapping, "universe", None)
        if universe is None:
            raise TypeError("universe must be provided, either directly or via dimensions or mapping.")
        if dimensions is not None:
            dimensions = universe.conform(dimensions)
        new_mapping: dict[str, DataIdValue] = {}
        if isinstance(mapping, DataCoordinate):
            if dimensions is None:
                if not kwargs:
                    # Already standardized to exactly what we want.
                    return mapping
            elif kwargs.keys().isdisjoint(dimensions.names):
                # User provided kwargs, but told us not to use them by
                # passing in dimensions that are disjoint from those kwargs.
                # This is not necessarily user error - it's a useful pattern
                # to pass in all of the key-value pairs you have and let the
                # code here pull out only what it needs.
                return mapping.subset(dimensions.names)
            new_mapping.update((name, mapping[name]) for name in mapping.dimensions.required)
            if mapping.hasFull():
                new_mapping.update((name, mapping[name]) for name in mapping.dimensions.implied)
        elif mapping is not None:
            new_mapping.update(mapping)
        new_mapping.update(kwargs)
        if dimensions is None:
            if defaults is not None:
                universe = defaults.universe
            elif universe is None:
                raise TypeError("universe must be provided if dimensions is not.")
            dimensions = DimensionGroup(universe, new_mapping.keys())
        if not dimensions:
            return DataCoordinate.make_empty(universe)
        # Some backends cannot handle numpy.int64 type which is a subclass of
        # numbers.Integral; convert that to int.
        for k, v in new_mapping.items():
            if isinstance(v, numbers.Integral):  # type: ignore
                new_mapping[k] = int(v)  # type: ignore
        if defaults is not None:
            for k, v in defaults.mapping.items():
                new_mapping.setdefault(k, v)
        if new_mapping.keys() >= dimensions.names:
            return DataCoordinate.from_full_values(
                dimensions, tuple(new_mapping[name] for name in dimensions.data_coordinate_keys)
            )
        else:
            try:
                values = tuple(new_mapping[name] for name in dimensions.required)
            except KeyError as err:
                raise DimensionNameError(
                    f"No value in data ID ({mapping}) for required dimension {err}."
                ) from err
            return DataCoordinate.from_required_values(dimensions, values)

    @property
    @abstractmethod
    def mapping(self) -> Mapping[str, DataIdValue]:
        """A mapping view of the data ID with keys for all dimensions it has
        values for.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def required(self) -> Mapping[str, DataIdValue]:
        """A mapping view of the data ID with keys for just its required
        dimensions.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def required_values(self) -> tuple[DataIdValue, ...]:
        """The required values (only) of this data ID as a tuple.

        Element order is consistent with `required`.

        In contexts where all data IDs have the same dimensions, comparing and
        hashing these tuples can be much faster than comparing the original
        `DataCoordinate` instances.
        """
        raise NotImplementedError()

    @property
    def full_values(self) -> tuple[DataIdValue, ...]:
        """The full values (only) of this data ID as a tuple.

        Element order is consistent with `DimensionGroup.data_coordinate_keys`,
        i.e. all required dimensions followed by all implied dimensions.
        """
        raise ValueError(f"DataCoordinate {self} has only required values.")

    @staticmethod
    def makeEmpty(universe: DimensionUniverse) -> DataCoordinate:
        """Return an empty `DataCoordinate`.

        It identifies the null set of dimensions.

        Parameters
        ----------
        universe : `DimensionUniverse`
            Universe to which this null dimension set belongs.

        Returns
        -------
        dataId : `DataCoordinate`
            A data ID object that identifies no dimensions.  `hasFull` and
            `hasRecords` are guaranteed to return `True`, because both `full`
            and `records` are just empty mappings.
        """
        return DataCoordinate.make_empty(universe)

    @staticmethod
    def make_empty(universe: DimensionUniverse) -> DataCoordinate:
        """Return an empty `DataCoordinate`.

        It identifies the null set of dimensions.

        Parameters
        ----------
        universe : `DimensionUniverse`
            Universe to which this null dimension set belongs.

        Returns
        -------
        data_id : `DataCoordinate`
            A data ID object that identifies no dimensions.  `hasFull` and
            `hasRecords` are guaranteed to return `True`, because both `full`
            and `records` are just empty mappings.
        """
        return _ExpandedTupleDataCoordinate(universe.empty, (), {})

    @staticmethod
    def from_required_values(dimensions: DimensionGroup, values: tuple[DataIdValue, ...]) -> DataCoordinate:
        """Construct a `DataCoordinate` from required dimension values.

        This is a low-level interface with at most assertion-level checking of
        inputs.  Most callers should use `standardize` instead.

        Parameters
        ----------
        dimensions : `DimensionGroup`
            Dimensions this data ID will identify.
        values : `tuple` [ `int` or `str` ]
            Tuple of primary key values corresponding to
            ``dimensions.required``, in that order.

        Returns
        -------
        data_id : `DataCoordinate`
            A data ID object that identifies the given dimensions.
            ``dataId.hasFull()`` will return `True` only if
            ``dimensions.implied`` is empty. ``dataId.hasRecords()`` will
            return `True` if and only if ``dimensions`` is empty.
        """
        assert len(dimensions.required) == len(values), (
            f"Inconsistency between dimensions {dimensions.required} and required values {values}."
        )
        if not dimensions:
            return DataCoordinate.make_empty(dimensions.universe)
        if not dimensions.implied:
            return _FullTupleDataCoordinate(dimensions, values)
        return _RequiredTupleDataCoordinate(dimensions, values)

    @staticmethod
    def from_full_values(dimensions: DimensionGroup, values: tuple[DataIdValue, ...]) -> DataCoordinate:
        """Construct a `DataCoordinate` from all dimension values.

        This is a low-level interface with at most assertion-level checking of
        inputs.  Most callers should use `standardize` instead.

        Parameters
        ----------
        dimensions : `DimensionGroup`
            Dimensions this data ID will identify.
        values : `tuple` [ `int` or `str` ]
            Tuple of primary key values corresponding to
            ``itertools.chain(dimensions.required, dimensions.implied)``, in
            that order.  Note that this is _not_ the same order as
            ``dimensions.names``, though these contain the same elements.

        Returns
        -------
        data_id : `DataCoordinate`
            A data ID object that identifies the given dimensions.
            ``dataId.hasFull()`` will always return `True`.
            ``dataId.hasRecords()`` will only return `True` if ``dimensions``
            is empty.
        """
        assert len(dimensions) == len(values), (
            f"Inconsistency between dimensions {dimensions.data_coordinate_keys} and full values {values}."
        )
        if not dimensions:
            return DataCoordinate.make_empty(dimensions.universe)
        return _FullTupleDataCoordinate(dimensions, values)

    def __bool__(self) -> bool:
        return bool(self.dimensions)

    def __hash__(self) -> int:
        return hash((self.dimensions,) + self.required_values)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, DataCoordinate):
            other = DataCoordinate.standardize(other, universe=self.universe)
        return self.dimensions == other.dimensions and self.required_values == other.required_values

    @abstractmethod
    def __getitem__(self, key: str) -> DataIdValue:
        raise NotImplementedError()

    def __contains__(self, key: str) -> bool:
        try:
            self.__getitem__(key)
            return True
        except KeyError:
            return False

    @overload
    def get(self, key: str) -> DataIdValue | None: ...

    @overload
    def get(self, key: str, default: int) -> int: ...

    @overload
    def get(self, key: str, default: str) -> str: ...

    def get(self, key: str, default: DataIdValue | None = None) -> DataIdValue | None:
        try:
            return self.__getitem__(key)
        except KeyError:
            return default

    def __repr__(self) -> str:
        # We can't make repr yield something that could be exec'd here without
        # printing out the whole DimensionUniverse.
        return str(self.mapping)

    def __lt__(self, other: Any) -> bool:
        if not isinstance(other, DataCoordinate):
            return NotImplemented
        # Unlike repr() we only use required keys here to ensure that __eq__
        # can not be true simultaneously with __lt__ being true.
        return self.required_values < other.required_values

    @abstractmethod
    def subset(self, dimensions: DimensionGroup | Iterable[str]) -> DataCoordinate:
        """Return a `DataCoordinate` whose diensions are a subset of
        ``self.dimensions``.

        Parameters
        ----------
        dimensions : `DimensionGroup` or `~collections.abc.Iterable` [ `str` ]
            The dimensions identified by the returned `DataCoordinate`.

        Returns
        -------
        coordinate : `DataCoordinate`
            A `DataCoordinate` instance that identifies only the given
            dimensions.  May be ``self`` if ``dimensions == self.dimensions``.

        Raises
        ------
        KeyError
            Raised if the primary key value for one or more required dimensions
            is unknown.  This may happen even if the required subset of the new
            dimensions are not a subset of the dimensions actually known by
            this data ID..  As an example, consider trying to go from a data ID
            with dimensions {instrument, physical_filter, band} to just
            {instrument, band}; band is implied by physical_filter and hence
            would have no value in the original data ID if ``self.hasFull()``
            is `False`.

        Notes
        -----
        If `hasFull` and `hasRecords` return `True` on ``self``, they will
        return `True` (respectively) on the returned `DataCoordinate` as well.
        The converse does not hold.
        """
        raise NotImplementedError()

    @abstractmethod
    def union(self, other: DataCoordinate) -> DataCoordinate:
        """Combine two data IDs.

        Yields a new one that identifies all dimensions that either of them
        identify.

        Parameters
        ----------
        other : `DataCoordinate`
            Data ID to combine with ``self``.

        Returns
        -------
        unioned : `DataCoordinate`
            A `DataCoordinate` instance that satisfies
            ``unioned.dimensions == self.dimensions.union(other.dimensions)``.
            Will preserve ``hasFull`` and ``hasRecords`` whenever possible.

        Notes
        -----
        No checking for consistency is performed on values for keys that
        ``self`` and ``other`` have in common, and which value is included in
        the returned data ID is not specified.
        """
        raise NotImplementedError()

    @abstractmethod
    def expanded(self, records: Mapping[str, DimensionRecord | None]) -> DataCoordinate:
        """Return a `DataCoordinate` that holds the given records.

        Guarantees that `hasRecords` returns `True`.

        This is a low-level interface with at most assertion-level checking of
        inputs.  Most callers should use `Registry.expandDataId` instead.

        Parameters
        ----------
        records : `~collections.abc.Mapping` [ `str`, `DimensionRecord` or \
                `None` ]
            A`~collections.abc.Mapping` with `str` (dimension element name)
            keys and `DimensionRecord` values.  Keys must cover all elements in
            ``self.dimensions.elements``.  Values may be `None`, but only to
            reflect actual NULL values in the database, not just records that
            have not been fetched.
        """
        raise NotImplementedError()

    @property
    def universe(self) -> DimensionUniverse:
        """Universe that defines all known compatible dimensions.

        The universe will be compatible with this coordinate
        (`DimensionUniverse`).
        """
        return self.dimensions.universe

    @property
    @abstractmethod
    def dimensions(self) -> DimensionGroup:
        """Dimensions identified by this data ID (`DimensionGroup`).

        Note that values are only required to be present for dimensions in
        ``self.dimensions.required``; all others may be retrieved (from a
        `Registry`) given these.
        """
        raise NotImplementedError()

    @abstractmethod
    def hasFull(self) -> bool:
        """Whether this data ID contains implied and required values.

        Returns
        -------
        state : `bool`
            If `True`, `__getitem__`, `get`, and `__contains__` (but not
            `keys`!) will act as though the mapping includes key-value pairs
            for implied dimensions, and the `full` property may be used.  If
            `False`, these operations only include key-value pairs for required
            dimensions, and accessing `full` is an error.  Always `True` if
            there are no implied dimensions.
        """
        raise NotImplementedError()

    @abstractmethod
    def hasRecords(self) -> bool:
        """Whether this data ID contains records.

        These are the records for all of the dimension elements it identifies.

        Returns
        -------
        state : `bool`
            If `True`, the following attributes may be accessed:

             - `records`
             - `region`
             - `timespan`
             - `pack`

            If `False`, accessing any of these is considered a logic error.
        """
        raise NotImplementedError()

    @property
    def records(self) -> Mapping[str, DimensionRecord | None]:
        """A  mapping that contains `DimensionRecord` objects for all
        elements identified by this data ID.

        Notes
        -----
        The values of this mapping may be `None` if and only if there is no
        record for that element with these dimensions in the database (which
        means some foreign key field must have a NULL value).

        Accessing this attribute if `hasRecords` returns `False` is a logic
        error that may raise an exception of unspecified type either
        immediately or when the returned mapping is used, depending on the
        implementation and whether assertions are enabled.
        """
        assert self.hasRecords(), "records may only be accessed if hasRecords() returns True."
        return _DataCoordinateRecordsView(self)

    @abstractmethod
    def _record(self, name: str) -> DimensionRecord | None:
        """Protected implementation hook that backs the ``records`` attribute.

        Parameters
        ----------
        name : `str`
            The name of a `DimensionElement`, guaranteed to be in
            ``self.dimensions.elements``.

        Returns
        -------
        record : `DimensionRecord` or `None`
            The dimension record for the given element identified by this
            data ID, or `None` if there is no such record.
        """
        raise NotImplementedError()

    @property
    def region(self) -> Region | None:
        """Spatial region associated with this data ID.

        (`lsst.sphgeom.Region` or `None`).

        This is `None` if and only if ``self.dimensions.spatial`` is empty.

        Accessing this attribute if `hasRecords` returns `False` is a logic
        error that may or may not raise an exception, depending on the
        implementation and whether assertions are enabled.
        """
        assert self.hasRecords(), "region may only be accessed if hasRecords() returns True."
        regions = []
        for family in self.dimensions.spatial:
            element = family.choose(self.dimensions)
            record = self._record(element.name)
            if record is None or record.region is None:
                return None
            else:
                regions.append(record.region)
        return _intersectRegions(*regions)

    @property
    def timespan(self) -> Timespan | None:
        """Temporal interval associated with this data ID.

        (`Timespan` or `None`).

        This is `None` if and only if ``self.dimensions.temporal`` is empty.

        Accessing this attribute if `hasRecords` returns `False` is a logic
        error that may or may not raise an exception, depending on the
        implementation and whether assertions are enabled.
        """
        assert self.hasRecords(), "timespan may only be accessed if hasRecords() returns True."
        timespans = []
        for family in self.dimensions.temporal:
            element = family.choose(self.dimensions)
            record = self._record(element.name)
            # DimensionRecord subclasses for temporal elements always have
            # .timespan, but they're dynamic so this can't be type-checked.
            if record is None or record.timespan is None:
                return None
            else:
                timespans.append(record.timespan)
        if not timespans:
            return None
        elif len(timespans) == 1:
            return timespans[0]
        else:
            return Timespan.intersection(*timespans)

    def to_simple(self, minimal: bool = False) -> SerializedDataCoordinate:
        """Convert this class to a simple python type.

        This is suitable for serialization.

        Parameters
        ----------
        minimal : `bool`, optional
            Use minimal serialization. If set the records will not be attached.

        Returns
        -------
        simple : `SerializedDataCoordinate`
            The object converted to simple form.
        """
        # Convert to a dict form
        records: dict[str, SerializedDimensionRecord] | None
        if not minimal and self.hasRecords():
            records = {
                k: v.to_simple() for k in self.dimensions.elements if (v := self.records[k]) is not None
            }
        else:
            records = None

        return SerializedDataCoordinate(dataId=dict(self.mapping), records=records)

    @classmethod
    def from_simple(
        cls,
        simple: SerializedDataCoordinate,
        universe: DimensionUniverse | None = None,
        registry: Registry | None = None,
    ) -> DataCoordinate:
        """Construct a new object from the simplified form.

        The data is assumed to be of the form returned from the `to_simple`
        method.

        Parameters
        ----------
        simple : `dict` of [`str`, `Any`]
            The `dict` returned by `to_simple()`.
        universe : `DimensionUniverse`
            Object that manages all known dimensions.
        registry : `lsst.daf.butler.Registry`, optional
            Registry from which a universe can be extracted. Can be `None`
            if universe is provided explicitly.

        Returns
        -------
        dataId : `DataCoordinate`
            Newly-constructed object.
        """
        key = (frozenset(simple.dataId.items()), simple.records is not None)
        cache = PersistenceContextVars.dataCoordinates.get()
        if cache is not None and (result := cache.get(key)) is not None:
            return result
        if universe is None and registry is None:
            raise ValueError("One of universe or registry is required to convert a dict to a DataCoordinate")
        if universe is None and registry is not None:
            universe = registry.dimensions
        if universe is None:
            # this is for mypy
            raise ValueError("Unable to determine a usable universe")

        dataId = cls.standardize(simple.dataId, universe=universe)
        if simple.records:
            dataId = dataId.expanded(
                {k: DimensionRecord.from_simple(v, universe=universe) for k, v in simple.records.items()}
            )
        if cache is not None:
            cache[key] = dataId
        return dataId

    to_json = to_json_pydantic
    from_json: ClassVar = classmethod(from_json_pydantic)


DataId = DataCoordinate | Mapping[str, Any]
"""A type-annotation alias for signatures that accept both informal data ID
dictionaries and validated `DataCoordinate` instances.
"""


class _DataCoordinateRecordsView(Mapping[str, DimensionRecord | None]):
    """View class for `DataCoordinate.records`.

    Provides the default implementation for
    `DataCoordinate.records`.

    Parameters
    ----------
    target : `DataCoordinate`
        The `DataCoordinate` instance this object provides a view of.
    """

    def __init__(self, target: DataCoordinate):
        self._target = target

    __slots__ = ("_target",)

    def __repr__(self) -> str:
        terms = [f"{d}: {self[d]!r}" for d in self._target.dimensions.elements]
        return "{{{}}}".format(", ".join(terms))

    def __str__(self) -> str:
        return "\n".join(str(v) for v in self.values())

    def __getitem__(self, key: str) -> DimensionRecord | None:
        return self._target._record(key)

    def __iter__(self) -> Iterator[str]:
        return iter(self._target.dimensions.elements)

    def __len__(self) -> int:
        return len(self._target.dimensions.elements)


class _BasicTupleDataCoordinate(DataCoordinate):
    """Intermediate base class for the standard implementation of
    `DataCoordinate`.

    This class should only be accessed outside this module via the
    `DataCoordinate` interface, and should only be constructed via the static
    methods there.

    Parameters
    ----------
    dimensions : `DimensionGroup`
        The dimensions to be identified.
    values : `tuple` [ `int` or `str` ]
        Data ID values, ordered to match
        ``dimensions.data_coordinate_keys``.  May include values for just
        required dimensions (which always come first) or all dimensions
        (concrete subclasses implementations will care which).
    """

    def __init__(self, dimensions: DimensionGroup, values: tuple[DataIdValue, ...]):
        self._dimensions = dimensions
        self._values = values

    __slots__ = ("_dimensions", "_values")

    @property
    def dimensions(self) -> DimensionGroup:
        # Docstring inherited from DataCoordinate.
        return self._dimensions

    @property
    def required(self) -> Mapping[str, DataIdValue]:
        # Docstring inherited from DataCoordinate.
        return _DataCoordinateRequiredMappingView(self)

    def __getitem__(self, key: str) -> DataIdValue:
        # Docstring inherited from DataCoordinate.

        index = self._dimensions._data_coordinate_indices[key]
        try:
            return self._values[index]
        except IndexError:
            # Caller asked for an implied dimension, but this object only has
            # values for the required ones.
            raise KeyError(key) from None

    def hasRecords(self) -> bool:
        # Docstring inherited from DataCoordinate.
        return False

    def _record(self, name: str) -> DimensionRecord | None:
        # Docstring inherited from DataCoordinate.
        raise AssertionError()

    def __getattr__(self, name: str) -> Any:
        if name in self.dimensions.elements:
            raise AttributeError(
                f"Dimension record attribute {name!r} is only available on expanded DataCoordinates."
            )
        raise AttributeError(name)


class _DataCoordinateRequiredMappingView(Mapping[str, DataIdValue]):
    """A DataCoordinate Mapping view class whose keys are just the required
    dimensions.
    """

    def __init__(self, target: DataCoordinate):
        self._target = target

    __slots__ = ("_target",)

    def __getitem__(self, key: str) -> DataIdValue:
        if key not in self._target.dimensions.required:
            raise KeyError(key)
        return self._target[key]

    def __len__(self) -> int:
        return len(self._target.dimensions.required)

    def __iter__(self) -> Iterator[str]:
        return iter(self._target.dimensions.required)

    def __repr__(self) -> str:
        return f"{{{', '.join(f'{k}: {v!r}' for k, v in self.items())}}}"


class _DataCoordinateFullMappingView(Mapping[str, DataIdValue]):
    """A DataCoordinate Mapping view class whose keys are all dimensions."""

    def __init__(self, target: DataCoordinate):
        self._target = target

    __slots__ = ("_target",)

    def __getitem__(self, key: str) -> DataIdValue:
        return self._target[key]

    def __len__(self) -> int:
        return len(self._target.dimensions)

    def __iter__(self) -> Iterator[str]:
        return iter(self._target.dimensions.data_coordinate_keys)

    def __repr__(self) -> str:
        return f"{{{', '.join(f'{k}: {v!r}' for k, v in self.items())}}}"


class _RequiredTupleDataCoordinate(_BasicTupleDataCoordinate):
    """A `DataCoordinate` implementation that has values for required
    dimensions only, when implied dimensions already exist.

    Note that `_FullTupleDataCoordinate` should be used if there are no
    implied dimensions.

    This class should only be accessed outside this module via the
    `DataCoordinate` interface, and should only be constructed via calls to
    `DataCoordinate.from_full_values`.
    """

    __slots__ = ()

    @property
    def mapping(self) -> Mapping[str, DataIdValue]:
        # Docstring inherited from DataCoordinate.
        return _DataCoordinateRequiredMappingView(self)

    @property
    def required_values(self) -> tuple[DataIdValue, ...]:
        # Docstring inherited from DataCoordinate.
        return self._values

    def subset(self, dimensions: DimensionGroup | Iterable[str]) -> DataCoordinate:
        # Docstring inherited from DataCoordinate.
        dimensions = self.universe.conform(dimensions)
        if self._dimensions == dimensions:
            return self
        elif self._dimensions.required >= dimensions.names:
            return DataCoordinate.from_full_values(
                dimensions,
                tuple(self[k] for k in dimensions.data_coordinate_keys),
            )
        else:
            return DataCoordinate.from_required_values(
                dimensions, tuple(self[k] for k in dimensions.required)
            )

    def union(self, other: DataCoordinate) -> DataCoordinate:
        # Docstring inherited from DataCoordinate.
        dimensions = self.dimensions.union(other.dimensions)
        # See if the other one is already what we want to return.  We don't
        # shortcut-return 'self' because `other` might have full values or
        # even records, and we want to return the more complete data ID.
        if other.dimensions == dimensions:
            return other
        # General case with actual merging of dictionaries.
        values = dict(self.mapping)
        values.update(other.mapping)
        return DataCoordinate.standardize(values, dimensions=dimensions)

    def expanded(self, records: Mapping[str, DimensionRecord | None]) -> DataCoordinate:
        # Docstring inherited from DataCoordinate
        # Extract a complete values tuple from the attributes of the given
        # records.  It's possible for these to be inconsistent with
        # self._values (which is a serious problem, of course), but we've
        # documented this as a no-checking API.
        values = self._values + tuple(
            getattr(records[d], self.universe.dimensions[d].primaryKey.name) for d in self._dimensions.implied
        )
        return _ExpandedTupleDataCoordinate(self._dimensions, values, records)

    def hasFull(self) -> bool:
        # Docstring inherited from DataCoordinate.
        return False

    def __reduce__(self) -> tuple[Any, ...]:
        return (_RequiredTupleDataCoordinate, (self._dimensions, self._values))


class _FullTupleDataCoordinate(_BasicTupleDataCoordinate):
    """A `DataCoordinate` implementation that has values for all dimensions.

    This class should only be accessed outside this module via the
    `DataCoordinate` interface, and should only be constructed via calls to
    `DataCoordinate.from_full_values`.
    """

    __slots__ = ()

    @property
    def mapping(self) -> Mapping[str, DataIdValue]:
        # Docstring inherited from DataCoordinate.
        return _DataCoordinateFullMappingView(self)

    @property
    def required_values(self) -> tuple[DataIdValue, ...]:
        # Docstring inherited from DataCoordinate.
        return self._values[: len(self._dimensions.required)]

    @property
    def full_values(self) -> tuple[DataIdValue, ...]:
        # Docstring inherited from DataCoordinate.
        return self._values

    def subset(self, dimensions: DimensionGroup | Iterable[str]) -> DataCoordinate:
        # Docstring inherited from DataCoordinate.
        dimensions = self.universe.conform(dimensions)
        if self._dimensions == dimensions:
            return self
        return DataCoordinate.from_full_values(
            dimensions,
            tuple(self[k] for k in dimensions.data_coordinate_keys),
        )

    def union(self, other: DataCoordinate) -> DataCoordinate:
        # Docstring inherited from DataCoordinate.
        dimensions = self.dimensions.union(other.dimensions)
        # See if one or both input data IDs is already what we want to return;
        # if so, return the most complete one we have.
        if other.dimensions == dimensions and other.hasRecords():
            return other
        elif self.dimensions == dimensions and not other.hasRecords():
            return self
        # General case with actual merging of dictionaries.
        values = dict(self.mapping)
        values.update(other.mapping)
        return DataCoordinate.standardize(values, dimensions=dimensions)

    def expanded(self, records: Mapping[str, DimensionRecord | None]) -> DataCoordinate:
        # Docstring inherited from DataCoordinate
        return _ExpandedTupleDataCoordinate(self._dimensions, self._values, records)

    def hasFull(self) -> bool:
        # Docstring inherited from DataCoordinate.
        return True

    def __reduce__(self) -> tuple[Any, ...]:
        return (_FullTupleDataCoordinate, (self._dimensions, self._values))


class _ExpandedTupleDataCoordinate(_FullTupleDataCoordinate):
    """A `DataCoordinate` implementation that directly holds `DimensionRecord`
    objects relevant to it.

    This class should only be accessed outside this module via the
    `DataCoordinate` interface, and should only be constructed via calls to
    `DataCoordinate.expanded`.

    Parameters
    ----------
    dimensions : `DimensionGroup`
        The dimensions to be identified.
    values : `tuple` [ `int` or `str` ]
        Data ID values, ordered to match
        ``dimensions._data_coordinate_indices``. Just include values for all
        dimensions.
    records : `~collections.abc.Mapping` [ `str`, `DimensionRecord` or `None` ]
        A `~collections.abc.Mapping` with `str` (dimension element name) keys
        and `DimensionRecord` values.  Keys must cover all elements in
        ``self.dimensions.elements``.  Values may be `None`, but only to
        reflect actual NULL values in the database, not just records that have
        not been fetched.
    """

    def __init__(
        self,
        dimensions: DimensionGroup,
        values: tuple[DataIdValue, ...],
        records: Mapping[str, DimensionRecord | None],
    ):
        super().__init__(dimensions, values)
        assert super().hasFull(), "This implementation requires full dimension records."
        self._records = records

    __slots__ = ("_records",)

    def subset(self, dimensions: DimensionGroup | Iterable[str]) -> DataCoordinate:
        # Docstring inherited from DataCoordinate.
        return super().subset(dimensions).expanded(self._records)

    def expanded(self, records: Mapping[str, DimensionRecord | None]) -> DataCoordinate:
        # Docstring inherited from DataCoordinate.
        return self

    def union(self, other: DataCoordinate) -> DataCoordinate:
        # Docstring inherited from DataCoordinate.
        result = super().union(other)
        if not result.hasRecords() and other.hasRecords():
            records = {e: self._record(e) for e in self.dimensions.elements} | {
                e: other._record(e) for e in other.dimensions.elements
            }
            if records.keys() >= result.dimensions.elements:
                return result.expanded(records)
        return result

    def hasRecords(self) -> bool:
        # Docstring inherited from DataCoordinate.
        return True

    def _record(self, name: str) -> DimensionRecord | None:
        # Docstring inherited from DataCoordinate.
        return self._records[name]

    def __reduce__(self) -> tuple[Any, ...]:
        return (_ExpandedTupleDataCoordinate, (self._dimensions, self._values, self._records))

    def __getattr__(self, name: str) -> Any:
        try:
            return self._record(name)
        except KeyError:
            raise AttributeError(name) from None

    def __dir__(self) -> list[str]:
        result = list(super().__dir__())
        result.extend(self.dimensions.elements)
        return result

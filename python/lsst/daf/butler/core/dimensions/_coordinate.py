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

#
# Design notes for this module are in
# doc/lsst.daf.butler/dev/dataCoordinate.py.
#

from __future__ import annotations

__all__ = ("DataCoordinate", "DataId", "DataIdKey", "DataIdValue", "SerializedDataCoordinate")

import numbers
from abc import abstractmethod
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Any,
    Dict,
    Iterator,
    Literal,
    Mapping,
    Optional,
    Tuple,
    Union,
    overload,
)

from lsst.sphgeom import IntersectionRegion, Region
from pydantic import BaseModel

from ..json import from_json_pydantic, to_json_pydantic
from ..named import NamedKeyDict, NamedKeyMapping, NamedValueAbstractSet, NameLookupMapping
from ..timespan import Timespan
from ._elements import Dimension, DimensionElement
from ._graph import DimensionGraph
from ._records import DimensionRecord, SerializedDimensionRecord

if TYPE_CHECKING:  # Imports needed only for type annotations; may be circular.
    from ...registry import Registry
    from ._universe import DimensionUniverse

DataIdKey = Union[str, Dimension]
"""Type annotation alias for the keys that can be used to index a
DataCoordinate.
"""

# Pydantic will cast int to str if str is first in the Union.
DataIdValue = Union[int, str, None]
"""Type annotation alias for the values that can be present in a
DataCoordinate or other data ID.
"""


class SerializedDataCoordinate(BaseModel):
    """Simplified model for serializing a `DataCoordinate`."""

    dataId: Dict[str, DataIdValue]
    records: Optional[Dict[str, SerializedDimensionRecord]] = None

    @classmethod
    def direct(cls, *, dataId: Dict[str, DataIdValue], records: Dict[str, Dict]) -> SerializedDataCoordinate:
        """Construct a `SerializedDataCoordinate` directly without validators.

        This differs from the pydantic "construct" method in that the arguments
        are explicitly what the model requires, and it will recurse through
        members, constructing them from their corresponding `direct` methods.

        This method should only be called when the inputs are trusted.
        """
        node = SerializedDataCoordinate.__new__(cls)
        setter = object.__setattr__
        setter(node, "dataId", dataId)
        setter(
            node,
            "records",
            records
            if records is None
            else {k: SerializedDimensionRecord.direct(**v) for k, v in records.items()},
        )
        setter(node, "__fields_set__", {"dataId", "records"})
        return node


def _intersectRegions(*args: Region) -> Optional[Region]:
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


class DataCoordinate(NamedKeyMapping[Dimension, DataIdValue]):
    """Data ID dictionary.

    An immutable data ID dictionary that guarantees that its key-value pairs
    identify at least all required dimensions in a `DimensionGraph`.

    `DataCoordinate` itself is an ABC, but provides `staticmethod` factory
    functions for private concrete implementations that should be sufficient
    for most purposes.  `standardize` is the most flexible and safe of these;
    the others (`makeEmpty`, `fromRequiredValues`, and `fromFullValues`) are
    more specialized and perform little or no checking of inputs.

    Notes
    -----
    Like any data ID class, `DataCoordinate` behaves like a dictionary, but
    with some subtleties:

     - Both `Dimension` instances and `str` names thereof may be used as keys
       in lookup operations, but iteration (and `keys`) will yield `Dimension`
       instances.  The `names` property can be used to obtain the corresponding
       `str` names.

     - Lookups for implied dimensions (those in ``self.graph.implied``) are
       supported if and only if `hasFull` returns `True`, and are never
       included in iteration or `keys`.  The `full` property may be used to
       obtain a mapping whose keys do include implied dimensions.

     - Equality comparison with other mappings is supported, but it always
       considers only required dimensions (as well as requiring both operands
       to identify the same dimensions).  This is not quite consistent with the
       way mappings usually work - normally differing keys imply unequal
       mappings - but it makes sense in this context because data IDs with the
       same values for required dimensions but different values for implied
       dimensions represent a serious problem with the data that
       `DataCoordinate` cannot generally recognize on its own, and a data ID
       that knows implied dimension values should still be able to compare as
       equal to one that does not.  This is of course not the way comparisons
       between simple `dict` data IDs work, and hence using a `DataCoordinate`
       instance for at least one operand in any data ID comparison is strongly
       recommended.
    """

    __slots__ = ()

    _serializedType = SerializedDataCoordinate

    @staticmethod
    def standardize(
        mapping: Optional[NameLookupMapping[Dimension, DataIdValue]] = None,
        *,
        graph: Optional[DimensionGraph] = None,
        universe: Optional[DimensionUniverse] = None,
        defaults: Optional[DataCoordinate] = None,
        **kwargs: Any,
    ) -> DataCoordinate:
        """Standardize the supplied dataId.

        Adapts an arbitrary mapping and/or additional arguments into a true
        `DataCoordinate`, or augment an existing one.

        Parameters
        ----------
        mapping : `~collections.abc.Mapping`, optional
            An informal data ID that maps dimensions or dimension names to
            their primary key values (may also be a true `DataCoordinate`).
        graph : `DimensionGraph`
            The dimensions to be identified by the new `DataCoordinate`.
            If not provided, will be inferred from the keys of ``mapping`` and
            ``**kwargs``, and ``universe`` must be provided unless ``mapping``
            is already a `DataCoordinate`.
        universe : `DimensionUniverse`
            All known dimensions and their relationships; used to expand
            and validate dependencies when ``graph`` is not provided.
        defaults : `DataCoordinate`, optional
            Default dimension key-value pairs to use when needed.  These are
            never used to infer ``graph``, and are ignored if a different value
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
        KeyError
            Raised if a key-value pair for a required dimension is missing.
        """
        d: Dict[str, DataIdValue] = {}
        if isinstance(mapping, DataCoordinate):
            if graph is None:
                if not kwargs:
                    # Already standardized to exactly what we want.
                    return mapping
            elif kwargs.keys().isdisjoint(graph.dimensions.names):
                # User provided kwargs, but told us not to use them by
                # passing in dimensions that are disjoint from those kwargs.
                # This is not necessarily user error - it's a useful pattern
                # to pass in all of the key-value pairs you have and let the
                # code here pull out only what it needs.
                return mapping.subset(graph)
            assert universe is None or universe == mapping.universe
            universe = mapping.universe
            d.update((name, mapping[name]) for name in mapping.graph.required.names)
            if mapping.hasFull():
                d.update((name, mapping[name]) for name in mapping.graph.implied.names)
        elif isinstance(mapping, NamedKeyMapping):
            d.update(mapping.byName())
        elif mapping is not None:
            d.update(mapping)
        d.update(kwargs)
        if graph is None:
            if defaults is not None:
                universe = defaults.universe
            elif universe is None:
                raise TypeError("universe must be provided if graph is not.")
            graph = DimensionGraph(universe, names=d.keys())
        if not graph.dimensions:
            return DataCoordinate.makeEmpty(graph.universe)
        if defaults is not None:
            if defaults.hasFull():
                for k, v in defaults.full.items():
                    d.setdefault(k.name, v)
            else:
                for k, v in defaults.items():
                    d.setdefault(k.name, v)
        if d.keys() >= graph.dimensions.names:
            values = tuple(d[name] for name in graph._dataCoordinateIndices.keys())
        else:
            try:
                values = tuple(d[name] for name in graph.required.names)
            except KeyError as err:
                raise KeyError(f"No value in data ID ({mapping}) for required dimension {err}.") from err
        # Some backends cannot handle numpy.int64 type which is a subclass of
        # numbers.Integral; convert that to int.
        values = tuple(
            int(val) if isinstance(val, numbers.Integral) else val for val in values  # type: ignore
        )
        return _BasicTupleDataCoordinate(graph, values)

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
        return _ExpandedTupleDataCoordinate(universe.empty, (), {})

    @staticmethod
    def fromRequiredValues(graph: DimensionGraph, values: Tuple[DataIdValue, ...]) -> DataCoordinate:
        """Construct a `DataCoordinate` from required dimension values.

        This is a low-level interface with at most assertion-level checking of
        inputs.  Most callers should use `standardize` instead.

        Parameters
        ----------
        graph : `DimensionGraph`
            Dimensions this data ID will identify.
        values : `tuple` [ `int` or `str` ]
            Tuple of primary key values corresponding to ``graph.required``,
            in that order.

        Returns
        -------
        dataId : `DataCoordinate`
            A data ID object that identifies the given dimensions.
            ``dataId.hasFull()`` will return `True` if and only if
            ``graph.implied`` is empty, and ``dataId.hasRecords()`` will never
            return `True`.
        """
        assert len(graph.required) == len(
            values
        ), f"Inconsistency between dimensions {graph.required} and required values {values}."
        return _BasicTupleDataCoordinate(graph, values)

    @staticmethod
    def fromFullValues(graph: DimensionGraph, values: Tuple[DataIdValue, ...]) -> DataCoordinate:
        """Construct a `DataCoordinate` from all dimension values.

        This is a low-level interface with at most assertion-level checking of
        inputs.  Most callers should use `standardize` instead.

        Parameters
        ----------
        graph : `DimensionGraph`
            Dimensions this data ID will identify.
        values : `tuple` [ `int` or `str` ]
            Tuple of primary key values corresponding to
            ``itertools.chain(graph.required, graph.implied)``, in that order.
            Note that this is _not_ the same order as ``graph.dimensions``,
            though these contain the same elements.

        Returns
        -------
        dataId : `DataCoordinate`
            A data ID object that identifies the given dimensions.
            ``dataId.hasFull()`` will return `True` if and only if
            ``graph.implied`` is empty, and ``dataId.hasRecords()`` will never
            return `True`.
        """
        assert len(graph.dimensions) == len(
            values
        ), f"Inconsistency between dimensions {graph.dimensions} and full values {values}."
        return _BasicTupleDataCoordinate(graph, values)

    def __hash__(self) -> int:
        return hash((self.graph,) + tuple(self[d.name] for d in self.graph.required))

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, DataCoordinate):
            other = DataCoordinate.standardize(other, universe=self.universe)
        return self.graph == other.graph and all(self[d.name] == other[d.name] for d in self.graph.required)

    def __repr__(self) -> str:
        # We can't make repr yield something that could be exec'd here without
        # printing out the whole DimensionUniverse the graph is derived from.
        # So we print something that mostly looks like a dict, but doesn't
        # quote its keys: that's both more compact and something that can't
        # be mistaken for an actual dict or something that could be exec'd.
        terms = [f"{d}: {self[d]!r}" for d in self.graph.required.names]
        if self.hasFull() and self.graph.required != self.graph.dimensions:
            terms.append("...")
        return "{{{}}}".format(", ".join(terms))

    def __lt__(self, other: Any) -> bool:
        # Allow DataCoordinate to be sorted
        if not isinstance(other, type(self)):
            return NotImplemented
        # Form tuple of tuples for each DataCoordinate:
        # Unlike repr() we only use required keys here to ensure that
        # __eq__ can not be true simultaneously with __lt__ being true.
        self_kv = tuple(self.items())
        other_kv = tuple(other.items())

        return self_kv < other_kv

    def __iter__(self) -> Iterator[Dimension]:
        return iter(self.keys())

    def __len__(self) -> int:
        return len(self.keys())

    def keys(self) -> NamedValueAbstractSet[Dimension]:  # type: ignore
        return self.graph.required

    @property
    def names(self) -> AbstractSet[str]:
        """Names of the required dimensions identified by this data ID.

        They are returned in the same order as `keys`
        (`collections.abc.Set` [ `str` ]).
        """
        return self.keys().names

    @abstractmethod
    def subset(self, graph: DimensionGraph) -> DataCoordinate:
        """Return a `DataCoordinate` whose graph is a subset of ``self.graph``.

        Parameters
        ----------
        graph : `DimensionGraph`
            The dimensions identified by the returned `DataCoordinate`.

        Returns
        -------
        coordinate : `DataCoordinate`
            A `DataCoordinate` instance that identifies only the given
            dimensions.  May be ``self`` if ``graph == self.graph``.

        Raises
        ------
        KeyError
            Raised if the primary key value for one or more required dimensions
            is unknown.  This may happen if ``graph.issubset(self.graph)`` is
            `False`, or even if ``graph.issubset(self.graph)`` is `True`, if
            ``self.hasFull()`` is `False` and
            ``graph.required.issubset(self.graph.required)`` is `False`.  As
            an example of the latter case, consider trying to go from a data ID
            with dimensions {instrument, physical_filter, band} to
            just {instrument, band}; band is implied by
            physical_filter and hence would have no value in the original data
            ID if ``self.hasFull()`` is `False`.

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
            ``unioned.graph == self.graph.union(other.graph)``.  Will preserve
            ``hasFull`` and ``hasRecords`` whenever possible.

        Notes
        -----
        No checking for consistency is performed on values for keys that
        ``self`` and ``other`` have in common, and which value is included in
        the returned data ID is not specified.
        """
        raise NotImplementedError()

    @abstractmethod
    def expanded(
        self, records: NameLookupMapping[DimensionElement, Optional[DimensionRecord]]
    ) -> DataCoordinate:
        """Return a `DataCoordinate` that holds the given records.

        Guarantees that `hasRecords` returns `True`.

        This is a low-level interface with at most assertion-level checking of
        inputs.  Most callers should use `Registry.expandDataId` instead.

        Parameters
        ----------
        records : `Mapping` [ `str`, `DimensionRecord` or `None` ]
            A `NamedKeyMapping` with `DimensionElement` keys or a regular
            `Mapping` with `str` (`DimensionElement` name) keys and
            `DimensionRecord` values.  Keys must cover all elements in
            ``self.graph.elements``.  Values may be `None`, but only to reflect
            actual NULL values in the database, not just records that have not
            been fetched.
        """
        raise NotImplementedError()

    @property
    def universe(self) -> DimensionUniverse:
        """Universe that defines all known compatible dimensions.

        The univers will be compatible with this coordinate
        (`DimensionUniverse`).
        """
        return self.graph.universe

    @property
    @abstractmethod
    def graph(self) -> DimensionGraph:
        """Dimensions identified by this data ID (`DimensionGraph`).

        Note that values are only required to be present for dimensions in
        ``self.graph.required``; all others may be retrieved (from a
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

    @property
    def full(self) -> NamedKeyMapping[Dimension, DataIdValue]:
        """Return mapping for all dimensions in ``self.graph``.

        The mapping includes key-value pairs for all dimensions in
        ``self.graph``, including implied (`NamedKeyMapping`).

        Accessing this attribute if `hasFull` returns `False` is a logic error
        that may raise an exception of unspecified type either immediately or
        when implied keys are accessed via the returned mapping, depending on
        the implementation and whether assertions are enabled.
        """
        assert self.hasFull(), "full may only be accessed if hasFull() returns True."
        return _DataCoordinateFullView(self)

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
    def records(self) -> NamedKeyMapping[DimensionElement, Optional[DimensionRecord]]:
        """Return the records.

        Returns a  mapping that contains `DimensionRecord` objects for all
        elements identified by this data ID (`NamedKeyMapping`).

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
    def _record(self, name: str) -> Optional[DimensionRecord]:
        """Protected implementation hook that backs the ``records`` attribute.

        Parameters
        ----------
        name : `str`
            The name of a `DimensionElement`, guaranteed to be in
            ``self.graph.elements.names``.

        Returns
        -------
        record : `DimensionRecord` or `None`
            The dimension record for the given element identified by this
            data ID, or `None` if there is no such record.
        """
        raise NotImplementedError()

    @property
    def region(self) -> Optional[Region]:
        """Spatial region associated with this data ID.

        (`lsst.sphgeom.Region` or `None`).

        This is `None` if and only if ``self.graph.spatial`` is empty.

        Accessing this attribute if `hasRecords` returns `False` is a logic
        error that may or may not raise an exception, depending on the
        implementation and whether assertions are enabled.
        """
        assert self.hasRecords(), "region may only be accessed if hasRecords() returns True."
        regions = []
        for family in self.graph.spatial:
            element = family.choose(self.graph.elements)
            record = self._record(element.name)
            if record is None or record.region is None:
                return None
            else:
                regions.append(record.region)
        return _intersectRegions(*regions)

    @property
    def timespan(self) -> Optional[Timespan]:
        """Temporal interval associated with this data ID.

        (`Timespan` or `None`).

        This is `None` if and only if ``self.graph.timespan`` is empty.

        Accessing this attribute if `hasRecords` returns `False` is a logic
        error that may or may not raise an exception, depending on the
        implementation and whether assertions are enabled.
        """
        assert self.hasRecords(), "timespan may only be accessed if hasRecords() returns True."
        timespans = []
        for family in self.graph.temporal:
            element = family.choose(self.graph.elements)
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

    @overload
    def pack(self, name: str, *, returnMaxBits: Literal[True]) -> Tuple[int, int]:
        ...

    @overload
    def pack(self, name: str, *, returnMaxBits: Literal[False]) -> int:
        ...

    def pack(self, name: str, *, returnMaxBits: bool = False) -> Union[Tuple[int, int], int]:
        """Pack this data ID into an integer.

        Parameters
        ----------
        name : `str`
            Name of the `DimensionPacker` algorithm (as defined in the
            dimension configuration).
        returnMaxBits : `bool`, optional
            If `True` (`False` is default), return the maximum number of
            nonzero bits in the returned integer across all data IDs.

        Returns
        -------
        packed : `int`
            Integer ID.  This ID is unique only across data IDs that have
            the same values for the packer's "fixed" dimensions.
        maxBits : `int`, optional
            Maximum number of nonzero bits in ``packed``.  Not returned unless
            ``returnMaxBits`` is `True`.

        Notes
        -----
        Accessing this attribute if `hasRecords` returns `False` is a logic
        error that may or may not raise an exception, depending on the
        implementation and whether assertions are enabled.
        """
        assert self.hasRecords(), "pack() may only be called if hasRecords() returns True."
        return self.universe.makePacker(name, self).pack(self, returnMaxBits=returnMaxBits)

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
        if self.hasFull():
            dataId = self.full.byName()
        else:
            dataId = self.byName()
        records: Optional[Dict[str, SerializedDimensionRecord]]
        if not minimal and self.hasRecords():
            records = {k: v.to_simple() for k, v in self.records.byName().items() if v is not None}
        else:
            records = None

        return SerializedDataCoordinate(dataId=dataId, records=records)

    @classmethod
    def from_simple(
        cls,
        simple: SerializedDataCoordinate,
        universe: Optional[DimensionUniverse] = None,
        registry: Optional[Registry] = None,
    ) -> DataCoordinate:
        """Construct a new object from the simplified form.

        The data is assumed to be of the form returned from the `to_simple`
        method.

        Parameters
        ----------
        simple : `dict` of [`str`, `Any`]
            The `dict` returned by `to_simple()`.
        universe : `DimensionUniverse`
            The special graph of all known dimensions.
        registry : `lsst.daf.butler.Registry`, optional
            Registry from which a universe can be extracted. Can be `None`
            if universe is provided explicitly.

        Returns
        -------
        dataId : `DataCoordinate`
            Newly-constructed object.
        """
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
        return dataId

    to_json = to_json_pydantic
    from_json = classmethod(from_json_pydantic)


DataId = Union[DataCoordinate, Mapping[str, Any]]
"""A type-annotation alias for signatures that accept both informal data ID
dictionaries and validated `DataCoordinate` instances.
"""


class _DataCoordinateFullView(NamedKeyMapping[Dimension, DataIdValue]):
    """View class for `DataCoordinate.full`.

    Provides the default implementation for
    `DataCoordinate.full`.

    Parameters
    ----------
    target : `DataCoordinate`
        The `DataCoordinate` instance this object provides a view of.
    """

    def __init__(self, target: DataCoordinate):
        self._target = target

    __slots__ = ("_target",)

    def __repr__(self) -> str:
        terms = [f"{d}: {self[d]!r}" for d in self._target.graph.dimensions.names]
        return "{{{}}}".format(", ".join(terms))

    def __getitem__(self, key: DataIdKey) -> DataIdValue:
        return self._target[key]

    def __iter__(self) -> Iterator[Dimension]:
        return iter(self.keys())

    def __len__(self) -> int:
        return len(self.keys())

    def keys(self) -> NamedValueAbstractSet[Dimension]:  # type: ignore
        return self._target.graph.dimensions

    @property
    def names(self) -> AbstractSet[str]:
        # Docstring inherited from `NamedKeyMapping`.
        return self.keys().names


class _DataCoordinateRecordsView(NamedKeyMapping[DimensionElement, Optional[DimensionRecord]]):
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
        terms = [f"{d}: {self[d]!r}" for d in self._target.graph.elements.names]
        return "{{{}}}".format(", ".join(terms))

    def __str__(self) -> str:
        return "\n".join(str(v) for v in self.values())

    def __getitem__(self, key: Union[DimensionElement, str]) -> Optional[DimensionRecord]:
        if isinstance(key, DimensionElement):
            key = key.name
        return self._target._record(key)

    def __iter__(self) -> Iterator[DimensionElement]:
        return iter(self.keys())

    def __len__(self) -> int:
        return len(self.keys())

    def keys(self) -> NamedValueAbstractSet[DimensionElement]:  # type: ignore
        return self._target.graph.elements

    @property
    def names(self) -> AbstractSet[str]:
        # Docstring inherited from `NamedKeyMapping`.
        return self.keys().names


class _BasicTupleDataCoordinate(DataCoordinate):
    """Standard implementation of `DataCoordinate`.

    Backed by a tuple of values.

    This class should only be accessed outside this module via the
    `DataCoordinate` interface, and should only be constructed via the static
    methods there.

    Parameters
    ----------
    graph : `DimensionGraph`
        The dimensions to be identified.
    values : `tuple` [ `int` or `str` ]
        Data ID values, ordered to match ``graph._dataCoordinateIndices``.  May
        include values for just required dimensions (which always come first)
        or all dimensions.
    """

    def __init__(self, graph: DimensionGraph, values: Tuple[DataIdValue, ...]):
        self._graph = graph
        self._values = values

    __slots__ = ("_graph", "_values")

    @property
    def graph(self) -> DimensionGraph:
        # Docstring inherited from DataCoordinate.
        return self._graph

    def __getitem__(self, key: DataIdKey) -> DataIdValue:
        # Docstring inherited from DataCoordinate.
        if isinstance(key, Dimension):
            key = key.name
        index = self._graph._dataCoordinateIndices[key]
        try:
            return self._values[index]
        except IndexError:
            # Caller asked for an implied dimension, but this object only has
            # values for the required ones.
            raise KeyError(key) from None

    def subset(self, graph: DimensionGraph) -> DataCoordinate:
        # Docstring inherited from DataCoordinate.
        if self._graph == graph:
            return self
        elif self.hasFull() or self._graph.required >= graph.dimensions:
            return _BasicTupleDataCoordinate(
                graph,
                tuple(self[k] for k in graph._dataCoordinateIndices.keys()),
            )
        else:
            return _BasicTupleDataCoordinate(graph, tuple(self[k] for k in graph.required.names))

    def union(self, other: DataCoordinate) -> DataCoordinate:
        # Docstring inherited from DataCoordinate.
        graph = self.graph.union(other.graph)
        # See if one or both input data IDs is already what we want to return;
        # if so, return the most complete one we have.
        if other.graph == graph:
            if self.graph == graph:
                # Input data IDs have the same graph (which is also the result
                # graph), but may not have the same content.
                # other might have records; self does not, so try other first.
                # If it at least has full values, it's no worse than self.
                if other.hasFull():
                    return other
                else:
                    return self
            elif other.hasFull():
                return other
            # There's some chance that neither self nor other has full values,
            # but together provide enough to the union to.  Let the general
            # case below handle that.
        elif self.graph == graph:
            # No chance at returning records.  If self has full values, it's
            # the best we can do.
            if self.hasFull():
                return self
        # General case with actual merging of dictionaries.
        values = self.full.byName() if self.hasFull() else self.byName()
        values.update(other.full.byName() if other.hasFull() else other.byName())
        return DataCoordinate.standardize(values, graph=graph)

    def expanded(
        self, records: NameLookupMapping[DimensionElement, Optional[DimensionRecord]]
    ) -> DataCoordinate:
        # Docstring inherited from DataCoordinate
        values = self._values
        if not self.hasFull():
            # Extract a complete values tuple from the attributes of the given
            # records.  It's possible for these to be inconsistent with
            # self._values (which is a serious problem, of course), but we've
            # documented this as a no-checking API.
            values += tuple(getattr(records[d.name], d.primaryKey.name) for d in self._graph.implied)
        return _ExpandedTupleDataCoordinate(self._graph, values, records)

    def hasFull(self) -> bool:
        # Docstring inherited from DataCoordinate.
        return len(self._values) == len(self._graph._dataCoordinateIndices)

    def hasRecords(self) -> bool:
        # Docstring inherited from DataCoordinate.
        return False

    def _record(self, name: str) -> Optional[DimensionRecord]:
        # Docstring inherited from DataCoordinate.
        assert False


class _ExpandedTupleDataCoordinate(_BasicTupleDataCoordinate):
    """A `DataCoordinate` implementation that can hold `DimensionRecord`.

    This class should only be accessed outside this module via the
    `DataCoordinate` interface, and should only be constructed via calls to
    `DataCoordinate.expanded`.

    Parameters
    ----------
    graph : `DimensionGraph`
        The dimensions to be identified.
    values : `tuple` [ `int` or `str` ]
        Data ID values, ordered to match ``graph._dataCoordinateIndices``.
        May include values for just required dimensions (which always come
        first) or all dimensions.
    records : `Mapping` [ `str`, `DimensionRecord` or `None` ]
        A `NamedKeyMapping` with `DimensionElement` keys or a regular
        `Mapping` with `str` (`DimensionElement` name) keys and
        `DimensionRecord` values.  Keys must cover all elements in
        ``self.graph.elements``.  Values may be `None`, but only to reflect
        actual NULL values in the database, not just records that have not
        been fetched.
    """

    def __init__(
        self,
        graph: DimensionGraph,
        values: Tuple[DataIdValue, ...],
        records: NameLookupMapping[DimensionElement, Optional[DimensionRecord]],
    ):
        super().__init__(graph, values)
        assert super().hasFull(), "This implementation requires full dimension records."
        self._records = records

    __slots__ = ("_records",)

    def subset(self, graph: DimensionGraph) -> DataCoordinate:
        # Docstring inherited from DataCoordinate.
        if self._graph == graph:
            return self
        return _ExpandedTupleDataCoordinate(
            graph, tuple(self[k] for k in graph._dataCoordinateIndices.keys()), records=self._records
        )

    def expanded(
        self, records: NameLookupMapping[DimensionElement, Optional[DimensionRecord]]
    ) -> DataCoordinate:
        # Docstring inherited from DataCoordinate.
        return self

    def union(self, other: DataCoordinate) -> DataCoordinate:
        # Docstring inherited from DataCoordinate.
        graph = self.graph.union(other.graph)
        # See if one or both input data IDs is already what we want to return;
        # if so, return the most complete one we have.
        if self.graph == graph:
            # self has records, so even if other is also a valid result, it's
            # no better.
            return self
        if other.graph == graph:
            # If other has full values, and self does not identify some of
            # those, it's the base we can do.  It may have records, too.
            if other.hasFull():
                return other
            # If other does not have full values, there's a chance self may
            # provide the values needed to complete it.  For example, self
            # could be {band} while other could be
            # {instrument, physical_filter, band}, with band unknown.
        # General case with actual merging of dictionaries.
        values = self.full.byName()
        values.update(other.full.byName() if other.hasFull() else other.byName())
        basic = DataCoordinate.standardize(values, graph=graph)
        # See if we can add records.
        if self.hasRecords() and other.hasRecords():
            # Sometimes the elements of a union of graphs can contain elements
            # that weren't in either input graph (because graph unions are only
            # on dimensions).  e.g. {visit} | {detector} brings along
            # visit_detector_region.
            elements = set(graph.elements.names)
            elements -= self.graph.elements.names
            elements -= other.graph.elements.names
            if not elements:
                records = NamedKeyDict[DimensionElement, Optional[DimensionRecord]](self.records)
                records.update(other.records)
                return basic.expanded(records.freeze())
        return basic

    def hasFull(self) -> bool:
        # Docstring inherited from DataCoordinate.
        return True

    def hasRecords(self) -> bool:
        # Docstring inherited from DataCoordinate.
        return True

    def _record(self, name: str) -> Optional[DimensionRecord]:
        # Docstring inherited from DataCoordinate.
        return self._records[name]

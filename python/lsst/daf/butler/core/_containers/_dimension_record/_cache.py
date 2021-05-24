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

__all__ = ("HeterogeneousDimensionRecordCache", "HomogeneousDimensionRecordCache")

from typing import Any, Callable, DefaultDict, Iterable, MutableMapping

from ...dimensions import (
    DataCoordinate,
    DimensionElement,
    DimensionRecord,
    DimensionUniverse,
)
from ...named import NamedKeyDict, NamedKeyMapping, NameLookupMapping
from .._data_coordinate import DataCoordinateFrozenSet, DataCoordinateIterable
from ._iterable import HomogeneousDimensionRecordIterable
from ._mutable_set import (
    HeterogeneousDimensionRecordMutableSet,
    HomogeneousDimensionRecordMutableSet,
)

FetchRecordCallback = Callable[[DataCoordinateIterable], HomogeneousDimensionRecordIterable]


class HeterogeneousDimensionRecordCache(HeterogeneousDimensionRecordMutableSet):
    """A concrete mutable container for heterogeneous, unique dimension
    records that fetches and adds missing records via a callback mapping.

    Parameters
    ----------
    universe : `DimensionUniverse`
        Definitions for all dimensions.
    callbacks : `NameLookupMapping` [ `DimensionElement`, `Callable` ]
        Mapping from dimension element name to a callable that takes a
        `DataCoordinateIterable` argument and returns a
        `HomogeneousDimensionRecordIterable`.
    records : `Iterable` [ `DimensionRecord` ]
        Dimension records to include at construction.
    """

    def __init__(
        self,
        universe: DimensionUniverse,
        callbacks: NameLookupMapping[DimensionElement, FetchRecordCallback],
        records: Iterable[DimensionRecord] = (),
    ):
        self._universe = universe
        self._by_definition = NamedKeyDict[DimensionElement, HomogeneousDimensionRecordCache](
            {
                element: HomogeneousDimensionRecordCache(element, callbacks[element.name])
                for element in universe.getStaticElements()
            }
        )
        self.update(records)

    __slots__ = (
        "_universe",
        "_by_definition",
    )

    @property
    def universe(self) -> DimensionUniverse:
        return self._universe

    @property
    def by_definition(
        self,
    ) -> NamedKeyMapping[DimensionElement, HomogeneousDimensionRecordCache]:
        # Docstring inherited.
        return self._by_definition


class _ByDataIdDict(DefaultDict[DataCoordinate, DimensionRecord]):
    """Implementation class for `HomogeneousDimensionRecordCache.by_data_ids`.

    Parameters
    ----------
    *args, **kwargs
        Forwarded to the `collections.defaultdict` constructor.
    callback : `Callable`
        Callable that takes a single `DataCoordinateIterable` argument and
        returns a `HomogeneousDimensionRecordIterable`.
    """

    def __init__(self, *args: Any, callback: FetchRecordCallback, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self._callback = callback

    __slots__ = ("_callback",)

    def __missing__(self, key: Any) -> DimensionRecord:
        if not isinstance(key, DataCoordinate):
            raise KeyError(key)
        # Keep this out of the try block because we want any ValueError it
        # raises to propagate up.
        records = self._callback(DataCoordinateFrozenSet.from_scalar(key))
        try:
            (record,) = records
        except ValueError:
            raise KeyError(key) from None
        self[key] = record
        return record


class HomogeneousDimensionRecordCache(HomogeneousDimensionRecordMutableSet):
    """A concrete mutable container for homogeneous, unique dimension
    records that fetches and adds missing records via a callback.

    Parameters
    ----------
    definition : `DimensionElement`
        Element associated with all records.
    callback : `Callable`
        Callable that takes a single `DataCoordinateIterable` argument and
        returns a `HomogeneousDimensionRecordIterable`.
    records : `Iterable` [ `DimensionRecord` ]
        Dimension records to include at construction.

    Raises
    ------
    TypeError
        Raised if ``record.definition != definition`` for any given record.
    """

    def __init__(
        self,
        definition: DimensionElement,
        callback: FetchRecordCallback,
        records: Iterable[DimensionRecord] = (),
    ):
        self._definition = definition
        self._by_data_id = _ByDataIdDict(callback=callback)
        self.update(records)

    __slots__ = ("_definition", "_by_data_id")

    @property
    def definition(self) -> DimensionElement:
        # Docstring inherited.
        return self._definition

    @property
    def by_data_id(self) -> MutableMapping[DataCoordinate, DimensionRecord]:
        # Docstring inherited.
        return self._by_data_id

    def _get_many(self, data_ids: DataCoordinateIterable) -> HomogeneousDimensionRecordCache:
        # Docstring inherited.
        if data_ids.graph != self.definition.graph:
            raise ValueError(
                f"Invalid dimensions for dimension record lookup; expected {self.definition.graph}, "
                f"got {data_ids.graph}."
            )
        desired = data_ids.to_set()
        missing = desired - self.data_ids
        if missing:
            self.update(self._by_data_id._callback(missing))
        result = HomogeneousDimensionRecordCache(self.definition, self._by_data_id._callback)
        for data_id in data_ids:
            if (record := self._by_data_id.get(data_id)) is not None:
                result._by_data_id[data_id] = record
        return result

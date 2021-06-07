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
    "HeterogeneousDimensionRecordAbstractSet",
    "HomogeneousDimensionRecordAbstractSet",
)

from abc import abstractmethod
from typing import Any, Dict, Iterator, Mapping, Optional, TypeVar, overload

from ...dimensions import (
    DataCoordinate,
    DataIdValue,
    Dimension,
    DimensionElement,
    DimensionGraph,
    DimensionRecord,
    InconsistentDataIdError,
)
from ...named import NamedKeyMapping
from .._data_coordinate import (
    DataCoordinateAbstractSet,
    DataCoordinateIterable,
    DataCoordinateSetView,
)
from ._iterable import (
    HeterogeneousDimensionRecordIterable,
    HomogeneousDimensionRecordIterable,
)

_S = TypeVar("_S", bound="HomogeneousDimensionRecordAbstractSet")


class HeterogeneousDimensionRecordAbstractSet(HeterogeneousDimensionRecordIterable):
    """An abstract base class for heterogeneous containers of unique dimension
    records.

    Notes
    -----
    This interface is only informally set-like - it guarantees that there are
    no duplicate elements (as determined by `DimensionRecord.dataId` equality,
    since this should imply equality for other fields), and provides no
    ordering guarantees.  It intentionally does not provide much of the
    `collections.abc.Set` interface, as this would add a lot of complexity
    without clear benefit (not just in implementation, but in behavior
    guarantees as well).
    """

    __slots__ = ()

    def __iter__(self) -> Iterator[DimensionRecord]:
        for inner in self.by_definition.values():
            yield from inner

    def __contains__(self, key: Any) -> bool:
        try:
            return key.dataId in self.by_definition[key.definition]
        except (AttributeError, KeyError):
            return False

    def __len__(self) -> int:
        return sum(len(inner) for inner in self.by_definition.values())

    def __eq__(self, other: Any) -> bool:
        try:
            return self.by_definition == other.by_definition
        except AttributeError:
            return NotImplemented

    def to_set(self) -> HeterogeneousDimensionRecordAbstractSet:
        # Docstring inherited.
        return self

    @property
    @abstractmethod
    def by_definition(
        self,
    ) -> NamedKeyMapping[DimensionElement, HomogeneousDimensionRecordAbstractSet]:
        """A mapping view that groups records by `DimensionElement`
        definition.

        This container always has all elements in its universe, even when it
        has no records for an element.
        """
        raise NotImplementedError()

    def expand_data_id_dict(
        self,
        data_id: Dict[str, DataIdValue],
        graph: DimensionGraph,
        *,
        related_records: Optional[Dict[str, Optional[DimensionRecord]]] = None,
    ) -> None:
        """Expand a data ID dictionary to include values for implied dimensions
        by looking them up in `DimensionRecord` objects.

        This is a low-level interface that should not generally be called
        outside `lsst.daf.butler`; external code should use
        `DataCoordinate.standardize` or `Registry.expandDataIds` instead.

        Parameters
        ----------
        data_id : `dict`
            Dictionary data ID with keys that are a superset of
            ``graph.required.names``.
        graph : `DimensionGraph`
            Target dimensions that this data ID should identify.
        related_records : `dict`, optional
            `DimensionRecord` objects already associated with this data ID.  If
            provided, new records associated with the data ID will be added to
            it, and those present will not need to be fetched from ``self``.

        Raises
        ------
        KeyError
            Raised if a record for a required dimension could not be found.
        """
        if related_records is None:
            related_records = {}
        for element in graph.primaryKeyTraversalOrder:
            # Use ... to mean "not in related_records yet"; we use None for
            # "we already looked for that record and didn't find it."
            if (record := related_records.get(element.name, ...)) is ...:
                if isinstance(element, Dimension) and data_id.get(element.name) is None:
                    if element in graph.required:
                        raise KeyError(f"No value or null value for required dimension {element.name}.")
                    data_id[element.name] = None
                    record = None
                else:
                    subset_data_id = DataCoordinate.fromRequiredValues(
                        element.graph,
                        tuple(
                            dimension.validated(data_id[dimension.name])
                            for dimension in element.graph.required
                        ),
                    )
                    try:
                        record = self.by_definition[element].by_data_id[subset_data_id]
                    except KeyError:
                        record = None
                related_records[element.name] = record
            if record is not None:
                # Use record to fill out data_id.
                for dimension in element.implied:
                    value = getattr(record, dimension.name)
                    if data_id.setdefault(dimension.name, value) != value:
                        raise InconsistentDataIdError(
                            f"Data ID {data_id} has "
                            f"{dimension.name}={data_id[dimension.name]!r}, "
                            f"but {element.name} implies {dimension.name}={value!r}."
                        )
            else:
                if element in graph.required:
                    raise LookupError(
                        f"Could not find record for required dimension {element.name} " f"via {data_id}."
                    )
                if element.alwaysJoin:
                    raise InconsistentDataIdError(
                        f"Could not fetch record for element {element.name} via {data_id}, ",
                        "but it is marked alwaysJoin=True; this means one or more dimensions are not "
                        "related.",
                    )
                for dimension in element.implied:
                    data_id.setdefault(dimension.name, None)
                    related_records.setdefault(dimension.name, None)


class HomogeneousDimensionRecordAbstractSet(HomogeneousDimensionRecordIterable):
    """An abstract base class for homogeneous containers of unique dimension
    records.

    Notes
    -----
    All elements of a `HomogeneousDimensionRecordAbstractSet` correspond to the
    same `DimensionElement` (i.e. share the same value for their
    `~DimensionRecord.definition` attribute).

    This interface is only informally set-like - it guarantees that there are
    no duplicate elements (as determined by `DimensionRecord.dataId` equality,
    since this should imply equality for other fields), and provides no
    ordering guarantees.  It intentionally does not provide much of the
    `collections.abc.Set` interface, as this would add a lot of complexity
    without clear benefit (not just in implementation, but in behavior
    guarantees as well).
    """

    __slots__ = ()

    def __iter__(self) -> Iterator[DimensionRecord]:
        return iter(self.by_data_id.values())

    def __contains__(self, key: Any) -> bool:
        try:
            return key.dataId in self.by_data_id
        except (AttributeError, KeyError):
            return False

    def __len__(self) -> int:
        return len(self.by_data_id)

    def __eq__(self, other: Any) -> bool:
        try:
            return self.by_data_id.keys() == other.by_data_id.keys()
        except AttributeError:
            return NotImplemented

    def to_set(self) -> HomogeneousDimensionRecordAbstractSet:
        # Docstring inherited
        return self

    @property
    @abstractmethod
    def by_data_id(self) -> Mapping[DataCoordinate, DimensionRecord]:
        """A mapping view keyed by data ID."""
        raise NotImplementedError()

    @property
    def data_ids(self) -> DataCoordinateAbstractSet:
        """Set view of the data IDs that identify these records.
        """
        return DataCoordinateSetView(
            self.by_data_id.keys(),
            self.definition.graph,
            has_full=(not self.definition.graph.implied),
            has_records=False,
        )

    @overload
    def __getitem__(self, key: DataCoordinate) -> DimensionRecord:
        raise NotImplementedError()

    @overload
    def __getitem__(self: _S, key: DataCoordinateIterable) -> _S:
        raise NotImplementedError()

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, DataCoordinate):
            return self.by_data_id[key]
        else:
            return self._get_many(key)

    @abstractmethod
    def _get_many(self: _S, data_ids: DataCoordinateIterable) -> _S:
        """Return a new set with records for just the given data IDs.

        Parameters
        ----------
        data_ids : `DataCoordinateIterable`
            Data IDs to select.

        Returns
        -------
        extraction : `HomogeneousDimensionRecordAbstractSet`
            New set with just the given data IDs.  Data IDs whose records are
            not in ``self`` will be ignored, and the order of the given data
            IDs and returned records are not guaranteed to be consistent.

        Raises
        ------
        ValueError
            Raised if the given data IDs do not have the right dimensions
            (``self.definition.graph``).

        Notes
        -----
        This conceptually-protected method should be implemented by derived
        classes, while external code should invoke `__getitem__` instead.
        """
        raise NotImplementedError()

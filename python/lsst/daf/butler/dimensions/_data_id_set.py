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

__all__ = ("DataIdSet",)

import dataclasses
from collections.abc import Collection, Iterable, Iterator, Mapping, Set
from typing import cast, final

from ._coordinate import (
    DataCoordinate,
    DataIdValue,
    _ExpandedTupleDataCoordinate,
    _FullTupleDataCoordinate,
    _RequiredTupleDataCoordinate,
)
from ._group import DimensionGroup
from ._records import DimensionRecord
from ._universe import DimensionUniverse


@dataclasses.dataclass(slots=True)
class _MappingValue:
    implied: tuple[DataIdValue, ...] = ()
    records: tuple[DimensionRecord | None, ...] = ()

    @classmethod
    def from_data_id(
        cls,
        data_id: DataCoordinate,
        dimensions: DimensionGroup,
        has_full_values: bool,
        has_dimension_records: bool,
    ) -> _MappingValue:
        assert data_id.dimensions == dimensions
        implied: tuple[DataIdValue, ...] = ()
        records: tuple[DimensionRecord | None, ...] = ()
        if has_full_values:
            implied = data_id.full_values[len(dimensions.required) :]
            if has_dimension_records:
                records = tuple([data_id.records[element_name] for element_name in dimensions.elements])
        return cls(implied, records)


class DataIdSet(Collection[DataCoordinate]):
    """A set-like collection of `DataCoordinate` objects.

    The constructor is a low-level interface intended primarily for other
    middleware callers; most users should use `from_data_ids` instead.

    Parameters
    ----------
    dimensions : `DimensionGroup`
        Dimensions of the data IDs in this set.
    mapping : `~collections.abc.Mapping` [ `tuple`, `tuple` ]
        A mapping from the required values of each data ID to its implied
        values, if present.
    has_implied_values : `bool`
        If `True`, implied values are present.  Note that this is not quite the
        same as whether the data IDs have "full" values; if there are no
        implied dimensions, ``has_implied_values=False`` but  `has_full_values`
        will be `True`.
    has_dimension_records : `bool`
        If `True`, this container holds all dimension records for all data IDs.
        This should always be `True` if ``dimensions`` is empty.
    """

    def __init__(
        self,
        dimensions: DimensionGroup,
        mapping: Mapping[tuple[DataIdValue, ...], _MappingValue],
        has_implied_values: bool,
        has_dimension_records: bool,
    ):
        self._dimensions = dimensions
        self._mapping = mapping
        self._has_implied_values = has_implied_values
        self._has_dimension_records = has_dimension_records
        if self._has_dimension_records:
            if self._has_implied_values:
                self._factory = self._add_implied_with_records_factory
            else:
                self._factory = self._nothing_implied_with_records_factory
        elif self._has_implied_values:
            self._factory = self._add_implied_factory
        elif not self._dimensions.implied:
            assert self._dimensions, "empty data IDs are considered to have dimension records"
            self._factory = self._nothing_implied_factory
        else:
            self._factory = self._required_only_factory

    @classmethod
    def from_data_ids(
        cls,
        dimensions: DimensionGroup | Iterable[str],
        data_ids: Iterable[DataCoordinate] = (),
        *,
        universe: DimensionUniverse | None = None,
        has_full_values: bool = False,
        has_dimension_records: bool = False,
    ) -> DataIdSet:
        if not isinstance(dimensions, DimensionGroup):
            if universe is None:
                raise TypeError("'universe' must be provided if 'dimensions' is not a DimensionGroup.")
            dimensions = universe.conform(dimensions)
        if not dimensions:
            has_dimension_records = True
        if has_dimension_records:
            has_full_values = True
        mapping = {}
        for data_id in data_ids:
            mapping[data_id.required_values] = _MappingValue.from_data_id(
                data_id,
                dimensions,
                has_full_values=has_full_values,
                has_dimension_records=has_dimension_records,
            )
        return cls(
            dimensions,
            mapping,
            has_implied_values=(has_full_values and bool(dimensions.implied)),
            has_dimension_records=has_dimension_records,
        )

    @property
    def dimensions(self) -> DimensionGroup:
        return self._dimensions

    @property
    def has_full_values(self) -> bool:
        return self._has_implied_values or not self._dimensions.implied

    @property
    def has_dimension_records(self) -> bool:
        return self._has_dimension_records

    @property
    def required_values(self) -> Set[tuple[DataIdValue, ...]]:
        return self._mapping.keys()

    def __iter__(self) -> Iterator[DataCoordinate]:
        for key, value in self._mapping.items():
            yield self._factory(key, value)

    def __len__(self) -> int:
        return len(self._mapping)

    def __contains__(self, key: object) -> bool:
        match key:
            case DataCoordinate() if key.dimensions == self._dimensions:
                required_values = key.required_values
            case {**mapping}:
                key = DataCoordinate.standardize(
                    cast(dict[str, DataIdValue], mapping), dimensions=self._dimensions
                ).required_values
            case _:
                return False
        return required_values in self.required_values

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DataIdSet):
            return False
        return self._dimensions is other._dimensions and self._mapping.keys() == other._mapping.keys()

    def __repr__(self) -> str:
        lines = [f"DataIdSet({self.dimensions}, ["]
        for data_id in self:
            lines.append(f"    {data_id},")
        lines.append("])")
        return "\n".join(lines)

    def find(self, data_id: DataCoordinate) -> DataCoordinate:
        return self.find_with_required_values(data_id.required_values)

    def find_with_required_values(self, required_values: tuple[DataIdValue, ...]) -> DataCoordinate:
        """Return the data ID with the given required values.

        Parameters
        ----------
        required_values : `tuple` [ `int` or `str` ]
            Data ID values to match.

        Returns
        -------
        data_id : `DataCoordinate`
            Matching data ID.
        """
        return self._factory(required_values, self._mapping[required_values])

    def issubset(self, other: DataIdSet) -> bool:
        """Test whether all elements in ``self`` are in ``other``.

        Parameters
        ----------
        other : `DataIdSet`
            Another data ID set with the same dimensions.

        Returns
        -------
        issubset ; `bool`
            Whether all elements in ``self`` are in ``other``.
        """
        if self._dimensions != other._dimensions:
            raise ValueError(
                "Invalid comparison between data ID sets with dimensions "
                f"{self._dimensions} and {other._dimensions}."
            )
        return self._mapping.keys() <= other._mapping.keys()

    def issuperset(self, other: DataIdSet) -> bool:
        """Test whether all elements in ``other`` are in ``self``.

        Parameters
        ----------
        other : `DataIdSet`
            Another data ID set with the same dimensions.

        Returns
        -------
        issuperset ; `bool`
            Whether all elements in ``other`` are in ``self``.
        """
        if self._dimensions != other._dimensions:
            raise ValueError(
                "Invalid comparison between data ID sets with dimensions "
                f"{self._dimensions} and {other._dimensions}."
            )
        return self._mapping.keys() >= other._mapping.keys()

    def isdisjoint(self, other: DataIdSet) -> bool:
        """Test whether the intersection of ``self`` and ``other`` is empty.

        Parameters
        ----------
        other : `DataIdSet`
            Another data ID set with the same dimensions.

        Returns
        -------
        isdisjoint ; `bool`
            Whether the intersection of ``self`` and ``other`` is empty.
        """
        if self._dimensions != other._dimensions:
            raise ValueError(
                "Invalid comparison between data ID sets with dimensions "
                f"{self._dimensions} and {other._dimensions}."
            )
        return self._mapping.keys().isdisjoint(other._mapping.keys())

    def intersection(self, other: DataIdSet) -> DataIdSet:
        """Return a new set with only data IDs that are in both ``self`` and
        ``other``.

        Parameters
        ----------
        other : `DataIdSet`
            Another data ID set with the same dimensions.

        Returns
        -------
        intersection : `DataIdSet`
            A new record set with all elements in both sets.  This will have
            full values and dimensions records if either operand does.
        """
        if self._dimensions != other._dimensions:
            raise ValueError(
                "Invalid intersection between data ID sets with dimensions "
                f"{self._dimensions} and {other._dimensions}."
            )
        # Take result's mapping values from whichever argument has more stuff
        # in its values.
        value_source = max(self, other, key=DataIdSet._completeness)._mapping
        return DataIdSet(
            self._dimensions,
            {k: value_source[k] for k in self._mapping.keys() & other._mapping.keys()},
            self._has_implied_values or other._has_implied_values,
            self._has_dimension_records or other._has_dimension_records,
        )

    def difference(self, other: DataIdSet) -> DataIdSet:
        """Return a new set with only data IDs that are in ``self`` and not in
        ``other``.

        Parameters
        ----------
        other : `DataIdSet`
            Another data ID set with the same dimensions.

        Returns
        -------
        difference : `DataIdSet`
            A new record set with all elements ``self`` that are not in
            ``other``.  This will have full values and dimensions records if
            ``self`` does.
        """
        if self._dimensions != other._dimensions:
            raise ValueError(
                "Invalid difference between data ID sets with dimensions "
                f"{self._dimensions} and {other.dimensions}."
            )
        return DataIdSet(
            self._dimensions,
            {k: self._mapping[k] for k in self.required_values - other.required_values},
            self._has_implied_values,
            self._has_dimension_records,
        )

    def union(self, other: DataIdSet) -> DataIdSet:
        """Return a new set with all data IDs that are either in ``self`` or
        ``other``.

        Parameters
        ----------
        other : `DataIdSet`
            Another data ID set with the same dimensions.

        Returns
        -------
        union : `DataIdSet`
            A new record set with all elements in either set.  This will have
            full values and dimension records only if both operands do.
        """
        if self._dimensions != other._dimensions:
            raise ValueError(
                "Invalid union between data ID sets with dimensions "
                f"{self._dimensions} and {other.dimensions}."
            )
        return DataIdSet(
            self._dimensions,
            # This mapping union can lead to mappings with heterogeneously
            # populated values (e.g. some may have records, while others may
            # not).  But this is okay because we always rely on the
            # has_implied_values and has_dimension_values to set minimum
            # expectations for what's in them.
            {**self._mapping, **other._mapping},
            self._has_implied_values and other._has_implied_values,
            self._has_dimension_records and other._has_dimension_records,
        )

    def project(self, dimensions: DimensionGroup | Iterable[str]) -> DataIdSet:
        dimensions = self._dimensions.universe.conform(dimensions)
        has_implied_values: bool = False
        iterable: Iterable[tuple[tuple[DataIdValue, ...], _MappingValue]]
        if self._dimensions.required >= dimensions.names:
            indexer = [self._dimensions._data_coordinate_indices[k] for k in dimensions.data_coordinate_keys]
            iterable = self._mapping.items()
            has_implied_values = bool(dimensions.implied)
        elif self._has_implied_values and self._dimensions.names >= dimensions.names:
            indexer = [self._dimensions._data_coordinate_indices[k] for k in dimensions.data_coordinate_keys]
            iterable = ((r + v.implied, v) for r, v in self._mapping.items())
            has_implied_values = bool(dimensions.implied)
        elif self._dimensions.required >= dimensions.required:
            indexer = [self._dimensions._data_coordinate_indices[k] for k in dimensions.required]
            iterable = self._mapping.items()
        elif self._has_implied_values and self._dimensions.names >= dimensions.required:
            indexer = [self._dimensions._data_coordinate_indices[k] for k in dimensions.required]
            iterable = ((r + v.implied, v) for r, v in self._mapping.items())
        else:
            raise ValueError(
                f"Dimensions {dimensions.required} are not a subset of "
                f"{self._dimensions if self._has_implied_values else self._dimensions.required}."
            )
        record_indexer: list[int] | None = None
        if ((has_implied_values or not dimensions.implied) and self._has_dimension_records) or not dimensions:
            current_element_indices = {k: i for i, k in enumerate(self._dimensions.elements)}
            record_indexer = [current_element_indices[k] for k in dimensions.elements]
        mapping: dict[tuple[DataIdValue, ...], _MappingValue] = {}
        n_required = len(dimensions.required)
        records: tuple[DimensionRecord | None, ...] = ()
        for full, mapping_value in iterable:
            new_values = tuple([full[index] for index in indexer])
            if record_indexer is not None:
                records = tuple([mapping_value.records[index] for index in record_indexer])
            mapping[new_values[:n_required]] = _MappingValue(implied=new_values[n_required:], records=records)
        return DataIdSet(dimensions, mapping, has_implied_values, record_indexer is not None)

    @final
    def _required_only_factory(self, key: tuple[DataIdValue, ...], value: _MappingValue) -> DataCoordinate:
        return _RequiredTupleDataCoordinate(self._dimensions, key)

    @final
    def _nothing_implied_factory(self, key: tuple[DataIdValue, ...], value: _MappingValue) -> DataCoordinate:
        return _FullTupleDataCoordinate(self._dimensions, key)

    @final
    def _add_implied_factory(self, key: tuple[DataIdValue, ...], value: _MappingValue) -> DataCoordinate:
        return _FullTupleDataCoordinate(self._dimensions, key + value.implied)

    @final
    def _nothing_implied_with_records_factory(
        self, key: tuple[DataIdValue, ...], value: _MappingValue
    ) -> DataCoordinate:
        return _ExpandedTupleDataCoordinate(
            self._dimensions, key, dict(zip(self._dimensions.elements, value.records))
        )

    @final
    def _add_implied_with_records_factory(
        self, key: tuple[DataIdValue, ...], value: _MappingValue
    ) -> DataCoordinate:
        return _ExpandedTupleDataCoordinate(
            self._dimensions, key + value.implied, dict(zip(self._dimensions.elements, value.records))
        )

    @final
    def _completeness(self) -> int:
        return self._has_implied_values + self._has_dimension_records

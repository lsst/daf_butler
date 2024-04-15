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
from collections.abc import Collection, Iterable, Iterator, Mapping, Sequence, Set
from typing import TYPE_CHECKING, cast, final

from ._coordinate import (
    DataCoordinate,
    DataIdValue,
    DimensionElement,
    _ExpandedTupleDataCoordinate,
    _FullTupleDataCoordinate,
    _RequiredTupleDataCoordinate,
)
from ._group import DimensionGroup
from ._records import DimensionRecord
from ._universe import DimensionUniverse

if TYPE_CHECKING:
    from ._record_set import DimensionRecordSet


class DataIdSet(Collection[DataCoordinate]):
    """A set-like collection of `DataCoordinate` objects.

    The constructor is a low-level interface intended primarily for other
    middleware callers; most users should use `from_data_ids` instead.

    Parameters
    ----------
    dimensions : `DimensionGroup`
        Dimensions of the data IDs in this set.
    values_mapping : `~collections.abc.Mapping` [ `tuple`, `tuple` ]
        A mapping from the required values of each data ID to its implied
        values, if present.
    has_implied_values : `bool`
        If `True`, implied values are present.  If `False`, the values of the
        ``values_mapping`` are empty tuples.  Note that this is not quite the
        same as whether the data IDs have "full" values; if there are no
        implied dimensions, ``has_implied_values=False`` but  `has_full_values`
        will be `True`.
    record_lookup_sets : `~collections.abc.Mapping` [ `str`, \
            `_DimensionRecordLookupSet` ] or `None`, optional
        A structure containing dimension records to be attached to
        `DataCoordinate` objects when they are obtained from this set.  This
        must have all records that are relevant for the data IDs in the set,
        but it may have other records that are not relevant as well.  If not
        `None`, the set must have full values, either because
        ``has_implied_values=True`` or because there are no implied dimensions.
    """

    def __init__(
        self,
        dimensions: DimensionGroup,
        values_mapping: Mapping[tuple[DataIdValue, ...], tuple[DataIdValue, ...]],
        has_implied_values: bool,
        record_lookup_sets: Mapping[str, _DimensionRecordLookupSet] | None = None,
    ):
        self._dimensions = dimensions
        self._values_mapping = values_mapping
        self._has_implied_values = has_implied_values
        self._record_lookup_sets = record_lookup_sets
        assert (
            self.has_full_values or self._record_lookup_sets is None
        ), "Cannot have dimension records without full values."
        if self._has_implied_values:
            if self._record_lookup_sets is not None:
                self._factory = self._add_implied_with_records_factory
            else:
                self._factory = self._add_implied_factory
        elif self._dimensions.implied:
            if self._record_lookup_sets is not None:
                self._factory = self._nothing_implied_with_records_factory
            else:
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
        values_mapping = {}
        extracted_records: dict[str, dict[tuple[DataIdValue, ...], DimensionRecord]] | None = None
        if has_dimension_records:
            has_full_values = True
            extracted_records = {element_name: {} for element_name in dimensions.elements}
        for data_id in data_ids:
            if has_full_values:
                values_mapping[data_id.required_values] = data_id.full_values[len(dimensions.required) :]
                if extracted_records is not None:
                    for element_name, records_by_required_value in extracted_records.items():
                        record = data_id.records[element_name]
                        if record is not None:
                            records_by_required_value[record.dataId.required_values] = record
            else:
                values_mapping[data_id.required_values] = ()
            raise NotImplementedError("Extract dimension records here.")
        record_lookup_sets = None
        if extracted_records is not None:
            from ._record_set import DimensionRecordSet

            record_lookup_sets = {
                element_name: _DimensionRecordLookupSet.build(
                    dimensions,
                    DimensionRecordSet(
                        element_name, universe=dimensions.universe, _by_required_values=records_for_element
                    ),
                )
                for element_name, records_for_element in extracted_records.items()
            }
        return cls(
            dimensions,
            values_mapping,
            has_implied_values=(has_full_values and bool(dimensions.implied)),
            record_lookup_sets=record_lookup_sets,
        )

    @property
    def dimensions(self) -> DimensionGroup:
        return self._dimensions

    @property
    def has_full_values(self) -> bool:
        return self._has_implied_values or not self._dimensions.implied

    @property
    def has_dimension_records(self) -> bool:
        return self._record_lookup_sets is not None

    @property
    def required_values(self) -> Set[tuple[DataIdValue, ...]]:
        return self._values_mapping.keys()

    def __iter__(self) -> Iterator[DataCoordinate]:
        for required_values in self.required_values:
            yield self._factory(required_values, ())

    def __len__(self) -> int:
        return len(self._values_mapping)

    def __contains__(self, key: object) -> bool:
        match key:
            case DataCoordinate() if key.dimensions == self._dimensions:
                required_values = key.required_values
            case {**mapping}:
                key = DataCoordinate.standardize(
                    cast(dict[str, DataIdValue], mapping), dimensions=self._dimensions
                )
            case _:
                return False
        return required_values in self.required_values

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DataIdSet):
            return False
        return (
            self._dimensions is other._dimensions
            and self._values_mapping.keys() == other._values_mapping.keys()
        )

    def __repr__(self) -> str:
        lines = [f"DataIdSet({self.dimensions}, ["]
        for data_id in self:
            lines.append(f"    {data_id},")
        lines.append("])")
        return "\n".join(lines)

    def compressed(
        self,
        drop_implied_values: bool = True,
        drop_dimension_records: bool = True,
    ) -> DataIdSet:
        values_mapping: dict[tuple[DataIdValue, ...], tuple[DataIdValue, ...]]
        has_implied_values = self._has_implied_values
        record_lookup_sets = self._record_lookup_sets
        if drop_implied_values and has_implied_values:
            values_mapping = dict.fromkeys(self._values_mapping.keys(), ())
            record_lookup_sets = None
        if drop_dimension_records and record_lookup_sets is not None:
            record_lookup_sets = None
        return DataIdSet(self._dimensions, values_mapping, has_implied_values, record_lookup_sets)

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
        if required_values not in self._values_mapping:
            raise KeyError(required_values)
        return self._factory(required_values, ())

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
        return self._values_mapping.keys() <= other._values_mapping.keys()

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
        return self._values_mapping.keys() >= other._values_mapping.keys()

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
        return self._values_mapping.keys().isdisjoint(other._values_mapping.keys())

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
            A new record set with all elements in both sets.
        """
        if self._dimensions != other._dimensions:
            raise ValueError(
                "Invalid intersection between data ID sets with dimensions "
                f"{self._dimensions} and {other._dimensions}."
            )
        implied_mapping = self._values_mapping if self._has_implied_values else other._values_mapping
        return DataIdSet(
            self._dimensions,
            {k: implied_mapping[k] for k in self._values_mapping.keys() & other._values_mapping.keys()},
            self._has_implied_values or other._has_implied_values,
            self._record_lookup_sets or other._record_lookup_sets,
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
            ``other``.
        """
        if self._dimensions != other._dimensions:
            raise ValueError(
                "Invalid difference between data ID sets with dimensions "
                f"{self._dimensions} and {other.dimensions}."
            )
        return DataIdSet(
            self._dimensions,
            {k: self._values_mapping[k] for k in self.required_values - other.required_values},
            self._has_implied_values,
            self._record_lookup_sets,
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
            A new record set with all elements in either set.
        """
        if self._dimensions != other._dimensions:
            raise ValueError(
                "Invalid union between data ID sets with dimensions "
                f"{self._dimensions} and {other.dimensions}."
            )
        if self._has_implied_values == other._has_implied_values:
            # Both operands have implied values or neither does: mapping union
            # does exactly what we want (because we assume implied values must
            # be the same in both operands when the required values are).
            values_mapping = {**self._values_mapping, **other._values_mapping}
        else:
            # Only one operand has implied values; drop them in the result.
            values_mapping = dict.fromkeys(self._values_mapping.keys() | other._values_mapping.keys(), ())
        record_lookup_sets = None
        if self._record_lookup_sets is not None and other._record_lookup_sets is not None:
            record_lookup_sets = {
                element_name: lookup_set.union(other._record_lookup_sets[element_name])
                for element_name, lookup_set in self._record_lookup_sets.items()
            }
        return DataIdSet(
            self._dimensions,
            values_mapping,
            self._has_implied_values and other._has_implied_values,
            record_lookup_sets,
        )

    def project(self, dimensions: DimensionGroup | Iterable[str]) -> DataIdSet:
        dimensions = self._dimensions.universe.conform(dimensions)
        has_implied_values: bool = False
        iterable: Iterable[tuple[DataIdValue, ...]]
        if self._dimensions.required >= dimensions.names:
            index_map = [
                self._dimensions._data_coordinate_indices[k] for k in dimensions.data_coordinate_keys
            ]
            iterable = self._values_mapping.keys()
            has_implied_values = bool(dimensions.implied)
        elif self._has_implied_values and self._dimensions.names >= dimensions.names:
            index_map = [
                self._dimensions._data_coordinate_indices[k] for k in dimensions.data_coordinate_keys
            ]
            iterable = (r + i for r, i in self._values_mapping.items())
            has_implied_values = bool(dimensions.implied)
        elif self._dimensions.required >= dimensions.required:
            index_map = [self._dimensions._data_coordinate_indices[k] for k in dimensions.required]
            iterable = self._values_mapping.keys()
        elif self._has_implied_values and self._dimensions.names >= dimensions.required:
            index_map = [self._dimensions._data_coordinate_indices[k] for k in dimensions.required]
            iterable = (r + i for r, i in self._values_mapping.items())
        else:
            raise ValueError(
                f"Dimensions {dimensions.required} are not a subset of "
                f"{self._dimensions if self._has_implied_values else self._dimensions.required}."
            )
        values_mapping: dict[tuple[DataIdValue, ...], tuple[DataIdValue, ...]] = {}
        n_required = len(dimensions.required)
        for original_values in iterable:
            new_values = tuple([original_values[index] for index in index_map])
            values_mapping[new_values[:n_required]] = new_values[n_required:]
        record_lookup_sets: dict[str, _DimensionRecordLookupSet] | None = None
        if self._record_lookup_sets is not None and (has_implied_values or not dimensions.implied):
            record_lookup_sets = {
                element_name: _DimensionRecordLookupSet.build(dimensions, lookup_set.records)
                for element_name, lookup_set in self._record_lookup_sets.items()
            }
        return DataIdSet(dimensions, values_mapping, has_implied_values, record_lookup_sets)

    @final
    def _required_only_factory(
        self, required_values: tuple[DataIdValue, ...], implied_values: tuple[DataIdValue, ...]
    ) -> DataCoordinate:
        return _RequiredTupleDataCoordinate(self._dimensions, required_values)

    @final
    def _nothing_implied_factory(
        self, required_values: tuple[DataIdValue, ...], implied_values: tuple[DataIdValue, ...]
    ) -> DataCoordinate:
        return _FullTupleDataCoordinate(self._dimensions, required_values)

    @final
    def _add_implied_factory(
        self, required_values: tuple[DataIdValue, ...], implied_values: tuple[DataIdValue, ...]
    ) -> DataCoordinate:
        return _FullTupleDataCoordinate(self._dimensions, required_values + implied_values)

    @final
    def _nothing_implied_with_records_factory(
        self, required_values: tuple[DataIdValue, ...], implied_values: tuple[DataIdValue, ...]
    ) -> DataCoordinate:
        record_lookup_sets = cast(Mapping[str, _DimensionRecordLookupSet], self._record_lookup_sets)
        return _ExpandedTupleDataCoordinate(
            self._dimensions,
            required_values,
            {
                element_name: lookup_set[required_values]
                for element_name, lookup_set in record_lookup_sets.items()
            },
        )

    @final
    def _add_implied_with_records_factory(
        self, required_values: tuple[DataIdValue, ...], implied_values: tuple[DataIdValue, ...]
    ) -> DataCoordinate:
        full_values = required_values + implied_values
        record_lookup_sets = cast(Mapping[str, _DimensionRecordLookupSet], self._record_lookup_sets)
        return _ExpandedTupleDataCoordinate(
            self._dimensions,
            full_values,
            {
                element_name: lookup_set[full_values]
                for element_name, lookup_set in record_lookup_sets.items()
            },
        )


@dataclasses.dataclass
class _DimensionRecordLookupSet:
    indexer: Sequence[int]
    records: DimensionRecordSet

    @classmethod
    def build(cls, dimensions: DimensionGroup, record_set: DimensionRecordSet) -> _DimensionRecordLookupSet:
        indexer = cls._build_indexer(dimensions, record_set.element)
        return cls(indexer, record_set)

    @staticmethod
    def _build_indexer(dimensions: DimensionGroup, element: DimensionElement) -> list[int]:
        return [dimensions._data_coordinate_indices[d] for d in element.required.names]

    def __getitem__(self, data_id_values: tuple[DataIdValue, ...]) -> DimensionRecord:
        return self.records.find_with_required_values(tuple([data_id_values[n] for n in self.indexer]))

    def union(self, other: _DimensionRecordLookupSet) -> _DimensionRecordLookupSet:
        return _DimensionRecordLookupSet(self.indexer, self.records.union(other.records, lazy=True))

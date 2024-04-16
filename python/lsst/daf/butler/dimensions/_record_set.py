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

__all__ = ("DimensionRecordSet",)

from collections import ChainMap
from collections.abc import Collection, Iterable, Iterator, KeysView, Mapping
from typing import TYPE_CHECKING, ClassVar, cast, final

from ._coordinate import DataCoordinate, DataIdValue
from ._records import DimensionRecord

if TYPE_CHECKING:
    from ._data_id_set import DataIdSet, _MappingValue
    from ._elements import DimensionElement
    from ._universe import DimensionUniverse


@final
class DimensionRecordSet(Collection[DimensionRecord]):  # numpydoc ignore=PR01
    """An immutable set-like container for `DimensionRecord` objects.

    Parameters
    ----------
    element : `DimensionElement` or `str`, optional
        The dimension element that defines the records held by this set. If
        not a `DimensionElement` instance, ``universe`` must be provided.
    records : `~collections.abc.Iterable` [ `DimensionRecord` ], optional
        Dimension records to add to the set.
    universe : `DimensionUniverse`, optional
        Object that defines all dimensions.  Ignored if ``element`` is a
        `DimensionElement` instance.

    Notes
    -----
    `DimensionRecordSet` maintains its insertion order (like `dict`, and unlike
    `set`).

    `DimensionRecordSet` implements `collections.abc.Collection` but not
    `collections.abc.Set` because the latter would require interoperability
    with all other `~collections.abc.Set` implementations rather than just
    `DimensionRecordSet`, and that adds a lot of complexity without much clear
    value.  To help make this clear to type checkers it implements only the
    named-method versions of these operations (e.g. `issubset`) rather than the
    operator special methods (e.g. ``__le__``).

    `DimensionRecord` equality is defined in terms of a record's data ID fields
    only, and `DimensionRecordSet` does not generally specify which record
    "wins" when two records with the same data ID interact (e.g. in
    `intersection`).

    Dimension records can also be held by `DimensionRecordTable`, which
    provides column-oriented access and Arrow interoperability.
    """

    def __init__(
        self,
        element: DimensionElement | str,
        records: Iterable[DimensionRecord] = (),
        universe: DimensionUniverse | None = None,
        *,
        _by_required_values: Mapping[tuple[DataIdValue, ...], DimensionRecord] | None = None,
    ):
        if isinstance(element, str):
            if universe is None:
                raise TypeError("'universe' must be provided if 'element' is not a DimensionElement.")
            element = universe[element]
        else:
            universe = element.universe
        self._record_type = element.RecordClass
        self._dimensions = element.minimal_group
        if _by_required_values is None:
            _by_required_values = {}
            for record in records:
                if record.definition != self.element:
                    raise ValueError(f"Cannot add record {record} to set for {self.element!r}.")
                _by_required_values[record.dataId.required_values] = record
        else:
            assert records == (), "records may not be passed if _by_required_values is."
        self._by_required_values: Mapping[tuple[DataIdValue, ...], DimensionRecord] = _by_required_values

    @property
    def element(self) -> DimensionElement:
        """Name of the dimension element these records correspond to."""
        return self._record_type.definition

    @property
    def data_ids(self) -> DataIdSet:
        from ._data_id_set import DataIdSet

        return DataIdSet(
            self.element.minimal_group,
            _DataIdMappingAdapter(self),
            has_implied_values=True,
            has_dimension_records=False,
        )

    def __contains__(self, key: object) -> bool:
        match key:
            case DimensionRecord() if key.definition == self.element:
                required_values = key.dataId.required_values
            case DataCoordinate() if key.dimensions == self.element.minimal_group:
                required_values = key.required_values
            case {**mapping}:
                key = DataCoordinate.standardize(
                    cast(dict[str, DataIdValue], mapping), dimensions=self.element.minimal_group
                )
            case _:
                return False
        return required_values in self._by_required_values

    def __len__(self) -> int:
        return len(self._by_required_values)

    def __iter__(self) -> Iterator[DimensionRecord]:
        return iter(self._by_required_values.values())

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DimensionRecordSet):
            return False
        return (
            self._record_type is other._record_type
            and self._by_required_values.keys() == other._by_required_values.keys()
        )

    def __repr__(self) -> str:
        lines = [f"DimensionRecordSet({self.element.name}, {{"]
        for record in self:
            lines.append(f"    {record!r},")
        lines.append("})")
        return "\n".join(lines)

    def issubset(self, other: DimensionRecordSet) -> bool:
        """Test whether all elements in ``self`` are in ``other``.

        Parameters
        ----------
        other : `DimensionRecordSet`
            Another record set with the same record type.

        Returns
        -------
        issubset ; `bool`
            Whether all elements in ``self`` are in ``other``.
        """
        if self._record_type is not other._record_type:
            raise ValueError(
                "Invalid comparison between dimension record sets for elements "
                f"{self.element.name!r} and {other.element.name!r}."
            )
        return self._by_required_values.keys() <= other._by_required_values.keys()

    def issuperset(self, other: DimensionRecordSet) -> bool:
        """Test whether all elements in ``other`` are in ``self``.

        Parameters
        ----------
        other : `DimensionRecordSet`
            Another record set with the same record type.

        Returns
        -------
        issuperset ; `bool`
            Whether all elements in ``other`` are in ``self``.
        """
        if self._record_type is not other._record_type:
            raise ValueError(
                "Invalid comparison between dimension record sets for elements "
                f"{self.element.name!r} and {other.element.name!r}."
            )
        return self._by_required_values.keys() >= other._by_required_values.keys()

    def isdisjoint(self, other: DimensionRecordSet) -> bool:
        """Test whether the intersection of ``self`` and ``other`` is empty.

        Parameters
        ----------
        other : `DimensionRecordSet`
            Another record set with the same record type.

        Returns
        -------
        isdisjoint ; `bool`
            Whether the intersection of ``self`` and ``other`` is empty.
        """
        if self._record_type is not other._record_type:
            raise ValueError(
                "Invalid comparison between dimension record sets for elements "
                f"{self.element.name!r} and {other.element.name!r}."
            )
        return self._by_required_values.keys().isdisjoint(other._by_required_values.keys())

    def intersection(self, other: DimensionRecordSet) -> DimensionRecordSet:
        """Return a new set with only records that are in both ``self`` and
        ``other``.

        Parameters
        ----------
        other : `DimensionRecordSet`
            Another record set with the same record type.

        Returns
        -------
        intersection : `DimensionRecordSet`
            A new record set with all elements in both sets.
        """
        if self._record_type is not other._record_type:
            raise ValueError(
                "Invalid intersection between dimension record sets for elements "
                f"{self.element.name!r} and {other.element.name!r}."
            )
        return DimensionRecordSet(
            self.element,
            _by_required_values={
                k: v for k, v in self._by_required_values.items() if k in other._by_required_values
            },
        )

    def difference(self, other: DimensionRecordSet) -> DimensionRecordSet:
        """Return a new set with only records that are in ``self`` and not in
        ``other``.

        Parameters
        ----------
        other : `DimensionRecordSet`
            Another record set with the same record type.

        Returns
        -------
        difference : `DimensionRecordSet`
            A new record set with all elements ``self`` that are not in
            ``other``.
        """
        if self._record_type is not other._record_type:
            raise ValueError(
                "Invalid difference between dimension record sets for elements "
                f"{self.element.name!r} and {other.element.name!r}."
            )
        return DimensionRecordSet(
            self.element,
            _by_required_values={
                k: v for k, v in self._by_required_values.items() if k not in other._by_required_values
            },
        )

    def union(self, other: DimensionRecordSet, lazy: bool = False) -> DimensionRecordSet:
        """Return a new set with all records that are either in ``self`` or
        ``other``.

        Parameters
        ----------
        other : `DimensionRecordSet`
            Another record set with the same record type.
        lazy : `bool`, optional
            If `True`, defer actually computing the union until iteration or
            lookup.

        Returns
        -------
        union : `DimensionRecordSet`
            A new record set with all elements in either set.
        """
        if self._record_type is not other._record_type:
            raise ValueError(
                "Invalid union between dimension record sets for elements "
                f"{self.element.name!r} and {other.element.name!r}."
            )
        by_required_values: Mapping[tuple[DataIdValue, ...], DimensionRecord]
        if lazy:
            # ChainMap is a MutableMapping that wants MutableMapping args.  But
            # we're not going to use the mutability and annotate it as just
            # Mapping to enforce that, so it's okay to give it non-mutable
            # arguments.
            by_required_values = ChainMap(self._by_required_values, other._by_required_values)  # type: ignore
        else:
            by_required_values = {**self._by_required_values, **other._by_required_values}
        return DimensionRecordSet(self.element, _by_required_values=by_required_values)

    def find(self, data_id: DataCoordinate) -> DimensionRecord:
        """Return the record with the given data ID.

        Parameters
        ----------
        data_id : `DataCoordinate`
            Data ID to match.

        Returns
        -------
        record : `DimensionRecord`
            Matching record.

        Raises
        ------
        KeyError
            Raised if no record with this data ID was found.
        ValueError
            Raised if the data ID did not have the right dimensions.
        """
        if data_id.dimensions != self._dimensions:
            raise ValueError(
                f"data ID {data_id} has incorrect dimensions for dimension records for {self.element!r}."
            )
        return self.find_with_required_values(data_id.required_values)

    def find_with_required_values(self, required_values: tuple[DataIdValue, ...]) -> DimensionRecord:
        """Return the record whose data ID has the given required values.

        Parameters
        ----------
        required_values : `tuple` [ `int` or `str` ]
            Data ID values to match.

        Returns
        -------
        record : `DimensionRecord`
            Matching record.

        Raises
        ------
        KeyError
            Raised if no record with these values were found.
        """
        return self._by_required_values[required_values]


class _DataIdMappingAdapter(Mapping[tuple[DataIdValue, ...], _MappingValue]):
    """A mapping adapter for `DimensionRecordSet` that can be used to back a
    `DataIdSet`.

    The keys of this dictionary are data ID 'required values' tuples and the
    values are empty tuples (representing 'implied' data ID values, which this
    adapter does not provide.

    Parameters
    ----------
    parent : `DimensionRecordSet`
        Record set to adapt.
    """

    def __init__(self, parent: DimensionRecordSet) -> None:
        self._parent = parent

    _value: ClassVar[_MappingValue] = _MappingValue()

    def __iter__(self) -> Iterator[tuple[DataIdValue, ...]]:
        return iter(self._parent._by_required_values.keys())

    def __getitem__(self, key: tuple[DataIdValue, ...]) -> _MappingValue:
        if key not in self._parent._by_required_values:
            raise KeyError(key)
        return self._value

    def __len__(self) -> int:
        return len(self._parent._by_required_values)

    def keys(self) -> KeysView[tuple[DataIdValue, ...]]:
        return self._parent._by_required_values.keys()

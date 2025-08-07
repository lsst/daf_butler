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

__all__ = (
    "DimensionDataAttacher",
    "DimensionDataExtractor",
    "DimensionRecordFactory",
    "DimensionRecordSet",
    "DimensionRecordSetDeserializer",
    "SerializableDimensionData",
)

import dataclasses
from collections.abc import Collection, Iterable, Iterator
from typing import TYPE_CHECKING, Any, Protocol, Self, TypeAlias, final

import pydantic

from ._coordinate import DataCoordinate, DataIdValue
from ._records import DimensionRecord, SerializedKeyValueDimensionRecord

if TYPE_CHECKING:
    from ..queries import Query
    from ._elements import DimensionElement
    from ._group import DimensionGroup
    from ._skypix import SkyPixDimension
    from ._universe import DimensionUniverse
    from .record_cache import DimensionRecordCache


SerializedDimensionRecordSetMapping: TypeAlias = dict[str, list[SerializedKeyValueDimensionRecord]]


class DimensionRecordFactory(Protocol):
    """Protocol for a callback that can be used to create a dimension record
    to add to a `DimensionRecordSet` when a search for an existing one fails.
    """

    def __call__(
        self, record_class: type[DimensionRecord], required_values: tuple[DataIdValue, ...]
    ) -> DimensionRecord:
        """Make a new `DimensionRecord` instance.

        Parameters
        ----------
        record_class : `type` [ `DimensionRecord` ]
            A concrete `DimensionRecord` subclass.
        required_values : `tuple`
            Tuple of data ID values, corresponding to
            ``record_class.definition.required``.
        """
        ...  # pragma: no cover


def fail_record_lookup(
    record_class: type[DimensionRecord], required_values: tuple[DataIdValue, ...]
) -> DimensionRecord:
    """Raise `LookupError` to indicate that a `DimensionRecord` could not be
    found or created.

    This is intended for use as the default value for arguments that take a
    `DimensionRecordFactory` callback.

    Parameters
    ----------
    record_class : `type` [ `DimensionRecord` ]
        Type of record to create.
    required_values : `tuple`
        Tuple of data ID required values that are sufficient to identify a
        record that exists in the data repository.

    Returns
    -------
    record :  `DimensionRecord`
        Never returned; this function always raises `LookupError`.
    """
    raise LookupError(
        f"No {record_class.definition.name!r} record with data ID "
        f"{DataCoordinate.from_required_values(record_class.definition.minimal_group, required_values)}."
    )


@final
class DimensionRecordSet(Collection[DimensionRecord]):  # numpydoc ignore=PR01
    """A mutable set-like container specialized for `DimensionRecord` objects.

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
    `intersection`).  The `add` and `update` methods are notable exceptions:
    they always replace the existing record with the new one.

    Dimension records can also be held by `DimensionRecordTable`, which
    provides column-oriented access and Arrow interoperability.
    """

    def __init__(
        self,
        element: DimensionElement | str,
        records: Iterable[DimensionRecord] = (),
        universe: DimensionUniverse | None = None,
        *,
        _by_required_values: dict[tuple[DataIdValue, ...], DimensionRecord] | None = None,
    ):
        if isinstance(element, str):
            if universe is None:
                raise TypeError("'universe' must be provided if 'element' is not a DimensionElement.")
            element = universe[element]
        else:
            universe = element.universe
        if _by_required_values is None:
            _by_required_values = {}
        self._record_type = element.RecordClass
        self._by_required_values = _by_required_values
        self._dimensions = element.minimal_group
        self.update(records)

    @property
    def element(self) -> DimensionElement:
        """Name of the dimension element these records correspond to."""
        return self._record_type.definition

    def __contains__(self, key: object) -> bool:
        match key:
            case DimensionRecord() if key.definition == self.element:
                required_values = key.dataId.required_values
            case DataCoordinate() if key.dimensions == self.element.minimal_group:
                required_values = key.required_values
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

    def union(self, other: DimensionRecordSet) -> DimensionRecordSet:
        """Return a new set with all records that are either in ``self`` or
        ``other``.

        Parameters
        ----------
        other : `DimensionRecordSet`
            Another record set with the same record type.

        Returns
        -------
        intersection : `DimensionRecordSet`
            A new record set with all elements in either set.
        """
        if self._record_type is not other._record_type:
            raise ValueError(
                "Invalid union between dimension record sets for elements "
                f"{self.element.name!r} and {other.element.name!r}."
            )
        return DimensionRecordSet(
            self.element,
            _by_required_values=self._by_required_values | other._by_required_values,
        )

    def find(
        self,
        data_id: DataCoordinate,
        or_add: DimensionRecordFactory = fail_record_lookup,
    ) -> DimensionRecord:
        """Return the record with the given data ID.

        Parameters
        ----------
        data_id : `DataCoordinate`
            Data ID to match.
        or_add : `DimensionRecordFactory`
            Callback that is invoked if no existing record is found, to create
            a new record that is added to the set and returned.  The return
            value of this callback is *not* checked to see if it is a valid
            dimension record with the right element and data ID.

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
        return self.find_with_required_values(data_id.required_values, or_add)

    def find_with_required_values(
        self, required_values: tuple[DataIdValue, ...], or_add: DimensionRecordFactory = fail_record_lookup
    ) -> DimensionRecord:
        """Return the record whose data ID has the given required values.

        Parameters
        ----------
        required_values : `tuple` [ `int` or `str` ]
            Data ID values to match.
        or_add : `DimensionRecordFactory`
            Callback that is invoked if no existing record is found, to create
            a new record that is added to the set and returned.  The return
            value of this callback is *not* checked to see if it is a valid
            dimension record with the right element and data ID.

        Returns
        -------
        record : `DimensionRecord`
            Matching record.

        Raises
        ------
        ValueError
            Raised if the data ID did not have the right dimensions.
        """
        if (result := self._by_required_values.get(required_values)) is None:
            result = or_add(self._record_type, required_values)
            self._by_required_values[required_values] = result
        return result

    def add(self, value: DimensionRecord, replace: bool = True) -> None:
        """Add a new record to the set.

        Parameters
        ----------
        value : `DimensionRecord`
            Record to add.
        replace : `bool`, optional
            If `True` (default) replace any existing record with the same data
            ID.  If `False` the existing record will be kept.

        Raises
        ------
        ValueError
            Raised if ``value.element != self.element``.
        """
        if value.definition.name != self.element:
            raise ValueError(
                f"Cannot add record {value} for {value.definition.name!r} to set for {self.element!r}."
            )
        if replace:
            self._by_required_values[value.dataId.required_values] = value
        else:
            self._by_required_values.setdefault(value.dataId.required_values, value)

    def update(self, values: Iterable[DimensionRecord], replace: bool = True) -> None:
        """Add new records to the set.

        Parameters
        ----------
        values : `~collections.abc.Iterable` [ `DimensionRecord` ]
            Records to add.
        replace : `bool`, optional
            If `True` (default) replace any existing records with the same data
            IDs.  If `False` the existing records will be kept.

        Raises
        ------
        ValueError
            Raised if ``value.element != self.element``.
        """
        for value in values:
            self.add(value, replace=replace)

    def update_from_data_coordinates(self, data_coordinates: Iterable[DataCoordinate]) -> None:
        """Add records to the set by extracting and deduplicating them from
        data coordinates.

        Parameters
        ----------
        data_coordinates : `~collections.abc.Iterable` [ `DataCoordinate` ]
            Data coordinates to extract from.  `DataCoordinate.hasRecords` must
            be `True`.
        """
        for data_coordinate in data_coordinates:
            if record := data_coordinate._record(self.element.name):
                self._by_required_values[record.dataId.required_values] = record

    def discard(self, value: DimensionRecord | DataCoordinate) -> None:
        """Remove a record if it exists.

        Parameters
        ----------
        value : `DimensionRecord` or `DataCoordinate`
            Record to remove, or its data ID.
        """
        if isinstance(value, DimensionRecord):
            value = value.dataId
        if value.dimensions != self._dimensions:
            raise ValueError(f"{value} has incorrect dimensions for dimension records for {self.element!r}.")
        self._by_required_values.pop(value.required_values, None)

    def remove(self, value: DimensionRecord | DataCoordinate) -> None:
        """Remove a record.

        Parameters
        ----------
        value : `DimensionRecord` or `DataCoordinate`
            Record to remove, or its data ID.

        Raises
        ------
        KeyError
            Raised if there is no matching record.
        """
        if isinstance(value, DimensionRecord):
            value = value.dataId
        if value.dimensions != self._dimensions:
            raise ValueError(f"{value} has incorrect dimensions for dimension records for {self.element!r}.")
        del self._by_required_values[value.required_values]

    def pop(self) -> DimensionRecord:
        """Remove and return an arbitrary record."""
        return self._by_required_values.popitem()[1]

    def __deepcopy__(self, memo: dict[str, Any]) -> DimensionRecordSet:
        return DimensionRecordSet(self.element, _by_required_values=self._by_required_values.copy())

    def serialize_records(self) -> list[SerializedKeyValueDimensionRecord]:
        """Serialize the records to a list.

        Returns
        -------
        raw_records : `list` [ `list` ]
            Serialized records, in the form returned by
            `DimensionRecord.serialize_key_value`.

        Notes
        -----
        This does not include the dimension element shared by all of the
        records, on the assumption that this is usually more conveniently saved
        separately (e.g. as the key of a dictionary of which the list of
        records is a value).
        """
        return [record.serialize_key_value() for record in self]

    def deserialize_records(self, raw_records: Iterable[SerializedKeyValueDimensionRecord]) -> None:
        """Deserialize records and add them to this set.

        Parameters
        ----------
        raw_records : `~collections.abc.Iterable` [ `list` ]
            Serialized records, as returned by `serialize_records` or repeated
            calls to `DimensionRecord.serialize_key_value`.

        Notes
        -----
        The caller is responsible for ensuring that the serialized records have
        the same dimension element as this set, as this cannot be checked.
        Mismatches will probably result in a (confusing) type-validation error,
        but are not guaranteed to.
        """
        deserializer = DimensionRecordSetDeserializer.from_raw(self.element, raw_records)
        self.update(deserializer)


class DimensionRecordSetDeserializer:
    """A helper class for deserializing sets of dimension records, with support
    for only fully deserializing certain records.

    The `from_raw` factory method should generally be used instead of calling
    the constructor directly.

    Parameters
    ----------
    element : `DimensionElement`
        Dimension element that defines all records.
    mapping : `dict` [ `tuple`, `list` ]
        A dictionary that maps the data ID required-values `tuple` for reach
        record to the remainder of its raw serialization (i.e. an item in this
        `dict` is a pair returned by `DimensionRecord.deserialize_key`).  This
        `dict` will be used directly to back the deserializer, not copied.

    Notes
    -----
    The keys (data ID required-values tuples) of all rows are deserialized
    immediately, but the remaining fields are deserialized only on demand; use
    `__iter__` to deserialize all records or `__getitem__` to deserialize only
    a few.  An instance should really only be used for a single iteration or
    multiple `__getitem__` calls, as each call will re-deserialize the records
    in play; deserialized records are not cached.

    The caller is responsible for ensuring that the serialized records are for
    the given dimension element, as this cannot be checked. Mismatches will
    probably result in a (confusing) type-validation error, but are not
    guaranteed to.
    """

    def __init__(
        self,
        element: DimensionElement,
        mapping: dict[tuple[DataIdValue, ...], SerializedKeyValueDimensionRecord],
    ):
        self.element = element
        self._mapping = mapping

    @classmethod
    def from_raw(
        cls, element: DimensionElement, raw_records: Iterable[SerializedKeyValueDimensionRecord]
    ) -> Self:
        """Construct from raw serialized records.

        Parameters
        ----------
        element : `DimensionElement`
            Dimension element that defines all records.
        raw_records : `~collections.abc.Iterable` [ `list` ]
            Serialized records, as returned by
            `DimensionRecordSet.serialize_records` or repeated calls to
            `DimensionRecord.serialize_key_value`.

        Returns
        -------
        deserializer : `DimensionRecordSetDeserializer`
            New deserializer instance.
        """
        return cls(element=element, mapping=dict(map(element.RecordClass.deserialize_key, raw_records)))

    def __len__(self) -> int:
        return len(self._mapping)

    def __iter__(self) -> Iterator[DimensionRecord]:
        deserialize = self.element.RecordClass.deserialize_value
        return (deserialize(k, v) for k, v in self._mapping.items())

    def __getitem__(self, key: tuple[DataIdValue, ...]) -> DimensionRecord:
        return self.element.RecordClass.deserialize_value(key, self._mapping[key])


@dataclasses.dataclass
class DimensionDataExtractor:
    """A helper class for extracting dimension records from expanded data IDs
    (e.g. for normalized serialization).

    Instances of this class must be initialized with empty sets (usually by one
    of the class method factories) with all of the dimension elements that
    should be extracted from the data IDs passed to `update_homogeneous` or
    `update_heterogeneous`.  Dimension elements not included will not be
    extracted (which may be useful).
    """

    records: dict[str, DimensionRecordSet] = dataclasses.field(default_factory=dict)

    @classmethod
    def from_element_names(
        cls, element_names: Iterable[str], universe: DimensionUniverse
    ) -> DimensionDataExtractor:
        """Construct from an iterable of dimension element names.

        Parameters
        ----------
        element_names : `~collections.abc.Iterable` [ `str` ]
            Names of dimension elements to include.
        universe : `DimensionUniverse`
            Definitions of all dimensions.

        Returns
        -------
        extractor : `DimensionDataExtractor`
            New extractor.
        """
        return cls(
            records={
                element_name: DimensionRecordSet(element_name, universe=universe)
                for element_name in element_names
            }
        )

    @classmethod
    def from_dimension_group(
        cls,
        dimensions: DimensionGroup,
        *,
        ignore: Iterable[str] = (),
        ignore_cached: bool = False,
        include_skypix: bool = False,
    ) -> DimensionDataExtractor:
        """Construct from a `DimensionGroup` and a set of dimension element
        names to ignore.

        Parameters
        ----------
        dimensions : `DimensionGroup`
            Dimensions that span the set of elements whose elements are to be
            extracted.
        ignore : `~collections.abc.Iterable` [ `str` ], optional
            Names of dimension elements that should not be extracted.
        ignore_cached : `bool`, optional
            If `True`, ignore all dimension elements for which
            `DimensionElement.is_cached` is `True`.
        include_skypix : `bool`, optional
            If `True`, include skypix dimensions.  These are ignored by default
            because they can always be recomputed from their IDs on-the-fly.

        Returns
        -------
        extractor : `DimensionDataExtractor`
            New extractor.
        """
        elements = set(dimensions.elements)
        elements.difference_update(ignore)
        if ignore_cached:
            elements.difference_update([e for e in elements if dimensions.universe[e].is_cached])
        if not include_skypix:
            elements.difference_update(dimensions.skypix)
        return cls.from_element_names(elements, universe=dimensions.universe)

    def update(self, data_ids: Iterable[DataCoordinate]) -> None:
        """Extract dimension records from an iterable of data IDs.

        Parameters
        ----------
        data_ids : `~collections.abc.Iterable` [ `DataCoordinate` ]
            Data IDs to extract dimension records from.
        """
        for data_id in data_ids:
            for element in data_id.dimensions.elements & self.records.keys():
                if (record := data_id.records[element]) is not None:
                    self.records[element].add(record)


class SerializableDimensionData(pydantic.RootModel):
    """A pydantic model for normalized serialization of dimension records.

    While dimension records are serialized directly via this model, they are
    deserialized by constructing a `DimensionRecordSetDeserializer` from this
    model, which allows full validation to be performed only on the records
    that are actually loaded.
    """

    root: dict[str, list[SerializedKeyValueDimensionRecord]] = pydantic.Field(default_factory=dict)

    @classmethod
    def from_record_sets(cls, record_sets: Iterable[DimensionRecordSet]) -> SerializableDimensionData:
        """Construct from an iterable of `DimensionRecordSet` objects.

        Parameters
        ----------
        record_sets : `~collections.abc.Iterable` [ `DimensionRecordSet` ]
            Sets of dimension records, each for a different dimension element.

        Returns
        -------
        model : `SerializableDimensionData`
            New model instance.
        """
        return cls.model_construct(
            root={record_set.element.name: record_set.serialize_records() for record_set in record_sets}
        )

    def make_deserializers(self, universe: DimensionUniverse) -> list[DimensionRecordSetDeserializer]:
        """Make objects from this model that handle the second phase of
        deserialization.

        Parameters
        ----------
        universe : `DimensionUniverse`
            Definitions of all dimensions.

        Returns
        -------
        deserializers : `list` [ `DimensionRecordSetDeserializer` ]
            A list of deserializers objects, one for each dimension element.
        """
        return [
            DimensionRecordSetDeserializer.from_raw(universe[element_name], raw_records)
            for element_name, raw_records in self.root.items()
        ]


class DimensionDataAttacher:
    """A helper class for attaching dimension records to data IDs.

    Parameters
    ----------
    records : `dict` [`str`, `DimensionRecordSet`], optional
        Regular dimension record sets, keyed by dimension element name.  Not
        copied, and may be modified in-place.
    deserializers : `dict` [`str`, `DimensionRecordSetDeserializer`], optional
        Partially-deserialized dimension records, keyed by dimension element
        name.  Records will be fully deserialized on demand and then cached.
    cache : `DimensionRecordCache`, optional
        A cache of dimension records from a butler instance. If present, this
        is assumed to have records for elements that are not in ``records`` and
        ``deserializers``.
    dimensions : `DimensionGroup`, optional
        Dimensions for which empty record sets should be added when no other
        source of records is given.  This allows data IDs with these dimensions
        to have records attached by fetching them via the ``query`` argument
        to the ``attach`` method, or by computing regions on the skypix
        dimensions.
    """

    def __init__(
        self,
        *,
        records: Iterable[DimensionRecordSet] = (),
        deserializers: Iterable[DimensionRecordSetDeserializer] = (),
        cache: DimensionRecordCache | None = None,
        dimensions: DimensionGroup | None = None,
    ):
        self.records = {record_set.element.name: record_set for record_set in records}
        self.deserializers: dict[str, DimensionRecordSetDeserializer] = {}
        for deserializer in deserializers:
            self.deserializers[deserializer.element.name] = deserializer
            if deserializer.element.name not in self.records:
                self.records[deserializer.element.name] = DimensionRecordSet(deserializer.element)
        self.cache = cache
        if dimensions is not None:
            for element in dimensions.elements:
                if element not in self.records and (self.cache is None or element not in self.cache):
                    self.records[element] = DimensionRecordSet(element, universe=dimensions.universe)

    def attach(
        self, dimensions: DimensionGroup, data_ids: Iterable[DataCoordinate], query: Query | None = None
    ) -> list[DataCoordinate]:
        """Attach dimension records to data IDs.

        Parameters
        ----------
        dimensions : `DimensionGroup`
            Dimensions of all given data IDs.  All dimension elements must have
            been referenced in at least one of the constructor arguments.
        data_ids : `~collections.abc.Iterable` [ `DataCoordinate` ]
            Data IDs to attach dimension records to (not in place; data
            coordinates are immutable).
        query : `.queries.Query`, optional
            A butler query that can be used to look up missing dimension
            records.  Records fetched via query are cached in the `records`
            attribute.

        Returns
        -------
        expanded : `list` [ `DataCoordinate` ]
            Data IDs with dimension records attached, in the same order as the
            original iterable.
        """
        lookup_helpers = [
            _DimensionRecordLookupHelper.build(dimensions, element_name, self)
            for element_name in dimensions.lookup_order
        ]
        records = [_InProgressRecordDicts(data_id) for data_id in data_ids]
        for lookup_helper in lookup_helpers:
            for r in records:
                lookup_helper.lookup(r)
            incomplete = lookup_helper.incomplete_records
            if incomplete:
                if query is not None:
                    lookup_helper.fetch_missing(query)
                    # We may still be missing records at this point, if they
                    # were not available in the database.
                    # This is intentional, because in existing Butler
                    # repositories dimension records are not always fully
                    # populated. (For example, it is common for a visit to
                    # exist without corresponding visit_detector_region
                    # records, since these are populated at different times
                    # by different processes.)
                else:
                    raise LookupError(
                        f"No dimension record for element '{lookup_helper.element}' "
                        f"for data ID {incomplete[0].data_id}.  "
                        f"{len(incomplete)} data ID{' was' if len(incomplete) == 1 else 's were'} "
                        "missing at least one record."
                    )

        return [r.data_id.expanded(r.done) for r in records]

    def serialized(
        self, *, ignore: Iterable[str] = (), ignore_cached: bool = False, include_skypix: bool = False
    ) -> SerializableDimensionData:
        """Serialize all dimension data in this attacher, with deduplication
        across fully- and partially-deserialized records.

        Parameters
        ----------
        ignore : `~collections.abc.Iterable` [ `str` ], optional
            Names of dimension elements that should not be serialized.
        ignore_cached : `bool`, optional
            If `True`, ignore all dimension elements for which
            `DimensionElement.is_cached` is `True`.
        include_skypix : `bool`, optional
            If `True`, include skypix dimensions.  These are ignored by default
            because they can always be recomputed from their IDs on-the-fly.

        Returns
        -------
        serialized : `SerializedDimensionData`
            Serialized dimension records.
        """
        from ._skypix import SkyPixDimension

        ignore = set(ignore)
        result = SerializableDimensionData()
        for record_set in self.records.values():
            if record_set.element.name in ignore:
                continue
            if not include_skypix and isinstance(record_set.element, SkyPixDimension):
                continue
            if ignore_cached and record_set.element.is_cached:
                continue
            serialized_records: dict[tuple[DataIdValue, ...], SerializedKeyValueDimensionRecord] = {}
            if (deserializer := self.deserializers.get(record_set.element.name)) is not None:
                for key, value in deserializer._mapping.items():
                    serialized_record = list(key)
                    serialized_record.extend(value)
                    serialized_records[key] = serialized_record
            for key, record in record_set._by_required_values.items():
                if key not in serialized_records:
                    serialized_records[key] = record.serialize_key_value()
            result.root[record_set.element.name] = list(serialized_records.values())
        if self.cache is not None and not ignore_cached:
            for record_set in self.cache.values():
                result.root[record_set.element.name] = record_set.serialize_records()
        return result


@dataclasses.dataclass
class _InProgressRecordDicts:
    data_id: DataCoordinate
    done: dict[str, DimensionRecord] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass
class _DimensionRecordLookupHelper:
    # These are the indices of the dimension record's data ID's required_values
    # tuple in the to-be-expanded data ID's full-values tuple.
    indices: list[int]
    record_set: DimensionRecordSet
    incomplete_records: list[_InProgressRecordDicts] = dataclasses.field(default_factory=list)

    @property
    def element(self) -> str:
        return self.record_set.element.name

    @staticmethod
    def build(
        dimensions: DimensionGroup, element: str, attacher: DimensionDataAttacher
    ) -> _DimensionRecordLookupHelper:
        indices = [
            dimensions._data_coordinate_indices[k]
            for k in dimensions.universe.elements[element].minimal_group.required
        ]
        if attacher.cache is not None and element in attacher.cache:
            return _DimensionRecordLookupHelper(indices, attacher.cache[element])
        elif element in dimensions.skypix:
            return _SkyPixDimensionRecordLookupHelper(
                indices,
                attacher.records[element],
                dimension=dimensions.universe.skypix_dimensions[element],
            )
        elif element in attacher.deserializers:
            return _DeserializingDimensionRecordLookupHelper(
                indices, attacher.records[element], deserializer=attacher.deserializers[element]
            )
        else:
            return _DimensionRecordLookupHelper(indices, attacher.records[element])

    def lookup(self, records: _InProgressRecordDicts) -> None:
        required_values = self._get_required_values(records)
        if (result := self.record_set._by_required_values.get(required_values)) is None:
            result = self.fallback(required_values)
            if result is not None:
                self.record_set.add(result)
                records.done[self.element] = result
            else:
                self.incomplete_records.append(records)
        else:
            records.done[self.element] = result

    def _get_required_values(self, records: _InProgressRecordDicts) -> tuple[DataIdValue, ...]:
        if records.data_id.hasFull():
            full_values = records.data_id.full_values
            return tuple([full_values[i] for i in self.indices])
        else:
            values = []
            dimensions = self.record_set.element.minimal_group.required
            for dimension in dimensions:
                value = records.data_id.get(dimension)
                if value is None:
                    value = self._find_implied_value(dimension, records)
                values.append(value)
            return tuple(values)

    def _find_implied_value(self, implied_dimension: str, records: _InProgressRecordDicts) -> DataIdValue:
        for rec in records.done.values():
            if implied_dimension in rec.definition.implied:
                return rec.get(implied_dimension)

        raise LookupError(
            f"Implied value for dimension '{implied_dimension}' not found in records for"
            f" {list(records.done.keys())}"
        )

    def fallback(self, required_values: tuple[DataIdValue, ...]) -> DimensionRecord | None:
        return None

    def fetch_missing(self, query: Query) -> None:
        if self.incomplete_records:
            missing_values = set(self._get_required_values(r) for r in self.incomplete_records)
            self.record_set.update(
                query.join_data_coordinates(
                    [
                        DataCoordinate.from_required_values(self.record_set.element.minimal_group, values)
                        for values in missing_values
                    ]
                ).dimension_records(self.record_set.element.name)
            )

        missing = self.incomplete_records
        self.incomplete_records = list()
        for record in missing:
            self.lookup(record)


@dataclasses.dataclass
class _DeserializingDimensionRecordLookupHelper(_DimensionRecordLookupHelper):
    deserializer: DimensionRecordSetDeserializer = dataclasses.field(kw_only=True)

    def fallback(self, required_values: tuple[DataIdValue, ...]) -> DimensionRecord | None:
        try:
            return self.deserializer[required_values]
        except KeyError:
            return None


@dataclasses.dataclass
class _SkyPixDimensionRecordLookupHelper(_DimensionRecordLookupHelper):
    dimension: SkyPixDimension = dataclasses.field(kw_only=True)

    def fallback(self, required_values: tuple[DataIdValue, ...]) -> DimensionRecord:
        id = required_values[0]
        return self.dimension.RecordClass(id=id, region=self.dimension.pixelization.pixel(id))

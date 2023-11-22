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
    "DataCoordinateReader",
    "DatasetRefReader",
    "DimensionRecordReader",
)

from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping, Set
from typing import TYPE_CHECKING, Any

from lsst.utils.classes import cached_getter

from ..._column_tags import DatasetColumnTag, DimensionKeyColumnTag
from ..._dataset_ref import DatasetRef
from ..._dataset_type import DatasetType
from ...dimensions import DataCoordinate, DimensionElement, DimensionGroup, DimensionRecord

if TYPE_CHECKING:
    from lsst.daf.relation import ColumnTag


class DataCoordinateReader(ABC):
    """Base class and factory for reader objects that extract `DataCoordinate`
    instances from query result rows.
    """

    @staticmethod
    def make(
        dimensions: DimensionGroup,
        full: bool = True,
        records: bool = False,
        record_caches: Mapping[DimensionElement, Mapping[DataCoordinate, DimensionRecord]] | None = None,
    ) -> DataCoordinateReader:
        """Construct a concrete reader for a set of dimensions.

        Parameters
        ----------
        dimensions : `DimensionGroup`
            Dimensions of the `DataCoordinate` instances the new reader will
            read.
        full : `bool`, optional
            Whether to expect and extract implied dimensions as well as
            required dimensions.
        records : `bool`, optional
            Whether to attach dimension records.
        record_caches : `~collections.abc.Mapping`, optional
            Nested mapping (outer keys are dimension elements, inner keys are
            data IDs for that element) of cached dimension records.  Ignored
            unless ``records=True``.

        Returns
        -------
        reader : `DataCoordinateReader`
            Concrete reader instance.
        """
        if full:
            full_reader = _FullDataCoordinateReader(dimensions)
            if records:
                if record_caches is None:
                    record_caches = {}
                else:
                    record_caches = {
                        e: cache for e, cache in record_caches.items() if e in dimensions.elements
                    }
                record_readers = {}
                for element_name in dimensions.elements:
                    element = dimensions.universe[element_name]
                    if element_name not in record_caches:
                        record_readers[element] = DimensionRecordReader(element)
                return _ExpandedDataCoordinateReader(full_reader, record_caches, record_readers)
            return full_reader
        else:
            assert not records, "Cannot add records unless full=True."
            return _BasicDataCoordinateReader(dimensions)

    __slots__ = ()

    @abstractmethod
    def read(self, row: Mapping[ColumnTag, Any]) -> DataCoordinate:
        """Read a `DataCoordinate` from a query result row.

        Parameters
        ----------
        row : `~collections.abc.Mapping`
            Mapping with `ColumnTag` keys representing a query result row.

        Returns
        -------
        data_coordinate : `DataCoordinate`
            New data ID.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def columns_required(self) -> Set[ColumnTag]:
        raise NotImplementedError()


class _BasicDataCoordinateReader(DataCoordinateReader):
    """Private subclass of `DataCoordinateReader` for the ``full=False`` case.

    Parameters
    ----------
    dimensions : `DimensionGroup`
        Dimensions of the `DataCoordinate` instances read.
    """

    def __init__(self, dimensions: DimensionGroup):
        self._dimensions = dimensions
        self._tags = tuple(DimensionKeyColumnTag(name) for name in self._dimensions.required.names)

    __slots__ = ("_dimensions", "_tags")

    def read(self, row: Mapping[ColumnTag, Any]) -> DataCoordinate:
        # Docstring inherited.
        return DataCoordinate.from_required_values(
            self._dimensions,
            tuple(row[tag] for tag in self._tags),
        )

    @property
    def columns_required(self) -> Set[ColumnTag]:
        return frozenset(self._tags)


class _FullDataCoordinateReader(DataCoordinateReader):
    """Private subclass of `DataCoordinateReader` for the ``full=True`` case.

    Parameters
    ----------
    dimensions : `DimensionGroup`
        Dimensions of the `DataCoordinate` instances read.
    """

    def __init__(self, dimensions: DimensionGroup):
        self._dimensions = dimensions
        self._tags = tuple(
            DimensionKeyColumnTag(name) for name in self._dimensions.as_group().data_coordinate_keys
        )

    __slots__ = ("_dimensions", "_tags")

    def read(self, row: Mapping[ColumnTag, Any]) -> DataCoordinate:
        # Docstring inherited.
        return DataCoordinate.from_full_values(
            self._dimensions,
            tuple(row[tag] for tag in self._tags),
        )

    @property
    def columns_required(self) -> Set[ColumnTag]:
        return frozenset(self._tags)


class _ExpandedDataCoordinateReader(DataCoordinateReader):
    """Private subclass of `DataCoordinateReader` for the ``full=True`` case.

    Parameters
    ----------
    full_reader : `_FullDataCoordinateReader`
        Reader for full data IDs that don't have records.
    record_caches : `~collections.abc.Mapping`
        Nested mapping (outer keys are dimension elements, inner keys are data
        IDs for that element) of cached dimension records.
    record_readers : `~collections.abc.Mapping`
        Mapping from `DimensionElement` to `DimensionRecordReaders`.  Should
        include all elements in the data coordinate's dimensions that are not
        in ``record_cache``.
    """

    def __init__(
        self,
        full_reader: _FullDataCoordinateReader,
        record_caches: Mapping[DimensionElement, Mapping[DataCoordinate, DimensionRecord]],
        record_readers: Mapping[DimensionElement, DimensionRecordReader],
    ):
        self._full_reader = full_reader
        self._record_readers = record_readers
        self._record_caches = record_caches

    __slots__ = ("_full_reader", "_record_readers", "_record_caches", "_cached_columns_required")

    def read(self, row: Mapping[ColumnTag, Any]) -> DataCoordinate:
        # Docstring inherited.
        full = self._full_reader.read(row)
        records = {}
        for element, cache in self._record_caches.items():
            records[element.name] = cache[full.subset(element.graph)]
        for element, reader in self._record_readers.items():
            records[element.name] = reader.read(row)
        return full.expanded(records)

    @property
    @cached_getter
    def columns_required(self) -> Set[ColumnTag]:
        result = set(self._full_reader.columns_required)
        for reader in self._record_readers.values():
            result.update(reader.columns_required)
        return result


class DatasetRefReader:
    """Reader class that extracts `DatasetRef` objects from query result rows.

    Parameters
    ----------
    dataset_type : `DatasetType`
        Dataset type for extracted references.
    full : `bool`, optional
        Whether to expect and extract implied dimensions as well as required
        dimensions.
    translate_collection : `~collections.abc.Callable`, optional
        Callable that returns `str` collection names given collection primary
        key values.  Optional only for registries that use names as primary
        keys, or if ``run`` is always passed to `read`.
    records : `bool`, optional
        Whether to attach dimension records to data IDs.
    record_caches : `~collections.abc.Mapping`, optional
        Nested mapping (outer keys are dimension element names, inner keys
        are data IDs for that element) of cached dimension records.
        Ignored unless ``records=True``.
    """

    def __init__(
        self,
        dataset_type: DatasetType,
        *,
        full: bool = True,
        translate_collection: Callable[[Any], str] | None = None,
        records: bool = False,
        record_caches: Mapping[DimensionElement, Mapping[DataCoordinate, DimensionRecord]] | None = None,
    ):
        self._data_coordinate_reader = DataCoordinateReader.make(
            dataset_type.dimensions.as_group(), full=full, records=records, record_caches=record_caches
        )
        self._dataset_type = dataset_type
        self._translate_collection = translate_collection
        self._id_tag = DatasetColumnTag(dataset_type.name, "dataset_id")
        self._run_tag = DatasetColumnTag(dataset_type.name, "run")

    __slots__ = (
        "_data_coordinate_reader",
        "_dataset_type",
        "_translate_collection",
        "_id_tag",
        "_run_tag",
        "_cached_columns_required",
    )

    def read(
        self,
        row: Mapping[ColumnTag, Any],
        *,
        run: str | None = None,
        data_id: DataCoordinate | None = None,
    ) -> DatasetRef:
        """Read a `DatasetRef` from a query result row.

        Parameters
        ----------
        row : `~collections.abc.Mapping`
            Mapping with `ColumnTag` keys representing a query result row.
        run : `str`, optional
            Name of the `~CollectionType.RUN` collection; when provided the run
            key does not need to be present in the result row, and
            ``translate_collection`` does not need to be provided at
            construction.
        data_id : `DataCoordinate`, optional
            Data ID; when provided the dimensions do not need to be present in
            the result row.
        """
        if data_id is None:
            data_id = self._data_coordinate_reader.read(row)
        if run is None:
            run_key = row[self._run_tag]
            if self._translate_collection is not None:
                run = self._translate_collection(run_key)
            else:
                run = run_key
        return DatasetRef(
            self._dataset_type,
            data_id,
            run=run,
            id=row[self._id_tag],
        )

    @property
    @cached_getter
    def columns_required(self) -> Set[ColumnTag]:
        result = set(self._data_coordinate_reader.columns_required)
        result.add(self._id_tag)
        result.add(self._run_tag)
        return result


class DimensionRecordReader:
    """Read dimension records."""

    def __init__(self, element: DimensionElement):
        self._cls = element.RecordClass
        self._tags = element.RecordClass.fields.columns

    __slots__ = ("_cls", "_tags")

    def read(self, row: Mapping[ColumnTag, Any]) -> DimensionRecord:
        return self._cls(**{name: row[tag] for tag, name in self._tags.items()})

    @property
    def columns_required(self) -> Set[ColumnTag]:
        return self._tags.keys()

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
    "DataCoordinateReader",
    "DatasetRefReader",
)

from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping
from typing import Any

from ...core import (
    ColumnTag,
    DataCoordinate,
    DatasetColumnTag,
    DatasetRef,
    DatasetType,
    DimensionGraph,
    DimensionKeyColumnTag,
)


class DataCoordinateReader(ABC):
    """Base class and factory for reader objects that extract `DataCoordinate`
    instances from query result rows.
    """

    @staticmethod
    def make(dimensions: DimensionGraph, full: bool = True) -> DataCoordinateReader:
        """Construct a concrete reader for a set of dimensions.

        Parameters
        ----------
        dimensions : `DimensionGraph`
            Dimensions of the `DataCoordinate` instances the new reader will
            read.
        full : `bool`, optional
            Whether to expect and extract implied dimensions as well as
            required dimensions.

        Returns
        -------
        reader : `DataCoordinateReader`
            Concrete reader instance.
        """
        if full:
            return _FullDataCoordinateReader(dimensions)
        else:
            return _BasicDataCoordinateReader(dimensions)

    __slots__ = ()

    @abstractmethod
    def read(self, row: Mapping[ColumnTag, Any]) -> DataCoordinate:
        """Read a `DataCoordinate` from a query result row.

        Parameters
        ----------
        row : `Mapping`
            Mapping with `ColumnTag` keys representing a query result row.

        Returns
        -------
        data_coordinate : `DataCoordinate`
            New data ID.
        """
        raise NotImplementedError()


class _BasicDataCoordinateReader(DataCoordinateReader):
    """Private subclass of `DataCoordinateReader` for the ``full=False`` case.

    Parameters
    ----------
    dimensions : `DimensionGraph`
        Dimensions of the `DataCoordinate` instances read.
    """

    def __init__(self, dimensions: DimensionGraph):
        self._dimensions = dimensions
        self._tags = tuple(DimensionKeyColumnTag(name) for name in self._dimensions.required.names)

    __slots__ = ("_dimensions", "_tags")

    def read(self, row: Mapping[ColumnTag, Any]) -> DataCoordinate:
        # Docstring inherited.
        return DataCoordinate.fromRequiredValues(
            self._dimensions,
            tuple(row[tag] for tag in self._tags),
        )


class _FullDataCoordinateReader(DataCoordinateReader):
    """Private subclass of `DataCoordinateReader` for the ``full=True`` case.

    Parameters
    ----------
    dimensions : `DimensionGraph`
        Dimensions of the `DataCoordinate` instances read.
    """

    def __init__(self, dimensions: DimensionGraph):
        self._dimensions = dimensions
        self._tags = tuple(DimensionKeyColumnTag(name) for name in self._dimensions._dataCoordinateIndices)

    __slots__ = ("_dimensions", "_tags")

    def read(self, row: Mapping[ColumnTag, Any]) -> DataCoordinate:
        # Docstring inherited.
        return DataCoordinate.fromFullValues(
            self._dimensions,
            tuple(row[tag] for tag in self._tags),
        )


class DatasetRefReader:
    """Reader class that extracts `DatasetRef` objects from query result rows.

    Parameters
    ----------
    dataset_type : `DatasetType`
        Dataset type for extracted references.
    full : `bool`, optional
        Whether to expect and extract implied dimensions as well as required
        dimensions.
    translate_collection : `Callable`, optional
        Callable that returns `str` collection names given collection primary
        key values.  Optional only for registries that use names as primary
        keys, or if ``run`` is always passed to `read`.
    """

    def __init__(
        self,
        dataset_type: DatasetType,
        *,
        full: bool = True,
        translate_collection: Callable[[Any], str] | None = None,
    ):
        self._data_coordinate_reader = DataCoordinateReader.make(dataset_type.dimensions, full=full)
        self._dataset_type = dataset_type
        self._translate_collection = translate_collection
        self._id_tag = DatasetColumnTag(dataset_type.name, "dataset_id")
        self._run_tag = DatasetColumnTag(dataset_type.name, "run")

    __slots__ = ("_data_coordinate_reader", "_dataset_type", "_translate_collection", "_id_tag", "_run_tag")

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
        row : `Mapping`
            Mapping with `ColumnTag` keys representing a query result row.
        run : `str`, optional
            Name of the `~CollectionType.RUN` collection; when provided the run
            key does not need to be present in the result row, and
            ``translate_collection` does not need to be provided at
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

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

from abc import abstractmethod
from collections.abc import Iterable
from typing import TYPE_CHECKING

import sqlalchemy

from .._dataset_ref import DatasetRef
from .._dataset_type import DatasetType
from ..dimensions import (
    DataCoordinate,
    DimensionElement,
    DimensionGroup,
    DimensionRecord,
    DimensionRecordSet,
    SkyPixDimension,
)
from ..queries import tree as qt
from ..queries.driver import (
    DataCoordinateResultPage,
    DatasetRefResultPage,
    DimensionRecordResultPage,
    PageKey,
    ResultPage,
)
from ..queries.result_specs import DataCoordinateResultSpec, DatasetRefResultSpec, DimensionRecordResultSpec

if TYPE_CHECKING:
    from ..name_shrinker import NameShrinker
    from ..registry.interfaces import Database


class ResultPageConverter:
    """Interface for raw SQL result row conversion to `ResultPage`."""

    @abstractmethod
    def convert(self, raw_rows: Iterable[sqlalchemy.Row], next_key: PageKey | None) -> ResultPage:
        """Convert raw SQL result rows into a `ResultPage` object containing
        high-level `Butler` types.

        Parameters
        ----------
        raw_rows : `~collections.abc.Iterable` [ `sqlalchemy.Row` ]
            Iterable of SQLAlchemy rows, with `Postprocessing` filters already
            applied.
        next_key : `PageKey` or `None`
            Key for the next page to add into the returned page object.

        Returns
        -------
        result_page : `ResultPage`
            Converted results.
        """
        raise NotImplementedError()


class DimensionRecordResultPageConverter(ResultPageConverter):  # numpydoc ignore=PR01
    """Converts raw SQL rows into pages of `DimensionRecord` query results."""

    def __init__(self, spec: DimensionRecordResultSpec, db: Database) -> None:
        self._result_spec = spec
        self._converter = _create_dimension_record_row_converter(spec.element, db)

    def convert(
        self, raw_rows: Iterable[sqlalchemy.Row], next_key: PageKey | None
    ) -> DimensionRecordResultPage:
        record_set = DimensionRecordSet(self._result_spec.element)
        for raw_row in raw_rows:
            record_set.add(self._converter.convert(raw_row))
        return DimensionRecordResultPage(spec=self._result_spec, next_key=next_key, rows=record_set)


def _create_dimension_record_row_converter(
    element: DimensionElement, db: Database
) -> _DimensionRecordRowConverter:
    if isinstance(element, SkyPixDimension):
        return _SkypixDimensionRecordRowConverter(element)
    else:
        return _NormalDimensionRecordRowConverter(element, db)


class _DimensionRecordRowConverter:
    """Interface definition for helper objects that convert a result row into a
    DimensionRecord instance.
    """

    @abstractmethod
    def convert(self, row: sqlalchemy.Row) -> DimensionRecord:
        raise NotImplementedError()


class _NormalDimensionRecordRowConverter(_DimensionRecordRowConverter):
    """Helper for converting result row into a DimensionRecord instance for
    typical dimensions (all non-skypix dimensions).
    """

    def __init__(self, element: DimensionElement, db: Database) -> None:
        self._db = db
        self._element = element
        self._record_cls = self._element.RecordClass

        # Mapping from DimensionRecord attribute name to qualified column
        # name, but as a list of tuples since we'd just iterate over items
        # anyway.
        column_map = list(
            zip(
                element.schema.dimensions.names,
                element.dimensions.names,
            )
        )
        for field in element.schema.remainder.names:
            if field != "timespan":
                column_map.append((field, qt.ColumnSet.get_qualified_name(element.name, field)))
        self._column_map = column_map

        self._timespan_qualified_name: str | None = None
        if element.temporal:
            self._timespan_qualified_name = qt.ColumnSet.get_qualified_name(element.name, "timespan")

    def convert(self, row: sqlalchemy.Row) -> DimensionRecord:
        m = row._mapping
        d = {k: m[v] for k, v in self._column_map}
        if self._timespan_qualified_name is not None:
            d["timespan"] = self._db.getTimespanRepresentation().extract(
                m, name=self._timespan_qualified_name
            )
        return self._record_cls(**d)


class _SkypixDimensionRecordRowConverter(_DimensionRecordRowConverter):
    """Helper for converting result row into a DimensionRecord instance for
    skypix dimensions.
    """

    def __init__(self, element: SkyPixDimension):
        self._pixelization = element.pixelization
        self._id_qualified_name = qt.ColumnSet.get_qualified_name(element.name, None)
        self._record_cls = element.RecordClass

    def convert(self, row: sqlalchemy.Row) -> DimensionRecord:
        pixel_id = row._mapping[self._id_qualified_name]
        return self._record_cls(id=pixel_id, region=self._pixelization.pixel(pixel_id))


class DataCoordinateResultPageConverter(ResultPageConverter):  # numpydoc ignore=PR01
    """Converts raw SQL result iterables into a page of `DataCoordinate`
    query results.
    """

    def __init__(self, spec: DataCoordinateResultSpec, column_order: qt.ColumnOrder) -> None:
        self._spec = spec
        self._converter = _DataCoordinateRowConverter(spec.dimensions, column_order)

    def convert(
        self,
        raw_rows: Iterable[sqlalchemy.Row],
        next_key: PageKey | None,
    ) -> DataCoordinateResultPage:
        convert = self._converter.convert
        rows = [convert(row) for row in raw_rows]
        return DataCoordinateResultPage(spec=self._spec, rows=rows, next_key=next_key)


class DatasetRefResultPageConverter(ResultPageConverter):  # numpydoc ignore=PR01
    """Convert raw SQL result iterables into pages of `DatasetRef` query
    results.
    """

    def __init__(
        self,
        spec: DatasetRefResultSpec,
        dataset_type: DatasetType,
        column_order: qt.ColumnOrder,
        name_shrinker: NameShrinker,
    ) -> None:
        self._spec = spec
        self._dataset_type = dataset_type
        self._data_coordinate_converter = _DataCoordinateRowConverter(spec.dimensions, column_order)
        self._column_order = column_order
        self._name_shrinker = name_shrinker

    def convert(
        self,
        raw_rows: Iterable[sqlalchemy.Row],
        next_key: PageKey | None,
    ) -> DatasetRefResultPage:
        run_column = self._name_shrinker.shrink(
            qt.ColumnSet.get_qualified_name(self._spec.dataset_type_name, "run")
        )
        dataset_id_column = self._name_shrinker.shrink(
            qt.ColumnSet.get_qualified_name(self._spec.dataset_type_name, "dataset_id")
        )
        rows = [
            DatasetRef(
                datasetType=self._dataset_type,
                dataId=self._data_coordinate_converter.convert(row),
                run=row._mapping[run_column],
                id=row._mapping[dataset_id_column],
                conform=False,
            )
            for row in raw_rows
        ]

        return DatasetRefResultPage(spec=self._spec, rows=rows, next_key=next_key)


class _DataCoordinateRowConverter:
    """Helper for converting a raw SQL result row into a DataCoordinate
    instance.
    """

    def __init__(self, dimensions: DimensionGroup, column_order: qt.ColumnOrder):
        assert (
            list(dimensions.data_coordinate_keys) == column_order.dimension_key_names
        ), "Dimension keys in result row should be in same order as those specified by the result spec"

        self._dimensions = dimensions
        self._column_order = column_order

    def convert(self, row: sqlalchemy.Row) -> DataCoordinate:
        return DataCoordinate.from_full_values(
            self._dimensions, tuple(self._column_order.extract_dimension_key_columns(row))
        )

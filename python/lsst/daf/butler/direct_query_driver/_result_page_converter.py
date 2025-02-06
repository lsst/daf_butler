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

import datetime
from abc import abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import astropy.time
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
from ..dimensions.record_cache import DimensionRecordCache
from ..queries import tree as qt
from ..queries.driver import (
    DataCoordinateResultPage,
    DatasetRefResultPage,
    DimensionRecordResultPage,
    GeneralResultPage,
    ResultPage,
)
from ..queries.result_specs import (
    DataCoordinateResultSpec,
    DatasetRefResultSpec,
    DimensionRecordResultSpec,
    GeneralResultSpec,
)
from ..timespan_database_representation import TimespanDatabaseRepresentation

if TYPE_CHECKING:
    from ..registry.interfaces import Database


class ResultPageConverter:
    """Interface for raw SQL result row conversion to `ResultPage`."""

    @abstractmethod
    def convert(self, raw_rows: Iterable[sqlalchemy.Row]) -> ResultPage:
        """Convert raw SQL result rows into a `ResultPage` object containing
        high-level `Butler` types.

        Parameters
        ----------
        raw_rows : `~collections.abc.Iterable` [ `sqlalchemy.Row` ]
            Iterable of SQLAlchemy rows, with `Postprocessing` filters already
            applied.

        Returns
        -------
        result_page : `ResultPage`
            Converted results.
        """
        raise NotImplementedError()


@dataclass(frozen=True)
class ResultPageConverterContext:
    """Parameters used by all result page converters."""

    db: Database
    column_order: qt.ColumnOrder
    dimension_record_cache: DimensionRecordCache


class DimensionRecordResultPageConverter(ResultPageConverter):  # numpydoc ignore=PR01
    """Converts raw SQL rows into pages of `DimensionRecord` query results."""

    def __init__(self, spec: DimensionRecordResultSpec, ctx: ResultPageConverterContext) -> None:
        self._result_spec = spec
        self._converter = _create_dimension_record_row_converter(spec.element, ctx, use_cache=False)

    def convert(self, raw_rows: Iterable[sqlalchemy.Row]) -> DimensionRecordResultPage:
        record_set = DimensionRecordSet(self._result_spec.element)
        for raw_row in raw_rows:
            record_set.add(self._converter.convert(raw_row))
        return DimensionRecordResultPage(spec=self._result_spec, rows=record_set)


def _create_dimension_record_row_converter(
    element: DimensionElement, ctx: ResultPageConverterContext, *, use_cache: bool = True
) -> _DimensionRecordRowConverter:
    if use_cache and element.is_cached:
        return _CachedDimensionRecordRowConverter(element, ctx.dimension_record_cache)
    elif isinstance(element, SkyPixDimension):
        return _SkypixDimensionRecordRowConverter(element)
    else:
        return _NormalDimensionRecordRowConverter(element, ctx.db)


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


class _CachedDimensionRecordRowConverter(_DimensionRecordRowConverter):
    """Helper for converting result row into a DimensionRecord instance for
    "cached" dimensions.  These are dimensions with few records, used by
    many/all dataset types.  For these dimensions, a complete cache of records
    is stored client-side instead of re-fetching them from the DB constantly.
    """

    def __init__(self, element: DimensionElement, cache: DimensionRecordCache) -> None:
        self._element = element
        self._cache = cache
        self._key_columns = [qt.ColumnSet.get_qualified_name(name, None) for name in element.required.names]

    def convert(self, row: sqlalchemy.Row) -> DimensionRecord:
        mapping = row._mapping
        values = tuple(mapping[key] for key in self._key_columns)
        return self._cache[self._element.name].find_with_required_values(values)


class DataCoordinateResultPageConverter(ResultPageConverter):  # numpydoc ignore=PR01
    """Converts raw SQL result iterables into a page of `DataCoordinate`
    query results.
    """

    def __init__(
        self,
        spec: DataCoordinateResultSpec,
        ctx: ResultPageConverterContext,
    ) -> None:
        self._spec = spec
        self._converter = _DataCoordinateRowConverter(
            spec.dimensions, ctx, include_dimension_records=spec.include_dimension_records
        )

    def convert(
        self,
        raw_rows: Iterable[sqlalchemy.Row],
    ) -> DataCoordinateResultPage:
        convert = self._converter.convert
        rows = [convert(row) for row in raw_rows]
        return DataCoordinateResultPage(spec=self._spec, rows=rows)


class DatasetRefResultPageConverter(ResultPageConverter):  # numpydoc ignore=PR01
    """Convert raw SQL result iterables into pages of `DatasetRef` query
    results.
    """

    def __init__(
        self,
        spec: DatasetRefResultSpec,
        dataset_type: DatasetType,
        ctx: ResultPageConverterContext,
    ) -> None:
        self._spec = spec
        self._dataset_type = dataset_type
        self._data_coordinate_converter = _DataCoordinateRowConverter(
            spec.dimensions, ctx, include_dimension_records=spec.include_dimension_records
        )
        self._column_order = ctx.column_order
        self._name_shrinker = ctx.db.name_shrinker

    def convert(
        self,
        raw_rows: Iterable[sqlalchemy.Row],
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

        return DatasetRefResultPage(spec=self._spec, rows=rows)


class _DataCoordinateRowConverter:
    """Helper for converting a raw SQL result row into a DataCoordinate
    instance.
    """

    def __init__(
        self,
        dimensions: DimensionGroup,
        ctx: ResultPageConverterContext,
        include_dimension_records: bool,
    ):
        assert list(dimensions.data_coordinate_keys) == ctx.column_order.dimension_key_names, (
            "Dimension keys in result row should be in same order as those specified by the result spec"
        )

        self._dimensions = dimensions
        self._column_order = ctx.column_order
        self._dimension_record_converter = None
        if include_dimension_records:
            self._dimension_record_converter = _DimensionGroupRecordRowConverter(dimensions, ctx)

    def convert(self, row: sqlalchemy.Row) -> DataCoordinate:
        coordinate = DataCoordinate.from_full_values(
            self._dimensions, tuple(self._column_order.extract_dimension_key_columns(row))
        )

        if self._dimension_record_converter is None:
            return coordinate
        else:
            return coordinate.expanded(self._dimension_record_converter.convert(row))


class _DimensionGroupRecordRowConverter:  # numpydoc ignore=PR01
    """Helper for pulling out all the DimensionRecords in a raw SQL result
    row.
    """

    def __init__(self, dimensions: DimensionGroup, ctx: ResultPageConverterContext) -> None:
        self._record_converters = {
            name: _create_dimension_record_row_converter(dimensions.universe[name], ctx)
            for name in dimensions.elements
        }

    def convert(self, row: sqlalchemy.Row) -> dict[str, DimensionRecord]:  # numpydoc ignore=PR01
        """Return a mapping from dimension name to dimension records for all
        the dimensions in the database row.
        """
        return {name: converter.convert(row) for name, converter in self._record_converters.items()}


class GeneralResultPageConverter(ResultPageConverter):  # numpydoc ignore=PR01
    """Converts raw SQL rows into pages of `GeneralResult` query results."""

    def __init__(self, spec: GeneralResultSpec, ctx: ResultPageConverterContext) -> None:
        self.spec = spec
        # In case `spec.include_dimension_records` is True then in addition to
        # columns returned by the query we have to add columns from dimension
        # records that are not returned by the query. These columns belong to
        # either cached or skypix dimensions.
        columns = spec.get_result_columns()
        universe = spec.dimensions.universe
        self.converters: list[_GeneralColumnConverter] = []
        self.record_converters: dict[DimensionElement, _DimensionRecordRowConverter] = {}
        for column in columns:
            column_name = qt.ColumnSet.get_qualified_name(column.logical_table, column.field)
            converter: _GeneralColumnConverter
            if column.field == TimespanDatabaseRepresentation.NAME:
                converter = _TimespanGeneralColumnConverter(column_name, ctx.db)
            elif column.field == "ingest_date":
                converter = _TimestampGeneralColumnConverter(column_name)
            else:
                converter = _DefaultGeneralColumnConverter(column_name)
            self.converters.append(converter)

        if spec.include_dimension_records:
            universe = self.spec.dimensions.universe
            for element_name in self.spec.dimensions.elements:
                element = universe[element_name]
                if isinstance(element, SkyPixDimension):
                    self.record_converters[element] = _SkypixDimensionRecordRowConverter(element)
                elif element.is_cached:
                    self.record_converters[element] = _CachedDimensionRecordRowConverter(
                        element, ctx.dimension_record_cache
                    )

    def convert(self, raw_rows: Iterable[sqlalchemy.Row]) -> GeneralResultPage:
        rows = []
        dimension_records = None
        if self.spec.include_dimension_records:
            dimension_records = {element: DimensionRecordSet(element) for element in self.record_converters}
        for row in raw_rows:
            rows.append(tuple(cvt.convert(row) for cvt in self.converters))
            if dimension_records:
                for element, converter in self.record_converters.items():
                    dimension_records[element].add(converter.convert(row))

        return GeneralResultPage(spec=self.spec, rows=rows, dimension_records=dimension_records)


class _GeneralColumnConverter:
    """Interface for converting one or more columns in a result row to a single
    column value in output row.
    """

    @abstractmethod
    def convert(self, row: sqlalchemy.Row) -> Any:
        """Convert one or more columns in the row into single value.

        Parameters
        ----------
        row : `sqlalchemy.Row`
            Row of values.

        Returns
        -------
        value : `Any`
            Result of the conversion.
        """
        raise NotImplementedError()


class _DefaultGeneralColumnConverter(_GeneralColumnConverter):
    """Converter that returns column value without conversion.

    Parameters
    ----------
    name : `str`
        Column name
    """

    def __init__(self, name: str):
        self.name = name

    def convert(self, row: sqlalchemy.Row) -> Any:
        return row._mapping[self.name]


class _TimestampGeneralColumnConverter(_GeneralColumnConverter):
    """Converter that transforms ``datetime`` instances into astropy Time. Only
    ``dataset.ingest_date`` column was using native timestamps in the initial
    schema version, and we are switching to our common nanoseconds-since-epoch
    representation for that column in newer schema versions. For both schema
    versions we want to return astropy time to clients.

    Parameters
    ----------
    name : `str`
        Column name
    """

    def __init__(self, name: str):
        self.name = name

    def convert(self, row: sqlalchemy.Row) -> Any:
        value = row._mapping[self.name]
        if isinstance(value, datetime.datetime):
            value = astropy.time.Time(value, scale="utc").tai
        return value


class _TimespanGeneralColumnConverter(_GeneralColumnConverter):
    """Converter that extracts timespan from the row.

    Parameters
    ----------
    name : `str`
        Column name or prefix.
    db : `Database`
        Database instance.
    """

    def __init__(self, name: str, db: Database):
        self.timespan_class = db.getTimespanRepresentation()
        self.name = name

    def convert(self, row: sqlalchemy.Row) -> Any:
        timespan = self.timespan_class.extract(row._mapping, self.name)
        return timespan

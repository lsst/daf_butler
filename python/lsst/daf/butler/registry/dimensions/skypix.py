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

__all__ = ["BasicSkyPixDimensionRecordStorage"]

from typing import AbstractSet, Iterable

import sqlalchemy

from ...core import DataCoordinateIterable, DimensionRecord, SkyPixDimension, sql
from ..interfaces import SkyPixDimensionRecordStorage


class BasicSkyPixDimensionRecordStorage(SkyPixDimensionRecordStorage):
    """A storage implementation specialized for `SkyPixDimension` records.

    `SkyPixDimension` records are never stored in a database, but are instead
    generated-on-the-fly from a `sphgeom.Pixelization` instance.

    Parameters
    ----------
    dimension : `SkyPixDimension`
        The dimension for which this instance will simulate storage.
    """

    def __init__(self, dimension: SkyPixDimension):
        self._dimension = dimension

    @property
    def element(self) -> SkyPixDimension:
        # Docstring inherited from DimensionRecordStorage.element.
        return self._dimension

    def clearCaches(self) -> None:
        # Docstring inherited from DimensionRecordStorage.clearCaches.
        pass

    def join_results_postprocessed(self) -> bool:
        # Docstring inherited.
        return True

    def join(
        self,
        relation: sql.Relation,
        sql_columns: AbstractSet[str],
        *,
        constraints: sql.LocalConstraints | None = None,
        result_records: bool = False,
        result_columns: AbstractSet[str] = frozenset(),
    ) -> sql.Relation:
        if sql_columns or self._dimension.name not in relation.columns.dimensions:
            raise NotImplementedError(
                "SkyPix indices and/or regions cannot be included in queries "
                "except via postprocessing.  Use `lsst.sphgeom.Pixelization` "
                "directly to obtain SkyPix indices that overlap a region."
            )
        # If joining some other element or dataset type already brought in
        # the key for this dimension, we just add the region via a
        # postprocessor if requested.
        if result_records or result_columns:
            return relation.postprocessed(_SkyPixRegionPostprocessor(self.element))
        else:
            return relation

    def insert(self, *records: DimensionRecord, replace: bool = False, skip_existing: bool = False) -> None:
        # Docstring inherited from DimensionRecordStorage.insert.
        raise TypeError(f"Cannot insert into SkyPix dimension {self._dimension.name}.")

    def sync(self, record: DimensionRecord, update: bool = False) -> bool:
        # Docstring inherited from DimensionRecordStorage.sync.
        raise TypeError(f"Cannot sync SkyPixdimension {self._dimension.name}.")

    def fetch(self, dataIds: DataCoordinateIterable) -> Iterable[DimensionRecord]:
        # Docstring inherited from DimensionRecordStorage.fetch.
        RecordClass = self._dimension.RecordClass
        for dataId in dataIds:
            index = dataId[self._dimension.name]
            yield RecordClass(id=index, region=self._dimension.pixelization.pixel(index))

    def digestTables(self) -> Iterable[sqlalchemy.schema.Table]:
        # Docstring inherited from DimensionRecordStorage.digestTables.
        return []


class _SkyPixRegionPostprocessor(sql.Postprocessor):
    def __init__(self, dimension: SkyPixDimension):
        self._pixelization = dimension.pixelization
        self._index_tag = sql.DimensionKeyColumnTag(dimension.name)
        self._region_tag = sql.DimensionRecordColumnTag(dimension.name, "region")

    __slots__ = ("_pixelization", "_index_tag", "_region_tag")

    @property
    def columns_required(self) -> AbstractSet[sql.ColumnTag]:
        return {self._index_tag}

    @property
    def columns_provided(self) -> AbstractSet[sql.ColumnTag]:
        return {self._region_tag}

    @property
    def row_multiplier(self) -> float:
        return 1.0

    def apply(self, row: sql.ResultRow) -> sql.ResultRow:
        row[self._region_tag] = self._pixelization.pixel(row[self._index_tag])
        return row

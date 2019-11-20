from __future__ import annotations

__all__ = ["DimensionRecordStorage", "SqlDimensionRecordStorage", "SkyPixDimensionRecordStorage"]

from abc import ABC, abstractmethod
from typing import (
    Optional,
)

import sqlalchemy

from ..core.dimensions import (
    DataCoordinate,
    DimensionElement,
    DimensionRecord,
    SkyPixDimension,
)
from .databaseLayer import DatabaseLayer


class DimensionRecordStorage(ABC):

    def __init__(self, element: DimensionElement):
        self.element = element

    @abstractmethod
    def insert(self, *records: DimensionRecord):
        pass

    @abstractmethod
    def fetch(self, dataId: DataCoordinate) -> Optional[DimensionRecord]:
        raise NotImplementedError()

    @abstractmethod
    def select(self) -> Optional[sqlalchemy.sql.FromClause]:
        pass

    @abstractmethod
    def selectCommonSkyPixOverlap(self) -> Optional[sqlalchemy.sql.FromClause]:
        pass

    element: DimensionElement


class SqlDimensionRecordStorage(DimensionRecordStorage):

    def __init__(self, *, db: DatabaseLayer, element: DimensionElement,
                 table: sqlalchemy.schema.Table,
                 commonSkyPixOverlapTable: Optional[sqlalchemy.schema.Table] = None):
        super().__init__(element=element)
        self._db = db
        self._table = table
        self._commonSkyPixOverlapTable = commonSkyPixOverlapTable

    def insert(self, *records: DimensionRecord):
        # Build lists of dicts to insert first, before any database operations,
        # to minimize the time spent in the transaction.
        elementRows = []
        if self.element.spatial:
            commonSkyPixRows = []
            commonSkyPix = self.element.universe.commonSkyPix
        for record in records:
            elementRows.append(record.toDict())
            if self.element.spatial:
                if record.region is None:
                    # TODO: should we warn about this case?
                    continue
                base = record.dataId.byName()
                for begin, end in commonSkyPix.pixelization.envelope(record.region):
                    for skypix in range(begin, end):
                        row = base.copy()
                        row[commonSkyPix.name] = skypix
                        commonSkyPixRows.append(row)
        # TODO: wrap the operations below in a transaction.
        self._db.connection.execute(self._elementTable.insert(), *elementRows)
        if self.element.spatial and commonSkyPixRows:
            self._db.connection.execute(self._commonSkyPixOverlapTable.insert(), *commonSkyPixRows)

    def fetch(self, dataId: DataCoordinate) -> DimensionRecord:
        RecordClass = self.element.RecordClass
        # I don't know how expensive it is to construct the query below, and
        # hence how much gain there might be to caching it, so I'm going to
        # wait for it to appear as a hotspot in a profile before trying that.
        nRequired = len(self.element.graph.required)
        if self.element.viewOf is not None:
            whereColumns = [self._table.columns[dimension.name]
                            for dimension in self.element.graph.required]
        else:
            whereColumns = [self._table.columns[fieldName]
                            for fieldName in RecordClass.__slots__[:nRequired]]
        selectColumns = whereColumns + [self._table.columns[name]
                                        for name in RecordClass.__slots__[nRequired:]]
        query = sqlalchemy.sql.select(
            selectColumns
        ).select_from(self._table).where(
            sqlalchemy.sql.and_(*[column == dataId[dimension.name]
                                for column, dimension in zip(whereColumns, self.element.graph.required)])
        )
        row = self._db.connection.execute(query).fetchone()
        if row is None:
            return None
        return RecordClass(*row)

    def select(self) -> sqlalchemy.sql.FromClause:
        return self._table

    def selectCommonSkyPixOverlap(self) -> Optional[sqlalchemy.sql.FromClause]:
        return self._commonSkyPixOverlapTable


class SkyPixDimensionRecordStorage(DimensionRecordStorage):
    """A storage implementation specialized for `SkyPixDimension` records.

    `SkyPixDimension` records are never stored in a database, but are instead
    generated-on-the-fly from a `sphgeom.Pixelization` instance.

    Parameters
    ----------
    dimension : `SkyPixDimension`
        The dimension for which this instance will simulate storage.
    """

    def __init__(self, element: SkyPixDimension):
        super().__init__(element=element)

    def select(self) -> None:
        return None

    def selectCommonSkyPixOverlap(self) -> None:
        return None

    def insert(self, *records: DimensionRecord):
        # Docstring inherited from DimensionRecordStorage.insert.
        raise TypeError(f"Cannot insert into SkyPix dimension {self.element.name}.")

    def fetch(self, dataId: DataCoordinate) -> Optional[DimensionRecord]:
        # Docstring inherited from DimensionRecordStorage.fetch.
        return self.element.RecordClass(dataId[self.element.name],
                                        self.element.pixelization.pixel(dataId[self.element.name]))

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

__all__ = ["CachingDimensionRecordStorage"]

from typing import Any, Dict, Iterable, Mapping, Optional, Set

import sqlalchemy

from lsst.utils import doImport

from ...core import (
    DatabaseDimensionElement,
    DataCoordinate,
    DataCoordinateIterable,
    DataCoordinateSet,
    DimensionElement,
    DimensionRecord,
    GovernorDimension,
    LogicalTable,
    NamedKeyDict,
    NamedKeyMapping,
    SpatialRegionDatabaseRepresentation,
    TimespanDatabaseRepresentation,
)
from ..interfaces import (
    Database,
    DatabaseDimensionRecordStorage,
    GovernorDimensionRecordStorage,
    StaticTablesContext,
)
from ..queries import QueryBuilder


class CachingDimensionRecordStorage(DatabaseDimensionRecordStorage):
    """A record storage implementation that adds caching to some other nested
    storage implementation.

    Parameters
    ----------
    nested : `DatabaseDimensionRecordStorage`
        The other storage to cache fetches from and to delegate all other
        operations to.
    """
    def __init__(self, nested: DatabaseDimensionRecordStorage):
        self._nested = nested
        self._cache: Dict[DataCoordinate, Optional[DimensionRecord]] = {}

    @classmethod
    def initialize(
        cls,
        db: Database,
        element: DatabaseDimensionElement, *,
        context: Optional[StaticTablesContext] = None,
        config: Mapping[str, Any],
        governors: NamedKeyMapping[GovernorDimension, GovernorDimensionRecordStorage],
    ) -> DatabaseDimensionRecordStorage:
        # Docstring inherited from DatabaseDimensionRecordStorage.
        config = config["nested"]
        NestedClass = doImport(config["cls"])
        nested = NestedClass.initialize(db, element, context=context, config=config, governors=governors)
        return cls(nested)

    @property
    def element(self) -> DatabaseDimensionElement:
        # Docstring inherited from DimensionRecordStorage.element.
        return self._nested.element

    def clearCaches(self) -> None:
        # Docstring inherited from DimensionRecordStorage.clearCaches.
        self._cache.clear()
        self._nested.clearCaches()

    def join(
        self,
        builder: QueryBuilder, *,
        regions: Optional[NamedKeyDict[DimensionElement, SpatialRegionDatabaseRepresentation]] = None,
        timespans: Optional[NamedKeyDict[DimensionElement, TimespanDatabaseRepresentation]] = None,
    ) -> None:
        # Docstring inherited from DimensionRecordStorage.
        return self._nested.join(builder, regions=regions, timespans=timespans)

    def insert(self, *records: DimensionRecord) -> None:
        # Docstring inherited from DimensionRecordStorage.insert.
        self._nested.insert(*records)
        for record in records:
            self._cache[record.dataId] = record

    def sync(self, record: DimensionRecord) -> bool:
        # Docstring inherited from DimensionRecordStorage.sync.
        inserted = self._nested.sync(record)
        if inserted:
            self._cache[record.dataId] = record
        return inserted

    def fetch(self, dataIds: DataCoordinateIterable) -> Iterable[DimensionRecord]:
        # Docstring inherited from DimensionRecordStorage.fetch.
        missing: Set[DataCoordinate] = set()
        for dataId in dataIds:
            # Use ... as sentinal value so we can also cache None == "no such
            # record exists".
            record = self._cache.get(dataId, ...)
            if record is ...:
                missing.add(dataId)
            elif record is not None:
                # Unclear why MyPy can't tell that this isn't ..., but it
                # thinks it's still a possibility.
                yield record  # type: ignore
        if missing:
            toFetch = DataCoordinateSet(missing, graph=self.element.graph)
            for record in self._nested.fetch(toFetch):
                self._cache[record.dataId] = record
                yield record
            missing -= self._cache.keys()
            for dataId in missing:
                self._cache[dataId] = None

    def digestTables(self) -> Iterable[sqlalchemy.schema.Table]:
        # Docstring inherited from DimensionRecordStorage.digestTables.
        return self._nested.digestTables()

    def makeLogicalTable(self) -> LogicalTable:
        # Docstring inherited from DatabaseDimensionRecordStorage.
        return self._nested.makeLogicalTable()

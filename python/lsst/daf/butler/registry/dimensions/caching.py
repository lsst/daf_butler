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

from collections.abc import Iterable, Mapping
from typing import Any

import sqlalchemy
from lsst.utils import doImportType

from ...core import (
    DatabaseDimensionElement,
    DataCoordinate,
    DataCoordinateIterable,
    DataCoordinateSet,
    DimensionElement,
    DimensionRecord,
    GovernorDimension,
    NamedKeyDict,
    NamedKeyMapping,
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
        self._cache: dict[DataCoordinate, DimensionRecord | None] = {}

    @classmethod
    def initialize(
        cls,
        db: Database,
        element: DatabaseDimensionElement,
        *,
        context: StaticTablesContext | None = None,
        config: Mapping[str, Any],
        governors: NamedKeyMapping[GovernorDimension, GovernorDimensionRecordStorage],
    ) -> DatabaseDimensionRecordStorage:
        # Docstring inherited from DatabaseDimensionRecordStorage.
        config = config["nested"]
        NestedClass = doImportType(config["cls"])
        if not hasattr(NestedClass, "initialize"):
            raise TypeError(f"Nested class {config['cls']} does not have an initialize() method.")
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
        builder: QueryBuilder,
        *,
        regions: NamedKeyDict[DimensionElement, sqlalchemy.sql.ColumnElement] | None = None,
        timespans: NamedKeyDict[DimensionElement, TimespanDatabaseRepresentation] | None = None,
    ) -> None:
        # Docstring inherited from DimensionRecordStorage.
        return self._nested.join(builder, regions=regions, timespans=timespans)

    def insert(self, *records: DimensionRecord, replace: bool = False, skip_existing: bool = False) -> None:
        # Docstring inherited from DimensionRecordStorage.insert.
        self._nested.insert(*records, replace=replace, skip_existing=skip_existing)
        for record in records:
            # We really shouldn't ever get into a situation where the record
            # here differs from the one in the DB, but the last thing we want
            # is to make it harder to debug by making the cache different from
            # the DB.
            if skip_existing:
                self._cache.setdefault(record.dataId, record)
            else:
                self._cache[record.dataId] = record

    def sync(self, record: DimensionRecord, update: bool = False) -> bool | dict[str, Any]:
        # Docstring inherited from DimensionRecordStorage.sync.
        inserted_or_updated = self._nested.sync(record, update=update)
        if inserted_or_updated:
            self._cache[record.dataId] = record
        return inserted_or_updated

    def fetch(self, dataIds: DataCoordinateIterable) -> Iterable[DimensionRecord]:
        # Docstring inherited from DimensionRecordStorage.fetch.
        missing: set[DataCoordinate] = set()
        for dataId in dataIds:
            # Use ... as sentinal value so we can also cache None == "no such
            # record exists".
            record = self._cache.get(dataId, ...)
            if record is ...:
                missing.add(dataId)
            elif record is not None:
                yield record
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

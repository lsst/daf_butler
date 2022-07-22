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

from typing import AbstractSet, Any, Dict, Iterable, Mapping, Optional, Set, Union

import sqlalchemy
from lsst.utils import doImportType

from ...core import (
    ButlerSqlEngine,
    DatabaseDimensionElement,
    DataCoordinate,
    DataCoordinateIterable,
    DataCoordinateSet,
    DimensionRecord,
    GovernorDimension,
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
        element: DatabaseDimensionElement,
        *,
        context: Optional[StaticTablesContext] = None,
        config: Mapping[str, Any],
        governors: NamedKeyMapping[GovernorDimension, GovernorDimensionRecordStorage],
        column_types: ButlerSqlEngine,
    ) -> DatabaseDimensionRecordStorage:
        # Docstring inherited from DatabaseDimensionRecordStorage.
        config = config["nested"]
        NestedClass = doImportType(config["cls"])
        if not hasattr(NestedClass, "initialize"):
            raise TypeError(f"Nested class {config['cls']} does not have an initialize() method.")
        nested = NestedClass.initialize(
            db, element, context=context, config=config, governors=governors, column_types=column_types
        )
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
        relation: sql.Relation,
        sql_columns: AbstractSet[str],
        *,
        constraints: sql.LocalConstraints | None = None,
        result_records: bool = False,
        result_columns: AbstractSet[str] = frozenset(),
    ) -> sql.Relation:
        # Docstring inherited.
        # It would be nice to use the cache to satisfy requests for result
        # columns and records via Postprocessors, as we do with governor
        # dimension storage.  But this cache is lazy and per-record, so we
        # can't guarantee everything we'll want is in the cache yet, and we
        # don't want to do per-row lookups on cache misses.  My long-term plan
        # is to switch the caching here to fetch all rows for a particular
        # governor dimension value at once, and then we'll be able to use
        # constraints.dimensions to make sure we fully populate the cache up
        # front.
        return self._nested.join(
            relation,
            sql_columns,
            constraints=constraints,
            result_records=result_records,
            result_columns=result_columns,
        )

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

    def sync(self, record: DimensionRecord, update: bool = False) -> Union[bool, Dict[str, Any]]:
        # Docstring inherited from DimensionRecordStorage.sync.
        inserted_or_updated = self._nested.sync(record, update=update)
        if inserted_or_updated:
            self._cache[record.dataId] = record
        return inserted_or_updated

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

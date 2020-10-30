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

from typing import List, Optional

import sqlalchemy

from ...core import NamedKeyDict
from ...core.dimensions import DimensionElement, DimensionUniverse
from ..interfaces import (
    Database,
    StaticTablesContext,
    DimensionRecordStorageManager,
    DimensionRecordStorage,
    GovernorDimensionRecordStorage,
    VersionTuple
)


# This has to be updated on every schema change
_VERSION = VersionTuple(5, 0, 0)


class StaticDimensionRecordStorageManager(DimensionRecordStorageManager):
    """An implementation of `DimensionRecordStorageManager` for single-layer
    `Registry` and the base layers of multi-layer `Registry`.

    This manager creates `DimensionRecordStorage` instances for all elements
    in the `DimensionUniverse` in its own `initialize` method, as part of
    static table creation, so it never needs to manage any dynamic registry
    tables.

    Parameters
    ----------
    db : `Database`
        Interface to the underlying database engine and namespace.
    records : `NamedKeyDict`
        Mapping from `DimensionElement` to `DimensionRecordStorage` for that
        element.
    universe : `DimensionUniverse`
        All known dimensions.
    """
    def __init__(
        self,
        db: Database, *,
        records: NamedKeyDict[DimensionElement, DimensionRecordStorage],
        universe: DimensionUniverse,
    ):
        super().__init__(universe=universe)
        self._db = db
        self._records = records

    @classmethod
    def initialize(cls, db: Database, context: StaticTablesContext, *,
                   universe: DimensionUniverse) -> DimensionRecordStorageManager:
        # Docstring inherited from DimensionRecordStorageManager.
        records: NamedKeyDict[DimensionElement, DimensionRecordStorage] = NamedKeyDict()
        for element in universe.getStaticElements():
            records[element] = element.makeStorage(db, context=context)
        return cls(db=db, records=records, universe=universe)

    def refresh(self) -> None:
        # Docstring inherited from DimensionRecordStorageManager.
        for dimension in self.universe.getGovernorDimensions():
            storage = self._records[dimension]
            assert isinstance(storage, GovernorDimensionRecordStorage)
            storage.refresh()

    def get(self, element: DimensionElement) -> Optional[DimensionRecordStorage]:
        # Docstring inherited from DimensionRecordStorageManager.
        return self._records.get(element)

    def register(self, element: DimensionElement) -> DimensionRecordStorage:
        # Docstring inherited from DimensionRecordStorageManager.
        result = self._records.get(element)
        assert result, "All records instances should be created in initialize()."
        return result

    def clearCaches(self) -> None:
        # Docstring inherited from DimensionRecordStorageManager.
        for storage in self._records.values():
            storage.clearCaches()

    @classmethod
    def currentVersion(cls) -> Optional[VersionTuple]:
        # Docstring inherited from VersionedExtension.
        return _VERSION

    def schemaDigest(self) -> Optional[str]:
        # Docstring inherited from VersionedExtension.
        tables: List[sqlalchemy.schema.Table] = []
        for recStorage in self._records.values():
            tables += recStorage.digestTables()
        return self._defaultSchemaDigest(tables, self._db.dialect)

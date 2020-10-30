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

import itertools
from typing import Dict, List, Optional, Tuple

import sqlalchemy

from ...core import (
    DatabaseDimensionElement,
    DatabaseTopologicalFamily,
    DimensionElement,
    DimensionUniverse,
    GovernorDimension,
    NamedKeyDict,
    SkyPixDimension,
)
from ..interfaces import (
    Database,
    StaticTablesContext,
    DatabaseDimensionRecordStorage,
    DatabaseDimensionOverlapStorage,
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
    overlaps : `list` [ `DatabaseDimensionOverlapStorage` ]
        Objects that manage materialized overlaps between database-backed
        dimensions.
    universe : `DimensionUniverse`
        All known dimensions.
    """
    def __init__(
        self,
        db: Database, *,
        records: NamedKeyDict[DimensionElement, DimensionRecordStorage],
        overlaps: Dict[Tuple[DatabaseDimensionElement, DatabaseDimensionElement],
                       DatabaseDimensionOverlapStorage],
        universe: DimensionUniverse,
    ):
        super().__init__(universe=universe)
        self._db = db
        self._records = records
        self._overlaps = overlaps

    @classmethod
    def initialize(cls, db: Database, context: StaticTablesContext, *,
                   universe: DimensionUniverse) -> DimensionRecordStorageManager:
        # Docstring inherited from DimensionRecordStorageManager.
        # Start by initializing governor dimensions; those go both in the main
        # 'records' mapping we'll pass to init, and a local dictionary that we
        # can pass in when initializing storage for DatabaseDimensionElements.
        governors: NamedKeyDict[GovernorDimension, GovernorDimensionRecordStorage] = NamedKeyDict()
        records: NamedKeyDict[DimensionElement, DimensionRecordStorage] = NamedKeyDict()
        for dimension in universe.getGovernorDimensions():
            governorStorage = dimension.makeStorage(db, context=context)
            governors[dimension] = governorStorage
            records[dimension] = governorStorage
        # Next we initialize storage for DatabaseDimensionElements.
        # We remember the spatial ones (grouped by family) so we can go back
        # and initialize overlap storage for them later.
        spatial: NamedKeyDict[DatabaseTopologicalFamily, List[DatabaseDimensionRecordStorage]] \
            = NamedKeyDict()
        for element in universe.getDatabaseElements():
            elementStorage = element.makeStorage(db, context=context, governors=governors)
            records[element] = elementStorage
            if element.spatial is not None:
                spatial.setdefault(element.spatial, []).append(elementStorage)
        # Finally we initialize overlap storage.  The implementation class for
        # this is currently hard-coded (it's not obvious there will ever be
        # others).  Note that overlaps between database-backed dimensions and
        # skypix dimensions is internal to `DatabaseDimensionRecordStorage`,
        # and hence is not included here.
        from ..dimensions.overlaps import CrossFamilyDimensionOverlapStorage
        overlaps: Dict[Tuple[DatabaseDimensionElement, DatabaseDimensionElement],
                       DatabaseDimensionOverlapStorage] = {}
        for (family1, storages1), (family2, storages2) in itertools.combinations(spatial.items(), 2):
            for elementStoragePair in itertools.product(storages1, storages2):
                governorStoragePair = (governors[family1.governor], governors[family2.governor])
                if elementStoragePair[0].element > elementStoragePair[1].element:
                    # mypy doesn't realize that tuple(reversed(...)) preserves
                    # the number of elements.
                    elementStoragePair = tuple(reversed(elementStoragePair))  # type: ignore
                    governorStoragePair = tuple(reversed(governorStoragePair))  # type: ignore
                overlapStorage = CrossFamilyDimensionOverlapStorage.initialize(
                    db,
                    elementStoragePair,
                    governorStoragePair,
                    context=context,
                )
                elementStoragePair[0].connect(overlapStorage)
                elementStoragePair[1].connect(overlapStorage)
                overlaps[overlapStorage.elements] = overlapStorage
        return cls(db=db, records=records, overlaps=overlaps, universe=universe)

    def refresh(self) -> None:
        # Docstring inherited from DimensionRecordStorageManager.
        for dimension in self.universe.getGovernorDimensions():
            storage = self._records[dimension]
            assert isinstance(storage, GovernorDimensionRecordStorage)
            storage.refresh()

    def get(self, element: DimensionElement) -> Optional[DimensionRecordStorage]:
        # Docstring inherited from DimensionRecordStorageManager.
        r = self._records.get(element)
        if r is None and isinstance(element, SkyPixDimension):
            return self.universe.skypix[element.system][element.level].makeStorage()
        return r

    def register(self, element: DimensionElement) -> DimensionRecordStorage:
        # Docstring inherited from DimensionRecordStorageManager.
        result = self.get(element)
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
        for overlapStorage in self._overlaps.values():
            tables += overlapStorage.digestTables()
        return self._defaultSchemaDigest(tables, self._db.dialect)

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

__all__ = ("DummyRegistry",)

from collections.abc import Iterable, Iterator
from typing import Any

import sqlalchemy
from lsst.daf.butler import DimensionUniverse, ddl
from lsst.daf.butler.registry.bridge.ephemeral import EphemeralDatastoreRegistryBridge
from lsst.daf.butler.registry.interfaces import (
    Database,
    DatasetIdRef,
    DatasetRecordStorageManager,
    DatastoreRegistryBridge,
    DatastoreRegistryBridgeManager,
    OpaqueTableStorage,
    OpaqueTableStorageManager,
    StaticTablesContext,
    VersionTuple,
)

from ..core.datastore import DatastoreTransaction


class DummyOpaqueTableStorage(OpaqueTableStorage):
    def __init__(self, name: str, spec: ddl.TableSpec) -> None:
        super().__init__(name=name)
        self._rows: list[dict] = []
        self._spec = spec

    def insert(self, *data: dict, transaction: DatastoreTransaction | None = None) -> None:
        # Docstring inherited from OpaqueTableStorage.
        uniqueConstraints = list(self._spec.unique)
        uniqueConstraints.append(tuple(field.name for field in self._spec.fields if field.primaryKey))
        for d in data:
            for constraint in uniqueConstraints:
                matching = list(self.fetch(**{k: d[k] for k in constraint}))
                if len(matching) != 0:
                    raise RuntimeError(
                        f"Unique constraint {constraint} violation in external table {self.name}."
                    )
            self._rows.append(d)
            if transaction is not None:
                transaction.registerUndo("insert", self.delete, [], d)

    def fetch(self, **where: Any) -> Iterator[dict]:
        # Docstring inherited from OpaqueTableStorage.
        where = where.copy()  # May need to modify it.

        # Can support an IN operator if given list.
        wherein = {}
        for k in list(where):
            if isinstance(where[k], (tuple, list, set)):
                wherein[k] = set(where[k])
                del where[k]

        for d in self._rows:
            if all(d[k] == v for k, v in where.items()):
                if wherein:
                    match = True
                    for k, v in wherein.items():
                        if d[k] not in v:
                            match = False
                            break
                    if match:
                        yield d
                else:
                    yield d

    def delete(self, columns: Iterable[str], *rows: dict) -> None:
        # Docstring inherited from OpaqueTableStorage.
        kept_rows = []
        for table_row in self._rows:
            for where_row in rows:
                if all(table_row[k] == v for k, v in where_row.items()):
                    break
            else:
                kept_rows.append(table_row)
        self._rows = kept_rows


class DummyOpaqueTableStorageManager(OpaqueTableStorageManager):
    def __init__(self) -> None:
        self._storages: dict[str, DummyOpaqueTableStorage] = {}

    @classmethod
    def initialize(cls, db: Database, context: StaticTablesContext) -> OpaqueTableStorageManager:
        # Docstring inherited from OpaqueTableStorageManager.
        # Not used, but needed to satisfy ABC requirement.
        return cls()

    def get(self, name: str) -> OpaqueTableStorage | None:
        # Docstring inherited from OpaqueTableStorageManager.
        return self._storages.get(name)

    def register(self, name: str, spec: ddl.TableSpec) -> OpaqueTableStorage:
        # Docstring inherited from OpaqueTableStorageManager.
        return self._storages.setdefault(name, DummyOpaqueTableStorage(name, spec))

    @classmethod
    def currentVersion(cls) -> VersionTuple | None:
        # Docstring inherited from VersionedExtension.
        return None

    def schemaDigest(self) -> str | None:
        # Docstring inherited from VersionedExtension.
        return None


class DummyDatastoreRegistryBridgeManager(DatastoreRegistryBridgeManager):
    def __init__(
        self, opaque: OpaqueTableStorageManager, universe: DimensionUniverse, datasetIdColumnType: type
    ):
        super().__init__(opaque=opaque, universe=universe, datasetIdColumnType=datasetIdColumnType)
        self._bridges: dict[str, EphemeralDatastoreRegistryBridge] = {}

    @classmethod
    def initialize(
        cls,
        db: Database,
        context: StaticTablesContext,
        *,
        opaque: OpaqueTableStorageManager,
        datasets: type[DatasetRecordStorageManager],
        universe: DimensionUniverse,
    ) -> DatastoreRegistryBridgeManager:
        # Docstring inherited from DatastoreRegistryBridgeManager
        # Not used, but needed to satisfy ABC requirement.
        return cls(opaque=opaque, universe=universe, datasetIdColumnType=datasets.getIdColumnType())

    def refresh(self) -> None:
        # Docstring inherited from DatastoreRegistryBridgeManager
        pass

    def register(self, name: str, *, ephemeral: bool = False) -> DatastoreRegistryBridge:
        # Docstring inherited from DatastoreRegistryBridgeManager
        return self._bridges.setdefault(name, EphemeralDatastoreRegistryBridge(name))

    def findDatastores(self, ref: DatasetIdRef) -> Iterable[str]:
        # Docstring inherited from DatastoreRegistryBridgeManager
        for name, bridge in self._bridges.items():
            if ref in bridge:
                yield name

    @classmethod
    def currentVersion(cls) -> VersionTuple | None:
        # Docstring inherited from VersionedExtension.
        return None

    def schemaDigest(self) -> str | None:
        # Docstring inherited from VersionedExtension.
        return None


class DummyRegistry:
    """Dummy Registry, for Datastore test purposes."""

    def __init__(self) -> None:
        self._opaque = DummyOpaqueTableStorageManager()
        self.dimensions = DimensionUniverse()
        self._datastoreBridges = DummyDatastoreRegistryBridgeManager(
            self._opaque, self.dimensions, sqlalchemy.BigInteger
        )

    def getDatastoreBridgeManager(self) -> DatastoreRegistryBridgeManager:
        return self._datastoreBridges

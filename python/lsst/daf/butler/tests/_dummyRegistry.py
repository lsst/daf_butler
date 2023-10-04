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

__all__ = ("DummyRegistry",)

from collections.abc import Iterable, Iterator
from typing import Any

import sqlalchemy
from lsst.daf.butler import DimensionUniverse, ddl
from lsst.daf.butler.registry.bridge.ephemeral import EphemeralDatastoreRegistryBridge
from lsst.daf.butler.registry.interfaces import (
    Database,
    DatabaseInsertMode,
    DatasetIdRef,
    DatasetRecordStorageManager,
    DatastoreRegistryBridge,
    DatastoreRegistryBridgeManager,
    OpaqueTableStorage,
    OpaqueTableStorageManager,
    StaticTablesContext,
    VersionTuple,
)

from ..datastore import DatastoreTransaction


class DummyOpaqueTableStorage(OpaqueTableStorage):
    def __init__(self, name: str, spec: ddl.TableSpec) -> None:
        super().__init__(name=name)
        self._rows: list[dict] = []
        self._spec = spec

    def insert(self, *data: dict, transaction: DatastoreTransaction | None = None) -> None:
        # Docstring inherited from OpaqueTableStorage.
        self._insert(*data, transaction=transaction, insert_mode=DatabaseInsertMode.INSERT)

    def replace(self, *data: dict, transaction: DatastoreTransaction | None = None) -> None:
        # Docstring inherited from OpaqueTableStorage.
        self._insert(*data, transaction=transaction, insert_mode=DatabaseInsertMode.REPLACE)

    def ensure(self, *data: dict, transaction: DatastoreTransaction | None = None) -> None:
        # Docstring inherited from OpaqueTableStorage.
        self._insert(*data, transaction=transaction, insert_mode=DatabaseInsertMode.ENSURE)

    def _insert(
        self,
        *data: dict,
        transaction: DatastoreTransaction | None = None,
        insert_mode: DatabaseInsertMode = DatabaseInsertMode.INSERT,
    ) -> None:
        uniqueConstraints = list(self._spec.unique)
        uniqueConstraints.append(tuple(field.name for field in self._spec.fields if field.primaryKey))
        for d in data:
            skipping = False
            for constraint in uniqueConstraints:
                matching = list(self.fetch(**{k: d[k] for k in constraint}))
                if len(matching) != 0:
                    match insert_mode:
                        case DatabaseInsertMode.INSERT:
                            raise RuntimeError(
                                f"Unique constraint {constraint} violation in external table {self.name}."
                            )
                        case DatabaseInsertMode.ENSURE:
                            # Row already exists. Skip.
                            skipping = True
                        case DatabaseInsertMode.REPLACE:
                            # Should try to put these rows back on transaction
                            # rollback...
                            self.delete([], *matching)
                        case _:
                            raise ValueError(f"Unrecognized insert mode: {insert_mode}.")

            if skipping:
                continue
            self._rows.append(d)
            if transaction is not None:
                transaction.registerUndo("insert", self.delete, [], d)

    def fetch(self, **where: Any) -> Iterator[dict]:
        # Docstring inherited from OpaqueTableStorage.
        where = where.copy()  # May need to modify it.

        # Can support an IN operator if given list.
        wherein = {}
        for k in list(where):
            if isinstance(where[k], tuple | list | set):
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
    def __init__(self, registry_schema_version: VersionTuple | None = None) -> None:
        super().__init__(registry_schema_version=registry_schema_version)
        self._storages: dict[str, DummyOpaqueTableStorage] = {}

    @classmethod
    def initialize(
        cls, db: Database, context: StaticTablesContext, registry_schema_version: VersionTuple | None = None
    ) -> OpaqueTableStorageManager:
        # Docstring inherited from OpaqueTableStorageManager.
        # Not used, but needed to satisfy ABC requirement.
        return cls(registry_schema_version=registry_schema_version)

    def get(self, name: str) -> OpaqueTableStorage | None:
        # Docstring inherited from OpaqueTableStorageManager.
        return self._storages.get(name)

    def register(self, name: str, spec: ddl.TableSpec) -> OpaqueTableStorage:
        # Docstring inherited from OpaqueTableStorageManager.
        return self._storages.setdefault(name, DummyOpaqueTableStorage(name, spec))

    @classmethod
    def currentVersions(cls) -> list[VersionTuple]:
        # Docstring inherited from VersionedExtension.
        return []


class DummyDatastoreRegistryBridgeManager(DatastoreRegistryBridgeManager):
    def __init__(
        self,
        opaque: OpaqueTableStorageManager,
        universe: DimensionUniverse,
        datasetIdColumnType: type,
        registry_schema_version: VersionTuple | None = None,
    ):
        super().__init__(
            opaque=opaque,
            universe=universe,
            datasetIdColumnType=datasetIdColumnType,
            registry_schema_version=registry_schema_version,
        )
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
        registry_schema_version: VersionTuple | None = None,
    ) -> DatastoreRegistryBridgeManager:
        # Docstring inherited from DatastoreRegistryBridgeManager
        # Not used, but needed to satisfy ABC requirement.
        return cls(
            opaque=opaque,
            universe=universe,
            datasetIdColumnType=datasets.getIdColumnType(),
            registry_schema_version=registry_schema_version,
        )

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
    def currentVersions(cls) -> list[VersionTuple]:
        # Docstring inherited from VersionedExtension.
        return []


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

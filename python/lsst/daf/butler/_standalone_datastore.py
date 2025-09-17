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

__all__ = ("instantiate_standalone_datastore",)

from typing import Any

from . import ddl
from ._butler_config import ButlerConfig
from .datastore import Datastore
from .dimensions import DimensionUniverse
from .registry.bridge.monolithic import MonolithicDatastoreRegistryBridgeManager
from .registry.databases.sqlite import SqliteDatabase
from .registry.interfaces import Database, DatastoreRegistryBridgeManager, OpaqueTableStorageManager
from .registry.opaque import ByNameOpaqueTableStorageManager


def instantiate_standalone_datastore(
    butler_config: ButlerConfig,
    dimensions: DimensionUniverse,
    filename: str | None = None,
    OpaqueManagerClass: type[OpaqueTableStorageManager] | None = None,
    BridgeManagerClass: type[DatastoreRegistryBridgeManager] | None = None,
) -> tuple[Datastore, Database]:
    """Initialize a `Datastore` instance without an associated `Registry`.

    Parameters
    ----------
    butler_config : `ButlerConfig`
        Butler configuration that defines the configuration for the `Datastore`
        to be initialized.
    dimensions : `DimensionUniverse`
        Dimension universe used by the Butler repository backing the
        `Datastore`.
    filename : `str`, optional
        Name for the SQLite database that will back the `Datastore`; defaults
        to an in-memory database.
    OpaqueManagerClass : `type`, optional
        A subclass of `OpaqueTableStorageManager` to use for datastore
        opaque records.  Default is a SQL-backed implementation.
    BridgeManagerClass : `type`, optional
        A subclass of `DatastoreRegistryBridgeManager` to use for datastore
        location records.  Default is a SQL-backed implementation.

    Returns
    -------
    datastore_and_database : `tuple` [ `Datastore` , `Database` ]
        The temporary datastore, and the database instance backing it.

    Notes
    -----
    This allows files to be written and read without access to the Butler
    database.  It is primarily used by `QuantumBackedButler`.
    """
    if filename is None:
        filename = ":memory:"
    if OpaqueManagerClass is None:
        OpaqueManagerClass = ByNameOpaqueTableStorageManager
    if BridgeManagerClass is None:
        BridgeManagerClass = MonolithicDatastoreRegistryBridgeManager

    butler_root = butler_config.get("root", butler_config.configDir)
    db = SqliteDatabase.fromUri(f"sqlite:///{filename}", origin=0)
    with db.declareStaticTables(create=True) as context:
        opaque_manager = OpaqueManagerClass.initialize(db, context)
        bridge_manager = BridgeManagerClass.initialize(
            db,
            context,
            opaque=opaque_manager,
            # MyPy can tell it's a fake, but we know it shouldn't care.
            datasets=_DatasetRecordStorageManagerDatastoreConstructionMimic,  # type: ignore
            universe=dimensions,
        )

    datastore = Datastore.fromConfig(butler_config, bridge_manager, butler_root)
    return (datastore, db)


class _DatasetRecordStorageManagerDatastoreConstructionMimic:
    """A partial implementation of `DatasetRecordStorageManager` that exists
    only to allow a `DatastoreRegistryBridgeManager` (and hence a `Datastore`)
    to be constructed without a full `Registry`.
    """

    @classmethod
    def getIdColumnType(cls) -> type:
        # Docstring inherited.
        return ddl.GUID

    @classmethod
    def addDatasetForeignKey(
        cls,
        tableSpec: ddl.TableSpec,
        *,
        name: str = "dataset",
        constraint: bool = True,
        onDelete: str | None = None,
        **kwargs: Any,
    ) -> ddl.FieldSpec:
        # Docstring inherited.
        idFieldSpec = ddl.FieldSpec(f"{name}_id", dtype=ddl.GUID, **kwargs)
        tableSpec.fields.add(idFieldSpec)
        return idFieldSpec

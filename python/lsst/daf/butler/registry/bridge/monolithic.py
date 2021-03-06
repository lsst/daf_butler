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

__all__ = ("MonolithicDatastoreRegistryBridgeManager", "MonolithicDatastoreRegistryBridge")

from collections import namedtuple
from contextlib import contextmanager
import copy
from typing import cast, Dict, Iterable, Iterator, List, Optional, Type, TYPE_CHECKING

import sqlalchemy

from lsst.daf.butler import DatasetRef, ddl, NamedValueSet
from lsst.daf.butler.registry.interfaces import (
    DatasetIdRef,
    DatastoreRegistryBridge,
    DatastoreRegistryBridgeManager,
    FakeDatasetRef,
    VersionTuple,
)
from lsst.daf.butler.registry.bridge.ephemeral import EphemeralDatastoreRegistryBridge

if TYPE_CHECKING:
    from lsst.daf.butler import DimensionUniverse
    from lsst.daf.butler.registry.interfaces import (
        Database,
        DatasetRecordStorageManager,
        OpaqueTableStorageManager,
        StaticTablesContext,
    )


_TablesTuple = namedtuple(
    "_TablesTuple",
    [
        "dataset_location",
        "dataset_location_trash",
    ]
)

# This has to be updated on every schema change
_VERSION = VersionTuple(0, 2, 0)


def _makeTableSpecs(datasets: Type[DatasetRecordStorageManager]) -> _TablesTuple:
    """Construct specifications for tables used by the monolithic datastore
    bridge classes.

    Parameters
    ----------
    universe : `DimensionUniverse`
        All dimensions known to the `Registry`.
    datasets : subclass of `DatasetRecordStorageManager`
        Manager class for datasets; used only to create foreign key fields.

    Returns
    -------
    specs : `_TablesTuple`
        A named tuple containing `ddl.TableSpec` instances.
    """
    # We want the dataset_location and dataset_location_trash tables
    # to have the same definition, aside from the behavior of their link
    # to the dataset table: the trash table has no foreign key constraint.
    dataset_location_spec = ddl.TableSpec(
        doc=(
            "A table that provides information on whether a dataset is stored in "
            "one or more Datastores.  The presence or absence of a record in this "
            "table itself indicates whether the dataset is present in that "
            "Datastore. "
        ),
        fields=NamedValueSet([
            ddl.FieldSpec(
                name="datastore_name",
                dtype=sqlalchemy.String,
                length=256,
                primaryKey=True,
                nullable=False,
                doc="Name of the Datastore this entry corresponds to.",
            ),
        ]),
    )
    dataset_location = copy.deepcopy(dataset_location_spec)
    datasets.addDatasetForeignKey(dataset_location, primaryKey=True)
    dataset_location_trash = copy.deepcopy(dataset_location_spec)
    datasets.addDatasetForeignKey(dataset_location_trash, primaryKey=True, constraint=False)
    return _TablesTuple(
        dataset_location=dataset_location,
        dataset_location_trash=dataset_location_trash,
    )


class MonolithicDatastoreRegistryBridge(DatastoreRegistryBridge):
    """An implementation of `DatastoreRegistryBridge` that uses the same two
    tables for all non-ephemeral datastores.

    Parameters
    ----------
    datastoreName : `str`
        Name of the `Datastore` as it should appear in `Registry` tables
        referencing it.
    db : `Database`
        Object providing a database connection and generic distractions.
    tables : `_TablesTuple`
        Named tuple containing `sqlalchemy.schema.Table` instances.
    """
    def __init__(self, datastoreName: str, *, db: Database, tables: _TablesTuple):
        super().__init__(datastoreName)
        self._db = db
        self._tables = tables

    def _refsToRows(self, refs: Iterable[DatasetIdRef]) -> List[dict]:
        """Transform an iterable of `DatasetRef` or `FakeDatasetRef` objects to
        a list of dictionaries that match the schema of the tables used by this
        class.

        Parameters
        ----------
        refs : `Iterable` [ `DatasetRef` or `FakeDatasetRef` ]
            Datasets to transform.

        Returns
        -------
        rows : `list` [ `dict` ]
            List of dictionaries, with "datastoreName" and "dataset_id" keys.
        """
        return [{"datastore_name": self.datastoreName, "dataset_id": ref.getCheckedId()} for ref in refs]

    def insert(self, refs: Iterable[DatasetIdRef]) -> None:
        # Docstring inherited from DatastoreRegistryBridge
        self._db.insert(self._tables.dataset_location, *self._refsToRows(refs))

    def forget(self, refs: Iterable[DatasetIdRef]) -> None:
        # Docstring inherited from DatastoreRegistryBridge
        rows = self._refsToRows(self.check(refs))
        self._db.delete(self._tables.dataset_location, ["datastore_name", "dataset_id"], *rows)

    def moveToTrash(self, refs: Iterable[DatasetIdRef]) -> None:
        # Docstring inherited from DatastoreRegistryBridge
        # TODO: avoid self.check() call via queries like
        #     INSERT INTO dataset_location_trash
        #         SELECT datastore_name, dataset_id FROM dataset_location
        #         WHERE datastore_name=? AND dataset_id IN (?);
        #     DELETE FROM dataset_location
        #         WHERE datastore_name=? AND dataset_id IN (?);
        # ...but the Database interface doesn't support those kinds of queries
        # right now.
        rows = self._refsToRows(self.check(refs))
        with self._db.transaction():
            self._db.delete(self._tables.dataset_location, ["datastore_name", "dataset_id"], *rows)
            self._db.insert(self._tables.dataset_location_trash, *rows)

    def check(self, refs: Iterable[DatasetIdRef]) -> Iterable[DatasetIdRef]:
        # Docstring inherited from DatastoreRegistryBridge
        byId = {ref.getCheckedId(): ref for ref in refs}
        sql = sqlalchemy.sql.select(
            [self._tables.dataset_location.columns.dataset_id]
        ).select_from(
            self._tables.dataset_location
        ).where(
            sqlalchemy.sql.and_(
                self._tables.dataset_location.columns.datastore_name == self.datastoreName,
                self._tables.dataset_location.columns.dataset_id.in_(byId.keys())
            )
        )
        for row in self._db.query(sql).fetchall():
            yield byId[row["dataset_id"]]

    @contextmanager
    def emptyTrash(self) -> Iterator[Iterable[DatasetIdRef]]:
        # Docstring inherited from DatastoreRegistryBridge
        sql = sqlalchemy.sql.select(
            [self._tables.dataset_location_trash.columns.dataset_id]
        ).select_from(
            self._tables.dataset_location_trash
        ).where(
            self._tables.dataset_location_trash.columns.datastore_name == self.datastoreName
        )
        # Run query, transform results into a list of dicts that we can later
        # use to delete.
        rows = [{"dataset_id": row["dataset_id"], "datastore_name": self.datastoreName}
                for row in self._db.query(sql).fetchall()]
        # Start contextmanager, returning generator expression to iterate over.
        yield (FakeDatasetRef(row["dataset_id"]) for row in rows)
        # No exception raised in context manager block.  Delete those rows
        # from the trash table.
        self._db.delete(self._tables.dataset_location_trash, ["dataset_id", "datastore_name"], *rows)


class MonolithicDatastoreRegistryBridgeManager(DatastoreRegistryBridgeManager):
    """An implementation of `DatastoreRegistryBridgeManager` that uses the same
    two tables for all non-ephemeral datastores.

    Parameters
    ----------
    db : `Database`
        Object providing a database connection and generic distractions.
    tables : `_TablesTuple`
        Named tuple containing `sqlalchemy.schema.Table` instances.
    opaque : `OpaqueTableStorageManager`
        Manager object for opaque table storage in the `Registry`.
    universe : `DimensionUniverse`
        All dimensions know to the `Registry`.
    """
    def __init__(self, *, db: Database, tables: _TablesTuple,
                 opaque: OpaqueTableStorageManager, universe: DimensionUniverse):
        super().__init__(opaque=opaque, universe=universe)
        self._db = db
        self._tables = tables
        self._ephemeral: Dict[str, EphemeralDatastoreRegistryBridge] = {}

    @classmethod
    def initialize(cls, db: Database, context: StaticTablesContext, *,
                   opaque: OpaqueTableStorageManager,
                   datasets: Type[DatasetRecordStorageManager],
                   universe: DimensionUniverse,
                   ) -> DatastoreRegistryBridgeManager:
        # Docstring inherited from DatastoreRegistryBridge
        tables = context.addTableTuple(_makeTableSpecs(datasets))
        return cls(db=db, tables=cast(_TablesTuple, tables), opaque=opaque, universe=universe)

    def refresh(self) -> None:
        # Docstring inherited from DatastoreRegistryBridge
        # This implementation has no in-Python state that depends on which
        # datastores exist, so there's nothing to do.
        pass

    def register(self, name: str, *, ephemeral: bool = False) -> DatastoreRegistryBridge:
        # Docstring inherited from DatastoreRegistryBridge
        if ephemeral:
            return self._ephemeral.setdefault(name, EphemeralDatastoreRegistryBridge(name))
        return MonolithicDatastoreRegistryBridge(name, db=self._db, tables=self._tables)

    def findDatastores(self, ref: DatasetRef) -> Iterable[str]:
        # Docstring inherited from DatastoreRegistryBridge
        sql = sqlalchemy.sql.select(
            [self._tables.dataset_location.columns.datastore_name]
        ).select_from(
            self._tables.dataset_location
        ).where(
            self._tables.dataset_location.columns.dataset_id == ref.getCheckedId()
        )
        for row in self._db.query(sql).fetchall():
            yield row[self._tables.dataset_location.columns.datastore_name]
        for name, bridge in self._ephemeral.items():
            if ref in bridge:
                yield name

    @classmethod
    def currentVersion(cls) -> Optional[VersionTuple]:
        # Docstring inherited from VersionedExtension.
        return _VERSION

    def schemaDigest(self) -> Optional[str]:
        # Docstring inherited from VersionedExtension.
        return self._defaultSchemaDigest(self._tables, self._db.dialect)

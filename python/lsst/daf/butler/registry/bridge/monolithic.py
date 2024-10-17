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

from ... import ddl

__all__ = ("MonolithicDatastoreRegistryBridgeManager", "MonolithicDatastoreRegistryBridge")

import copy
from collections import namedtuple
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, cast

import sqlalchemy

from ..._named import NamedValueSet
from ...datastore.stored_file_info import StoredDatastoreItemInfo
from ..interfaces import (
    DatasetIdRef,
    DatastoreRegistryBridge,
    DatastoreRegistryBridgeManager,
    FakeDatasetRef,
    OpaqueTableStorage,
    VersionTuple,
)
from ..opaque import ByNameOpaqueTableStorage
from .ephemeral import EphemeralDatastoreRegistryBridge

if TYPE_CHECKING:
    from ...datastore import DatastoreTransaction
    from ...dimensions import DimensionUniverse
    from ..interfaces import (
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
    ],
)

# This has to be updated on every schema change
_VERSION = VersionTuple(0, 2, 0)


def _makeTableSpecs(datasets: type[DatasetRecordStorageManager]) -> _TablesTuple:
    """Construct specifications for tables used by the monolithic datastore
    bridge classes.

    Parameters
    ----------
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
        fields=NamedValueSet(
            [
                ddl.FieldSpec(
                    name="datastore_name",
                    dtype=sqlalchemy.String,
                    length=256,
                    primaryKey=True,
                    nullable=False,
                    doc="Name of the Datastore this entry corresponds to.",
                ),
            ]
        ),
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

    def _refsToRows(self, refs: Iterable[DatasetIdRef]) -> list[dict]:
        """Transform an iterable of `DatasetRef` or `FakeDatasetRef` objects to
        a list of dictionaries that match the schema of the tables used by this
        class.

        Parameters
        ----------
        refs : `~collections.abc.Iterable` [ `DatasetRef` or `FakeDatasetRef` ]
            Datasets to transform.

        Returns
        -------
        rows : `list` [ `dict` ]
            List of dictionaries, with "datastoreName" and "dataset_id" keys.
        """
        return [{"datastore_name": self.datastoreName, "dataset_id": ref.id} for ref in refs]

    def ensure(self, refs: Iterable[DatasetIdRef]) -> None:
        # Docstring inherited from DatastoreRegistryBridge
        self._db.ensure(self._tables.dataset_location, *self._refsToRows(refs))

    def insert(self, refs: Iterable[DatasetIdRef]) -> None:
        # Docstring inherited from DatastoreRegistryBridge
        self._db.insert(self._tables.dataset_location, *self._refsToRows(refs))

    def forget(self, refs: Iterable[DatasetIdRef]) -> None:
        # Docstring inherited from DatastoreRegistryBridge
        rows = self._refsToRows(self.check(refs))
        self._db.delete(self._tables.dataset_location, ["datastore_name", "dataset_id"], *rows)

    def moveToTrash(self, refs: Iterable[DatasetIdRef], transaction: DatastoreTransaction | None) -> None:
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
        byId = {ref.id: ref for ref in refs}
        sql = (
            sqlalchemy.sql.select(self._tables.dataset_location.columns.dataset_id)
            .select_from(self._tables.dataset_location)
            .where(
                sqlalchemy.sql.and_(
                    self._tables.dataset_location.columns.datastore_name == self.datastoreName,
                    self._tables.dataset_location.columns.dataset_id.in_(byId.keys()),
                )
            )
        )
        with self._db.query(sql) as sql_result:
            sql_rows = sql_result.fetchall()
        for row in sql_rows:
            yield byId[row.dataset_id]

    @contextmanager
    def emptyTrash(
        self,
        records_table: OpaqueTableStorage | None = None,
        record_class: type[StoredDatastoreItemInfo] | None = None,
        record_column: str | None = None,
    ) -> Iterator[tuple[Iterable[tuple[DatasetIdRef, StoredDatastoreItemInfo | None]], set[str] | None]]:
        # Docstring inherited from DatastoreRegistryBridge

        if records_table is None:
            raise ValueError("This implementation requires a records table.")

        assert isinstance(
            records_table, ByNameOpaqueTableStorage
        ), f"Records table must support hidden attributes. Got {type(records_table)}."

        if record_class is None:
            raise ValueError("Record class must be provided if records table is given.")

        # Helper closure to generate the common join+where clause.
        def join_records(
            select: sqlalchemy.sql.Select, location_table: sqlalchemy.schema.Table
        ) -> sqlalchemy.sql.Select:
            # mypy needs to be sure
            assert isinstance(records_table, ByNameOpaqueTableStorage)
            return select.select_from(
                records_table._table.join(
                    location_table,
                    onclause=records_table._table.columns.dataset_id == location_table.columns.dataset_id,
                )
            ).where(location_table.columns.datastore_name == self.datastoreName)

        # SELECT records.dataset_id, records.path FROM records
        #    JOIN records on dataset_location.dataset_id == records.dataset_id
        #    WHERE dataset_location.datastore_name = datastoreName

        # It's possible that we may end up with a ref listed in the trash
        # table that is not listed in the records table. Such an
        # inconsistency would be missed by this query.
        info_in_trash = join_records(records_table._table.select(), self._tables.dataset_location_trash)

        # Run query, transform results into a list of dicts that we can later
        # use to delete.
        with self._db.query(info_in_trash) as sql_result:
            rows = [dict(row, datastore_name=self.datastoreName) for row in sql_result.mappings()]

        # It is possible for trashed refs to be linked to artifacts that
        # are still associated with refs that are not to be trashed. We
        # need to be careful to consider those and indicate to the caller
        # that those artifacts should be retained. Can only do this check
        # if the caller provides a column name that can map to multiple
        # refs.
        preserved: set[str] | None = None
        if record_column is not None:
            # Some helper subqueries
            items_not_in_trash = join_records(
                sqlalchemy.sql.select(records_table._table.columns[record_column]),
                self._tables.dataset_location,
            ).alias("items_not_in_trash")
            items_in_trash = join_records(
                sqlalchemy.sql.select(records_table._table.columns[record_column]),
                self._tables.dataset_location_trash,
            ).alias("items_in_trash")

            # A query for paths that are referenced by datasets in the trash
            # and datasets not in the trash.
            items_to_preserve = sqlalchemy.sql.select(items_in_trash.columns[record_column]).select_from(
                items_not_in_trash.join(
                    items_in_trash,
                    onclause=items_in_trash.columns[record_column]
                    == items_not_in_trash.columns[record_column],
                )
            )
            with self._db.query(items_to_preserve) as sql_result:
                preserved = {row[record_column] for row in sql_result.mappings()}

        # Convert results to a tuple of id+info and a record of the artifacts
        # that should not be deleted from datastore. The id+info tuple is
        # solely to allow logging to report the relevant ID.
        id_info = ((FakeDatasetRef(row["dataset_id"]), record_class.from_record(row)) for row in rows)

        # Start contextmanager, return results
        yield ((id_info, preserved))

        # No exception raised in context manager block.
        if not rows:
            return

        # Delete the rows from the records table
        records_table.delete(["dataset_id"], *[{"dataset_id": row["dataset_id"]} for row in rows])

        # Delete those rows from the trash table.
        self._db.delete(
            self._tables.dataset_location_trash,
            ["dataset_id", "datastore_name"],
            *[{"dataset_id": row["dataset_id"], "datastore_name": row["datastore_name"]} for row in rows],
        )


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
    registry_schema_version : `VersionTuple` or `None`, optional
        The version of the registry schema.
    """

    def __init__(
        self,
        *,
        db: Database,
        tables: _TablesTuple,
        opaque: OpaqueTableStorageManager,
        universe: DimensionUniverse,
        registry_schema_version: VersionTuple | None = None,
    ):
        super().__init__(
            opaque=opaque,
            universe=universe,
            registry_schema_version=registry_schema_version,
        )
        self._db = db
        self._tables = tables
        self._ephemeral: dict[str, EphemeralDatastoreRegistryBridge] = {}

    def clone(self, *, db: Database, opaque: OpaqueTableStorageManager) -> DatastoreRegistryBridgeManager:
        return MonolithicDatastoreRegistryBridgeManager(
            db=db,
            tables=self._tables,
            opaque=opaque,
            universe=self.universe,
            registry_schema_version=self._registry_schema_version,
        )

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
        # Docstring inherited from DatastoreRegistryBridge
        tables = context.addTableTuple(_makeTableSpecs(datasets))
        return cls(
            db=db,
            tables=cast(_TablesTuple, tables),
            opaque=opaque,
            universe=universe,
            registry_schema_version=registry_schema_version,
        )

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

    def findDatastores(self, ref: DatasetIdRef) -> Iterable[str]:
        # Docstring inherited from DatastoreRegistryBridge
        sql = (
            sqlalchemy.sql.select(self._tables.dataset_location.columns.datastore_name)
            .select_from(self._tables.dataset_location)
            .where(self._tables.dataset_location.columns.dataset_id == ref.id)
        )
        with self._db.query(sql) as sql_result:
            sql_rows = sql_result.mappings().fetchall()
        for row in sql_rows:
            yield row[self._tables.dataset_location.columns.datastore_name]
        for name, bridge in self._ephemeral.items():
            if ref in bridge:
                yield name

    @classmethod
    def currentVersions(cls) -> list[VersionTuple]:
        # Docstring inherited from VersionedExtension.
        return [_VERSION]

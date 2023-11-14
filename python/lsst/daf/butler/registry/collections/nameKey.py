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

__all__ = ["NameKeyCollectionManager"]

from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any

import sqlalchemy

from ... import ddl
from ..._timespan import TimespanDatabaseRepresentation
from .._collection_type import CollectionType
from ..interfaces import ChainedCollectionRecord, CollectionRecord, RunRecord, VersionTuple
from ._base import (
    CollectionTablesTuple,
    DefaultCollectionManager,
    makeCollectionChainTableSpec,
    makeRunTableSpec,
)

if TYPE_CHECKING:
    from .._caching_context import CachingContext
    from ..interfaces import Database, DimensionRecordStorageManager, StaticTablesContext


_KEY_FIELD_SPEC = ddl.FieldSpec("name", dtype=sqlalchemy.String, length=64, primaryKey=True)


# This has to be updated on every schema change
_VERSION = VersionTuple(2, 0, 0)


def _makeTableSpecs(TimespanReprClass: type[TimespanDatabaseRepresentation]) -> CollectionTablesTuple:
    return CollectionTablesTuple(
        collection=ddl.TableSpec(
            fields=[
                _KEY_FIELD_SPEC,
                ddl.FieldSpec("type", dtype=sqlalchemy.SmallInteger, nullable=False),
                ddl.FieldSpec("doc", dtype=sqlalchemy.Text, nullable=True),
            ],
        ),
        run=makeRunTableSpec("name", sqlalchemy.String, TimespanReprClass),
        collection_chain=makeCollectionChainTableSpec("name", sqlalchemy.String),
    )


class NameKeyCollectionManager(DefaultCollectionManager[str]):
    """A `CollectionManager` implementation that uses collection names for
    primary/foreign keys and aggressively loads all collection/run records in
    the database into memory.

    Most of the logic, including caching policy, is implemented in the base
    class, this class only adds customizations specific to this particular
    table schema.
    """

    @classmethod
    def initialize(
        cls,
        db: Database,
        context: StaticTablesContext,
        *,
        dimensions: DimensionRecordStorageManager,
        caching_context: CachingContext,
        registry_schema_version: VersionTuple | None = None,
    ) -> NameKeyCollectionManager:
        # Docstring inherited from CollectionManager.
        return cls(
            db,
            tables=context.addTableTuple(_makeTableSpecs(db.getTimespanRepresentation())),  # type: ignore
            collectionIdName="name",
            dimensions=dimensions,
            caching_context=caching_context,
            registry_schema_version=registry_schema_version,
        )

    @classmethod
    def getCollectionForeignKeyName(cls, prefix: str = "collection") -> str:
        # Docstring inherited from CollectionManager.
        return f"{prefix}_name"

    @classmethod
    def getRunForeignKeyName(cls, prefix: str = "run") -> str:
        # Docstring inherited from CollectionManager.
        return f"{prefix}_name"

    @classmethod
    def addCollectionForeignKey(
        cls,
        tableSpec: ddl.TableSpec,
        *,
        prefix: str = "collection",
        onDelete: str | None = None,
        constraint: bool = True,
        **kwargs: Any,
    ) -> ddl.FieldSpec:
        # Docstring inherited from CollectionManager.
        original = _KEY_FIELD_SPEC
        copy = ddl.FieldSpec(
            cls.getCollectionForeignKeyName(prefix), dtype=original.dtype, length=original.length, **kwargs
        )
        tableSpec.fields.add(copy)
        if constraint:
            tableSpec.foreignKeys.append(
                ddl.ForeignKeySpec(
                    "collection", source=(copy.name,), target=(original.name,), onDelete=onDelete
                )
            )
        return copy

    @classmethod
    def addRunForeignKey(
        cls,
        tableSpec: ddl.TableSpec,
        *,
        prefix: str = "run",
        onDelete: str | None = None,
        constraint: bool = True,
        **kwargs: Any,
    ) -> ddl.FieldSpec:
        # Docstring inherited from CollectionManager.
        original = _KEY_FIELD_SPEC
        copy = ddl.FieldSpec(
            cls.getRunForeignKeyName(prefix), dtype=original.dtype, length=original.length, **kwargs
        )
        tableSpec.fields.add(copy)
        if constraint:
            tableSpec.foreignKeys.append(
                ddl.ForeignKeySpec("run", source=(copy.name,), target=(original.name,), onDelete=onDelete)
            )
        return copy

    def getParentChains(self, key: str) -> set[str]:
        # Docstring inherited from CollectionManager.
        table = self._tables.collection_chain
        sql = (
            sqlalchemy.sql.select(table.columns["parent"])
            .select_from(table)
            .where(table.columns["child"] == key)
        )
        with self._db.query(sql) as sql_result:
            parent_names = set(sql_result.scalars().all())
        return parent_names

    def _fetch_by_name(self, names: Iterable[str]) -> list[CollectionRecord[str]]:
        # Docstring inherited from base class.
        return self._fetch_by_key(names)

    def _fetch_by_key(self, collection_ids: Iterable[str] | None) -> list[CollectionRecord[str]]:
        # Docstring inherited from base class.
        sql = sqlalchemy.sql.select(*self._tables.collection.columns, *self._tables.run.columns).select_from(
            self._tables.collection.join(self._tables.run, isouter=True)
        )

        chain_sql = sqlalchemy.sql.select(
            self._tables.collection_chain.columns["parent"],
            self._tables.collection_chain.columns["position"],
            self._tables.collection_chain.columns["child"],
        )

        records: list[CollectionRecord[str]] = []
        # We want to keep transactions as short as possible. When we fetch
        # everything we want to quickly fetch things into memory and finish
        # transaction. When we fetch just few records we need to process result
        # of the first query before we can run the second one.
        if collection_ids is not None:
            sql = sql.where(self._tables.collection.columns[self._collectionIdName].in_(collection_ids))
            with self._db.transaction():
                with self._db.query(sql) as sql_result:
                    sql_rows = sql_result.mappings().fetchall()

                records, chained_ids = self._rows_to_records(sql_rows)

                if chained_ids:
                    # Retrieve chained collection compositions
                    chain_sql = chain_sql.where(
                        self._tables.collection_chain.columns["parent"].in_(chained_ids)
                    )
                    with self._db.query(chain_sql) as sql_result:
                        chain_rows = sql_result.mappings().fetchall()

                    records += self._rows_to_chains(chain_rows, chained_ids)

        else:
            with self._db.transaction():
                with self._db.query(sql) as sql_result:
                    sql_rows = sql_result.mappings().fetchall()
                with self._db.query(chain_sql) as sql_result:
                    chain_rows = sql_result.mappings().fetchall()

            records, chained_ids = self._rows_to_records(sql_rows)
            records += self._rows_to_chains(chain_rows, chained_ids)

        return records

    def _rows_to_records(self, rows: Iterable[Mapping]) -> tuple[list[CollectionRecord[str]], list[str]]:
        """Convert rows returned from collection query to a list of records
        and a list chained collection names.
        """
        records: list[CollectionRecord[str]] = []
        TimespanReprClass = self._db.getTimespanRepresentation()
        chained_ids: list[str] = []
        for row in rows:
            name = row[self._tables.collection.columns.name]
            type = CollectionType(row["type"])
            record: CollectionRecord[str]
            if type is CollectionType.RUN:
                record = RunRecord[str](
                    key=name,
                    name=name,
                    host=row[self._tables.run.columns.host],
                    timespan=TimespanReprClass.extract(row),
                )
                records.append(record)
            elif type is CollectionType.CHAINED:
                # Need to delay chained collection construction until to
                # fetch their children names.
                chained_ids.append(name)
            else:
                record = CollectionRecord[str](key=name, name=name, type=type)
                records.append(record)

        return records, chained_ids

    def _rows_to_chains(self, rows: Iterable[Mapping], chained_ids: list[str]) -> list[CollectionRecord[str]]:
        """Convert rows returned from collection chain query to a list of
        records.
        """
        chains_defs: dict[str, list[tuple[int, str]]] = {chain_id: [] for chain_id in chained_ids}
        for row in rows:
            chains_defs[row["parent"]].append((row["position"], row["child"]))

        records: list[CollectionRecord[str]] = []
        for name, children in chains_defs.items():
            children_names = [child for _, child in sorted(children)]
            record = ChainedCollectionRecord[str](
                key=name,
                name=name,
                children=children_names,
            )
            records.append(record)

        return records

    @classmethod
    def currentVersions(cls) -> list[VersionTuple]:
        # Docstring inherited from VersionedExtension.
        return [_VERSION]

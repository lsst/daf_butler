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

__all__ = ["SynthIntKeyCollectionManager"]

from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any

import sqlalchemy

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


_KEY_FIELD_SPEC = ddl.FieldSpec(
    "collection_id", dtype=sqlalchemy.BigInteger, primaryKey=True, autoincrement=True
)


# This has to be updated on every schema change
_VERSION = VersionTuple(2, 0, 0)


def _makeTableSpecs(TimespanReprClass: type[TimespanDatabaseRepresentation]) -> CollectionTablesTuple:
    return CollectionTablesTuple(
        collection=ddl.TableSpec(
            fields=[
                _KEY_FIELD_SPEC,
                ddl.FieldSpec("name", dtype=sqlalchemy.String, length=64, nullable=False),
                ddl.FieldSpec("type", dtype=sqlalchemy.SmallInteger, nullable=False),
                ddl.FieldSpec("doc", dtype=sqlalchemy.Text, nullable=True),
            ],
            unique=[("name",)],
        ),
        run=makeRunTableSpec("collection_id", sqlalchemy.BigInteger, TimespanReprClass),
        collection_chain=makeCollectionChainTableSpec("collection_id", sqlalchemy.BigInteger),
    )


class SynthIntKeyCollectionManager(DefaultCollectionManager[int]):
    """A `CollectionManager` implementation that uses synthetic primary key
    (auto-incremented integer) for collections table.
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
    ) -> SynthIntKeyCollectionManager:
        # Docstring inherited from CollectionManager.
        return cls(
            db,
            tables=context.addTableTuple(_makeTableSpecs(db.getTimespanRepresentation())),  # type: ignore
            collectionIdName="collection_id",
            dimensions=dimensions,
            caching_context=caching_context,
            registry_schema_version=registry_schema_version,
        )

    @classmethod
    def getCollectionForeignKeyName(cls, prefix: str = "collection") -> str:
        # Docstring inherited from CollectionManager.
        return f"{prefix}_id"

    @classmethod
    def getRunForeignKeyName(cls, prefix: str = "run") -> str:
        # Docstring inherited from CollectionManager.
        return f"{prefix}_id"

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
            cls.getCollectionForeignKeyName(prefix), dtype=original.dtype, autoincrement=False, **kwargs
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
            cls.getRunForeignKeyName(prefix), dtype=original.dtype, autoincrement=False, **kwargs
        )
        tableSpec.fields.add(copy)
        if constraint:
            tableSpec.foreignKeys.append(
                ddl.ForeignKeySpec("run", source=(copy.name,), target=(original.name,), onDelete=onDelete)
            )
        return copy

    def getParentChains(self, key: int) -> set[str]:
        # Docstring inherited from CollectionManager.
        chain = self._tables.collection_chain
        collection = self._tables.collection
        sql = (
            sqlalchemy.sql.select(collection.columns["name"])
            .select_from(collection)
            .join(chain, onclause=collection.columns[self._collectionIdName] == chain.columns["parent"])
            .where(chain.columns["child"] == key)
        )
        with self._db.query(sql) as sql_result:
            parent_names = set(sql_result.scalars().all())
        return parent_names

    def _fetch_by_name(self, names: Iterable[str]) -> list[CollectionRecord[int]]:
        # Docstring inherited from base class.
        return self._fetch("name", names)

    def _fetch_by_key(self, collection_ids: Iterable[int] | None) -> list[CollectionRecord[int]]:
        # Docstring inherited from base class.
        return self._fetch(self._collectionIdName, collection_ids)

    def _fetch(
        self, column_name: str, collections: Iterable[int | str] | None
    ) -> list[CollectionRecord[int]]:
        collection_chain = self._tables.collection_chain
        collection = self._tables.collection
        sql = sqlalchemy.sql.select(*collection.columns, *self._tables.run.columns).select_from(
            collection.join(self._tables.run, isouter=True)
        )

        chain_sql = (
            sqlalchemy.sql.select(
                collection_chain.columns["parent"],
                collection_chain.columns["position"],
                collection.columns["name"].label("child_name"),
            )
            .select_from(collection_chain)
            .join(
                collection,
                onclause=collection_chain.columns["child"] == collection.columns[self._collectionIdName],
            )
        )

        records: list[CollectionRecord[int]] = []
        # We want to keep transactions as short as possible. When we fetch
        # everything we want to quickly fetch things into memory and finish
        # transaction. When we fetch just few records we need to process first
        # query before wi can run second one,
        if collections is not None:
            sql = sql.where(collection.columns[column_name].in_(collections))
            with self._db.transaction():
                with self._db.query(sql) as sql_result:
                    sql_rows = sql_result.mappings().fetchall()

                records, chained_ids = self._rows_to_records(sql_rows)

                if chained_ids:
                    chain_sql = chain_sql.where(collection_chain.columns["parent"].in_(list(chained_ids)))

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

    def _rows_to_records(self, rows: Iterable[Mapping]) -> tuple[list[CollectionRecord[int]], dict[int, str]]:
        """Convert rows returned from collection query to a list of records
        and a dict chained collection names.
        """
        records: list[CollectionRecord[int]] = []
        chained_ids: dict[int, str] = {}
        TimespanReprClass = self._db.getTimespanRepresentation()
        for row in rows:
            key: int = row[self._collectionIdName]
            name: str = row[self._tables.collection.columns.name]
            type = CollectionType(row["type"])
            record: CollectionRecord[int]
            if type is CollectionType.RUN:
                record = RunRecord[int](
                    key=key,
                    name=name,
                    host=row[self._tables.run.columns.host],
                    timespan=TimespanReprClass.extract(row),
                )
                records.append(record)
            elif type is CollectionType.CHAINED:
                # Need to delay chained collection construction until to
                # fetch their children names.
                chained_ids[key] = name
            else:
                record = CollectionRecord[int](key=key, name=name, type=type)
                records.append(record)
        return records, chained_ids

    def _rows_to_chains(
        self, rows: Iterable[Mapping], chained_ids: dict[int, str]
    ) -> list[CollectionRecord[int]]:
        """Convert rows returned from collection chain query to a list of
        records.
        """
        chains_defs: dict[int, list[tuple[int, str]]] = {chain_id: [] for chain_id in chained_ids}
        for row in rows:
            chains_defs[row["parent"]].append((row["position"], row["child_name"]))

        records: list[CollectionRecord[int]] = []
        for key, children in chains_defs.items():
            name = chained_ids[key]
            children_names = [child for _, child in sorted(children)]
            record = ChainedCollectionRecord[int](
                key=key,
                name=name,
                children=children_names,
            )
            records.append(record)

        return records

    @classmethod
    def currentVersions(cls) -> list[VersionTuple]:
        # Docstring inherited from VersionedExtension.
        return [_VERSION]

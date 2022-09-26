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

__all__ = ["NameKeyCollectionManager"]

from typing import TYPE_CHECKING, Any

import sqlalchemy

from ...core import TimespanDatabaseRepresentation, ddl
from ..interfaces import VersionTuple
from ._base import (
    CollectionTablesTuple,
    DefaultCollectionManager,
    makeCollectionChainTableSpec,
    makeRunTableSpec,
)

if TYPE_CHECKING:
    from ..interfaces import CollectionRecord, Database, DimensionRecordStorageManager, StaticTablesContext


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


class NameKeyCollectionManager(DefaultCollectionManager):
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
    ) -> NameKeyCollectionManager:
        # Docstring inherited from CollectionManager.
        return cls(
            db,
            tables=context.addTableTuple(_makeTableSpecs(db.getTimespanRepresentation())),  # type: ignore
            collectionIdName="name",
            dimensions=dimensions,
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

    def _getByName(self, name: str) -> CollectionRecord | None:
        # Docstring inherited from DefaultCollectionManager.
        return self._records.get(name)

    @classmethod
    def currentVersion(cls) -> VersionTuple | None:
        # Docstring inherited from VersionedExtension.
        return _VERSION

    def schemaDigest(self) -> str | None:
        # Docstring inherited from VersionedExtension.
        return self._defaultSchemaDigest(self._tables, self._db.dialect)

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

from typing import (
    Any,
    Optional,
    Tuple,
    TYPE_CHECKING,
)

import sqlalchemy

from ._base import (
    CollectionTablesTuple,
    DefaultCollectionManager,
    makeRunTableSpec,
    makeCollectionChainTableSpec,
)
from ...core import ddl

if TYPE_CHECKING:
    from ..interfaces import CollectionRecord, Database, StaticTablesContext

_TABLES_SPEC = CollectionTablesTuple(
    collection=ddl.TableSpec(
        fields=[
            ddl.FieldSpec("name", dtype=sqlalchemy.String, length=64, primaryKey=True),
            ddl.FieldSpec("type", dtype=sqlalchemy.SmallInteger, nullable=False),
        ],
    ),
    run=makeRunTableSpec("name", sqlalchemy.String),
    collection_chain=makeCollectionChainTableSpec("name", sqlalchemy.String),
)


class NameKeyCollectionManager(DefaultCollectionManager[Tuple[str]]):
    """A `CollectionManager` implementation that uses collection names for
    primary/foreign keys and aggressively loads all collection/run records in
    the database into memory.

    Most of the logic, including caching policy, is implemented in the base
    class, this class only adds customisations specific to this particular
    table schema.
    """

    @classmethod
    def initialize(cls, db: Database, context: StaticTablesContext) -> NameKeyCollectionManager:
        # Docstring inherited from CollectionManager.
        return cls(db, tables=context.addTableTuple(_TABLES_SPEC),  # type: ignore
                   collectionKeyNames=("name",))

    @classmethod
    def getCollectionForeignKeyNames(cls, prefix: str = "collection") -> Tuple[str]:
        # Docstring inherited from CollectionManager.
        return (f"{prefix}_name",)

    @classmethod
    def getRunForeignKeyNames(cls, prefix: str = "run") -> Tuple[str]:
        # Docstring inherited from CollectionManager.
        return (f"{prefix}_name",)

    @classmethod
    def addCollectionForeignKeys(cls, tableSpec: ddl.TableSpec, *,
                                 prefix: str = "collection",
                                 onDelete: Optional[str] = None,
                                 **kwargs: Any
                                 ) -> Tuple[ddl.FieldSpec]:
        # Docstring inherited from CollectionManager.
        original = _TABLES_SPEC.collection.fields["name"]
        copy = ddl.FieldSpec(cls.getCollectionForeignKeyNames(prefix)[0], dtype=original.dtype,
                             length=original.length, **kwargs)
        tableSpec.fields.add(copy)
        tableSpec.foreignKeys.append(ddl.ForeignKeySpec("collection", source=(copy.name,),
                                                        target=(original.name,), onDelete=onDelete))
        return (copy,)

    @classmethod
    def addRunForeignKeys(cls, tableSpec: ddl.TableSpec, *,
                          prefix: str = "run",
                          onDelete: Optional[str] = None,
                          **kwargs: Any
                          ) -> Tuple[ddl.FieldSpec]:
        # Docstring inherited from CollectionManager.
        original = _TABLES_SPEC.run.fields["name"]
        copy = ddl.FieldSpec(cls.getRunForeignKeyNames(prefix)[0], dtype=original.dtype,
                             length=original.length, **kwargs)
        tableSpec.fields.add(copy)
        tableSpec.foreignKeys.append(ddl.ForeignKeySpec("run", source=(copy.name,),
                                                        target=(original.name,), onDelete=onDelete))
        return (copy,)

    def _getByName(self, name: str) -> Optional[CollectionRecord]:
        # Docstring inherited from DefaultCollectionManager.
        return self._records.get(name)

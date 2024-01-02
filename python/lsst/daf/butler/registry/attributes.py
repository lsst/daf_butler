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

"""The default concrete implementation of the class that manages
attributes for `Registry`.
"""

from __future__ import annotations

__all__ = ["DefaultButlerAttributeManager"]

from collections.abc import Iterable
from typing import ClassVar

import sqlalchemy

from ..ddl import FieldSpec, TableSpec
from .interfaces import (
    ButlerAttributeExistsError,
    ButlerAttributeManager,
    Database,
    StaticTablesContext,
    VersionTuple,
)

# Schema version 1.0.1 signifies that we do not write schema digests. Writing
# is done by the `versions` module, but table is controlled by this manager.
_VERSION = VersionTuple(1, 0, 1)


class DefaultButlerAttributeManager(ButlerAttributeManager):
    """An implementation of `ButlerAttributeManager` that stores attributes
    in a database table.

    Parameters
    ----------
    db : `Database`
        Database engine interface for the namespace in which this table lives.
    table : `sqlalchemy.schema.Table`
        SQLAlchemy representation of the table that stores attributes.
    registry_schema_version : `VersionTuple` or `None`, optional
        The version of the registry schema.
    """

    def __init__(
        self,
        db: Database,
        table: sqlalchemy.schema.Table,
        registry_schema_version: VersionTuple | None = None,
    ):
        super().__init__(registry_schema_version=registry_schema_version)
        self._db = db
        self._table = table

    _TABLE_NAME: ClassVar[str] = "butler_attributes"

    _TABLE_SPEC: ClassVar[TableSpec] = TableSpec(
        fields=[
            FieldSpec("name", dtype=sqlalchemy.String, length=1024, primaryKey=True),
            FieldSpec("value", dtype=sqlalchemy.String, length=65535, nullable=False),
        ],
    )

    def clone(self, db: Database) -> DefaultButlerAttributeManager:
        # Docstring inherited from ButlerAttributeManager.
        return DefaultButlerAttributeManager(db, self._table, self._registry_schema_version)

    @classmethod
    def initialize(
        cls, db: Database, context: StaticTablesContext, registry_schema_version: VersionTuple | None = None
    ) -> ButlerAttributeManager:
        # Docstring inherited from ButlerAttributeManager.
        table = context.addTable(cls._TABLE_NAME, cls._TABLE_SPEC)
        return cls(db=db, table=table, registry_schema_version=registry_schema_version)

    def get(self, name: str, default: str | None = None) -> str | None:
        # Docstring inherited from ButlerAttributeManager.
        sql = sqlalchemy.sql.select(self._table.columns.value).where(self._table.columns.name == name)
        with self._db.query(sql) as sql_result:
            row = sql_result.fetchone()
        if row is not None:
            return row[0]
        return default

    def set(self, name: str, value: str, *, force: bool = False) -> None:
        # Docstring inherited from ButlerAttributeManager.
        if not name or not value:
            raise ValueError("name and value cannot be empty")
        if force:
            self._db.replace(
                self._table,
                {
                    "name": name,
                    "value": value,
                },
            )
        else:
            try:
                self._db.insert(
                    self._table,
                    {
                        "name": name,
                        "value": value,
                    },
                )
            except sqlalchemy.exc.IntegrityError as exc:
                raise ButlerAttributeExistsError(f"attribute {name} already exists") from exc

    def delete(self, name: str) -> bool:
        # Docstring inherited from ButlerAttributeManager.
        numRows = self._db.delete(self._table, ["name"], {"name": name})
        return numRows > 0

    def items(self) -> Iterable[tuple[str, str]]:
        # Docstring inherited from ButlerAttributeManager.
        sql = sqlalchemy.sql.select(
            self._table.columns.name,
            self._table.columns.value,
        )
        with self._db.query(sql) as sql_result:
            sql_rows = sql_result.fetchall()
        for row in sql_rows:
            yield row[0], row[1]

    def empty(self) -> bool:
        # Docstring inherited from ButlerAttributeManager.
        sql = sqlalchemy.sql.select(sqlalchemy.sql.func.count()).select_from(self._table)
        with self._db.query(sql) as sql_result:
            count = sql_result.scalar()
        return count == 0

    @classmethod
    def currentVersions(cls) -> list[VersionTuple]:
        # Docstring inherited from VersionedExtension.
        return [_VERSION]

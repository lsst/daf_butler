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
"""The default concrete implementation of the class that manages
attributes for `Registry`.
"""

__all__ = ["DefaultButlerAttributeManager"]

from typing import (
    ClassVar,
    Iterable,
    Optional,
    Tuple,
)

import sqlalchemy

from ..core.ddl import TableSpec, FieldSpec
from .interfaces import (
    Database,
    ButlerAttributeExistsError,
    ButlerAttributeManager,
    StaticTablesContext,
    VersionTuple
)


class MissingAttributesTableError(RuntimeError):
    """Exception raised when a database is missing attributes table.
    """
    pass


# This manager is supposed to have super-stable schema that never changes
# but there may be cases when we need data migration on this table so we
# keep version for it as well.
_VERSION = VersionTuple(1, 0, 0)


class DefaultButlerAttributeManager(ButlerAttributeManager):
    """An implementation of `ButlerAttributeManager` that stores attributes
    in a database table.

    Parameters
    ----------
    db : `Database`
        Database engine interface for the namespace in which this table lives.
    table : `sqlalchemy.schema.Table`
        SQLAlchemy representation of the table that stores attributes.
    """
    def __init__(self, db: Database, table: sqlalchemy.schema.Table):
        self._db = db
        self._table = table

    _TABLE_NAME: ClassVar[str] = "butler_attributes"

    _TABLE_SPEC: ClassVar[TableSpec] = TableSpec(
        fields=[
            FieldSpec("name", dtype=sqlalchemy.String, length=1024, primaryKey=True),
            FieldSpec("value", dtype=sqlalchemy.String, length=65535, nullable=False),
        ],
    )

    @classmethod
    def initialize(cls, db: Database, context: StaticTablesContext) -> ButlerAttributeManager:
        # Docstring inherited from ButlerAttributeManager.
        table = context.addTable(cls._TABLE_NAME, cls._TABLE_SPEC)
        return cls(db=db, table=table)

    def _checkTableExists(self) -> None:
        """Check that attributes table exists or raise an exception.

        This should not be called on every read operation but instead only
        when an exception is raised from SELECT query.
        """
        # Database metadata is likely not populated at this point yet, so we
        # have to use low-level connection object to check it.
        # TODO: Once we have stable gen3 schema everywhere this test can be
        # dropped (DM-27373).
        if not self._table.exists(bind=self._db._connection):
            raise MissingAttributesTableError(
                f"`{self._table.name}` table is missing from schema, schema has to"
                " be initialized before use (database is probably outdated)."
            ) from None

    def get(self, name: str, default: Optional[str] = None) -> Optional[str]:
        # Docstring inherited from ButlerAttributeManager.
        try:
            sql = sqlalchemy.sql.select([self._table.columns.value]).where(
                self._table.columns.name == name
            )
            row = self._db.query(sql).fetchone()
            if row is not None:
                return row[0]
            return default
        except sqlalchemy.exc.OperationalError:
            # if this is due to missing table raise different exception
            self._checkTableExists()
            raise

    def set(self, name: str, value: str, *, force: bool = False) -> None:
        # Docstring inherited from ButlerAttributeManager.
        if not name or not value:
            raise ValueError("name and value cannot be empty")
        if force:
            self._db.replace(self._table, {
                "name": name,
                "value": value,
            })
        else:
            try:
                self._db.insert(self._table, {
                    "name": name,
                    "value": value,
                })
            except sqlalchemy.exc.IntegrityError as exc:
                raise ButlerAttributeExistsError(f"attribute {name} already exists") from exc

    def delete(self, name: str) -> bool:
        # Docstring inherited from ButlerAttributeManager.
        numRows = self._db.delete(self._table, ["name"], {"name": name})
        return numRows > 0

    def items(self) -> Iterable[Tuple[str, str]]:
        # Docstring inherited from ButlerAttributeManager.
        sql = sqlalchemy.sql.select([
            self._table.columns.name,
            self._table.columns.value,
        ])
        for row in self._db.query(sql):
            yield row[0], row[1]

    def empty(self) -> bool:
        # Docstring inherited from ButlerAttributeManager.
        try:
            sql = sqlalchemy.sql.select([sqlalchemy.sql.func.count()]).select_from(self._table)
            row = self._db.query(sql).fetchone()
            return row[0] == 0
        except sqlalchemy.exc.OperationalError:
            # if this is due to missing table raise different exception
            self._checkTableExists()
            raise

    @classmethod
    def currentVersion(cls) -> Optional[VersionTuple]:
        # Docstring inherited from VersionedExtension.
        return _VERSION

    def schemaDigest(self) -> Optional[str]:
        # Docstring inherited from VersionedExtension.
        return self._defaultSchemaDigest([self._table], self._db.dialect)

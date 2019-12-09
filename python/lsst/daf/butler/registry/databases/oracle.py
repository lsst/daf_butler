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

__all__ = ["OracleDatabase"]

import copy
from typing import Optional

import sqlalchemy

from ..interfaces import Database, ReadOnlyDatabaseError
from .. import ddl
from ..nameShrinker import NameShrinker


class _Merge(sqlalchemy.sql.expression.Executable, sqlalchemy.sql.ClauseElement):
    """A SQLAlchemy query that compiles to a MERGE invocation that is the
    equivalent of PostgreSQL and SQLite's INSERT ... ON CONFLICT REPLACE on the
    primary key constraint for the table.
    """

    def __init__(self, table):
        super().__init__()
        self.table = table


@sqlalchemy.ext.compiler.compiles(_Merge, "oracle")
def _merge(merge, compiler, **kw):
    """Generate MERGE query for inserting or updating records.
    """
    table = merge.table
    preparer = compiler.preparer

    allColumns = [col.name for col in table.columns]
    pkColumns = [col.name for col in table.primary_key]
    nonPkColumns = [col for col in allColumns if col not in pkColumns]

    # To properly support type decorators defined in core/schema.py we need
    # to pass column type to `bindparam`.
    selectColumns = [sqlalchemy.sql.bindparam(col.name, type_=col.type).label(col.name)
                     for col in table.columns]
    selectClause = sqlalchemy.sql.select(selectColumns)

    tableAlias = table.alias("t")
    tableAliasText = compiler.process(tableAlias, asfrom=True, **kw)
    selectAlias = selectClause.alias("d")
    selectAliasText = compiler.process(selectAlias, asfrom=True, **kw)

    condition = sqlalchemy.sql.and_(
        *[tableAlias.columns[col] == selectAlias.columns[col] for col in pkColumns]
    )
    conditionText = compiler.process(condition, **kw)

    query = f"MERGE INTO {tableAliasText}" \
            f"\nUSING {selectAliasText}" \
            f"\nON ({conditionText})"
    updates = []
    for col in nonPkColumns:
        src = compiler.process(selectAlias.columns[col], **kw)
        dst = compiler.process(tableAlias.columns[col], **kw)
        updates.append(f"{dst} = {src}")
    updates = ", ".join(updates)
    query += f"\nWHEN MATCHED THEN UPDATE SET {updates}"

    insertColumns = ", ".join([preparer.format_column(col) for col in table.columns])
    insertValues = ", ".join([compiler.process(selectAlias.columns[col], **kw) for col in allColumns])

    query += f"\nWHEN NOT MATCHED THEN INSERT ({insertColumns}) VALUES ({insertValues})"
    return query


class OracleDatabase(Database):
    """An implementation of the `Database` interface for Oracle.

    Parameters
    ----------
    origin : `int`
        An integer ID that should be used as the default for any datasets,
        quanta, or other entities that use a (autoincrement, origin) compound
        primary key.
    uri : `str`, optional
        Database connection string.  If not provided, ``engine`` must be.
    engine : `sqlalchemy.engine.Engine`, optional
        SQLAlchemy engine instance to use directly.
    namespace : `str`, optional
        Namespace (schema) for all tables used by this database.  May be,
        `None` which will use the schema implied by ``uri`` or ``engine``
        (which may be `None` for no schema).
    prefix : `str`, optional
        Prefix to add to all table names, effectively defining a virtual
        schema that can coexist with others within the same actual database
        schema.  This prefix must not be used in the un-prefixed names of
        tables.
    writeable : `bool`, optional
        If `True` (default) allow writes to the database.
    """

    def __init__(self, *, origin: int,
                 uri: Optional[str] = None,
                 engine: Optional[sqlalchemy.engine.Engine] = None,
                 namespace: Optional[str] = None,
                 prefix: Optional[str] = None,
                 writeable: bool = True):
        if engine is None:
            engine = sqlalchemy.engine.create_engine(uri, pool_size=1)
        connection = engine.connect()
        # Work around SQLAlchemy assuming that the Oracle limit on identifier
        # names is even shorter than it is after 12.2.
        oracle_ver = engine.dialect._get_server_version_info(connection)
        if oracle_ver < (12, 2):
            raise RuntimeError("Oracle server version >= 12.2 required.")
        engine.dialect.max_identifier_length = 128
        # Get the schema that was included/implicit in the URI we used to
        # connect.
        dbapi = engine.raw_connection()
        if namespace is None:
            namespace = dbapi.current_schema
        else:
            if dbapi.current_schema != namespace:
                # TODO: could we instead add the schema to the URI before
                # trying to connect?  Or is an Oracle connection without a
                # schema not something that can exist?
                raise RuntimeError("'namespace' and 'uri' arguments specify different schemas.")
        # TODO: is there anything we can do to make the connection read-only if
        # writeable is False?  If not (and at present) we just rely on the
        # Python code being well-behaved and not *trying* to make changes.
        super().__init__(origin=origin, engine=engine, namespace=namespace, connection=connection)
        self._writeable = writeable
        self.dsn = dbapi.dsn
        self.prefix = prefix
        self._shrinker = NameShrinker(engine.dialect.max_identifier_length)

    def isWriteable(self) -> bool:
        return self._writeable

    def __str__(self) -> str:
        if self.namespace is None:
            name = self.dsn
        else:
            name = f"{self.dsn:self.namespace}"
        return f"Oracle@{name}"

    def shrinkDatabaseEntityName(self, original: str) -> str:
        return self._shrinker.shrink(original)

    def expandDatabaseEntityName(self, shrunk: str) -> str:
        return self._shrinker.expand(shrunk)

    def _convertForeignKeySpec(self, table: str, spec: ddl.ForeignKeySpec, metadata: sqlalchemy.MetaData,
                               **kwds) -> sqlalchemy.schema.ForeignKeyConstraint:
        if self.prefix is not None:
            spec = copy.copy(spec)
            spec.table = self.prefix + spec.table
        return super()._convertForeignKeySpec(table, spec, metadata, **kwds)

    def _convertTableSpec(self, name: str, spec: ddl.TableSpec, metadata: sqlalchemy.MetaData,
                          **kwds) -> sqlalchemy.schema.Table:
        if self.prefix is not None and not name.startswith(self.prefix):
            name = self.prefix + name
        return super()._convertTableSpec(name, spec, metadata, **kwds)

    def getExistingTable(self, name: str, spec: ddl.TableSpec) -> Optional[sqlalchemy.schema.Table]:
        if self.prefix is not None and not name.startswith(self.prefix):
            name = self.prefix + name
        return super().getExistingTable(name, spec)

    def replace(self, table: sqlalchemy.schema.Table, *rows: dict):
        if not self.isWriteable():
            raise ReadOnlyDatabaseError(f"Attempt to replace into read-only database '{self}'.")
        self._connection.execute(_Merge(table), *rows)

    prefix: Optional[str]
    """A prefix included in all table names to simulate a database namespace
    (`str` or `None`).
    """

    dsn: str
    """The TNS entry of the database this instance is connected to (`str`).
    """

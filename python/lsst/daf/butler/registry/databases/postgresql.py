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

from sqlalchemy.sql.expression import ColumnElement as ColumnElement

from ... import ddl, time_utils

__all__ = ["PostgresqlDatabase"]

import re
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import closing, contextmanager
from typing import Any

import psycopg2
import sqlalchemy
import sqlalchemy.dialects.postgresql
from sqlalchemy import sql

from ..._named import NamedValueAbstractSet
from ..._timespan import Timespan
from ...timespan_database_representation import TimespanDatabaseRepresentation
from ..interfaces import Database, DatabaseMetadata


class PostgresqlDatabase(Database):
    """An implementation of the `Database` interface for PostgreSQL.

    Parameters
    ----------
    engine : `sqlalchemy.engine.Engine`
        Engine to use for this connection.
    origin : `int`
        An integer ID that should be used as the default for any datasets,
        quanta, or other entities that use a (autoincrement, origin) compound
        primary key.
    namespace : `str`, optional
        The namespace (schema) this database is associated with.  If `None`,
        the default schema for the connection is used (which may be `None`).
    writeable : `bool`, optional
        If `True`, allow write operations on the database, including
        ``CREATE TABLE``.

    Notes
    -----
    This currently requires the psycopg2 driver to be used as the backend for
    SQLAlchemy.  Running the tests for this class requires the
    ``testing.postgresql`` be installed, which we assume indicates that a
    PostgreSQL server is installed and can be run locally in userspace.

    Some functionality provided by this class (and used by `Registry`) requires
    the ``btree_gist`` PostgreSQL server extension to be installed an enabled
    on the database being connected to; this is checked at connection time.
    """

    def __init__(
        self,
        *,
        engine: sqlalchemy.engine.Engine,
        origin: int,
        namespace: str | None = None,
        writeable: bool = True,
    ):
        with engine.connect() as connection:
            # `Any` to make mypy ignore the line below, can't use type: ignore
            dbapi: Any = connection.connection
            try:
                dsn = dbapi.get_dsn_parameters()
            except (AttributeError, KeyError) as err:
                raise RuntimeError("Only the psycopg2 driver for PostgreSQL is supported.") from err
            if namespace is None:
                query = sql.select(sql.func.current_schema())
                namespace = connection.execute(query).scalar()
            query_text = "SELECT COUNT(*) FROM pg_extension WHERE extname='btree_gist';"
            if not connection.execute(sqlalchemy.text(query_text)).scalar():
                raise RuntimeError(
                    "The Butler PostgreSQL backend requires the btree_gist extension. "
                    "As extensions are enabled per-database, this may require an administrator to run "
                    "`CREATE EXTENSION btree_gist;` in a database before a butler client for it is "
                    " initialized."
                )

            pg_version = get_postgres_server_version(connection)
        self._init(
            engine=engine,
            origin=origin,
            namespace=namespace,
            writeable=writeable,
            dbname=dsn.get("dbname"),
            metadata=None,
            pg_version=pg_version,
        )

    def _init(
        self,
        *,
        engine: sqlalchemy.engine.Engine,
        origin: int,
        namespace: str | None = None,
        writeable: bool = True,
        dbname: str,
        metadata: DatabaseMetadata | None,
        pg_version: tuple[int, int],
    ) -> None:
        # Initialization logic shared between ``__init__`` and ``clone``.
        super().__init__(origin=origin, engine=engine, namespace=namespace, metadata=metadata)
        self._writeable = writeable
        self.dbname = dbname
        self._pg_version = pg_version

    def clone(self) -> PostgresqlDatabase:
        clone = self.__new__(type(self))
        clone._init(
            origin=self.origin,
            engine=self._engine,
            namespace=self.namespace,
            writeable=self._writeable,
            dbname=self.dbname,
            metadata=self._metadata,
            pg_version=self._pg_version,
        )
        return clone

    @classmethod
    def makeEngine(
        cls, uri: str | sqlalchemy.engine.URL, *, writeable: bool = True
    ) -> sqlalchemy.engine.Engine:
        return sqlalchemy.engine.create_engine(
            uri,
            # Prevent stale database connections from throwing exeptions, at
            # the expense of a round trip to the database server each time we
            # check out a session.  Many services using the Butler operate in
            # networks where connections are dropped when idle for some time.
            pool_pre_ping=True,
            # This engine and database connection pool can be shared between
            # multiple Butler instances created via Butler.clone() or
            # LabeledButlerFactory, and typically these will be used from
            # multiple threads simultaneously.  So we need to configure
            # SQLAlchemy to pool connections for multi-threaded usage.
            #
            # This pool size was chosen to work well for services using
            # FastAPI.  FastAPI uses a thread pool of 40 by default, so this
            # gives us a connection for each thread in the pool. Because Butler
            # is currently sync-only, we won't ever be executing more queries
            # than we have threads.
            #
            # Connections are only created as they are needed, so in typical
            # single-threaded Butler use only one connection will ever be
            # created. Services with low peak concurrency may never create this
            # many connections.
            #
            # The main Butler databases at SLAC are run behind pgbouncer, so we
            # can support a larger number of simultaneous connections than if
            # we were connecting directly to Postgres.
            #
            # See
            # https://docs.sqlalchemy.org/en/20/core/pooling.html#sqlalchemy.pool.QueuePool.__init__
            # for more information on the behavior of this parameter.
            pool_size=40,
            # If we are experiencing heavy enough load that we overflow the
            # connection pool, it will be harmful to start creating extra
            # connections that we disconnect immediately after use.
            # Connecting from scratch is fairly expensive, which is why we have
            # a pool in the first place.
            max_overflow=0,
            # If the pool is full, this is the maximum number of seconds we
            # will wait for a connection to become available before giving up.
            pool_timeout=60,
            # In combination with pool_pre_ping, prevent SQLAlchemy from
            # unnecessarily reviving pooled connections that have gone stale.
            # Setting this to true makes it always re-use the most recent
            # known-good connection when possible, instead of cycling to other
            # connections in the pool that we may no longer need.
            pool_use_lifo=True,
        )

    @classmethod
    def fromEngine(
        cls,
        engine: sqlalchemy.engine.Engine,
        *,
        origin: int,
        namespace: str | None = None,
        writeable: bool = True,
    ) -> Database:
        return cls(engine=engine, origin=origin, namespace=namespace, writeable=writeable)

    @contextmanager
    def _transaction(
        self,
        *,
        interrupting: bool = False,
        savepoint: bool = False,
        lock: Iterable[sqlalchemy.schema.Table] = (),
        for_temp_tables: bool = False,
    ) -> Iterator[tuple[bool, sqlalchemy.engine.Connection]]:
        with super()._transaction(interrupting=interrupting, savepoint=savepoint, lock=lock) as (
            is_new,
            connection,
        ):
            if is_new:
                # pgbouncer with transaction-level pooling (which we aim to
                # support) says that SET cannot be used, except for a list of
                # "Startup parameters" that includes "timezone" (see
                # https://www.pgbouncer.org/features.html#fnref:0).  But I
                # don't see "timezone" in PostgreSQL's list of parameters
                # passed when creating a new connection
                # (https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS).
                # Given that the pgbouncer docs say, "PgBouncer detects their
                # changes and so it can guarantee they remain consistent for
                # the client", I assume we can use "SET TIMESPAN" and pgbouncer
                # will take care of clients that share connections being set
                # consistently.  And if that assumption is wrong, we should
                # still probably be okay, since all clients should be Butler
                # clients, and they'll all be setting the same thing.
                #
                # The "SET TRANSACTION READ ONLY" should also be safe, because
                # it only ever acts on the current transaction; I think it's
                # not included in pgbouncer's declaration that SET is
                # incompatible with transaction-level pooling because
                # PostgreSQL actually considers SET TRANSACTION to be a
                # fundamentally different statement from SET (they have their
                # own distinct doc pages, at least).
                with closing(connection.connection.cursor()) as cursor:
                    # PostgreSQL permits writing to temporary tables inside
                    # read-only transactions, but it doesn't permit creating
                    # them.
                    if not (self.isWriteable() or for_temp_tables):
                        cursor.execute("SET TRANSACTION READ ONLY")
                    # Make timestamps UTC, because we didn't use TIMESTAMPZ
                    # for the column type.  When we can tolerate a schema
                    # change, we should change that type and remove this
                    # line.
                    cursor.execute("SET TIME ZONE 0")
                    # Using server-side cursors with complex queries frequently
                    # generates suboptimal query plan, setting
                    # cursor_tuple_fraction=1 helps for those cases.
                    cursor.execute("SET cursor_tuple_fraction = 1")
            yield is_new, connection

    @contextmanager
    def temporary_table(
        self, spec: ddl.TableSpec, name: str | None = None
    ) -> Iterator[sqlalchemy.schema.Table]:
        # Docstring inherited.
        with self.transaction(for_temp_tables=True), super().temporary_table(spec, name) as table:
            yield table

    def _lockTables(
        self, connection: sqlalchemy.engine.Connection, tables: Iterable[sqlalchemy.schema.Table] = ()
    ) -> None:
        # Docstring inherited.
        for table in tables:
            connection.execute(sqlalchemy.text(f"LOCK TABLE {table.key} IN EXCLUSIVE MODE"))

    def isWriteable(self) -> bool:
        return self._writeable

    def __str__(self) -> str:
        return f"PostgreSQL@{self.dbname}:{self.namespace}"

    def shrinkDatabaseEntityName(self, original: str) -> str:
        return self.name_shrinker.shrink(original)

    def expandDatabaseEntityName(self, shrunk: str) -> str:
        return self.name_shrinker.expand(shrunk)

    def _convertExclusionConstraintSpec(
        self,
        table: str,
        spec: tuple[str | type[TimespanDatabaseRepresentation], ...],
        metadata: sqlalchemy.MetaData,
    ) -> sqlalchemy.schema.Constraint:
        # Docstring inherited.
        args: list[tuple[sqlalchemy.schema.Column, str]] = []
        names = ["excl"]
        for item in spec:
            if isinstance(item, str):
                args.append((sqlalchemy.schema.Column(item), "="))
                names.append(item)
            elif issubclass(item, TimespanDatabaseRepresentation):
                assert item is self.getTimespanRepresentation()
                args.append((sqlalchemy.schema.Column(TimespanDatabaseRepresentation.NAME), "&&"))
                names.append(TimespanDatabaseRepresentation.NAME)
        return sqlalchemy.dialects.postgresql.ExcludeConstraint(
            *args,
            name=self.shrinkDatabaseEntityName("_".join(names)),
        )

    def _make_temporary_table(
        self,
        connection: sqlalchemy.engine.Connection,
        spec: ddl.TableSpec,
        name: str | None = None,
        **kwargs: Any,
    ) -> sqlalchemy.schema.Table:
        # Docstring inherited
        # Adding ON COMMIT DROP here is really quite defensive: we already
        # manually drop the table at the end of the temporary_table context
        # manager, and that will usually happen first.  But this will guarantee
        # that we drop the table at the end of the transaction even if the
        # connection lasts longer, and that's good citizenship when connections
        # may be multiplexed by e.g. pgbouncer.
        return super()._make_temporary_table(connection, spec, name, postgresql_on_commit="DROP", **kwargs)

    @classmethod
    def getTimespanRepresentation(cls) -> type[TimespanDatabaseRepresentation]:
        # Docstring inherited.
        return _RangeTimespanRepresentation

    def replace(self, table: sqlalchemy.schema.Table, *rows: dict) -> None:
        self.assertTableWriteable(table, f"Cannot replace into read-only table {table}.")
        if not rows:
            return
        # This uses special support for UPSERT in PostgreSQL backend:
        # https://docs.sqlalchemy.org/en/13/dialects/postgresql.html#insert-on-conflict-upsert
        query = sqlalchemy.dialects.postgresql.dml.insert(table)
        # In the SET clause assign all columns using special `excluded`
        # pseudo-table.  If some column in the table does not appear in the
        # INSERT list this will set it to NULL.
        excluded = query.excluded
        data = {
            column.name: getattr(excluded, column.name)
            for column in table.columns
            if column.name not in table.primary_key
        }
        if not data:
            self.ensure(table, *rows)
            return
        query = query.on_conflict_do_update(constraint=table.primary_key, set_=data)
        with self._transaction() as (_, connection):
            connection.execute(query, rows)

    def ensure(self, table: sqlalchemy.schema.Table, *rows: dict, primary_key_only: bool = False) -> int:
        # Docstring inherited.
        self.assertTableWriteable(table, f"Cannot ensure into read-only table {table}.")
        if not rows:
            return 0
        # Like `replace`, this uses UPSERT.
        base_insert = sqlalchemy.dialects.postgresql.dml.insert(table)
        if primary_key_only:
            query = base_insert.on_conflict_do_nothing(constraint=table.primary_key)
        else:
            query = base_insert.on_conflict_do_nothing()
        with self._transaction() as (_, connection):
            return connection.execute(query, rows).rowcount

    def constant_rows(
        self,
        fields: NamedValueAbstractSet[ddl.FieldSpec],
        *rows: dict,
        name: str | None = None,
    ) -> sqlalchemy.sql.FromClause:
        # Docstring inherited.
        return super().constant_rows(fields, *rows, name=name)

    @property
    def has_distinct_on(self) -> bool:
        # Docstring inherited.
        return True

    @property
    def has_any_aggregate(self) -> bool:
        # Docstring inherited.
        return self._pg_version >= (16, 0)

    def apply_any_aggregate(self, column: sqlalchemy.ColumnElement[Any]) -> sqlalchemy.ColumnElement[Any]:
        # Docstring inherited

        # The cast is required to prevent sqlalchemy from forgetting the type
        # of the initial column. Without the cast, for example, Base64Region
        # would become a String column in the output.
        return sqlalchemy.cast(sqlalchemy.func.any_value(column), column.type)


class _RangeTimespanType(sqlalchemy.TypeDecorator):
    """A single-column `Timespan` representation usable only with
    PostgreSQL.

    This type should be able to take advantage of PostgreSQL's built-in
    range operators, and the indexing and EXCLUSION table constraints built
    off of them.
    """

    impl = sqlalchemy.dialects.postgresql.INT8RANGE

    cache_ok = True

    def process_bind_param(
        self, value: Timespan | None, dialect: sqlalchemy.engine.Dialect
    ) -> psycopg2.extras.NumericRange | None:
        if value is None:
            return None
        if not isinstance(value, Timespan):
            raise TypeError(f"Unsupported type: {type(value)}, expected Timespan.")
        if value.isEmpty():
            return psycopg2.extras.NumericRange(empty=True)
        else:
            converter = time_utils.TimeConverter()
            assert value.nsec[0] >= converter.min_nsec, "Guaranteed by Timespan.__init__."
            assert value.nsec[1] <= converter.max_nsec, "Guaranteed by Timespan.__init__."
            lower = None if value.nsec[0] == converter.min_nsec else value.nsec[0]
            upper = None if value.nsec[1] == converter.max_nsec else value.nsec[1]
            return psycopg2.extras.NumericRange(lower=lower, upper=upper)

    def process_result_value(
        self, value: psycopg2.extras.NumericRange | None, dialect: sqlalchemy.engine.Dialect
    ) -> Timespan | None:
        if value is None:
            return None
        if value.isempty:
            return Timespan.makeEmpty()
        converter = time_utils.TimeConverter()
        begin_nsec = converter.min_nsec if value.lower is None else value.lower
        end_nsec = converter.max_nsec if value.upper is None else value.upper
        return Timespan(begin=None, end=None, _nsec=(begin_nsec, end_nsec))


class _RangeTimespanRepresentation(TimespanDatabaseRepresentation):
    """An implementation of `TimespanDatabaseRepresentation` that uses
    `_RangeTimespanType` to store a timespan in a single
    PostgreSQL-specific field.

    Parameters
    ----------
    column : `sqlalchemy.sql.ColumnElement`
        SQLAlchemy object representing the column.
    """

    def __init__(self, column: sqlalchemy.sql.ColumnElement, name: str):
        self.column = column
        self._name = name

    __slots__ = ("column", "_name")

    @classmethod
    def makeFieldSpecs(
        cls, nullable: bool, name: str | None = None, **kwargs: Any
    ) -> tuple[ddl.FieldSpec, ...]:
        # Docstring inherited.
        if name is None:
            name = cls.NAME
        return (
            ddl.FieldSpec(
                name,
                dtype=_RangeTimespanType,
                nullable=nullable,
                default=(None if nullable else sqlalchemy.sql.text("'(,)'::int8range")),
                **kwargs,
            ),
        )

    @classmethod
    def getFieldNames(cls, name: str | None = None) -> tuple[str, ...]:
        # Docstring inherited.
        if name is None:
            name = cls.NAME
        return (name,)

    @classmethod
    def update(
        cls, extent: Timespan | None, name: str | None = None, result: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        # Docstring inherited.
        if name is None:
            name = cls.NAME
        if result is None:
            result = {}
        result[name] = extent
        return result

    @classmethod
    def extract(cls, mapping: Mapping[str, Any], name: str | None = None) -> Timespan | None:
        # Docstring inherited.
        if name is None:
            name = cls.NAME
        return mapping[name]

    @classmethod
    def fromLiteral(cls, timespan: Timespan | None) -> _RangeTimespanRepresentation:
        # Docstring inherited.
        if timespan is None:
            # Cast NULL to an expected type, helps Postgres to figure out
            # column type when doing UNION.
            return cls(
                column=sqlalchemy.func.cast(sqlalchemy.sql.null(), type_=_RangeTimespanType), name=cls.NAME
            )
        return cls(
            column=sqlalchemy.sql.cast(
                sqlalchemy.sql.literal(timespan, type_=_RangeTimespanType), type_=_RangeTimespanType
            ),
            name=cls.NAME,
        )

    @classmethod
    def from_columns(
        cls, columns: sqlalchemy.sql.ColumnCollection, name: str | None = None
    ) -> _RangeTimespanRepresentation:
        # Docstring inherited.
        if name is None:
            name = cls.NAME
        return cls(columns[name], name)

    @property
    def name(self) -> str:
        # Docstring inherited.
        return self._name

    def isNull(self) -> sqlalchemy.sql.ColumnElement:
        # Docstring inherited.
        return self.column.is_(None)

    def isEmpty(self) -> sqlalchemy.sql.ColumnElement:
        # Docstring inherited
        return sqlalchemy.sql.func.isempty(self.column)

    def __lt__(
        self, other: _RangeTimespanRepresentation | sqlalchemy.sql.ColumnElement
    ) -> sqlalchemy.sql.ColumnElement:
        # Docstring inherited.
        if isinstance(other, sqlalchemy.sql.ColumnElement):
            return sqlalchemy.sql.and_(
                sqlalchemy.sql.not_(sqlalchemy.sql.func.upper_inf(self.column)),
                sqlalchemy.sql.not_(sqlalchemy.sql.func.isempty(self.column)),
                sqlalchemy.sql.func.upper(self.column) <= other,
            )
        else:
            return self.column << other.column

    def __gt__(
        self, other: _RangeTimespanRepresentation | sqlalchemy.sql.ColumnElement
    ) -> sqlalchemy.sql.ColumnElement:
        # Docstring inherited.
        if isinstance(other, sqlalchemy.sql.ColumnElement):
            return sqlalchemy.sql.and_(
                sqlalchemy.sql.not_(sqlalchemy.sql.func.lower_inf(self.column)),
                sqlalchemy.sql.not_(sqlalchemy.sql.func.isempty(self.column)),
                sqlalchemy.sql.func.lower(self.column) > other,
            )
        else:
            return self.column >> other.column

    def overlaps(
        self, other: _RangeTimespanRepresentation | sqlalchemy.sql.ColumnElement
    ) -> sqlalchemy.sql.ColumnElement:
        # Docstring inherited.
        if not isinstance(other, _RangeTimespanRepresentation):
            return self.contains(other)
        return self.column.overlaps(other.column)

    def contains(
        self, other: _RangeTimespanRepresentation | sqlalchemy.sql.ColumnElement
    ) -> sqlalchemy.sql.ColumnElement:
        # Docstring inherited
        if isinstance(other, _RangeTimespanRepresentation):
            return self.column.contains(other.column)
        else:
            return self.column.contains(other)

    def lower(self) -> sqlalchemy.sql.ColumnElement:
        # Docstring inherited.
        return sqlalchemy.sql.functions.coalesce(
            sqlalchemy.sql.func.lower(self.column), sqlalchemy.sql.literal(0)
        )

    def upper(self) -> sqlalchemy.sql.ColumnElement:
        # Docstring inherited.
        return sqlalchemy.sql.functions.coalesce(
            sqlalchemy.sql.func.upper(self.column), sqlalchemy.sql.literal(0)
        )

    def flatten(self, name: str | None = None) -> tuple[sqlalchemy.sql.ColumnElement]:
        # Docstring inherited.
        if name is None:
            return (self.column,)
        else:
            return (self.column.label(name),)

    def apply_any_aggregate(
        self, func: Callable[[ColumnElement[Any]], ColumnElement[Any]]
    ) -> TimespanDatabaseRepresentation:
        # Docstring inherited.
        return _RangeTimespanRepresentation(func(self.column), self.name)


def get_postgres_server_version(connection: sqlalchemy.Connection) -> tuple[int, int]:  # numpydoc ignore=PR01
    """Return the postgres DB server version as a tuple
    (major version, minor version).
    """
    raw_pg_version = connection.execute(sqlalchemy.text("SHOW server_version")).scalar()
    _SERVER_VERSION_REGEX = re.compile(r"(?P<major>\d+)\.(?P<minor>\d+)")
    if raw_pg_version is not None and (m := _SERVER_VERSION_REGEX.search(raw_pg_version)):
        return (int(m.group("major")), int(m.group("minor")))
    else:
        raise RuntimeError("Failed to get PostgreSQL server version.")

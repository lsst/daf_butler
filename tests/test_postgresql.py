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

import gc
import itertools
import os
import secrets
import unittest
import warnings
from contextlib import contextmanager

import astropy.time

try:
    # It's possible but silly to have testing.postgresql installed without
    # having the postgresql server installed (because then nothing in
    # testing.postgresql would work), so we use the presence of that module
    # to test whether we can expect the server to be available.
    import testing.postgresql
except ImportError:
    testing = None

import sqlalchemy
from lsst.daf.butler import Timespan, ddl
from lsst.daf.butler.registry import _RegistryFactory
from lsst.daf.butler.registry.sql_registry import SqlRegistry

try:
    from lsst.daf.butler.registry.databases.postgresql import PostgresqlDatabase, _RangeTimespanType
except ImportError:
    testing = None
from lsst.daf.butler.registry.tests import DatabaseTests, RegistryTests
from lsst.daf.butler.tests.utils import makeTestTempDir, removeTestTempDir

TESTDIR = os.path.abspath(os.path.dirname(__file__))


def _startServer(root):
    """Start a PostgreSQL server and create a database within it, returning
    an object encapsulating both.
    """
    server = testing.postgresql.Postgresql(base_dir=root)
    engine = sqlalchemy.engine.create_engine(server.url())
    with engine.begin() as connection:
        connection.execute(sqlalchemy.text("CREATE EXTENSION btree_gist;"))
    return server


@unittest.skipUnless(testing is not None, "testing.postgresql module not found")
class PostgresqlDatabaseTestCase(unittest.TestCase, DatabaseTests):
    """Test a postgres Registry."""

    @classmethod
    def setUpClass(cls):
        cls.root = makeTestTempDir(TESTDIR)
        cls.server = _startServer(cls.root)

    @classmethod
    def tearDownClass(cls):
        # Clean up any lingering SQLAlchemy engines/connections
        # so they're closed before we shut down the server.
        gc.collect()
        cls.server.stop()
        removeTestTempDir(cls.root)

    def makeEmptyDatabase(self, origin: int = 0) -> PostgresqlDatabase:
        namespace = f"namespace_{secrets.token_hex(8).lower()}"
        return PostgresqlDatabase.fromUri(origin=origin, uri=self.server.url(), namespace=namespace)

    def getNewConnection(self, database: PostgresqlDatabase, *, writeable: bool) -> PostgresqlDatabase:
        return PostgresqlDatabase.fromUri(
            origin=database.origin, uri=self.server.url(), namespace=database.namespace, writeable=writeable
        )

    @contextmanager
    def asReadOnly(self, database: PostgresqlDatabase) -> PostgresqlDatabase:
        yield self.getNewConnection(database, writeable=False)

    def testNameShrinking(self):
        """Test that too-long names for database entities other than tables
        and columns (which we preserve, and just expect to fit) are shrunk.
        """
        db = self.makeEmptyDatabase(origin=1)
        with db.declareStaticTables(create=True) as context:
            # Table and field names are each below the 63-char limit even when
            # accounting for the prefix, but their combination (which will
            # appear in sequences and constraints) is not.
            tableName = "a_table_with_a_very_very_long_42_char_name"
            fieldName1 = "a_column_with_a_very_very_long_43_char_name"
            fieldName2 = "another_column_with_a_very_very_long_49_char_name"
            context.addTable(
                tableName,
                ddl.TableSpec(
                    fields=[
                        ddl.FieldSpec(
                            fieldName1, dtype=sqlalchemy.BigInteger, autoincrement=True, primaryKey=True
                        ),
                        ddl.FieldSpec(
                            fieldName2,
                            dtype=sqlalchemy.String,
                            length=16,
                            nullable=False,
                        ),
                    ],
                    unique={(fieldName2,)},
                ),
            )
        # Add another table, this time dynamically, with a foreign key to the
        # first table.
        db.ensureTableExists(
            tableName + "_b",
            ddl.TableSpec(
                fields=[
                    ddl.FieldSpec(
                        fieldName1 + "_b", dtype=sqlalchemy.BigInteger, autoincrement=True, primaryKey=True
                    ),
                    ddl.FieldSpec(
                        fieldName2 + "_b",
                        dtype=sqlalchemy.String,
                        length=16,
                        nullable=False,
                    ),
                ],
                foreignKeys=[
                    ddl.ForeignKeySpec(tableName, source=(fieldName2 + "_b",), target=(fieldName2,)),
                ],
            ),
        )

    def test_RangeTimespanType(self):
        start = astropy.time.Time("2020-01-01T00:00:00", format="isot", scale="tai")
        offset = astropy.time.TimeDelta(60, format="sec")
        timestamps = [start + offset * n for n in range(3)]
        timespans = [Timespan(begin=None, end=None)]
        timespans.extend(Timespan(begin=None, end=t) for t in timestamps)
        timespans.extend(Timespan(begin=t, end=None) for t in timestamps)
        timespans.extend(Timespan(begin=a, end=b) for a, b in itertools.combinations(timestamps, 2))
        db = self.makeEmptyDatabase(origin=1)
        with db.declareStaticTables(create=True) as context:
            tbl = context.addTable(
                "tbl",
                ddl.TableSpec(
                    fields=[
                        ddl.FieldSpec(name="id", dtype=sqlalchemy.Integer, primaryKey=True),
                        ddl.FieldSpec(name="timespan", dtype=_RangeTimespanType),
                    ],
                ),
            )
        rows = [{"id": n, "timespan": t} for n, t in enumerate(timespans)]
        db.insert(tbl, *rows)

        # Test basic round-trip through database.
        with db.query(tbl.select().order_by(tbl.columns.id)) as sql_result:
            self.assertEqual(rows, [row._asdict() for row in sql_result])

        # Test that Timespan's Python methods are consistent with our usage of
        # half-open ranges and PostgreSQL operators on ranges.
        def subquery(alias: str) -> sqlalchemy.sql.FromClause:
            return (
                sqlalchemy.sql.select(tbl.columns.id.label("id"), tbl.columns.timespan.label("timespan"))
                .select_from(tbl)
                .alias(alias)
            )

        sq1 = subquery("sq1")
        sq2 = subquery("sq2")
        query = sqlalchemy.sql.select(
            sq1.columns.id.label("n1"),
            sq2.columns.id.label("n2"),
            sq1.columns.timespan.overlaps(sq2.columns.timespan).label("overlaps"),
        )

        # `columns` is deprecated since 1.4, but
        # `selected_columns` method did not exist in 1.3.
        if hasattr(query, "selected_columns"):
            columns = query.selected_columns
        else:
            columns = query.columns

        # SQLAlchemy issues a warning about cartesian product of two tables,
        # which we do intentionally. Disable that warning temporarily.
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore", message=".*cartesian product", category=sqlalchemy.exc.SAWarning
            )
            with db.query(query) as sql_result:
                dbResults = {
                    (row[columns.n1], row[columns.n2]): row[columns.overlaps] for row in sql_result.mappings()
                }

        pyResults = {
            (n1, n2): t1.overlaps(t2)
            for (n1, t1), (n2, t2) in itertools.product(enumerate(timespans), enumerate(timespans))
        }
        self.assertEqual(pyResults, dbResults)


@unittest.skipUnless(testing is not None, "testing.postgresql module not found")
class PostgresqlRegistryTests(RegistryTests):
    """Tests for `Registry` backed by a PostgreSQL database.

    Notes
    -----
    This is not a subclass of `unittest.TestCase` but to avoid repetition it
    defines methods that override `unittest.TestCase` methods. To make this
    work subclasses have to have this class first in the bases list.
    """

    @classmethod
    def setUpClass(cls):
        cls.root = makeTestTempDir(TESTDIR)
        cls.server = _startServer(cls.root)

    @classmethod
    def tearDownClass(cls):
        # Clean up any lingering SQLAlchemy engines/connections
        # so they're closed before we shut down the server.
        gc.collect()
        cls.server.stop()
        removeTestTempDir(cls.root)

    @classmethod
    def getDataDir(cls) -> str:
        return os.path.normpath(os.path.join(os.path.dirname(__file__), "data", "registry"))

    def makeRegistry(self, share_repo_with: SqlRegistry | None = None) -> SqlRegistry:
        if share_repo_with is None:
            namespace = f"namespace_{secrets.token_hex(8).lower()}"
        else:
            namespace = share_repo_with._db.namespace
        config = self.makeRegistryConfig()
        config["db"] = self.server.url()
        config["namespace"] = namespace
        if share_repo_with is None:
            return _RegistryFactory(config).create_from_config()
        else:
            return _RegistryFactory(config).from_config()


class PostgresqlRegistryNameKeyCollMgrUUIDTestCase(PostgresqlRegistryTests, unittest.TestCase):
    """Tests for `Registry` backed by a PostgreSQL database.

    This test case uses NameKeyCollectionManager and
    ByDimensionsDatasetRecordStorageManagerUUID.
    """

    collectionsManager = "lsst.daf.butler.registry.collections.nameKey.NameKeyCollectionManager"
    datasetsManager = (
        "lsst.daf.butler.registry.datasets.byDimensions.ByDimensionsDatasetRecordStorageManagerUUID"
    )


class PostgresqlRegistrySynthIntKeyCollMgrUUIDTestCase(PostgresqlRegistryTests, unittest.TestCase):
    """Tests for `Registry` backed by a PostgreSQL database.

    This test case uses SynthIntKeyCollectionManager and
    ByDimensionsDatasetRecordStorageManagerUUID.
    """

    collectionsManager = "lsst.daf.butler.registry.collections.synthIntKey.SynthIntKeyCollectionManager"
    datasetsManager = (
        "lsst.daf.butler.registry.datasets.byDimensions.ByDimensionsDatasetRecordStorageManagerUUID"
    )


if __name__ == "__main__":
    unittest.main()

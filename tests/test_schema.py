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

import os
import unittest

from sqlalchemy import create_engine, MetaData

import lsst.utils
import lsst.utils.tests

from lsst.daf.butler.core.utils import iterable
from lsst.daf.butler.core.schema import SchemaConfig, Schema, Table, Column, SchemaBuilder
from sqlalchemy.sql.expression import TableClause

"""Tests for Schema.
"""


class SchemaTestCase(lsst.utils.tests.TestCase):
    """Tests for Schema.

    .. warning::
        This unittest does not verify the validaty of the schema description.
        It only checks that the generated tables match it.
    """

    def setUp(self):
        self.testDir = os.path.dirname(__file__)
        self.config = SchemaConfig()
        self.schema = Schema(self.config)
        self.engine = create_engine("sqlite:///:memory:")
        self.schema.metadata.create_all(self.engine)

    def testConstructor(self):
        """Independent check for `Schema` constructor.
        """
        schema = Schema()
        self.assertIsInstance(schema, Schema)

    def testSchemaCreation(self):
        """Check that the generated `Schema` tables match its description.
        """
        self.assertIsInstance(self.schema.metadata, MetaData)
        allTables = {}
        allTables.update(self.config["tables"])
        for tableName, tableDescription in allTables.items():
            if "sql" in tableDescription:
                self.assertView(tableName, tableDescription)
            else:
                self.assertTable(tableName, tableDescription)

    def assertView(self, tableName, tableDescription):
        """Check that a generated view matches its `tableDescription`.
        """
        table = self.schema.tables[tableName]
        self.assertIsInstance(table, TableClause)
        for columnDescription in tableDescription["columns"]:
            column = getattr(table.c, columnDescription["name"])
            self.assertIsInstance(column, Column)

    def assertTable(self, tableName, tableDescription):
        """Check that a generated table matches its `tableDescription`.
        """
        table = self.schema.tables[tableName]
        self.assertIsInstance(table, Table)
        for columnDescription in tableDescription["columns"]:
            self.assertColumn(table, columnDescription["name"], columnDescription)
        if "foreignKeys" in tableDescription:
            self.assertForeignKeyConstraints(table, tableDescription["foreignKeys"])

    def assertColumn(self, table, columnName, columnDescription):
        """Check that a generated column matches its `columnDescription`.
        """
        column = getattr(table.c, columnName)
        self.assertIsInstance(column, Column)
        self.assertEqual(column.primary_key, columnDescription.get("primary_key", False))
        self.assertEqual(column.nullable, columnDescription.get("nullable", True) and not column.primary_key)
        self.assertIsInstance(column.type, SchemaBuilder.VALID_COLUMN_TYPES[columnDescription["type"]])

    def assertForeignKeyConstraints(self, table, constraintsDescription):
        """Check that foreign-key constraints match the
        `constraintsDescription`.
        """
        # Gather all actual constraints on the current table.
        tableConstraints = {}
        for constraint in table.foreign_key_constraints:
            src = tuple(sorted(constraint.column_keys))
            tgt = tuple(sorted((e.target_fullname for e in constraint.elements)))
            tableConstraints[src] = tgt
        # Check that all constraints in the description are indeed applied.
        # Note that this is only a one-way check, the table may have more
        # constraints imposed by other means.
        for constraint in constraintsDescription:
            src = tuple(sorted(iterable(constraint["src"])))
            tgt = tuple(sorted(iterable(constraint["tgt"])))
            tgtTable, _ = tgt[0].split(".")
            if tgtTable in self.schema.views:
                continue
            self.assertIn(src, tableConstraints)
            self.assertEqual(tableConstraints[src], tgt)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()

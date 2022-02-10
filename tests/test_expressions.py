# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import unittest

from lsst.daf.butler import DataCoordinate, DimensionUniverse
from lsst.daf.butler.core import NamedKeyDict, TimespanDatabaseRepresentation
from lsst.daf.butler.registry.queries._structs import QueryColumns
from lsst.daf.butler.registry.queries.expressions import (
    CheckVisitor,
    NormalForm,
    NormalFormExpression,
    ParserYacc,
    convertExpressionToSql,
)
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.schema import Column


class FakeDatasetRecordStorageManager:
    ingestDate = Column("ingest_date")


class ConvertExpressionToSqlTestCase(unittest.TestCase):
    """A test case for convertExpressionToSql method"""

    def setUp(self):
        self.universe = DimensionUniverse()

    def test_simple(self):
        """Test with a trivial expression"""

        parser = ParserYacc()
        tree = parser.parse("1 > 0")
        self.assertIsNotNone(tree)

        columns = QueryColumns()
        elements = NamedKeyDict()
        column_element = convertExpressionToSql(
            tree, self.universe, columns, elements, {}, TimespanDatabaseRepresentation.Compound
        )
        self.assertEqual(str(column_element.compile()), ":param_1 > :param_2")
        self.assertEqual(str(column_element.compile(compile_kwargs={"literal_binds": True})), "1 > 0")

    def test_time(self):
        """Test with a trivial expression including times"""

        parser = ParserYacc()
        tree = parser.parse("T'1970-01-01 00:00/tai' < T'2020-01-01 00:00/tai'")
        self.assertIsNotNone(tree)

        columns = QueryColumns()
        elements = NamedKeyDict()
        column_element = convertExpressionToSql(
            tree, self.universe, columns, elements, {}, TimespanDatabaseRepresentation.Compound
        )
        self.assertEqual(str(column_element.compile()), ":param_1 < :param_2")
        self.assertEqual(
            str(column_element.compile(compile_kwargs={"literal_binds": True})), "0 < 1577836800000000000"
        )

    def test_ingest_date(self):
        """Test with an expression including ingest_date which is native UTC"""

        parser = ParserYacc()
        tree = parser.parse("ingest_date < T'2020-01-01 00:00/utc'")
        self.assertIsNotNone(tree)

        columns = QueryColumns()
        columns.datasets = FakeDatasetRecordStorageManager()
        elements = NamedKeyDict()
        column_element = convertExpressionToSql(
            tree, self.universe, columns, elements, {}, TimespanDatabaseRepresentation.Compound
        )

        # render it, needs specific dialect to convert column to expression
        dialect = postgresql.dialect()
        self.assertEqual(str(column_element.compile(dialect=dialect)), "ingest_date < TIMESTAMP %(param_1)s")
        self.assertEqual(
            str(column_element.compile(dialect=dialect, compile_kwargs={"literal_binds": True})),
            "ingest_date < TIMESTAMP '2020-01-01 00:00:00.000000'",
        )

        dialect = sqlite.dialect()
        self.assertEqual(str(column_element.compile(dialect=dialect)), "datetime(ingest_date) < datetime(?)")
        self.assertEqual(
            str(column_element.compile(dialect=dialect, compile_kwargs={"literal_binds": True})),
            "datetime(ingest_date) < datetime('2020-01-01 00:00:00.000000')",
        )


class CheckVisitorTestCase(unittest.TestCase):
    """Tests for CheckVisitor class."""

    def test_governor(self):
        """Test with governor dimension in expression"""

        parser = ParserYacc()

        universe = DimensionUniverse()
        graph = universe.extract(("instrument", "visit"))
        dataId = DataCoordinate.makeEmpty(universe)
        defaults = DataCoordinate.makeEmpty(universe)

        # governor-only constraint
        tree = parser.parse("instrument = 'LSST'")
        expr = NormalFormExpression.fromTree(tree, NormalForm.DISJUNCTIVE)
        binds = {}
        visitor = CheckVisitor(dataId, graph, binds, defaults)
        expr.visit(visitor)

        tree = parser.parse("'LSST' = instrument")
        expr = NormalFormExpression.fromTree(tree, NormalForm.DISJUNCTIVE)
        binds = {}
        visitor = CheckVisitor(dataId, graph, binds, defaults)
        expr.visit(visitor)

        # use bind for governor
        tree = parser.parse("instrument = instr")
        expr = NormalFormExpression.fromTree(tree, NormalForm.DISJUNCTIVE)
        binds = {"instr": "LSST"}
        visitor = CheckVisitor(dataId, graph, binds, defaults)
        expr.visit(visitor)


if __name__ == "__main__":
    unittest.main()

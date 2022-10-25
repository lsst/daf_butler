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
    InspectionVisitor,
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

    def test_bind(self):
        """Test with bind parameters"""

        parser = ParserYacc()
        tree = parser.parse("a > b OR t in (x, y, z)")
        self.assertIsNotNone(tree)

        columns = QueryColumns()
        elements = NamedKeyDict()
        column_element = convertExpressionToSql(
            tree,
            self.universe,
            columns,
            elements,
            {"a": 1, "b": 2, "t": 0, "x": 10, "y": 20, "z": 30},
            TimespanDatabaseRepresentation.Compound,
        )
        self.assertEqual(
            str(column_element.compile()), ":param_1 > :param_2 OR :param_3 IN (:param_4, :param_5, :param_6)"
        )
        self.assertEqual(
            str(column_element.compile(compile_kwargs={"literal_binds": True})), "1 > 2 OR 0 IN (10, 20, 30)"
        )

    def test_bind_list(self):
        """Test with bind parameter which is list/tuple/set inside IN rhs."""

        parser = ParserYacc()
        columns = QueryColumns()
        elements = NamedKeyDict()

        # Single bound variable inside IN()
        tree = parser.parse("a > b OR t in (x)")
        self.assertIsNotNone(tree)
        column_element = convertExpressionToSql(
            tree,
            self.universe,
            columns,
            elements,
            {"a": 1, "b": 2, "t": 0, "x": (10, 20, 30)},
            TimespanDatabaseRepresentation.Compound,
        )
        self.assertEqual(
            str(column_element.compile()), ":param_1 > :param_2 OR :param_3 IN (:param_4, :param_5, :param_6)"
        )
        self.assertEqual(
            str(column_element.compile(compile_kwargs={"literal_binds": True})), "1 > 2 OR 0 IN (10, 20, 30)"
        )

        # Couple of bound variables inside IN() with different combinations
        # of scalars and list.
        tree = parser.parse("a > b OR t in (x, y)")
        self.assertIsNotNone(tree)
        column_element = convertExpressionToSql(
            tree,
            self.universe,
            columns,
            elements,
            {"a": 1, "b": 2, "t": 0, "x": 10, "y": 20},
            TimespanDatabaseRepresentation.Compound,
        )
        self.assertEqual(
            str(column_element.compile()), ":param_1 > :param_2 OR :param_3 IN (:param_4, :param_5)"
        )
        self.assertEqual(
            str(column_element.compile(compile_kwargs={"literal_binds": True})), "1 > 2 OR 0 IN (10, 20)"
        )

        column_element = convertExpressionToSql(
            tree,
            self.universe,
            columns,
            elements,
            {"a": 1, "b": 2, "t": 0, "x": [10, 30], "y": 20},
            TimespanDatabaseRepresentation.Compound,
        )
        self.assertEqual(
            str(column_element.compile()), ":param_1 > :param_2 OR :param_3 IN (:param_4, :param_5, :param_6)"
        )
        self.assertEqual(
            str(column_element.compile(compile_kwargs={"literal_binds": True})), "1 > 2 OR 0 IN (10, 30, 20)"
        )

        column_element = convertExpressionToSql(
            tree,
            self.universe,
            columns,
            elements,
            {"a": 1, "b": 2, "t": 0, "x": (10, 30), "y": {20}},
            TimespanDatabaseRepresentation.Compound,
        )
        self.assertEqual(
            str(column_element.compile()),
            ":param_1 > :param_2 OR :param_3 IN (:param_4, :param_5, :param_6)",
        )
        self.assertEqual(
            str(column_element.compile(compile_kwargs={"literal_binds": True})),
            "1 > 2 OR 0 IN (10, 30, 20)",
        )


class InspectionVisitorTestCase(unittest.TestCase):
    """Tests for InspectionVisitor class."""

    def test_simple(self):
        """Test for simple expressions"""

        universe = DimensionUniverse()
        parser = ParserYacc()

        tree = parser.parse("instrument = 'LSST'")
        bind = {}
        summary = tree.visit(InspectionVisitor(universe, bind))
        self.assertEqual(summary.dimensions.names, {"instrument"})
        self.assertFalse(summary.columns)
        self.assertFalse(summary.hasIngestDate)
        self.assertEqual(summary.dataIdKey, universe["instrument"])
        self.assertEqual(summary.dataIdValue, "LSST")

        tree = parser.parse("instrument != 'LSST'")
        summary = tree.visit(InspectionVisitor(universe, bind))
        self.assertEqual(summary.dimensions.names, {"instrument"})
        self.assertFalse(summary.columns)
        self.assertIsNone(summary.dataIdKey)
        self.assertIsNone(summary.dataIdValue)

        tree = parser.parse("instrument = 'LSST' AND visit = 1")
        summary = tree.visit(InspectionVisitor(universe, bind))
        self.assertEqual(summary.dimensions.names, {"instrument", "visit", "band", "physical_filter"})
        self.assertFalse(summary.columns)
        self.assertIsNone(summary.dataIdKey)
        self.assertIsNone(summary.dataIdValue)

        tree = parser.parse("instrument = 'LSST' AND visit = 1 AND skymap = 'x'")
        summary = tree.visit(InspectionVisitor(universe, bind))
        self.assertEqual(
            summary.dimensions.names, {"instrument", "visit", "band", "physical_filter", "skymap"}
        )
        self.assertFalse(summary.columns)
        self.assertIsNone(summary.dataIdKey)
        self.assertIsNone(summary.dataIdValue)

    def test_bind(self):
        """Test for simple expressions with binds."""

        universe = DimensionUniverse()
        parser = ParserYacc()

        tree = parser.parse("instrument = instr")
        bind = {"instr": "LSST"}
        summary = tree.visit(InspectionVisitor(universe, bind))
        self.assertEqual(summary.dimensions.names, {"instrument"})
        self.assertFalse(summary.hasIngestDate)
        self.assertEqual(summary.dataIdKey, universe["instrument"])
        self.assertEqual(summary.dataIdValue, "LSST")

        tree = parser.parse("instrument != instr")
        self.assertEqual(summary.dimensions.names, {"instrument"})
        summary = tree.visit(InspectionVisitor(universe, bind))
        self.assertIsNone(summary.dataIdKey)
        self.assertIsNone(summary.dataIdValue)

        tree = parser.parse("instrument = instr AND visit = visit_id")
        bind = {"instr": "LSST", "visit_id": 1}
        summary = tree.visit(InspectionVisitor(universe, bind))
        self.assertEqual(summary.dimensions.names, {"instrument", "visit", "band", "physical_filter"})
        self.assertIsNone(summary.dataIdKey)
        self.assertIsNone(summary.dataIdValue)

        tree = parser.parse("instrument = 'LSST' AND visit = 1 AND skymap = skymap_name")
        bind = {"instr": "LSST", "visit_id": 1, "skymap_name": "x"}
        summary = tree.visit(InspectionVisitor(universe, bind))
        self.assertEqual(
            summary.dimensions.names, {"instrument", "visit", "band", "physical_filter", "skymap"}
        )
        self.assertIsNone(summary.dataIdKey)
        self.assertIsNone(summary.dataIdValue)

    def test_in(self):
        """Test for IN expressions."""

        universe = DimensionUniverse()
        parser = ParserYacc()

        tree = parser.parse("instrument IN ('LSST')")
        bind = {}
        summary = tree.visit(InspectionVisitor(universe, bind))
        self.assertEqual(summary.dimensions.names, {"instrument"})
        self.assertFalse(summary.hasIngestDate)
        # we do not handle IN with a single item as `=`
        self.assertIsNone(summary.dataIdKey)
        self.assertIsNone(summary.dataIdValue)

        tree = parser.parse("instrument IN (instr)")
        bind = {"instr": "LSST"}
        summary = tree.visit(InspectionVisitor(universe, bind))
        self.assertEqual(summary.dimensions.names, {"instrument"})
        self.assertIsNone(summary.dataIdKey)
        self.assertIsNone(summary.dataIdValue)

        tree = parser.parse("visit IN (1,2,3)")
        bind = {}
        summary = tree.visit(InspectionVisitor(universe, bind))
        self.assertEqual(summary.dimensions.names, {"instrument", "visit", "band", "physical_filter"})
        self.assertIsNone(summary.dataIdKey)
        self.assertIsNone(summary.dataIdValue)

        tree = parser.parse("visit IN (visit1, visit2, visit3)")
        bind = {"visit1": 1, "visit2": 2, "visit3": 3}
        summary = tree.visit(InspectionVisitor(universe, bind))
        self.assertEqual(summary.dimensions.names, {"instrument", "visit", "band", "physical_filter"})
        self.assertIsNone(summary.dataIdKey)
        self.assertIsNone(summary.dataIdValue)

        tree = parser.parse("visit IN (visits)")
        bind = {"visits": (1, 2, 3)}
        summary = tree.visit(InspectionVisitor(universe, bind))
        self.assertEqual(summary.dimensions.names, {"instrument", "visit", "band", "physical_filter"})
        self.assertIsNone(summary.dataIdKey)
        self.assertIsNone(summary.dataIdValue)


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

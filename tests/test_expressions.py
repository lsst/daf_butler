# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import datetime
import unittest

import astropy.time
import sqlalchemy
from lsst.daf.butler import (
    ColumnTypeInfo,
    DataCoordinate,
    DatasetColumnTag,
    DimensionUniverse,
    ddl,
    time_utils,
)
from lsst.daf.butler.registry.queries.expressions import make_string_expression_predicate
from lsst.daf.butler.registry.queries.expressions.check import CheckVisitor, InspectionVisitor
from lsst.daf.butler.registry.queries.expressions.normalForm import NormalForm, NormalFormExpression
from lsst.daf.butler.registry.queries.expressions.parser import ParserYacc
from lsst.daf.butler.timespan_database_representation import TimespanDatabaseRepresentation
from lsst.daf.relation import ColumnContainer, ColumnExpression
from sqlalchemy.schema import Column


class FakeDatasetRecordStorageManager:
    """Fake class for representing dataset record storage."""

    ingestDate = Column("ingest_date")


class ConvertExpressionToPredicateTestCase(unittest.TestCase):
    """A test case for the make_string_expression_predicate function"""

    ingest_date_dtype = sqlalchemy.TIMESTAMP
    ingest_date_pytype = datetime.datetime
    ingest_date_literal = datetime.datetime(2020, 1, 1)

    def setUp(self):
        self.column_types = ColumnTypeInfo(
            timespan_cls=TimespanDatabaseRepresentation.Compound,
            universe=DimensionUniverse(),
            dataset_id_spec=ddl.FieldSpec("dataset_id", dtype=ddl.GUID),
            run_key_spec=ddl.FieldSpec("run_id", dtype=sqlalchemy.BigInteger),
            ingest_date_dtype=self.ingest_date_dtype,
        )

    def test_simple(self):
        """Test with a trivial expression"""
        self.assertEqual(
            make_string_expression_predicate(
                "1 > 0", self.column_types.universe.empty, column_types=self.column_types
            )[0],
            ColumnExpression.literal(1, dtype=int).gt(ColumnExpression.literal(0, dtype=int)),
        )

    def test_time(self):
        """Test with a trivial expression including times"""
        time_converter = time_utils.TimeConverter()
        self.assertEqual(
            make_string_expression_predicate(
                "T'1970-01-01 00:00/tai' < T'2020-01-01 00:00/tai'",
                self.column_types.universe.empty,
                column_types=self.column_types,
            )[0],
            ColumnExpression.literal(time_converter.nsec_to_astropy(0), dtype=astropy.time.Time).lt(
                ColumnExpression.literal(
                    time_converter.nsec_to_astropy(1577836800000000000), dtype=astropy.time.Time
                )
            ),
        )

    def test_ingest_date(self):
        """Test with an expression including ingest_date which is native UTC"""
        self.assertEqual(
            make_string_expression_predicate(
                "ingest_date < T'2020-01-01 00:00/utc'",
                self.column_types.universe.empty,
                column_types=self.column_types,
                dataset_type_name="fake",
            )[0],
            ColumnExpression.reference(
                DatasetColumnTag("fake", "ingest_date"), dtype=self.ingest_date_pytype
            ).lt(ColumnExpression.literal(self.ingest_date_literal, dtype=self.ingest_date_pytype)),
        )

    def test_bind(self):
        """Test with bind parameters"""
        self.assertEqual(
            make_string_expression_predicate(
                "a > b OR t in (x, y, z)",
                self.column_types.universe.empty,
                column_types=self.column_types,
                bind={"a": 1, "b": 2, "t": 0, "x": 10, "y": 20, "z": 30},
            )[0],
            ColumnExpression.literal(1, dtype=int)
            .gt(ColumnExpression.literal(2, dtype=int))
            .logical_or(
                ColumnContainer.sequence(
                    [
                        ColumnExpression.literal(10, dtype=int),
                        ColumnExpression.literal(20, dtype=int),
                        ColumnExpression.literal(30, dtype=int),
                    ],
                    dtype=int,
                ).contains(ColumnExpression.literal(0, dtype=int))
            ),
        )

    def test_bind_list(self):
        """Test with bind parameter which is list/tuple/set inside IN rhs."""
        self.assertEqual(
            make_string_expression_predicate(
                "a > b OR t in (x)",
                self.column_types.universe.empty,
                column_types=self.column_types,
                bind={"a": 1, "b": 2, "t": 0, "x": (10, 20, 30)},
            )[0],
            ColumnExpression.literal(1, dtype=int)
            .gt(ColumnExpression.literal(2, dtype=int))
            .logical_or(
                ColumnContainer.sequence(
                    [
                        ColumnExpression.literal(10, dtype=int),
                        ColumnExpression.literal(20, dtype=int),
                        ColumnExpression.literal(30, dtype=int),
                    ],
                    dtype=int,
                ).contains(
                    ColumnExpression.literal(0, dtype=int),
                )
            ),
        )
        # Couple of bound variables inside IN() with different combinations
        # of scalars and list.
        self.assertEqual(
            make_string_expression_predicate(
                "a > b OR t in (x, y)",
                self.column_types.universe.empty,
                column_types=self.column_types,
                bind={"a": 1, "b": 2, "t": 0, "x": 10, "y": 20},
            )[0],
            ColumnExpression.literal(1, dtype=int)
            .gt(ColumnExpression.literal(2, dtype=int))
            .logical_or(
                ColumnContainer.sequence(
                    [
                        ColumnExpression.literal(10, dtype=int),
                        ColumnExpression.literal(20, dtype=int),
                    ],
                    dtype=int,
                ).contains(
                    ColumnExpression.literal(0, dtype=int),
                )
            ),
        )
        self.assertEqual(
            make_string_expression_predicate(
                "a > b OR t in (x, y)",
                self.column_types.universe.empty,
                column_types=self.column_types,
                bind={"a": 1, "b": 2, "t": 0, "x": [10, 30], "y": 20},
            )[0],
            ColumnExpression.literal(1, dtype=int)
            .gt(ColumnExpression.literal(2, dtype=int))
            .logical_or(
                ColumnContainer.sequence(
                    [
                        ColumnExpression.literal(10, dtype=int),
                        ColumnExpression.literal(30, dtype=int),
                        ColumnExpression.literal(20, dtype=int),
                    ],
                    dtype=int,
                ).contains(
                    ColumnExpression.literal(0, dtype=int),
                )
            ),
        )
        self.assertEqual(
            make_string_expression_predicate(
                "a > b OR t in (x, y)",
                self.column_types.universe.empty,
                column_types=self.column_types,
                bind={"a": 1, "b": 2, "t": 0, "x": (10, 30), "y": {20}},
            )[0],
            ColumnExpression.literal(1, dtype=int)
            .gt(ColumnExpression.literal(2, dtype=int))
            .logical_or(
                ColumnContainer.sequence(
                    [
                        ColumnExpression.literal(10, dtype=int),
                        ColumnExpression.literal(30, dtype=int),
                        ColumnExpression.literal(20, dtype=int),
                    ],
                    dtype=int,
                ).contains(ColumnExpression.literal(0, dtype=int))
            ),
        )


class ConvertExpressionToPredicateTestCaseAstropy(ConvertExpressionToPredicateTestCase):
    """A test case for the make_string_expression_predicate function with
    ingest_date defined as nanoseconds.
    """

    ingest_date_dtype = ddl.AstropyTimeNsecTai
    ingest_date_pytype = astropy.time.Time
    ingest_date_literal = astropy.time.Time(datetime.datetime(2020, 1, 1), scale="utc")


class InspectionVisitorTestCase(unittest.TestCase):
    """Tests for InspectionVisitor class."""

    def test_simple(self):
        """Test for simple expressions"""
        universe = DimensionUniverse()
        parser = ParserYacc()

        tree = parser.parse("instrument = 'LSST'")
        bind = {}
        summary = tree.visit(InspectionVisitor(universe, bind))
        self.assertEqual(summary.dimensions, {"instrument"})
        self.assertFalse(summary.columns)
        self.assertFalse(summary.hasIngestDate)
        self.assertEqual(summary.dataIdKey, universe["instrument"])
        self.assertEqual(summary.dataIdValue, "LSST")

        tree = parser.parse("instrument != 'LSST'")
        summary = tree.visit(InspectionVisitor(universe, bind))
        self.assertEqual(summary.dimensions, {"instrument"})
        self.assertFalse(summary.columns)
        self.assertIsNone(summary.dataIdKey)
        self.assertIsNone(summary.dataIdValue)

        tree = parser.parse("instrument = 'LSST' AND visit = 1")
        summary = tree.visit(InspectionVisitor(universe, bind))
        self.assertEqual(summary.dimensions, {"instrument", "visit", "band", "physical_filter", "day_obs"})
        self.assertFalse(summary.columns)
        self.assertIsNone(summary.dataIdKey)
        self.assertIsNone(summary.dataIdValue)

        tree = parser.parse("instrument = 'LSST' AND visit = 1 AND skymap = 'x'")
        summary = tree.visit(InspectionVisitor(universe, bind))
        self.assertEqual(
            summary.dimensions, {"instrument", "visit", "band", "physical_filter", "skymap", "day_obs"}
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
        self.assertEqual(summary.dimensions, {"instrument"})
        self.assertFalse(summary.hasIngestDate)
        self.assertEqual(summary.dataIdKey, universe["instrument"])
        self.assertEqual(summary.dataIdValue, "LSST")

        tree = parser.parse("instrument != instr")
        self.assertEqual(summary.dimensions, {"instrument"})
        summary = tree.visit(InspectionVisitor(universe, bind))
        self.assertIsNone(summary.dataIdKey)
        self.assertIsNone(summary.dataIdValue)

        tree = parser.parse("instrument = instr AND visit = visit_id")
        bind = {"instr": "LSST", "visit_id": 1}
        summary = tree.visit(InspectionVisitor(universe, bind))
        self.assertEqual(summary.dimensions, {"instrument", "visit", "band", "physical_filter", "day_obs"})
        self.assertIsNone(summary.dataIdKey)
        self.assertIsNone(summary.dataIdValue)

        tree = parser.parse("instrument = 'LSST' AND visit = 1 AND skymap = skymap_name")
        bind = {"instr": "LSST", "visit_id": 1, "skymap_name": "x"}
        summary = tree.visit(InspectionVisitor(universe, bind))
        self.assertEqual(
            summary.dimensions, {"instrument", "visit", "band", "physical_filter", "skymap", "day_obs"}
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
        self.assertEqual(summary.dimensions, {"instrument"})
        self.assertFalse(summary.hasIngestDate)
        # we do not handle IN with a single item as `=`
        self.assertIsNone(summary.dataIdKey)
        self.assertIsNone(summary.dataIdValue)

        tree = parser.parse("instrument IN (instr)")
        bind = {"instr": "LSST"}
        summary = tree.visit(InspectionVisitor(universe, bind))
        self.assertEqual(summary.dimensions, {"instrument"})
        self.assertIsNone(summary.dataIdKey)
        self.assertIsNone(summary.dataIdValue)

        tree = parser.parse("visit IN (1,2,3)")
        bind = {}
        summary = tree.visit(InspectionVisitor(universe, bind))
        self.assertEqual(summary.dimensions, {"instrument", "visit", "band", "physical_filter", "day_obs"})
        self.assertIsNone(summary.dataIdKey)
        self.assertIsNone(summary.dataIdValue)

        tree = parser.parse("visit IN (visit1, visit2, visit3)")
        bind = {"visit1": 1, "visit2": 2, "visit3": 3}
        summary = tree.visit(InspectionVisitor(universe, bind))
        self.assertEqual(summary.dimensions, {"instrument", "visit", "band", "physical_filter", "day_obs"})
        self.assertIsNone(summary.dataIdKey)
        self.assertIsNone(summary.dataIdValue)

        tree = parser.parse("visit IN (visits)")
        bind = {"visits": (1, 2, 3)}
        summary = tree.visit(InspectionVisitor(universe, bind))
        self.assertEqual(summary.dimensions, {"instrument", "visit", "band", "physical_filter", "day_obs"})
        self.assertIsNone(summary.dataIdKey)
        self.assertIsNone(summary.dataIdValue)


class CheckVisitorTestCase(unittest.TestCase):
    """Tests for CheckVisitor class."""

    def test_governor(self):
        """Test with governor dimension in expression"""
        parser = ParserYacc()

        universe = DimensionUniverse()
        dimensions = universe.conform(("instrument", "visit"))
        dataId = DataCoordinate.make_empty(universe)
        defaults = DataCoordinate.make_empty(universe)

        # governor-only constraint
        tree = parser.parse("instrument = 'LSST'")
        expr = NormalFormExpression.fromTree(tree, NormalForm.DISJUNCTIVE)
        binds = {}
        visitor = CheckVisitor(dataId, dimensions, binds, defaults)
        expr.visit(visitor)

        tree = parser.parse("'LSST' = instrument")
        expr = NormalFormExpression.fromTree(tree, NormalForm.DISJUNCTIVE)
        binds = {}
        visitor = CheckVisitor(dataId, dimensions, binds, defaults)
        expr.visit(visitor)

        # use bind for governor
        tree = parser.parse("instrument = instr")
        expr = NormalFormExpression.fromTree(tree, NormalForm.DISJUNCTIVE)
        binds = {"instr": "LSST"}
        visitor = CheckVisitor(dataId, dimensions, binds, defaults)
        expr.visit(visitor)


if __name__ == "__main__":
    unittest.main()

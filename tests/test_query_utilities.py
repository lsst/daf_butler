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

"""Tests for non-public Butler._query functionality that is not specific to
any Butler or QueryDriver implementation.
"""

from __future__ import annotations

import unittest
from collections.abc import Iterable, Set

import astropy.time

from lsst.daf.butler import DimensionUniverse, InvalidQueryError, Timespan
from lsst.daf.butler.dimensions import DimensionElement, DimensionGroup
from lsst.daf.butler.queries import tree as qt
from lsst.daf.butler.queries.expression_factory import ExpressionFactory
from lsst.daf.butler.queries.overlaps import OverlapsVisitor, _NaiveDisjointSet
from lsst.daf.butler.queries.visitors import PredicateVisitFlags
from lsst.sphgeom import Mq3cPixelization, Region


class ColumnSetTestCase(unittest.TestCase):
    """Tests for lsst.daf.butler.queries.ColumnSet."""

    def setUp(self) -> None:
        self.universe = DimensionUniverse()

    def test_basics(self) -> None:
        columns = qt.ColumnSet(self.universe.conform(["detector"]))
        self.assertNotEqual(columns, columns.dimensions.names)  # intentionally not comparable to other sets
        self.assertEqual(columns.dimensions, self.universe.conform(["detector"]))
        self.assertFalse(columns.dataset_fields)
        columns.dataset_fields["bias"].add("dataset_id")
        self.assertEqual(dict(columns.dataset_fields), {"bias": {"dataset_id"}})
        columns.dimension_fields["detector"].add("purpose")
        self.assertEqual(columns.dimension_fields["detector"], {"purpose"})
        self.assertTrue(columns)
        self.assertEqual(
            list(columns),
            [(k, None) for k in columns.dimensions.data_coordinate_keys]
            + [("detector", "purpose"), ("bias", "dataset_id")],
        )
        self.assertEqual(str(columns), "{instrument, detector, detector:purpose, bias:dataset_id}")
        empty = qt.ColumnSet(self.universe.empty)
        self.assertFalse(empty)
        self.assertFalse(columns.issubset(empty))
        self.assertTrue(columns.issuperset(empty))
        self.assertTrue(columns.isdisjoint(empty))
        copy = columns.copy()
        self.assertEqual(columns, copy)
        self.assertTrue(columns.issubset(copy))
        self.assertTrue(columns.issuperset(copy))
        self.assertFalse(columns.isdisjoint(copy))
        copy.dataset_fields["bias"].add("timespan")
        copy.dimension_fields["detector"].add("name")
        copy.update_dimensions(self.universe.conform(["band"]))
        self.assertEqual(copy.dataset_fields["bias"], {"dataset_id", "timespan"})
        self.assertEqual(columns.dataset_fields["bias"], {"dataset_id"})
        self.assertEqual(copy.dimension_fields["detector"], {"purpose", "name"})
        self.assertEqual(columns.dimension_fields["detector"], {"purpose"})
        self.assertTrue(columns.issubset(copy))
        self.assertFalse(columns.issuperset(copy))
        self.assertFalse(columns.isdisjoint(copy))
        columns.update(copy)
        self.assertEqual(columns, copy)
        self.assertTrue(columns.is_timespan("visit", "timespan"))
        self.assertFalse(columns.is_timespan("visit", None))
        self.assertFalse(columns.is_timespan("detector", "purpose"))

    def test_drop_dimension_keys(self):
        columns = qt.ColumnSet(self.universe.conform(["physical_filter"]))
        columns.drop_implied_dimension_keys()
        self.assertEqual(list(columns), [("instrument", None), ("physical_filter", None)])
        undropped = qt.ColumnSet(columns.dimensions)
        self.assertTrue(columns.issubset(undropped))
        self.assertFalse(columns.issuperset(undropped))
        self.assertFalse(columns.isdisjoint(undropped))
        band_only = qt.ColumnSet(self.universe.conform(["band"]))
        self.assertFalse(columns.issubset(band_only))
        self.assertFalse(columns.issuperset(band_only))
        self.assertTrue(columns.isdisjoint(band_only))
        copy = columns.copy()
        copy.update(band_only)
        self.assertEqual(copy, undropped)
        columns.restore_dimension_keys()
        self.assertEqual(columns, undropped)

    def test_get_column_spec(self) -> None:
        columns = qt.ColumnSet(self.universe.conform(["detector"]))
        columns.dimension_fields["detector"].add("purpose")
        columns.dataset_fields["bias"].update(["dataset_id", "run", "collection", "timespan", "ingest_date"])
        self.assertEqual(columns.get_column_spec("instrument", None).name, "instrument")
        self.assertEqual(columns.get_column_spec("instrument", None).type, "string")
        self.assertEqual(columns.get_column_spec("instrument", None).nullable, False)
        self.assertEqual(columns.get_column_spec("detector", None).name, "detector")
        self.assertEqual(columns.get_column_spec("detector", None).type, "int")
        self.assertEqual(columns.get_column_spec("detector", None).nullable, False)
        self.assertEqual(columns.get_column_spec("detector", "purpose").name, "detector:purpose")
        self.assertEqual(columns.get_column_spec("detector", "purpose").type, "string")
        self.assertEqual(columns.get_column_spec("detector", "purpose").nullable, True)
        self.assertEqual(columns.get_column_spec("bias", "dataset_id").name, "bias:dataset_id")
        self.assertEqual(columns.get_column_spec("bias", "dataset_id").type, "uuid")
        self.assertEqual(columns.get_column_spec("bias", "dataset_id").nullable, False)
        self.assertEqual(columns.get_column_spec("bias", "run").name, "bias:run")
        self.assertEqual(columns.get_column_spec("bias", "run").type, "string")
        self.assertEqual(columns.get_column_spec("bias", "run").nullable, False)
        self.assertEqual(columns.get_column_spec("bias", "collection").name, "bias:collection")
        self.assertEqual(columns.get_column_spec("bias", "collection").type, "string")
        self.assertEqual(columns.get_column_spec("bias", "collection").nullable, False)
        self.assertEqual(columns.get_column_spec("bias", "timespan").name, "bias:timespan")
        self.assertEqual(columns.get_column_spec("bias", "timespan").type, "timespan")
        self.assertEqual(columns.get_column_spec("bias", "timespan").nullable, False)
        self.assertEqual(columns.get_column_spec("bias", "ingest_date").name, "bias:ingest_date")
        self.assertEqual(columns.get_column_spec("bias", "ingest_date").type, "datetime")
        self.assertEqual(columns.get_column_spec("bias", "ingest_date").nullable, True)


class _RecordingOverlapsVisitor(OverlapsVisitor):
    def __init__(self, dimensions: DimensionGroup, calibration_dataset_types: Set[str] = frozenset()):
        super().__init__(dimensions, calibration_dataset_types)
        self.spatial_constraints: list[tuple[str, PredicateVisitFlags]] = []
        self.spatial_joins: list[tuple[str, str, PredicateVisitFlags]] = []
        self.temporal_dimension_joins: list[tuple[str, str, PredicateVisitFlags]] = []
        self.validity_range_dimension_joins: list[tuple[str, str, PredicateVisitFlags]] = []
        self.validity_range_joins: list[tuple[str, str, PredicateVisitFlags]] = []

    def visit_spatial_constraint(
        self, element: DimensionElement, region: Region, flags: PredicateVisitFlags
    ) -> qt.Predicate | None:
        self.spatial_constraints.append((element.name, flags))
        return super().visit_spatial_constraint(element, region, flags)

    def visit_spatial_join(
        self, a: DimensionElement, b: DimensionElement, flags: PredicateVisitFlags
    ) -> qt.Predicate | None:
        self.spatial_joins.append((a.name, b.name, flags))
        return super().visit_spatial_join(a, b, flags)

    def visit_temporal_dimension_join(
        self, a: DimensionElement, b: DimensionElement, flags: PredicateVisitFlags
    ) -> qt.Predicate | None:
        self.temporal_dimension_joins.append((a.name, b.name, flags))
        return super().visit_temporal_dimension_join(a, b, flags)

    def visit_validity_range_dimension_join(
        self, a: str, b: DimensionElement, flags: PredicateVisitFlags
    ) -> qt.Predicate | None:
        self.validity_range_dimension_joins.append((a, b.name, flags))
        return super().visit_validity_range_dimension_join(a, b, flags)

    def visit_validity_range_join(self, a: str, b: str, flags: PredicateVisitFlags) -> qt.Predicate | None:
        self.validity_range_joins.append((a, b, flags))
        return super().visit_validity_range_join(a, b, flags)


class OverlapsVisitorTestCase(unittest.TestCase):
    """Tests for lsst.daf.butler.queries.overlaps.OverlapsVisitor, which is
    responsible for validating and inferring spatial and temporal joins and
    constraints.
    """

    def setUp(self) -> None:
        self.universe = DimensionUniverse()

    def run_visitor(
        self,
        dimensions: Iterable[str],
        predicate: qt.Predicate,
        expected: str | None = None,
        join_operands: Iterable[DimensionGroup] = (),
        calibration_dataset_types: Set[str] = frozenset(),
    ) -> _RecordingOverlapsVisitor:
        visitor = _RecordingOverlapsVisitor(self.universe.conform(dimensions), calibration_dataset_types)
        if expected is None:
            expected = str(predicate)
        new_predicate = visitor.run(predicate, join_operands=join_operands)
        self.assertEqual(str(new_predicate), expected)
        return visitor

    def test_trivial(self) -> None:
        """Test the overlaps visitor when there is nothing spatial or temporal
        in the query at all.
        """
        x = ExpressionFactory(self.universe)
        # Trivial predicate.
        visitor = self.run_visitor(["physical_filter"], qt.Predicate.from_bool(True))
        self.assertFalse(visitor.spatial_joins)
        self.assertFalse(visitor.spatial_constraints)
        self.assertFalse(visitor.temporal_dimension_joins)
        # Non-overlap predicate.
        visitor = self.run_visitor(["physical_filter"], x.any(x.band == "r", x.band == "i"))
        self.assertFalse(visitor.spatial_joins)
        self.assertFalse(visitor.spatial_constraints)
        self.assertFalse(visitor.temporal_dimension_joins)

    def test_one_spatial_family(self) -> None:
        """Test the overlaps visitor when there is one spatial family."""
        x = ExpressionFactory(self.universe)
        pixelization = Mq3cPixelization(10)
        region = pixelization.quad(12058870)
        # Trivial predicate.
        visitor = self.run_visitor(["visit"], qt.Predicate.from_bool(True))
        self.assertFalse(visitor.spatial_joins)
        self.assertFalse(visitor.spatial_constraints)
        self.assertFalse(visitor.temporal_dimension_joins)
        # Non-overlap predicate.
        visitor = self.run_visitor(["visit"], x.any(x.band == "r", x.visit > 2))
        self.assertFalse(visitor.spatial_joins)
        self.assertFalse(visitor.spatial_constraints)
        self.assertFalse(visitor.temporal_dimension_joins)
        # Spatial constraint predicate, in various positions relative to other
        # non-overlap predicates.
        visitor = self.run_visitor(["visit"], x.visit.region.overlaps(region))
        self.assertEqual(visitor.spatial_constraints, [(self.universe["visit"], PredicateVisitFlags(0))])
        visitor = self.run_visitor(["visit"], x.all(x.visit.region.overlaps(region), x.band == "r"))
        self.assertEqual(
            visitor.spatial_constraints, [(self.universe["visit"], PredicateVisitFlags.HAS_AND_SIBLINGS)]
        )
        visitor = self.run_visitor(["visit"], x.any(x.visit.region.overlaps(region), x.band == "r"))
        self.assertEqual(
            visitor.spatial_constraints, [(self.universe["visit"], PredicateVisitFlags.HAS_OR_SIBLINGS)]
        )
        visitor = self.run_visitor(
            ["visit"],
            x.all(
                x.any(x.literal(region).overlaps(x.visit.region), x.band == "r"),
                x.visit.observation_reason == "science",
            ),
        )
        self.assertEqual(
            visitor.spatial_constraints,
            [
                (
                    self.universe["visit"],
                    PredicateVisitFlags.HAS_OR_SIBLINGS | PredicateVisitFlags.HAS_AND_SIBLINGS,
                )
            ],
        )
        visitor = self.run_visitor(
            ["visit"],
            x.any(
                x.all(x.visit.region.overlaps(region), x.band == "r"),
                x.visit.observation_reason == "science",
            ),
        )
        self.assertEqual(
            visitor.spatial_constraints,
            [
                (
                    self.universe["visit"],
                    PredicateVisitFlags.HAS_OR_SIBLINGS | PredicateVisitFlags.HAS_AND_SIBLINGS,
                )
            ],
        )
        # A spatial join between dimensions in the same family is an error.
        with self.assertRaises(InvalidQueryError):
            self.run_visitor(["patch", "tract"], x.patch.region.overlaps(x.tract.region))

    def test_single_unambiguous_spatial_join(self) -> None:
        """Test the overlaps visitor when there are two spatial families with
        one dimension element in each, and hence exactly one join is needed.
        """
        x = ExpressionFactory(self.universe)
        # Trivial predicate; an automatic join is added.  Order of elements in
        # automatic joins is lexicographical in order to be deterministic.
        visitor = self.run_visitor(
            ["visit", "tract"], qt.Predicate.from_bool(True), "tract.region OVERLAPS visit.region"
        )
        self.assertEqual(visitor.spatial_joins, [("tract", "visit", PredicateVisitFlags.HAS_AND_SIBLINGS)])
        self.assertFalse(visitor.spatial_constraints)
        self.assertFalse(visitor.temporal_dimension_joins)
        # Non-overlap predicate; an automatic join is added.
        visitor = self.run_visitor(
            ["visit", "tract"],
            x.all(x.band == "r", x.visit > 2),
            "band == 'r' AND visit > 2 AND tract.region OVERLAPS visit.region",
        )
        self.assertEqual(visitor.spatial_joins, [("tract", "visit", PredicateVisitFlags.HAS_AND_SIBLINGS)])
        self.assertFalse(visitor.spatial_constraints)
        self.assertFalse(visitor.temporal_dimension_joins)
        # The same overlap predicate that would be added automatically has been
        # added manually.
        visitor = self.run_visitor(
            ["visit", "tract"],
            x.tract.region.overlaps(x.visit.region),
            "tract.region OVERLAPS visit.region",
        )
        self.assertEqual(visitor.spatial_joins, [("tract", "visit", PredicateVisitFlags(0))])
        self.assertFalse(visitor.spatial_constraints)
        self.assertFalse(visitor.temporal_dimension_joins)
        # Add the join overlap predicate in an OR expression, which is unusual
        # but enough to block the addition of an automatic join; we assume the
        # user knows what they're doing.
        visitor = self.run_visitor(
            ["visit", "tract"],
            x.any(x.visit > 2, x.tract.region.overlaps(x.visit.region)),
            "visit > 2 OR tract.region OVERLAPS visit.region",
        )
        self.assertEqual(visitor.spatial_joins, [("tract", "visit", PredicateVisitFlags.HAS_OR_SIBLINGS)])
        self.assertFalse(visitor.spatial_constraints)
        self.assertFalse(visitor.temporal_dimension_joins)
        # Add the join overlap predicate in a NOT expression, which is unusual
        # but permitted in the same sense as OR expressions.
        visitor = self.run_visitor(
            ["visit", "tract"],
            x.not_(x.tract.region.overlaps(x.visit.region)),
            "NOT tract.region OVERLAPS visit.region",
        )
        self.assertEqual(visitor.spatial_joins, [("tract", "visit", PredicateVisitFlags.INVERTED)])
        self.assertFalse(visitor.spatial_constraints)
        self.assertFalse(visitor.temporal_dimension_joins)
        # Add a "join operand" whose dimensions include both spatial families.
        # This blocks an automatic join from being created, because we assume
        # that join operand (e.g. a materialization or dataset search) already
        # encodes some spatial join.
        visitor = self.run_visitor(
            ["visit", "tract"],
            qt.Predicate.from_bool(True),
            "True",
            join_operands=[self.universe.conform(["tract", "visit"])],
        )
        self.assertFalse(visitor.spatial_joins)
        self.assertFalse(visitor.spatial_constraints)
        self.assertFalse(visitor.temporal_dimension_joins)

    def test_single_flexible_spatial_join(self) -> None:
        """Test the overlaps visitor when there are two spatial families and
        one has multiple dimension elements.
        """
        x = ExpressionFactory(self.universe)
        # Trivial predicate; an automatic join between the fine-grained
        # elements is added.  Order of elements in automatic joins is
        # lexicographical in order to be deterministic.
        visitor = self.run_visitor(
            ["visit", "detector", "patch"],
            qt.Predicate.from_bool(True),
            "patch.region OVERLAPS visit_detector_region.region",
        )
        self.assertEqual(
            visitor.spatial_joins, [("patch", "visit_detector_region", PredicateVisitFlags.HAS_AND_SIBLINGS)]
        )
        self.assertFalse(visitor.spatial_constraints)
        self.assertFalse(visitor.temporal_dimension_joins)
        # The same overlap predicate that would be added automatically has been
        # added manually.
        visitor = self.run_visitor(
            ["visit", "detector", "patch"],
            x.patch.region.overlaps(x.visit_detector_region.region),
            "patch.region OVERLAPS visit_detector_region.region",
        )
        self.assertEqual(visitor.spatial_joins, [("patch", "visit_detector_region", PredicateVisitFlags(0))])
        self.assertFalse(visitor.spatial_constraints)
        self.assertFalse(visitor.temporal_dimension_joins)
        # A coarse overlap join has been added; respect it and do not add an
        # automatic one.
        visitor = self.run_visitor(
            ["visit", "detector", "patch"],
            x.tract.region.overlaps(x.visit.region),
            "tract.region OVERLAPS visit.region",
        )
        self.assertEqual(visitor.spatial_joins, [("tract", "visit", PredicateVisitFlags(0))])
        self.assertFalse(visitor.spatial_constraints)
        self.assertFalse(visitor.temporal_dimension_joins)
        # Add a "join operand" whose dimensions include both spatial families
        # with the most fine-grained dimensions in the query.
        # This blocks an automatic join from being created, because we assume
        # that join operand (e.g. a materialization or dataset search) already
        # encodes some spatial join.
        visitor = self.run_visitor(
            ["visit", "detector", "patch"],
            qt.Predicate.from_bool(True),
            "True",
            join_operands=[self.universe.conform(["patch", "visit_detector_region"])],
        )
        self.assertFalse(visitor.spatial_joins)
        self.assertFalse(visitor.spatial_constraints)
        self.assertFalse(visitor.temporal_dimension_joins)

    def test_multiple_spatial_joins(self) -> None:
        """Test the overlaps visitor when there are >2 spatial families."""
        x = ExpressionFactory(self.universe)
        # Trivial predicate.  This is an error, because we cannot generate
        # automatic spatial joins when there are more than two families
        with self.assertRaises(InvalidQueryError):
            self.run_visitor(["visit", "patch", "htm7"], qt.Predicate.from_bool(True))
        # Predicate that joins one pair of families but orphans the the other;
        # also an error.
        with self.assertRaises(InvalidQueryError):
            self.run_visitor(["visit", "patch", "htm7"], x.visit.region.overlaps(x.htm7.region))
        # A sufficient overlap join predicate has been added; each family is
        # connected to at least one other.
        visitor = self.run_visitor(
            ["visit", "patch", "htm7"],
            x.all(x.tract.region.overlaps(x.visit.region), x.tract.region.overlaps(x.htm7.region)),
            "tract.region OVERLAPS visit.region AND tract.region OVERLAPS htm7.region",
        )
        self.assertEqual(
            visitor.spatial_joins,
            [
                ("tract", "visit", PredicateVisitFlags.HAS_AND_SIBLINGS),
                ("tract", "htm7", PredicateVisitFlags.HAS_AND_SIBLINGS),
            ],
        )
        self.assertFalse(visitor.spatial_constraints)
        self.assertFalse(visitor.temporal_dimension_joins)
        # Add a "join operand" whose dimensions includes two spatial families,
        # with the most fine-grained dimensions in the query, and a predicate
        # that joins the third in.
        visitor = self.run_visitor(
            ["visit", "patch", "htm7"],
            x.tract.region.overlaps(x.htm7.region),
            "tract.region OVERLAPS htm7.region",
            join_operands=[self.universe.conform(["visit", "patch"])],
        )
        self.assertEqual(
            visitor.spatial_joins,
            [
                ("tract", "htm7", PredicateVisitFlags(0)),
            ],
        )
        self.assertFalse(visitor.spatial_constraints)
        self.assertFalse(visitor.temporal_dimension_joins)

    def test_one_temporal_family(self) -> None:
        """Test the overlaps visitor when there is one temporal family."""
        x = ExpressionFactory(self.universe)
        begin = astropy.time.Time("2020-01-01T00:00:00", format="isot", scale="tai")
        end = astropy.time.Time("2020-01-01T00:01:00", format="isot", scale="tai")
        timespan = Timespan(begin, end)
        # Trivial predicate.
        visitor = self.run_visitor(["exposure"], qt.Predicate.from_bool(True))
        self.assertFalse(visitor.spatial_joins)
        self.assertFalse(visitor.spatial_constraints)
        self.assertFalse(visitor.temporal_dimension_joins)
        # Non-overlap predicate.
        visitor = self.run_visitor(["exposure"], x.any(x.band == "r", x.exposure > 2))
        self.assertFalse(visitor.spatial_joins)
        self.assertFalse(visitor.spatial_constraints)
        self.assertFalse(visitor.temporal_dimension_joins)
        # Temporal constraint predicate.
        visitor = self.run_visitor(["exposure"], x.exposure.timespan.overlaps(timespan))
        self.assertFalse(visitor.spatial_joins)
        self.assertFalse(visitor.spatial_constraints)
        self.assertFalse(visitor.temporal_dimension_joins)
        # A temporal join between dimensions in the same family is an error.
        with self.assertRaises(InvalidQueryError):
            self.run_visitor(["exposure", "visit"], x.exposure.timespan.overlaps(x.visit.timespan))
        # Overlap join with a calibration dataset's validity ranges.
        visitor = self.run_visitor(
            ["exposure"], x.exposure.timespan.overlaps(x["bias"].timespan), calibration_dataset_types={"bias"}
        )
        self.assertFalse(visitor.spatial_joins)
        self.assertFalse(visitor.spatial_constraints)
        self.assertFalse(visitor.temporal_dimension_joins)
        self.assertEqual(
            visitor.validity_range_dimension_joins, [("bias", "exposure", PredicateVisitFlags(0))]
        )
        self.assertFalse(visitor.validity_range_joins)
        # Overlap join between two calibration dataset validity ranges.
        # (It's not clear this kind of query is ever useful in practice, but
        # there's a good consistency argument for what it ought to do).
        visitor = self.run_visitor(
            [], x["flat"].timespan.overlaps(x["bias"].timespan), calibration_dataset_types={"bias", "flat"}
        )
        self.assertFalse(visitor.spatial_joins)
        self.assertFalse(visitor.spatial_constraints)
        self.assertFalse(visitor.temporal_dimension_joins)
        self.assertFalse(visitor.validity_range_dimension_joins)
        self.assertEqual(visitor.validity_range_joins, [("flat", "bias", PredicateVisitFlags(0))])

    # There are no tests for temporal dimension joins, because the default
    # dimension universe only has one spatial family, and the untested logic
    # trivially duplicates the spatial-join logic.


class NaiveDisjointSetTestCase(unittest.TestCase):
    """Test the naive disjoint-set implementation that backs automatic overlap
    join creation.
    """

    def test_naive_disjoint_set(self) -> None:
        s = _NaiveDisjointSet(range(8))
        self.assertCountEqual(s.subsets(), [{n} for n in range(8)])
        s.merge(3, 4)
        self.assertCountEqual(s.subsets(), [{0}, {1}, {2}, {3, 4}, {5}, {6}, {7}])
        s.merge(2, 1)
        self.assertCountEqual(s.subsets(), [{0}, {1, 2}, {3, 4}, {5}, {6}, {7}])
        s.merge(1, 3)
        self.assertCountEqual(s.subsets(), [{0}, {1, 2, 3, 4}, {5}, {6}, {7}])


if __name__ == "__main__":
    unittest.main()

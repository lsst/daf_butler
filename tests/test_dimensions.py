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

import unittest
import itertools

import lsst.utils
import lsst.utils.tests

from lsst.daf.butler import DimensionGraph, DimensionUnit, DimensionJoin, DimensionJoinSet


class DimensionTestCase(lsst.utils.tests.TestCase):
    """Tests for dimensions.

    All tests here rely on the content of ``config/dimensions.yaml``, either
    to test that the definitions there are read in properly or just as generic
    data for testing various operations.
    """

    def setUp(self):
        self.universe = DimensionGraph.fromConfig()

    def checkSetInvariants(self, dimensions):
        """Run tests on DimensionSet that should pass for any instance.
        """
        copy1 = dimensions.copy()
        copy2 = dimensions.union(dimensions)
        copy3 = dimensions.intersection(dimensions)
        for name, copy in [("copy", copy1), ("union", copy2), ("intersection", copy3)]:
            with self.subTest(copy=name):
                self.assertLessEqual(dimensions, copy)
                self.assertGreaterEqual(dimensions, copy)
                self.assertEqual(dimensions, copy)
                self.assertTrue(dimensions.issubset(copy))
                self.assertTrue(dimensions.issuperset(copy))
                self.assertFalse(dimensions != copy)
                self.assertFalse(dimensions < copy)
                self.assertFalse(dimensions > copy)
                self.assertTrue(not dimensions.isdisjoint(copy) or not dimensions)

    def checkUnitSetInvariants(self, units):
        """Run tests on DimensionUnitSet that should pass for any instance.
        """
        self.checkSetInvariants(units)
        self.assertLessEqual(units, self.universe.units)
        for unit in units:
            self.assertIn(unit, units)
            self.assertIn(unit.name, units)
            self.assertEqual(units[unit.name], unit)
            self.assertEqual(units.get(unit.name), unit)
            self.assertIsInstance(unit, DimensionUnit)
            self.assertGreaterEqual(unit.dependencies(), unit.dependencies(optional=False))
            self.assertGreaterEqual(unit.dependencies(), unit.dependencies(optional=True))
            self.assertLessEqual(unit.dependencies(), units)

    def checkGraphInvariants(self, graph):
        """Run tests on DimensionGraph that should pass for any instance.
        """
        self.checkUnitSetInvariants(graph.units)
        self.checkSetInvariants(graph.joins(summaries=False))
        self.checkSetInvariants(graph.joins(summaries=True))
        copy1 = graph.subgraph(graph.units)
        copy2 = graph.union(graph)
        copy3 = graph.intersection(graph)
        for name, copy in [("subgraph", copy1), ("union", copy2), ("intersection", copy3)]:
            with self.subTest(copy=name):
                self.assertLessEqual(graph, copy)
                self.assertGreaterEqual(graph, copy)
                self.assertEqual(graph, copy)
                self.assertTrue(graph.issubset(copy))
                self.assertTrue(graph.issuperset(copy))
                self.assertTrue(not graph.isdisjoint(copy) or not graph)
                self.assertFalse(graph != copy)
                self.assertFalse(graph < copy)
                self.assertFalse(graph > copy)
        self.assertLessEqual(graph, copy)
        self.assertGreaterEqual(graph, copy)
        self.assertEqual(graph, copy)
        self.assertTrue(graph.issubset(copy))
        self.assertTrue(graph.issuperset(copy))
        self.assertFalse(graph != copy)
        self.assertFalse(graph < copy)
        self.assertFalse(graph > copy)
        self.assertFalse(graph.isdisjoint(copy))
        for join in graph.joins(summaries=True):
            self.assertIsInstance(join, DimensionJoin)
            self.assertGreater(join.dependencies(), join.lhs)
            self.assertGreater(join.dependencies(), join.rhs)
            self.assertLessEqual(join.dependencies(), graph.units)
        for join in graph.joins(summaries=False):
            self.assertTrue(join.summarizes.isdisjoint(graph.joins()))

    def testInstrumentUnits(self):
        """Test that the Instrument units and joins we expect to be defined in
        ``dimensions.yaml`` are present and related correctly.
        """
        graph1 = self.universe.subgraph(where=lambda unit: "Instrument" in unit.dependencies().names)
        self.checkGraphInvariants(graph1)
        # AbstractFilter is included below because PhysicalFilter depends on AbstractFilter
        self.assertCountEqual(graph1.units.names,
                              ["Instrument", "Detector", "PhysicalFilter", "Visit", "Exposure",
                               "ExposureRange", "AbstractFilter"])
        self.assertCountEqual(graph1.joins().names, ["VisitDetectorRegion"])
        self.assertEqual(graph1.getRegionHolder(), graph1.joins().get("VisitDetectorRegion"))
        graph2 = graph1.subgraph(["Visit"])
        self.assertCountEqual(graph2.units.names, ["Instrument", "PhysicalFilter", "Visit", "AbstractFilter"])
        self.assertEqual(graph2.getRegionHolder(), graph1.units["Visit"])
        self.assertCountEqual(graph2.joins().names, [])
        self.checkGraphInvariants(graph2)
        graph3 = graph1.subgraph(["Detector"])
        self.checkGraphInvariants(graph3)
        self.assertCountEqual(graph3.units.names, ["Instrument", "Detector"])
        self.assertIsNone(graph3.getRegionHolder())
        self.assertCountEqual(graph3.joins(), [])
        visit = self.universe.units["Visit"]
        self.assertCountEqual(visit.dependencies(optional=True).names,
                              ["Instrument", "PhysicalFilter", "AbstractFilter"])
        self.assertCountEqual(visit.dependencies(optional=False).names,
                              ["Instrument"])

    def testSkyMapUnits(self):
        """Test that the SkyMap units and joins we expect to be defined in
        ``dimensions.yaml`` are present and related correctly.
        """
        patchGraph = self.universe.subgraph(["Patch"])
        self.checkGraphInvariants(patchGraph)
        self.assertCountEqual(patchGraph.units.names, ["SkyMap", "Tract", "Patch"])
        self.assertCountEqual(patchGraph.joins(), [])
        self.assertEqual(patchGraph.getRegionHolder(), patchGraph.units["Patch"])
        tractGraph = patchGraph.subgraph(["Tract"])
        self.checkGraphInvariants(tractGraph)
        self.assertCountEqual(tractGraph.units.names, ["SkyMap", "Tract"])
        self.assertEqual(tractGraph.getRegionHolder(), tractGraph.units["Tract"])
        self.assertCountEqual(tractGraph.joins(), [])
        skyMapOnly = tractGraph.subgraph(["SkyMap"])
        self.checkGraphInvariants(skyMapOnly)
        self.assertCountEqual(skyMapOnly.units.names, ["SkyMap"])
        self.assertIsNone(skyMapOnly.getRegionHolder())
        self.assertCountEqual(skyMapOnly.joins(), [])

    def testMiscUnits(self):
        """Test that the miscalleneous units and joins we expect to be defined
        in ``dimensions.yaml`` are present and related correctly.
        """
        misc = self.universe.subgraph(
            where=lambda unit: (unit.dependencies().names.isdisjoint(["Instrument", "SkyMap"]) and
                                unit.name not in ("Instrument", "SkyMap"))
        )
        self.checkGraphInvariants(misc)
        self.assertCountEqual(misc.units.names, ["SkyPix", "Label", "AbstractFilter"])
        self.assertCountEqual(misc.joins(), [])
        self.assertEqual(misc.getRegionHolder(), misc.units["SkyPix"])

    def checkSpatialJoin(self, lhsNames, rhsNames, joinName=None):
        """Test the spatial join that relates the given units.

        Parameters
        ----------
        lhsNames : `list` of `str`
            Name of the DimensionUnits of the left-hand side of the join.
        rhsNames : `list` of `str`
            Name of the DimensionUnits of the right-hand side of the join.
        joinName : `str`, optional
            Name of the DimensionJoin to be tested; if `None`, computed by
            concatenating ``lhsNames`` and ``rhsNames``.
        """
        if joinName is None:
            joinName = "{}{}Join".format("".join(lhsNames), "".join(rhsNames))
        lhs = self.universe.subgraph(lhsNames)
        rhs = self.universe.subgraph(rhsNames)
        both = self.universe.subgraph(joins=[joinName])
        self.checkGraphInvariants(both)
        join = both.joins().get(joinName)
        self.assertIsNotNone(join)
        self.assertLess(lhs.units, both.units)
        self.assertGreater(both.units, rhs.units)
        self.assertGreaterEqual(lhs.units, join.lhs)  # [lr]hs.units has optionals, join.[lr]hs does not
        self.assertGreaterEqual(rhs.units, join.rhs)
        allExpectedJoins = set([join.name]).union(lhs.joins().names, rhs.joins().names)
        self.assertEqual(both.joins().names, allExpectedJoins)

    def testSpatialJoins(self):
        """Test that the spatial joins defined in ``dimensions.yaml`` are
        present and related correctly.
        """
        self.checkSpatialJoin(["Tract"], ["SkyPix"])
        self.checkSpatialJoin(["Patch"], ["SkyPix"])
        self.checkSpatialJoin(["Visit"], ["SkyPix"])
        self.checkSpatialJoin(["Visit", "Detector"], ["SkyPix"])
        self.checkSpatialJoin(["Visit"], ["Tract"])
        self.checkSpatialJoin(["Visit", "Detector"], ["Tract"])
        self.checkSpatialJoin(["Visit"], ["Patch"])
        self.checkSpatialJoin(["Visit", "Detector"], ["Patch"])

    def testGraphSetOperations(self):
        """Test set-like operations on DimensionGraph.

        Also provides test coverage of DimensionUnitSet, because that's what
        DimensionGraph delegates to.
        """
        # characters in the keys (interpreted as sets) have same expected
        # relationships as the corresponding values
        graphs = {
            # expands to [Detector, Instrument]
            "di": self.universe.subgraph(["Detector"]),
            # expands to [PhysicalFilter, Instrument, AbstractFilter]
            "pia": self.universe.subgraph(["PhysicalFilter"]),
            # expands to [Visit, PhysicalFilter, Instrument, AbstractFilter]
            "vpia": self.universe.subgraph(["Visit"]),
            # expands to [Tract, SkyMap]
            "ts": self.universe.subgraph(["Tract"]),
            # empty
            "": self.universe.subgraph([]),
        }
        # A big loop to test all of the combinations we can predict
        # mechanically (many of these are trivial).
        for (lhsName, lhsGraph), (rhsName, rhsGraph) in itertools.product(graphs.items(), repeat=2):
            with self.subTest(lhs=lhsName, rhs=rhsName):
                lhsChars = frozenset(lhsName)
                rhsChars = frozenset(rhsName)
                self.assertEqual(lhsChars == rhsChars, lhsGraph == rhsGraph)
                self.assertEqual(lhsChars != rhsChars, lhsGraph != rhsGraph)
                self.assertEqual(lhsChars <= rhsChars, lhsGraph <= rhsGraph)
                self.assertEqual(lhsChars >= rhsChars, lhsGraph >= rhsGraph)
                self.assertEqual(lhsChars < rhsChars, lhsGraph < rhsGraph)
                self.assertEqual(lhsChars > rhsChars, lhsGraph > rhsGraph)
                self.assertEqual(lhsChars.issubset(rhsChars), lhsGraph.issubset(rhsGraph))
                self.assertEqual(lhsChars.issuperset(rhsChars), lhsGraph.issuperset(rhsGraph))
                self.assertEqual(lhsChars.isdisjoint(rhsChars), lhsGraph.isdisjoint(rhsGraph))
                self.assertEqual(lhsGraph.intersection(rhsGraph), lhsGraph & rhsGraph)
                self.assertEqual(lhsGraph.union(rhsGraph), lhsGraph | rhsGraph)

        # A few more spot-checks for graph-creating operations to make sure
        # we get exactly what we expect in those cases.
        self.assertEqual(graphs["di"] | graphs["ts"], self.universe.subgraph(["Detector", "Tract"]))
        self.assertEqual(graphs["di"] & graphs["ts"], self.universe.subgraph([]))
        self.assertEqual(graphs["di"] | graphs["pia"],
                         self.universe.subgraph(["Detector", "PhysicalFilter"]))
        self.assertEqual(graphs["di"] & graphs["pia"], self.universe.subgraph(["Instrument"]))
        self.assertEqual(graphs["vpia"] | graphs["pia"], graphs["vpia"])
        self.assertEqual(graphs["vpia"] & graphs["pia"], graphs["pia"])

    def testDimensionJoinSetOperations(self):
        """Test set-like operations on DimensionJoinSet.

        Also provides some test coverage for DimensionUnitSet because it shares
        a common base class that provides the vast majority of this
        functionality.
        """
        # characters in the keys (interepreted as sets) have same expected
        # relationships as the corresponding values
        joins = {
            "t": DimensionJoinSet(self.universe, ["TractSkyPixJoin"]),
            "dt": DimensionJoinSet(self.universe, ["VisitDetectorSkyPixJoin", "TractSkyPixJoin"]),
            "pt": DimensionJoinSet(self.universe, ["PatchSkyPixJoin", "TractSkyPixJoin"]),
            "v": DimensionJoinSet(self.universe, ["VisitSkyPixJoin"]),
            "": DimensionJoinSet(self.universe, []),
        }
        # A big loop to test all of the combinations we can predict
        # mechanically.
        for (lhsName, lhs), (rhsName, rhs) in itertools.product(joins.items(), repeat=2):
            with self.subTest(lhs=lhsName, rhs=rhsName):
                # Make regular Python sets with the same contents; they'll be
                # sorted differently, but should otherwise behave the same.
                lhsSet = set(lhs)
                rhsSet = set(rhs)
                self.assertCountEqual(lhsSet, lhs)
                self.assertCountEqual(rhsSet, rhs)
                self.assertEqual(lhsSet == rhsSet, lhs == rhs)
                self.assertEqual(lhsSet != rhsSet, lhs != rhs)
                self.assertEqual(lhsSet <= rhsSet, lhs <= rhs)
                self.assertEqual(lhsSet >= rhsSet, lhs >= rhs)
                self.assertEqual(lhsSet < rhsSet, lhs < rhs)
                self.assertEqual(lhsSet > rhsSet, lhs > rhs)
                self.assertEqual(lhsSet.issubset(rhsSet), lhs.issubset(rhs))
                self.assertEqual(lhsSet.issuperset(rhsSet), lhs.issuperset(rhs))
                self.assertEqual(lhsSet.isdisjoint(rhsSet), lhs.isdisjoint(rhs))
                self.assertEqual(lhs.intersection(rhs), lhs & rhs)
                self.assertEqual(lhs.union(rhs), lhs | rhs)

        # A few more spot-checks for set-creating operations to make sure
        # we get exactly what we expect in those cases.
        self.assertEqual(joins["t"] | joins["pt"], joins["pt"])
        self.assertEqual(joins["t"] & joins["pt"], joins["t"])
        self.assertEqual(joins["t"] ^ joins["pt"], DimensionJoinSet(self.universe, ["PatchSkyPixJoin"]))
        self.assertEqual(joins["pt"] - joins["t"], DimensionJoinSet(self.universe, ["PatchSkyPixJoin"]))
        self.assertEqual(joins["dt"] | joins["pt"], DimensionJoinSet(self.universe,
                                                                     ["VisitDetectorSkyPixJoin",
                                                                      "TractSkyPixJoin",
                                                                      "PatchSkyPixJoin"]))
        self.assertEqual(joins["dt"] & joins["pt"], joins["t"])
        self.assertEqual(joins["dt"] ^ joins["pt"], DimensionJoinSet(self.universe,
                                                                     ["VisitDetectorSkyPixJoin",
                                                                      "PatchSkyPixJoin"]))
        self.assertEqual(joins["t"] | joins[""], joins["t"])
        self.assertEqual(joins["t"] & joins[""], joins[""])
        self.assertEqual(joins["t"] ^ joins[""], joins["t"])
        self.assertEqual(joins["t"] - joins[""], joins["t"])
        self.assertEqual(joins["t"] | joins["v"], DimensionJoinSet(self.universe,
                                                                   ["TractSkyPixJoin", "VisitSkyPixJoin"]))
        self.assertEqual(joins["t"] & joins["v"], joins[""])
        self.assertEqual(joins["t"] ^ joins["v"], DimensionJoinSet(self.universe,
                                                                   ["TractSkyPixJoin", "VisitSkyPixJoin"]))
        self.assertEqual(joins["t"] - joins["v"], joins["t"])


if __name__ == "__main__":
    unittest.main()

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

from lsst.daf.butler import DimensionUniverse, Dimension, DimensionJoin


class DimensionTestCase(unittest.TestCase):
    """Tests for dimensions.

    All tests here rely on the content of ``config/dimensions.yaml``, either
    to test that the definitions there are read in properly or just as generic
    data for testing various operations.
    """

    def setUp(self):
        self.universe = DimensionUniverse.fromConfig()

    def checkSetInvariants(self, dimensions):
        """Run tests on DimensionSet that should pass for any instance.
        """
        # DimensionSet should be interoperable with regular sets of
        # DimensionElements and regular sets of their names
        self.assertEqual(dimensions, set(dimensions))
        self.assertEqual(dimensions, set(dimensions.names))
        self.assertLessEqual(dimensions, set(dimensions))
        self.assertLessEqual(dimensions, set(dimensions.names))
        self.assertGreaterEqual(dimensions, set(dimensions))
        self.assertGreaterEqual(dimensions, set(dimensions.names))
        self.assertFalse(dimensions < set(dimensions))
        self.assertFalse(dimensions < set(dimensions.names))
        self.assertFalse(dimensions > set(dimensions))
        self.assertFalse(dimensions > set(dimensions.names))

        copy2 = dimensions.union(dimensions)
        copy3 = dimensions.intersection(dimensions)
        for name, copy in [("union", copy2), ("intersection", copy3)]:
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

    def checkGraphInvariants(self, graph):
        """Run tests on DimensionGraph that should pass for any instance.
        """
        self.checkSetInvariants(graph.toSet())
        self.assertLessEqual(graph, self.universe)
        for dim in graph:
            self.assertIn(dim, graph)
            self.assertIn(dim.name, graph)
            self.assertIs(graph[dim.name], dim)
            self.assertIs(graph.get(dim.name), dim)
            self.assertIsInstance(dim, Dimension)
            self.assertEqual(dim.dependencies(), dim.dependencies(implied=False))
            self.assertLessEqual(dim.dependencies(), dim.dependencies(implied=True))
            self.assertLessEqual(dim.dependencies(), graph)

        self.checkSetInvariants(graph.joins(summaries=True))
        for join in graph.joins(summaries=True):
            self.assertIsInstance(join, DimensionJoin)
            self.assertGreater(join.dependencies(), join.lhs)
            self.assertGreater(join.dependencies(), join.rhs)
            self.assertLessEqual(join.dependencies(), graph)
        self.checkSetInvariants(graph.joins(summaries=False))
        for join in graph.joins(summaries=False):
            self.assertTrue(join.summarizes.isdisjoint(graph.joins()))

        copy2 = graph.union(graph)
        copy3 = graph.intersection(graph)
        for name, copy in [("union", copy2), ("intersection", copy3)]:
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

    def testinstrumentDimensions(self):
        """Test that the instrument dimensions and joins we expect to be
        defined in ``dimensions.yaml`` are present and related correctly.
        """
        graph1 = self.universe.extract(
            dim for dim in self.universe if "instrument" in dim.dependencies()
        )
        self.checkGraphInvariants(graph1)
        self.assertCountEqual(graph1.names,
                              ["instrument", "detector", "physical_filter", "visit", "exposure",
                               "calibration_label"])
        self.assertCountEqual(graph1.joins().names, ["visit_detector_region",
                                                     "exposure_calibration_label_join"])
        self.assertEqual(graph1.getRegionHolder(), graph1.joins().get("visit_detector_region"))
        graph2 = graph1.intersection(["visit"])
        self.assertCountEqual(graph2.names, ["instrument", "visit"])
        self.assertEqual(graph2.getRegionHolder(), graph1["visit"])
        self.assertCountEqual(graph2.joins().names, [])
        self.checkGraphInvariants(graph2)
        graph3 = graph1.intersection(["detector"])
        self.checkGraphInvariants(graph3)
        self.assertCountEqual(graph3.names, ["instrument", "detector"])
        self.assertIsNone(graph3.getRegionHolder())
        self.assertCountEqual(graph3.joins(), [])
        visit = self.universe["visit"]
        self.assertCountEqual(visit.dependencies(implied=True).names,
                              ["instrument", "physical_filter"])
        self.assertCountEqual(visit.dependencies(implied=False).names, ["instrument"])

    def testSkyMapDimensions(self):
        """Test that the skymap dimensions and joins we expect to be defined
        in ``dimensions.yaml`` are present and related correctly.
        """
        patchGraph = self.universe.extract(["patch"])
        self.checkGraphInvariants(patchGraph)
        self.assertCountEqual(patchGraph.names, ["skymap", "tract", "patch"])
        self.assertCountEqual(patchGraph.joins(), [])
        self.assertEqual(patchGraph.getRegionHolder(), patchGraph["patch"])
        tractGraph = patchGraph.intersection(["tract"])
        self.checkGraphInvariants(tractGraph)
        self.assertCountEqual(tractGraph.names, ["skymap", "tract"])
        self.assertEqual(tractGraph.getRegionHolder(), tractGraph["tract"])
        self.assertCountEqual(tractGraph.joins(), [])
        skyMapOnly = tractGraph.intersection(["skymap"])
        self.checkGraphInvariants(skyMapOnly)
        self.assertCountEqual(skyMapOnly.names, ["skymap"])
        self.assertIsNone(skyMapOnly.getRegionHolder())
        self.assertCountEqual(skyMapOnly.joins(), [])

    def testMiscDimensions(self):
        """Test that the miscelleneous dimensions and joins we expect to be
        defined in ``dimensions.yaml`` are present and related correctly.
        """
        def predicate(dim):
            return ()

        misc = self.universe.extract(
            dim for dim in self.universe if (
                dim.dependencies().names.isdisjoint(["instrument", "skymap"]) and
                dim.name not in ("instrument", "skymap")
            )
        )
        self.checkGraphInvariants(misc)
        self.assertCountEqual(misc.names, ["skypix", "label", "abstract_filter"])
        self.assertCountEqual(misc.joins(), [])
        self.assertEqual(misc.getRegionHolder(), misc["skypix"])

    def checkSpatialJoin(self, lhsNames, rhsNames, joinName=None):
        """Test the spatial join that relates the given dimensions.

        Parameters
        ----------
        lhsNames : `list` of `str`
            Name of the Dimensions of the left-hand side of the join.
        rhsNames : `list` of `str`
            Name of the Dimensions of the right-hand side of the join.
        joinName : `str`, implied
            Name of the DimensionJoin to be tested; if `None`, computed by
            concatenating ``lhsNames`` and ``rhsNames``.
        """
        if joinName is None:
            joinName = "{}_{}_join".format("_".join(lhsNames), "_".join(rhsNames))
        lhs = self.universe.extract(lhsNames)
        rhs = self.universe.extract(rhsNames)
        both = self.universe.extract(joins=[joinName])
        self.checkGraphInvariants(both)
        join = both.joins().get(joinName)
        self.assertIsNotNone(join)
        self.assertLess(lhs, both)
        self.assertGreater(both, rhs)
        self.assertGreaterEqual(lhs, join.lhs)  # [lr]hs has implieds, join.[lr]hs does not
        self.assertGreaterEqual(rhs, join.rhs)
        allExpectedJoins = set([join]).union(lhs.joins(summaries=False), rhs.joins(summaries=False))
        self.assertEqual(both.joins(summaries=False), allExpectedJoins)

    def testSpatialJoins(self):
        """Test that the spatial joins defined in ``dimensions.yaml`` are
        present and related correctly.
        """
        self.checkSpatialJoin(["tract"], ["skypix"])
        self.checkSpatialJoin(["patch"], ["skypix"])
        self.checkSpatialJoin(["visit"], ["skypix"])
        self.checkSpatialJoin(["visit", "detector"], ["skypix"])
        self.checkSpatialJoin(["visit"], ["tract"])
        self.checkSpatialJoin(["visit", "detector"], ["tract"])
        self.checkSpatialJoin(["visit"], ["patch"])
        self.checkSpatialJoin(["visit", "detector"], ["patch"])

    def testGraphSetOperations(self):
        """Test set-like operations on DimensionGraph.

        Also provides test coverage of DimensionSet, because that's what
        DimensionGraph delegates to.
        """
        # characters in the keys (interpreted as sets) have same expected
        # relationships as the corresponding values
        graphs = {
            # expands to [detector, instrument]
            "di": self.universe.extract(["detector"]),
            # expands to [physical_filter, instrument, abstract_filter]
            "pia": self.universe.extract(["physical_filter"], implied=True),
            # expands to [visit, physical_filter, instrument, abstract_filter]
            "vpia": self.universe.extract(["visit"], implied=True),
            # expands to [tract, skymap]
            "ts": self.universe.extract(["tract"]),
            # empty
            "": self.universe.extract([]),
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
        self.assertEqual(graphs["di"] | graphs["ts"],
                         self.universe.extract(["detector", "tract"]))
        self.assertEqual(graphs["di"] & graphs["ts"],
                         self.universe.extract([]))
        self.assertEqual(graphs["di"] | graphs["pia"],
                         self.universe.extract(["detector", "physical_filter"], implied=True))
        self.assertEqual(graphs["di"] & graphs["pia"], self.universe.extract(["instrument"]))
        self.assertEqual(graphs["vpia"] | graphs["pia"], graphs["vpia"])
        self.assertEqual(graphs["vpia"] & graphs["pia"], graphs["pia"])

    def testDimensionJoinSetOperations(self):
        """Test set-like operations on DimensionSet with joins.
        """
        # characters in the keys (interepreted as sets) have same expected
        # relationships as the corresponding values
        joins = {
            "t": self.universe.joins().intersection(["tract_skypix_join"]),
            "dt": self.universe.joins().intersection(["visit_detector_skypix_join", "tract_skypix_join"]),
            "pt": self.universe.joins().intersection(["patch_skypix_join", "tract_skypix_join"]),
            "v": self.universe.joins().intersection(["visit_skypix_join"]),
            "": self.universe.joins().intersection([]),
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
        self.assertEqual(joins["t"] ^ joins["pt"], self.universe.joins().intersection(["patch_skypix_join"]))
        self.assertEqual(joins["pt"] - joins["t"], self.universe.joins().intersection(["patch_skypix_join"]))
        self.assertEqual(joins["dt"] | joins["pt"],
                         self.universe.joins().intersection(["visit_detector_skypix_join",
                                                             "tract_skypix_join",
                                                             "patch_skypix_join"]))
        self.assertEqual(joins["dt"] & joins["pt"], joins["t"])
        self.assertEqual(joins["dt"] ^ joins["pt"],
                         self.universe.joins().intersection(["visit_detector_skypix_join",
                                                             "patch_skypix_join"]))
        self.assertEqual(joins["t"] | joins[""], joins["t"])
        self.assertEqual(joins["t"] & joins[""], joins[""])
        self.assertEqual(joins["t"] ^ joins[""], joins["t"])
        self.assertEqual(joins["t"] - joins[""], joins["t"])
        self.assertEqual(joins["t"] | joins["v"],
                         self.universe.joins().intersection(["tract_skypix_join", "visit_skypix_join"]))
        self.assertEqual(joins["t"] & joins["v"], joins[""])
        self.assertEqual(joins["t"] ^ joins["v"],
                         self.universe.joins().intersection(["tract_skypix_join", "visit_skypix_join"]))
        self.assertEqual(joins["t"] - joins["v"], joins["t"])


if __name__ == "__main__":
    unittest.main()

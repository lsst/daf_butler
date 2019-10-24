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
import copy
import pickle
import itertools
import re

from lsst.daf.butler.core.dimensions import DimensionUniverse, DimensionGraph, Dimension
from lsst.daf.butler.core.dimensions.schema import OVERLAP_TABLE_NAME_PATTERN


class DimensionTestCase(unittest.TestCase):
    """Tests for dimensions.

    All tests here rely on the content of ``config/dimensions.yaml``, either
    to test that the definitions there are read in properly or just as generic
    data for testing various operations.
    """

    def setUp(self):
        self.universe = DimensionUniverse()

    def checkGraphInvariants(self, graph):
        elements = list(graph.elements)
        for n, element in enumerate(elements):
            # Ordered comparisons on graphs behave like sets.
            self.assertLessEqual(element.graph, graph)
            # Ordered comparisons on elements correspond to the ordering within
            # a DimensionUniverse (topological, with deterministic
            # tiebreakers).
            for other in elements[:n]:
                self.assertLess(other, element)
                self.assertLessEqual(other, element)
            for other in elements[n + 1:]:
                self.assertGreater(other, element)
                self.assertGreaterEqual(other, element)
        self.assertEqual(DimensionGraph(self.universe, graph.required), graph)
        self.assertCountEqual(graph.required,
                              [dimension for dimension in graph.dimensions
                               if not any(dimension in other.graph.implied for other in graph.elements)])
        self.assertCountEqual(graph.implied, graph.dimensions - graph.required)
        self.assertCountEqual(graph.dimensions,
                              [element for element in graph.elements
                               if isinstance(element, Dimension)])
        self.assertCountEqual(graph.dimensions, itertools.chain(graph.required, graph.implied))

    def testConfigRead(self):
        self.assertEqual(self.universe.dimensions.names,
                         {"instrument", "visit", "exposure", "detector", "physical_filter",
                          "abstract_filter", "calibration_label", "skymap", "tract", "patch", "htm7", "htm9"})

    def testGraphs(self):
        self.checkGraphInvariants(self.universe.empty)
        self.checkGraphInvariants(self.universe)
        for element in self.universe.elements:
            self.checkGraphInvariants(element.graph)

    def testInstrumentDimensions(self):
        graph = DimensionGraph(self.universe, names=("exposure", "detector", "calibration_label"))
        self.assertCountEqual(graph.dimensions.names,
                              ("instrument", "exposure", "detector", "calibration_label",
                               "visit", "physical_filter", "abstract_filter"))
        self.assertCountEqual(graph.required.names, ("instrument", "exposure", "detector",
                                                     "calibration_label"))
        self.assertCountEqual(graph.implied.names, ("visit", "physical_filter", "abstract_filter"))
        self.assertCountEqual(graph.elements.names - graph.dimensions.names, ("visit_detector_region",))

    def testCalibrationDimensions(self):
        graph = DimensionGraph(self.universe, names=("calibration_label", "physical_filter", "detector"))
        self.assertCountEqual(graph.dimensions.names,
                              ("instrument", "detector", "calibration_label",
                               "physical_filter", "abstract_filter"))
        self.assertCountEqual(graph.required.names, ("instrument", "detector", "calibration_label",
                                                     "physical_filter"))
        self.assertCountEqual(graph.implied.names, ("abstract_filter",))
        self.assertCountEqual(graph.elements.names, graph.dimensions.names)

    def testObservationDimensions(self):
        graph = DimensionGraph(self.universe, names=("exposure", "detector"))
        self.assertCountEqual(graph.dimensions.names, ("instrument", "detector", "visit", "exposure",
                                                       "physical_filter", "abstract_filter"))
        self.assertCountEqual(graph.required.names, ("instrument", "detector", "exposure"))
        self.assertCountEqual(graph.implied.names, ("physical_filter", "abstract_filter", "visit"))
        self.assertCountEqual(graph.elements.names - graph.dimensions.names, ("visit_detector_region",))
        self.assertCountEqual(graph.spatial.names, ("visit_detector_region",))
        self.assertCountEqual(graph.getSpatial(independent=False).names,
                              ("visit", "visit_detector_region",))
        self.assertCountEqual(graph.getSpatial(prefer=("visit",)).names, ("visit",))
        self.assertCountEqual(graph.getTemporal(independent=False).names, ("visit", "exposure"))
        self.assertCountEqual(graph.temporal.names, ("exposure",))
        self.assertCountEqual(graph.getTemporal(prefer=("visit",)).names, ("visit",))

    def testSkyMapDimensions(self):
        graph = DimensionGraph(self.universe, names=("patch",))
        self.assertCountEqual(graph.dimensions.names, ("skymap", "tract", "patch"))
        self.assertCountEqual(graph.required.names, ("skymap", "tract", "patch"))
        self.assertCountEqual(graph.implied.names, ())
        self.assertCountEqual(graph.elements.names, graph.dimensions.names)
        self.assertCountEqual(graph.spatial.names, ("patch",))
        self.assertCountEqual(graph.getSpatial(independent=False).names, ("patch", "tract"))
        self.assertCountEqual(graph.getSpatial(prefer=("tract",)).names, ("tract",))

    def testSubsetCalculation(self):
        """Test that independent spatial and temporal options are computed
        correctly.
        """
        graph = DimensionGraph(self.universe, names=("visit", "detector", "tract", "patch", "htm7",
                                                     "exposure", "calibration_label"))
        self.assertCountEqual(graph.spatial.names,
                              ("visit_detector_region", "patch", "htm7"))
        self.assertCountEqual(graph.getSpatial(independent=False).names,
                              ("visit_detector_region", "patch", "htm7", "visit", "tract"))
        self.assertCountEqual(graph.getSpatial(prefer=["tract"]).names,
                              ("visit_detector_region", "tract", "htm7"))
        self.assertCountEqual(graph.temporal.names,
                              ("exposure", "calibration_label"))
        self.assertCountEqual(graph.getTemporal(independent=False).names,
                              ("visit", "exposure", "calibration_label"))
        self.assertCountEqual(graph.getTemporal(prefer=["visit"]).names,
                              ("visit", "calibration_label"))

    def testSchemaGeneration(self):
        overlapTableRegex = re.compile(
            OVERLAP_TABLE_NAME_PATTERN.format("(.+)", self.universe.commonSkyPix.name)
        )
        tableSpecs = self.universe.makeSchemaSpec()
        for tableName, tableSpec in tableSpecs.items():
            element = self.universe.get(tableName)
            if element is None:
                elementName = overlapTableRegex.match(tableName)[1]
                element = self.universe[elementName]
                for dep in list(element.graph.required) + [self.universe.commonSkyPix]:
                    with self.subTest(element=element.name, dep=dep.name):
                        self.assertIn(dep.name, tableSpec.fields)
                        self.assertEqual(tableSpec.fields[dep.name].dtype, dep.primaryKey.dtype)
                        self.assertEqual(tableSpec.fields[dep.name].length, dep.primaryKey.length)
                        self.assertEqual(tableSpec.fields[dep.name].nbytes, dep.primaryKey.nbytes)
                        self.assertFalse(tableSpec.fields[dep.name].nullable)
                        self.assertTrue(tableSpec.fields[dep.name].primaryKey)
            else:
                for dep in element.graph.required:
                    with self.subTest(element=element.name, dep=dep.name):
                        if dep != element:
                            self.assertIn(dep.name, tableSpec.fields)
                            self.assertEqual(tableSpec.fields[dep.name].dtype, dep.primaryKey.dtype)
                            self.assertEqual(tableSpec.fields[dep.name].length, dep.primaryKey.length)
                            self.assertEqual(tableSpec.fields[dep.name].nbytes, dep.primaryKey.nbytes)
                            self.assertFalse(tableSpec.fields[dep.name].nullable)
                            self.assertTrue(tableSpec.fields[dep.name].primaryKey)
                        else:
                            self.assertIn(element.primaryKey.name, tableSpec.fields)
                            self.assertEqual(tableSpec.fields[element.primaryKey.name].dtype,
                                             dep.primaryKey.dtype)
                            self.assertEqual(tableSpec.fields[element.primaryKey.name].length,
                                             dep.primaryKey.length)
                            self.assertEqual(tableSpec.fields[element.primaryKey.name].nbytes,
                                             dep.primaryKey.nbytes)
                            self.assertFalse(tableSpec.fields[element.primaryKey.name].nullable)
                            self.assertTrue(tableSpec.fields[element.primaryKey.name].primaryKey)
                for dep in element.implied:
                    with self.subTest(element=element.name, dep=dep.name):
                        self.assertIn(dep.name, tableSpec.fields)
                        self.assertEqual(tableSpec.fields[dep.name].dtype, dep.primaryKey.dtype)
                        self.assertFalse(tableSpec.fields[dep.name].primaryKey)
                for foreignKey in tableSpec.foreignKeys:
                    self.assertIn(foreignKey.table, tableSpecs)
                    self.assertIn(foreignKey.table, element.graph.dimensions.names)
                    self.assertEqual(len(foreignKey.source), len(foreignKey.target))
                    for source, target in zip(foreignKey.source, foreignKey.target):
                        self.assertIn(source, tableSpec.fields.names)
                        self.assertIn(target, tableSpecs[foreignKey.table].fields.names)
                        self.assertEqual(tableSpec.fields[source].dtype,
                                         tableSpecs[foreignKey.table].fields[target].dtype)
                        self.assertEqual(tableSpec.fields[source].length,
                                         tableSpecs[foreignKey.table].fields[target].length)
                        self.assertEqual(tableSpec.fields[source].nbytes,
                                         tableSpecs[foreignKey.table].fields[target].nbytes)
                self.assertEqual(tuple(tableSpec.fields.names), element.RecordClass.__slots__)

    def testPickling(self):
        # Pickling and copying should always yield the exact same object within
        # a single process (cross-process is impossible to test here).
        universe1 = DimensionUniverse()
        universe2 = pickle.loads(pickle.dumps(universe1))
        universe3 = copy.copy(universe1)
        universe4 = copy.deepcopy(universe1)
        self.assertIs(universe1, universe2)
        self.assertIs(universe1, universe3)
        self.assertIs(universe1, universe4)
        for element1 in universe1.elements:
            element2 = pickle.loads(pickle.dumps(element1))
            self.assertIs(element1, element2)
            graph1 = element1.graph
            graph2 = pickle.loads(pickle.dumps(graph1))
            self.assertIs(graph1, graph2)


if __name__ == "__main__":
    unittest.main()

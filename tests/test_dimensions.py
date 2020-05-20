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

from lsst.daf.butler import (
    Dimension,
    DimensionGraph,
    DimensionUniverse,
    makeDimensionElementTableSpec,
    NamedKeyDict,
    NamedValueSet,
)


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
            if isinstance(element, Dimension):
                self.assertEqual(element.graph.required, element.required)
        self.assertEqual(DimensionGraph(self.universe, graph.required), graph)
        self.assertCountEqual(graph.required,
                              [dimension for dimension in graph.dimensions
                               if not any(dimension in other.graph.implied for other in graph.elements)])
        self.assertCountEqual(graph.implied, graph.dimensions - graph.required)
        self.assertCountEqual(graph.dimensions,
                              [element for element in graph.elements
                               if isinstance(element, Dimension)])
        self.assertCountEqual(graph.dimensions, itertools.chain(graph.required, graph.implied))
        # Check primary key traversal order: each element should follow any it
        # requires, and element that is implied by any other in the graph
        # follow at least one of those.
        seen = NamedValueSet()
        for element in graph.primaryKeyTraversalOrder:
            with self.subTest(required=graph.required, implied=graph.implied, element=element):
                seen.add(element)
                self.assertLessEqual(element.graph.required, seen)
                if element in graph.implied:
                    self.assertTrue(any(element in s.implied for s in seen))
        self.assertCountEqual(seen, graph.elements)
        # Test encoding and decoding of DimensionGraphs to bytes.
        encoded = graph.encode()
        self.assertEqual(len(encoded), self.universe.getEncodeLength())
        self.assertEqual(DimensionGraph.decode(encoded, universe=self.universe), graph)

    def testConfigRead(self):
        self.assertEqual(self.universe.dimensions.names,
                         {"instrument", "visit", "visit_system", "exposure", "detector",
                          "physical_filter", "abstract_filter", "subfilter", "calibration_label",
                          "skymap", "tract", "patch", "htm7", "htm9"})

    def testGraphs(self):
        self.checkGraphInvariants(self.universe.empty)
        self.checkGraphInvariants(self.universe)
        for element in self.universe.elements:
            self.checkGraphInvariants(element.graph)

    def testInstrumentDimensions(self):
        graph = DimensionGraph(self.universe, names=("exposure", "detector", "visit", "calibration_label"))
        self.assertCountEqual(graph.dimensions.names,
                              ("instrument", "exposure", "detector", "calibration_label",
                               "visit", "physical_filter", "abstract_filter", "visit_system"))
        self.assertCountEqual(graph.required.names, ("instrument", "exposure", "detector",
                                                     "calibration_label", "visit"))
        self.assertCountEqual(graph.implied.names, ("physical_filter", "abstract_filter", "visit_system"))
        self.assertCountEqual(graph.elements.names - graph.dimensions.names,
                              ("visit_detector_region", "visit_definition"))

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
        graph = DimensionGraph(self.universe, names=("exposure", "detector", "visit"))
        self.assertCountEqual(graph.dimensions.names, ("instrument", "detector", "visit", "exposure",
                                                       "physical_filter", "abstract_filter", "visit_system"))
        self.assertCountEqual(graph.required.names, ("instrument", "detector", "exposure", "visit"))
        self.assertCountEqual(graph.implied.names, ("physical_filter", "abstract_filter", "visit_system"))
        self.assertCountEqual(graph.elements.names - graph.dimensions.names,
                              ("visit_detector_region", "visit_definition"))
        self.assertCountEqual(graph.spatial.names, ("visit_detector_region",))
        self.assertCountEqual(graph.temporal.names, ("exposure",))

    def testSkyMapDimensions(self):
        graph = DimensionGraph(self.universe, names=("patch",))
        self.assertCountEqual(graph.dimensions.names, ("skymap", "tract", "patch"))
        self.assertCountEqual(graph.required.names, ("skymap", "tract", "patch"))
        self.assertCountEqual(graph.implied.names, ())
        self.assertCountEqual(graph.elements.names, graph.dimensions.names)
        self.assertCountEqual(graph.spatial.names, ("patch",))

    def testSubsetCalculation(self):
        """Test that independent spatial and temporal options are computed
        correctly.
        """
        graph = DimensionGraph(self.universe, names=("visit", "detector", "tract", "patch", "htm7",
                                                     "exposure", "calibration_label"))
        self.assertCountEqual(graph.spatial.names,
                              ("visit_detector_region", "patch", "htm7"))
        self.assertCountEqual(graph.temporal.names,
                              ("exposure", "calibration_label"))

    def testSchemaGeneration(self):
        tableSpecs = NamedKeyDict({})
        for element in self.universe.elements:
            if element.hasTable and element.viewOf is None:
                tableSpecs[element] = makeDimensionElementTableSpec(element)
        for element, tableSpec in tableSpecs.items():
            for dep in element.required:
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

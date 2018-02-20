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

import lsst.utils.tests

from lsst.daf.butler.datastores.posixDatastore import PosixDatastore, DatastoreConfig
from examplePythonTypes import MetricsExample


def makeExampleMetrics():
    return MetricsExample({"AM1": 5.2, "AM2": 30.6},
                          {"a": [1, 2, 3],
                           "b": {"blue": 5, "red": "green"}},
                          [563, 234, 456.7]
                          )


class PosixDatastoreTestCase(lsst.utils.tests.TestCase):
    """Some basic tests of a simple POSIX datastore."""

    def assertEqualMetrics(self, first, second):
        self.assertListEqual(first.data, second.data)
        self.assertDictEqual(first.summary, second.summary)
        self.assertDictEqual(first.output, second.output)

    def setUp(self):
        self.testDir = os.path.dirname(__file__)
        self.configFile = os.path.join(self.testDir, "config/basic/butler.yaml")

    def testConstructor(self):
        datastore = PosixDatastore(config=self.configFile)
        self.assertIsNotNone(datastore)

    def testBasicPutGet(self):
        metrics = makeExampleMetrics()
        datastore = PosixDatastore(config=self.configFile)
        # Put
        storageClass = datastore.storageClassFactory.getStorageClass("StructuredData")
        uri, comps = datastore.put(metrics, storageClass=storageClass, storageHint="tester.json",
                                   typeName=None)

        # Get
        metricsOut = datastore.get(uri, storageClass=storageClass, parameters=None)
        self.assertEqualMetrics(metrics, metricsOut)

        # Get a component
        summary = datastore.get(comps["output"], storageClass=storageClass)
        self.assertEqual(summary, metricsOut.output)

        # Get a component even though it was written without by forming URI ourselves
        summary = datastore.get("{}#{}".format(uri, "summary"), storageClass=storageClass)
        self.assertEqual(summary, metricsOut.summary)

        # These should raise
        with self.assertRaises(ValueError):
            # non-existing file
            datastore.get(uri="file:///non_existing.json", storageClass=storageClass, parameters=None)
        with self.assertRaises(ValueError):
            # invalid storage class
            datastore.get(uri="file:///non_existing.json", storageClass=object, parameters=None)
        with self.assertRaises(ValueError):
            # Missing component
            datastore.get("{}#{}".format(uri, "missing"), storageClass=storageClass)
        with self.assertRaises(TypeError):
            # Retrieve component of mismatched type
            datastore.get("{}#{}".format(uri, "data"), storageClass=storageClass)

    def testCompositePutGet(self):
        metrics = makeExampleMetrics()
        datastore = PosixDatastore(config=self.configFile)
        # Put
        storageClass = datastore.storageClassFactory.getStorageClass("StructuredComposite")
        uri, comps = datastore.put(metrics, storageClass=storageClass, storageHint="testerc.json",
                                   typeName=None)
        self.assertIsNone(uri)

        # Read all the components into a dict
        components = {}
        for c, u in comps.items():
            components[c] = datastore.get(u, storageClass=storageClass.components[c], parameters=None)

        # combine them into a new metrics composite object
        metricsOut = storageClass.assembler().assemble(components)
        self.assertEqualMetrics(metrics, metricsOut)

    def testRemove(self):
        metrics = makeExampleMetrics()
        datastore = PosixDatastore(config=self.configFile)
        # Put
        storageClass = datastore.storageClassFactory.getStorageClass("StructuredData")
        uri, _ = datastore.put(metrics, storageClass=storageClass, storageHint="tester.json", typeName=None)
        # Get
        metricsOut = datastore.get(uri, storageClass=storageClass, parameters=None)
        self.assertEqualMetrics(metrics, metricsOut)
        # Remove
        datastore.remove(uri)
        # Get should now fail
        with self.assertRaises(ValueError):
            datastore.get(uri, storageClass=storageClass, parameters=None)
        # Can only delete once
        with self.assertRaises(FileNotFoundError):
            datastore.remove(uri)

    def testTransfer(self):
        metrics = makeExampleMetrics()
        path = "tester.json"
        inputConfig = DatastoreConfig(self.configFile)
        inputConfig['datastore.root'] = os.path.join(self.testDir, "./test_input_datastore")
        inputPosixDatastore = PosixDatastore(config=inputConfig)
        outputConfig = inputConfig.copy()
        outputConfig['datastore.root'] = os.path.join(self.testDir, "./test_output_datastore")
        outputPosixDatastore = PosixDatastore(config=outputConfig)
        storageClass = outputPosixDatastore.storageClassFactory.getStorageClass("StructuredData")
        inputUri, _ = inputPosixDatastore.put(metrics, storageClass, path)
        outputUri, _ = outputPosixDatastore.transfer(inputPosixDatastore, inputUri, storageClass, path)
        metricsOut = outputPosixDatastore.get(outputUri, storageClass)
        self.assertEqualMetrics(metrics, metricsOut)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()

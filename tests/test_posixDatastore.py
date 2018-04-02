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

from lsst.daf.butler import StorageClassFactory
from lsst.daf.butler.datastores.posixDatastore import PosixDatastore, DatastoreConfig

from datasetsHelper import DatasetTestHelper
from examplePythonTypes import MetricsExample

from dummyRegistry import DummyRegistry


def makeExampleMetrics():
    return MetricsExample({"AM1": 5.2, "AM2": 30.6},
                          {"a": [1, 2, 3],
                           "b": {"blue": 5, "red": "green"}},
                          [563, 234, 456.7]
                          )


class PosixDatastoreTestCase(lsst.utils.tests.TestCase, DatasetTestHelper):
    """Some basic tests of a simple POSIX datastore."""

    @classmethod
    def setUpClass(cls):
        cls.testDir = os.path.dirname(__file__)
        cls.storageClassFactory = StorageClassFactory()
        cls.configFile = os.path.join(cls.testDir, "config/basic/butler.yaml")
        cls.storageClassFactory.addFromConfig(cls.configFile)

    def setUp(self):
        self.registry = DummyRegistry()

        # Need to keep ID for each datasetRef since we have no butler
        # for these tests
        self.id = 1

    def testConstructor(self):
        datastore = PosixDatastore(config=self.configFile, registry=self.registry)
        self.assertIsNotNone(datastore)

    def testBasicPutGet(self):
        metrics = makeExampleMetrics()
        datastore = PosixDatastore(config=self.configFile, registry=self.registry)

        # Create multiple storage classes for testing different formulations
        storageClasses = [self.storageClassFactory.getStorageClass(sc)
                          for sc in ("StructuredData",
                                     "StructuredDataJson",
                                     "StructuredDataPickle")]

        dataUnits = frozenset(("visit", "filter"))
        dataId = {"visit": 52, "filter": "V"}

        for sc in storageClasses:
            ref = self.makeDatasetRef("metric", dataUnits, sc, dataId)
            print("Using storageClass: {}".format(sc.name))
            datastore.put(metrics, ref)

            # Does it exist?
            self.assertTrue(datastore.exists(ref))

            # Get
            metricsOut = datastore.get(ref, parameters=None)
            self.assertEqual(metrics, metricsOut)

            uri = datastore.getUri(ref)
            self.assertEqual(uri[:5], "file:")

            # Get a component -- we need to construct new refs for them
            # with derived storage classes but with parent ID
            comp = "output"
            compRef = self.makeDatasetRef(ref.datasetType.componentTypeName(comp), dataUnits,
                                          sc.components[comp], dataId, id=ref.id)
            output = datastore.get(compRef)
            self.assertEqual(output, metricsOut.output)

            uri = datastore.getUri(compRef)
            self.assertEqual(uri[:5], "file:")

        storageClass = sc

        # These should raise
        ref = self.makeDatasetRef("metrics", dataUnits, storageClass, dataId, id=10000)
        with self.assertRaises(FileNotFoundError):
            # non-existing file
            datastore.get(ref)

        # Get a URI from it
        uri = datastore.getUri(ref, predict=True)
        self.assertEqual(uri[:5], "file:")

        with self.assertRaises(FileNotFoundError):
            datastore.getUri(ref)

    def testCompositePutGet(self):
        metrics = makeExampleMetrics()
        datastore = PosixDatastore(config=self.configFile, registry=self.registry)

        # Create multiple storage classes for testing different formulations
        # of composites
        storageClasses = [self.storageClassFactory.getStorageClass(sc)
                          for sc in ("StructuredComposite",
                                     "StructuredCompositeTestA",
                                     "StructuredCompositeTestB")]

        dataUnits = frozenset(("visit", "filter"))
        dataId = {"visit": 428, "filter": "R"}

        for sc in storageClasses:
            print("Using storageClass: {}".format(sc.name))
            ref = self.makeDatasetRef("metric", dataUnits, sc, dataId)

            components = sc.assembler().disassemble(metrics)
            self.assertTrue(components)

            compsRead = {}
            for compName, compInfo in components.items():
                compRef = self.makeDatasetRef(ref.datasetType.componentTypeName(compName), dataUnits,
                                              components[compName].storageClass, dataId)

                print("Writing component {} with {}".format(compName, compRef.datasetType.storageClass.name))
                datastore.put(compInfo.component, compRef)

                uri = datastore.getUri(compRef)
                self.assertEqual(uri[:5], "file:")

                compsRead[compName] = datastore.get(compRef)

            # combine all the components we read back into a new composite
            metricsOut = sc.assembler().assemble(compsRead)
            self.assertEqual(metrics, metricsOut)

    def testRemove(self):
        metrics = makeExampleMetrics()
        datastore = PosixDatastore(config=self.configFile, registry=self.registry)
        # Put
        dataUnits = frozenset(("visit", "filter"))
        dataId = {"visit": 638, "filter": "U"}

        sc = self.storageClassFactory.getStorageClass("StructuredData")
        ref = self.makeDatasetRef("metric", dataUnits, sc, dataId)
        datastore.put(metrics, ref)

        # Does it exist?
        self.assertTrue(datastore.exists(ref))

        # Get
        metricsOut = datastore.get(ref)
        self.assertEqual(metrics, metricsOut)
        # Remove
        datastore.remove(ref)

        # Does it exist?
        self.assertFalse(datastore.exists(ref))

        # Do we now get a predicted URI?
        uri = datastore.getUri(ref, predict=True)
        self.assertTrue(uri.endswith("#predicted"))

        # Get should now fail
        with self.assertRaises(FileNotFoundError):
            datastore.get(ref)
        # Can only delete once
        with self.assertRaises(FileNotFoundError):
            datastore.remove(ref)

    def testTransfer(self):
        metrics = makeExampleMetrics()

        dataUnits = frozenset(("visit", "filter"))
        dataId = {"visit": 2048, "filter": "Uprime"}

        sc = self.storageClassFactory.getStorageClass("StructuredData")
        ref = self.makeDatasetRef("metric", dataUnits, sc, dataId)

        inputConfig = DatastoreConfig(self.configFile)
        inputConfig['datastore.root'] = os.path.join(self.testDir, "./test_input_datastore")
        inputPosixDatastore = PosixDatastore(config=inputConfig, registry=self.registry)
        outputConfig = inputConfig.copy()
        outputConfig['datastore.root'] = os.path.join(self.testDir, "./test_output_datastore")
        outputPosixDatastore = PosixDatastore(config=outputConfig,
                                              registry=DummyRegistry())

        inputPosixDatastore.put(metrics, ref)
        outputPosixDatastore.transfer(inputPosixDatastore, ref)

        metricsOut = outputPosixDatastore.get(ref)
        self.assertEqual(metrics, metricsOut)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()

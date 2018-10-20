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

"""Tests for Butler.
"""

import os
import unittest
import tempfile
import shutil
import pickle

import lsst.utils.tests

from lsst.daf.butler import Butler, Config
from lsst.daf.butler import StorageClassFactory
from lsst.daf.butler import DatasetType, DatasetRef
from examplePythonTypes import MetricsExample

TESTDIR = os.path.abspath(os.path.dirname(__file__))


def makeExampleMetrics():
    return MetricsExample({"AM1": 5.2, "AM2": 30.6},
                          {"a": [1, 2, 3],
                           "b": {"blue": 5, "red": "green"}},
                          [563, 234, 456.7, 752, 8, 9, 27]
                          )


class TransactionTestError(Exception):
    """Specific error for testing transactions, to prevent misdiagnosing
    that might otherwise occur when a standard exception is used.
    """
    pass


class ButlerTests:
    """Tests for Butler.
    """
    useTempRoot = True

    @staticmethod
    def addDatasetType(datasetTypeName, dataUnits, storageClass, registry):
        """Create a DatasetType and register it
        """
        datasetType = DatasetType(datasetTypeName, dataUnits, storageClass)
        registry.registerDatasetType(datasetType)
        return datasetType

    @classmethod
    def setUpClass(cls):
        cls.storageClassFactory = StorageClassFactory()
        cls.storageClassFactory.addFromConfig(cls.configFile)

    def assertGetComponents(self, butler, datasetRef, components, reference):
        datasetTypeName = datasetRef.datasetType.name
        dataId = datasetRef.dataId
        for component in components:
            compTypeName = DatasetType.nameWithComponent(datasetTypeName, component)
            result = butler.get(compTypeName, dataId)
            self.assertEqual(result, getattr(reference, component))

    def setUp(self):
        """Create a new butler root for each test."""
        if self.useTempRoot:
            self.root = tempfile.mkdtemp(dir=TESTDIR)
            Butler.makeRepo(self.root, config=Config(self.configFile))
            self.tmpConfigFile = os.path.join(self.root, "butler.yaml")
        else:
            self.root = None
            self.tmpConfigFile = self.configFile

    def tearDown(self):
        if self.root is not None and os.path.exists(self.root):
            shutil.rmtree(self.root, ignore_errors=True)

    def testConstructor(self):
        """Independent test of constructor.
        """
        butler = Butler(self.tmpConfigFile)
        self.assertIsInstance(butler, Butler)

    def testBasicPutGet(self):
        storageClass = self.storageClassFactory.getStorageClass("StructuredDataNoComponents")
        self.runPutGetTest(storageClass, "test_metric")

    def testCompositePutGetConcrete(self):
        storageClass = self.storageClassFactory.getStorageClass("StructuredData")
        self.runPutGetTest(storageClass, "test_metric")

    def testCompositePutGetVirtual(self):
        storageClass = self.storageClassFactory.getStorageClass("StructuredComposite")
        self.runPutGetTest(storageClass, "test_metric_comp")

    def runPutGetTest(self, storageClass, datasetTypeName):
        butler = Butler(self.tmpConfigFile)
        # Create and register a DatasetType
        dataUnits = ("Instrument", "Visit")

        # We can not delete datasets so for now create two so we can do
        # two puts.
        self.addDatasetType(datasetTypeName, dataUnits, storageClass, butler.registry)

        datasetTypeName2 = datasetTypeName + "2"
        self.addDatasetType(datasetTypeName2, dataUnits, storageClass, butler.registry)

        # Add a third type to test putting with a DataSetType
        datasetTypeName3 = datasetTypeName + "3"
        self.addDatasetType(datasetTypeName3, dataUnits, storageClass, butler.registry)

        # Add needed DataUnits
        butler.registry.addDataUnitEntry("Instrument", {"instrument": "DummyCamComp"})
        butler.registry.addDataUnitEntry("PhysicalFilter", {"instrument": "DummyCamComp",
                                                            "physical_filter": "d-r"})
        butler.registry.addDataUnitEntry("Visit", {"instrument": "DummyCamComp", "visit": 423,
                                                   "physical_filter": "d-r"})

        # Create and store a dataset
        metric = makeExampleMetrics()
        dataId = {"instrument": "DummyCamComp", "visit": 423}

        # Create a DatasetRef for put
        datasetType2 = butler.registry.getDatasetType(datasetTypeName2)
        ref2 = DatasetRef(datasetType2, dataId, id=None)

        datasetType3 = butler.registry.getDatasetType(datasetTypeName3)

        # Put with a preexisting id should fail
        with self.assertRaises(ValueError):
            butler.put(metric, DatasetRef(datasetType2, dataId, id=100))

        # Put the dataset once as a DatasetRef, once as a dataId, and once with a DataSetType
        for args in ((ref2,), (datasetTypeName, dataId), (datasetType3, dataId)):
            ref = butler.put(metric, *args)
            self.assertIsInstance(ref, DatasetRef)

            # Test getDirect
            metricOut = butler.getDirect(ref)
            self.assertEqual(metric, metricOut)
            # Test get
            metricOut = butler.get(ref.datasetType.name, dataId)
            self.assertEqual(metric, metricOut)
            # Test get with a datasetRef
            metricOut = butler.get(ref)
            self.assertEqual(metric, metricOut)

            # Check we can get components
            if storageClass.isComposite():
                self.assertGetComponents(butler, ref,
                                         ("summary", "data", "output"), metric)

        # Get with parameters
        stop = 4
        sliced = butler.get(ref2, parameters={"slice": slice(stop)})
        self.assertNotEqual(metric, sliced)
        self.assertEqual(metric.summary, sliced.summary)
        self.assertEqual(metric.output, sliced.output)
        self.assertEqual(metric.data[:stop], sliced.data)

        # Combining a DatasetRef with a dataId should fail
        with self.assertRaises(ValueError):
            butler.get(ref, dataId)
        # Getting with an explicit ref should fail if the id doesn't match
        with self.assertRaises(ValueError):
            butler.get(DatasetRef(ref.datasetType, ref.dataId, id=101))

        # Getting a dataset with unknown parameters should fail
        with self.assertRaises(KeyError):
            butler.get(ref, parameters={"unsupported": True})

    def testPickle(self):
        """Test pickle support.
        """
        butler = Butler(self.tmpConfigFile)
        butlerOut = pickle.loads(pickle.dumps(butler))
        self.assertIsInstance(butlerOut, Butler)
        self.assertEqual(butlerOut.config, butler.config)

    def testTransaction(self):
        butler = Butler(self.tmpConfigFile)
        datasetTypeName = "test_metric"
        dataUnits = ("Instrument", "Visit")
        dataUnitEntries = (("Instrument", {"instrument": "DummyCam"}),
                           ("PhysicalFilter", {"instrument": "DummyCam", "physical_filter": "d-r"}),
                           ("Visit", {"instrument": "DummyCam", "visit": 42, "physical_filter": "d-r"}))
        storageClass = self.storageClassFactory.getStorageClass("StructuredData")
        metric = makeExampleMetrics()
        dataId = {"instrument": "DummyCam", "visit": 42}
        with self.assertRaises(TransactionTestError):
            with butler.transaction():
                # Create and register a DatasetType
                datasetType = self.addDatasetType(datasetTypeName, dataUnits, storageClass, butler.registry)
                # Add needed DataUnits
                for name, value in dataUnitEntries:
                    butler.registry.addDataUnitEntry(name, value)
                # Store a dataset
                ref = butler.put(metric, datasetTypeName, dataId)
                self.assertIsInstance(ref, DatasetRef)
                # Test getDirect
                metricOut = butler.getDirect(ref)
                self.assertEqual(metric, metricOut)
                # Test get
                metricOut = butler.get(datasetTypeName, dataId)
                self.assertEqual(metric, metricOut)
                # Check we can get components
                self.assertGetComponents(butler, ref,
                                         ("summary", "data", "output"), metric)
                raise TransactionTestError("This should roll back the entire transaction")

        with self.assertRaises(KeyError):
            butler.registry.getDatasetType(datasetTypeName)
        for name, value in dataUnitEntries:
            self.assertIsNone(butler.registry.findDataUnitEntry(name, value))
        # Should raise KeyError for missing DatasetType
        with self.assertRaises(KeyError):
            butler.get(datasetTypeName, dataId)
        # Also check explicitly if Dataset entry is missing
        self.assertIsNone(butler.registry.find(butler.collection, datasetType, dataId))
        # Direct retrieval should not find the file in the Datastore
        with self.assertRaises(FileNotFoundError):
            butler.getDirect(ref)

    def testMakeRepo(self):
        """Test that we can write butler configuration to a new repository via
        the Butler.makeRepo interface and then instantiate a butler from the
        repo root.
        """
        # Do not run the test if we know this datastore configuration does
        # not support a file system root
        if self.fullConfigKey is None:
            return

        # Remove the file created in setUp
        os.unlink(self.tmpConfigFile)

        Butler.makeRepo(self.root, config=Config(self.configFile))
        limited = Config(self.configFile)
        butler1 = Butler(self.root, collection="ingest")
        Butler.makeRepo(self.root, standalone=True, createRegistry=False,
                        config=Config(self.configFile))
        full = Config(self.tmpConfigFile)
        butler2 = Butler(self.root, collection="ingest")
        # Butlers should have the same configuration regardless of whether
        # defaults were expanded.
        self.assertEqual(butler1.config, butler2.config)
        # Config files loaded directly should not be the same.
        self.assertNotEqual(limited, full)
        # Make sure "limited" doesn't have a few keys we know it should be
        # inheriting from defaults.
        self.assertIn(self.fullConfigKey, full)
        self.assertNotIn(self.fullConfigKey, limited)

    def testStringification(self):
        butler = Butler(self.configFile)
        if self.datastoreStr is not None:
            self.assertIn(self.datastoreStr, str(butler))
        if self.registryStr is not None:
            self.assertIn(self.registryStr, str(butler))


class PosixDatastoreButlerTestCase(ButlerTests, lsst.utils.tests.TestCase):
    """PosixDatastore specialization of a butler"""
    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")
    fullConfigKey = ".datastore.formatters"

    datastoreStr = "datastore='./butler_test_repository"
    registryStr = "registry='sqlite:///:memory:'"


class InMemoryDatastoreButlerTestCase(ButlerTests, lsst.utils.tests.TestCase):
    """InMemoryDatastore specialization of a butler"""
    configFile = os.path.join(TESTDIR, "config/basic/butler-inmemory.yaml")
    fullConfigKey = None
    useTempRoot = False
    datastoreStr = "datastore='InMemory'"
    registryStr = "registry='sqlite:///:memory:'"


class ChainedDatastoreButlerTestCase(ButlerTests, lsst.utils.tests.TestCase):
    """PosixDatastore specialization"""
    configFile = os.path.join(TESTDIR, "config/basic/butler-chained.yaml")
    fullConfigKey = ".datastore.datastores.1.formatters"
    datastoreStr = "datastore='InMemory, ./butler_test_repository, ./butler_test_repository2'"
    registryStr = "registry='sqlite:///:memory:'"


class ButlerConfigNoRunTestCase(lsst.utils.tests.TestCase):
    """Test case for butler config which does not have ``run``.
    """
    configFile = os.path.join(TESTDIR, "config/basic/butler-norun.yaml")

    def testPickle(self):
        """Test pickle support.
        """
        butler = Butler(self.configFile, run="ingest")
        butlerOut = pickle.loads(pickle.dumps(butler))
        self.assertIsInstance(butlerOut, Butler)
        self.assertEqual(butlerOut.config, butler.config)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()

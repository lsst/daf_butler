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

from lsst.daf.butler import Butler, Config
from lsst.daf.butler import StorageClassFactory
from lsst.daf.butler import DatasetType, DatasetRef
from lsst.daf.butler import FileTemplateValidationError, ValidationError
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
    def addDatasetType(datasetTypeName, dimensions, storageClass, registry):
        """Create a DatasetType and register it
        """
        datasetType = DatasetType(datasetTypeName, dimensions, storageClass)
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

        collections = butler.registry.getAllCollections()
        self.assertEqual(collections, set())

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

        # There will not be a collection yet
        collections = butler.registry.getAllCollections()
        self.assertEqual(collections, set())

        # Create and register a DatasetType
        dimensions = ("Instrument", "Visit")

        datasetType = self.addDatasetType(datasetTypeName, dimensions, storageClass, butler.registry)

        # Add needed Dimensions
        butler.registry.addDimensionEntry("Instrument", {"instrument": "DummyCamComp"})
        butler.registry.addDimensionEntry("PhysicalFilter", {"instrument": "DummyCamComp",
                                                             "physical_filter": "d-r"})
        butler.registry.addDimensionEntry("Visit", {"instrument": "DummyCamComp", "visit": 423,
                                                    "physical_filter": "d-r"})

        # Create and store a dataset
        metric = makeExampleMetrics()
        dataId = {"instrument": "DummyCamComp", "visit": 423}

        # Create a DatasetRef for put
        refIn = DatasetRef(datasetType, dataId, id=None)

        # Put with a preexisting id should fail
        with self.assertRaises(ValueError):
            butler.put(metric, DatasetRef(datasetType, dataId, id=100))

        # Put and remove the dataset once as a DatasetRef, once as a dataId,
        # and once with a DatasetType
        for args in ((refIn,), (datasetTypeName, dataId), (datasetType, dataId)):
            with self.subTest(args=args):
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

                # Remove from collection only; after that we shouldn't be able
                # to find it unless we use the dataset_id.
                butler.remove(*args, delete=False)
                with self.assertRaises(LookupError):
                    butler.datasetExists(*args)
                # If we use the output ref with the dataset_id, we should
                # still be able to load it with getDirect().
                self.assertEqual(metric, butler.getDirect(ref))

                # Reinsert into collection, then delete from Datastore *and*
                # remove from collection.
                butler.registry.associate(butler.collection, [ref])
                butler.remove(*args)
                # Lookup with original args should still fail.
                with self.assertRaises(LookupError):
                    butler.datasetExists(*args)
                # Now getDirect() should fail, too.
                with self.assertRaises(FileNotFoundError):
                    butler.getDirect(ref)
                # Registry still knows about it, if we use the dataset_id.
                self.assertEqual(butler.registry.getDataset(ref.id), ref)

                # Put again, then remove completely (this generates a new
                # dataset record in registry, with a new ID - the old one
                # still exists but it is not in any collection so we don't
                # care).
                ref = butler.put(metric, *args)
                butler.remove(*args, remember=False)
                # Lookup with original args should still fail.
                with self.assertRaises(LookupError):
                    butler.datasetExists(*args)
                # getDirect() should still fail.
                with self.assertRaises(FileNotFoundError):
                    butler.getDirect(ref)
                # Registry shouldn't be able to find it by dataset_id anymore.
                self.assertIsNone(butler.registry.getDataset(ref.id))

        # Put the dataset again, since the last thing we did was remove it.
        ref = butler.put(metric, refIn)

        # Get with parameters
        stop = 4
        sliced = butler.get(ref, parameters={"slice": slice(stop)})
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

        # Check we have a collection
        collections = butler.registry.getAllCollections()
        self.assertEqual(collections, {"ingest", })

    def testPickle(self):
        """Test pickle support.
        """
        butler = Butler(self.tmpConfigFile)
        butlerOut = pickle.loads(pickle.dumps(butler))
        self.assertIsInstance(butlerOut, Butler)
        self.assertEqual(butlerOut.config, butler.config)

    def testGetDatasetTypes(self):
        butler = Butler(self.tmpConfigFile)
        dimensions = ("Instrument", "Visit", "PhysicalFilter")
        dimensionEntries = (("Instrument", {"instrument": "DummyCam"}),
                            ("Instrument", {"instrument": "DummyHSC"}),
                            ("Instrument", {"instrument": "DummyCamComp"}),
                            ("PhysicalFilter", {"instrument": "DummyCam", "physical_filter": "d-r"}),
                            ("Visit", {"instrument": "DummyCam", "visit": 42, "physical_filter": "d-r"}))
        storageClass = self.storageClassFactory.getStorageClass("StructuredData")
        # Add needed Dimensions
        for name, value in dimensionEntries:
            butler.registry.addDimensionEntry(name, value)

        # When a DatasetType is added to the registry entries are created
        # for each component. Need entries for each component in the test
        # configuration otherwise validation won't work. The ones that
        # are deliberately broken will be ignored later.
        datasetTypeNames = {"metric", "metric2", "metric4", "metric33", "pvi"}
        components = set()
        for datasetTypeName in datasetTypeNames:
            # Create and register a DatasetType
            self.addDatasetType(datasetTypeName, dimensions, storageClass, butler.registry)

            for componentName in storageClass.components:
                components.add(DatasetType.nameWithComponent(datasetTypeName, componentName))

        fromRegistry = butler.registry.getAllDatasetTypes()
        self.assertEqual({d.name for d in fromRegistry}, datasetTypeNames | components)

        # Now that we have some dataset types registered, validate them
        butler.validateConfiguration(ignore=["test_metric_comp", "metric3", "calexp", "DummySC",
                                             "datasetType.component"])

        # Add a new datasetType that will fail template validation
        self.addDatasetType("test_metric_comp", dimensions, storageClass, butler.registry)
        if self.validationCanFail:
            with self.assertRaises(ValidationError):
                butler.validateConfiguration()

        # Rerun validation but with a subset of dataset type names
        butler.validateConfiguration(datasetTypeNames=["metric4"])

        # Rerun validation but ignore the bad datasetType
        butler.validateConfiguration(ignore=["test_metric_comp", "metric3", "calexp", "DummySC",
                                             "datasetType.component"])

    def testTransaction(self):
        butler = Butler(self.tmpConfigFile)
        datasetTypeName = "test_metric"
        dimensions = ("Instrument", "Visit")
        dimensionEntries = (("Instrument", {"instrument": "DummyCam"}),
                            ("PhysicalFilter", {"instrument": "DummyCam", "physical_filter": "d-r"}),
                            ("Visit", {"instrument": "DummyCam", "visit": 42, "physical_filter": "d-r"}))
        storageClass = self.storageClassFactory.getStorageClass("StructuredData")
        metric = makeExampleMetrics()
        dataId = {"instrument": "DummyCam", "visit": 42}
        with self.assertRaises(TransactionTestError):
            with butler.transaction():
                # Create and register a DatasetType
                datasetType = self.addDatasetType(datasetTypeName, dimensions, storageClass, butler.registry)
                # Add needed Dimensions
                for name, value in dimensionEntries:
                    butler.registry.addDimensionEntry(name, value)
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
        for name, value in dimensionEntries:
            self.assertIsNone(butler.registry.findDimensionEntry(name, value))
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

        # Collections don't appear until something is put in them
        collections1 = butler1.registry.getAllCollections()
        self.assertEqual(collections1, set())
        self.assertEqual(butler2.registry.getAllCollections(), collections1)

    def testStringification(self):
        butler = Butler(self.tmpConfigFile)
        butlerStr = str(butler)

        if self.datastoreStr is not None:
            for testStr in self.datastoreStr:
                self.assertIn(testStr, butlerStr)
        if self.registryStr is not None:
            self.assertIn(self.registryStr, butlerStr)

        datastoreName = butler.datastore.name
        if self.datastoreName is not None:
            for testStr in self.datastoreName:
                self.assertIn(testStr, datastoreName)


class PosixDatastoreButlerTestCase(ButlerTests, unittest.TestCase):
    """PosixDatastore specialization of a butler"""
    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")
    fullConfigKey = ".datastore.formatters"
    validationCanFail = True

    datastoreStr = ["/datastore"]
    datastoreName = ["POSIXDatastore@<root>/datastore"]
    registryStr = "/gen3.sqlite3'"

    def testPutTemplates(self):
        storageClass = self.storageClassFactory.getStorageClass("StructuredDataNoComponents")
        butler = Butler(self.tmpConfigFile)

        # Add needed Dimensions
        butler.registry.addDimensionEntry("Instrument", {"instrument": "DummyCamComp"})
        butler.registry.addDimensionEntry("PhysicalFilter", {"instrument": "DummyCamComp",
                                                             "physical_filter": "d-r"})
        butler.registry.addDimensionEntry("Visit", {"instrument": "DummyCamComp", "visit": 423,
                                                    "physical_filter": "d-r"})
        butler.registry.addDimensionEntry("Visit", {"instrument": "DummyCamComp", "visit": 425,
                                                    "physical_filter": "d-r"})

        # Create and store a dataset
        metric = makeExampleMetrics()

        # Create two almost-identical DatasetTypes (both will use default
        # template)
        dimensions = ("Instrument", "Visit")
        butler.registry.registerDatasetType(DatasetType("metric1", dimensions, storageClass))
        butler.registry.registerDatasetType(DatasetType("metric2", dimensions, storageClass))
        butler.registry.registerDatasetType(DatasetType("metric3", dimensions, storageClass))

        dataId1 = {"instrument": "DummyCamComp", "visit": 423}
        dataId2 = {"instrument": "DummyCamComp", "visit": 423, "physical_filter": "d-r"}
        dataId3 = {"instrument": "DummyCamComp", "visit": 425}

        # Put with exactly the data ID keys needed
        ref = butler.put(metric, "metric1", dataId1)
        self.assertTrue(os.path.exists(os.path.join(butler.datastore.root,
                                                    "ingest/metric1/DummyCamComp_423.pickle")))

        # Check the template based on dimensions
        butler.datastore.templates.validateTemplates([ref])

        # Put with extra data ID keys (physical_filter is an optional
        # dependency); should not change template (at least the way we're
        # defining them  to behave now; the important thing is that they
        # must be consistent).
        ref = butler.put(metric, "metric2", dataId2)
        self.assertTrue(os.path.exists(os.path.join(butler.datastore.root,
                                                    "ingest/metric2/DummyCamComp_423.pickle")))

        # Check the template based on dimensions
        butler.datastore.templates.validateTemplates([ref])

        # Now use a file template that will not result in unique filenames
        ref = butler.put(metric, "metric3", dataId1)

        # Check the template based on dimensions. This one is a bad template
        with self.assertRaises(FileTemplateValidationError):
            butler.datastore.templates.validateTemplates([ref])

        with self.assertRaises(FileExistsError):
            butler.put(metric, "metric3", dataId3)


class InMemoryDatastoreButlerTestCase(ButlerTests, unittest.TestCase):
    """InMemoryDatastore specialization of a butler"""
    configFile = os.path.join(TESTDIR, "config/basic/butler-inmemory.yaml")
    fullConfigKey = None
    useTempRoot = False
    validationCanFail = False
    datastoreStr = ["datastore='InMemory'"]
    datastoreName = ["InMemoryDatastore@"]
    registryStr = "registry='sqlite:///:memory:'"


class ChainedDatastoreButlerTestCase(ButlerTests, unittest.TestCase):
    """PosixDatastore specialization"""
    configFile = os.path.join(TESTDIR, "config/basic/butler-chained.yaml")
    fullConfigKey = ".datastore.datastores.1.formatters"
    validationCanFail = True
    datastoreStr = ["datastore='InMemory", "/datastore/PosixDatastore_1,", "/datastore/PosixDatastore_2'"]
    datastoreName = ["InMemoryDatastore@", "POSIXDatastore@<root>/datastore/PosixDatastore_1",
                     "SecondDatastore"]
    registryStr = "/gen3.sqlite3'"


class ButlerConfigNoRunTestCase(unittest.TestCase):
    """Test case for butler config which does not have ``run``.
    """
    configFile = os.path.join(TESTDIR, "config/basic/butler-norun.yaml")

    def testPickle(self):
        """Test pickle support.
        """
        self.root = tempfile.mkdtemp(dir=TESTDIR)
        Butler.makeRepo(self.root, config=Config(self.configFile))
        self.tmpConfigFile = os.path.join(self.root, "butler.yaml")
        butler = Butler(self.tmpConfigFile, run="ingest")
        butlerOut = pickle.loads(pickle.dumps(butler))
        self.assertIsInstance(butlerOut, Butler)
        self.assertEqual(butlerOut.config, butler.config)

    def tearDown(self):
        if self.root is not None and os.path.exists(self.root):
            shutil.rmtree(self.root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()

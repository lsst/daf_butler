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
import posixpath
import unittest
import tempfile
import shutil
import pickle
import string
import random

try:
    import boto3
    import botocore
    from moto import mock_s3
except ImportError:
    boto3 = None

    def mock_s3(cls):
        """A no-op decorator in case moto mock_s3 can not be imported.
        """
        return cls

from lsst.utils import doImport
from lsst.daf.butler.core.safeFileIo import safeMakeDir
from lsst.daf.butler import Butler, Config, ButlerConfig
from lsst.daf.butler import StorageClassFactory
from lsst.daf.butler import DatasetType, DatasetRef
from lsst.daf.butler import FileTemplateValidationError, ValidationError
from lsst.daf.butler import FileDataset
from examplePythonTypes import MetricsExample
from lsst.daf.butler.core.repoRelocation import BUTLER_ROOT_TAG
from lsst.daf.butler.core.location import ButlerURI
from lsst.daf.butler.core.s3utils import (s3CheckFileExists, setAwsEnvCredentials,
                                          unsetAwsEnvCredentials)

from datasetsHelper import MultiDetectorFormatter

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


class ButlerConfigTests(unittest.TestCase):
    """Simple tests for ButlerConfig that are not tested in other test cases.
    """

    def testSearchPath(self):
        configFile = os.path.join(TESTDIR, "config", "basic", "butler.yaml")
        with self.assertLogs("lsst.daf.butler", level="DEBUG") as cm:
            config1 = ButlerConfig(configFile)
        self.assertNotIn("testConfigs", "\n".join(cm.output))

        overrideDirectory = os.path.join(TESTDIR, "config", "testConfigs")
        with self.assertLogs("lsst.daf.butler", level="DEBUG") as cm:
            config2 = ButlerConfig(configFile, searchPaths=[overrideDirectory])
        self.assertIn("testConfigs", "\n".join(cm.output))

        key = ("datastore", "records", "table")
        self.assertNotEqual(config1[key], config2[key])
        self.assertEqual(config2[key], "override_record")


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
        dimensions = butler.registry.dimensions.extract(["instrument", "visit"])

        datasetType = self.addDatasetType(datasetTypeName, dimensions, storageClass, butler.registry)

        # Add needed Dimensions
        butler.registry.insertDimensionData("instrument", {"name": "DummyCamComp"})
        butler.registry.insertDimensionData("physical_filter", {"instrument": "DummyCamComp",
                                                                "name": "d-r",
                                                                "abstract_filter": "R"})
        butler.registry.insertDimensionData("visit", {"instrument": "DummyCamComp", "id": 423,
                                                      "name": "fourtwentythree", "physical_filter": "d-r"})

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

        return butler

    def testIngest(self):
        butler = Butler(self.tmpConfigFile)

        # Create and register a DatasetType
        dimensions = butler.registry.dimensions.extract(["instrument", "visit", "detector"])

        storageClass = self.storageClassFactory.getStorageClass("StructuredDataDictYaml")
        datasetTypeName = "metric"

        datasetType = self.addDatasetType(datasetTypeName, dimensions, storageClass, butler.registry)

        # Add needed Dimensions
        butler.registry.insertDimensionData("instrument", {"name": "DummyCamComp"})
        butler.registry.insertDimensionData("physical_filter", {"instrument": "DummyCamComp",
                                                                "name": "d-r",
                                                                "abstract_filter": "R"})
        for detector in (1, 2):
            butler.registry.insertDimensionData("detector", {"instrument": "DummyCamComp", "id": detector,
                                                             "full_name": f"detector{detector}"})

        butler.registry.insertDimensionData("visit", {"instrument": "DummyCamComp", "id": 423,
                                                      "name": "fourtwentythree", "physical_filter": "d-r"},
                                                     {"instrument": "DummyCamComp", "id": 424,
                                                      "name": "fourtwentyfour", "physical_filter": "d-r"})

        formatter = doImport("lsst.daf.butler.formatters.yamlFormatter.YamlFormatter")
        dataRoot = os.path.join(TESTDIR, "data", "basic")
        datasets = []
        for detector in (1, 2):
            detector_name = f"detector_{detector}"
            metricFile = os.path.join(dataRoot, f"{detector_name}.yaml")
            dataId = {"instrument": "DummyCamComp", "visit": 423, "detector": detector}
            # Create a DatasetRef for ingest
            refIn = DatasetRef(datasetType, dataId, id=None)

            datasets.append(FileDataset(path=metricFile,
                                        refs=[refIn],
                                        formatter=formatter))

        butler.ingest(*datasets, transfer="copy")

        dataId1 = {"instrument": "DummyCamComp", "detector": 1, "visit": 423}
        dataId2 = {"instrument": "DummyCamComp", "detector": 2, "visit": 423}

        metrics1 = butler.get(datasetTypeName, dataId1)
        metrics2 = butler.get(datasetTypeName, dataId2)
        self.assertNotEqual(metrics1, metrics2)

        # Compare URIs
        uri1 = butler.getUri(datasetTypeName, dataId1)
        uri2 = butler.getUri(datasetTypeName, dataId2)
        self.assertNotEqual(uri1, uri2)

        # Now do a multi-dataset but single file ingest
        metricFile = os.path.join(dataRoot, "detectors.yaml")
        refs = []
        for detector in (1, 2):
            detector_name = f"detector_{detector}"
            dataId = {"instrument": "DummyCamComp", "visit": 424, "detector": detector}
            # Create a DatasetRef for ingest
            refs.append(DatasetRef(datasetType, dataId, id=None))

        datasets = []
        datasets.append(FileDataset(path=metricFile,
                                    refs=refs,
                                    formatter=MultiDetectorFormatter))

        butler.ingest(*datasets, transfer="copy")

        dataId1 = {"instrument": "DummyCamComp", "detector": 1, "visit": 424}
        dataId2 = {"instrument": "DummyCamComp", "detector": 2, "visit": 424}

        multi1 = butler.get(datasetTypeName, dataId1)
        multi2 = butler.get(datasetTypeName, dataId2)

        self.assertEqual(multi1, metrics1)
        self.assertEqual(multi2, metrics2)

        # Compare URIs
        uri1 = butler.getUri(datasetTypeName, dataId1)
        uri2 = butler.getUri(datasetTypeName, dataId2)
        self.assertEqual(uri1, uri2)

        # Test that removing one does not break the second
        butler.remove(datasetTypeName, dataId1)
        with self.assertRaises(LookupError):
            butler.datasetExists(datasetTypeName, dataId1)
        self.assertTrue(butler.datasetExists(datasetTypeName, dataId2))
        multi2b = butler.get(datasetTypeName, dataId2)
        self.assertEqual(multi2, multi2b)

    def testPickle(self):
        """Test pickle support.
        """
        butler = Butler(self.tmpConfigFile)
        butlerOut = pickle.loads(pickle.dumps(butler))
        self.assertIsInstance(butlerOut, Butler)
        self.assertEqual(butlerOut.config, butler.config)
        self.assertEqual(butlerOut.collection, butler.collection)
        self.assertEqual(butlerOut.run, butler.run)

    def testGetDatasetTypes(self):
        butler = Butler(self.tmpConfigFile)
        dimensions = butler.registry.dimensions.extract(["instrument", "visit", "physical_filter"])
        dimensionEntries = [
            ("instrument", {"instrument": "DummyCam"}, {"instrument": "DummyHSC"},
             {"instrument": "DummyCamComp"}),
            ("physical_filter", {"instrument": "DummyCam", "name": "d-r", "abstract_filter": "R"}),
            ("visit", {"instrument": "DummyCam", "id": 42, "name": "fortytwo", "physical_filter": "d-r"})
        ]
        storageClass = self.storageClassFactory.getStorageClass("StructuredData")
        # Add needed Dimensions
        for args in dimensionEntries:
            butler.registry.insertDimensionData(*args)

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
        dimensions = butler.registry.dimensions.extract(["instrument", "visit"])
        dimensionEntries = (("instrument", {"instrument": "DummyCam"}),
                            ("physical_filter", {"instrument": "DummyCam", "name": "d-r",
                                                 "abstract_filter": "R"}),
                            ("visit", {"instrument": "DummyCam", "id": 42, "name": "fortytwo",
                                       "physical_filter": "d-r"}))
        storageClass = self.storageClassFactory.getStorageClass("StructuredData")
        metric = makeExampleMetrics()
        dataId = {"instrument": "DummyCam", "visit": 42}
        with self.assertRaises(TransactionTestError):
            with butler.transaction():
                # Create and register a DatasetType
                datasetType = self.addDatasetType(datasetTypeName, dimensions, storageClass, butler.registry)
                # Add needed Dimensions
                for args in dimensionEntries:
                    butler.registry.insertDimensionData(*args)
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
        with self.assertRaises(LookupError):
            butler.registry.expandDataId(dataId)
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

        butlerConfig = Butler.makeRepo(self.root, config=Config(self.configFile))
        limited = Config(self.configFile)
        butler1 = Butler(butlerConfig, collection="ingest")
        butlerConfig = Butler.makeRepo(self.root, standalone=True, createRegistry=False,
                                       config=Config(self.configFile))
        full = Config(self.tmpConfigFile)
        butler2 = Butler(butlerConfig, collection="ingest")
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

        # Check that a config with no associated file name will not
        # work properly with relocatable Butler repo
        butlerConfig.configFile = None
        with self.assertRaises(ValueError):
            Butler(butlerConfig, collection="ingest")

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


class FileLikeDatastoreButlerTests(ButlerTests):
    """Common tests and specialization of ButlerTests for butlers backed
    by datastores that inherit from FileLikeDatastore.
    """

    def checkFileExists(self, root, path):
        """Checks if file exists at a given path (relative to root).

        Test testPutTemplates verifies actual physical existance of the files
        in the requested location. For POSIXDatastore this test is equivalent
        to `os.path.exists` call.
        """
        return os.path.exists(os.path.join(root, path))

    def testPutTemplates(self):
        storageClass = self.storageClassFactory.getStorageClass("StructuredDataNoComponents")
        butler = Butler(self.tmpConfigFile)

        # Add needed Dimensions
        butler.registry.insertDimensionData("instrument", {"name": "DummyCamComp"})
        butler.registry.insertDimensionData("physical_filter", {"instrument": "DummyCamComp",
                                                                "name": "d-r",
                                                                "abstract_filter": "R"})
        butler.registry.insertDimensionData("visit", {"instrument": "DummyCamComp", "id": 423, "name": "v423",
                                                      "physical_filter": "d-r"})
        butler.registry.insertDimensionData("visit", {"instrument": "DummyCamComp", "id": 425, "name": "v425",
                                                      "physical_filter": "d-r"})

        # Create and store a dataset
        metric = makeExampleMetrics()

        # Create two almost-identical DatasetTypes (both will use default
        # template)
        dimensions = butler.registry.dimensions.extract(["instrument", "visit"])
        butler.registry.registerDatasetType(DatasetType("metric1", dimensions, storageClass))
        butler.registry.registerDatasetType(DatasetType("metric2", dimensions, storageClass))
        butler.registry.registerDatasetType(DatasetType("metric3", dimensions, storageClass))

        dataId1 = {"instrument": "DummyCamComp", "visit": 423}
        dataId2 = {"instrument": "DummyCamComp", "visit": 423, "physical_filter": "d-r"}
        dataId3 = {"instrument": "DummyCamComp", "visit": 425}

        # Put with exactly the data ID keys needed
        ref = butler.put(metric, "metric1", dataId1)
        self.assertTrue(self.checkFileExists(butler.datastore.root,
                                             "ingest/metric1/d-r/DummyCamComp_423.pickle"))

        # Check the template based on dimensions
        butler.datastore.templates.validateTemplates([ref])

        # Put with extra data ID keys (physical_filter is an optional
        # dependency); should not change template (at least the way we're
        # defining them  to behave now; the important thing is that they
        # must be consistent).
        ref = butler.put(metric, "metric2", dataId2)
        self.assertTrue(self.checkFileExists(butler.datastore.root,
                                             "ingest/metric2/d-r/DummyCamComp_v423.pickle"))

        # Check the template based on dimensions
        butler.datastore.templates.validateTemplates([ref])

        # Now use a file template that will not result in unique filenames
        ref = butler.put(metric, "metric3", dataId1)

        # Check the template based on dimensions. This one is a bad template
        with self.assertRaises(FileTemplateValidationError):
            butler.datastore.templates.validateTemplates([ref])

        with self.assertRaises(FileExistsError):
            butler.put(metric, "metric3", dataId3)

    def testImportExport(self):
        # Run put/get tests just to create and populate a repo.
        storageClass = self.storageClassFactory.getStorageClass("StructuredDataNoComponents")
        exportButler = self.runPutGetTest(storageClass, "test_metric")
        # Test that the repo actually has at least one dataset.
        datasets = list(exportButler.registry.queryDatasets(..., collections=...))
        self.assertGreater(len(datasets), 0)
        # Export those datasets.  We used TemporaryDirectory because there
        # doesn't seem to be a way to get the filename (as opposed to the file
        # object) from any of tempfile's temporary-file context managers.
        with tempfile.TemporaryDirectory() as exportDir:
            # TODO: When PosixDatastore supports transfer-on-exist, add tests
            # for that.
            exportFile = os.path.join(exportDir, "exports.yaml")
            with exportButler.export(filename=exportFile) as export:
                export.saveDatasets(datasets)
            self.assertTrue(os.path.exists(exportFile))
            with tempfile.TemporaryDirectory() as importDir:
                Butler.makeRepo(importDir, config=Config(self.configFile))
                importButler = Butler(importDir)
                importButler.import_(filename=exportFile, directory=exportButler.datastore.root,
                                     transfer="symlink")
                for ref in datasets:
                    with self.subTest(ref=ref):
                        # Test for existence by passing in the DatasetType and
                        # data ID separately, to avoid lookup by dataset_id.
                        self.assertTrue(importButler.datasetExists(ref.datasetType, ref.dataId))


class PosixDatastoreButlerTestCase(FileLikeDatastoreButlerTests, unittest.TestCase):
    """PosixDatastore specialization of a butler"""
    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")
    fullConfigKey = ".datastore.formatters"
    validationCanFail = True
    datastoreStr = ["/tmp"]
    datastoreName = [f"PosixDatastore@{BUTLER_ROOT_TAG}"]
    registryStr = "/gen3.sqlite3"


class InMemoryDatastoreButlerTestCase(ButlerTests, unittest.TestCase):
    """InMemoryDatastore specialization of a butler"""
    configFile = os.path.join(TESTDIR, "config/basic/butler-inmemory.yaml")
    fullConfigKey = None
    useTempRoot = False
    validationCanFail = False
    datastoreStr = ["datastore='InMemory"]
    datastoreName = ["InMemoryDatastore@"]
    registryStr = ":memory:"

    def testIngest(self):
        pass


class ChainedDatastoreButlerTestCase(ButlerTests, unittest.TestCase):
    """PosixDatastore specialization"""
    configFile = os.path.join(TESTDIR, "config/basic/butler-chained.yaml")
    fullConfigKey = ".datastore.datastores.1.formatters"
    validationCanFail = True
    datastoreStr = ["datastore='InMemory", "/PosixDatastore_1,", "/PosixDatastore_2'"]
    datastoreName = ["InMemoryDatastore@", f"PosixDatastore@{BUTLER_ROOT_TAG}/PosixDatastore_1",
                     "SecondDatastore"]
    registryStr = "/gen3.sqlite3"


class ButlerExplicitRootTestCase(PosixDatastoreButlerTestCase):
    """Test that a yaml file in one location can refer to a root in another."""

    datastoreStr = ["dir1"]
    # Disable the makeRepo test since we are deliberately not using
    # butler.yaml as the config name.
    fullConfigKey = None

    def setUp(self):
        self.root = tempfile.mkdtemp(dir=TESTDIR)

        # Make a new repository in one place
        self.dir1 = os.path.join(self.root, "dir1")
        Butler.makeRepo(self.dir1, config=Config(self.configFile))

        # Move the yaml file to a different place and add a "root"
        self.dir2 = os.path.join(self.root, "dir2")
        safeMakeDir(self.dir2)
        configFile1 = os.path.join(self.dir1, "butler.yaml")
        config = Config(configFile1)
        config["root"] = self.dir1
        configFile2 = os.path.join(self.dir2, "butler2.yaml")
        config.dumpToFile(configFile2)
        os.remove(configFile1)
        self.tmpConfigFile = configFile2

    def testFileLocations(self):
        self.assertNotEqual(self.dir1, self.dir2)
        self.assertTrue(os.path.exists(os.path.join(self.dir2, "butler2.yaml")))
        self.assertFalse(os.path.exists(os.path.join(self.dir1, "butler.yaml")))
        self.assertTrue(os.path.exists(os.path.join(self.dir1, "gen3.sqlite3")))


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


@unittest.skipIf(not boto3, "Warning: boto3 AWS SDK not found!")
@mock_s3
class S3DatastoreButlerTestCase(FileLikeDatastoreButlerTests, unittest.TestCase):
    """S3Datastore specialization of a butler; an S3 storage Datastore +
    a local in-memory SqlRegistry.
    """
    configFile = os.path.join(TESTDIR, "config/basic/butler-s3store.yaml")
    fullConfigKey = None
    validationCanFail = True

    bucketName = "anybucketname"
    """Name of the Bucket that will be used in the tests. The name is read from
    the config file used with the tests during set-up.
    """

    root = "butlerRoot/"
    """Root repository directory expected to be used in case useTempRoot=False.
    Otherwise the root is set to a 20 characters long randomly generated string
    during set-up.
    """

    datastoreStr = [f"datastore={root}"]
    """Contains all expected root locations in a format expected to be
    returned by Butler stringification.
    """

    datastoreName = ["S3Datastore@s3://{bucketName}/{root}"]
    """The expected format of the S3Datastore string."""

    registryStr = f":memory:"
    """Expected format of the Registry string."""

    def genRoot(self):
        """Returns a random string of len 20 to serve as a root
        name for the temporary bucket repo.

        This is equivalent to tempfile.mkdtemp as this is what self.root
        becomes when useTempRoot is True.
        """
        rndstr = "".join(
            random.choice(string.ascii_uppercase + string.digits) for _ in range(20)
        )
        return rndstr + "/"

    def setUp(self):
        config = Config(self.configFile)
        uri = ButlerURI(config[".datastore.datastore.root"])
        self.bucketName = uri.netloc

        # set up some fake credentials if they do not exist
        self.usingDummyCredentials = setAwsEnvCredentials()

        if self.useTempRoot:
            self.root = self.genRoot()
        rooturi = f"s3://{self.bucketName}/{self.root}"
        config.update({"datastore": {"datastore": {"root": rooturi}}})

        # MOTO needs to know that we expect Bucket bucketname to exist
        # (this used to be the class attribute bucketName)
        s3 = boto3.resource("s3")
        s3.create_bucket(Bucket=self.bucketName)

        self.datastoreStr = f"datastore={self.root}"
        self.datastoreName = [f"S3Datastore@{rooturi}"]
        Butler.makeRepo(rooturi, config=config, forceConfigRoot=False)
        self.tmpConfigFile = posixpath.join(rooturi, "butler.yaml")

    def tearDown(self):
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(self.bucketName)
        try:
            bucket.objects.all().delete()
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                # the key was not reachable - pass
                pass
            else:
                raise

        bucket = s3.Bucket(self.bucketName)
        bucket.delete()

        # unset any potentially set dummy credentials
        if self.usingDummyCredentials:
            unsetAwsEnvCredentials()

    def checkFileExists(self, root, relpath):
        """Checks if file exists at a given path (relative to root).

        Test testPutTemplates verifies actual physical existance of the files
        in the requested location. For S3Datastore this test is equivalent to
        `lsst.daf.butler.core.s3utils.s3checkFileExists` call.
        """
        uri = ButlerURI(root)
        client = boto3.client("s3")
        return s3CheckFileExists(uri, client=client)[0]

    @unittest.expectedFailure
    def testImportExport(self):
        super().testImportExport()


if __name__ == "__main__":
    unittest.main()

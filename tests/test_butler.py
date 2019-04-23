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
import string
import random

import boto3
import botocore
from moto import mock_s3

from lsst.daf.butler.core.safeFileIo import safeMakeDir
from lsst.daf.butler import Butler, Config, ButlerConfig
from lsst.daf.butler import StorageClassFactory
from lsst.daf.butler import DatasetType, DatasetRef
from lsst.daf.butler import FileTemplateValidationError, ValidationError
from examplePythonTypes import MetricsExample
from lsst.daf.butler.core.repoRelocation import BUTLER_ROOT_TAG

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
        self.assertEqual(config2[key], "OverrideRecord")


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
        # is root declared in the self.configFile, which one exactly is this?
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

    datastoreStr = ["/tmp"]
    datastoreName = [f"POSIXDatastore@{BUTLER_ROOT_TAG}"]
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
    datastoreStr = ["datastore='InMemory", "/PosixDatastore_1,", "/PosixDatastore_2'"]
    datastoreName = ["InMemoryDatastore@", f"POSIXDatastore@{BUTLER_ROOT_TAG}/PosixDatastore_1",
                     "SecondDatastore"]
    registryStr = "/gen3.sqlite3'"


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

@mock_s3
class S3DatastoreButlerTestCase(ButlerTests, unittest.TestCase):
    """S3Datastore specialization of a butler. Realistically this is a
    S3 storage DataStore + a local SqlRegistry (could be an interesting usecase, but
    currently absolutely useless as local registry is in memory.)

    Very practical for testing the S3Datastore quickly (~6x as fast as RDS setup/teardown).
    I need to have a closer look at why is that - is it only because there are many DB
    being simultaneously oppened on a 20GB 1vcpu machine or something else.
    """
    configFile = os.path.join(TESTDIR, "config/basic/butler-s3store.yaml")
    fullConfigKey = None
    validationCanFail = True

    bucketName = 'bucketname'
    permRoot = 'root/'

    registryStr = f"registry='sqlite:///:memory:'"

    def genRoot(self):
        """Returns a random string of len 20 to serve as a root
        name for the temporary bucket repo.
        """
        rndstr = ''.join(
            random.choice(string.ascii_uppercase + string.digits) for _ in range(20)
        )
        return rndstr

    def setUp(self):
        # I finally understand, inside MOTO's virtual AWS acc nothing yet exists
        # we need to recreate everything
        s3 = boto3.resource('s3')
        s3.create_bucket(Bucket=self.bucketName)

        # create new repo root dir - analog to mkdir
        if self.useTempRoot:
            self.root = self.genRoot()+'/'
        else:
            self.root = self.permRoot

        # datastoreStr and Name need to be created here, otherwise how can we tell the
        # name of the randomly created root, I'm not sure how to work this out with the magical
        # BUTLER_ROOT_TAG
        rooturi = f's3://{self.bucketName}/{self.root}'
        self.datastoreStr = f"datastore={self.root}"
        self.datastoreName = [f"S3Datastore@{rooturi}"]
        Butler.makeRepo(rooturi, config=Config(self.configFile))
        self.tmpConfigFile = os.path.join(rooturi, "butler.yaml")

    def tearDown(self):
        # clean up the test bucket, note that there are no directories in S3
        # so there is no recursive Key deleteion functionality either.
        s3 = boto3.resource('s3')
        try:
            s3.Object(self.bucketName, self.root).load()
            bucket = s3.Bucket(self.bucketName)
            bucket.objects.filter(Prefix=self.root).delete()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                # the key was not reachable, pass
                pass
            else:
                raise
        else:
            # key exists, remove it
            bucket = s3.Bucket(self.bucketName)
            bucket.objects.filter(Prefix=self.root).delete()

        bucket = s3.Bucket(self.bucketName)
        bucket.delete()

    def testPutTemplates(self):
        storageClass = self.storageClassFactory.getStorageClass("StructuredDataNoComponents")
        butler = Butler(self.tmpConfigFile)

        from lsst.daf.butler.core.utils import s3CheckFileExists

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
        self.assertTrue(s3CheckFileExists(butler.datastore.client, butler.datastore.bucket,
                                          os.path.join(self.root,  "ingest/metric1/DummyCamComp_423.pickle"))[0])

        # Check the template based on dimensions
        butler.datastore.templates.validateTemplates([ref])

        # Put with extra data ID keys (physical_filter is an optional
        # dependency); should not change template (at least the way we're
        # defining them  to behave now; the important thing is that they
        # must be consistent).
        ref = butler.put(metric, "metric2", dataId2)
        self.assertTrue(s3CheckFileExists(butler.datastore.client, butler.datastore.bucket,
                                          os.path.join(self.root,  "ingest/metric2/DummyCamComp_423.pickle"))[0])

        # Check the template based on dimensions
        butler.datastore.templates.validateTemplates([ref])

        # Now use a file template that will not result in unique filenames
        ref = butler.put(metric, "metric3", dataId1)

        # Check the template based on dimensions. This one is a bad template
        with self.assertRaises(FileTemplateValidationError):
            butler.datastore.templates.validateTemplates([ref])

        with self.assertRaises(FileExistsError):
            butler.put(metric, "metric3", dataId3)

@mock_s3
class S3RdsButlerTestCase(S3DatastoreButlerTestCase):
    """An Amazon cloud oriented Butler. DataStore is the S3 Storage and the Registry
    is an RDS PostgreSql service.
    """
    configFile = os.path.join(TESTDIR, "config/basic/butler-s3rds.yaml")
    fullConfigKey = None # ".datastore.formatters"
    validationCanFail = True

    bucketName = 'bucketname'
    permRoot = 'root'

    # there are too many important parameters in the db string that need to
    # carry over to here, so instead of making people copy-paste it on every change
    # read it from config and re-format it to something useful. Caveat: the RDS name can,
    # but does not have to be, the same as the dbname, we want to avoid replacing anything
    # except the dbname that's why we manually add the name at the end instead of replace().
    config =  Config(configFile)
    tmpconstr = config['.registry.registry.db']
    defaultName = tmpconstr.split('/')[-1]
    constr = tmpconstr[:-len(defaultName)]+'{dbname}'
    connectionStr = constr


    def genRoot(self):
        """Returns a random string of len 20 to serve as a root
        name for the temporary bucket repo.
        """
        rndstr = ''.join(
            random.choice(string.ascii_lowercase) for _ in range(20)
        )
        return rndstr

    def setUp(self):
        # create a new test bucket so that moto knows it exists
        s3 = boto3.resource('s3')
        s3.create_bucket(Bucket=self.bucketName)

        # create a new repo root - analog of tempdir
        if self.useTempRoot:
            self.root = self.genRoot()
        else:
            self.root = self.permRoot
        # datastoreStr and Name need to be created here, otherwise how can we tell the
        # name of the randomly created root, I'm not sure how to work this out with the magical
        # BUTLER_ROOT_TAG
        rooturi = f's3://{self.bucketName}/{self.root}'
        self.registryStr = self.connectionStr.format(dbname=self.root)
        self.datastoreStr = f"datastore={self.root}"
        self.datastoreName = [f"S3Datastore@{rooturi}"]

        # [SANITY WARNING] - Multiple DBs as individual repositories
        # Sqlalchemy won't be able to create a new db by just connecting to a new name.
        # In PostgreSql CREATE DATABSE statement can not be issued within a transaction.
        # So pure execute won't work since it creates a Connection implicitly. That Connection
        # needs to be closed before CREATE statement can be issued. This means users need to have
        # sufficient permissions to create DBs if they want to run this test.
        from sqlalchemy import create_engine
        import urllib.parse as urlparse
        import configparser

        # First, replace the default connection string with a db that we know will always exist.
        # The name will always be last, but its easier to just replace it than to deconstruct and
        # then reconstruct the whole string
        defaultName = self.connectionStr.split('/')[-1]
        constr = self.connectionStr.replace(defaultName, 'postgres')

        # Second, get the local connection credentials from ~/.rds
        parsed = urlparse.urlparse(constr)
        localconf = configparser.ConfigParser()
        localconf.read(os.path.expanduser('~/.rds/credentials'))

        username = localconf[parsed.username]['username']
        password = localconf[parsed.username]['password']
        constr = constr.replace(parsed.username, f'{username}:{password}')

        # Third, connect as that user, commit to drop out of transaction scope and create new DB
        engine = create_engine(constr)
        connection = engine.connect()
        connection.execute('commit')
        connection.execute(f'CREATE DATABASE {self.root}')
        connection.close()

        # Finally, now that we know that DB exists, replace the connection string from yaml
        # config file for a one, that points to newly created temporary DB, and create new repo
        config = Config(self.configFile)
        config['.registry.registry.db'] = self.registryStr
        Butler.makeRepo(rooturi, config=config)
        self.tmpConfigFile = rooturi+"/butler.yaml"

    def tearDown(self):
        # clean up the test bucket
        s3 = boto3.resource('s3')
        try:
            s3.Object(self.bucketName, self.root).load()
            bucket = s3.Bucket(self.bucketName)
            bucket.objects.filter(Prefix=self.root).delete()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                # the key was not reachable, pass
                pass
            else:
                raise
        else:
            # key exists, remove it
            bucket = s3.Bucket(self.bucketName)
            bucket.objects.filter(Prefix=self.root).delete()

        bucket = s3.Bucket(self.bucketName)
        bucket.delete()

        # [SANITY WARNING #2] - Multiple DBs as repositories strike back
        # We have to undo everything done in setup. The quickest way is to just drop the
        # temporary database. PostgreSql won't allow dropping the DB as long as there are
        # users connected to it. RDS service seems to do a lot in the background with these
        # DBs since seemingly there is always a process attached. So we need to ban all future
        # connections to the DB, kill all current connections to it, and then attempt to drop
        # the DB.
        from sqlalchemy.exc import OperationalError
        from sqlalchemy import create_engine
        import urllib.parse as urlparse
        import configparser

        # First, replace the default connection string with a db that we know will always exist.
        defaultName = self.connectionStr.split('/')[-1]
        constr = self.connectionStr.replace(defaultName, 'postgres')

        # Second, get the local connection credentials from ~/.rds
        parsed = urlparse.urlparse(constr)
        localconf = configparser.ConfigParser()
        localconf.read(os.path.expanduser('~/.rds/credentials'))

        username = localconf[parsed.username]['username']
        password = localconf[parsed.username]['password']
        constr = constr.replace(parsed.username, f'{username}:{password}')

        # Third, connect as that user, commit to drop out of transaction scope and create new DB
        engine = create_engine(constr)
        connection = engine.connect()
        connection.execute("commit")

        # Fourth, ban all future connections. Make sure to end transaction scope.
        connection.execute(f'REVOKE CONNECT ON DATABASE "{self.root}" FROM public;')
        connection.execute('commit')

        # Fifth, kill all currently connected processes, except ours.
        # IT IS INCREDIBLY IMPORTANT that the db name is single-quoted!!!
        connection.execute((f'SELECT pid, pg_terminate_backend(pid) '
                            'FROM pg_stat_activity WHERE '
                            f'datname = \'{self.root}\' and pid <> pg_backend_pid();'))
        connection.execute('commit')

        # Sixth, attempt to drop the table. It does not seem like processes detach *immediatelly*
        # always, so give them some time
        import time
        dropped = False
        for i in range(3):
            try:
                connection.execute(f'DROP DATABASE {self.root}')
                break
            except OperationalError as e:
                time.sleep(1)
                # if database isn't dropped on 3 attempts add details for debugging and reraise it
                if i>=2:
                    columns = ('datid', 'datname', 'pid', 'usesysid', 'usename',
                               'application_name', 'client_addr', 'client_hostname',
                               'client_port', 'wait_event_type', 'wait_event', 'state',
                               'query')
                    places = ['{'+str(i)+':15}' for i in range(0, len(columns))]
                    frmtstr = "".join(places)+"\n"
                    header = frmtstr.format(*columns)
                    queryCols = ", ".join(columns)
                    res = connection.execute((f'SELECT {queryCols} FROM pg_stat_activity WHERE '
                                              f'datname = \'{self.root}\' and pid <> pg_backend_pid();'))

                    details = header
                    for row in res:
                        row = [str(val) for val in row.values()] if None in row.values() else row.values()
                        details += frmtstr.format(*row)

                    e.add_detail(details)
                    raise e
            connection.execute('commit')
        connection.close()


if __name__ == "__main__":
    unittest.main()

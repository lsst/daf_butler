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
import shutil
import yaml
import tempfile
import lsst.utils

from lsst.daf.butler import StorageClassFactory, StorageClass, DimensionUniverse
from lsst.daf.butler import DatastoreConfig, DatasetTypeNotSupportedError, DatastoreValidationError

from lsst.utils import doImport

from datasetsHelper import DatasetTestHelper, DatastoreTestHelper
from examplePythonTypes import MetricsExample

from dummyRegistry import DummyRegistry


TESTDIR = os.path.dirname(__file__)


def makeExampleMetrics():
    return MetricsExample({"AM1": 5.2, "AM2": 30.6},
                          {"a": [1, 2, 3],
                           "b": {"blue": 5, "red": "green"}},
                          [563, 234, 456.7]
                          )


class TransactionTestError(Exception):
    """Specific error for transactions, to prevent misdiagnosing
    that might otherwise occur when a standard exception is used.
    """
    pass


class DatastoreTestsBase(DatasetTestHelper, DatastoreTestHelper):
    """Support routines for datastore testing"""
    root = None

    @classmethod
    def setUpClass(cls):
        # Storage Classes are fixed for all datastores in these tests
        scConfigFile = os.path.join(TESTDIR, "config/basic/storageClasses.yaml")
        cls.storageClassFactory = StorageClassFactory()
        cls.storageClassFactory.addFromConfig(scConfigFile)

        # Read the Datastore config so we can get the class
        # information (since we should not assume the constructor
        # name here, but rely on the configuration file itself)
        datastoreConfig = DatastoreConfig(cls.configFile)
        cls.datastoreType = doImport(datastoreConfig["cls"])
        cls.universe = DimensionUniverse.fromConfig()

    def setUp(self):
        self.setUpDatastoreTests(DummyRegistry, DatastoreConfig)

    def tearDown(self):
        if self.root is not None and os.path.exists(self.root):
            shutil.rmtree(self.root, ignore_errors=True)


class DatastoreTests(DatastoreTestsBase):
    """Some basic tests of a simple datastore."""

    hasUnsupportedPut = True

    def testConfigRoot(self):
        full = DatastoreConfig(self.configFile)
        config = DatastoreConfig(self.configFile, mergeDefaults=False)
        newroot = "/random/location"
        self.datastoreType.setConfigRoot(newroot, config, full)
        if self.rootKeys:
            for k in self.rootKeys:
                self.assertIn(newroot, config[k])

    def testConstructor(self):
        datastore = self.makeDatastore()
        self.assertIsNotNone(datastore)
        self.assertIs(datastore.isEphemeral, self.isEphemeral)

    def testConfigurationValidation(self):
        datastore = self.makeDatastore()
        sc = self.storageClassFactory.getStorageClass("ThingOne")
        datastore.validateConfiguration([sc])

        sc2 = self.storageClassFactory.getStorageClass("ThingTwo")
        if self.validationCanFail:
            with self.assertRaises(DatastoreValidationError):
                datastore.validateConfiguration([sc2], logFailures=True)

        dimensions = self.universe.extract(("visit", "physical_filter"))
        dataId = {"instrument": "dummy", "visit": 52, "physical_filter": "V"}
        ref = self.makeDatasetRef("metric", dimensions, sc, dataId)
        datastore.validateConfiguration([ref])

        dataId = {"visit": 52, "physical_filter": "V", "foo": "bar"}
        ref = self.makeDatasetRef("metric", dimensions, sc, dataId)
        if self.validationCanFail:
            with self.assertRaises(DatastoreValidationError):
                datastore.validateConfiguration([ref])

    def testParameterValidation(self):
        """Check that parameters are validated"""
        sc = self.storageClassFactory.getStorageClass("ThingOne")
        dimensions = self.universe.extract(("visit", "physical_filter"))
        dataId = {"instrument": "dummy", "visit": 52, "physical_filter": "V"}
        ref = self.makeDatasetRef("metric", dimensions, sc, dataId)
        datastore = self.makeDatastore()
        data = {1: 2, 3: 4}
        datastore.put(data, ref)
        newdata = datastore.get(ref)
        self.assertEqual(data, newdata)
        with self.assertRaises(KeyError):
            newdata = datastore.get(ref, parameters={"missing": 5})

    def testBasicPutGet(self):
        metrics = makeExampleMetrics()
        datastore = self.makeDatastore()

        # Create multiple storage classes for testing different formulations
        storageClasses = [self.storageClassFactory.getStorageClass(sc)
                          for sc in ("StructuredData",
                                     "StructuredDataJson",
                                     "StructuredDataPickle")]

        dimensions = self.universe.extract(("visit", "physical_filter"))
        dataId = {"instrument": "dummy", "visit": 52, "physical_filter": "V"}

        for sc in storageClasses:
            ref = self.makeDatasetRef("metric", dimensions, sc, dataId)
            print("Using storageClass: {}".format(sc.name))
            datastore.put(metrics, ref)

            # Does it exist?
            self.assertTrue(datastore.exists(ref))

            # Get
            metricsOut = datastore.get(ref, parameters=None)
            self.assertEqual(metrics, metricsOut)

            uri = datastore.getUri(ref)
            self.assertEqual(uri[:len(self.uriScheme)], self.uriScheme)

            # Get a component -- we need to construct new refs for them
            # with derived storage classes but with parent ID
            comp = "output"
            compRef = self.makeDatasetRef(ref.datasetType.componentTypeName(comp), dimensions,
                                          sc.components[comp], dataId, id=ref.id)
            output = datastore.get(compRef)
            self.assertEqual(output, metricsOut.output)

            uri = datastore.getUri(compRef)
            self.assertEqual(uri[:len(self.uriScheme)], self.uriScheme)

        storageClass = sc

        # Check that a put fails if the dataset type is not supported
        if self.hasUnsupportedPut:
            sc = StorageClass("UnsupportedSC", pytype=type(metrics))
            ref = self.makeDatasetRef("unsupportedType", dimensions, sc, dataId)
            with self.assertRaises(DatasetTypeNotSupportedError):
                datastore.put(metrics, ref)

        # These should raise
        ref = self.makeDatasetRef("metrics", dimensions, storageClass, dataId, id=10000)
        with self.assertRaises(FileNotFoundError):
            # non-existing file
            datastore.get(ref)

        # Get a URI from it
        uri = datastore.getUri(ref, predict=True)
        self.assertEqual(uri[:len(self.uriScheme)], self.uriScheme)

        with self.assertRaises(FileNotFoundError):
            datastore.getUri(ref)

    def testCompositePutGet(self):
        metrics = makeExampleMetrics()
        datastore = self.makeDatastore()

        # Create multiple storage classes for testing different formulations
        # of composites
        storageClasses = [self.storageClassFactory.getStorageClass(sc)
                          for sc in ("StructuredComposite",
                                     "StructuredCompositeTestA",
                                     "StructuredCompositeTestB")]

        dimensions = self.universe.extract(("visit", "physical_filter"))
        dataId = {"instrument": "dummy", "visit": 428, "physical_filter": "R"}

        for sc in storageClasses:
            print("Using storageClass: {}".format(sc.name))
            ref = self.makeDatasetRef("metric", dimensions, sc, dataId)

            components = sc.assembler().disassemble(metrics)
            self.assertTrue(components)

            compsRead = {}
            for compName, compInfo in components.items():
                compRef = self.makeDatasetRef(ref.datasetType.componentTypeName(compName), dimensions,
                                              components[compName].storageClass, dataId)

                print("Writing component {} with {}".format(compName, compRef.datasetType.storageClass.name))
                datastore.put(compInfo.component, compRef)

                uri = datastore.getUri(compRef)
                self.assertEqual(uri[:len(self.uriScheme)], self.uriScheme)

                compsRead[compName] = datastore.get(compRef)

                # We can generate identical files for each storage class
                # so remove the component here
                datastore.remove(compRef)

            # combine all the components we read back into a new composite
            metricsOut = sc.assembler().assemble(compsRead)
            self.assertEqual(metrics, metricsOut)

    def testRemove(self):
        metrics = makeExampleMetrics()
        datastore = self.makeDatastore()
        # Put
        dimensions = self.universe.extract(("visit", "physical_filter"))
        dataId = {"instrument": "dummy", "visit": 638, "physical_filter": "U"}

        sc = self.storageClassFactory.getStorageClass("StructuredData")
        ref = self.makeDatasetRef("metric", dimensions, sc, dataId)
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

        dimensions = self.universe.extract(("visit", "physical_filter"))
        dataId = {"instrument": "dummy", "visit": 2048, "physical_filter": "Uprime"}

        sc = self.storageClassFactory.getStorageClass("StructuredData")
        ref = self.makeDatasetRef("metric", dimensions, sc, dataId)

        inputDatastore = self.makeDatastore("test_input_datastore")
        outputDatastore = self.makeDatastore("test_output_datastore")

        inputDatastore.put(metrics, ref)
        outputDatastore.transfer(inputDatastore, ref)

        metricsOut = outputDatastore.get(ref)
        self.assertEqual(metrics, metricsOut)

    def testBasicTransaction(self):
        datastore = self.makeDatastore()
        storageClass = self.storageClassFactory.getStorageClass("StructuredData")
        dimensions = self.universe.extract(("visit", "physical_filter"))
        nDatasets = 6
        dataIds = [{"instrument": "dummy", "visit": i, "physical_filter": "V"} for i in range(nDatasets)]
        data = [(self.makeDatasetRef("metric", dimensions, storageClass, dataId), makeExampleMetrics())
                for dataId in dataIds]
        succeed = data[:nDatasets//2]
        fail = data[nDatasets//2:]
        # All datasets added in this transaction should continue to exist
        with datastore.transaction():
            for ref, metrics in succeed:
                datastore.put(metrics, ref)
        # Whereas datasets added in this transaction should not
        with self.assertRaises(TransactionTestError):
            with datastore.transaction():
                for ref, metrics in fail:
                    datastore.put(metrics, ref)
                raise TransactionTestError("This should propagate out of the context manager")
        # Check for datasets that should exist
        for ref, metrics in succeed:
            # Does it exist?
            self.assertTrue(datastore.exists(ref))
            # Get
            metricsOut = datastore.get(ref, parameters=None)
            self.assertEqual(metrics, metricsOut)
            # URI
            uri = datastore.getUri(ref)
            self.assertEqual(uri[:len(self.uriScheme)], self.uriScheme)
        # Check for datasets that should not exist
        for ref, _ in fail:
            # These should raise
            with self.assertRaises(FileNotFoundError):
                # non-existing file
                datastore.get(ref)
            with self.assertRaises(FileNotFoundError):
                datastore.getUri(ref)

    def testNestedTransaction(self):
        datastore = self.makeDatastore()
        storageClass = self.storageClassFactory.getStorageClass("StructuredData")
        dimensions = self.universe.extract(("visit", "physical_filter"))
        metrics = makeExampleMetrics()

        dataId = {"instrument": "dummy", "visit": 0, "physical_filter": "V"}
        refBefore = self.makeDatasetRef("metric", dimensions, storageClass, dataId)
        datastore.put(metrics, refBefore)
        with self.assertRaises(TransactionTestError):
            with datastore.transaction():
                dataId = {"instrument": "dummy", "visit": 1, "physical_filter": "V"}
                refOuter = self.makeDatasetRef("metric", dimensions, storageClass, dataId)
                datastore.put(metrics, refOuter)
                with datastore.transaction():
                    dataId = {"instrument": "dummy", "visit": 2, "physical_filter": "V"}
                    refInner = self.makeDatasetRef("metric", dimensions, storageClass, dataId)
                    datastore.put(metrics, refInner)
                # All datasets should exist
                for ref in (refBefore, refOuter, refInner):
                    metricsOut = datastore.get(ref, parameters=None)
                    self.assertEqual(metrics, metricsOut)
                raise TransactionTestError("This should roll back the transaction")
        # Dataset(s) inserted before the transaction should still exist
        metricsOut = datastore.get(refBefore, parameters=None)
        self.assertEqual(metrics, metricsOut)
        # But all datasets inserted during the (rolled back) transaction
        # should be gone
        with self.assertRaises(FileNotFoundError):
            datastore.get(refOuter)
        with self.assertRaises(FileNotFoundError):
            datastore.get(refInner)

    def runIngestTest(self, func, expectOutput=True):
        storageClass = self.storageClassFactory.getStorageClass("StructuredData")
        dimensions = self.universe.extract(("visit", "physical_filter"))
        metrics = makeExampleMetrics()
        dataId = {"instrument": "dummy", "visit": 0, "physical_filter": "V"}
        ref = self.makeDatasetRef("metric", dimensions, storageClass, dataId)
        with lsst.utils.tests.getTempFilePath(".yaml", expectOutput=expectOutput) as path:
            with open(path, 'w') as fd:
                yaml.dump(metrics._asdict(), stream=fd)
            func(metrics, path, ref)

    def testIngestNoTransfer(self):
        """Test ingesting existing files with no transfer.
        """
        datastore = self.makeDatastore()

        def succeed(obj, path, ref):
            """Ingest a file already in the datastore root."""
            # first move it into the root, and adjust the path accordingly
            path = shutil.copy(path, datastore.root)
            path = os.path.relpath(path, start=datastore.root)
            datastore.ingest(path, ref, transfer=None)
            self.assertEqual(obj, datastore.get(ref))

        def failInputDoesNotExist(obj, path, ref):
            """Can't ingest files if we're given a bad path."""
            with self.assertRaises(FileNotFoundError):
                datastore.ingest("this-file-does-not-exist.yaml", ref, transfer=None)
            self.assertFalse(datastore.exists(ref))

        def failOutsideRoot(obj, path, ref):
            """Can't ingest files outside of datastore root."""
            with self.assertRaises(RuntimeError):
                datastore.ingest(os.path.abspath(path), ref, transfer=None)
            self.assertFalse(datastore.exists(ref))

        def failNotImplemented(obj, path, ref):
            with self.assertRaises(NotImplementedError):
                datastore.ingest(path, ref, transfer=None)

        if None in self.ingestTransferModes:
            self.runIngestTest(failOutsideRoot)
            self.runIngestTest(failInputDoesNotExist)
            self.runIngestTest(succeed)
        else:
            self.runIngestTest(failNotImplemented)

    def testIngestTransfer(self):
        """Test ingesting existing files after transferring them.
        """

        for mode in ("copy", "move", "hardlink", "symlink"):
            with self.subTest(mode=mode):
                datastore = self.makeDatastore(mode)

                def succeed(obj, path, ref):
                    """Ingest a file by transferring it to the template
                    location."""
                    datastore.ingest(os.path.abspath(path), ref, transfer=mode)
                    self.assertEqual(obj, datastore.get(ref))

                def failInputDoesNotExist(obj, path, ref):
                    """Can't ingest files if we're given a bad path."""
                    with self.assertRaises(FileNotFoundError):
                        datastore.ingest("this-file-does-not-exist.yaml", ref, transfer=mode)
                    self.assertFalse(datastore.exists(ref))

                def failOutputExists(obj, path, ref):
                    """Can't ingest files if transfer destination already
                    exists."""
                    with self.assertRaises(FileExistsError):
                        datastore.ingest(os.path.abspath(path), ref, transfer=mode)
                    self.assertFalse(datastore.exists(ref))

                def failNotImplemented(obj, path, ref):
                    with self.assertRaises(NotImplementedError):
                        datastore.ingest(os.path.abspath(path), ref, transfer=mode)

                if mode in self.ingestTransferModes:
                    self.runIngestTest(failInputDoesNotExist)
                    self.runIngestTest(succeed, expectOutput=(mode != "move"))
                    self.runIngestTest(failOutputExists)
                else:
                    self.runIngestTest(failNotImplemented)


class PosixDatastoreTestCase(DatastoreTests, unittest.TestCase):
    """PosixDatastore specialization"""
    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")
    uriScheme = "file:"
    ingestTransferModes = (None, "copy", "move", "hardlink", "symlink")
    isEphemeral = False
    rootKeys = ("root",)
    validationCanFail = True

    def setUp(self):
        # Override the working directory before calling the base class
        self.root = tempfile.mkdtemp(dir=TESTDIR)
        super().setUp()


class InMemoryDatastoreTestCase(DatastoreTests, unittest.TestCase):
    """PosixDatastore specialization"""
    configFile = os.path.join(TESTDIR, "config/basic/inMemoryDatastore.yaml")
    uriScheme = "mem:"
    hasUnsupportedPut = False
    ingestTransferModes = ()
    isEphemeral = True
    rootKeys = None
    validationCanFail = False


class ChainedDatastoreTestCase(PosixDatastoreTestCase):
    """ChainedDatastore specialization using a POSIXDatastore"""
    configFile = os.path.join(TESTDIR, "config/basic/chainedDatastore.yaml")
    hasUnsupportedPut = False
    ingestTransferModes = ("copy", "move", "hardlink", "symlink")
    isEphemeral = False
    rootKeys = (".datastores.1.root", ".datastores.2.root")
    validationCanFail = True


class ChainedDatastoreMemoryTestCase(InMemoryDatastoreTestCase):
    """ChainedDatastore specialization using all InMemoryDatastore"""
    configFile = os.path.join(TESTDIR, "config/basic/chainedDatastore2.yaml")
    validationCanFail = False


class DatastoreConstraintsTests(DatastoreTestsBase):
    """Basic tests of constraints model of Datastores."""

    def testConstraints(self):
        """Test constraints model.  Assumes that each test class has the
        same constraints."""
        metrics = makeExampleMetrics()
        datastore = self.makeDatastore()

        sc1 = self.storageClassFactory.getStorageClass("StructuredData")
        sc2 = self.storageClassFactory.getStorageClass("StructuredDataJson")
        dimensions = self.universe.extract(("visit", "physical_filter", "instrument"))
        dataId = {"visit": 52, "physical_filter": "V", "instrument": "DummyCamComp"}

        # Write empty file suitable for ingest check
        testfile = tempfile.NamedTemporaryFile()

        for datasetTypeName, sc, accepted in (("metric", sc1, True), ("metric2", sc1, False),
                                              ("metric33", sc1, True), ("metric2", sc2, True)):
            with self.subTest(datasetTypeName=datasetTypeName):
                ref = self.makeDatasetRef(datasetTypeName, dimensions, sc, dataId)
                if accepted:
                    datastore.put(metrics, ref)
                    self.assertTrue(datastore.exists(ref))
                    datastore.remove(ref)

                    # Try ingest
                    if self.canIngest:
                        datastore.ingest(testfile.name, ref, transfer="symlink")
                        self.assertTrue(datastore.exists(ref))
                        datastore.remove(ref)
                else:
                    with self.assertRaises(DatasetTypeNotSupportedError):
                        datastore.put(metrics, ref)
                    self.assertFalse(datastore.exists(ref))

                    # Again with ingest
                    if self.canIngest:
                        with self.assertRaises(DatasetTypeNotSupportedError):
                            datastore.ingest(testfile.name, ref, transfer="symlink")
                        self.assertFalse(datastore.exists(ref))


class PosixDatastoreConstraintsTestCase(DatastoreConstraintsTests, unittest.TestCase):
    """PosixDatastore specialization"""
    configFile = os.path.join(TESTDIR, "config/basic/posixDatastoreP.yaml")
    canIngest = True

    def setUp(self):
        # Override the working directory before calling the base class
        self.root = tempfile.mkdtemp(dir=TESTDIR)
        super().setUp()


class InMemoryDatastoreConstraintsTestCase(DatastoreConstraintsTests, unittest.TestCase):
    """InMemoryDatastore specialization"""
    configFile = os.path.join(TESTDIR, "config/basic/inMemoryDatastoreP.yaml")
    canIngest = False


class ChainedDatastoreConstraintsNativeTestCase(PosixDatastoreConstraintsTestCase):
    """ChainedDatastore specialization using a POSIXDatastore and constraints
    at the ChainedDatstore """
    configFile = os.path.join(TESTDIR, "config/basic/chainedDatastorePa.yaml")


class ChainedDatastoreConstraintsTestCase(PosixDatastoreConstraintsTestCase):
    """ChainedDatastore specialization using a POSIXDatastore"""
    configFile = os.path.join(TESTDIR, "config/basic/chainedDatastoreP.yaml")


class ChainedDatastoreMemoryConstraintsTestCase(InMemoryDatastoreConstraintsTestCase):
    """ChainedDatastore specialization using all InMemoryDatastore"""
    configFile = os.path.join(TESTDIR, "config/basic/chainedDatastore2P.yaml")
    canIngest = False


class ChainedDatastorePerStoreConstraintsTests(DatastoreTestsBase, unittest.TestCase):
    """Test that a chained datastore can control constraints per-datastore
    even if child datastore would accept."""

    configFile = os.path.join(TESTDIR, "config/basic/chainedDatastorePb.yaml")

    def setUp(self):
        # Override the working directory before calling the base class
        self.root = tempfile.mkdtemp(dir=TESTDIR)
        super().setUp()

    def testConstraints(self):
        """Test chained datastore constraints model."""
        metrics = makeExampleMetrics()
        datastore = self.makeDatastore()

        sc1 = self.storageClassFactory.getStorageClass("StructuredData")
        sc2 = self.storageClassFactory.getStorageClass("StructuredDataJson")
        dimensions = self.universe.extract(("visit", "physical_filter", "instrument"))
        dataId1 = {"visit": 52, "physical_filter": "V", "instrument": "DummyCamComp"}
        dataId2 = {"visit": 52, "physical_filter": "V", "instrument": "HSC"}

        # Write empty file suitable for ingest check
        testfile = tempfile.NamedTemporaryFile()

        for typeName, dataId, sc, accept, ingest in (("metric", dataId1, sc1, (False, True, False), True),
                                                     ("metric2", dataId1, sc1, (False, False, False), False),
                                                     ("metric2", dataId2, sc1, (True, False, False), False),
                                                     ("metric33", dataId2, sc2, (True, True, False), True),
                                                     ("metric2", dataId1, sc2, (False, True, False), True)):
            with self.subTest(datasetTypeName=typeName, dataId=dataId, sc=sc.name):
                ref = self.makeDatasetRef(typeName, dimensions, sc, dataId)
                if any(accept):
                    datastore.put(metrics, ref)
                    self.assertTrue(datastore.exists(ref))

                    # Check each datastore inside the chained datastore
                    for childDatastore, expected in zip(datastore.datastores, accept):
                        self.assertEqual(childDatastore.exists(ref), expected,
                                         f"Testing presence of {ref} in datastore {childDatastore.name}")

                    datastore.remove(ref)

                    # Check that ingest works
                    if ingest:
                        datastore.ingest(testfile.name, ref, transfer="symlink")
                        self.assertTrue(datastore.exists(ref))

                        # Check each datastore inside the chained datastore
                        for childDatastore, expected in zip(datastore.datastores, accept):
                            # Ephemeral datastores means InMemory at the moment
                            # and that does not accept ingest of files.
                            if childDatastore.isEphemeral:
                                expected = False
                            self.assertEqual(childDatastore.exists(ref), expected,
                                             f"Testing presence of ingested {ref} in datastore"
                                             f" {childDatastore.name}")

                        datastore.remove(ref)
                    else:
                        with self.assertRaises(DatasetTypeNotSupportedError):
                            datastore.ingest(testfile.name, ref, transfer="symlink")

                else:
                    with self.assertRaises(DatasetTypeNotSupportedError):
                        datastore.put(metrics, ref)
                    self.assertFalse(datastore.exists(ref))

                    # Again with ingest
                    with self.assertRaises(DatasetTypeNotSupportedError):
                        datastore.ingest(testfile.name, ref, transfer="symlink")
                    self.assertFalse(datastore.exists(ref))


if __name__ == "__main__":
    unittest.main()

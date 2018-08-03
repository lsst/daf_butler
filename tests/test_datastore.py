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

import lsst.utils.tests

from lsst.daf.butler import StorageClassFactory
from lsst.daf.butler import DatastoreConfig

from lsst.daf.butler.core.utils import doImport

from datasetsHelper import DatasetTestHelper
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


class DatastoreTests(DatasetTestHelper):
    """Some basic tests of a simple POSIX datastore."""
    root = None

    def makeDatastore(self, sub=None):
        """Make a new Datastore instance of the appropriate type.

        Parameters
        ----------
        sub : str, optional
            If not None, the returned Datastore will be distinct from any
            Datastore constructed with a different value of ``sub``.  For
            PosixDatastore, for example, the converse is also true, and ``sub``
            is used as a subdirectory to form the new root.

        Returns
        -------
        datastore : `Datastore`
            Datastore constructed by this routine using the supplied
            optional subdirectory if supported.
        """
        config = self.config.copy()
        if sub is not None and self.root is not None:
            self.datastoreType.setConfigRoot(os.path.join(self.root, sub), config, self.config)
        return self.datastoreType(config=config, registry=self.registry)

    @classmethod
    def setUpClass(cls):
        # Storage Classes are fixed for all datastores in these tests
        scConfigFile = os.path.join(TESTDIR, "config/basic/butler.yaml")
        cls.storageClassFactory = StorageClassFactory()
        cls.storageClassFactory.addFromConfig(scConfigFile)

        # Read the Datastore config so we can get the class
        # information (since we should not assume the constructor
        # name here, but rely on the configuration file itself)
        datastoreConfig = DatastoreConfig(cls.configFile)
        cls.datastoreType = doImport(datastoreConfig["cls"])

    def setUp(self):
        self.registry = DummyRegistry()

        # Need to keep ID for each datasetRef since we have no butler
        # for these tests
        self.id = 1

        self.config = DatastoreConfig(self.configFile)

        # Some subclasses override the working root directory
        if self.root is not None:
            def cleanup(path):
                if path is not None and os.path.exists(path):
                    shutil.rmtree(path)
            self.addCleanup(cleanup, self.root)
            self.datastoreType.setConfigRoot(self.root, self.config, self.config.copy())

    def testConstructor(self):
        datastore = self.makeDatastore()
        self.assertIsNotNone(datastore)

    def testBasicPutGet(self):
        metrics = makeExampleMetrics()
        datastore = self.makeDatastore()

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
            self.assertEqual(uri[:len(self.uriScheme)], self.uriScheme)

            # Get a component -- we need to construct new refs for them
            # with derived storage classes but with parent ID
            comp = "output"
            compRef = self.makeDatasetRef(ref.datasetType.componentTypeName(comp), dataUnits,
                                          sc.components[comp], dataId, id=ref.id)
            output = datastore.get(compRef)
            self.assertEqual(output, metricsOut.output)

            uri = datastore.getUri(compRef)
            self.assertEqual(uri[:len(self.uriScheme)], self.uriScheme)

        storageClass = sc

        # These should raise
        ref = self.makeDatasetRef("metrics", dataUnits, storageClass, dataId, id=10000)
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
                self.assertEqual(uri[:len(self.uriScheme)], self.uriScheme)

                compsRead[compName] = datastore.get(compRef)

            # combine all the components we read back into a new composite
            metricsOut = sc.assembler().assemble(compsRead)
            self.assertEqual(metrics, metricsOut)

    def testRemove(self):
        metrics = makeExampleMetrics()
        datastore = self.makeDatastore()
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

        inputDatastore = self.makeDatastore("test_input_datastore")
        outputDatastore = self.makeDatastore("test_output_datastore")

        inputDatastore.put(metrics, ref)
        outputDatastore.transfer(inputDatastore, ref)

        metricsOut = outputDatastore.get(ref)
        self.assertEqual(metrics, metricsOut)

    def testBasicTransaction(self):
        datastore = self.makeDatastore()
        storageClass = self.storageClassFactory.getStorageClass("StructuredData")
        dataUnits = frozenset(("visit", "filter"))
        nDatasets = 6
        dataIds = [{"visit": i, "filter": "V"} for i in range(nDatasets)]
        data = [(self.makeDatasetRef("metric", dataUnits, storageClass, dataId), makeExampleMetrics())
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
        dataUnits = frozenset(("visit", "filter"))
        metrics = makeExampleMetrics()

        dataId = {"visit": 0, "filter": "V"}
        refBefore = self.makeDatasetRef("metric", dataUnits, storageClass, dataId)
        datastore.put(metrics, refBefore)
        with self.assertRaises(TransactionTestError):
            with datastore.transaction():
                dataId = {"visit": 1, "filter": "V"}
                refOuter = self.makeDatasetRef("metric", dataUnits, storageClass, dataId)
                datastore.put(metrics, refOuter)
                with datastore.transaction():
                    dataId = {"visit": 2, "filter": "V"}
                    refInner = self.makeDatasetRef("metric", dataUnits, storageClass, dataId)
                    datastore.put(metrics, refInner)
                # All datasets should exist
                for ref in (refBefore, refOuter, refInner):
                    metricsOut = datastore.get(ref, parameters=None)
                    self.assertEqual(metrics, metricsOut)
                raise TransactionTestError("This should roll back the transaction")
        # Dataset(s) inserted before the transaction should still exist
        metricsOut = datastore.get(refBefore, parameters=None)
        self.assertEqual(metrics, metricsOut)
        # But all datasets inserted during the (rolled back) transaction should be gone
        with self.assertRaises(FileNotFoundError):
            datastore.get(refOuter)
        with self.assertRaises(FileNotFoundError):
            datastore.get(refInner)

    def runIngestTest(self, func, expectOutput=True):
        storageClass = self.storageClassFactory.getStorageClass("StructuredData")
        dataUnits = frozenset(("visit", "filter"))
        metrics = makeExampleMetrics()
        dataId = {"visit": 0, "filter": "V"}
        ref = self.makeDatasetRef("metric", dataUnits, storageClass, dataId)
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
                    """Ingest a file by transferring it to the template location."""
                    datastore.ingest(os.path.abspath(path), ref, transfer=mode)
                    self.assertEqual(obj, datastore.get(ref))

                def failInputDoesNotExist(obj, path, ref):
                    """Can't ingest files if we're given a bad path."""
                    with self.assertRaises(FileNotFoundError):
                        datastore.ingest("this-file-does-not-exist.yaml", ref, transfer=mode)
                    self.assertFalse(datastore.exists(ref))

                def failOutputExists(obj, path, ref):
                    """Can't ingest files if transfer destination already exists."""
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


class PosixDatastoreTestCase(DatastoreTests, lsst.utils.tests.TestCase):
    """PosixDatastore specialization"""
    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")
    uriScheme = "file:"
    ingestTransferModes = (None, "copy", "move", "hardlink", "symlink")

    def setUp(self):
        # Override the working directory before calling the base class
        self.root = tempfile.mkdtemp(dir=TESTDIR)
        super().setUp()


class InMemoryDatastoreTestCase(DatastoreTests, lsst.utils.tests.TestCase):
    """PosixDatastore specialization"""
    configFile = os.path.join(TESTDIR, "config/basic/inMemoryDatastore.yaml")
    uriScheme = "mem:"
    ingestTransferModes = ()


class ChainedDatastoreTestCase(PosixDatastoreTestCase):
    """ChainedDatastore specialization using a POSIXDatastore"""
    configFile = os.path.join(TESTDIR, "config/basic/chainedDatastore.yaml")
    uriScheme = "mem:"
    ingestTransferModes = ("copy", "move", "hardlink", "symlink")


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()

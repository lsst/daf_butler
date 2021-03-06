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

from lsst.utils import doImport

from lsst.daf.butler import StorageClassFactory, StorageClass, DimensionUniverse, FileDataset
from lsst.daf.butler import DatastoreConfig, DatasetTypeNotSupportedError, DatastoreValidationError
from lsst.daf.butler.formatters.yaml import YamlFormatter

from lsst.daf.butler.tests import (DatasetTestHelper, DatastoreTestHelper, BadWriteFormatter,
                                   BadNoWriteFormatter, MetricsExample, DummyRegistry)


TESTDIR = os.path.dirname(__file__)


def makeExampleMetrics(use_none=False):
    if use_none:
        array = None
    else:
        array = [563, 234, 456.7, 105, 2054, -1045]
    return MetricsExample({"AM1": 5.2, "AM2": 30.6},
                          {"a": [1, 2, 3],
                           "b": {"blue": 5, "red": "green"}},
                          array,
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
        cls.universe = DimensionUniverse()

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
        ref = self.makeDatasetRef("metric", dimensions, sc, dataId, conform=False)
        datastore.validateConfiguration([ref])

    def testParameterValidation(self):
        """Check that parameters are validated"""
        sc = self.storageClassFactory.getStorageClass("ThingOne")
        dimensions = self.universe.extract(("visit", "physical_filter"))
        dataId = {"instrument": "dummy", "visit": 52, "physical_filter": "V"}
        ref = self.makeDatasetRef("metric", dimensions, sc, dataId, conform=False)
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
            ref = self.makeDatasetRef("metric", dimensions, sc, dataId, conform=False)
            print("Using storageClass: {}".format(sc.name))
            datastore.put(metrics, ref)

            # Does it exist?
            self.assertTrue(datastore.exists(ref))

            # Get
            metricsOut = datastore.get(ref, parameters=None)
            self.assertEqual(metrics, metricsOut)

            uri = datastore.getURI(ref)
            self.assertEqual(uri.scheme, self.uriScheme)

            # Get a component -- we need to construct new refs for them
            # with derived storage classes but with parent ID
            for comp in ("data", "output"):
                compRef = ref.makeComponentRef(comp)
                output = datastore.get(compRef)
                self.assertEqual(output, getattr(metricsOut, comp))

                uri = datastore.getURI(compRef)
                self.assertEqual(uri.scheme, self.uriScheme)

        storageClass = sc

        # Check that we can put a metric with None in a component and
        # get it back as None
        metricsNone = makeExampleMetrics(use_none=True)
        dataIdNone = {"instrument": "dummy", "visit": 54, "physical_filter": "V"}
        refNone = self.makeDatasetRef("metric", dimensions, sc, dataIdNone, conform=False)
        datastore.put(metricsNone, refNone)

        comp = "data"
        for comp in ("data", "output"):
            compRef = refNone.makeComponentRef(comp)
            output = datastore.get(compRef)
            self.assertEqual(output, getattr(metricsNone, comp))

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
        uri = datastore.getURI(ref, predict=True)
        self.assertEqual(uri.scheme, self.uriScheme)

        with self.assertRaises(FileNotFoundError):
            datastore.getURI(ref)

    def testTrustGetRequest(self):
        """Check that we can get datasets that registry knows nothing about.
        """

        datastore = self.makeDatastore()

        # Skip test if the attribute is not defined
        if not hasattr(datastore, "trustGetRequest"):
            return

        metrics = makeExampleMetrics()

        i = 0
        for sc_name in ("StructuredData", "StructuredComposite"):
            i += 1
            datasetTypeName = f"metric{i}"

            if sc_name == "StructuredComposite":
                disassembled = True
            else:
                disassembled = False

            # Start datastore in default configuration of using registry
            datastore.trustGetRequest = False

            # Create multiple storage classes for testing with or without
            # disassembly
            sc = self.storageClassFactory.getStorageClass(sc_name)
            dimensions = self.universe.extract(("visit", "physical_filter"))
            dataId = {"instrument": "dummy", "visit": 52 + i, "physical_filter": "V"}

            ref = self.makeDatasetRef(datasetTypeName, dimensions, sc, dataId, conform=False)
            datastore.put(metrics, ref)

            # Does it exist?
            self.assertTrue(datastore.exists(ref))

            # Get
            metricsOut = datastore.get(ref)
            self.assertEqual(metrics, metricsOut)

            # Get the URI(s)
            primaryURI, componentURIs = datastore.getURIs(ref)
            if disassembled:
                self.assertIsNone(primaryURI)
                self.assertEqual(len(componentURIs), 3)
            else:
                self.assertIn(datasetTypeName, primaryURI.path)
                self.assertFalse(componentURIs)

            # Delete registry entry so now we are trusting
            datastore.removeStoredItemInfo(ref)

            # Now stop trusting and check that things break
            datastore.trustGetRequest = False

            # Does it exist?
            self.assertFalse(datastore.exists(ref))

            with self.assertRaises(FileNotFoundError):
                datastore.get(ref)

            with self.assertRaises(FileNotFoundError):
                datastore.get(ref.makeComponentRef("data"))

            # URI should fail unless we ask for prediction
            with self.assertRaises(FileNotFoundError):
                datastore.getURIs(ref)

            predicted_primary, predicted_disassembled = datastore.getURIs(ref, predict=True)
            if disassembled:
                self.assertIsNone(predicted_primary)
                self.assertEqual(len(predicted_disassembled), 3)
                for uri in predicted_disassembled.values():
                    self.assertEqual(uri.fragment, "predicted")
                    self.assertIn(datasetTypeName, uri.path)
            else:
                self.assertIn(datasetTypeName, predicted_primary.path)
                self.assertFalse(predicted_disassembled)
                self.assertEqual(predicted_primary.fragment, "predicted")

            # Now enable registry-free trusting mode
            datastore.trustGetRequest = True

            # Try again to get it
            metricsOut = datastore.get(ref)
            self.assertEqual(metricsOut, metrics)

            # Does it exist?
            self.assertTrue(datastore.exists(ref))

            # Get a component
            comp = "data"
            compRef = ref.makeComponentRef(comp)
            output = datastore.get(compRef)
            self.assertEqual(output, getattr(metrics, comp))

            # Get the URI -- if we trust this should work even without
            # enabling prediction.
            primaryURI2, componentURIs2 = datastore.getURIs(ref)
            self.assertEqual(primaryURI2, primaryURI)
            self.assertEqual(componentURIs2, componentURIs)

    def testDisassembly(self):
        """Test disassembly within datastore."""
        metrics = makeExampleMetrics()
        if self.isEphemeral:
            # in-memory datastore does not disassemble
            return

        # Create multiple storage classes for testing different formulations
        # of composites. One of these will not disassemble to provide
        # a reference.
        storageClasses = [self.storageClassFactory.getStorageClass(sc)
                          for sc in ("StructuredComposite",
                                     "StructuredCompositeTestA",
                                     "StructuredCompositeTestB",
                                     "StructuredCompositeReadComp",
                                     "StructuredData",  # No disassembly
                                     "StructuredCompositeReadCompNoDisassembly",
                                     )]

        # Create the test datastore
        datastore = self.makeDatastore()

        # Dummy dataId
        dimensions = self.universe.extract(("visit", "physical_filter"))
        dataId = {"instrument": "dummy", "visit": 428, "physical_filter": "R"}

        for i, sc in enumerate(storageClasses):
            with self.subTest(storageClass=sc.name):
                # Create a different dataset type each time round
                # so that a test failure in this subtest does not trigger
                # a cascade of tests because of file clashes
                ref = self.makeDatasetRef(f"metric_comp_{i}", dimensions, sc, dataId,
                                          conform=False)

                disassembled = sc.name not in {"StructuredData", "StructuredCompositeReadCompNoDisassembly"}

                datastore.put(metrics, ref)

                baseURI, compURIs = datastore.getURIs(ref)
                if disassembled:
                    self.assertIsNone(baseURI)
                    self.assertEqual(set(compURIs), {"data", "output", "summary"})
                else:
                    self.assertIsNotNone(baseURI)
                    self.assertEqual(compURIs, {})

                metrics_get = datastore.get(ref)
                self.assertEqual(metrics_get, metrics)

                # Retrieve the composite with read parameter
                stop = 4
                metrics_get = datastore.get(ref, parameters={"slice": slice(stop)})
                self.assertEqual(metrics_get.summary, metrics.summary)
                self.assertEqual(metrics_get.output, metrics.output)
                self.assertEqual(metrics_get.data, metrics.data[:stop])

                # Retrieve a component
                data = datastore.get(ref.makeComponentRef("data"))
                self.assertEqual(data, metrics.data)

                # On supported storage classes attempt to access a read
                # only component
                if "ReadComp" in sc.name:
                    cRef = ref.makeComponentRef("counter")
                    counter = datastore.get(cRef)
                    self.assertEqual(counter, len(metrics.data))

                    counter = datastore.get(cRef, parameters={"slice": slice(stop)})
                    self.assertEqual(counter, stop)

                datastore.remove(ref)

    def testRegistryCompositePutGet(self):
        """Tests the case where registry disassembles and puts to datastore.
        """
        metrics = makeExampleMetrics()
        datastore = self.makeDatastore()

        # Create multiple storage classes for testing different formulations
        # of composites
        storageClasses = [self.storageClassFactory.getStorageClass(sc)
                          for sc in ("StructuredComposite",
                                     "StructuredCompositeTestA",
                                     "StructuredCompositeTestB",
                                     )]

        dimensions = self.universe.extract(("visit", "physical_filter"))
        dataId = {"instrument": "dummy", "visit": 428, "physical_filter": "R"}

        for sc in storageClasses:
            print("Using storageClass: {}".format(sc.name))
            ref = self.makeDatasetRef("metric", dimensions, sc, dataId,
                                      conform=False)

            components = sc.delegate().disassemble(metrics)
            self.assertTrue(components)

            compsRead = {}
            for compName, compInfo in components.items():
                compRef = self.makeDatasetRef(ref.datasetType.componentTypeName(compName), dimensions,
                                              components[compName].storageClass, dataId,
                                              conform=False)

                print("Writing component {} with {}".format(compName, compRef.datasetType.storageClass.name))
                datastore.put(compInfo.component, compRef)

                uri = datastore.getURI(compRef)
                self.assertEqual(uri.scheme, self.uriScheme)

                compsRead[compName] = datastore.get(compRef)

                # We can generate identical files for each storage class
                # so remove the component here
                datastore.remove(compRef)

            # combine all the components we read back into a new composite
            metricsOut = sc.delegate().assemble(compsRead)
            self.assertEqual(metrics, metricsOut)

    def prepDeleteTest(self):
        metrics = makeExampleMetrics()
        datastore = self.makeDatastore()
        # Put
        dimensions = self.universe.extract(("visit", "physical_filter"))
        dataId = {"instrument": "dummy", "visit": 638, "physical_filter": "U"}

        sc = self.storageClassFactory.getStorageClass("StructuredData")
        ref = self.makeDatasetRef("metric", dimensions, sc, dataId, conform=False)
        datastore.put(metrics, ref)

        # Does it exist?
        self.assertTrue(datastore.exists(ref))

        # Get
        metricsOut = datastore.get(ref)
        self.assertEqual(metrics, metricsOut)

        return datastore, ref

    def testRemove(self):
        datastore, ref = self.prepDeleteTest()

        # Remove
        datastore.remove(ref)

        # Does it exist?
        self.assertFalse(datastore.exists(ref))

        # Do we now get a predicted URI?
        uri = datastore.getURI(ref, predict=True)
        self.assertEqual(uri.fragment, "predicted")

        # Get should now fail
        with self.assertRaises(FileNotFoundError):
            datastore.get(ref)
        # Can only delete once
        with self.assertRaises(FileNotFoundError):
            datastore.remove(ref)

    def testForget(self):
        datastore, ref = self.prepDeleteTest()

        # Remove
        datastore.forget([ref])

        # Does it exist (as far as we know)?
        self.assertFalse(datastore.exists(ref))

        # Do we now get a predicted URI?
        uri = datastore.getURI(ref, predict=True)
        self.assertEqual(uri.fragment, "predicted")

        # Get should now fail
        with self.assertRaises(FileNotFoundError):
            datastore.get(ref)

        # Forgetting again is a silent no-op
        datastore.forget([ref])

        # Predicted URI should still point to the file.
        self.assertTrue(uri.exists())

    def testTransfer(self):
        metrics = makeExampleMetrics()

        dimensions = self.universe.extract(("visit", "physical_filter"))
        dataId = {"instrument": "dummy", "visit": 2048, "physical_filter": "Uprime"}

        sc = self.storageClassFactory.getStorageClass("StructuredData")
        ref = self.makeDatasetRef("metric", dimensions, sc, dataId, conform=False)

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
        data = [(self.makeDatasetRef("metric", dimensions, storageClass, dataId, conform=False),
                 makeExampleMetrics(),)
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
            uri = datastore.getURI(ref)
            self.assertEqual(uri.scheme, self.uriScheme)
        # Check for datasets that should not exist
        for ref, _ in fail:
            # These should raise
            with self.assertRaises(FileNotFoundError):
                # non-existing file
                datastore.get(ref)
            with self.assertRaises(FileNotFoundError):
                datastore.getURI(ref)

    def testNestedTransaction(self):
        datastore = self.makeDatastore()
        storageClass = self.storageClassFactory.getStorageClass("StructuredData")
        dimensions = self.universe.extract(("visit", "physical_filter"))
        metrics = makeExampleMetrics()

        dataId = {"instrument": "dummy", "visit": 0, "physical_filter": "V"}
        refBefore = self.makeDatasetRef("metric", dimensions, storageClass, dataId,
                                        conform=False)
        datastore.put(metrics, refBefore)
        with self.assertRaises(TransactionTestError):
            with datastore.transaction():
                dataId = {"instrument": "dummy", "visit": 1, "physical_filter": "V"}
                refOuter = self.makeDatasetRef("metric", dimensions, storageClass, dataId,
                                               conform=False)
                datastore.put(metrics, refOuter)
                with datastore.transaction():
                    dataId = {"instrument": "dummy", "visit": 2, "physical_filter": "V"}
                    refInner = self.makeDatasetRef("metric", dimensions, storageClass, dataId,
                                                   conform=False)
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

    def _prepareIngestTest(self):
        storageClass = self.storageClassFactory.getStorageClass("StructuredData")
        dimensions = self.universe.extract(("visit", "physical_filter"))
        metrics = makeExampleMetrics()
        dataId = {"instrument": "dummy", "visit": 0, "physical_filter": "V"}
        ref = self.makeDatasetRef("metric", dimensions, storageClass, dataId, conform=False)
        return metrics, ref

    def runIngestTest(self, func, expectOutput=True):
        metrics, ref = self._prepareIngestTest()
        # The file will be deleted after the test.
        # For symlink tests this leads to a situation where the datastore
        # points to a file that does not exist. This will make os.path.exist
        # return False but then the new symlink will fail with
        # FileExistsError later in the code so the test still passes.
        with lsst.utils.tests.getTempFilePath(".yaml", expectOutput=expectOutput) as path:
            with open(path, 'w') as fd:
                yaml.dump(metrics._asdict(), stream=fd)
            func(metrics, path, ref)

    def testIngestNoTransfer(self):
        """Test ingesting existing files with no transfer.
        """
        for mode in (None, "auto"):

            # Some datastores have auto but can't do in place transfer
            if mode == "auto" and "auto" in self.ingestTransferModes and not self.canIngestNoTransferAuto:
                continue

            with self.subTest(mode=mode):
                datastore = self.makeDatastore()

                def succeed(obj, path, ref):
                    """Ingest a file already in the datastore root."""
                    # first move it into the root, and adjust the path
                    # accordingly
                    path = shutil.copy(path, datastore.root.ospath)
                    path = os.path.relpath(path, start=datastore.root.ospath)
                    datastore.ingest(FileDataset(path=path, refs=ref), transfer=mode)
                    self.assertEqual(obj, datastore.get(ref))

                def failInputDoesNotExist(obj, path, ref):
                    """Can't ingest files if we're given a bad path."""
                    with self.assertRaises(FileNotFoundError):
                        datastore.ingest(FileDataset(path="this-file-does-not-exist.yaml", refs=ref),
                                         transfer=mode)
                    self.assertFalse(datastore.exists(ref))

                def failOutsideRoot(obj, path, ref):
                    """Can't ingest files outside of datastore root unless
                    auto."""
                    if mode == "auto":
                        datastore.ingest(FileDataset(path=os.path.abspath(path), refs=ref), transfer=mode)
                        self.assertTrue(datastore.exists(ref))
                    else:
                        with self.assertRaises(RuntimeError):
                            datastore.ingest(FileDataset(path=os.path.abspath(path), refs=ref), transfer=mode)
                        self.assertFalse(datastore.exists(ref))

                def failNotImplemented(obj, path, ref):
                    with self.assertRaises(NotImplementedError):
                        datastore.ingest(FileDataset(path=path, refs=ref), transfer=mode)

                if mode in self.ingestTransferModes:
                    self.runIngestTest(failOutsideRoot)
                    self.runIngestTest(failInputDoesNotExist)
                    self.runIngestTest(succeed)
                else:
                    self.runIngestTest(failNotImplemented)

    def testIngestTransfer(self):
        """Test ingesting existing files after transferring them.
        """
        for mode in ("copy", "move", "link", "hardlink", "symlink", "relsymlink", "auto"):
            with self.subTest(mode=mode):
                datastore = self.makeDatastore(mode)

                def succeed(obj, path, ref):
                    """Ingest a file by transferring it to the template
                    location."""
                    datastore.ingest(FileDataset(path=os.path.abspath(path), refs=ref), transfer=mode)
                    self.assertEqual(obj, datastore.get(ref))

                def failInputDoesNotExist(obj, path, ref):
                    """Can't ingest files if we're given a bad path."""
                    with self.assertRaises(FileNotFoundError):
                        # Ensure the file does not look like it is in
                        # datastore for auto mode
                        datastore.ingest(FileDataset(path="../this-file-does-not-exist.yaml", refs=ref),
                                         transfer=mode)
                    self.assertFalse(datastore.exists(ref), f"Checking not in datastore using mode {mode}")

                def failOutputExists(obj, path, ref):
                    """Can't ingest files if transfer destination already
                    exists."""
                    with self.assertRaises(FileExistsError):
                        datastore.ingest(FileDataset(path=os.path.abspath(path), refs=ref), transfer=mode)
                    self.assertFalse(datastore.exists(ref), f"Checking not in datastore using mode {mode}")

                def failNotImplemented(obj, path, ref):
                    with self.assertRaises(NotImplementedError):
                        datastore.ingest(FileDataset(path=os.path.abspath(path), refs=ref), transfer=mode)

                if mode in self.ingestTransferModes:
                    self.runIngestTest(failInputDoesNotExist)
                    self.runIngestTest(succeed, expectOutput=(mode != "move"))
                    self.runIngestTest(failOutputExists)
                else:
                    self.runIngestTest(failNotImplemented)

    def testIngestSymlinkOfSymlink(self):
        """Special test for symlink to a symlink ingest"""
        metrics, ref = self._prepareIngestTest()
        # The aim of this test is to create a dataset on disk, then
        # create a symlink to it and finally ingest the symlink such that
        # the symlink in the datastore points to the original dataset.
        for mode in ("symlink", "relsymlink"):
            if mode not in self.ingestTransferModes:
                continue

            print(f"Trying mode {mode}")
            with lsst.utils.tests.getTempFilePath(".yaml") as realpath:
                with open(realpath, 'w') as fd:
                    yaml.dump(metrics._asdict(), stream=fd)
                with lsst.utils.tests.getTempFilePath(".yaml") as sympath:
                    os.symlink(os.path.abspath(realpath), sympath)

                    datastore = self.makeDatastore()
                    datastore.ingest(FileDataset(path=os.path.abspath(sympath), refs=ref), transfer=mode)

                    uri = datastore.getURI(ref)
                    self.assertTrue(uri.isLocal, f"Check {uri.scheme}")
                    self.assertTrue(os.path.islink(uri.ospath), f"Check {uri} is a symlink")

                    linkTarget = os.readlink(uri.ospath)
                    if mode == "relsymlink":
                        self.assertFalse(os.path.isabs(linkTarget))
                    else:
                        self.assertEqual(linkTarget, os.path.abspath(realpath))

                    # Check that we can get the dataset back regardless of mode
                    metric2 = datastore.get(ref)
                    self.assertEqual(metric2, metrics)

                    # Cleanup the file for next time round loop
                    # since it will get the same file name in store
                    datastore.remove(ref)


class PosixDatastoreTestCase(DatastoreTests, unittest.TestCase):
    """PosixDatastore specialization"""
    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")
    uriScheme = "file"
    canIngestNoTransferAuto = True
    ingestTransferModes = (None, "copy", "move", "link", "hardlink", "symlink", "relsymlink", "auto")
    isEphemeral = False
    rootKeys = ("root",)
    validationCanFail = True

    def setUp(self):
        # Override the working directory before calling the base class
        self.root = tempfile.mkdtemp(dir=TESTDIR)
        super().setUp()


class PosixDatastoreNoChecksumsTestCase(PosixDatastoreTestCase):
    """Posix datastore tests but with checksums disabled."""
    configFile = os.path.join(TESTDIR, "config/basic/posixDatastoreNoChecksums.yaml")

    def testChecksum(self):
        """Ensure that checksums have not been calculated."""

        datastore = self.makeDatastore()
        storageClass = self.storageClassFactory.getStorageClass("StructuredData")
        dimensions = self.universe.extract(("visit", "physical_filter"))
        metrics = makeExampleMetrics()

        dataId = {"instrument": "dummy", "visit": 0, "physical_filter": "V"}
        ref = self.makeDatasetRef("metric", dimensions, storageClass, dataId,
                                  conform=False)

        # Configuration should have disabled checksum calculation
        datastore.put(metrics, ref)
        infos = datastore.getStoredItemsInfo(ref)
        self.assertIsNone(infos[0].checksum)

        # Remove put back but with checksums enabled explicitly
        datastore.remove(ref)
        datastore.useChecksum = True
        datastore.put(metrics, ref)

        infos = datastore.getStoredItemsInfo(ref)
        self.assertIsNotNone(infos[0].checksum)


class CleanupPosixDatastoreTestCase(DatastoreTestsBase, unittest.TestCase):
    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")

    def setUp(self):
        # Override the working directory before calling the base class
        self.root = tempfile.mkdtemp(dir=TESTDIR)
        super().setUp()

    def testCleanup(self):
        """Test that a failed formatter write does cleanup a partial file."""
        metrics = makeExampleMetrics()
        datastore = self.makeDatastore()

        storageClass = self.storageClassFactory.getStorageClass("StructuredData")

        dimensions = self.universe.extract(("visit", "physical_filter"))
        dataId = {"instrument": "dummy", "visit": 52, "physical_filter": "V"}

        ref = self.makeDatasetRef("metric", dimensions, storageClass, dataId, conform=False)

        # Determine where the file will end up (we assume Formatters use
        # the same file extension)
        expectedUri = datastore.getURI(ref, predict=True)
        self.assertEqual(expectedUri.fragment, "predicted")

        self.assertEqual(expectedUri.getExtension(), ".yaml",
                         f"Is there a file extension in {expectedUri}")

        # Try formatter that fails and formatter that fails and leaves
        # a file behind
        for formatter in (BadWriteFormatter, BadNoWriteFormatter):
            with self.subTest(formatter=formatter):

                # Monkey patch the formatter
                datastore.formatterFactory.registerFormatter(ref.datasetType, formatter,
                                                             overwrite=True)

                # Try to put the dataset, it should fail
                with self.assertRaises(Exception):
                    datastore.put(metrics, ref)

                # Check that there is no file on disk
                self.assertFalse(expectedUri.exists(), f"Check for existence of {expectedUri}")

                # Check that there is a directory
                dir = expectedUri.dirname()
                self.assertTrue(dir.exists(),
                                f"Check for existence of directory {dir}")

        # Force YamlFormatter and check that this time a file is written
        datastore.formatterFactory.registerFormatter(ref.datasetType, YamlFormatter,
                                                     overwrite=True)
        datastore.put(metrics, ref)
        self.assertTrue(expectedUri.exists(), f"Check for existence of {expectedUri}")
        datastore.remove(ref)
        self.assertFalse(expectedUri.exists(), f"Check for existence of now removed {expectedUri}")


class InMemoryDatastoreTestCase(DatastoreTests, unittest.TestCase):
    """PosixDatastore specialization"""
    configFile = os.path.join(TESTDIR, "config/basic/inMemoryDatastore.yaml")
    uriScheme = "mem"
    hasUnsupportedPut = False
    ingestTransferModes = ()
    isEphemeral = True
    rootKeys = None
    validationCanFail = False


class ChainedDatastoreTestCase(PosixDatastoreTestCase):
    """ChainedDatastore specialization using a POSIXDatastore"""
    configFile = os.path.join(TESTDIR, "config/basic/chainedDatastore.yaml")
    hasUnsupportedPut = False
    canIngestNoTransferAuto = False
    ingestTransferModes = ("copy", "hardlink", "symlink", "relsymlink", "link", "auto")
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

        # Write empty file suitable for ingest check (JSON and YAML variants)
        testfile_y = tempfile.NamedTemporaryFile(suffix=".yaml")
        testfile_j = tempfile.NamedTemporaryFile(suffix=".json")
        for datasetTypeName, sc, accepted in (("metric", sc1, True), ("metric2", sc1, False),
                                              ("metric33", sc1, True), ("metric2", sc2, True)):
            # Choose different temp file depending on StorageClass
            testfile = testfile_j if sc.name.endswith("Json") else testfile_y

            with self.subTest(datasetTypeName=datasetTypeName, storageClass=sc.name, file=testfile.name):
                ref = self.makeDatasetRef(datasetTypeName, dimensions, sc, dataId, conform=False)
                if accepted:
                    datastore.put(metrics, ref)
                    self.assertTrue(datastore.exists(ref))
                    datastore.remove(ref)

                    # Try ingest
                    if self.canIngest:
                        datastore.ingest(FileDataset(testfile.name, [ref]), transfer="link")
                        self.assertTrue(datastore.exists(ref))
                        datastore.remove(ref)
                else:
                    with self.assertRaises(DatasetTypeNotSupportedError):
                        datastore.put(metrics, ref)
                    self.assertFalse(datastore.exists(ref))

                    # Again with ingest
                    if self.canIngest:
                        with self.assertRaises(DatasetTypeNotSupportedError):
                            datastore.ingest(FileDataset(testfile.name, [ref]), transfer="link")
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

        # Write empty file suitable for ingest check (JSON and YAML variants)
        testfile_y = tempfile.NamedTemporaryFile(suffix=".yaml")
        testfile_j = tempfile.NamedTemporaryFile(suffix=".json")

        for typeName, dataId, sc, accept, ingest in (("metric", dataId1, sc1, (False, True, False), True),
                                                     ("metric2", dataId1, sc1, (False, False, False), False),
                                                     ("metric2", dataId2, sc1, (True, False, False), False),
                                                     ("metric33", dataId2, sc2, (True, True, False), True),
                                                     ("metric2", dataId1, sc2, (False, True, False), True)):

            # Choose different temp file depending on StorageClass
            testfile = testfile_j if sc.name.endswith("Json") else testfile_y

            with self.subTest(datasetTypeName=typeName, dataId=dataId, sc=sc.name):
                ref = self.makeDatasetRef(typeName, dimensions, sc, dataId,
                                          conform=False)
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
                        datastore.ingest(FileDataset(testfile.name, [ref]), transfer="link")
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
                            datastore.ingest(FileDataset(testfile.name, [ref]), transfer="link")

                else:
                    with self.assertRaises(DatasetTypeNotSupportedError):
                        datastore.put(metrics, ref)
                    self.assertFalse(datastore.exists(ref))

                    # Again with ingest
                    with self.assertRaises(DatasetTypeNotSupportedError):
                        datastore.ingest(FileDataset(testfile.name, [ref]), transfer="link")
                    self.assertFalse(datastore.exists(ref))


if __name__ == "__main__":
    unittest.main()

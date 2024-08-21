# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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

from __future__ import annotations

import contextlib
import os
import pickle
import shutil
import tempfile
import time
import unittest
import unittest.mock
import uuid
from collections.abc import Callable, Iterator
from typing import Any, cast

import lsst.utils.tests
import yaml
from lsst.daf.butler import (
    Config,
    DataCoordinate,
    DatasetIdGenEnum,
    DatasetRef,
    DatasetType,
    DatasetTypeNotSupportedError,
    Datastore,
    DimensionUniverse,
    FileDataset,
    StorageClass,
    StorageClassFactory,
)
from lsst.daf.butler.datastore import DatasetRefURIs, DatastoreConfig, DatastoreValidationError, NullDatastore
from lsst.daf.butler.datastore.cache_manager import (
    DatastoreCacheManager,
    DatastoreCacheManagerConfig,
    DatastoreDisabledCacheManager,
)
from lsst.daf.butler.datastore.stored_file_info import StoredFileInfo
from lsst.daf.butler.formatters.yaml import YamlFormatter
from lsst.daf.butler.tests import (
    BadNoWriteFormatter,
    BadWriteFormatter,
    DatasetTestHelper,
    DatastoreTestHelper,
    DummyRegistry,
    MetricsExample,
    MetricsExampleDataclass,
    MetricsExampleModel,
)
from lsst.daf.butler.tests.dict_convertible_model import DictConvertibleModel
from lsst.daf.butler.tests.utils import TestCaseMixin
from lsst.resources import ResourcePath
from lsst.utils import doImport

TESTDIR = os.path.dirname(__file__)


def makeExampleMetrics(use_none: bool = False) -> MetricsExample:
    """Make example dataset that can be stored in butler."""
    if use_none:
        array = None
    else:
        array = [563, 234, 456.7, 105, 2054, -1045]
    return MetricsExample(
        {"AM1": 5.2, "AM2": 30.6},
        {"a": [1, 2, 3], "b": {"blue": 5, "red": "green"}},
        array,
    )


class TransactionTestError(Exception):
    """Specific error for transactions, to prevent misdiagnosing
    that might otherwise occur when a standard exception is used.
    """

    pass


class DatastoreTestsBase(DatasetTestHelper, DatastoreTestHelper, TestCaseMixin):
    """Support routines for datastore testing"""

    root: str | None = None
    universe: DimensionUniverse
    storageClassFactory: StorageClassFactory

    @classmethod
    def setUpClass(cls) -> None:
        # Storage Classes are fixed for all datastores in these tests
        scConfigFile = os.path.join(TESTDIR, "config/basic/storageClasses.yaml")
        cls.storageClassFactory = StorageClassFactory()
        cls.storageClassFactory.addFromConfig(scConfigFile)

        # Read the Datastore config so we can get the class
        # information (since we should not assume the constructor
        # name here, but rely on the configuration file itself)
        datastoreConfig = DatastoreConfig(cls.configFile)
        cls.datastoreType = cast(type[Datastore], doImport(datastoreConfig["cls"]))
        cls.universe = DimensionUniverse()

    def setUp(self) -> None:
        self.setUpDatastoreTests(DummyRegistry, DatastoreConfig)

    def tearDown(self) -> None:
        if self.root is not None and os.path.exists(self.root):
            shutil.rmtree(self.root, ignore_errors=True)


class DatastoreTests(DatastoreTestsBase):
    """Some basic tests of a simple datastore."""

    hasUnsupportedPut = True
    rootKeys: tuple[str, ...] | None = None
    isEphemeral: bool = False
    validationCanFail: bool = False

    def testConfigRoot(self) -> None:
        full = DatastoreConfig(self.configFile)
        config = DatastoreConfig(self.configFile, mergeDefaults=False)
        newroot = "/random/location"
        self.datastoreType.setConfigRoot(newroot, config, full)
        if self.rootKeys:
            for k in self.rootKeys:
                self.assertIn(newroot, config[k])

    def testConstructor(self) -> None:
        datastore = self.makeDatastore()
        self.assertIsNotNone(datastore)
        self.assertIs(datastore.isEphemeral, self.isEphemeral)

    def testConfigurationValidation(self) -> None:
        datastore = self.makeDatastore()
        sc = self.storageClassFactory.getStorageClass("ThingOne")
        datastore.validateConfiguration([sc])

        sc2 = self.storageClassFactory.getStorageClass("ThingTwo")
        if self.validationCanFail:
            with self.assertRaises(DatastoreValidationError):
                datastore.validateConfiguration([sc2], logFailures=True)

        dimensions = self.universe.conform(("visit", "physical_filter"))
        dataId = {
            "instrument": "dummy",
            "visit": 52,
            "physical_filter": "V",
            "band": "v",
            "day_obs": 20250101,
        }
        ref = self.makeDatasetRef("metric", dimensions, sc, dataId)
        datastore.validateConfiguration([ref])

    def testParameterValidation(self) -> None:
        """Check that parameters are validated"""
        sc = self.storageClassFactory.getStorageClass("ThingOne")
        dimensions = self.universe.conform(("visit", "physical_filter"))
        dataId = {
            "instrument": "dummy",
            "visit": 52,
            "physical_filter": "V",
            "band": "v",
            "day_obs": 20250101,
        }
        ref = self.makeDatasetRef("metric", dimensions, sc, dataId)
        datastore = self.makeDatastore()
        data = {1: 2, 3: 4}
        datastore.put(data, ref)
        newdata = datastore.get(ref)
        self.assertEqual(data, newdata)
        with self.assertRaises(KeyError):
            newdata = datastore.get(ref, parameters={"missing": 5})

    def testBasicPutGet(self) -> None:
        metrics = makeExampleMetrics()
        datastore = self.makeDatastore()

        # Create multiple storage classes for testing different formulations
        storageClasses = [
            self.storageClassFactory.getStorageClass(sc)
            for sc in ("StructuredData", "StructuredDataJson", "StructuredDataPickle")
        ]

        dimensions = self.universe.conform(("visit", "physical_filter"))
        dataId = {
            "instrument": "dummy",
            "visit": 52,
            "physical_filter": "V",
            "band": "v",
            "day_obs": 20250101,
        }
        dataId2 = {
            "instrument": "dummy",
            "visit": 53,
            "physical_filter": "V",
            "band": "v",
            "day_obs": 20250101,
        }

        for sc in storageClasses:
            ref = self.makeDatasetRef("metric", dimensions, sc, dataId)
            ref2 = self.makeDatasetRef("metric", dimensions, sc, dataId2)

            # Make sure that using getManyURIs without predicting before the
            # dataset has been put raises.
            with self.assertRaises(FileNotFoundError):
                datastore.getManyURIs([ref], predict=False)

            # Make sure that using getManyURIs with predicting before the
            # dataset has been put predicts the URI.
            uris = datastore.getManyURIs([ref, ref2], predict=True)
            self.assertIn("52", uris[ref].primaryURI.geturl())
            self.assertIn("#predicted", uris[ref].primaryURI.geturl())
            self.assertIn("53", uris[ref2].primaryURI.geturl())
            self.assertIn("#predicted", uris[ref2].primaryURI.geturl())

            datastore.put(metrics, ref)

            # Does it exist?
            self.assertTrue(datastore.exists(ref))
            self.assertTrue(datastore.knows(ref))
            multi = datastore.knows_these([ref])
            self.assertTrue(multi[ref])
            multi = datastore.mexists([ref, ref2])
            self.assertTrue(multi[ref])
            self.assertFalse(multi[ref2])

            # Get
            metricsOut = datastore.get(ref, parameters=None)
            self.assertEqual(metrics, metricsOut)

            uri = datastore.getURI(ref)
            self.assertEqual(uri.scheme, self.uriScheme)

            uris = datastore.getManyURIs([ref])
            self.assertEqual(len(uris), 1)
            ref, uri = uris.popitem()
            self.assertTrue(uri.primaryURI.exists())
            self.assertFalse(uri.componentURIs)

            # Get a component -- we need to construct new refs for them
            # with derived storage classes but with parent ID
            for comp in ("data", "output"):
                compRef = ref.makeComponentRef(comp)
                output = datastore.get(compRef)
                self.assertEqual(output, getattr(metricsOut, comp))

                uri = datastore.getURI(compRef)
                self.assertEqual(uri.scheme, self.uriScheme)

                uris = datastore.getManyURIs([compRef])
                self.assertEqual(len(uris), 1)

        storageClass = sc

        # Check that we can put a metric with None in a component and
        # get it back as None
        metricsNone = makeExampleMetrics(use_none=True)
        dataIdNone = {
            "instrument": "dummy",
            "visit": 54,
            "physical_filter": "V",
            "band": "v",
            "day_obs": 20250101,
        }
        refNone = self.makeDatasetRef("metric", dimensions, sc, dataIdNone)
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
        ref = self.makeDatasetRef("metrics", dimensions, storageClass, dataId)
        with self.assertRaises(FileNotFoundError):
            # non-existing file
            datastore.get(ref)

        # Get a URI from it
        uri = datastore.getURI(ref, predict=True)
        self.assertEqual(uri.scheme, self.uriScheme)

        with self.assertRaises(FileNotFoundError):
            datastore.getURI(ref)

    def testTrustGetRequest(self) -> None:
        """Check that we can get datasets that registry knows nothing about."""
        datastore = self.makeDatastore()

        # Skip test if the attribute is not defined
        if not hasattr(datastore, "trustGetRequest"):
            return

        metrics = makeExampleMetrics()

        i = 0
        for sc_name in ("StructuredDataNoComponents", "StructuredData", "StructuredComposite"):
            i += 1
            datasetTypeName = f"test_metric{i}"  # Different dataset type name each time.

            if sc_name == "StructuredComposite":
                disassembled = True
            else:
                disassembled = False

            # Start datastore in default configuration of using registry
            datastore.trustGetRequest = False

            # Create multiple storage classes for testing with or without
            # disassembly
            sc = self.storageClassFactory.getStorageClass(sc_name)
            dimensions = self.universe.conform(("visit", "physical_filter"))

            dataId = {
                "instrument": "dummy",
                "visit": 52 + i,
                "physical_filter": "V",
                "band": "v",
                "day_obs": 20250101,
            }

            ref = self.makeDatasetRef(datasetTypeName, dimensions, sc, dataId)
            datastore.put(metrics, ref)

            # Does it exist?
            self.assertTrue(datastore.exists(ref))
            self.assertTrue(datastore.knows(ref))
            multi = datastore.knows_these([ref])
            self.assertTrue(multi[ref])
            multi = datastore.mexists([ref])
            self.assertTrue(multi[ref])

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
            self.assertFalse(datastore.knows(ref))
            multi = datastore.knows_these([ref])
            self.assertFalse(multi[ref])
            multi = datastore.mexists([ref])
            self.assertFalse(multi[ref])

            with self.assertRaises(FileNotFoundError):
                datastore.get(ref)

            if sc_name != "StructuredDataNoComponents":
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
            if sc_name != "StructuredDataNoComponents":
                comp = "data"
                compRef = ref.makeComponentRef(comp)
                output = datastore.get(compRef)
                self.assertEqual(output, getattr(metrics, comp))

            # Get the URI -- if we trust this should work even without
            # enabling prediction.
            primaryURI2, componentURIs2 = datastore.getURIs(ref)
            self.assertEqual(primaryURI2, primaryURI)
            self.assertEqual(componentURIs2, componentURIs)

            # Check for compatible storage class.
            if sc_name in ("StructuredDataNoComponents", "StructuredData"):
                # Make new dataset ref with compatible storage class.
                ref_comp = ref.overrideStorageClass("StructuredDataDictJson")

                # Without `set_retrieve_dataset_type_method` it will fail to
                # find correct file.
                self.assertFalse(datastore.exists(ref_comp))
                with self.assertRaises(FileNotFoundError):
                    datastore.get(ref_comp)
                with self.assertRaises(FileNotFoundError):
                    datastore.get(ref, storageClass="StructuredDataDictJson")

                # Need a special method to generate stored dataset type.
                def _stored_dataset_type(name: str, ref: DatasetRef = ref) -> DatasetType:
                    if name == ref.datasetType.name:
                        return ref.datasetType
                    raise ValueError(f"Unexpected dataset type name {ref.datasetType.name}")

                datastore.set_retrieve_dataset_type_method(_stored_dataset_type)

                # Storage class override with original dataset ref.
                metrics_as_dict = datastore.get(ref, storageClass="StructuredDataDictJson")
                self.assertIsInstance(metrics_as_dict, dict)

                # get() should return a dict now.
                metrics_as_dict = datastore.get(ref_comp)
                self.assertIsInstance(metrics_as_dict, dict)

                # exists() should work as well.
                self.assertTrue(datastore.exists(ref_comp))

                datastore.set_retrieve_dataset_type_method(None)

    def testDisassembly(self) -> None:
        """Test disassembly within datastore."""
        metrics = makeExampleMetrics()
        if self.isEphemeral:
            # in-memory datastore does not disassemble
            return

        # Create multiple storage classes for testing different formulations
        # of composites. One of these will not disassemble to provide
        # a reference.
        storageClasses = [
            self.storageClassFactory.getStorageClass(sc)
            for sc in (
                "StructuredComposite",
                "StructuredCompositeTestA",
                "StructuredCompositeTestB",
                "StructuredCompositeReadComp",
                "StructuredData",  # No disassembly
                "StructuredCompositeReadCompNoDisassembly",
            )
        ]

        # Create the test datastore
        datastore = self.makeDatastore()

        # Dummy dataId
        dimensions = self.universe.conform(("visit", "physical_filter"))
        dataId = {"instrument": "dummy", "visit": 428, "physical_filter": "R"}

        for i, sc in enumerate(storageClasses):
            with self.subTest(storageClass=sc.name):
                # Create a different dataset type each time round
                # so that a test failure in this subtest does not trigger
                # a cascade of tests because of file clashes
                ref = self.makeDatasetRef(f"metric_comp_{i}", dimensions, sc, dataId)

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

    def prepDeleteTest(self, n_refs: int = 1) -> tuple[Datastore, tuple[DatasetRef, ...]]:
        metrics = makeExampleMetrics()
        datastore = self.makeDatastore()
        # Put
        dimensions = self.universe.conform(("visit", "physical_filter"))
        sc = self.storageClassFactory.getStorageClass("StructuredData")
        refs = []
        for i in range(n_refs):
            dataId = {
                "instrument": "dummy",
                "visit": 638 + i,
                "physical_filter": "U",
                "band": "u",
                "day_obs": 20250101,
            }
            ref = self.makeDatasetRef("metric", dimensions, sc, dataId)
            datastore.put(metrics, ref)

            # Does it exist?
            self.assertTrue(datastore.exists(ref))

            # Get
            metricsOut = datastore.get(ref)
            self.assertEqual(metrics, metricsOut)
            refs.append(ref)

        return datastore, *refs

    def testRemove(self) -> None:
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

    def testForget(self) -> None:
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

    def testTransfer(self) -> None:
        metrics = makeExampleMetrics()

        dimensions = self.universe.conform(("visit", "physical_filter"))
        dataId = {
            "instrument": "dummy",
            "visit": 2048,
            "physical_filter": "Uprime",
            "band": "u",
            "day_obs": 20250101,
        }

        sc = self.storageClassFactory.getStorageClass("StructuredData")
        ref = self.makeDatasetRef("metric", dimensions, sc, dataId)

        inputDatastore = self.makeDatastore("test_input_datastore")
        outputDatastore = self.makeDatastore("test_output_datastore")

        inputDatastore.put(metrics, ref)
        outputDatastore.transfer(inputDatastore, ref)

        metricsOut = outputDatastore.get(ref)
        self.assertEqual(metrics, metricsOut)

    def testBasicTransaction(self) -> None:
        datastore = self.makeDatastore()
        storageClass = self.storageClassFactory.getStorageClass("StructuredData")
        dimensions = self.universe.conform(("visit", "physical_filter"))
        nDatasets = 6
        dataIds = [
            {"instrument": "dummy", "visit": i, "physical_filter": "V", "band": "v", "day_obs": 20250101}
            for i in range(nDatasets)
        ]
        data = [
            (
                self.makeDatasetRef("metric", dimensions, storageClass, dataId),
                makeExampleMetrics(),
            )
            for dataId in dataIds
        ]
        succeed = data[: nDatasets // 2]
        fail = data[nDatasets // 2 :]
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

    def testNestedTransaction(self) -> None:
        datastore = self.makeDatastore()
        storageClass = self.storageClassFactory.getStorageClass("StructuredData")
        dimensions = self.universe.conform(("visit", "physical_filter"))
        metrics = makeExampleMetrics()

        dataId = {"instrument": "dummy", "visit": 0, "physical_filter": "V", "band": "v", "day_obs": 20250101}
        refBefore = self.makeDatasetRef("metric", dimensions, storageClass, dataId)
        datastore.put(metrics, refBefore)
        with self.assertRaises(TransactionTestError):
            with datastore.transaction():
                dataId = {
                    "instrument": "dummy",
                    "visit": 1,
                    "physical_filter": "V",
                    "band": "v",
                    "day_obs": 20250101,
                }
                refOuter = self.makeDatasetRef("metric", dimensions, storageClass, dataId)
                datastore.put(metrics, refOuter)
                with datastore.transaction():
                    dataId = {
                        "instrument": "dummy",
                        "visit": 2,
                        "physical_filter": "V",
                        "band": "v",
                        "day_obs": 20250101,
                    }
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

    def _prepareIngestTest(self) -> tuple[MetricsExample, DatasetRef]:
        storageClass = self.storageClassFactory.getStorageClass("StructuredData")
        dimensions = self.universe.conform(("visit", "physical_filter"))
        metrics = makeExampleMetrics()
        dataId = {"instrument": "dummy", "visit": 0, "physical_filter": "V", "band": "v", "day_obs": 20250101}
        ref = self.makeDatasetRef("metric", dimensions, storageClass, dataId)
        return metrics, ref

    def runIngestTest(self, func: Callable[[MetricsExample, str, DatasetRef], None]) -> None:
        metrics, ref = self._prepareIngestTest()
        # The file will be deleted after the test.
        # For symlink tests this leads to a situation where the datastore
        # points to a file that does not exist. This will make os.path.exist
        # return False but then the new symlink will fail with
        # FileExistsError later in the code so the test still passes.
        with _temp_yaml_file(metrics._asdict()) as path:
            func(metrics, path, ref)

    def testIngestNoTransfer(self) -> None:
        """Test ingesting existing files with no transfer."""
        for mode in (None, "auto"):
            # Some datastores have auto but can't do in place transfer
            if mode == "auto" and "auto" in self.ingestTransferModes and not self.canIngestNoTransferAuto:
                continue

            with self.subTest(mode=mode):
                datastore = self.makeDatastore()

                def succeed(
                    obj: MetricsExample,
                    path: str,
                    ref: DatasetRef,
                    mode: str | None = mode,
                    datastore: Datastore = datastore,
                ) -> None:
                    """Ingest a file already in the datastore root."""
                    # first move it into the root, and adjust the path
                    # accordingly
                    path = shutil.copy(path, datastore.root.ospath)
                    path = os.path.relpath(path, start=datastore.root.ospath)
                    datastore.ingest(FileDataset(path=path, refs=ref), transfer=mode)
                    self.assertEqual(obj, datastore.get(ref))

                def failInputDoesNotExist(
                    obj: MetricsExample,
                    path: str,
                    ref: DatasetRef,
                    mode: str | None = mode,
                    datastore: Datastore = datastore,
                ) -> None:
                    """Can't ingest files if we're given a bad path."""
                    with self.assertRaises(FileNotFoundError):
                        datastore.ingest(
                            FileDataset(path="this-file-does-not-exist.yaml", refs=ref), transfer=mode
                        )
                    self.assertFalse(datastore.exists(ref))

                def failOutsideRoot(
                    obj: MetricsExample,
                    path: str,
                    ref: DatasetRef,
                    mode: str | None = mode,
                    datastore: Datastore = datastore,
                ) -> None:
                    """Can't ingest files outside of datastore root unless
                    auto.
                    """
                    if mode == "auto":
                        datastore.ingest(FileDataset(path=os.path.abspath(path), refs=ref), transfer=mode)
                        self.assertTrue(datastore.exists(ref))
                    else:
                        with self.assertRaises(RuntimeError):
                            datastore.ingest(FileDataset(path=os.path.abspath(path), refs=ref), transfer=mode)
                        self.assertFalse(datastore.exists(ref))

                def failNotImplemented(
                    obj: MetricsExample,
                    path: str,
                    ref: DatasetRef,
                    mode: str | None = mode,
                    datastore: Datastore = datastore,
                ) -> None:
                    with self.assertRaises(NotImplementedError):
                        datastore.ingest(FileDataset(path=path, refs=ref), transfer=mode)

                if mode in self.ingestTransferModes:
                    self.runIngestTest(failOutsideRoot)
                    self.runIngestTest(failInputDoesNotExist)
                    self.runIngestTest(succeed)
                else:
                    self.runIngestTest(failNotImplemented)

    def testIngestTransfer(self) -> None:
        """Test ingesting existing files after transferring them."""
        for mode in ("copy", "move", "link", "hardlink", "symlink", "relsymlink", "auto"):
            with self.subTest(mode=mode):
                datastore = self.makeDatastore(mode)

                def succeed(
                    obj: MetricsExample,
                    path: str,
                    ref: DatasetRef,
                    mode: str | None = mode,
                    datastore: Datastore = datastore,
                ) -> None:
                    """Ingest a file by transferring it to the template
                    location.
                    """
                    datastore.ingest(FileDataset(path=os.path.abspath(path), refs=ref), transfer=mode)
                    self.assertEqual(obj, datastore.get(ref))
                    file_exists = os.path.exists(path)
                    if mode == "move":
                        self.assertFalse(file_exists)
                    else:
                        self.assertTrue(file_exists)

                def failInputDoesNotExist(
                    obj: MetricsExample,
                    path: str,
                    ref: DatasetRef,
                    mode: str | None = mode,
                    datastore: Datastore = datastore,
                ) -> None:
                    """Can't ingest files if we're given a bad path."""
                    with self.assertRaises(FileNotFoundError):
                        # Ensure the file does not look like it is in
                        # datastore for auto mode
                        datastore.ingest(
                            FileDataset(path="../this-file-does-not-exist.yaml", refs=ref), transfer=mode
                        )
                    self.assertFalse(datastore.exists(ref), f"Checking not in datastore using mode {mode}")

                def failNotImplemented(
                    obj: MetricsExample,
                    path: str,
                    ref: DatasetRef,
                    mode: str | None = mode,
                    datastore: Datastore = datastore,
                ) -> None:
                    with self.assertRaises(NotImplementedError):
                        datastore.ingest(FileDataset(path=os.path.abspath(path), refs=ref), transfer=mode)

                if mode in self.ingestTransferModes:
                    self.runIngestTest(failInputDoesNotExist)
                    self.runIngestTest(succeed)
                else:
                    self.runIngestTest(failNotImplemented)

    def testIngestSymlinkOfSymlink(self) -> None:
        """Special test for symlink to a symlink ingest"""
        metrics, ref = self._prepareIngestTest()
        # The aim of this test is to create a dataset on disk, then
        # create a symlink to it and finally ingest the symlink such that
        # the symlink in the datastore points to the original dataset.
        for mode in ("symlink", "relsymlink"):
            if mode not in self.ingestTransferModes:
                continue

            print(f"Trying mode {mode}")
            with _temp_yaml_file(metrics._asdict()) as realpath:
                with tempfile.TemporaryDirectory() as tmpdir:
                    sympath = os.path.join(tmpdir, "symlink.yaml")
                    os.symlink(os.path.realpath(realpath), sympath)

                    datastore = self.makeDatastore()
                    datastore.ingest(FileDataset(path=os.path.abspath(sympath), refs=ref), transfer=mode)

                    uri = datastore.getURI(ref)
                    self.assertTrue(uri.isLocal, f"Check {uri.scheme}")
                    self.assertTrue(os.path.islink(uri.ospath), f"Check {uri} is a symlink")

                    linkTarget = os.readlink(uri.ospath)
                    if mode == "relsymlink":
                        self.assertFalse(os.path.isabs(linkTarget))
                    else:
                        self.assertTrue(os.path.samefile(linkTarget, realpath))

                    # Check that we can get the dataset back regardless of mode
                    metric2 = datastore.get(ref)
                    self.assertEqual(metric2, metrics)

                    # Cleanup the file for next time round loop
                    # since it will get the same file name in store
                    datastore.remove(ref)

    def _populate_export_datastore(self, name: str) -> tuple[Datastore, list[DatasetRef]]:
        datastore = self.makeDatastore(name)

        # For now only the FileDatastore can be used for this test.
        # ChainedDatastore that only includes InMemoryDatastores have to be
        # skipped as well.
        for name in datastore.names:
            if not name.startswith("InMemoryDatastore"):
                break
        else:
            raise unittest.SkipTest("in-memory datastore does not support record export/import")

        metrics = makeExampleMetrics()
        dimensions = self.universe.conform(("visit", "physical_filter"))
        sc = self.storageClassFactory.getStorageClass("StructuredData")

        refs = []
        for visit in (2048, 2049, 2050):
            dataId = {
                "instrument": "dummy",
                "visit": visit,
                "physical_filter": "Uprime",
                "band": "u",
                "day_obs": 20250101,
            }
            ref = self.makeDatasetRef("metric", dimensions, sc, dataId)
            datastore.put(metrics, ref)
            refs.append(ref)
        return datastore, refs

    def testExportImportRecords(self) -> None:
        """Test for export_records and import_records methods."""
        datastore, refs = self._populate_export_datastore("test_datastore")
        for exported_refs in (refs, refs[1:]):
            n_refs = len(exported_refs)
            records = datastore.export_records(exported_refs)
            self.assertGreater(len(records), 0)
            self.assertTrue(set(records.keys()) <= set(datastore.names))
            # In a ChainedDatastore each FileDatastore will have a complete set
            for datastore_name in records:
                record_data = records[datastore_name]
                self.assertEqual(len(record_data.records), n_refs)

                # Check that subsetting works, include non-existing dataset ID.
                dataset_ids = {exported_refs[0].id, uuid.uuid4()}
                subset = record_data.subset(dataset_ids)
                assert subset is not None
                self.assertEqual(len(subset.records), 1)
                subset = record_data.subset({uuid.uuid4()})
                self.assertIsNone(subset)

        # Use the same datastore name to import relative path.
        datastore2 = self.makeDatastore("test_datastore")

        records = datastore.export_records(refs[1:])
        datastore2.import_records(records)

        with self.assertRaises(FileNotFoundError):
            data = datastore2.get(refs[0])
        data = datastore2.get(refs[1])
        self.assertIsNotNone(data)
        data = datastore2.get(refs[2])
        self.assertIsNotNone(data)

    def testExport(self) -> None:
        datastore, refs = self._populate_export_datastore("test_datastore")

        datasets = list(datastore.export(refs))
        self.assertEqual(len(datasets), 3)

        for transfer in (None, "auto"):
            # Both will default to None
            datasets = list(datastore.export(refs, transfer=transfer))
            self.assertEqual(len(datasets), 3)

        with self.assertRaises(TypeError):
            list(datastore.export(refs, transfer="copy"))

        with self.assertRaises(TypeError):
            list(datastore.export(refs, directory="exportDir", transfer="move"))

        # Create a new ref that is not known to the datastore and try to
        # export it.
        sc = self.storageClassFactory.getStorageClass("ThingOne")
        dimensions = self.universe.conform(("visit", "physical_filter"))
        dataId = {
            "instrument": "dummy",
            "visit": 52,
            "physical_filter": "V",
            "band": "v",
            "day_obs": 20250101,
        }
        ref = self.makeDatasetRef("metric", dimensions, sc, dataId)
        with self.assertRaises(FileNotFoundError):
            list(datastore.export(refs + [ref], transfer=None))

    def test_pydantic_dict_storage_class_conversions(self) -> None:
        """Test converting a dataset stored as a pydantic model into a dict on
        read.
        """
        datastore = self.makeDatastore()
        store_as_model = self.makeDatasetRef(
            "store_as_model",
            dimensions=self.universe.empty,
            storageClass="DictConvertibleModel",
            dataId=DataCoordinate.make_empty(self.universe),
        )
        content = {"a": "one", "b": "two"}
        model = DictConvertibleModel.from_dict(content, extra="original content")
        datastore.put(model, store_as_model)
        retrieved_model = datastore.get(store_as_model)
        self.assertEqual(retrieved_model, model)
        loaded = datastore.get(store_as_model.overrideStorageClass("NativeDictForConvertibleModel"))
        self.assertEqual(type(loaded), dict)
        self.assertEqual(loaded, content)

    def test_simple_class_put_get(self) -> None:
        """Test that we can put and get a simple class with dict()
        constructor.
        """
        datastore = self.makeDatastore()
        data = MetricsExample(summary={"a": 1}, data=[1, 2, 3], output={"b": 2})
        self._assert_different_puts(datastore, "MetricsExample", data)

    def test_dataclass_put_get(self) -> None:
        """Test that we can put and get a simple dataclass."""
        datastore = self.makeDatastore()
        data = MetricsExampleDataclass(summary={"a": 1}, data=[1, 2, 3], output={"b": 2})
        self._assert_different_puts(datastore, "MetricsExampleDataclass", data)

    def test_pydantic_put_get(self) -> None:
        """Test that we can put and get a simple Pydantic model."""
        datastore = self.makeDatastore()
        data = MetricsExampleModel(summary={"a": 1}, data=[1, 2, 3], output={"b": 2})
        self._assert_different_puts(datastore, "MetricsExampleModel", data)

    def test_tuple_put_get(self) -> None:
        """Test that we can put and get a tuple."""
        datastore = self.makeDatastore()
        data = ("a", "b", 1)
        self._assert_different_puts(datastore, "TupleExample", data)

    def _assert_different_puts(self, datastore: Datastore, storageClass_root: str, data: Any) -> None:
        refs = {
            x: self.makeDatasetRef(
                f"stora_as_{x}",
                dimensions=self.universe.empty,
                storageClass=f"{storageClass_root}{x}",
                dataId=DataCoordinate.make_empty(self.universe),
            )
            for x in ["A", "B"]
        }

        for ref in refs.values():
            datastore.put(data, ref)

        self.assertEqual(datastore.get(refs["A"]), datastore.get(refs["B"]))


class PosixDatastoreTestCase(DatastoreTests, unittest.TestCase):
    """PosixDatastore specialization"""

    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")
    uriScheme = "file"
    canIngestNoTransferAuto = True
    ingestTransferModes = (None, "copy", "move", "link", "hardlink", "symlink", "relsymlink", "auto")
    isEphemeral = False
    rootKeys = ("root",)
    validationCanFail = True

    def setUp(self) -> None:
        # The call to os.path.realpath is necessary because Mac temporary files
        # can end up in either /private/var/folders or /var/folders, which
        # refer to the same location but don't appear to.
        # This matters for "relsymlink" transfer mode, because it needs to be
        # able to read the file through a relative symlink, but some of the
        # intermediate directories are not traversable if you try to get from a
        # tempfile in /var/folders to one in /private/var/folders via a
        # relative path.
        self.root = os.path.realpath(self.enterContext(tempfile.TemporaryDirectory()))
        super().setUp()

    def testAtomicWrite(self) -> None:
        """Test that we write to a temporary and then rename"""
        datastore = self.makeDatastore()
        storageClass = self.storageClassFactory.getStorageClass("StructuredData")
        dimensions = self.universe.conform(("visit", "physical_filter"))
        metrics = makeExampleMetrics()

        dataId = {"instrument": "dummy", "visit": 0, "physical_filter": "V", "band": "v", "day_obs": 20250101}
        ref = self.makeDatasetRef("metric", dimensions, storageClass, dataId)

        with self.assertLogs("lsst.resources", "DEBUG") as cm:
            datastore.put(metrics, ref)
        move_logs = [ll for ll in cm.output if "transfer=" in ll]
        self.assertIn("transfer=move", move_logs[0])

        # And the transfer should be file to file.
        self.assertEqual(move_logs[0].count("file://"), 2)

    def testCanNotDeterminePutFormatterLocation(self) -> None:
        """Verify that the expected exception is raised if the FileDatastore
        can not determine the put formatter location.
        """
        _ = makeExampleMetrics()
        datastore = self.makeDatastore()

        # Create multiple storage classes for testing different formulations
        storageClass = self.storageClassFactory.getStorageClass("StructuredData")

        sccomp = StorageClass("Dummy")
        compositeStorageClass = StorageClass(
            "StructuredComposite", components={"dummy": sccomp, "dummy2": sccomp}
        )

        dimensions = self.universe.conform(("visit", "physical_filter"))
        dataId = {
            "instrument": "dummy",
            "visit": 52,
            "physical_filter": "V",
            "band": "v",
            "day_obs": 20250101,
        }

        ref = self.makeDatasetRef("metric", dimensions, storageClass, dataId)
        compRef = self.makeDatasetRef("metric", dimensions, compositeStorageClass, dataId)

        def raiser(ref: DatasetRef) -> None:
            raise DatasetTypeNotSupportedError()

        with unittest.mock.patch.object(
            lsst.daf.butler.datastores.fileDatastore.FileDatastore,
            "_determine_put_formatter_location",
            side_effect=raiser,
        ):
            # verify the non-composite ref execution path:
            with self.assertRaises(DatasetTypeNotSupportedError):
                datastore.getURIs(ref, predict=True)

            # verify the composite-ref execution path:
            with self.assertRaises(DatasetTypeNotSupportedError):
                datastore.getURIs(compRef, predict=True)

    def test_roots(self):
        datastore = self.makeDatastore()

        self.assertEqual(set(datastore.names), set(datastore.roots.keys()))
        for root in datastore.roots.values():
            if root is not None:
                self.assertTrue(root.exists())

    def test_prepare_get_for_external_client(self):
        datastore = self.makeDatastore()
        storageClass = self.storageClassFactory.getStorageClass("StructuredData")
        dimensions = self.universe.conform(("visit", "physical_filter"))
        dataId = {"instrument": "dummy", "visit": 52, "physical_filter": "V", "band": "v"}
        ref = self.makeDatasetRef("metric", dimensions, storageClass, dataId)
        # Most of the coverage for this function is in test_server.py,
        # because it requires a file backend that supports URL signing.
        self.assertIsNone(datastore.prepare_get_for_external_client(ref))


class PosixDatastoreNoChecksumsTestCase(PosixDatastoreTestCase):
    """Posix datastore tests but with checksums disabled."""

    configFile = os.path.join(TESTDIR, "config/basic/posixDatastoreNoChecksums.yaml")

    def testChecksum(self) -> None:
        """Ensure that checksums have not been calculated."""
        datastore = self.makeDatastore()
        storageClass = self.storageClassFactory.getStorageClass("StructuredData")
        dimensions = self.universe.conform(("visit", "physical_filter"))
        metrics = makeExampleMetrics()

        dataId = {"instrument": "dummy", "visit": 0, "physical_filter": "V", "band": "v", "day_obs": 20250101}
        ref = self.makeDatasetRef("metric", dimensions, storageClass, dataId)

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

    def test_repeat_ingest(self):
        """Test that repeatedly ingesting the same file in direct mode
        is allowed.

        Test can only run with FileDatastore since that is the only one
        supporting "direct" ingest.
        """
        metrics, v4ref = self._prepareIngestTest()
        datastore = self.makeDatastore()
        v5ref = DatasetRef(
            v4ref.datasetType, v4ref.dataId, v4ref.run, id_generation_mode=DatasetIdGenEnum.DATAID_TYPE_RUN
        )

        with _temp_yaml_file(metrics._asdict()) as path:
            datastore.ingest(FileDataset(path=path, refs=v4ref), transfer="direct")

            # This will fail because the ref is using UUIDv4.
            with self.assertRaises(RuntimeError):
                datastore.ingest(FileDataset(path=path, refs=v4ref), transfer="direct")

            # UUIDv5 can be repeatedly ingested in direct mode.
            datastore.ingest(FileDataset(path=path, refs=v5ref), transfer="direct")
            datastore.ingest(FileDataset(path=path, refs=v5ref), transfer="direct")

            with self.assertRaises(RuntimeError):
                datastore.ingest(FileDataset(path=path, refs=v5ref), transfer="copy")


class TrashDatastoreTestCase(PosixDatastoreTestCase):
    """Restrict trash test to FileDatastore."""

    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")

    def testTrash(self) -> None:
        datastore, *refs = self.prepDeleteTest(n_refs=10)

        # Trash one of them.
        ref = refs.pop()
        uri = datastore.getURI(ref)
        datastore.trash(ref)
        self.assertTrue(uri.exists(), uri)  # Not deleted yet
        datastore.emptyTrash()
        self.assertFalse(uri.exists(), uri)

        # Trash it again should be fine.
        datastore.trash(ref)

        # Trash multiple items at once.
        subset = [refs.pop(), refs.pop()]
        datastore.trash(subset)
        datastore.emptyTrash()

        # Remove a record and trash should do nothing.
        # This is execution butler scenario.
        ref = refs.pop()
        uri = datastore.getURI(ref)
        datastore._table.delete(["dataset_id"], {"dataset_id": ref.id})
        self.assertTrue(uri.exists())
        datastore.trash(ref)
        datastore.emptyTrash()
        self.assertTrue(uri.exists())

        # Switch on trust and it should delete the file.
        datastore.trustGetRequest = True
        datastore.trash([ref])
        self.assertFalse(uri.exists())

        # Remove multiples at once in trust mode.
        subset = [refs.pop() for i in range(3)]
        datastore.trash(subset)
        datastore.trash(refs.pop())  # Check that a single ref can trash


class CleanupPosixDatastoreTestCase(DatastoreTestsBase, unittest.TestCase):
    """Test datastore cleans up on failure."""

    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")

    def setUp(self) -> None:
        # Override the working directory before calling the base class
        self.root = tempfile.mkdtemp()
        super().setUp()

    def testCleanup(self) -> None:
        """Test that a failed formatter write does cleanup a partial file."""
        metrics = makeExampleMetrics()
        datastore = self.makeDatastore()

        storageClass = self.storageClassFactory.getStorageClass("StructuredData")

        dimensions = self.universe.conform(("visit", "physical_filter"))
        dataId = {
            "instrument": "dummy",
            "visit": 52,
            "physical_filter": "V",
            "band": "v",
            "day_obs": 20250101,
        }

        ref = self.makeDatasetRef("metric", dimensions, storageClass, dataId)

        # Determine where the file will end up (we assume Formatters use
        # the same file extension)
        expectedUri = datastore.getURI(ref, predict=True)
        self.assertEqual(expectedUri.fragment, "predicted")

        self.assertEqual(expectedUri.getExtension(), ".yaml", f"Is there a file extension in {expectedUri}")

        # Try formatter that fails and formatter that fails and leaves
        # a file behind
        for formatter in (BadWriteFormatter, BadNoWriteFormatter):
            with self.subTest(formatter=formatter):
                # Monkey patch the formatter
                datastore.formatterFactory.registerFormatter(ref.datasetType, formatter, overwrite=True)

                # Try to put the dataset, it should fail
                with self.assertRaises(RuntimeError):
                    datastore.put(metrics, ref)

                # Check that there is no file on disk
                self.assertFalse(expectedUri.exists(), f"Check for existence of {expectedUri}")

                # Check that there is a directory
                dir = expectedUri.dirname()
                self.assertTrue(dir.exists(), f"Check for existence of directory {dir}")

        # Force YamlFormatter and check that this time a file is written
        datastore.formatterFactory.registerFormatter(ref.datasetType, YamlFormatter, overwrite=True)
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
    ingestTransferModes = ("copy", "move", "hardlink", "symlink", "relsymlink", "link", "auto")
    isEphemeral = False
    rootKeys = (".datastores.1.root", ".datastores.2.root")
    validationCanFail = True


class ChainedDatastoreMemoryTestCase(InMemoryDatastoreTestCase):
    """ChainedDatastore specialization using all InMemoryDatastore"""

    configFile = os.path.join(TESTDIR, "config/basic/chainedDatastore2.yaml")
    validationCanFail = False


class DatastoreConstraintsTests(DatastoreTestsBase):
    """Basic tests of constraints model of Datastores."""

    def testConstraints(self) -> None:
        """Test constraints model.  Assumes that each test class has the
        same constraints.
        """
        metrics = makeExampleMetrics()
        datastore = self.makeDatastore()

        sc1 = self.storageClassFactory.getStorageClass("StructuredData")
        sc2 = self.storageClassFactory.getStorageClass("StructuredDataJson")
        dimensions = self.universe.conform(("visit", "physical_filter", "instrument"))
        dataId = {
            "visit": 52,
            "physical_filter": "V",
            "band": "v",
            "instrument": "DummyCamComp",
            "day_obs": 20250101,
        }

        # Write empty file suitable for ingest check (JSON and YAML variants)
        testfile_y = tempfile.NamedTemporaryFile(suffix=".yaml")
        testfile_j = tempfile.NamedTemporaryFile(suffix=".json")
        for datasetTypeName, sc, accepted in (
            ("metric", sc1, True),
            ("metric5", sc1, False),
            ("metric33", sc1, True),
            ("metric5", sc2, True),
        ):
            # Choose different temp file depending on StorageClass
            testfile = testfile_j if sc.name.endswith("Json") else testfile_y

            with self.subTest(datasetTypeName=datasetTypeName, storageClass=sc.name, file=testfile.name):
                ref = self.makeDatasetRef(datasetTypeName, dimensions, sc, dataId)
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

    def setUp(self) -> None:
        # Override the working directory before calling the base class
        self.root = tempfile.mkdtemp()
        super().setUp()


class InMemoryDatastoreConstraintsTestCase(DatastoreConstraintsTests, unittest.TestCase):
    """InMemoryDatastore specialization."""

    configFile = os.path.join(TESTDIR, "config/basic/inMemoryDatastoreP.yaml")
    canIngest = False


class ChainedDatastoreConstraintsNativeTestCase(PosixDatastoreConstraintsTestCase):
    """ChainedDatastore specialization using a POSIXDatastore and constraints
    at the ChainedDatstore.
    """

    configFile = os.path.join(TESTDIR, "config/basic/chainedDatastorePa.yaml")


class ChainedDatastoreConstraintsTestCase(PosixDatastoreConstraintsTestCase):
    """ChainedDatastore specialization using a POSIXDatastore."""

    configFile = os.path.join(TESTDIR, "config/basic/chainedDatastoreP.yaml")


class ChainedDatastoreMemoryConstraintsTestCase(InMemoryDatastoreConstraintsTestCase):
    """ChainedDatastore specialization using all InMemoryDatastore."""

    configFile = os.path.join(TESTDIR, "config/basic/chainedDatastore2P.yaml")
    canIngest = False


class ChainedDatastorePerStoreConstraintsTests(DatastoreTestsBase, unittest.TestCase):
    """Test that a chained datastore can control constraints per-datastore
    even if child datastore would accept.
    """

    configFile = os.path.join(TESTDIR, "config/basic/chainedDatastorePb.yaml")

    def setUp(self) -> None:
        # Override the working directory before calling the base class
        self.root = tempfile.mkdtemp()
        super().setUp()

    def testConstraints(self) -> None:
        """Test chained datastore constraints model."""
        metrics = makeExampleMetrics()
        datastore = self.makeDatastore()

        sc1 = self.storageClassFactory.getStorageClass("StructuredData")
        sc2 = self.storageClassFactory.getStorageClass("StructuredDataJson")
        dimensions = self.universe.conform(("visit", "physical_filter", "instrument"))
        dataId1 = {
            "visit": 52,
            "physical_filter": "V",
            "band": "v",
            "instrument": "DummyCamComp",
            "day_obs": 20250101,
        }
        dataId2 = {"visit": 52, "physical_filter": "V", "band": "v", "instrument": "HSC", "day_obs": 20250101}

        # Write empty file suitable for ingest check (JSON and YAML variants)
        testfile_y = tempfile.NamedTemporaryFile(suffix=".yaml")
        testfile_j = tempfile.NamedTemporaryFile(suffix=".json")

        for typeName, dataId, sc, accept, ingest in (
            ("metric", dataId1, sc1, (False, True, False), True),
            ("metric5", dataId1, sc1, (False, False, False), False),
            ("metric5", dataId2, sc1, (True, False, False), False),
            ("metric33", dataId2, sc2, (True, True, False), True),
            ("metric5", dataId1, sc2, (False, True, False), True),
        ):
            # Choose different temp file depending on StorageClass
            testfile = testfile_j if sc.name.endswith("Json") else testfile_y

            with self.subTest(datasetTypeName=typeName, dataId=dataId, sc=sc.name):
                ref = self.makeDatasetRef(typeName, dimensions, sc, dataId)
                if any(accept):
                    datastore.put(metrics, ref)
                    self.assertTrue(datastore.exists(ref))

                    # Check each datastore inside the chained datastore
                    for childDatastore, expected in zip(datastore.datastores, accept, strict=True):
                        self.assertEqual(
                            childDatastore.exists(ref),
                            expected,
                            f"Testing presence of {ref} in datastore {childDatastore.name}",
                        )

                    datastore.remove(ref)

                    # Check that ingest works
                    if ingest:
                        datastore.ingest(FileDataset(testfile.name, [ref]), transfer="link")
                        self.assertTrue(datastore.exists(ref))

                        # Check each datastore inside the chained datastore
                        for childDatastore, expected in zip(datastore.datastores, accept, strict=True):
                            # Ephemeral datastores means InMemory at the moment
                            # and that does not accept ingest of files.
                            if childDatastore.isEphemeral:
                                expected = False
                            self.assertEqual(
                                childDatastore.exists(ref),
                                expected,
                                f"Testing presence of ingested {ref} in datastore {childDatastore.name}",
                            )

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


@unittest.mock.patch.dict(os.environ, {}, clear=True)
class DatastoreCacheTestCase(DatasetTestHelper, unittest.TestCase):
    """Tests for datastore caching infrastructure."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.storageClassFactory = StorageClassFactory()
        cls.universe = DimensionUniverse()

        # Ensure that we load the test storage class definitions.
        scConfigFile = os.path.join(TESTDIR, "config/basic/storageClasses.yaml")
        cls.storageClassFactory.addFromConfig(scConfigFile)

    def setUp(self) -> None:
        self.id = 0

        # Create a root that we can use for caching tests.
        self.root = tempfile.mkdtemp()

        # Create some test dataset refs and associated test files
        sc = self.storageClassFactory.getStorageClass("StructuredDataDict")
        dimensions = self.universe.conform(("visit", "physical_filter"))
        dataId = {
            "instrument": "dummy",
            "visit": 52,
            "physical_filter": "V",
            "band": "v",
            "day_obs": 20250101,
        }

        # Create list of refs and list of temporary files
        n_datasets = 10
        self.refs = [self.makeDatasetRef(f"metric{n}", dimensions, sc, dataId) for n in range(n_datasets)]

        root_uri = ResourcePath(self.root, forceDirectory=True)
        self.files = [root_uri.join(f"file{n}.txt") for n in range(n_datasets)]

        # Create test files.
        for uri in self.files:
            uri.write(b"0123456789")

        # Create some composite refs with component files.
        sc = self.storageClassFactory.getStorageClass("StructuredData")
        self.composite_refs = [self.makeDatasetRef(f"composite{n}", dimensions, sc, dataId) for n in range(3)]
        self.comp_files = []
        self.comp_refs = []
        for n, ref in enumerate(self.composite_refs):
            component_refs = []
            component_files = []
            for component in sc.components:
                component_ref = ref.makeComponentRef(component)
                file = root_uri.join(f"composite_file-{n}-{component}.txt")
                component_refs.append(component_ref)
                component_files.append(file)
                file.write(b"9876543210")

            self.comp_files.append(component_files)
            self.comp_refs.append(component_refs)

    def tearDown(self) -> None:
        if self.root is not None and os.path.exists(self.root):
            shutil.rmtree(self.root, ignore_errors=True)

    def _make_cache_manager(self, config_str: str) -> DatastoreCacheManager:
        config = Config.fromYaml(config_str)
        return DatastoreCacheManager(DatastoreCacheManagerConfig(config), universe=self.universe)

    def testNoCacheDir(self) -> None:
        config_str = """
cached:
  root: null
  cacheable:
    metric0: true
        """
        cache_manager = self._make_cache_manager(config_str)

        # Look inside to check we don't have a cache directory
        self.assertIsNone(cache_manager._cache_directory)

        self.assertCache(cache_manager)

        # Test that the cache directory is marked temporary
        self.assertTrue(cache_manager.cache_directory.isTemporary)

    def testNoCacheDirReversed(self) -> None:
        """Use default caching status and metric1 to false"""
        config_str = """
cached:
  root: null
  default: true
  cacheable:
    metric1: false
        """
        cache_manager = self._make_cache_manager(config_str)

        self.assertCache(cache_manager)

    def testEnvvarCacheDir(self) -> None:
        config_str = f"""
cached:
  root: '{self.root}'
  cacheable:
    metric0: true
        """

        root = ResourcePath(self.root, forceDirectory=True)
        env_dir = root.join("somewhere", forceDirectory=True)
        elsewhere = root.join("elsewhere", forceDirectory=True)

        # Environment variable should override the config value.
        with unittest.mock.patch.dict(os.environ, {"DAF_BUTLER_CACHE_DIRECTORY": env_dir.ospath}):
            cache_manager = self._make_cache_manager(config_str)
        self.assertEqual(cache_manager.cache_directory, env_dir)

        # This environment variable should not override the config value.
        with unittest.mock.patch.dict(os.environ, {"DAF_BUTLER_CACHE_DIRECTORY_IF_UNSET": env_dir.ospath}):
            cache_manager = self._make_cache_manager(config_str)
        self.assertEqual(cache_manager.cache_directory, root)

        # No default setting.
        config_str = """
cached:
  root: null
  default: true
  cacheable:
    metric1: false
        """
        cache_manager = self._make_cache_manager(config_str)

        # This environment variable should override the config value.
        with unittest.mock.patch.dict(os.environ, {"DAF_BUTLER_CACHE_DIRECTORY_IF_UNSET": env_dir.ospath}):
            cache_manager = self._make_cache_manager(config_str)
        self.assertEqual(cache_manager.cache_directory, env_dir)

        # If both environment variables are set the main (not IF_UNSET)
        # variable should win.
        with unittest.mock.patch.dict(
            os.environ,
            {
                "DAF_BUTLER_CACHE_DIRECTORY": env_dir.ospath,
                "DAF_BUTLER_CACHE_DIRECTORY_IF_UNSET": elsewhere.ospath,
            },
        ):
            cache_manager = self._make_cache_manager(config_str)
        self.assertEqual(cache_manager.cache_directory, env_dir)

        # Use the API to set the environment variable, making sure that the
        # variable is reset on exit.
        with unittest.mock.patch.dict(
            os.environ,
            {"DAF_BUTLER_CACHE_DIRECTORY_IF_UNSET": ""},
        ):
            defined, cache_dir = DatastoreCacheManager.set_fallback_cache_directory_if_unset()
            self.assertTrue(defined)
            cache_manager = self._make_cache_manager(config_str)
            self.assertEqual(cache_manager.cache_directory, ResourcePath(cache_dir, forceDirectory=True))

        # Now create the cache manager ahead of time and set the fallback
        # later.
        cache_manager = self._make_cache_manager(config_str)
        self.assertIsNone(cache_manager._cache_directory)
        with unittest.mock.patch.dict(
            os.environ,
            {"DAF_BUTLER_CACHE_DIRECTORY_IF_UNSET": ""},
        ):
            defined, cache_dir = DatastoreCacheManager.set_fallback_cache_directory_if_unset()
            self.assertTrue(defined)
            self.assertEqual(cache_manager.cache_directory, ResourcePath(cache_dir, forceDirectory=True))

    def testExplicitCacheDir(self) -> None:
        config_str = f"""
cached:
  root: '{self.root}'
  cacheable:
    metric0: true
        """
        cache_manager = self._make_cache_manager(config_str)

        # Look inside to check we do have a cache directory.
        self.assertEqual(cache_manager.cache_directory, ResourcePath(self.root, forceDirectory=True))

        self.assertCache(cache_manager)

        # Test that the cache directory is not marked temporary
        self.assertFalse(cache_manager.cache_directory.isTemporary)

    def assertCache(self, cache_manager: DatastoreCacheManager) -> None:
        self.assertTrue(cache_manager.should_be_cached(self.refs[0]))
        self.assertFalse(cache_manager.should_be_cached(self.refs[1]))

        uri = cache_manager.move_to_cache(self.files[0], self.refs[0])
        self.assertIsInstance(uri, ResourcePath)
        self.assertIsNone(cache_manager.move_to_cache(self.files[1], self.refs[1]))

        # Check presence in cache using ref and then using file extension.
        self.assertFalse(cache_manager.known_to_cache(self.refs[1]))
        self.assertTrue(cache_manager.known_to_cache(self.refs[0]))
        self.assertFalse(cache_manager.known_to_cache(self.refs[1], self.files[1].getExtension()))
        self.assertTrue(cache_manager.known_to_cache(self.refs[0], self.files[0].getExtension()))

        # Cached file should no longer exist but uncached file should be
        # unaffected.
        self.assertFalse(self.files[0].exists())
        self.assertTrue(self.files[1].exists())

        # Should find this file and it should be within the cache directory.
        with cache_manager.find_in_cache(self.refs[0], ".txt") as found:
            self.assertTrue(found.exists())
            self.assertIsNotNone(found.relative_to(cache_manager.cache_directory))

        # Should not be able to find these in cache
        with cache_manager.find_in_cache(self.refs[0], ".fits") as found:
            self.assertIsNone(found)
        with cache_manager.find_in_cache(self.refs[1], ".fits") as found:
            self.assertIsNone(found)

    def testNoCache(self) -> None:
        cache_manager = DatastoreDisabledCacheManager("", universe=self.universe)
        for uri, ref in zip(self.files, self.refs, strict=True):
            self.assertFalse(cache_manager.should_be_cached(ref))
            self.assertIsNone(cache_manager.move_to_cache(uri, ref))
            self.assertFalse(cache_manager.known_to_cache(ref))
            with cache_manager.find_in_cache(ref, ".txt") as found:
                self.assertIsNone(found, msg=f"{cache_manager}")

    def _expiration_config(self, mode: str, threshold: int) -> str:
        return f"""
cached:
  default: true
  expiry:
    mode: {mode}
    threshold: {threshold}
  cacheable:
    unused: true
        """

    def testCacheExpiryFiles(self) -> None:
        threshold = 2  # Keep at least 2 files.
        mode = "files"
        config_str = self._expiration_config(mode, threshold)

        cache_manager = self._make_cache_manager(config_str)

        # Check that an empty cache returns unknown for arbitrary ref
        self.assertFalse(cache_manager.known_to_cache(self.refs[0]))

        # Should end with datasets: 2, 3, 4
        self.assertExpiration(cache_manager, 5, threshold + 1)
        self.assertIn(f"{mode}={threshold}", str(cache_manager))

        # Check that we will not expire a file that is actively in use.
        with cache_manager.find_in_cache(self.refs[2], ".txt") as found:
            self.assertIsNotNone(found)

            # Trigger cache expiration that should remove the file
            # we just retrieved. Should now have: 3, 4, 5
            cached = cache_manager.move_to_cache(self.files[5], self.refs[5])
            self.assertIsNotNone(cached)

            # Cache should still report the standard file count.
            self.assertEqual(cache_manager.file_count, threshold + 1)

            # Add additional entry to cache.
            # Should now have 4, 5, 6
            cached = cache_manager.move_to_cache(self.files[6], self.refs[6])
            self.assertIsNotNone(cached)

            # Is the file still there?
            self.assertTrue(found.exists())

            # Can we read it?
            data = found.read()
            self.assertGreater(len(data), 0)

        # Outside context the file should no longer exist.
        self.assertFalse(found.exists())

        # File count should not have changed.
        self.assertEqual(cache_manager.file_count, threshold + 1)

        # Dataset 2 was in the exempt directory but because hardlinks
        # are used it was deleted from the main cache during cache expiry
        # above and so should no longer be found.
        with cache_manager.find_in_cache(self.refs[2], ".txt") as found:
            self.assertIsNone(found)

        # And the one stored after it is also gone.
        with cache_manager.find_in_cache(self.refs[3], ".txt") as found:
            self.assertIsNone(found)

        # But dataset 4 is present.
        with cache_manager.find_in_cache(self.refs[4], ".txt") as found:
            self.assertIsNotNone(found)

        # Adding a new dataset to the cache should now delete it.
        cache_manager.move_to_cache(self.files[7], self.refs[7])

        with cache_manager.find_in_cache(self.refs[2], ".txt") as found:
            self.assertIsNone(found)

    def testCacheExpiryDatasets(self) -> None:
        threshold = 2  # Keep 2 datasets.
        mode = "datasets"
        config_str = self._expiration_config(mode, threshold)

        cache_manager = self._make_cache_manager(config_str)
        self.assertExpiration(cache_manager, 5, threshold + 1)
        self.assertIn(f"{mode}={threshold}", str(cache_manager))

    def testCacheExpiryDatasetsFromDisabled(self) -> None:
        threshold = 2
        mode = "datasets"
        with unittest.mock.patch.dict(
            os.environ,
            {"DAF_BUTLER_CACHE_EXPIRATION_MODE": f"{mode}={threshold}"},
        ):
            cache_manager = DatastoreCacheManager.create_disabled(universe=DimensionUniverse())
            self.assertExpiration(cache_manager, 5, threshold + 1)
            self.assertIn(f"{mode}={threshold}", str(cache_manager))

    def testExpirationModeOverride(self) -> None:
        threshold = 2  # Keep 2 datasets.
        mode = "datasets"
        config_str = self._expiration_config(mode, threshold)

        mode = "size"
        threshold = 55
        with unittest.mock.patch.dict(
            os.environ,
            {"DAF_BUTLER_CACHE_EXPIRATION_MODE": f"{mode}={threshold}"},
        ):
            cache_manager = self._make_cache_manager(config_str)
            self.assertExpiration(cache_manager, 10, 6)
            self.assertIn(f"{mode}={threshold}", str(cache_manager))

        # Check we get a warning with unrecognized form.
        with unittest.mock.patch.dict(
            os.environ,
            {"DAF_BUTLER_CACHE_EXPIRATION_MODE": "something"},
        ):
            with self.assertLogs(level="WARNING") as cm:
                self._make_cache_manager(config_str)
            self.assertIn("Unrecognized form (something)", cm.output[0])

        with unittest.mock.patch.dict(
            os.environ,
            {"DAF_BUTLER_CACHE_EXPIRATION_MODE": "something=5"},
        ):
            with self.assertRaises(ValueError) as cm:
                self._make_cache_manager(config_str)
            self.assertIn("Unrecognized value", str(cm.exception))

    def testMissingThreshold(self) -> None:
        threshold = ""
        mode = "datasets"
        config_str = self._expiration_config(mode, threshold)

        with self.assertRaises(ValueError) as cm:
            self._make_cache_manager(config_str)
        self.assertIn("Cache expiration threshold", str(cm.exception))

    def testCacheExpiryDatasetsComposite(self) -> None:
        threshold = 2  # Keep 2 datasets.
        mode = "datasets"
        config_str = self._expiration_config(mode, threshold)

        cache_manager = self._make_cache_manager(config_str)

        n_datasets = 3
        for i in range(n_datasets):
            for component_file, component_ref in zip(self.comp_files[i], self.comp_refs[i], strict=True):
                cached = cache_manager.move_to_cache(component_file, component_ref)
                self.assertIsNotNone(cached)
                self.assertTrue(cache_manager.known_to_cache(component_ref))
                self.assertTrue(cache_manager.known_to_cache(component_ref.makeCompositeRef()))
                self.assertTrue(cache_manager.known_to_cache(component_ref, component_file.getExtension()))

        self.assertEqual(cache_manager.file_count, 6)  # 2 datasets each of 3 files

        # Write two new non-composite and the number of files should drop.
        self.assertExpiration(cache_manager, 2, 5)

    def testCacheExpirySize(self) -> None:
        threshold = 55  # Each file is 10 bytes
        mode = "size"
        config_str = self._expiration_config(mode, threshold)

        cache_manager = self._make_cache_manager(config_str)
        self.assertExpiration(cache_manager, 10, 6)
        self.assertIn(f"{mode}={threshold}", str(cache_manager))

    def testDisabledCache(self) -> None:
        # Configure an active cache but disable via environment.
        threshold = 2
        mode = "datasets"
        config_str = self._expiration_config(mode, threshold)

        with unittest.mock.patch.dict(
            os.environ,
            {"DAF_BUTLER_CACHE_EXPIRATION_MODE": "disabled"},
        ):
            env_cache_manager = self._make_cache_manager(config_str)

        # Configure to be disabled
        threshold = 0
        mode = "disabled"
        config_str = self._expiration_config(mode, threshold)
        cfg_cache_manager = self._make_cache_manager(config_str)

        for cache_manager in (
            cfg_cache_manager,
            env_cache_manager,
            DatastoreCacheManager.create_disabled(universe=DimensionUniverse()),
        ):
            for uri, ref in zip(self.files, self.refs, strict=True):
                self.assertFalse(cache_manager.should_be_cached(ref))
                self.assertIsNone(cache_manager.move_to_cache(uri, ref))
                self.assertFalse(cache_manager.known_to_cache(ref))
                with cache_manager.find_in_cache(ref, ".txt") as found:
                    self.assertIsNone(found, msg=f"{cache_manager}")
                self.assertIn("disabled", str(cache_manager))

    def assertExpiration(
        self, cache_manager: DatastoreCacheManager, n_datasets: int, n_retained: int
    ) -> None:
        """Insert the datasets and then check the number retained."""
        for i in range(n_datasets):
            cached = cache_manager.move_to_cache(self.files[i], self.refs[i])
            self.assertIsNotNone(cached)

        self.assertEqual(cache_manager.file_count, n_retained)

        # The oldest file should not be in the cache any more.
        for i in range(n_datasets):
            with cache_manager.find_in_cache(self.refs[i], ".txt") as found:
                if i >= n_datasets - n_retained:
                    self.assertIsInstance(found, ResourcePath)
                else:
                    self.assertIsNone(found)

    def testCacheExpiryAge(self) -> None:
        threshold = 1  # Expire older than 2 seconds
        mode = "age"
        config_str = self._expiration_config(mode, threshold)

        cache_manager = self._make_cache_manager(config_str)
        self.assertIn(f"{mode}={threshold}", str(cache_manager))

        # Insert 3 files, then sleep, then insert more.
        for i in range(2):
            cached = cache_manager.move_to_cache(self.files[i], self.refs[i])
            self.assertIsNotNone(cached)
        time.sleep(2.0)
        for j in range(4):
            i = 2 + j  # Continue the counting
            cached = cache_manager.move_to_cache(self.files[i], self.refs[i])
            self.assertIsNotNone(cached)

        # Only the files written after the sleep should exist.
        self.assertEqual(cache_manager.file_count, 4)
        with cache_manager.find_in_cache(self.refs[1], ".txt") as found:
            self.assertIsNone(found)
        with cache_manager.find_in_cache(self.refs[2], ".txt") as found:
            self.assertIsInstance(found, ResourcePath)


class NullDatastoreTestCase(DatasetTestHelper, unittest.TestCase):
    """Test the null datastore."""

    storageClassFactory = StorageClassFactory()

    def test_basics(self) -> None:
        storageClass = self.storageClassFactory.getStorageClass("StructuredDataDict")
        ref = self.makeDatasetRef("metric", DimensionUniverse().empty, storageClass, {})

        null = NullDatastore(None, None)

        self.assertFalse(null.exists(ref))
        self.assertFalse(null.knows(ref))
        knows = null.knows_these([ref])
        self.assertFalse(knows[ref])
        null.validateConfiguration(ref)

        with self.assertRaises(FileNotFoundError):
            null.get(ref)
        with self.assertRaises(NotImplementedError):
            null.put("", ref)
        with self.assertRaises(FileNotFoundError):
            null.getURI(ref)
        with self.assertRaises(FileNotFoundError):
            null.getURIs(ref)
        with self.assertRaises(FileNotFoundError):
            null.getManyURIs([ref])
        with self.assertRaises(NotImplementedError):
            null.getLookupKeys()
        with self.assertRaises(NotImplementedError):
            null.import_records({})
        with self.assertRaises(NotImplementedError):
            null.export_records([])
        with self.assertRaises(NotImplementedError):
            null.export([ref])
        with self.assertRaises(NotImplementedError):
            null.transfer(null, ref)
        with self.assertRaises(NotImplementedError):
            null.emptyTrash()
        with self.assertRaises(NotImplementedError):
            null.trash(ref)
        with self.assertRaises(NotImplementedError):
            null.forget([ref])
        with self.assertRaises(NotImplementedError):
            null.remove(ref)
        with self.assertRaises(NotImplementedError):
            null.retrieveArtifacts([ref], ResourcePath("."))
        with self.assertRaises(NotImplementedError):
            null.transfer_from(null, [ref])
        with self.assertRaises(NotImplementedError):
            null.ingest()


class DatasetRefURIsTestCase(unittest.TestCase):
    """Tests for DatasetRefURIs."""

    def testSequenceAccess(self) -> None:
        """Verify that DatasetRefURIs can be treated like a two-item tuple."""
        uris = DatasetRefURIs()

        self.assertEqual(len(uris), 2)
        self.assertEqual(uris[0], None)
        self.assertEqual(uris[1], {})

        primaryURI = ResourcePath("1/2/3")
        componentURI = ResourcePath("a/b/c")

        # affirm that DatasetRefURIs does not support MutableSequence functions
        with self.assertRaises(TypeError):
            uris[0] = primaryURI
        with self.assertRaises(TypeError):
            uris[1] = {"foo": componentURI}

        # but DatasetRefURIs can be set by property name:
        uris.primaryURI = primaryURI
        uris.componentURIs = {"foo": componentURI}
        self.assertEqual(uris.primaryURI, primaryURI)
        self.assertEqual(uris[0], primaryURI)

        primary, components = uris
        self.assertEqual(primary, primaryURI)
        self.assertEqual(components, {"foo": componentURI})

    def testRepr(self) -> None:
        """Verify __repr__ output."""
        uris = DatasetRefURIs(ResourcePath("/1/2/3"), {"comp": ResourcePath("/a/b/c")})
        self.assertEqual(
            repr(uris),
            'DatasetRefURIs(ResourcePath("file:///1/2/3"), {\'comp\': ResourcePath("file:///a/b/c")})',
        )


class StoredFileInfoTestCase(DatasetTestHelper, unittest.TestCase):
    """Test the StoredFileInfo class."""

    storageClassFactory = StorageClassFactory()

    def test_StoredFileInfo(self) -> None:
        storageClass = self.storageClassFactory.getStorageClass("StructuredDataDict")
        ref = self.makeDatasetRef("metric", DimensionUniverse().empty, storageClass, {})

        record = dict(
            storage_class="StructuredDataDict",
            formatter="lsst.daf.butler.Formatter",
            path="a/b/c.txt",
            component="component",
            checksum=None,
            file_size=5,
        )
        info = StoredFileInfo.from_record(record)

        self.assertEqual(info.to_record(), record)

        ref2 = self.makeDatasetRef("metric", DimensionUniverse().empty, storageClass, {})
        rebased = info.rebase(ref2)
        self.assertEqual(rebased.rebase(ref), info)

        with self.assertRaises(TypeError):
            rebased.update(formatter=42)

        with self.assertRaises(ValueError):
            rebased.update(something=42, new="42")

        # Check that pickle works on StoredFileInfo.
        pickled_info = pickle.dumps(info)
        unpickled_info = pickle.loads(pickled_info)
        self.assertEqual(unpickled_info, info)


@contextlib.contextmanager
def _temp_yaml_file(data: Any) -> Iterator[str]:
    fh = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml")
    try:
        yaml.dump(data, stream=fh)
        fh.flush()
        yield fh.name
    finally:
        # Some tests delete the file
        with contextlib.suppress(FileNotFoundError):
            fh.close()


if __name__ == "__main__":
    unittest.main()

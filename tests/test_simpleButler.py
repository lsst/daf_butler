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

import json
import os
import re
import tempfile
import unittest
from typing import Any

try:
    import numpy as np
except ImportError:
    np = None

import astropy.time
from lsst.daf.butler import Butler, ButlerConfig, CollectionType, DatasetId, DatasetRef, DatasetType, Timespan
from lsst.daf.butler.registry import RegistryConfig, RegistryDefaults, _RegistryFactory
from lsst.daf.butler.tests import DatastoreMock
from lsst.daf.butler.tests.utils import makeTestTempDir, removeTestTempDir

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class SimpleButlerTestCase(unittest.TestCase):
    """Tests for butler (including import/export functionality) that should not
    depend on the Registry Database backend or Datastore implementation, and
    can instead utilize an in-memory SQLite Registry and a mocked Datastore.
    """

    datasetsManager = (
        "lsst.daf.butler.registry.datasets.byDimensions.ByDimensionsDatasetRecordStorageManagerUUID"
    )
    datasetsImportFile = "datasets-uuid.yaml"

    def setUp(self):
        self.root = makeTestTempDir(TESTDIR)

    def tearDown(self):
        removeTestTempDir(self.root)

    def makeButler(self, **kwargs: Any) -> Butler:
        """Return new Butler instance on each call."""
        config = ButlerConfig()

        # make separate temporary directory for registry of this instance
        tmpdir = tempfile.mkdtemp(dir=self.root)
        config["registry", "db"] = f"sqlite:///{tmpdir}/gen3.sqlite3"
        config["registry", "managers", "datasets"] = self.datasetsManager
        config["root"] = self.root

        # have to make a registry first
        registryConfig = RegistryConfig(config.get("registry"))
        _RegistryFactory(registryConfig).create_from_config()

        butler = Butler.from_config(config, **kwargs)
        DatastoreMock.apply(butler)
        return butler

    def comparableRef(self, ref: DatasetRef) -> DatasetRef:
        """Return a DatasetRef that can be compared to a DatasetRef from
        other repository.

        For repositories that do not support round-trip of ID values this
        method returns unresolved DatasetRef, for round-trip-safe repos it
        returns unchanged ref.
        """
        return ref

    def testReadBackwardsCompatibility(self):
        """Test that we can read an export file written by a previous version
        and commit to the daf_butler git repo.

        Notes
        -----
        At present this export file includes only dimension data, not datasets,
        which greatly limits the usefulness of this test.  We should address
        this at some point, but I think it's best to wait for the changes to
        the export format required for CALIBRATION collections to land.
        """
        butler = self.makeButler(writeable=True)
        butler.import_(filename=os.path.join(TESTDIR, "data", "registry", "hsc-rc2-subset.yaml"))
        # Spot-check a few things, but the most important test is just that
        # the above does not raise.
        self.assertGreaterEqual(
            {record.id for record in butler.registry.queryDimensionRecords("detector", instrument="HSC")},
            set(range(104)),  # should have all science CCDs; may have some focus ones.
        )
        self.assertGreaterEqual(
            {
                (record.id, record.physical_filter)
                for record in butler.registry.queryDimensionRecords("visit", instrument="HSC")
            },
            {
                (27136, "HSC-Z"),
                (11694, "HSC-G"),
                (23910, "HSC-R"),
                (11720, "HSC-Y"),
                (23900, "HSC-R"),
                (22646, "HSC-Y"),
                (1248, "HSC-I"),
                (19680, "HSC-I"),
                (1240, "HSC-I"),
                (424, "HSC-Y"),
                (19658, "HSC-I"),
                (344, "HSC-Y"),
                (1218, "HSC-R"),
                (1190, "HSC-Z"),
                (23718, "HSC-R"),
                (11700, "HSC-G"),
                (26036, "HSC-G"),
                (23872, "HSC-R"),
                (1170, "HSC-Z"),
                (1876, "HSC-Y"),
            },
        )

    def testDatasetTransfers(self):
        """Test exporting all datasets from a repo and then importing them all
        back in again.
        """
        # Import data to play with.
        butler1 = self.makeButler(writeable=True)
        butler1.import_(filename=os.path.join(TESTDIR, "data", "registry", "base.yaml"))
        butler1.import_(filename=os.path.join(TESTDIR, "data", "registry", self.datasetsImportFile))
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml") as file:
            # Export all datasets.
            with butler1.export(filename=file.name) as exporter:
                exporter.saveDatasets(butler1.registry.queryDatasets(..., collections=...))
            # Import it all again.
            butler2 = self.makeButler(writeable=True)
            butler2.import_(filename=file.name)
        datasets1 = list(butler1.registry.queryDatasets(..., collections=...))
        datasets2 = list(butler2.registry.queryDatasets(..., collections=...))
        self.assertTrue(all(isinstance(ref.id, DatasetId) for ref in datasets1))
        self.assertTrue(all(isinstance(ref.id, DatasetId) for ref in datasets2))
        self.assertCountEqual(
            [self.comparableRef(ref) for ref in datasets1],
            [self.comparableRef(ref) for ref in datasets2],
        )

    def testImportTwice(self):
        """Test exporting dimension records and datasets from a repo and then
        importing them all back in again twice.
        """
        # Import data to play with.
        butler1 = self.makeButler(writeable=True)
        butler1.import_(filename=os.path.join(TESTDIR, "data", "registry", "base.yaml"))
        butler1.import_(filename=os.path.join(TESTDIR, "data", "registry", self.datasetsImportFile))
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as file:
            # Export all datasets.
            with butler1.export(filename=file.name) as exporter:
                exporter.saveDatasets(butler1.registry.queryDatasets(..., collections=...))
            butler2 = self.makeButler(writeable=True)
            # Import it once.
            butler2.import_(filename=file.name)
            # Import it again
            butler2.import_(filename=file.name)
        datasets1 = list(butler1.registry.queryDatasets(..., collections=...))
        datasets2 = list(butler2.registry.queryDatasets(..., collections=...))
        self.assertTrue(all(isinstance(ref.id, DatasetId) for ref in datasets1))
        self.assertTrue(all(isinstance(ref.id, DatasetId) for ref in datasets2))
        self.assertCountEqual(
            [self.comparableRef(ref) for ref in datasets1],
            [self.comparableRef(ref) for ref in datasets2],
        )

    def testCollectionTransfers(self):
        """Test exporting and then importing collections of various types."""
        # Populate a registry with some datasets.
        butler1 = self.makeButler(writeable=True)
        butler1.import_(filename=os.path.join(TESTDIR, "data", "registry", "base.yaml"))
        butler1.import_(filename=os.path.join(TESTDIR, "data", "registry", self.datasetsImportFile))
        registry1 = butler1.registry
        # Add some more collections.
        registry1.registerRun("run1")
        registry1.registerCollection("tag1", CollectionType.TAGGED)
        registry1.registerCollection("calibration1", CollectionType.CALIBRATION)
        registry1.registerCollection("chain1", CollectionType.CHAINED)
        registry1.registerCollection("chain2", CollectionType.CHAINED)
        registry1.setCollectionChain("chain1", ["tag1", "run1", "chain2"])
        registry1.setCollectionChain("chain2", ["calibration1", "run1"])
        # Associate some datasets into the TAGGED and CALIBRATION collections.
        flats1 = list(registry1.queryDatasets("flat", collections=...))
        registry1.associate("tag1", flats1)
        t1 = astropy.time.Time("2020-01-01T01:00:00", format="isot", scale="tai")
        t2 = astropy.time.Time("2020-01-01T02:00:00", format="isot", scale="tai")
        t3 = astropy.time.Time("2020-01-01T03:00:00", format="isot", scale="tai")
        bias1a = registry1.findDataset("bias", instrument="Cam1", detector=1, collections="imported_g")
        bias2a = registry1.findDataset("bias", instrument="Cam1", detector=2, collections="imported_g")
        bias3a = registry1.findDataset("bias", instrument="Cam1", detector=3, collections="imported_g")
        bias2b = registry1.findDataset("bias", instrument="Cam1", detector=2, collections="imported_r")
        bias3b = registry1.findDataset("bias", instrument="Cam1", detector=3, collections="imported_r")
        registry1.certify("calibration1", [bias2a, bias3a], Timespan(t1, t2))
        registry1.certify("calibration1", [bias2b], Timespan(t2, None))
        registry1.certify("calibration1", [bias3b], Timespan(t2, t3))
        registry1.certify("calibration1", [bias1a], Timespan.makeEmpty())

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml") as file:
            # Export all collections, and some datasets.
            with butler1.export(filename=file.name) as exporter:
                # Sort results to put chain1 before chain2, which is
                # intentionally not topological order.
                for collection in sorted(registry1.queryCollections()):
                    exporter.saveCollection(collection)
                exporter.saveDatasets(flats1)
                exporter.saveDatasets([bias1a, bias2a, bias2b, bias3a, bias3b])
            # Import them into a new registry.
            butler2 = self.makeButler(writeable=True)
            butler2.import_(filename=file.name)
        registry2 = butler2.registry
        # Check that it all round-tripped, starting with the collections
        # themselves.
        self.assertIs(registry2.getCollectionType("run1"), CollectionType.RUN)
        self.assertIs(registry2.getCollectionType("tag1"), CollectionType.TAGGED)
        self.assertIs(registry2.getCollectionType("calibration1"), CollectionType.CALIBRATION)
        self.assertIs(registry2.getCollectionType("chain1"), CollectionType.CHAINED)
        self.assertIs(registry2.getCollectionType("chain2"), CollectionType.CHAINED)
        self.assertEqual(
            list(registry2.getCollectionChain("chain1")),
            ["tag1", "run1", "chain2"],
        )
        self.assertEqual(
            list(registry2.getCollectionChain("chain2")),
            ["calibration1", "run1"],
        )
        # Check that tag collection contents are the same.
        self.maxDiff = None
        self.assertCountEqual(
            [self.comparableRef(ref) for ref in registry1.queryDatasets(..., collections="tag1")],
            [self.comparableRef(ref) for ref in registry2.queryDatasets(..., collections="tag1")],
        )
        # Check that calibration collection contents are the same.
        self.assertCountEqual(
            [
                (self.comparableRef(assoc.ref), assoc.timespan)
                for assoc in registry1.queryDatasetAssociations("bias", collections="calibration1")
            ],
            [
                (self.comparableRef(assoc.ref), assoc.timespan)
                for assoc in registry2.queryDatasetAssociations("bias", collections="calibration1")
            ],
        )

    def testButlerGet(self):
        """Test that butler.get can work with different variants."""
        # Import data to play with.
        butler = self.makeButler(writeable=True)
        butler.import_(filename=os.path.join(TESTDIR, "data", "registry", "base.yaml"))
        butler.import_(filename=os.path.join(TESTDIR, "data", "registry", self.datasetsImportFile))

        # Find the DatasetRef for a flat
        coll = "imported_g"
        flat2g = butler.find_dataset(
            "flat", instrument="Cam1", full_name="Ab", physical_filter="Cam1-G", collections=coll
        )

        # Create a numpy integer to check that works fine
        detector_np = np.int64(2) if np else 2

        # Try to get it using different variations of dataId + keyword
        # arguments
        # Note that instrument.class_name does not work
        variants = (
            (None, {"instrument": "Cam1", "detector": 2, "physical_filter": "Cam1-G"}),
            (None, {"instrument": "Cam1", "detector": detector_np, "physical_filter": "Cam1-G"}),
            ({"instrument": "Cam1", "detector": 2, "physical_filter": "Cam1-G"}, {}),
            ({"instrument": "Cam1", "detector": detector_np, "physical_filter": "Cam1-G"}, {}),
            ({"instrument": "Cam1", "detector": 2}, {"physical_filter": "Cam1-G"}),
            ({"detector.full_name": "Ab"}, {"instrument": "Cam1", "physical_filter": "Cam1-G"}),
            ({"full_name": "Ab"}, {"instrument": "Cam1", "physical_filter": "Cam1-G"}),
            (None, {"full_name": "Ab", "instrument": "Cam1", "physical_filter": "Cam1-G"}),
            (None, {"detector": "Ab", "instrument": "Cam1", "physical_filter": "Cam1-G"}),
            ({"name_in_raft": "b", "raft": "A"}, {"instrument": "Cam1", "physical_filter": "Cam1-G"}),
            ({"name_in_raft": "b"}, {"raft": "A", "instrument": "Cam1", "physical_filter": "Cam1-G"}),
            (None, {"name_in_raft": "b", "raft": "A", "instrument": "Cam1", "physical_filter": "Cam1-G"}),
            (
                {"detector.name_in_raft": "b", "detector.raft": "A"},
                {"instrument": "Cam1", "physical_filter": "Cam1-G"},
            ),
            (
                {
                    "detector.name_in_raft": "b",
                    "detector.raft": "A",
                    "instrument": "Cam1",
                    "physical_filter": "Cam1-G",
                },
                {},
            ),
            # Duplicate (but valid) information.
            (None, {"instrument": "Cam1", "detector": 2, "raft": "A", "physical_filter": "Cam1-G"}),
            ({"detector": 2}, {"instrument": "Cam1", "raft": "A", "physical_filter": "Cam1-G"}),
            ({"raft": "A"}, {"instrument": "Cam1", "detector": 2, "physical_filter": "Cam1-G"}),
            ({"raft": "A"}, {"instrument": "Cam1", "detector": "Ab", "physical_filter": "Cam1-G"}),
        )

        for dataId, kwds in variants:
            try:
                flat_id, _ = butler.get("flat", dataId=dataId, collections=coll, **kwds)
            except Exception as e:
                raise type(e)(f"{str(e)}: dataId={dataId}, kwds={kwds}") from e
            self.assertEqual(flat_id, flat2g.id, msg=f"DataId: {dataId}, kwds: {kwds}")

        # Check that bad combinations raise.
        variants = (
            # Inconsistent detector information.
            (None, {"instrument": "Cam1", "detector": 2, "raft": "B", "physical_filter": "Cam1-G"}),
            ({"detector": 2}, {"instrument": "Cam1", "raft": "B", "physical_filter": "Cam1-G"}),
            ({"detector": 12}, {"instrument": "Cam1", "raft": "B", "physical_filter": "Cam1-G"}),
            ({"raft": "B"}, {"instrument": "Cam1", "detector": 2, "physical_filter": "Cam1-G"}),
            ({"raft": "B"}, {"instrument": "Cam1", "detector": "Ab", "physical_filter": "Cam1-G"}),
            # Under-specified.
            ({"raft": "B"}, {"instrument": "Cam1", "physical_filter": "Cam1-G"}),
            # Spurious kwargs.
            (None, {"instrument": "Cam1", "detector": 2, "physical_filter": "Cam1-G", "x": "y"}),
            ({"x": "y"}, {"instrument": "Cam1", "detector": 2, "physical_filter": "Cam1-G"}),
        )
        for dataId, kwds in variants:
            with self.assertRaises((ValueError, LookupError)):
                butler.get("flat", dataId=dataId, collections=coll, **kwds)

    def testGetCalibration(self):
        """Test that `Butler.get` can be used to fetch from
        `~CollectionType.CALIBRATION` collections if the data ID includes
        extra dimensions with temporal information.
        """
        # Import data to play with.
        butler = self.makeButler(writeable=True)
        butler.import_(filename=os.path.join(TESTDIR, "data", "registry", "base.yaml"))
        butler.import_(filename=os.path.join(TESTDIR, "data", "registry", self.datasetsImportFile))
        # Certify some biases into a CALIBRATION collection.
        registry = butler.registry
        registry.registerCollection("calibs", CollectionType.CALIBRATION)
        t1 = astropy.time.Time("2020-01-01T01:00:00", format="isot", scale="tai")
        t2 = astropy.time.Time("2020-01-01T02:00:00", format="isot", scale="tai")
        t3 = astropy.time.Time("2020-01-01T03:00:00", format="isot", scale="tai")
        bias2a = registry.findDataset("bias", instrument="Cam1", detector=2, collections="imported_g")
        bias3a = registry.findDataset("bias", instrument="Cam1", detector=3, collections="imported_g")
        bias2b = registry.findDataset("bias", instrument="Cam1", detector=2, collections="imported_r")
        bias3b = registry.findDataset("bias", instrument="Cam1", detector=3, collections="imported_r")
        registry.certify("calibs", [bias2a, bias3a], Timespan(t1, t2))
        registry.certify("calibs", [bias2b], Timespan(t2, None))
        registry.certify("calibs", [bias3b], Timespan(t2, t3))
        # Insert some exposure dimension data.
        registry.insertDimensionData(
            "exposure",
            {
                "instrument": "Cam1",
                "id": 3,
                "obs_id": "three",
                "timespan": Timespan(t1, t2),
                "physical_filter": "Cam1-G",
                "day_obs": 20201114,
                "seq_num": 55,
            },
            {
                "instrument": "Cam1",
                "id": 4,
                "obs_id": "four",
                "timespan": Timespan(t2, t3),
                "physical_filter": "Cam1-G",
                "day_obs": 20211114,
                "seq_num": 42,
            },
        )
        # Get some biases from raw-like data IDs.
        bias2a_id, _ = butler.get(
            "bias", {"instrument": "Cam1", "exposure": 3, "detector": 2}, collections="calibs"
        )
        self.assertEqual(bias2a_id, bias2a.id)
        bias3b_id, _ = butler.get(
            "bias", {"instrument": "Cam1", "exposure": 4, "detector": 3}, collections="calibs"
        )
        self.assertEqual(bias3b_id, bias3b.id)

        # Get using the kwarg form
        bias3b_id, _ = butler.get("bias", instrument="Cam1", exposure=4, detector=3, collections="calibs")
        self.assertEqual(bias3b_id, bias3b.id)

        # Do it again but using the record information
        bias2a_id, _ = butler.get(
            "bias",
            {"instrument": "Cam1", "exposure.obs_id": "three", "detector.full_name": "Ab"},
            collections="calibs",
        )
        self.assertEqual(bias2a_id, bias2a.id)
        bias3b_id, _ = butler.get(
            "bias",
            {"exposure.obs_id": "four", "detector.full_name": "Ba"},
            collections="calibs",
            instrument="Cam1",
        )
        self.assertEqual(bias3b_id, bias3b.id)

        # And again but this time using the alternate value rather than
        # the primary.
        bias3b_id, _ = butler.get(
            "bias", {"exposure": "four", "detector": "Ba"}, collections="calibs", instrument="Cam1"
        )
        self.assertEqual(bias3b_id, bias3b.id)

        # And again but this time using the alternate value rather than
        # the primary and do it in the keyword arguments.
        bias3b_id, _ = butler.get(
            "bias", exposure="four", detector="Ba", collections="calibs", instrument="Cam1"
        )
        self.assertEqual(bias3b_id, bias3b.id)

        # Now with implied record columns
        bias3b_id, _ = butler.get(
            "bias",
            day_obs=20211114,
            seq_num=42,
            raft="B",
            name_in_raft="a",
            collections="calibs",
            instrument="Cam1",
        )
        self.assertEqual(bias3b_id, bias3b.id)

        # Allow a fully-specified dataId and unnecessary extra information
        # that comes from the record.
        bias3b_id, _ = butler.get(
            "bias",
            dataId=dict(
                exposure=4,
                day_obs=20211114,
                seq_num=42,
                detector=3,
                instrument="Cam1",
            ),
            collections="calibs",
        )
        self.assertEqual(bias3b_id, bias3b.id)

        # Extra but inconsistent record values are a problem.
        with self.assertRaises(ValueError):
            bias3b_id, _ = butler.get(
                "bias",
                exposure=3,
                day_obs=20211114,
                seq_num=42,
                detector=3,
                collections="calibs",
                instrument="Cam1",
            )

        # Ensure that spurious kwargs cause an exception.
        with self.assertRaises(ValueError):
            butler.get(
                "bias",
                {"exposure.obs_id": "four", "immediate": True, "detector.full_name": "Ba"},
                collections="calibs",
                instrument="Cam1",
            )

        with self.assertRaises(ValueError):
            butler.get(
                "bias",
                day_obs=20211114,
                seq_num=42,
                raft="B",
                name_in_raft="a",
                collections="calibs",
                instrument="Cam1",
                immediate=True,
            )

    def testRegistryDefaults(self):
        """Test that we can default the collections and some data ID keys when
        constructing a butler.

        Many tests that use default run already exist in ``test_butler.py``, so
        that isn't tested here.  And while most of this functionality is
        implemented in `Registry`, we test it here instead of
        ``daf/butler/tests/registry.py`` because it shouldn't depend on the
        database backend at all.
        """
        butler = self.makeButler(writeable=True)
        butler.import_(filename=os.path.join(TESTDIR, "data", "registry", "base.yaml"))
        butler.import_(filename=os.path.join(TESTDIR, "data", "registry", self.datasetsImportFile))
        # Need to actually set defaults later, not at construction, because
        # we need to import the instrument before we can use it as a default.
        # Don't set a default instrument value for data IDs, because 'Cam1'
        # should be inferred by virtue of that being the only value in the
        # input collections.
        butler.registry.defaults = RegistryDefaults(collections=["imported_g"])
        # Use findDataset without collections or instrument.
        ref = butler.find_dataset("flat", detector=2, physical_filter="Cam1-G")
        # Do the same with Butler.get; this should ultimately invoke a lot of
        # the same code, so it's a bit circular, but mostly we're checking that
        # it works at all.
        dataset_id, _ = butler.get("flat", detector=2, physical_filter="Cam1-G")
        self.assertEqual(ref.id, dataset_id)
        # Query for datasets.  Test defaulting the data ID in both kwargs and
        # in the WHERE expression.
        queried_refs_1 = set(butler.registry.queryDatasets("flat", detector=2, physical_filter="Cam1-G"))
        self.assertEqual({ref}, queried_refs_1)
        queried_refs_2 = set(
            butler.registry.queryDatasets("flat", where="detector=2 AND physical_filter='Cam1-G'")
        )
        self.assertEqual({ref}, queried_refs_2)
        # Query for data IDs with a dataset constraint.
        queried_data_ids = set(
            butler.registry.queryDataIds(
                {"instrument", "detector", "physical_filter"},
                datasets={"flat"},
                detector=2,
                physical_filter="Cam1-G",
            )
        )
        self.assertEqual({ref.dataId}, queried_data_ids)
        # Add another instrument to the repo, and a dataset that uses it to
        # the `imported_g` collection.
        butler.registry.insertDimensionData("instrument", {"name": "Cam2"})
        camera = DatasetType(
            "camera",
            dimensions=butler.dimensions["instrument"].graph,
            storageClass="Camera",
        )
        butler.registry.registerDatasetType(camera)
        butler.registry.insertDatasets(camera, [{"instrument": "Cam2"}], run="imported_g")
        # Initialize a new butler with `imported_g` as its default run.
        # This should not have a default instrument, because there are two.
        # Pass run instead of collections; this should set both.
        butler2 = Butler.from_config(butler=butler, run="imported_g")
        self.assertEqual(list(butler2.registry.defaults.collections), ["imported_g"])
        self.assertEqual(butler2.registry.defaults.run, "imported_g")
        self.assertFalse(butler2.registry.defaults.dataId)
        # Initialize a new butler with an instrument default explicitly given.
        # Set collections instead of run, which should then be None.
        butler3 = Butler.from_config(butler=butler, collections=["imported_g"], instrument="Cam2")
        self.assertEqual(list(butler3.registry.defaults.collections), ["imported_g"])
        self.assertIsNone(butler3.registry.defaults.run, None)
        self.assertEqual(butler3.registry.defaults.dataId.required, {"instrument": "Cam2"})

        # Check that repr() does not fail.
        defaults = RegistryDefaults(collections=["imported_g"], run="test")
        r = repr(defaults)
        self.assertIn("collections=('imported_g',)", r)
        self.assertIn("run='test'", r)

        defaults = RegistryDefaults(run="test", instrument="DummyCam", skypix="pix")
        r = repr(defaults)
        self.assertIn("skypix='pix'", r)
        self.assertIn("instrument='DummyCam'", r)

    def testJson(self):
        """Test JSON serialization mediated by registry."""
        butler = self.makeButler(writeable=True)
        butler.import_(filename=os.path.join(TESTDIR, "data", "registry", "base.yaml"))
        butler.import_(filename=os.path.join(TESTDIR, "data", "registry", self.datasetsImportFile))
        # Need to actually set defaults later, not at construction, because
        # we need to import the instrument before we can use it as a default.
        # Don't set a default instrument value for data IDs, because 'Cam1'
        # should be inferred by virtue of that being the only value in the
        # input collections.
        butler.registry.defaults = RegistryDefaults(collections=["imported_g"])
        # Use findDataset without collections or instrument.
        ref = butler.find_dataset("flat", detector=2, physical_filter="Cam1-G")

        # Transform the ref and dataset type to and from JSON
        # and check that it can be reconstructed properly

        # Do it with the ref and a component ref in minimal and standard form
        compRef = ref.makeComponentRef("wcs")

        for test_item in (ref, ref.datasetType, compRef, compRef.datasetType):
            for minimal in (False, True):
                json_str = test_item.to_json(minimal=minimal)
                from_json = type(test_item).from_json(json_str, registry=butler.registry)
                self.assertEqual(from_json, test_item, msg=f"From JSON '{json_str}' using registry")

                # for minimal=False case also do a test without registry
                if not minimal:
                    from_json = type(test_item).from_json(json_str, universe=butler.dimensions)
                    self.assertEqual(from_json, test_item, msg=f"From JSON '{json_str}' using universe")

    def testJsonDimensionRecordsAndHtmlRepresentation(self):
        # Dimension Records
        butler = self.makeButler(writeable=True)
        butler.import_(filename=os.path.join(TESTDIR, "data", "registry", "hsc-rc2-subset.yaml"))

        for dimension in ("detector", "visit"):
            records = butler.registry.queryDimensionRecords(dimension, instrument="HSC")
            for r in records:
                for minimal in (True, False):
                    json_str = r.to_json(minimal=minimal)
                    r_json = type(r).from_json(json_str, registry=butler.registry)
                    self.assertEqual(r_json, r)
                    # check with direct method
                    simple = r.to_simple()
                    fromDirect = type(simple).direct(**json.loads(json_str))
                    self.assertEqual(simple, fromDirect)
                    # Also check equality of each of the components as dicts
                    self.assertEqual(r_json.toDict(), r.toDict())

                    # check the html representation of records
                    r_html = r._repr_html_()
                    self.assertTrue(isinstance(r_html, str))
                    self.assertIn(dimension, r_html)

    def testWildcardQueries(self):
        """Test that different collection type queries work."""
        # Import data to play with.
        butler = self.makeButler(writeable=True)
        butler.import_(filename=os.path.join(TESTDIR, "data", "registry", "base.yaml"))

        # Create some collections
        created = {"collection", "u/user/test", "coll3"}
        for collection in created:
            butler.registry.registerCollection(collection, type=CollectionType.RUN)

        collections = butler.registry.queryCollections()
        self.assertEqual(set(collections), created)

        expressions = (
            ("collection", {"collection"}),
            (..., created),
            ("*", created),
            (("collection", "*"), created),
            ("u/*", {"u/user/test"}),
            (re.compile("u.*"), {"u/user/test"}),
            (re.compile(".*oll.*"), {"collection", "coll3"}),
            ("*oll*", {"collection", "coll3"}),
            ((re.compile(r".*\d$"), "u/user/test"), {"coll3", "u/user/test"}),
            ("*[0-9]", {"coll3"}),
        )
        for expression, expected in expressions:
            result = butler.registry.queryCollections(expression)
            self.assertEqual(set(result), expected)


class SimpleButlerMixedUUIDTestCase(SimpleButlerTestCase):
    """Same as SimpleButlerTestCase but uses UUID-based datasets manager and
    loads datasets from YAML file with integer IDs.
    """

    datasetsImportFile = "datasets.yaml"


if __name__ == "__main__":
    unittest.main()

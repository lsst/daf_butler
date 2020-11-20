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

from __future__ import annotations

import os
import shutil
import tempfile
from typing import Any
import unittest

import astropy.time

from lsst.daf.butler import (
    Butler,
    ButlerConfig,
    CollectionType,
    Registry,
    Timespan,
)
from lsst.daf.butler.registry import RegistryConfig
from lsst.daf.butler.tests import DatastoreMock


TESTDIR = os.path.abspath(os.path.dirname(__file__))


class SimpleButlerTestCase(unittest.TestCase):
    """Tests for butler (including import/export functionality) that should not
    depend on the Registry Database backend or Datastore implementation, and
    can instead utilize an in-memory SQLite Registry and a mocked Datastore.
    """

    def setUp(self):
        self.root = tempfile.mkdtemp()

    def tearDown(self):
        if self.root is not None and os.path.exists(self.root):
            shutil.rmtree(self.root, ignore_errors=True)

    def makeButler(self, **kwargs: Any) -> Butler:
        """Return new Butler instance on each call.
        """
        config = ButlerConfig()

        # make separate temporary directory for registry of this instance
        tmpdir = tempfile.mkdtemp(dir=self.root)
        config["registry", "db"] = f"sqlite:///{tmpdir}/gen3.sqlite3"
        config["root"] = self.root

        # have to make a registry first
        registryConfig = RegistryConfig(config.get("registry"))
        Registry.createFromConfig(registryConfig)

        butler = Butler(config, **kwargs)
        DatastoreMock.apply(butler)
        return butler

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
            set(record.id for record in butler.registry.queryDimensionRecords("detector", instrument="HSC")),
            set(range(104)),  # should have all science CCDs; may have some focus ones.
        )
        self.assertGreaterEqual(
            {
                (record.id, record.physical_filter)
                for record in butler.registry.queryDimensionRecords("visit", instrument="HSC")
            },
            {
                (27136, 'HSC-Z'),
                (11694, 'HSC-G'),
                (23910, 'HSC-R'),
                (11720, 'HSC-Y'),
                (23900, 'HSC-R'),
                (22646, 'HSC-Y'),
                (1248, 'HSC-I'),
                (19680, 'HSC-I'),
                (1240, 'HSC-I'),
                (424, 'HSC-Y'),
                (19658, 'HSC-I'),
                (344, 'HSC-Y'),
                (1218, 'HSC-R'),
                (1190, 'HSC-Z'),
                (23718, 'HSC-R'),
                (11700, 'HSC-G'),
                (26036, 'HSC-G'),
                (23872, 'HSC-R'),
                (1170, 'HSC-Z'),
                (1876, 'HSC-Y'),
            }
        )

    def testDatasetTransfers(self):
        """Test exporting all datasets from a repo and then importing them all
        back in again.
        """
        # Import data to play with.
        butler1 = self.makeButler(writeable=True)
        butler1.import_(filename=os.path.join(TESTDIR, "data", "registry", "base.yaml"))
        butler1.import_(filename=os.path.join(TESTDIR, "data", "registry", "datasets.yaml"))
        with tempfile.NamedTemporaryFile(mode='w', suffix=".yaml") as file:
            # Export all datasets.
            with butler1.export(filename=file.name) as exporter:
                exporter.saveDatasets(
                    butler1.registry.queryDatasets(..., collections=...)
                )
            # Import it all again.
            butler2 = self.makeButler(writeable=True)
            butler2.import_(filename=file.name)
        # Check that it all round-tripped.  Use unresolved() to make
        # comparison not care about dataset_id values, which may be
        # rewritten.
        self.assertCountEqual(
            [ref.unresolved() for ref in butler1.registry.queryDatasets(..., collections=...)],
            [ref.unresolved() for ref in butler2.registry.queryDatasets(..., collections=...)],
        )

    def testCollectionTransfers(self):
        """Test exporting and then importing collections of various types.
        """
        # Populate a registry with some datasets.
        butler1 = self.makeButler(writeable=True)
        butler1.import_(filename=os.path.join(TESTDIR, "data", "registry", "base.yaml"))
        butler1.import_(filename=os.path.join(TESTDIR, "data", "registry", "datasets.yaml"))
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
        t1 = astropy.time.Time('2020-01-01T01:00:00', format="isot", scale="tai")
        t2 = astropy.time.Time('2020-01-01T02:00:00', format="isot", scale="tai")
        t3 = astropy.time.Time('2020-01-01T03:00:00', format="isot", scale="tai")
        bias2a = registry1.findDataset("bias", instrument="Cam1", detector=2, collections="imported_g")
        bias3a = registry1.findDataset("bias", instrument="Cam1", detector=3, collections="imported_g")
        bias2b = registry1.findDataset("bias", instrument="Cam1", detector=2, collections="imported_r")
        bias3b = registry1.findDataset("bias", instrument="Cam1", detector=3, collections="imported_r")
        registry1.certify("calibration1", [bias2a, bias3a], Timespan(t1, t2))
        registry1.certify("calibration1", [bias2b], Timespan(t2, None))
        registry1.certify("calibration1", [bias3b], Timespan(t2, t3))

        with tempfile.NamedTemporaryFile(mode='w', suffix=".yaml") as file:
            # Export all collections, and some datasets.
            with butler1.export(filename=file.name) as exporter:
                # Sort results to put chain1 before chain2, which is
                # intentionally not topological order.
                for collection in sorted(registry1.queryCollections()):
                    exporter.saveCollection(collection)
                exporter.saveDatasets(flats1)
                exporter.saveDatasets([bias2a, bias2b, bias3a, bias3b])
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
            [ref.unresolved() for ref in registry1.queryDatasets(..., collections="tag1")],
            [ref.unresolved() for ref in registry2.queryDatasets(..., collections="tag1")],
        )
        # Check that calibration collection contents are the same.
        self.assertCountEqual(
            [(assoc.ref.unresolved(), assoc.timespan)
             for assoc in registry1.queryDatasetAssociations("bias", collections="calibration1")],
            [(assoc.ref.unresolved(), assoc.timespan)
             for assoc in registry2.queryDatasetAssociations("bias", collections="calibration1")],
        )

    def testGetCalibration(self):
        """Test that `Butler.get` can be used to fetch from
        `~CollectionType.CALIBRATION` collections if the data ID includes
        extra dimensions with temporal information.
        """
        # Import data to play with.
        butler = self.makeButler(writeable=True)
        butler.import_(filename=os.path.join(TESTDIR, "data", "registry", "base.yaml"))
        butler.import_(filename=os.path.join(TESTDIR, "data", "registry", "datasets.yaml"))
        # Certify some biases into a CALIBRATION collection.
        registry = butler.registry
        registry.registerCollection("calibs", CollectionType.CALIBRATION)
        t1 = astropy.time.Time('2020-01-01T01:00:00', format="isot", scale="tai")
        t2 = astropy.time.Time('2020-01-01T02:00:00', format="isot", scale="tai")
        t3 = astropy.time.Time('2020-01-01T03:00:00', format="isot", scale="tai")
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
        bias2a_id, _ = butler.get("bias", {"instrument": "Cam1", "exposure": 3, "detector": 2},
                                  collections="calibs")
        self.assertEqual(bias2a_id, bias2a.id)
        bias3b_id, _ = butler.get("bias", {"instrument": "Cam1", "exposure": 4, "detector": 3},
                                  collections="calibs")
        self.assertEqual(bias3b_id, bias3b.id)

        # Get using the kwarg form
        bias3b_id, _ = butler.get("bias",
                                  instrument="Cam1", exposure=4, detector=3,
                                  collections="calibs")
        self.assertEqual(bias3b_id, bias3b.id)

        # Do it again but using the record information
        bias2a_id, _ = butler.get("bias", {"instrument": "Cam1", "exposure.obs_id": "three",
                                           "detector.full_name": "Ab"},
                                  collections="calibs")
        self.assertEqual(bias2a_id, bias2a.id)
        bias3b_id, _ = butler.get("bias", {"exposure.obs_id": "four",
                                           "detector.full_name": "Ba"},
                                  collections="calibs", instrument="Cam1")
        self.assertEqual(bias3b_id, bias3b.id)

        # And again but this time using the alternate value rather than
        # the primary.
        bias3b_id, _ = butler.get("bias", {"exposure": "four",
                                           "detector": "Ba"},
                                  collections="calibs", instrument="Cam1")
        self.assertEqual(bias3b_id, bias3b.id)

        # And again but this time using the alternate value rather than
        # the primary and do it in the keyword arguments.
        bias3b_id, _ = butler.get("bias",
                                  exposure="four", detector="Ba",
                                  collections="calibs", instrument="Cam1")
        self.assertEqual(bias3b_id, bias3b.id)

        # Now with implied record columns
        bias3b_id, _ = butler.get("bias", day_obs=20211114, seq_num=42,
                                  raft="B", name_in_raft="a",
                                  collections="calibs", instrument="Cam1")
        self.assertEqual(bias3b_id, bias3b.id)


if __name__ == "__main__":
    unittest.main()

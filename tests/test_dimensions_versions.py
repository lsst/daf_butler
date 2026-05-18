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

import os
import unittest
from contextlib import closing

from pydantic import ValidationError

from lsst.daf.butler import Butler, Config, InconsistentUniverseError
from lsst.daf.butler.tests import addDataIdValue, addDatasetType, makeTestRepo
from lsst.daf.butler.tests.utils import makeTestTempDir, removeTestTempDir

TESTDIR = os.path.abspath(os.path.dirname(__file__))

_INSTRUMENT = "🔭"


def _make_butler(root: str, dimensions_version: int) -> Butler:
    path = os.path.join(
        os.path.dirname(__file__), "config", "dimensions", f"dimensions{dimensions_version}.yaml"
    )
    config = Config()
    config["datastore", "cls"] = "lsst.daf.butler.datastores.fileDatastore.FileDatastore"
    butler = makeTestRepo(root, config=config, dimensionConfig=path)

    return butler


def _populate_data_ids(butler: Butler, dimension_version: int) -> None:
    """Populate the repository with data IDs for the dimensions."""
    addDataIdValue(butler, "instrument", _INSTRUMENT)

    for detector in range(10):
        addDataIdValue(butler, "detector", detector, instrument=_INSTRUMENT)

    if dimension_version in (1, 2):
        for visit in range(10):
            addDataIdValue(butler, "visit", visit, instrument=_INSTRUMENT)

    if dimension_version in (2, 3):
        for group in "ABCD":
            addDataIdValue(butler, "group", group, instrument=_INSTRUMENT)

    if dimension_version in (3, 4):
        for day_obs in range(2):
            addDataIdValue(butler, "day_obs", day_obs, instrument=_INSTRUMENT)
        for visit in range(10):
            addDataIdValue(butler, "visit", visit, day_obs=visit % 2, instrument=_INSTRUMENT)


def _populate_repo(butler: Butler, dimension_version: int) -> None:
    """Populate the repository with some data."""
    butler.collections.register("test1")
    butler.collections.register("test2")
    butler.collections.register("test3")

    if dimension_version in (1, 2):
        dataset_type = addDatasetType(butler, "test1", {"instrument", "visit", "detector"}, "int")
        for data in range(10):
            butler.put(data, dataset_type, run="test1", instrument=_INSTRUMENT, visit=data, detector=data)

    if dimension_version == 2:
        dataset_type = addDatasetType(butler, "test2", {"instrument", "visit", "detector", "group"}, "int")
        for data in range(10):
            butler.put(
                data,
                dataset_type,
                run="test2",
                instrument=_INSTRUMENT,
                visit=data,
                detector=data,
                group="ABCD"[data % 4],
            )

    if dimension_version in (3, 4):
        dataset_type = addDatasetType(butler, "test3", {"instrument", "visit", "detector"}, "int")
        for data in range(10):
            butler.put(
                data,
                dataset_type,
                run="test3",
                instrument=_INSTRUMENT,
                visit=data,
                day_obs=data % 2,
                detector=data,
            )


class DimensionsVersionsTestCase(unittest.TestCase):
    """Tests for dimensions version compatibility."""

    def _prep_repo(self, root: str, subdir: str, dimension_version: int, fill: bool = False) -> Butler:
        butler = _make_butler(os.path.join(root, subdir), dimension_version)
        self.assertEqual(butler.dimensions.namespace, "test")
        self.assertEqual(butler.dimensions.version, dimension_version)
        if fill:
            _populate_data_ids(butler, dimension_version)
            _populate_repo(butler, dimension_version)
        return butler

    def setUp(self) -> None:
        self.root = makeTestTempDir(TESTDIR)

    def tearDown(self) -> None:
        removeTestTempDir(self.root)

    def test_v1_v2_transfer(self):
        # Transfer from v1 to v2 works fine as v2 is a superset of v1.
        with (
            closing(self._prep_repo(self.root, "butler1", 1, True)) as butler1,
            closing(self._prep_repo(self.root, "butler2", 2)) as butler2,
        ):
            refs = butler1.query_datasets("test1", collections="test1")
            self.assertEqual(len(refs), 10)
            butler2.transfer_from(butler1, refs, register_dataset_types=True, transfer_dimensions=True)

    def test_v1_v2_ingest_zip(self):
        # Ingest_zip v1 to v2 works fine as v2 is a superset of v1.
        with (
            closing(self._prep_repo(self.root, "butler1", 1, True)) as butler1,
            closing(self._prep_repo(self.root, "butler2", 2)) as butler2,
        ):
            refs = butler1.query_datasets("test1", collections="test1", with_dimension_records=True)
            self.assertEqual(len(refs), 10)
            export_path = butler1.retrieve_artifacts_zip(refs, os.path.join(self.root, "export"))
            butler2.ingest_zip(export_path, transfer_dimensions=True)

    def test_v2_v1_transfer(self):
        # Transfer from v2 to v1 works if using a subset of v2 dimensions.
        with (
            closing(self._prep_repo(self.root, "butler1", 1)) as butler1,
            closing(self._prep_repo(self.root, "butler2", 2, True)) as butler2,
        ):
            refs = butler2.query_datasets("test1", collections="test1")
            self.assertEqual(len(refs), 10)
            butler1.transfer_from(butler2, refs, register_dataset_types=True, transfer_dimensions=True)

            refs = butler2.query_datasets("test2", collections="test2")
            self.assertEqual(len(refs), 10)
            with self.assertRaises(InconsistentUniverseError):
                butler1.transfer_from(butler2, refs, register_dataset_types=True, transfer_dimensions=True)

    def test_v2_v1_ingest_zip(self):
        # Ingest_zip from v2 to v1 works if using a subset of v2 dimensions.
        with (
            closing(self._prep_repo(self.root, "butler1", 1)) as butler1,
            closing(self._prep_repo(self.root, "butler2", 2, True)) as butler2,
        ):
            refs = butler2.query_datasets("test1", collections="test1", with_dimension_records=True)
            self.assertEqual(len(refs), 10)
            export_path = butler2.retrieve_artifacts_zip(refs, os.path.join(self.root, "export"))
            butler1.ingest_zip(export_path, transfer_dimensions=True)

            refs = butler2.query_datasets("test2", collections="test2", with_dimension_records=True)
            self.assertEqual(len(refs), 10)
            export_path = butler2.retrieve_artifacts_zip(refs, os.path.join(self.root, "export2"))
            with self.assertRaises(InconsistentUniverseError):
                butler1.ingest_zip(export_path, transfer_dimensions=True)

    def test_v2_v3_transfer(self):
        # Transfer from v2 to v3 fails because day_obs is required for visit.
        with (
            closing(self._prep_repo(self.root, "butler2", 2, True)) as butler2,
            closing(self._prep_repo(self.root, "butler3", 3)) as butler3,
        ):
            refs = butler2.query_datasets("test1", collections="test1")
            self.assertEqual(len(refs), 10)
            with self.assertRaises(InconsistentUniverseError):
                butler3.transfer_from(butler2, refs, register_dataset_types=True, transfer_dimensions=True)

    def test_v2_v3_ingest_zip(self):
        # Transfer from v2 to v3 fails because day_obs is required for visit.
        with (
            closing(self._prep_repo(self.root, "butler2", 2, True)) as butler2,
            closing(self._prep_repo(self.root, "butler3", 3)) as butler3,
        ):
            refs = butler2.query_datasets("test1", collections="test1", with_dimension_records=True)
            self.assertEqual(len(refs), 10)
            export_path = butler2.retrieve_artifacts_zip(refs, os.path.join(self.root, "export"))
            with self.assertRaises(InconsistentUniverseError):
                butler3.ingest_zip(export_path, transfer_dimensions=True)

    def test_v3_v2_transfer(self):
        # Transfer from v3 to v2 fails because day_obs is required for visit.
        with (
            closing(self._prep_repo(self.root, "butler2", 2)) as butler2,
            closing(self._prep_repo(self.root, "butler3", 3, True)) as butler3,
        ):
            refs = butler3.query_datasets("test3", collections="test3")
            self.assertEqual(len(refs), 10)
            with self.assertRaises(InconsistentUniverseError):
                butler2.transfer_from(butler3, refs, register_dataset_types=True, transfer_dimensions=True)

    def test_v3_v2_ingest_zip(self):
        # Transfer from v3 to v2 fails because day_obs is required for visit.
        with (
            closing(self._prep_repo(self.root, "butler2", 2)) as butler2,
            closing(self._prep_repo(self.root, "butler3", 3, True)) as butler3,
        ):
            refs = butler3.query_datasets("test3", collections="test3", with_dimension_records=True)
            self.assertEqual(len(refs), 10)
            export_path = butler3.retrieve_artifacts_zip(refs, os.path.join(self.root, "export"))
            with self.assertRaises(InconsistentUniverseError):
                butler2.ingest_zip(export_path, transfer_dimensions=True)

    def test_v3_v4_transfer(self):
        # Transfer from v3 to v4 works OK.
        with (
            closing(self._prep_repo(self.root, "butler3", 3, True)) as butler3,
            closing(self._prep_repo(self.root, "butler4", 4)) as butler4,
        ):
            refs = butler3.query_datasets("test3", collections="test3")
            self.assertEqual(len(refs), 10)
            butler4.transfer_from(butler3, refs, register_dataset_types=True, transfer_dimensions=True)

    def test_v3_v4_ingest_zip(self):
        # Ingest_zip from v3 to v4 fails due to pydantic validation error.
        with (
            closing(self._prep_repo(self.root, "butler3", 3, True)) as butler3,
            closing(self._prep_repo(self.root, "butler4", 4)) as butler4,
        ):
            refs = butler3.query_datasets("test3", collections="test3", with_dimension_records=True)
            self.assertEqual(len(refs), 10)
            export_path = butler3.retrieve_artifacts_zip(refs, os.path.join(self.root, "export"))
            with self.assertRaises(ValidationError):
                butler4.ingest_zip(export_path, transfer_dimensions=True)

    def test_v4_v3_transfer(self):
        # Transfer from v4 to v3 works OK.
        with (
            closing(self._prep_repo(self.root, "butler3", 3)) as butler3,
            closing(self._prep_repo(self.root, "butler4", 4, True)) as butler4,
        ):
            refs = butler4.query_datasets("test3", collections="test3")
            self.assertEqual(len(refs), 10)
            butler3.transfer_from(butler4, refs, register_dataset_types=True, transfer_dimensions=True)

    def test_v4_v3_ingest_zip(self):
        # Ingest_zip from v4 to v3 fails due to pydantic validation error.
        with (
            closing(self._prep_repo(self.root, "butler3", 3)) as butler3,
            closing(self._prep_repo(self.root, "butler4", 4, True)) as butler4,
        ):
            refs = butler4.query_datasets("test3", collections="test3", with_dimension_records=True)
            self.assertEqual(len(refs), 10)
            export_path = butler4.retrieve_artifacts_zip(refs, os.path.join(self.root, "export"))
            with self.assertRaises(ValidationError):
                butler3.ingest_zip(export_path, transfer_dimensions=True)


if __name__ == "__main__":
    unittest.main()

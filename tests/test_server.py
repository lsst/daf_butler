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

import os.path
import unittest

try:
    import lsst.daf.butler.server
except ImportError:
    pass  # Import below will set skip variable.

try:
    from fastapi.testclient import TestClient
except ImportError:
    TestClient = None
from lsst.daf.butler import Butler, Config, DataCoordinate

try:
    from lsst.daf.butler.server import app
except ImportError:
    app = None
from lsst.daf.butler.tests.utils import MetricTestRepo, makeTestTempDir, removeTestTempDir

TESTDIR = os.path.abspath(os.path.dirname(__file__))


@unittest.skipIf(TestClient is None or app is None, "FastAPI not installed.")
class ButlerClientServerTestCase(unittest.TestCase):
    """Test for Butler client/server."""

    @classmethod
    def setUpClass(cls):
        # First create a butler and populate it.
        cls.root = makeTestTempDir(TESTDIR)
        cls.repo = MetricTestRepo(root=cls.root, configFile=os.path.join(TESTDIR, "config/basic/butler.yaml"))

        # Globally change where the server thinks its butler repository
        # is located. This will prevent any other server tests and is
        # not a long term fix.
        lsst.daf.butler.server.BUTLER_ROOT = cls.root
        cls.client = TestClient(app)

        # Create a client butler. We need to modify the contents of the
        # server configuration to reflect the use of the test client.
        response = cls.client.get("/butler/butler.json")
        config = Config(response.json())
        config["registry", "db"] = cls.client

        # Since there is no client datastore we also need to specify
        # the datastore root.
        config["datastore", "root"] = cls.root
        print(config)
        cls.butler = Butler(config)

    @classmethod
    def tearDownClass(cls):
        removeTestTempDir(cls.root)

    def test_simple(self):
        response = self.client.get("/butler/")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Butler Server", response.json())

        response = self.client.get("/butler/butler.json")
        self.assertEqual(response.status_code, 200)
        self.assertIn("registry", response.json())

        response = self.client.get("/butler/universe")
        self.assertEqual(response.status_code, 200)
        self.assertIn("namespace", response.json())

    def test_registry(self):
        universe = self.butler.registry.dimensions
        self.assertEqual(universe.namespace, "daf_butler")

        dataset_type = self.butler.registry.getDatasetType("test_metric_comp")
        self.assertEqual(dataset_type.name, "test_metric_comp")

        dataset_types = list(self.butler.registry.queryDatasetTypes(...))
        self.assertEqual(len(dataset_types), 1)
        dataset_types = list(self.butler.registry.queryDatasetTypes("test_*"))
        self.assertEqual(len(dataset_types), 1)

        collections = self.butler.registry.queryCollections(...)
        self.assertEqual(len(collections), 2, collections)

        collection_type = self.butler.registry.getCollectionType("ingest")
        self.assertEqual(collection_type.name, "TAGGED")

        datasets = list(self.butler.registry.queryDatasets(..., collections=...))
        self.assertEqual(len(datasets), 2)

        ref = self.butler.registry.getDataset(datasets[0].id)
        self.assertEqual(ref, datasets[0])

        dataIds = list(self.butler.registry.queryDataIds("visit", dataId={"instrument": "DummyCamComp"}))
        self.assertEqual(len(dataIds), 2)

        # Create a DataCoordinate to test the alternate path for specifying
        # a data ID.
        data_id = DataCoordinate.standardize(
            {"instrument": "DummyCamComp"}, universe=self.butler.registry.dimensions
        )
        records = list(self.butler.registry.queryDimensionRecords("physical_filter", dataId=data_id))
        self.assertEqual(len(records), 1)


if __name__ == "__main__":
    unittest.main()

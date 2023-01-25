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
import uuid

try:
    # Failing to import any of these should disable the tests.
    import lsst.daf.butler.server
    from fastapi.testclient import TestClient
    from lsst.daf.butler.server import app
except ImportError:
    TestClient = None
    app = None

from lsst.daf.butler import Butler, CollectionType, Config, DataCoordinate, DatasetRef
from lsst.daf.butler.tests import addDatasetType
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

        # Add a collection chain.
        cls.repo.butler.registry.registerCollection("chain", CollectionType.CHAINED)
        cls.repo.butler.registry.setCollectionChain("chain", ["ingest"])

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

        response = self.client.get("/butler/v1/universe")
        self.assertEqual(response.status_code, 200)
        self.assertIn("namespace", response.json())

    def test_registry(self):
        universe = self.butler.registry.dimensions
        self.assertEqual(universe.namespace, "daf_butler")

        dataset_type = self.butler.registry.getDatasetType("test_metric_comp")
        self.assertEqual(dataset_type.name, "test_metric_comp")

        dataset_types = list(self.butler.registry.queryDatasetTypes(...))
        self.assertIn("test_metric_comp", [ds.name for ds in dataset_types])
        dataset_types = list(self.butler.registry.queryDatasetTypes("test_*"))
        self.assertEqual(len(dataset_types), 1)

        collections = self.butler.registry.queryCollections(
            ..., collectionTypes={CollectionType.RUN, CollectionType.TAGGED}
        )
        self.assertEqual(len(collections), 2, collections)

        collection_type = self.butler.registry.getCollectionType("ingest")
        self.assertEqual(collection_type.name, "TAGGED")

        chain = self.butler.registry.getCollectionChain("chain")
        self.assertEqual([coll for coll in chain], ["ingest"])

        datasets = list(self.butler.registry.queryDatasets("test_metric_comp", collections=...))
        self.assertEqual(len(datasets), 2)

        ref = self.butler.registry.getDataset(datasets[0].id)
        self.assertEqual(ref, datasets[0])

        locations = self.butler.registry.getDatasetLocations(ref)
        self.assertEqual(locations[0], "FileDatastore@<butlerRoot>")

        fake_ref = DatasetRef(
            dataset_type,
            dataId={"instrument": "DummyCamComp", "physical_filter": "d-r", "visit": 424},
            id=uuid.uuid4(),
            run="missing",
        )
        locations = self.butler.registry.getDatasetLocations(fake_ref)
        self.assertEqual(locations, [])

        dataIds = list(self.butler.registry.queryDataIds("visit", dataId={"instrument": "DummyCamComp"}))
        self.assertEqual(len(dataIds), 2)

        # Create a DataCoordinate to test the alternate path for specifying
        # a data ID.
        data_id = DataCoordinate.standardize(
            {"instrument": "DummyCamComp"}, universe=self.butler.registry.dimensions
        )
        records = list(self.butler.registry.queryDimensionRecords("physical_filter", dataId=data_id))
        self.assertEqual(len(records), 1)

    def test_experimental(self):
        """Experimental interfaces."""
        # Got URI testing we can not yet support disassembly so must
        # add a dataset with a different dataset type.
        datasetType = addDatasetType(
            self.repo.butler, "metric", {"instrument", "visit"}, "StructuredCompositeReadCompNoDisassembly"
        )

        self.repo.addDataset({"instrument": "DummyCamComp", "visit": 424}, datasetType=datasetType)
        self.butler.registry.refresh()

        # Need a DatasetRef.
        datasets = list(self.butler.registry.queryDatasets("metric", collections=...))

        response = self.client.get(f"/butler/v1/uri/{datasets[0].id}")
        self.assertEqual(response.status_code, 200)
        self.assertIn("file://", response.json())


if __name__ == "__main__":
    unittest.main()

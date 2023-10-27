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

import os.path
import unittest
import uuid

try:
    # Failing to import any of these should disable the tests.
    from fastapi.testclient import TestClient
    from lsst.daf.butler.remote_butler import RemoteButler
    from lsst.daf.butler.remote_butler.server import Factory, app, factory_dependency
except ImportError:
    TestClient = None
    app = None

from lsst.daf.butler import Butler, DataCoordinate, DatasetRef
from lsst.daf.butler.tests import DatastoreMock
from lsst.daf.butler.tests.utils import MetricTestRepo, makeTestTempDir, removeTestTempDir

TESTDIR = os.path.abspath(os.path.dirname(__file__))


def _make_remote_butler(http_client):
    return RemoteButler(
        config={
            "remote_butler": {
                # This URL is ignored because we override the HTTP client, but
                # must be valid to satisfy the config validation
                "url": "https://test.example"
            }
        },
        http_client=http_client,
    )


@unittest.skipIf(TestClient is None or app is None, "FastAPI not installed.")
class ButlerClientServerTestCase(unittest.TestCase):
    """Test for Butler client/server."""

    @classmethod
    def setUpClass(cls):
        # First create a butler and populate it.
        cls.root = makeTestTempDir(TESTDIR)
        cls.repo = MetricTestRepo(root=cls.root, configFile=os.path.join(TESTDIR, "config/basic/butler.yaml"))
        # Override the server's Butler initialization to point at our test repo
        server_butler = Butler.from_config(cls.root, writeable=True)

        # Not yet testing butler.get()
        DatastoreMock.apply(server_butler)

        def create_factory_dependency():
            return Factory(butler=server_butler)

        app.dependency_overrides[factory_dependency] = create_factory_dependency

        # Set up the RemoteButler that will connect to the server
        cls.client = TestClient(app)
        cls.butler = _make_remote_butler(cls.client)

        # Populate the test server.
        server_butler.import_(filename=os.path.join(TESTDIR, "data", "registry", "base.yaml"))
        server_butler.import_(filename=os.path.join(TESTDIR, "data", "registry", "datasets-uuid.yaml"))

    @classmethod
    def tearDownClass(cls):
        del app.dependency_overrides[factory_dependency]
        removeTestTempDir(cls.root)

    def test_simple(self):
        response = self.client.get("/butler/v1/universe")
        self.assertEqual(response.status_code, 200)
        self.assertIn("namespace", response.json())

    def test_remote_butler(self):
        universe = self.butler.dimensions
        self.assertEqual(universe.namespace, "daf_butler")
        self.assertFalse(self.butler.isWriteable())

    def test_get_dataset_type(self):
        bias_type = self.butler.get_dataset_type("bias")
        self.assertEqual(bias_type.name, "bias")

    def test_find_dataset(self):
        ref = self.butler.find_dataset("bias", collections="imported_g", detector=1, instrument="Cam1")
        self.assertIsInstance(ref, DatasetRef)
        self.assertEqual(ref.id, uuid.UUID("e15ab039-bc8b-4135-87c5-90902a7c0b22"))

        # Try again with variation of parameters.
        ref_new = self.butler.find_dataset(
            "bias",
            {"detector": 1},
            collections="imported_g",
            instrument="Cam1",
        )
        self.assertEqual(ref_new, ref)

        ref_new = self.butler.find_dataset(
            ref.datasetType,
            DataCoordinate.standardize(detector=1, instrument="Cam1", universe=self.butler.dimensions),
            collections="imported_g",
        )
        self.assertEqual(ref_new, ref)

        ref2 = self.butler.get_dataset(ref.id)
        self.assertEqual(ref2, ref)

        # Unknown dataset should not fail.
        self.assertIsNone(self.butler.get_dataset(uuid.uuid4()))


if __name__ == "__main__":
    unittest.main()

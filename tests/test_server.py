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

try:
    # Failing to import any of these should disable the tests.
    import lsst.daf.butler.server
    from fastapi.testclient import TestClient
    from lsst.daf.butler.remote_butler import RemoteButler
    from lsst.daf.butler.server import app
except ImportError:
    TestClient = None
    app = None

from lsst.daf.butler import CollectionType
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

        # Add a collection chain.
        cls.repo.butler.registry.registerCollection("chain", CollectionType.CHAINED)
        cls.repo.butler.registry.setCollectionChain("chain", ["ingest"])

        # Globally change where the server thinks its butler repository
        # is located. This will prevent any other server tests and is
        # not a long term fix.
        lsst.daf.butler.server.BUTLER_ROOT = cls.root
        cls.client = TestClient(app)

        cls.butler = _make_remote_butler(cls.client)

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

    def test_remote_butler(self):
        universe = self.butler.dimensions
        self.assertEqual(universe.namespace, "daf_butler")


if __name__ == "__main__":
    unittest.main()

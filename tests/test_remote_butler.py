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
from unittest.mock import patch

from lsst.daf.butler import Butler, Registry
from lsst.daf.butler._exceptions import UnknownButlerUserError
from lsst.daf.butler.datastores.file_datastore.retrieve_artifacts import (
    determine_destination_for_retrieved_artifact,
)
from lsst.daf.butler.registry.tests import RegistryTests
from lsst.resources import ResourcePath
from pydantic import ValidationError

try:
    import httpx
    from lsst.daf.butler.remote_butler import ButlerServerError, RemoteButler
except ImportError:
    # httpx is not available in rubin-env yet, so skip these tests if it's not
    # available
    RemoteButler = None

try:
    from lsst.daf.butler.tests.server import create_test_server
except ImportError:
    create_test_server = None

TESTDIR = os.path.abspath(os.path.dirname(__file__))


@unittest.skipIf(RemoteButler is None, "httpx is not installed")
class RemoteButlerConfigTests(unittest.TestCase):
    """Test construction of RemoteButler via Butler()"""

    def test_bad_config(self):
        with self.assertRaises(ValidationError):
            Butler({"cls": "lsst.daf.butler.remote_butler.RemoteButler", "remote_butler": {"url": "!"}})


@unittest.skipIf(create_test_server is None, "Server dependencies not installed")
class RemoteButlerErrorHandlingTests(unittest.TestCase):
    """Test RemoteButler error handling."""

    def setUp(self):
        server_instance = self.enterContext(create_test_server(TESTDIR))
        self.butler = server_instance.remote_butler
        self.mock = self.enterContext(patch.object(self.butler._connection._client, "request"))

    def _mock_error_response(self, content: str) -> None:
        self.mock.return_value = httpx.Response(
            status_code=422, content=content, request=httpx.Request("GET", "/")
        )

    def test_internal_server_error(self):
        self.mock.side_effect = httpx.HTTPError("unhandled error")
        with self.assertRaises(ButlerServerError):
            self.butler.get_dataset_type("int")

    def test_unknown_error_type(self):
        self.mock.return_value = httpx.Response(
            status_code=422, json={"error_type": "not a known error type", "detail": "an error happened"}
        )
        with self.assertRaises(UnknownButlerUserError):
            self.butler.get_dataset_type("int")

    def test_non_json_error(self):
        # Server returns a non-JSON body with an error
        self._mock_error_response("notvalidjson")
        with self.assertRaises(ButlerServerError):
            self.butler.get_dataset_type("int")

    def test_malformed_error(self):
        # Server returns JSON, but not in the expected format.
        self._mock_error_response("{}")
        with self.assertRaises(ButlerServerError):
            self.butler.get_dataset_type("int")


class RemoteButlerMiscTests(unittest.TestCase):
    """Test miscellaneous RemoteButler functionality."""

    def test_retrieve_artifacts_security(self):
        # Make sure that the function used to determine output file paths for
        # retrieveArtifacts throws if a malicious server tries to escape its
        # destination directory.
        with self.assertRaisesRegex(ValueError, "^File path attempts to escape destination directory"):
            determine_destination_for_retrieved_artifact(
                ResourcePath("output_directory/"),
                ResourcePath("../something.txt", forceAbsolute=False),
                preserve_path=True,
            )

        # Make sure all paths are forced to relative paths, even if the server
        # sends an absolute path.
        self.assertEqual(
            determine_destination_for_retrieved_artifact(
                ResourcePath("/tmp/output_directory/"),
                ResourcePath("file:///not/relative.txt"),
                preserve_path=True,
            ),
            ResourcePath("/tmp/output_directory/not/relative.txt"),
        )


@unittest.skipIf(create_test_server is None, "Server dependencies not installed.")
class RemoteButlerRegistryTests(RegistryTests, unittest.TestCase):
    """Tests for RemoteButler's `Registry` shim."""

    supportsCollectionRegex = False

    # RemoteButler implements registry.query methods by forwarding to the new
    # query system, which doesn't have the same diagnostics as the old one
    # and also does not support query offset.
    supportsDetailedQueryExplain = False
    supportsQueryOffset = False
    supportsQueryGovernorValidation = False

    def setUp(self):
        self.server_instance = self.enterContext(create_test_server(TESTDIR))

    @classmethod
    def getDataDir(cls) -> str:
        return os.path.join(TESTDIR, "data", "registry")

    def makeRegistry(self, share_repo_with: Registry | None = None) -> Registry:
        if share_repo_with is None:
            return self.server_instance.hybrid_butler.registry
        else:
            return self.server_instance.hybrid_butler._clone().registry

    def testBasicTransaction(self):
        # RemoteButler will never support transactions.
        pass

    def testNestedTransaction(self):
        # RemoteButler will never support transactions.
        pass

    def testOpaque(self):
        # This tests an internal implementation detail that isn't exposed to
        # the client side.
        pass

    def testCollectionChainPrependConcurrency(self):
        # This tests an implementation detail that requires access to the
        # collection manager object.
        pass

    def testCollectionChainReplaceConcurrency(self):
        # This tests an implementation detail that requires access to the
        # collection manager object.
        pass

    def testAttributeManager(self):
        # Tests a non-public API that isn't relevant on the client side.
        pass


if __name__ == "__main__":
    unittest.main()

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

from pydantic import ValidationError

from lsst.daf.butler import Butler
from lsst.daf.butler._exceptions import UnknownButlerUserError
from lsst.daf.butler.datastores.file_datastore.retrieve_artifacts import (
    determine_destination_for_retrieved_artifact,
)
from lsst.daf.butler.registry.tests import RegistryTests
from lsst.daf.butler.tests.postgresql import TemporaryPostgresInstance, setup_postgres_test_db
from lsst.resources import ResourcePath

try:
    import httpx

    from lsst.daf.butler.remote_butler import ButlerServerError, RemoteButler

    remote_butler_import_fail_message = ""
except ImportError as e:
    # httpx is not available in rubin-env yet, so skip these tests if it's not
    # available
    RemoteButler = None
    remote_butler_import_fail_message = str(e)

try:
    from lsst.daf.butler.tests.server import create_test_server

    server_import_fail_message = ""
except ImportError as e:
    create_test_server = None
    server_import_fail_message = str(e)

TESTDIR = os.path.abspath(os.path.dirname(__file__))


@unittest.skipIf(
    RemoteButler is None, f"Remote butler can not be imported: {remote_butler_import_fail_message}"
)
class RemoteButlerConfigTests(unittest.TestCase):
    """Test construction of RemoteButler via Butler()"""

    def test_bad_config(self):
        with self.assertRaises(ValidationError):
            Butler({"cls": "lsst.daf.butler.remote_butler.RemoteButler", "remote_butler": {"url": "!"}})


@unittest.skipIf(
    create_test_server is None, f"Server dependencies not installed: {server_import_fail_message}"
)
class RemoteButlerErrorHandlingTests(unittest.TestCase):
    """Test RemoteButler error handling."""

    def setUp(self):
        server_instance = self.enterContext(create_test_server(TESTDIR))
        self.butler = server_instance.remote_butler
        self.mock = self.enterContext(patch.object(self.butler._connection._client, "send"))

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

        # Test prefixing.
        self.assertEqual(
            determine_destination_for_retrieved_artifact(
                ResourcePath("/tmp/output_directory/"),
                ResourcePath("file:///not/relative.txt"),
                preserve_path=False,
                prefix="prefix-",
            ),
            ResourcePath("/tmp/output_directory/prefix-relative.txt"),
        )


class RemoteButlerRegistryTests(RegistryTests):
    """Tests for RemoteButler's `Registry` shim."""

    supportsCollectionRegex = False

    # RemoteButler implements registry.query methods by forwarding to the new
    # query system, which doesn't have the same diagnostics as the old one
    # and also does not support query offset.
    supportsDetailedQueryExplain = False
    supportsQueryOffset = False
    supportsQueryGovernorValidation = False
    supportsNonCommonSkypixQueries = False

    # Jim decided to drop these expressions from the new query system -- they
    # can be written less ambiguously by writing e.g. ``time <
    # timespan.begin`` instead of ``time < timespan``.
    supportsExtendedTimeQueryOperators = False

    postgres: TemporaryPostgresInstance | None

    def setUp(self):
        self.server_instance = self.enterContext(create_test_server(TESTDIR, postgres=self.postgres))

    @classmethod
    def getDataDir(cls) -> str:
        return os.path.join(TESTDIR, "data", "registry")

    def make_butler(self) -> Butler:
        return self.server_instance.hybrid_butler

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

    def testQueryDataIdsGovernorExceptions(self):
        # The new query system doesn't throw exceptions for invalid governor
        # data IDs in queries -- instead it returns zero rows.  So this set of
        # tests is not applicable to RemoteButler.
        pass

    def test_query_projection_drop_postprocessing(self):
        # Tests a query system implementation detail that isn't relevant to
        # RemoteButler.
        pass


@unittest.skipIf(
    create_test_server is None, f"Server dependencies not installed: {server_import_fail_message}"
)
class RemoteButlerSqliteRegistryTests(RemoteButlerRegistryTests, unittest.TestCase):
    """Tests for RemoteButler's registry shim, with a SQLite DB backing the
    server.
    """

    postgres = None


@unittest.skipIf(
    create_test_server is None, f"Server dependencies not installed: {server_import_fail_message}"
)
class RemoteButlerPostgresRegistryTests(RemoteButlerRegistryTests, unittest.TestCase):
    """Tests for RemoteButler's registry shim, with a Postgres DB backing the
    server.
    """

    @classmethod
    def setUpClass(cls):
        cls.postgres = cls.enterClassContext(setup_postgres_test_db())
        super().setUpClass()

    def testSkipCalibs(self):
        if self.postgres.server_major_version() < 16:
            # TODO DM-44875: This test currently fails for older Postgres.
            self.skipTest("TODO DM-44875")
        return super().testSkipCalibs()


if __name__ == "__main__":
    unittest.main()

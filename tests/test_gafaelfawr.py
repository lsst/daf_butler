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

import unittest

from lsst.daf.butler.tests.server_available import butler_server_import_error, butler_server_is_available

if butler_server_is_available:
    import fastapi
    import httpx

    from lsst.daf.butler.remote_butler.server._dependencies import repository_authorization_dependency
    from lsst.daf.butler.remote_butler.server._gafaelfawr import GafaelfawrClient, GafaelfawrGroupAuthorizer


# FastAPI is not installed during LSST Pipelines stack builds, so skip these
# tests if it is not available.
@unittest.skipIf(not butler_server_is_available, butler_server_import_error)
class GafaelfawrAuthorizationTestCase(unittest.IsolatedAsyncioTestCase):
    """Test authorization checks using Gafaelfawr group membership."""

    async def test_gafaelfawr_group_auth(self) -> None:
        response_code = 200
        response_data = {"username": "some-user", "groups": [{"name": "some-group"}, {"name": "b"}]}
        request_headers: httpx.Headers = httpx.Headers(None)
        request_count = 0

        def handler(request: httpx.Request):
            nonlocal request_headers
            request_headers = request.headers
            nonlocal request_count
            request_count += 1
            return httpx.Response(response_code, json=response_data)

        transport = httpx.MockTransport(handler)

        client = GafaelfawrClient("http://gafaelfawr.example", transport=transport)
        authorizer = GafaelfawrGroupAuthorizer(
            client, {"any_group": ["*"], "group_a": ["a"], "group_b": ["c", "b"]}
        )

        with self.assertRaises(fastapi.HTTPException) as e:
            await repository_authorization_dependency("group_a", "username", "mock-token", authorizer)
        self.assertEqual(e.exception.status_code, 403)
        self.assertEqual(request_headers.get("Authorization"), "Bearer mock-token")

        # Should authorize the special '*' all users group without hitting
        # Gafaelfawr service.
        request_count = 0
        await repository_authorization_dependency("any_group", "username", "mock-token", authorizer)
        self.assertEqual(request_count, 0)

        # Should hit the Gafaelfawr service to check that the user is in group
        # "b".
        request_count = 0
        await repository_authorization_dependency("group_b", "username", "mock-token", authorizer)
        self.assertEqual(request_count, 1)

        # A second request with the same username should be cached...
        request_count = 0
        await repository_authorization_dependency("group_b", "username", "mock-token", authorizer)
        self.assertEqual(request_count, 0)

        # But it should go back to the server for a different user
        request_count = 0
        response_data = {"username": "other-username", "groups": [{"name": "incorrect-group"}]}
        with self.assertRaises(fastapi.HTTPException) as e:
            await repository_authorization_dependency("group_b", "other-username", "mock-token", authorizer)
        self.assertEqual(e.exception.status_code, 403)
        self.assertEqual(request_count, 1)

        # Bad repository name
        with self.assertRaises(ValueError):
            await repository_authorization_dependency(
                "unknown_repository", "username", "mock-token", authorizer
            )

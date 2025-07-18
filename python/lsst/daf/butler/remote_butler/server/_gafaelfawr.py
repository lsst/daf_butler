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

from collections import defaultdict

import httpx
import pydantic

from ..authentication.rubin import RubinAuthenticationProvider


class GafaelfawrClient:
    """REST client for retrieving authentication information from
    Gafaelfawr.

    Parameters
    ----------
    base_url : `str`
        The top-level HTTP path where Gafaelfawr can be found (e.g.
        ``"https://data-int.lsst.cloud/auth"``).
    transport : ``httpx.AsyncBaseTransport``, optional
        Override the HTTP client's transport.  (For unit tests).
    """

    def __init__(self, base_url: str, *, transport: httpx.AsyncBaseTransport | None = None) -> None:
        if transport is None:
            transport = httpx.AsyncHTTPTransport(retries=3)
        self._client = httpx.AsyncClient(base_url=base_url, transport=transport, timeout=20.0)

    async def get_groups(self, user_token: str) -> list[str]:
        response = await self._client.get(
            "/api/v1/user-info", headers=RubinAuthenticationProvider(user_token).get_server_headers()
        )
        response.raise_for_status()
        info = _GafaelfawrUserInfo.model_validate_json(response.content)
        if info.groups is None:
            return []
        return [group.name for group in info.groups]


class _GafaelfawrUserInfo(pydantic.BaseModel):
    username: str
    groups: list[_GafaelfawrGroup] | None = None


class _GafaelfawrGroup(pydantic.BaseModel):
    name: str


class GafaelfawrGroupAuthorizer:
    """Authorizes access to Butler repositories on the basis of Gafaelfawr
    groups.

    Parameters
    ----------
    client : `GafaelfawrClient`
        `GafaelfawrClient` instance that will be used to access group
        information.
    repository_groups : `dict` [ `str, `list` [ `str` ]]
        Mapping from repository name to list of Gafaelfawr groups authorized to
        access that repository.  If a user is a member of any one of the groups
        in the list, access will be granted.
    """

    def __init__(self, client: GafaelfawrClient, repository_groups: dict[str, list[str]]) -> None:
        self._client = client
        self._repository_groups = repository_groups
        self._cache: dict[str, set[str]] = defaultdict(set)

    async def is_user_authorized_for_repository(
        self, *, repository: str, user_name: str, user_token: str
    ) -> bool:
        allowed_groups = self._repository_groups.get(repository)
        if allowed_groups is None:
            raise ValueError(f"Unknown repository '${repository}'")

        if "*" in allowed_groups:
            return True

        if user_name in self._cache[repository]:
            return True

        user_groups = await self._client.get_groups(user_token)
        if any(group in allowed_groups for group in user_groups):
            self._cache[repository].add(user_name)
            return True

        return False


class MockGafaelfawrGroupAuthorizer:
    """Mock implementation of ``GafaelfawrGroupAuthorizer`` for unit tests."""

    def __init__(self) -> None:
        self._response = True

    def set_response(self, value: bool) -> None:
        self._response = value

    async def is_user_authorized_for_repository(self, **kwargs: str) -> bool:
        return self._response

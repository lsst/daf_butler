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

import httpx
import pydantic

from .._authentication import get_authentication_headers


class GafaelfawrClient:
    """REST client for retrieving authentication information from
    Gafaelfawr.

    Parameters
    ----------
    base_url : `str`
        The top-level HTTP path where Gafaelfawr can be found (e.g.
        ``"https://data-int.lsst.cloud"``).
    """

    def __init__(self, base_url: str) -> None:
        self._client = httpx.AsyncClient(base_url=base_url)

    async def get_groups(self, user_token: str) -> list[str]:
        response = await self._client.get(
            "/auth/api/v1/user-info", headers=get_authentication_headers(user_token)
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

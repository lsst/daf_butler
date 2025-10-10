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

import os
from fnmatch import fnmatchcase
from urllib.parse import urlparse

from .interface import RemoteButlerAuthenticationProvider


class CadcAuthenticationProvider(RemoteButlerAuthenticationProvider):
    """
    Represents an authentication provider for remote Butler services specific
    to CADC connection requirements.

    This class handles the creation and management of authentication headers
    required for interaction with remote Butler services by handling bearer
    tokens. It ensures that the object is pickleable as it may need to be
    serialized and transferred between processes for file transfer operations.

    Parameters
    ----------
    access_token : `str`
        The bearer token used for authentication with CADC StorageInventory.
    """

    # NOTE -- This object needs to be pickleable. It will sometimes be
    # serialized and transferred to another process to execute file transfers.

    def __init__(self, access_token: str):
        # Access tokens are opaque bearer tokens. See https://sqr-069.lsst.io/
        self._headers = {"Authorization": f"Bearer {access_token}"}

    @staticmethod
    def create_from_environment(server_url: str) -> CadcAuthenticationProvider:
        access_token = _get_authentication_token_from_environment(server_url)
        if access_token is None:
            raise RuntimeError(
                "Attempting to connect to Butler server,"
                " but no access credentials were found in the environment."
            )
        return CadcAuthenticationProvider(access_token)

    def get_server_headers(self) -> dict[str, str]:
        return {}

    def get_datastore_headers(self) -> dict[str, str]:
        return self._headers


_SERVER_WHITELIST = ["*.cadc-ccda.hia-hia.nrc-cnrc.gc.ca", "*.canfar.net", "host.docker.internal"]
_CADC_TOKEN_ENVIRONMENT_KEY = "CADC_TOKEN"


def _get_authentication_token_from_environment(server_url: str) -> str | None:
    """
    Retrieve an authentication token from the environment.

    This function checks if the provided server URL's hostname matches any
    pattern in the server whitelist and if a valid token is available in
    the environment variable. If both conditions are satisfied, the token is
    returned; otherwise, None is returned.

    Parameters
    ----------
        server_url (str): The URL of the server for which an authentication
            token is being retrieved.

    Returns
    -------
        str | None: The authentication token if available and hostname matches
        the whitelist; otherwise, None.
    """
    hostname = urlparse(server_url.lower()).hostname
    hostname_in_whitelist = any(hostname and fnmatchcase(hostname, pattern) for pattern in _SERVER_WHITELIST)
    notebook_token = os.getenv(_CADC_TOKEN_ENVIRONMENT_KEY)
    if hostname_in_whitelist and notebook_token:
        return notebook_token

    return None

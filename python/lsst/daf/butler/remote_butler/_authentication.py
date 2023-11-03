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

__all__ = ()

import os
from fnmatch import fnmatchcase
from urllib.parse import urlparse

_SERVER_WHITELIST = ["*.lsst.cloud"]
_EXPLICIT_BUTLER_ACCESS_TOKEN_ENVIRONMENT_KEY = "BUTLER_RUBIN_ACCESS_TOKEN"
_RSP_JUPYTER_ACCESS_TOKEN_ENVIRONMENT_KEY = "ACCESS_TOKEN"


def get_authentication_token_from_environment(server_url: str) -> str | None:
    """Search the environment for a Rubin Science Platform access token.

    The token may come from the following sources in this order:

    1. The ``BUTLER_RUBIN_ACCESS_TOKEN`` environment variable.
       This environment variable is meant primarily for development use,
       running outside the Rubin Science Platform.  This token will be sent
       to EVERY server that we connect to, so be careful when connecting to
       untrusted servers.
    2. The ``ACCESS_TOKEN`` environment variable.
       This environment variable is provided by the Rubin Science Platform
       Jupyter notebooks.  It will only be returned if the given ``server_url``
       is in a whitelist of servers known to belong to the Rubin Science
       Platform.  Because this is a long-lived token that can be used to
       impersonate the user with their full access rights, it should not be
       sent to untrusted servers.

    Parameters
    ----------
    server_url : `str`
        URL of the Butler server that the caller intends to connect to.

    Returns
    -------
     access_token: `str` or `None`
        A Rubin Science Platform access token, or `None` if no token was
        configured in the environment.
    """
    explicit_butler_token = os.getenv(_EXPLICIT_BUTLER_ACCESS_TOKEN_ENVIRONMENT_KEY)
    if explicit_butler_token:
        return explicit_butler_token

    hostname = urlparse(server_url.lower()).hostname
    hostname_in_whitelist = any(
        (hostname and fnmatchcase(hostname, pattern) for pattern in _SERVER_WHITELIST)
    )
    notebook_token = os.getenv(_RSP_JUPYTER_ACCESS_TOKEN_ENVIRONMENT_KEY)
    if hostname_in_whitelist and notebook_token:
        return notebook_token

    return None


def get_authentication_headers(access_token: str) -> dict[str, str]:
    """Return HTTP headers required for authenticating the user via Rubin
    Science Platform's Gafaelfawr service.

    Parameters
    ----------
    access_token : `str`
        Rubin Science Platform access token.

    Returns
    -------
    header_map : `dict` [`str`, `str`]
        HTTP header names and values as a mapping from name to value.
    """
    # Access tokens are opaque bearer tokens. See https://sqr-069.lsst.io/
    return {"Authorization": f"Bearer {access_token}"}

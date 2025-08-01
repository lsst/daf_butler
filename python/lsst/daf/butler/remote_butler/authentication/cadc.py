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

import base64
import logging
import os
import re
import ssl
import time
from pathlib import Path

import httpx

from .interface import RemoteButlerAuthenticationProvider

OPENID_CONFIG_URL = os.getenv(
    "OPENID_CONFIG_URL", "https://ws-cadc.canfar.net/ac/.well-known/openid-configuration"
)
CADC_SSL_PROXY_FILENAME = os.getenv("SSL_PROXY_FILENAME", os.path.join(Path.home(), ".ssl", "cadcproxy.pem"))
CADC_TOKEN_ENVIRONMENT_KEY = "CADC_TOKEN"


def get_cadc_authorize_url() -> str:
    """Query the openid configuration to get the authorization URL."""
    try:
        response = httpx.get(OPENID_CONFIG_URL)
        response.raise_for_status()
        config = response.json()
        return config["authorization_endpoint"]
    except httpx.RequestError as e:
        raise RuntimeError(f"Failed to find authorization URL at {OPENID_CONFIG_URL}") from e


class CadcAuthenticationProvider(RemoteButlerAuthenticationProvider):
    """Provide HTTP headers required for authenticating the user at the
    Canadian Astronomy Data Centre.
    """

    # NOTE -- This object needs to be pickleable. It will sometimes be
    # serialized and transferred to another process to execute file transfers.

    def __init__(self) -> None:
        self._token = os.environ.get(CADC_TOKEN_ENVIRONMENT_KEY)

    @property
    def token(self) -> str:
        if self._token_is_valid:
            return self._token

        # Get a new token from authorize endpoint and ssl cert.
        if not os.path.exists(CADC_SSL_PROXY_FILENAME):
            logging.warning(f"Proxy certificate file not found: {CADC_SSL_PROXY_FILENAME}")
            return self._token

        try:
            ctx = ssl.create_default_context()
            ctx.load_cert_chain(certfile=CADC_SSL_PROXY_FILENAME)  # Optionally also keyfile or password.
            params = {"response_type": "token"}
            auth_url = get_cadc_authorize_url()
            response = httpx.Client(verify=ctx).get(auth_url, params=params)
            response.raise_for_status()
            self._token = response.text
            os.environ[CADC_TOKEN_ENVIRONMENT_KEY] = self._token
        except httpx.RequestError:
            logging.warning("Failed to refresh token")

        return self.token

    @property
    def _token_is_valid(self) -> bool:
        if self._token is None:
            return False
        try:
            # Decode the base64 string
            decoded_bytes = base64.b64decode(self._token)
            decoded_str = decoded_bytes.decode("utf-8")

            # Search for expirytime using a regular expression
            match = re.search(r"expirytime=(\d+)", decoded_str)
            if not match:
                return False

            exp_time = int(match.group(1))
            current_time = int(time.time())
            return exp_time > current_time
        except Exception as e:
            logging.debug(e)
            return False

    def get_server_headers(self) -> dict[str, str]:
        return {}

    def get_datastore_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}

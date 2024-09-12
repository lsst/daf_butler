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

__all__ = ("RemoteButlerFactory",)

import httpx
from lsst.daf.butler.repo_relocation import replaceRoot

from .._butler_config import ButlerConfig
from .._butler_instance_options import ButlerInstanceOptions
from ..registry import RegistryDefaults
from ._authentication import get_authentication_token_from_environment
from ._config import RemoteButlerConfigModel
from ._http_connection import RemoteButlerHttpConnection
from ._remote_butler import RemoteButler, RemoteButlerCache


class RemoteButlerFactory:
    """Factory for instantiating RemoteButler instances bound to a user's Rubin
    Science Platform Gafaelfawr access token.  All Butler instances created by
    this factory share a common HTTP connection pool.

    Parameters
    ----------
    server_url : `str`
        The URL of the Butler server that RemoteButler instances created by
        this factory will connect to.
    http_client : `httpx.Client`, optional
        The httpx connection pool that RemoteButler instances created by this
        factory will use for making HTTP requests.  If omitted, creates a new
        connection pool.

    Notes
    -----
    Most users should not directly call this constructor -- instead use
    ``create_factory_from_config`` or ``create_factory_for_url``.
    """

    def __init__(self, server_url: str, http_client: httpx.Client | None = None):
        self.server_url = server_url
        if http_client is not None:
            self.http_client = http_client
        else:
            self.http_client = httpx.Client(
                # This timeout is fairly conservative.  This value isn't the
                # maximum amount of time the request can take -- it's the
                # maximum amount of time to wait after receiving the last chunk
                # of data from the server.
                #
                # Long-running, streamed queries send a keep-alive every 15
                # seconds.  However, unstreamed operations like
                # queryCollections can potentially take a while if the database
                # is under duress.
                timeout=120  # seconds
            )
        self._cache = RemoteButlerCache()

    @staticmethod
    def create_factory_from_config(config: ButlerConfig) -> RemoteButlerFactory:
        # There is a convention in Butler config files where <butlerRoot> in a
        # configuration option refers to the directory containing the
        # configuration file. We allow this for the remote butler's URL so
        # that the server doesn't have to know which hostname it is being
        # accessed from.
        server_url_key = ("remote_butler", "url")
        if server_url_key in config:
            config[server_url_key] = replaceRoot(config[server_url_key], config.configDir)
        remote_config = RemoteButlerConfigModel.model_validate(config)
        return RemoteButlerFactory.create_factory_for_url(str(remote_config.remote_butler.url))

    @staticmethod
    def create_factory_for_url(server_url: str) -> RemoteButlerFactory:
        return RemoteButlerFactory(server_url)

    def create_butler_for_access_token(
        self,
        access_token: str,
        *,
        butler_options: ButlerInstanceOptions | None = None,
        use_disabled_datastore_cache: bool = True,
    ) -> RemoteButler:
        if butler_options is None:
            butler_options = ButlerInstanceOptions()
        return RemoteButler(
            connection=RemoteButlerHttpConnection(
                http_client=self.http_client, server_url=self.server_url, access_token=access_token
            ),
            defaults=RegistryDefaults.from_butler_instance_options(butler_options),
            cache=self._cache,
            use_disabled_datastore_cache=use_disabled_datastore_cache,
        )

    def create_butler_with_credentials_from_environment(
        self,
        *,
        butler_options: ButlerInstanceOptions | None = None,
        use_disabled_datastore_cache: bool = True,
    ) -> RemoteButler:
        token = get_authentication_token_from_environment(self.server_url)
        if token is None:
            raise RuntimeError(
                "Attempting to connect to Butler server,"
                " but no access credentials were found in the environment."
            )
        return self.create_butler_for_access_token(
            token, butler_options=butler_options, use_disabled_datastore_cache=use_disabled_datastore_cache
        )

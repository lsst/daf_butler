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

from collections.abc import Iterator
from contextlib import contextmanager
from functools import cache
from typing import Literal

from pydantic import AnyHttpUrl, BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

from .._config import AuthenticationMode


class RepositoryConfig(BaseModel):
    """Per-repository configuration for the Butler server."""

    config_uri: str
    """Path to DirectButler configuration YAML file for the repository."""
    authorized_groups: list[str]
    """List of Gafaelfawr groups that will be allowed to access this
    repository.  If this list contains the special group `*`, all users will be
    granted access.
    """


class ButlerServerConfig(BaseSettings):
    """Butler server configuration loaded from environment variables."""

    model_config = SettingsConfigDict(env_prefix="DAF_BUTLER_SERVER_")

    repositories: dict[str, RepositoryConfig]
    """Mapping from repository name to configuration for the repository."""

    gafaelfawr_url: AnyHttpUrl | Literal["DISABLED"]
    """URL to the top-level HTTP path where Gafaelfawr can be found (e.g.
    "https://data-int.lsst.cloud").

    This can instead be the special string "DISABLED" to turn off all features
    requiring Gafaelfawr integration.
    """

    authentication: AuthenticationMode

    static_files_path: str | None = None
    """Absolute path to a directory of files that will be served to end-users
    as static files from the `configs/` HTTP route.
    """

    @property
    def gafaelfawr_enabled(self) -> bool:
        return self.gafaelfawr_url != "DISABLED"


_config: ButlerServerConfig | None = None


@cache
def load_config() -> ButlerServerConfig:
    """Read the Butler server configuration from the environment."""
    global _config
    if _config is None:
        _config = ButlerServerConfig()
    return _config


@contextmanager
def mock_config(temporary_config: ButlerServerConfig | None = None) -> Iterator[ButlerServerConfig]:
    """Replace the global Butler server configuration with a temporary value.

    Parameters
    ----------
    temporary_config : `ButlerServerConfig`, optional
        Configuration to replace the global value with.  If not provided,
        a default empty configuration will be used.

    Returns
    -------
    config : `ButlerServerConfig`
        The new configuration object.
    """
    global _config
    orig = _config
    try:
        if temporary_config is None:
            temporary_config = ButlerServerConfig(
                repositories={},
                gafaelfawr_url="http://gafaelfawr.example",
                authentication="rubin_science_platform",
                static_files_path=None,
            )
        _config = temporary_config
        load_config.cache_clear()
        yield _config
    finally:
        _config = orig

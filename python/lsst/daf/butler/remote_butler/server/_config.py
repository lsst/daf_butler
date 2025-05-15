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

from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict


class ButlerServerConfig(BaseSettings):
    """Butler server configuration loaded from environment variables."""

    model_config = SettingsConfigDict(env_prefix="DAF_BUTLER_SERVER_")

    static_files_path: str | None = None
    """Absolute path to a directory of files that will be served to end-users
    as static files from the `configs/` HTTP route.
    """

    repositories: dict[str, RepositoryConfig]
    """Mapping from repository name to configuration for the repository."""


class RepositoryConfig(BaseModel):
    """Per-repository configuration for the Butler server."""

    config_uri: str
    """Path to DirectButler configuration YAML file for the repository."""


_config: ButlerServerConfig | None = None


def load_config() -> ButlerServerConfig:
    """Read the Butler server configuration from the environment."""
    global _config
    if _config is None:
        _config = ButlerServerConfig()
    return _config


@contextmanager
def mock_config() -> Iterator[None]:
    global _config
    orig = _config
    try:
        _config = ButlerServerConfig(repositories={})
        yield
    finally:
        _config = orig

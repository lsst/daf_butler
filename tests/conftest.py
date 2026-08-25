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

"""Shared pytest configuration for the daf_butler test suite."""

from __future__ import annotations

import os

import pytest

pytest_plugins = ["lsst.daf.butler.tests.fixtures"]

DEFAULT_TIMEOUT = 300
"""Seconds after which a single test is considered hung."""


def pytest_configure(config: pytest.Config) -> None:
    """Apply a default per-test timeout when pytest-timeout is installed.

    pytest-timeout is an optional development convenience rather than a test
    dependency: the postgres and server tests are the ones that hang, and a
    hung xdist worker otherwise consumes the whole job's budget.

    It is deliberately not configured through ``[tool.pytest.ini_options]``.
    A ``timeout`` key there raises ``PytestConfigWarning: Unknown config
    option`` on every run in an environment without the plugin, and becomes a
    hard error under ``--strict-config``. Environments that do not ship it,
    including the conda stack the Jenkins build validates against, simply run
    without timeouts.

    Parameters
    ----------
    config : `pytest.Config`
        Active pytest configuration.
    """
    if config.pluginmanager.hasplugin("timeout") and getattr(config.option, "timeout", None) is None:
        config.option.timeout = DEFAULT_TIMEOUT


@pytest.fixture(scope="session")
def test_directory() -> str:
    """Return the absolute path of this tests directory.

    The shipped fixture plugin cannot know where a consuming package keeps its
    Butler test configuration, so each package supplies this itself.
    """
    return os.path.abspath(os.path.dirname(__file__))

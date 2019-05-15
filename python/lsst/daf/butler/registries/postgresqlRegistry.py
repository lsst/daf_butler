# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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

__all__ = ("PostgreSqlRegistry", )

import re
import configparser
import urllib.parse as urlparse
import os

from sqlalchemy import event
from sqlalchemy import create_engine

from lsst.daf.butler.core.config import Config
from lsst.daf.butler.core.registry import RegistryConfig

from .oracleRegistry import OracleRegistry
from .sqlRegistry import SqlRegistryConfig

class PostgreSqlRegistry(OracleRegistry):
    """Registry backed by an PostgreSQL Amazon RDS service.

    Parameters
    ----------
    config : `SqlRegistryConfig` or `str`
        Load configuration
    """

    @classmethod
    def setConfigRoot(cls, root, config, full):
        """Set any filesystem-dependent config options for this Registry to
        be appropriate for a new empty repository with the given root.

        Parameters
        ----------
        root : `str`
            Filesystem path to the root of the data repository.
        config : `Config`
            A Butler-level config object to update (but not a
            `ButlerConfig`, to avoid included expanded defaults).
        full : `ButlerConfig`
            A complete Butler config with all defaults expanded;
            repository-specific options that should not be obtained
            from defaults when Butler instances are constructed
            should be copied from `full` to `Config`.
        """
        super().setConfigRoot(root, config, full)
        Config.overrideParameters(RegistryConfig, config, full,
                                  toCopy=("cls", "deferDatasetIdQueries"))

    def __init__(self, registryConfig, schemaConfig, dimensionConfig, create=False,
                 butlerRoot=None):
        registryConfig = SqlRegistryConfig(registryConfig)
        self.schemaConfig = schemaConfig
        super().__init__(registryConfig, schemaConfig, dimensionConfig, create,
                         butlerRoot=butlerRoot)

    def _createEngine(self):
        constr = self.config['db']

        # if both username and pass are in the db string - all is well
        # if username and pass are a nick that exists in a local conf file
        # read username and password from there
        parsed = urlparse.urlparse(constr)
        if parsed.username and (parsed.password is None):
            localconf = configparser.ConfigParser()
            localconf.read(os.path.expanduser('~/.rds/credentials'))

            username = localconf[parsed.username]['username']
            password = localconf[parsed.username]['password']
            constr = constr.replace(parsed.username, f'{username}:{password}')

        return create_engine(constr, pool_size=1)

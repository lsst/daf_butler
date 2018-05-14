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

"""
Configuration classes specific to the Butler
"""

import os

from .config import Config
from .datastore import DatastoreConfig
from .schema import SchemaConfig
from .registry import RegistryConfig
from .storageClass import StorageClassConfig
from .utils import doImport

__all__ = ("ButlerConfig",)


class ButlerConfig(Config):
    """Contains the configuration for a `Butler`

    The configuration is read and merged with default configurations for
    the particular classes. The defaults are read from
    ``$DAF_BUTLER_DIR/config`` and ``$DAF_BULTER_CONFIG_PATH``. The defaults
    are constructed by reading first the global defaults, and then adding
    in overrides from each entry in the colon-separated
    ``$DAF_BUTLER_CONFIG_PATH`` in reverse order such that the entries ahead
    in the list take priority. The registry and datastore configurations
    are read using the names specified by the appropriate classes defined
    in the supplied butler configuration.

    The externally supplied butler configuration must include definitions
    for ``registry.cls`` and ``datastore.cls`` to enable the correct defaults
    to be located.

    Parameters
    ----------
    other : `str`
        Path to butler configuration YAML file.
    """

    def __init__(self, other):

        # Create an empty config for us to populate
        super().__init__()

        # Read the supplied config so that we can work out which other
        # defaults to use.
        butlerConfig = Config(other)

        # All the configs that can be associated with default files
        configComponents = (SchemaConfig, StorageClassConfig, DatastoreConfig, RegistryConfig)

        # This is a list of all the config files to search
        defaultsFiles = [(c, c.defaultConfigFile) for c in configComponents]

        # Components that derive their configurations from implementation
        # classes rather than configuration classes
        specializedConfigs = (DatastoreConfig, RegistryConfig)

        # Look for class specific default files to be added to the file list
        # These class definitions must come from the suppliedbutler config and
        # are not defaulted.
        for componentConfig in specializedConfigs:
            k = "{}.cls".format(componentConfig.component)
            if k not in butlerConfig:
                raise ValueError("Required configuration {} not present in {}".format(k, other))
            cls = doImport(butlerConfig[k])
            defaultsFile = cls.defaultConfigFile
            if defaultsFile is not None:
                defaultsFiles.append((componentConfig, defaultsFile))

        # We can pick up defaults from multiple search paths
        # We fill defaults by using the butler config path and then
        # the config path environment variable in reverse order.
        self.defaultsPaths = []

        # Find the butler configs
        if "DAF_BUTLER_DIR" in os.environ:
            self.defaultsPaths.append(os.path.join(os.environ["DAF_BUTLER_DIR"], "config"))

        if "DAF_BUTLER_CONFIG_PATH" in os.environ:
            externalPaths = list(reversed(os.environ["DAF_BUTLER_CONFIG_PATH"].split(os.pathsep)))
            self.defaultsPaths.append(externalPaths)

        # Search each directory for each of the default files
        for pathDir in self.defaultsPaths:
            for configClass, configFile in defaultsFiles:
                # Assume external paths have same config files as global config
                # directory. Absolute paths are possible for external
                # code.
                # Should this be a log message? Are we using lsst.log?
                # print("Checking path {} for {} ({})".format(pathDir, configClass, configFile))
                if not os.path.isabs(configFile):
                    configFile = os.path.join(pathDir, configFile)
                if os.path.exists(configFile):
                    # this checks a specific part of the tree
                    # We may need to turn off validation since we do not
                    # require that each defaults file found is fully
                    # consistent.
                    config = configClass(configFile)
                    # Attach it using the global namespace
                    self.update({configClass.component: config})

        # Now that we have all the defaults we can merge the externally
        # provided config into the defaults.
        self.update(butlerConfig)

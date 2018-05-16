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

from .config import Config
from .datastore import DatastoreConfig
from .schema import SchemaConfig
from .registry import RegistryConfig
from .storageClass import StorageClassConfig

__all__ = ("ButlerConfig",)

CONFIG_COMPONENT_CLASSES = (SchemaConfig, RegistryConfig, StorageClassConfig, DatastoreConfig)


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
    other : `str`, `Config`, optional
        Path to butler configuration YAML file. If `None` the butler will
        be configured based entirely on defaults read from the environment.
        No defaults will be read if a `ButlerConfig` is supplied.
    """

    def __init__(self, other=None):

        # If this is already a ButlerConfig we assume that defaults
        # have already been loaded.
        if other is not None and isinstance(other, ButlerConfig):
            super().__init__(other)
            return

        # Create an empty config for us to populate
        super().__init__()

        # Read the supplied config so that we can work out which other
        # defaults to use.
        butlerConfig = Config(other)

        # A Butler config contains defaults defined by each of the component
        # configuration classes. We ask each of them to apply defaults to
        # the values we have been supplied by the user.
        for configClass in CONFIG_COMPONENT_CLASSES:
            config = configClass(butlerConfig)
            # Re-attach it using the global namespace
            self.update({configClass.component: config})
            # Remove the key from the butlerConfig since we have already
            # merged that information.
            if configClass.component in butlerConfig:
                del butlerConfig[configClass.component]

        # Now that we have all the defaults we can merge the externally
        # provided config into the defaults.
        # Not needed if there is never information in a butler config file
        # not present in component configurations
        self.update(butlerConfig)

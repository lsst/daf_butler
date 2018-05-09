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
from .utils import doImport

__all__ = ("ButlerConfig",)


class ButlerConfig(Config):
    """Contains the configuration for a `Butler`

    The configuration is read and merged with default configurations for
    the particular classes. The defaults are read from
    ``$DAF_BUTLER_DIR/config`` using names specified by the appropriate classes
    for registry and datastore, and including the standard schema and storage
    class definitions.

    Parameters
    ----------
    other : `str`
        Path to butler configuration YAML file.
    """

    def __init__(self, other):

        # Create an empty config for us to populate
        super().__init__()

        # Find the butler configs
        self.defaultsDir = None
        if "DAF_BUTLER_DIR" in os.environ:
            self.defaultsDir = os.path.join(os.environ["DAF_BUTLER_DIR"], "config")

        # Storage classes
        storageClasses = Config(os.path.join(self.defaultsDir, "storageClasses.yaml"))
        self.update(storageClasses)

        # Default schema
        schema = Config(os.path.join(self.defaultsDir, "registry", "default_schema.yaml"))
        self.update(schema)

        # Read the supplied config so that we can work out which other
        # defaults to use.
        butlerConfig = Config(other)

        # Check that fundamental keys are present
        self._validate(butlerConfig)

        # Look for class specific defaults
        for section in ("datastore", "registry"):
            k = f"{section}.cls"
            print("Checking {}: {}".format(k, butlerConfig[k]))
            cls = doImport(butlerConfig[k])
            defaultsPath = cls.defaults
            if defaultsPath is not None:
                if not os.path.isabs(defaultsPath):
                    defaultsPath = os.path.join(self.defaultsDir, defaultsPath)
                c = Config(defaultsPath)
                # Merge into baseline
                self.update(c)

        # Now that we have all the defaults we can merge the externally
        # provided config into the defaults.
        self.update(butlerConfig)

    def _validate(self, config=None):
        """Check a butler config contains mandatory keys.

        Parameters
        ----------
        config : `Config`, optional
            By default checks itself, but if ``config`` is given, this
            config will be checked instead.
        """
        if config is None:
            config = self
        for k in ['datastore.cls', 'registry.cls']:
            if k not in config:
                raise ValueError(f"Missing ButlerConfig parameter: {k}")

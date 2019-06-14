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

"""Support for reading and writing composite objects."""

__all__ = ("CompositesConfig", "CompositesMap")

import logging

from .configSupport import processLookupConfigs
from .config import ConfigSubset

log = logging.getLogger(__name__)

# Key to access disassembly information
DISASSEMBLY_KEY = "disassembled"


class CompositesConfig(ConfigSubset):
    component = "composites"
    requiredKeys = ("default", DISASSEMBLY_KEY)
    defaultConfigFile = "composites.yaml"

    def validate(self):
        """Validate entries have the correct type."""
        super().validate()
        # For now assume flat config with keys mapping to booleans
        for k, v in self[DISASSEMBLY_KEY].items():
            if not isinstance(v, bool):
                raise ValueError(f"CompositesConfig: Key {k} is not a Boolean")


class CompositesMap:
    """Determine whether a specific datasetType or StorageClass should be
    disassembled.

    Parameters
    ----------
    config : `str`, `ButlerConfig`, or `CompositesConfig`
        Configuration to control composites disassembly.
    universe : `DimensionUniverse`
        Set of all known dimensions, used to expand and validate any used
        in lookup keys.
    """

    def __init__(self, config, *, universe):
        if not isinstance(config, type(self)):
            config = CompositesConfig(config)
        assert isinstance(config, CompositesConfig)
        self.config = config

        # Calculate the disassembly lookup table -- no need to process
        # the values
        self._lut = processLookupConfigs(self.config[DISASSEMBLY_KEY], universe=universe)

    def shouldBeDisassembled(self, entity):
        """Given some choices, indicate whether the entity should be
        disassembled.

        Parameters
        ----------
        entity : `StorageClass` or `DatasetType` or `DatasetRef`
            Thing to test against the configuration. The ``name`` property
            is used to determine a match.  A `DatasetType` will first check
            its name, before checking its `StorageClass`.  If there are no
            matches the default will be returned. If the associated
            `StorageClass` is not a composite, will always return `False`.

        Returns
        -------
        disassemble : `bool`
            Returns `True` if disassembly should occur; `False` otherwise.

        Raises
        ------
        ValueError
            The supplied argument is not understood.
        """

        if not hasattr(entity, "isComposite"):
            raise ValueError(f"Supplied entity ({entity}) is not understood.")

        # If this is not a composite there is nothing to disassemble.
        if not entity.isComposite():
            log.debug("%s will not be disassembled (not a composite)", entity)
            return False

        matchName = "{} (via default)".format(entity)
        disassemble = self.config["default"]

        for key in (entity._lookupNames()):
            if key is not None and key in self._lut:
                disassemble = self._lut[key]
                matchName = key
                break

        log.debug("%s will%s be disassembled", matchName, "" if disassemble else " not")
        return disassemble

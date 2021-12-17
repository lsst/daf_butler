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

from __future__ import annotations

"""Support for reading and writing composite objects."""

__all__ = ("CompositesConfig", "CompositesMap")

import logging
from typing import TYPE_CHECKING, Union

import yaml

from .config import ConfigSubset
from .configSupport import processLookupConfigs

if TYPE_CHECKING:
    from .._butlerConfig import ButlerConfig
    from .configSupport import LookupKey
    from .datasets import DatasetRef, DatasetType
    from .dimensions import DimensionUniverse
    from .storageClass import StorageClass

log = logging.getLogger(__name__)

# Key to access disassembly information
DISASSEMBLY_KEY = "disassembled"


class CompositesConfig(ConfigSubset):
    """Configuration specifics for Composites."""

    component = "composites"
    requiredKeys = ("default", DISASSEMBLY_KEY)
    defaultConfigFile = "datastores/composites.yaml"

    def validate(self) -> None:
        """Validate entries have the correct type."""
        super().validate()
        # For now assume flat config with keys mapping to booleans
        for k, v in self[DISASSEMBLY_KEY].items():
            if not isinstance(v, bool):
                raise ValueError(f"CompositesConfig: Key {k} is not a Boolean")


class CompositesMap:
    """Determine whether something should be disassembled.

    Compares a `DatasetType` or `StorageClass` with the map and determines
    whether disassembly is requested.

    Parameters
    ----------
    config : `str`, `ButlerConfig`, or `CompositesConfig`
        Configuration to control composites disassembly.
    universe : `DimensionUniverse`
        Set of all known dimensions, used to expand and validate any used
        in lookup keys.
    """

    def __init__(self, config: Union[str, ButlerConfig, CompositesConfig], *, universe: DimensionUniverse):
        if not isinstance(config, CompositesConfig):
            config = CompositesConfig(config)
        assert isinstance(config, CompositesConfig)
        self.config = config

        # Pre-filter the disassembly lookup table to remove the
        # placeholder __ key we added for documentation.
        # It should be harmless but might confuse validation
        # Retain the entry as a Config so change in place
        disassemblyMap = self.config[DISASSEMBLY_KEY]
        for k in set(disassemblyMap):
            if k.startswith("__"):
                del disassemblyMap[k]

        # Calculate the disassembly lookup table -- no need to process
        # the values
        self._lut = processLookupConfigs(disassemblyMap, universe=universe)

    def shouldBeDisassembled(self, entity: Union[DatasetRef, DatasetType, StorageClass]) -> bool:
        """Indicate whether the entity should be disassembled.

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

        matchName: Union[LookupKey, str] = "{} (via default)".format(entity)
        disassemble = self.config["default"]

        for key in entity._lookupNames():
            if key in self._lut:
                disassemble = self._lut[key]
                matchName = key
                break

        if not isinstance(disassemble, bool):
            raise TypeError(
                f"Got disassemble value {disassemble!r} for config entry {matchName!r}; expected bool."
            )

        log.debug("%s will%s be disassembled", matchName, "" if disassemble else " not")
        return disassemble

    def __str__(self) -> str:
        result = {}
        result["default"] = self.config["default"]
        result["disassembled"] = {}
        for key in self._lut:
            result["disassembled"][str(key)] = self._lut[key]
        return yaml.dump(result)

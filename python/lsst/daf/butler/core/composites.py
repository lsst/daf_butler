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

from .config import ConfigSubset
from .datasets import DatasetType, DatasetRef
from .storageClass import StorageClass

log = logging.getLogger(__name__)


class CompositesConfig(ConfigSubset):
    component = "composites"
    requiredKeys = ("default", "names")
    defaultConfigFile = "composites.yaml"

    def validate(self):
        """Validate entries have the correct type."""
        super().validate()
        for k in self["names"]:
            key = f"names.{k}"
            if not isinstance(self[key], bool):
                raise ValueError(f"CompositesConfig: Key {key} is not a Boolean")


class CompositesMap:
    """Determine whether a specific datasetType or StorageClass should be
    disassembled.

    Parameters
    ----------
    config : `str`, `ButlerConfig`, or `CompositesConfig`
        Configuration to control composites disassembly.
    """

    def __init__(self, config):
        if not isinstance(config, type(self)):
            config = CompositesConfig(config)
        assert isinstance(config, CompositesConfig)
        self.config = config

    def doDisassembly(self, entity):
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
        """
        components = None
        datasetTypeName = None
        storageClassName = None
        if isinstance(entity, DatasetRef):
            entity = entity.datasetType
        if isinstance(entity, DatasetType):
            datasetTypeName = entity.name
            storageClassName = entity.storageClass.name
            components = entity.storageClass.components
        elif isinstance(entity, StorageClass):
            storageClassName = entity.name
            components = entity.components
        else:
            raise ValueError(f"Unexpected argument: {entity:!r}")

        # We know for a fact this is not a composite
        if not components:
            log.debug("%s will not be disassembled (not a composite)", entity)
            return False

        matchName = "{} (via default)".format(entity)
        disassemble = self.config["default"]

        for name in (datasetTypeName, storageClassName):
            if name is not None and name in self.config["names"]:
                disassemble = self.config[f"names.{name}"]
                matchName = name
                break

        log.debug("%s will%s be disassembled", matchName, "" if disassemble else " not")
        return disassemble

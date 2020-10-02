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

__all__ = ("DimensionConfig",)

from typing import Any, Dict, Tuple

from ..config import Config, ConfigSubset
from ..utils import doImport
from .. import ddl
from .elements import DimensionElement, Dimension, SkyPixDimension, RelatedDimensions


class DimensionConfig(ConfigSubset):
    """Configuration that defines a `DimensionUniverse`.

    The configuration tree for dimensions is a (nested) dictionary
    with four top-level entries:

    - version: an integer version number, used as keys in a singleton registry
      of all `DimensionUniverse` instances;

    - skypix: a dictionary whose entries each define a `SkyPixDimension`,
      along with a special "common" key whose value is the name of a skypix
      dimension that is used to relate all other spatial dimensions in the
      `Registry` database;

    - elements: a nested dictionary whose entries each define a non-skypix
      `DimensionElement`;

    - packers: a nested dictionary whose entries define a factory for a
      `DimensionPacker` instance.
    """
    component = "dimensions"
    requiredKeys = ("version", "elements", "skypix")
    defaultConfigFile = "dimensions.yaml"


def processSkyPixConfig(config: Config) -> Tuple[Dict[str, SkyPixDimension], SkyPixDimension]:
    """Process the "skypix" section of a `DimensionConfig`.

    Parameters
    ----------
    config : `Config`
        The subset of a `DimensionConfig` that corresponds to the "skypix" key.

    Returns
    -------
    dimensions: `dict`
        A dictionary mapping `str` names to partially-constructed
        `SkyPixDimension` instances; the caller (i.e. a `DimensionUniverse`)
        is responsible for calling `DimensionElement._finish` to complete
        construction.
    common: `SkyPixDimension`
        The special dimension used to relate all other spatial dimensions in
        the universe.  This instance is also guaranteed to be a value in
        the returned ``dimensions``.
    """
    skyPixSysNames = set(config.keys())
    try:
        skyPixSysNames.remove("common")
    except KeyError as err:
        raise ValueError("No common skypix dimension defined in configuration.") from err
    dimensions = {}
    for sysName in sorted(skyPixSysNames):
        subconfig = config[sysName]
        pixelizationClass = doImport(subconfig["class"])
        max_level = subconfig.get("max_level", 24)
        for level in range(max_level + 1):
            name = f"{sysName}{level}"
            dimensions[name] = SkyPixDimension(name, pixelizationClass(level))
    try:
        common = dimensions[config["common"]]
    except KeyError as err:
        raise ValueError(f"Undefined name for common skypix dimension ({config['common']}).") from err
    return dimensions, common


def processElementsConfig(config: Config) -> Dict[str, DimensionElement]:
    """Process the "elements" section of a `DimensionConfig`.

    Parameters
    ----------
    config : `Config`
        The subset of a `DimensionConfig` that corresponds to the "elements"
        key.

    Returns
    -------
    dimensions : `dict`
        A dictionary mapping `str` names to partially-constructed
        `DimensionElement` instances; the caller (i.e. a `DimensionUniverse`)
        is responsible for calling `DimensionElement._finish` to complete
        construction.
    """
    elements: Dict[str, DimensionElement] = dict()
    for name, subconfig in config.items():
        kwargs: Dict[str, Any] = {}
        kwargs["related"] = RelatedDimensions(
            required=set(subconfig.get("requires", ())),
            implied=set(subconfig.get("implies", ())),
            spatial=subconfig.get("spatial"),
            temporal=subconfig.get("temporal"),
        )
        kwargs["metadata"] = [ddl.FieldSpec.fromConfig(c) for c in subconfig.get("metadata", ())]
        kwargs["cached"] = subconfig.get("cached", False)
        kwargs["viewOf"] = subconfig.get("view_of", None)
        kwargs["alwaysJoin"] = subconfig.get("always_join", False)
        keys = subconfig.get("keys")
        if keys is not None:
            uniqueKeys = [ddl.FieldSpec.fromConfig(c, nullable=False) for c in keys]
            uniqueKeys[0].primaryKey = True
            elements[name] = Dimension(name, uniqueKeys=uniqueKeys, **kwargs)
        else:
            elements[name] = DimensionElement(name, **kwargs)
    return elements

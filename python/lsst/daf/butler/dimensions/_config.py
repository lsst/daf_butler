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

__all__ = ("DimensionConfig",)

import warnings
from collections.abc import Iterator, Mapping, Sequence
from typing import Any

from lsst.resources import ResourcePath, ResourcePathExpression

from .. import ddl
from .._config import Config, ConfigSubset
from .._topology import TopologicalSpace
from ._database import (
    DatabaseDimensionElementConstructionVisitor,
    DatabaseTopologicalFamilyConstructionVisitor,
)
from ._governor import GovernorDimensionConstructionVisitor
from ._packer import DimensionPackerConstructionVisitor
from ._skypix import SkyPixConstructionVisitor
from .construction import DimensionConstructionBuilder, DimensionConstructionVisitor

# The default namespace to use on older dimension config files that only
# have a version.
_DEFAULT_NAMESPACE = "daf_butler"


class DimensionConfig(ConfigSubset):
    """Configuration that defines a `DimensionUniverse`.

    The configuration tree for dimensions is a (nested) dictionary
    with five top-level entries:

    - version: an integer version number, used as keys in a singleton registry
      of all `DimensionUniverse` instances;

    - namespace: a string to be associated with the version in the singleton
      registry of all `DimensionUnivers` instances;

    - skypix: a dictionary whose entries each define a `SkyPixSystem`,
      along with a special "common" key whose value is the name of a skypix
      dimension that is used to relate all other spatial dimensions in the
      `Registry` database;

    - elements: a nested dictionary whose entries each define
      `StandardDimension` or `StandardDimensionCombination`.

    - topology: a nested dictionary with ``spatial`` and ``temporal`` keys,
      with dictionary values that each define a `StandardTopologicalFamily`.

    - packers: a nested dictionary whose entries define factories for a
      `DimensionPacker` instance.

    See the documentation for the linked classes above for more information
    on the configuration syntax.

    Parameters
    ----------
    other : `Config` or `str` or `dict`, optional
        Argument specifying the configuration information as understood
        by `Config`. If `None` is passed then defaults are loaded from
        "dimensions.yaml", otherwise defaults are not loaded.
    validate : `bool`, optional
        If `True` required keys will be checked to ensure configuration
        consistency.
    searchPaths : `list` or `tuple`, optional
        Explicit additional paths to search for defaults. They should
        be supplied in priority order. These paths have higher priority
        than those read from the environment in
        `ConfigSubset.defaultSearchPaths()`.  Paths can be `str` referring to
        the local file system or URIs, `lsst.resources.ResourcePath`.
    """

    requiredKeys = ("version", "elements", "skypix")
    defaultConfigFile = "dimensions.yaml"

    def __init__(
        self,
        other: Config | ResourcePathExpression | Mapping[str, Any] | None = None,
        validate: bool = True,
        searchPaths: Sequence[ResourcePathExpression] | None = None,
    ):
        # if argument is not None then do not load/merge defaults
        mergeDefaults = other is None
        super().__init__(other=other, validate=validate, mergeDefaults=mergeDefaults, searchPaths=searchPaths)

    def _updateWithConfigsFromPath(
        self, searchPaths: Sequence[str | ResourcePath], configFile: ResourcePath | str
    ) -> None:
        """Search the supplied paths reading config from first found.

        Raises
        ------
        FileNotFoundError
            Raised if config file is not found in any of given locations.

        Notes
        -----
        This method overrides base class method with different behavior.
        Instead of merging all found files into a single configuration it
        finds first matching file and reads it.
        """
        uri = ResourcePath(configFile)
        if uri.isabs() and uri.exists():
            # Assume this resource exists
            self._updateWithOtherConfigFile(configFile)
            self.filesRead.append(configFile)
        else:
            for pathDir in searchPaths:
                if isinstance(pathDir, str | ResourcePath):
                    pathDir = ResourcePath(pathDir, forceDirectory=True)
                    file = pathDir.join(configFile)
                    if file.exists():
                        self.filesRead.append(file)
                        self._updateWithOtherConfigFile(file)
                        break
                else:
                    raise TypeError(f"Unexpected search path type encountered: {pathDir!r}")
            else:
                raise FileNotFoundError(f"Could not find {configFile} in search path {searchPaths}")

    def _updateWithOtherConfigFile(self, file: Config | str | ResourcePath | Mapping[str, Any]) -> None:
        """Override for base class method.

        Parameters
        ----------
        file : `Config`, `str`, `lsst.resources.ResourcePath`, or `dict`
            Entity that can be converted to a `ConfigSubset`.
        """
        # Use this class to read the defaults so that subsetting can happen
        # correctly.
        externalConfig = type(self)(file, validate=False)
        self.update(externalConfig)

    def _extractSkyPixVisitors(self) -> Iterator[DimensionConstructionVisitor]:
        """Process the 'skypix' section of the configuration.

        Yields a construction visitor for each `SkyPixSystem`.

        Yields
        ------
        visitor : `DimensionConstructionVisitor`
            Object that adds a skypix system and its dimensions to an
            under-construction `DimensionUniverse`.
        """
        config = self["skypix"]
        systemNames = set(config.keys())
        systemNames.remove("common")
        for systemName in sorted(systemNames):
            subconfig = config[systemName]
            pixelizationClassName = subconfig["class"]
            maxLevel = subconfig.get("max_level", 24)
            yield SkyPixConstructionVisitor(systemName, pixelizationClassName, maxLevel)

    def _extractElementVisitors(self) -> Iterator[DimensionConstructionVisitor]:
        """Process the 'elements' section of the configuration.

        Yields a construction visitor for each `StandardDimension` or
        `StandardDimensionCombination`.

        Yields
        ------
        visitor : `DimensionConstructionVisitor`
            Object that adds a `StandardDimension` or
            `StandardDimensionCombination` to an under-construction
            `DimensionUniverse`.
        """
        for name, subconfig in self["elements"].items():
            metadata = [ddl.FieldSpec.fromConfig(c) for c in subconfig.get("metadata", ())]
            uniqueKeys = [ddl.FieldSpec.fromConfig(c, nullable=False) for c in subconfig.get("keys", ())]
            if uniqueKeys:
                uniqueKeys[0].primaryKey = True
            if subconfig.get("governor", False):
                unsupported = {"required", "implied", "viewOf", "alwaysJoin"}
                if not unsupported.isdisjoint(subconfig.keys()):
                    raise RuntimeError(
                        f"Unsupported config key(s) for governor {name}: {unsupported & subconfig.keys()}."
                    )
                if not subconfig.get("cached", True):
                    raise RuntimeError(f"Governor dimension {name} is always cached.")
                yield GovernorDimensionConstructionVisitor(
                    name=name,
                    storage=subconfig["storage"],
                    metadata=metadata,
                    uniqueKeys=uniqueKeys,
                )
            else:
                yield DatabaseDimensionElementConstructionVisitor(
                    name=name,
                    storage=subconfig["storage"],
                    required=set(subconfig.get("requires", ())),
                    implied=set(subconfig.get("implies", ())),
                    metadata=metadata,
                    alwaysJoin=subconfig.get("always_join", False),
                    uniqueKeys=uniqueKeys,
                    populated_by=subconfig.get("populated_by", None),
                )

    def _extractTopologyVisitors(self) -> Iterator[DimensionConstructionVisitor]:
        """Process the 'topology' section of the configuration.

        Yields a construction visitor for each `StandardTopologicalFamily`.

        Yields
        ------
        visitor : `DimensionConstructionVisitor`
            Object that adds a `StandardTopologicalFamily` to an
            under-construction `DimensionUniverse` and updates its member
            `DimensionElement` instances.
        """
        for spaceName, subconfig in self.get("topology", {}).items():
            space = TopologicalSpace.__members__[spaceName.upper()]
            for name, members in subconfig.items():
                yield DatabaseTopologicalFamilyConstructionVisitor(
                    name=name,
                    space=space,
                    members=members,
                )

    # TODO: remove this method and callers on DM-38687.
    # Note that the corresponding entries in the dimensions config should
    # not be removed at that time, because that's formally a schema migration.
    def _extractPackerVisitors(self) -> Iterator[DimensionConstructionVisitor]:
        """Process the 'packers' section of the configuration.

        Yields construction visitors for each `DimensionPackerFactory`.

        Yields
        ------
        visitor : `DimensionConstructionVisitor`
            Object that adds a `DinmensionPackerFactory` to an
            under-construction `DimensionUniverse`.
        """
        with warnings.catch_warnings():
            # Don't warn when deprecated code calls other deprecated code.
            warnings.simplefilter("ignore", FutureWarning)
            for name, subconfig in self["packers"].items():
                yield DimensionPackerConstructionVisitor(
                    name=name,
                    clsName=subconfig["cls"],
                    fixed=subconfig["fixed"],
                    dimensions=subconfig["dimensions"],
                )

    def makeBuilder(self) -> DimensionConstructionBuilder:
        """Construct a `DinmensionConstructionBuilder`.

        The builder will reflect this configuration.

        Returns
        -------
        builder : `DimensionConstructionBuilder`
            A builder object populated with all visitors from this
            configuration.  The `~DimensionConstructionBuilder.finish` method
            will not have been called.
        """
        builder = DimensionConstructionBuilder(
            self["version"],
            self["skypix", "common"],
            self,
            namespace=self.get("namespace", _DEFAULT_NAMESPACE),
        )
        builder.update(self._extractSkyPixVisitors())
        builder.update(self._extractElementVisitors())
        builder.update(self._extractTopologyVisitors())
        builder.update(self._extractPackerVisitors())
        return builder

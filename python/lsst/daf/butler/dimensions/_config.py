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

__all__ = ("DimensionConfig", "SerializedDimensionConfig")

import textwrap
from collections.abc import Mapping, Sequence, Set
from typing import Any, ClassVar, Literal, Union, final

import pydantic

from lsst.resources import ResourcePath, ResourcePathExpression
from lsst.sphgeom import PixelizationABC
from lsst.utils.doImport import doImportType

from .._config import Config, ConfigSubset
from .._named import NamedValueSet
from .._topology import TopologicalSpace
from ._database import DatabaseTopologicalFamilyConstructionVisitor
from ._elements import Dimension, KeyColumnSpec, MetadataColumnSpec
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

    - packers: ignored.

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

    def to_simple(self) -> SerializedDimensionConfig:
        """Convert this configuration to a serializable Pydantic model.

        Returns
        -------
        model : `SerializedDimensionConfig`
            Serializable Pydantic version of this configuration.
        """
        return SerializedDimensionConfig.model_validate(self.toDict())

    @staticmethod
    def from_simple(simple: SerializedDimensionConfig) -> DimensionConfig:
        """Load the configuration from a serialized version.

        Parameters
        ----------
        simple : `SerializedDimensionConfig`
            Serialized configuration to be loaded.

        Returns
        -------
        config : `DimensionConfig`
            Dimension configuration.
        """
        return DimensionConfig(
            simple.model_dump(
                # Some of the fields in Pydantic model config have aliases
                # (e.g. remapping 'class_' to 'class').  Pydantic ignores these
                # in model_dump() by default, so we have to add by_alias to
                # make sure that we end up with the right names in the dict.
                by_alias=True
            )
        )

    def makeBuilder(self) -> DimensionConstructionBuilder:
        """Construct a `DimensionConstructionBuilder`.

        The builder will reflect this configuration.

        Returns
        -------
        builder : `DimensionConstructionBuilder`
            A builder object populated with all visitors from this
            configuration.  The `~DimensionConstructionBuilder.finish` method
            will not have been called.
        """
        validated = self.to_simple()
        builder = DimensionConstructionBuilder(
            validated.version,
            validated.skypix.common,
            self,
            namespace=validated.namespace,
        )
        for system_name, system_config in sorted(validated.skypix.systems.items()):
            builder.add(system_name, system_config)
        for element_name, element_config in validated.elements.items():
            builder.add(element_name, element_config)
        for family_name, members in validated.topology.spatial.items():
            builder.add(
                family_name,
                DatabaseTopologicalFamilyConstructionVisitor(space=TopologicalSpace.SPATIAL, members=members),
            )
        for family_name, members in validated.topology.temporal.items():
            builder.add(
                family_name,
                DatabaseTopologicalFamilyConstructionVisitor(
                    space=TopologicalSpace.TEMPORAL, members=members
                ),
            )
        return builder


@final
class _SkyPixSystemConfig(pydantic.BaseModel, DimensionConstructionVisitor):
    """Description of a hierarchical sky pixelization system in dimension
    universe configuration.
    """

    class_: str = pydantic.Field(
        alias="class",
        description="Fully-qualified name of an `lsst.sphgeom.PixelizationABC implementation.",
    )

    min_level: int = 1
    """Minimum level for this pixelization."""

    max_level: int | None
    """Maximum level for this pixelization."""

    def has_dependencies_in(self, others: Set[str]) -> bool:
        # Docstring inherited from DimensionConstructionVisitor.
        return False

    def visit(self, name: str, builder: DimensionConstructionBuilder) -> None:
        # Docstring inherited from DimensionConstructionVisitor.
        PixelizationClass = doImportType(self.class_)
        assert issubclass(PixelizationClass, PixelizationABC)
        if self.max_level is None:
            max_level: int | None = getattr(PixelizationClass, "MAX_LEVEL", None)
            if max_level is None:
                raise TypeError(
                    f"Skypix pixelization class {self.class_} does"
                    " not have MAX_LEVEL but no max level has been set explicitly."
                )
            self.max_level = max_level

        from ._skypix import SkyPixSystem

        system = SkyPixSystem(
            name,
            maxLevel=self.max_level,
            PixelizationClass=PixelizationClass,
        )
        builder.topology[TopologicalSpace.SPATIAL].add(system)
        for level in range(self.min_level, self.max_level + 1):
            dimension = system[level]
            builder.dimensions.add(dimension)
            builder.elements.add(dimension)


@final
class _SkyPixSectionConfig(pydantic.BaseModel):
    """Section of the dimension universe configuration that describes sky
    pixelizations.
    """

    common: str = pydantic.Field(
        description="Name of the dimension used to relate all other spatial dimensions."
    )

    systems: dict[str, _SkyPixSystemConfig] = pydantic.Field(
        default_factory=dict, description="Descriptions of the supported sky pixelization systems."
    )

    model_config = pydantic.ConfigDict(extra="allow")

    @pydantic.model_validator(mode="after")
    def _move_extra_to_systems(self) -> _SkyPixSectionConfig:
        """Reinterpret extra fields in this model as members of the `systems`
        dictionary.
        """
        if self.__pydantic_extra__ is None:
            self.__pydantic_extra__ = {}
        for name, data in self.__pydantic_extra__.items():
            self.systems[name] = _SkyPixSystemConfig.model_validate(data)
        self.__pydantic_extra__.clear()
        return self


@final
class _TopologySectionConfig(pydantic.BaseModel):
    """Section of the dimension universe configuration that describes spatial
    and temporal relationships.
    """

    spatial: dict[str, list[str]] = pydantic.Field(
        default_factory=dict,
        description=textwrap.dedent(
            """\
            Dictionary of spatial dimension elements, grouped by the "family"
            they belong to.

            Elements in a family are ordered from fine-grained to coarse-grained.
            """
        ),
    )

    temporal: dict[str, list[str]] = pydantic.Field(
        default_factory=dict,
        description=textwrap.dedent(
            """\
            Dictionary of temporal dimension elements, grouped by the "family"
            they belong to.

            Elements in a family are ordered from fine-grained to coarse-grained.
            """
        ),
    )


@final
class _LegacyGovernorDimensionStorage(pydantic.BaseModel):
    """Legacy storage configuration for governor dimensions."""

    cls: Literal["lsst.daf.butler.registry.dimensions.governor.BasicGovernorDimensionRecordStorage"] = (
        "lsst.daf.butler.registry.dimensions.governor.BasicGovernorDimensionRecordStorage"
    )

    has_own_table: ClassVar[Literal[True]] = True
    """Whether this dimension needs a database table to be defined."""

    is_cached: ClassVar[Literal[True]] = True
    """Whether this dimension's records should be cached in clients."""

    implied_union_target: ClassVar[Literal[None]] = None
    """Name of another dimension that implies this one, whose values for this
    dimension define the set of allowed values for this dimension.
    """


@final
class _LegacyTableDimensionStorage(pydantic.BaseModel):
    """Legacy storage configuration for regular dimension tables stored in the
    database.
    """

    cls: Literal["lsst.daf.butler.registry.dimensions.table.TableDimensionRecordStorage"] = (
        "lsst.daf.butler.registry.dimensions.table.TableDimensionRecordStorage"
    )

    has_own_table: ClassVar[Literal[True]] = True
    """Whether this dimension element needs a database table to be defined."""

    is_cached: ClassVar[Literal[False]] = False
    """Whether this dimension element's records should be cached in clients."""

    implied_union_target: ClassVar[Literal[None]] = None
    """Name of another dimension that implies this one, whose values for this
    dimension define the set of allowed values for this dimension.
    """


@final
class _LegacyImpliedUnionDimensionStorage(pydantic.BaseModel):
    """Legacy storage configuration for dimensions whose allowable values are
    computed from the union of the values in another dimension that implies
    this one.
    """

    cls: Literal["lsst.daf.butler.registry.dimensions.query.QueryDimensionRecordStorage"] = (
        "lsst.daf.butler.registry.dimensions.query.QueryDimensionRecordStorage"
    )

    view_of: str
    """The dimension that implies this one and defines its values."""

    has_own_table: ClassVar[Literal[False]] = False
    """Whether this dimension needs a database table to be defined."""

    is_cached: ClassVar[Literal[False]] = False
    """Whether this dimension element's records should be cached in clients."""

    @property
    def implied_union_target(self) -> str:
        """Name of another dimension that implies this one, whose values for
        this dimension define the set of allowed values for this dimension.
        """
        return self.view_of


@final
class _LegacyCachingDimensionStorage(pydantic.BaseModel):
    """Legacy storage configuration that wraps another to indicate that its
    records should be cached.
    """

    cls: Literal["lsst.daf.butler.registry.dimensions.caching.CachingDimensionRecordStorage"] = (
        "lsst.daf.butler.registry.dimensions.caching.CachingDimensionRecordStorage"
    )

    nested: _LegacyTableDimensionStorage | _LegacyImpliedUnionDimensionStorage
    """Dimension storage configuration wrapped by this one."""

    @property
    def has_own_table(self) -> bool:
        """Whether this dimension needs a database table to be defined."""
        return self.nested.has_own_table

    is_cached: ClassVar[Literal[True]] = True
    """Whether this dimension element's records should be cached in clients."""

    @property
    def implied_union_target(self) -> str | None:
        """Name of another dimension that implies this one, whose values for
        this dimension define the set of allowed values for this dimension.
        """
        return self.nested.implied_union_target


@final
class _ElementConfig(pydantic.BaseModel, DimensionConstructionVisitor):
    """Description of a single dimension or dimension join relation in
    dimension universe configuration.
    """

    doc: str = pydantic.Field(default="", description="Documentation for the dimension or relationship.")

    keys: list[KeyColumnSpec] = pydantic.Field(
        default_factory=list,
        description=textwrap.dedent(
            """\
            Key columns that (along with required dependency values) uniquely
            identify the dimension's records.

            The first columns in this list is the primary key and is used in
            data coordinate values.  Other columns are alternative keys and
            are defined with SQL ``UNIQUE`` constraints.
            """
        ),
    )

    requires: set[str] = pydantic.Field(
        default_factory=set,
        description=(
            "Other dimensions whose primary keys are part of this dimension element's (compound) primary key."
        ),
    )

    implies: set[str] = pydantic.Field(
        default_factory=set,
        description="Other dimensions whose primary keys appear as foreign keys in this dimension element.",
    )

    metadata: list[MetadataColumnSpec] = pydantic.Field(
        default_factory=list,
        description="Non-key columns that provide extra information about a dimension element.",
    )

    is_cached: bool = pydantic.Field(
        default=False,
        description="Whether this element's records should be cached in the client.",
    )

    implied_union_target: str | None = pydantic.Field(
        default=None,
        description=textwrap.dedent(
            """\
            Another dimension whose stored values for this dimension form the
            set of all allowed values.

            The target dimension must have this dimension in its "implies"
            list.  This means the current dimension will have no table of its
            own in the database.
            """
        ),
    )

    governor: bool = pydantic.Field(
        default=False,
        description=textwrap.dedent(
            """\
            Whether this is a governor dimension.

            Governor dimensions are expected to have a tiny number of rows and
            must be explicitly provided in any dimension expression in which
            dependent dimensions appear.

            Implies is_cached=True.
            """
        ),
    )

    always_join: bool = pydantic.Field(
        default=False,
        description=textwrap.dedent(
            """\
            Whether this dimension join relation should always be included in
            any query where its required dependencies appear.
            """
        ),
    )

    populated_by: str | None = pydantic.Field(
        default=None,
        description=textwrap.dedent(
            """\
            The name of a required dimension that this dimension join
            relation's rows should transferred alongside.
            """
        ),
    )

    storage: Union[
        _LegacyGovernorDimensionStorage,
        _LegacyTableDimensionStorage,
        _LegacyImpliedUnionDimensionStorage,
        _LegacyCachingDimensionStorage,
        None,
    ] = pydantic.Field(
        description="How this dimension element's rows should be stored in the database and client.",
        discriminator="cls",
        default=None,
    )

    def has_dependencies_in(self, others: Set[str]) -> bool:
        # Docstring inherited from DimensionConstructionVisitor.
        return not (self.requires.isdisjoint(others) and self.implies.isdisjoint(others))

    def visit(self, name: str, builder: DimensionConstructionBuilder) -> None:
        # Docstring inherited from DimensionConstructionVisitor.
        if self.governor:
            from ._governor import GovernorDimension

            governor = GovernorDimension(
                name,
                metadata_columns=NamedValueSet(self.metadata).freeze(),
                unique_keys=NamedValueSet(self.keys).freeze(),
                doc=self.doc,
            )
            builder.dimensions.add(governor)
            builder.elements.add(governor)
            return
        # Expand required dependencies.
        for dependency_name in tuple(self.requires):  # iterate over copy
            self.requires.update(builder.dimensions[dependency_name].required.names)
        # Transform required and implied Dimension names into instances,
        # and reorder to match builder's order.
        required: NamedValueSet[Dimension] = NamedValueSet()
        implied: NamedValueSet[Dimension] = NamedValueSet()
        for dimension in builder.dimensions:
            if dimension.name in self.requires:
                required.add(dimension)
            if dimension.name in self.implies:
                implied.add(dimension)
        # Elements with keys are Dimensions; the rest are
        # DimensionCombinations.
        if self.keys:
            from ._database import DatabaseDimension

            dimension = DatabaseDimension(
                name,
                required=required,
                implied=implied.freeze(),
                metadata_columns=NamedValueSet(self.metadata).freeze(),
                unique_keys=NamedValueSet(self.keys).freeze(),
                is_cached=self.is_cached,
                implied_union_target=self.implied_union_target,
                doc=self.doc,
            )
            builder.dimensions.add(dimension)
            builder.elements.add(dimension)
        else:
            from ._database import DatabaseDimensionCombination

            combination = DatabaseDimensionCombination(
                name,
                required=required,
                implied=implied.freeze(),
                doc=self.doc,
                metadata_columns=NamedValueSet(self.metadata).freeze(),
                is_cached=self.is_cached,
                always_join=self.always_join,
                populated_by=(
                    builder.dimensions[self.populated_by] if self.populated_by is not None else None
                ),
            )
            builder.elements.add(combination)

    @pydantic.model_validator(mode="after")
    def _primary_key_types(self) -> _ElementConfig:
        if self.keys and self.keys[0].type not in ("int", "string"):
            raise ValueError(
                "The dimension primary key type (the first entry in the keys list) must be 'int' "
                f"or 'string'; got '{self.keys[0].type}'."
            )
        return self

    @pydantic.model_validator(mode="after")
    def _not_nullable_keys(self) -> _ElementConfig:
        for key in self.keys:
            key.nullable = False
        return self

    @pydantic.model_validator(mode="after")
    def _invalid_dimension_fields(self) -> _ElementConfig:
        if self.keys:
            if self.always_join:
                raise ValueError("Dimensions (elements with key columns) may not have always_join=True.")
            if self.populated_by:
                raise ValueError("Dimensions (elements with key columns) may not have populated_by.")
        return self

    @pydantic.model_validator(mode="after")
    def _storage(self) -> _ElementConfig:
        if self.storage is not None:
            # 'storage' is legacy; pull its implications into the regular
            # attributes and set it to None for consistency.
            self.is_cached = self.storage.is_cached
            self.implied_union_target = self.storage.implied_union_target
            self.storage = None
        if self.governor:
            self.is_cached = True
        if self.implied_union_target is not None:
            if self.requires:
                raise ValueError("Implied-union dimension may not have required dependencies.")
            if self.implies:
                raise ValueError("Implied-union dimension may not have implied dependencies.")
            if len(self.keys) > 1:
                raise ValueError("Implied-union dimension may not have alternate keys.")
            if self.metadata:
                raise ValueError("Implied-union dimension may not have metadata columns.")
        return self

    @pydantic.model_validator(mode="after")
    def _relationship_dependencies(self) -> _ElementConfig:
        if not self.keys and not self.requires:
            raise ValueError(
                "Dimension relationships (elements with no key columns) must have at least one "
                "required dependency."
            )
        return self


@final
class SerializedDimensionConfig(pydantic.BaseModel):
    """Configuration that describes a complete dimension data model."""

    version: int = pydantic.Field(
        default=0,
        description=textwrap.dedent(
            """\
            Integer version number for this universe.

            This and 'namespace' are expected to uniquely identify a
            dimension universe.
            """
        ),
    )

    namespace: str = pydantic.Field(
        default=_DEFAULT_NAMESPACE,
        description=textwrap.dedent(
            """\
            String namespace for this universe.

            This and 'version' are expected to uniquely identify a
            dimension universe.
            """
        ),
    )

    skypix: _SkyPixSectionConfig = pydantic.Field(
        description="Hierarchical sky pixelization systems recognized by this dimension universe."
    )

    elements: dict[str, _ElementConfig] = pydantic.Field(
        default_factory=dict, description="Non-skypix dimensions and dimension join relations."
    )

    topology: _TopologySectionConfig = pydantic.Field(
        description="Spatial and temporal relationships between dimensions.",
        default_factory=_TopologySectionConfig,
    )

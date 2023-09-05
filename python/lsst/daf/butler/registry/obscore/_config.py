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

__all__ = [
    "ConfigCollectionType",
    "DatasetTypeConfig",
    "ExtraColumnConfig",
    "ExtraColumnType",
    "ObsCoreConfig",
    "ObsCoreManagerConfig",
    "SpatialPluginConfig",
]

import enum
from collections.abc import Mapping
from typing import Any

import pydantic
from lsst.daf.butler._compat import PYDANTIC_V2, _BaseModelCompat
from pydantic import StrictBool, StrictFloat, StrictInt, StrictStr


class ExtraColumnType(str, enum.Enum):
    """Enum class defining possible values for types of extra columns."""

    bool = "bool"
    int = "int"
    float = "float"
    string = "string"


class ExtraColumnConfig(_BaseModelCompat):
    """Configuration class describing specification of additional column in
    obscore table.
    """

    template: str
    """Template string for formatting the column value."""

    type: ExtraColumnType = ExtraColumnType.string
    """Column type, formatted string will be converted to this actual type."""

    length: int | None = None
    """Optional length qualifier for a column, only used for strings."""

    doc: str | None = None
    """Documentation string for this column."""


class DatasetTypeConfig(_BaseModelCompat):
    """Configuration describing dataset type-related options."""

    dataproduct_type: str
    """Value for the ``dataproduct_type`` column."""

    dataproduct_subtype: str | None = None
    """Value for the ``dataproduct_subtype`` column, optional."""

    calib_level: int
    """Value for the ``calib_level`` column."""

    o_ucd: str | None = None
    """Value for the ``o_ucd`` column, optional."""

    access_format: str | None = None
    """Value for the ``access_format`` column, optional."""

    obs_id_fmt: str | None = None
    """Format string for ``obs_id`` column, optional. Uses `str.format`
    syntax.
    """

    datalink_url_fmt: str | None = None
    """Format string for ``access_url`` column for DataLink."""

    obs_collection: str | None = None
    """Value for the ``obs_collection`` column, if specified it overrides
    global value in `ObsCoreConfig`."""

    extra_columns: None | (
        dict[str, StrictFloat | StrictInt | StrictBool | StrictStr | ExtraColumnConfig]
    ) = None
    """Description for additional columns, optional.

    Keys are the names of the columns, values can be literal constants with the
    values, or ExtraColumnConfig mappings."""


class SpatialPluginConfig(_BaseModelCompat):
    """Configuration class for a spatial plugin."""

    cls: str
    """Name of the class implementing plugin methods."""

    config: dict[str, Any] = {}
    """Configuration object passed to plugin ``initialize()`` method."""


class ObsCoreConfig(_BaseModelCompat):
    """Configuration which controls conversion of Registry datasets into
    obscore records.

    This configuration is a base class for ObsCore manager configuration class.
    It can also be used by other tools that use `RecordFactory` to convert
    datasets into obscore records.
    """

    collections: list[str] | None = None
    """Registry collections to include, if missing then all collections are
    used. Depending on implementation the name in the list can be either a
    full collection name or a regular expression.
    """

    dataset_types: dict[str, DatasetTypeConfig]
    """Per-dataset type configuration, key is the dataset type name."""

    obs_collection: str | None = None
    """Value for the ``obs_collection`` column. This can be overridden in
    dataset type configuration.
    """

    facility_name: str
    """Value for the ``facility_name`` column."""

    extra_columns: None | (
        dict[str, StrictFloat | StrictInt | StrictBool | StrictStr | ExtraColumnConfig]
    ) = None
    """Description for additional columns, optional.

    Keys are the names of the columns, values can be literal constants with the
    values, or ExtraColumnConfig mappings."""

    indices: dict[str, str | list[str]] | None = None
    """Description of indices, key is the index name, value is the list of
    column names or a single column name. The index name may not be used for
    an actual index.
    """

    spectral_ranges: dict[str, tuple[float | None, float | None]] = {}
    """Maps band name or filter name to a min/max of spectral range. One or
    both ends can be specified as `None`.
    """

    spatial_plugins: dict[str, SpatialPluginConfig] = {}
    """Optional configuration for plugins managing spatial columns and
    indices. The key is an arbitrary name and the value is an object describing
    plugin class and its configuration options. By default there is no spatial
    indexing support, but a standard ``s_region`` column is always included.
    """


class ConfigCollectionType(str, enum.Enum):
    """Enum class defining possible values for configuration attributes."""

    RUN = "RUN"
    TAGGED = "TAGGED"


class ObsCoreManagerConfig(ObsCoreConfig):
    """Complete configuration for ObsCore manager."""

    namespace: str = "daf_butler_obscore"
    """Unique namespace to distinguish different instances, used for schema
    migration purposes.
    """

    version: int
    """Version of configuration, used for schema migration purposes. It needs
    to be incremented on every change of configuration that causes a schema or
    data migration.
    """

    table_name: str = "obscore"
    """Name of the table for ObsCore records."""

    collection_type: ConfigCollectionType
    """Type of the collections that can appear in ``collections`` attribute.

    When ``collection_type`` is ``RUN`` then ``collections`` contains regular
    expressions that will be used to match RUN collections only. When
    ``collection_type`` is ``TAGGED`` then ``collections`` must contain
    exactly one collection name which must be TAGGED collection.
    """

    if PYDANTIC_V2:

        @pydantic.model_validator(mode="after")  # type: ignore[attr-defined]
        def validate_collection_type(self) -> ObsCoreManagerConfig:
            """Check that contents of ``collections`` is consistent with
            ``collection_type``.
            """
            if self.collection_type is ConfigCollectionType.TAGGED:
                collections: list[str] | None = self.collections
                if collections is None or len(collections) != 1:
                    raise ValueError("'collections' must have one element when 'collection_type' is TAGGED")
            return self

    else:

        @pydantic.validator("collection_type")
        def validate_collection_type(
            cls, value: ConfigCollectionType, values: Mapping[str, Any]  # noqa: N805
        ) -> Any:
            """Check that contents of ``collections`` is consistent with
            ``collection_type``.
            """
            if value is ConfigCollectionType.TAGGED:
                collections: list[str] | None = values["collections"]
                if collections is None or len(collections) != 1:
                    raise ValueError("'collections' must have one element when 'collection_type' is TAGGED")
            return value

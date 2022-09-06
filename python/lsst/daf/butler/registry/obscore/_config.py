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

__all__ = ["DatasetTypeConfig", "ObsCoreConfig"]

from typing import Any, Dict, List, Optional, Tuple

from lsst.daf.butler import Config
from pydantic import BaseModel, validator


def _validate_extra_columns(extra_columns: Any) -> Any:
    """Validate ``extra_columns`` contents."""
    if extra_columns is None:
        return extra_columns
    result: Dict[str, Any] = {}
    for key, value in extra_columns.items():
        if isinstance(value, Config):
            value = value.toDict()
        if isinstance(value, dict):
            if "template" not in value:
                raise ValueError(
                    f"required 'template' attribute is missing from extra_columns {key}: {value}"
                )
            unknown_keys = set(value) - {"template", "type", "length"}
            if unknown_keys:
                raise ValueError(f"Unexpected attributes in extra_columns: {unknown_keys}; value: {value}")
            if not isinstance(value["template"], str):
                raise ValueError(f"'{key}.template' attribute must be a string")
            # set or check column type
            if "type" not in value:
                value["type"] = "str"
            else:
                column_type = value["type"]
                types = {"bool", "int", "float", "string"}
                if column_type not in types:
                    raise ValueError(
                        f"Unexpected '{key}.type' attribute: {column_type}; must be one of {types}"
                    )
        result[key] = value
    return result


class DatasetTypeConfig(BaseModel):
    """Configuration describing dataset type-related options."""

    dataproduct_type: str
    """Value for the ``dataproduct_type`` column."""

    dataproduct_subtype: Optional[str] = None
    """Value for the ``dataproduct_subtype`` column, optional."""

    calib_level: int
    """Value for the ``calib_level`` column."""

    o_ucd: Optional[str] = None
    """Value for the ``o_ucd`` column, optional."""

    access_format: Optional[str] = None
    """Value for the ``access_format`` column, optional."""

    obs_id_fmt: Optional[str] = None
    """Format string for ``obs_id`` column, optional. Uses `str.format`
    syntax.
    """

    datalink_url_fmt: Optional[str] = None
    """Format string for ``access_url`` column for DataLink."""

    obs_collection: Optional[str] = None
    """Value for the ``obs_collection`` column, if specified it overrides
    global value in `ObsCoreConfig`."""

    extra_columns: Optional[Dict[str, Any]] = None
    """Values for additional columns, optional."""

    @validator("extra_columns")
    def validate_extra_columns(cls, value: Any) -> Any:  # noqa: N805
        """If the value is a dict then check the keys and values"""
        return _validate_extra_columns(value)


class ObsCoreConfig(BaseModel):
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

    collections: Optional[List[str]] = None
    """Names of registry collections to search, if missing then all collections
    are used. Names should be complete, wildcards are not supported.
    """

    dataset_types: Dict[str, DatasetTypeConfig]
    """Per-dataset type configuration, key is the dataset type name."""

    obs_collection: Optional[str] = None
    """Value for the ``obs_collection`` column. This can be overridden in
    dataset type configuration.
    """

    facility_name: str
    """Value for the ``facility_name`` column."""

    extra_columns: Optional[Dict[str, Any]] = None
    """Values for additional columns, optional."""

    spectral_ranges: Dict[str, Tuple[float, float]] = {}
    """Maps band name or filter name to a min/max of spectral range."""

    spatial_backend: Optional[str] = None
    """The name of a spatial backend which manages additional spatial
    columns and indices (e.g. "pgsphere"). By default there is no spatial
    indexing support, but a standard ``s_region`` column is always included.
    """

    @validator("extra_columns")
    def validate_extra_columns(cls, value: Any) -> Any:  # noqa: N805
        """If the value is a dict then check the keys and values"""
        return _validate_extra_columns(value)

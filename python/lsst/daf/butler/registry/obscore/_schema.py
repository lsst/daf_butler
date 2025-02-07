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

__all__ = ["ObsCoreSchema"]

import re
from collections.abc import Sequence
from typing import TYPE_CHECKING

import sqlalchemy

from lsst.daf.butler import ddl
from lsst.utils.iteration import ensure_iterable

from ._config import DatasetTypeConfig, ExtraColumnConfig, ObsCoreConfig
from ._spatial import SpatialObsCorePlugin

if TYPE_CHECKING:
    from ..interfaces import DatasetRecordStorageManager


# Regular expression to match templates in extra_columns that specify simple
# dimensions, e.g. "{exposure}".
_DIMENSION_TEMPLATE_RE = re.compile(r"^[{](\w+)[}]$")

# List of standard columns in output file. This should include at least all
# mandatory columns defined in ObsCore note (revision 1.1, Appendix B). Extra
# columns can be added via `extra_columns` parameters in configuration.
_STATIC_COLUMNS = (
    ddl.FieldSpec(
        name="dataproduct_type", dtype=sqlalchemy.String, length=255, doc="Logical data product type"
    ),
    ddl.FieldSpec(
        name="dataproduct_subtype", dtype=sqlalchemy.String, length=255, doc="Data product specific type"
    ),
    ddl.FieldSpec(
        name="facility_name",
        dtype=sqlalchemy.String,
        length=255,
        doc="The name of the facility used for the observation",
    ),
    ddl.FieldSpec(name="calib_level", dtype=sqlalchemy.SmallInteger, doc="Calibration level {0, 1, 2, 3, 4}"),
    ddl.FieldSpec(name="target_name", dtype=sqlalchemy.String, length=255, doc="Object of interest"),
    ddl.FieldSpec(name="obs_id", dtype=sqlalchemy.String, length=255, doc="Observation ID"),
    ddl.FieldSpec(
        name="obs_collection", dtype=sqlalchemy.String, length=255, doc="Name of the data collection"
    ),
    ddl.FieldSpec(
        name="obs_publisher_did",
        dtype=sqlalchemy.String,
        length=255,
        doc="Dataset identifier given by the publisher",
    ),
    ddl.FieldSpec(
        name="access_url", dtype=sqlalchemy.String, length=65535, doc="URL used to access (download) dataset"
    ),
    ddl.FieldSpec(name="access_format", dtype=sqlalchemy.String, length=255, doc="File content format"),
    # Spatial columns s_ra, s_dec, s_fow, s_region are managed by a default
    # spatial plugin
    ddl.FieldSpec(
        name="s_resolution", dtype=sqlalchemy.Float, doc="Spatial resolution of data as FWHM (arcsec)"
    ),
    ddl.FieldSpec(
        name="s_xel1", dtype=sqlalchemy.Integer, doc="Number of elements along the first spatial axis"
    ),
    ddl.FieldSpec(
        name="s_xel2", dtype=sqlalchemy.Integer, doc="Number of elements along the second spatial axis"
    ),
    ddl.FieldSpec(name="t_xel", dtype=sqlalchemy.Integer, doc="Number of elements along the time axis"),
    ddl.FieldSpec(name="t_min", dtype=sqlalchemy.Float, doc="Start time in MJD"),
    ddl.FieldSpec(name="t_max", dtype=sqlalchemy.Float, doc="Stop time in MJD"),
    ddl.FieldSpec(name="t_exptime", dtype=sqlalchemy.Float, doc="Total exposure time (sec)"),
    ddl.FieldSpec(name="t_resolution", dtype=sqlalchemy.Float, doc="Temporal resolution (sec)"),
    ddl.FieldSpec(name="em_xel", dtype=sqlalchemy.Integer, doc="Number of elements along the spectral axis"),
    ddl.FieldSpec(name="em_min", dtype=sqlalchemy.Float, doc="Start in spectral coordinates (m)"),
    ddl.FieldSpec(name="em_max", dtype=sqlalchemy.Float, doc="Stop in spectral coordinates (m)"),
    ddl.FieldSpec(name="em_res_power", dtype=sqlalchemy.Float, doc="Spectral resolving power"),
    ddl.FieldSpec(
        name="em_filter_name", dtype=sqlalchemy.String, length=255, doc="Filter name (non-standard column)"
    ),
    ddl.FieldSpec(name="o_ucd", dtype=sqlalchemy.String, length=255, doc="UCD of observable"),
    ddl.FieldSpec(name="pol_xel", dtype=sqlalchemy.Integer, doc="Number of polarization samples"),
    ddl.FieldSpec(
        name="instrument_name",
        dtype=sqlalchemy.String,
        length=255,
        doc="Name of the instrument used for this observation",
    ),
)

_TYPE_MAP = {
    int: sqlalchemy.BigInteger,
    float: sqlalchemy.Float,
    bool: sqlalchemy.Boolean,
    str: sqlalchemy.String,
}


class ObsCoreSchema:
    """Generate table specification for an ObsCore table based on its
    configuration.

    Parameters
    ----------
    config : `ObsCoreConfig`
        ObsCore configuration instance.
    spatial_plugins : `~collections.abc.Sequence` of `SpatialObsCorePlugin`
        Spatial plugins.
    datasets : `type`, optional
        Type of dataset records manager. If specified, the ObsCore table will
        define a foreign key to ``datasets`` table with "ON DELETE CASCADE"
        constraint.

    Notes
    -----
    This class is designed to support both "live" obscore table which is
    located in the same database as the Registry, and standalone table in a
    completely separate database. Live obscore table depends on foreign key
    constraints with "ON DELETE CASCADE" option to manage lifetime of obscore
    records when their original datasets are removed.
    """

    def __init__(
        self,
        config: ObsCoreConfig,
        spatial_plugins: Sequence[SpatialObsCorePlugin],
        datasets: type[DatasetRecordStorageManager] | None = None,
    ):
        self._dimension_columns: dict[str, str] = {"instrument": "instrument_name"}

        fields = list(_STATIC_COLUMNS)

        column_names = {col.name for col in fields}

        all_configs: list[ObsCoreConfig | DatasetTypeConfig] = [config]
        if config.dataset_types:
            all_configs += list(config.dataset_types.values())
        for cfg in all_configs:
            if cfg.extra_columns:
                for col_name, col_value in cfg.extra_columns.items():
                    if col_name in column_names:
                        continue
                    doc: str | None = None
                    if isinstance(col_value, ExtraColumnConfig):
                        col_type = ddl.VALID_CONFIG_COLUMN_TYPES.get(col_value.type.name)
                        col_length = col_value.length
                        doc = col_value.doc
                        # For columns that store dimensions remember their
                        # column names.
                        if match := _DIMENSION_TEMPLATE_RE.match(col_value.template):
                            dimension = match.group(1)
                            self._dimension_columns[dimension] = col_name
                    else:
                        # Only value is provided, guess type from Python, and
                        # use a fixed length of 255 for strings.
                        col_type = _TYPE_MAP.get(type(col_value))
                        col_length = 255 if isinstance(col_value, str) else None
                    if col_type is None:
                        raise TypeError(
                            f"Unexpected type in extra_columns: column={col_name}, value={col_value}"
                        )
                    fields.append(ddl.FieldSpec(name=col_name, dtype=col_type, length=col_length, doc=doc))
                    column_names.add(col_name)

        indices: list[ddl.IndexSpec] = []
        if config.indices:
            for columns in config.indices.values():
                indices.append(ddl.IndexSpec(*ensure_iterable(columns)))

        self._table_spec = ddl.TableSpec(fields=fields, indexes=indices)

        # Possibly extend table specs with plugin-added stuff.
        for plugin in spatial_plugins:
            plugin.extend_table_spec(self._table_spec)

        self._dataset_fk: ddl.FieldSpec | None = None
        if datasets is not None:
            # Add FK to datasets, is also a PK for this table
            self._dataset_fk = datasets.addDatasetForeignKey(
                self._table_spec, name="registry_dataset", onDelete="CASCADE", doc="Registry dataset ID"
            )
            self._dataset_fk.primaryKey = True

    @property
    def table_spec(self) -> ddl.TableSpec:
        """Specification for obscore table (`ddl.TableSpec`)."""
        return self._table_spec

    @property
    def dataset_fk(self) -> ddl.FieldSpec | None:
        """Specification for the field which is a foreign key to ``datasets``
        table, and also a primary key for obscore table (`ddl.FieldSpec` or
        `None`).
        """
        return self._dataset_fk

    def dimension_column(self, dimension: str) -> str | None:
        """Return column name for a given dimension.

        Parameters
        ----------
        dimension : `str`
            Dimension name, e.g. "exposure".

        Returns
        -------
        column_name : `str` or `None`
            Name of the column in obscore table or `None` if there is no
            configured column for this dimension.
        """
        return self._dimension_columns.get(dimension)

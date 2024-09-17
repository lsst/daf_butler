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

__all__ = ["ExposureRegionFactory", "Record", "RecordFactory"]

import logging
import warnings
from abc import abstractmethod
from collections.abc import Callable, Collection, Mapping
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID

import astropy.time
from lsst.daf.butler import DataCoordinate, DatasetRef, Dimension, DimensionRecord, DimensionUniverse
from lsst.utils.introspection import find_outside_stacklevel

from ._config import ExtraColumnConfig, ExtraColumnType, ObsCoreConfig
from ._spatial import RegionTypeError, RegionTypeWarning

if TYPE_CHECKING:
    from lsst.sphgeom import Region

    from ._schema import ObsCoreSchema
    from ._spatial import SpatialObsCorePlugin

_LOG = logging.getLogger(__name__)

# Map extra column type to a conversion method that takes string.
_TYPE_CONVERSION: Mapping[str, Callable[[str], Any]] = {
    ExtraColumnType.bool: lambda x: bool(int(x)),  # expect integer number/string as input.
    ExtraColumnType.int: int,
    ExtraColumnType.float: float,
    ExtraColumnType.string: str,
}


class ExposureRegionFactory:
    """Abstract interface for a class that returns a Region for an exposure."""

    @abstractmethod
    def exposure_region(self, dataId: DataCoordinate) -> Region | None:
        """Return a region for a given DataId that corresponds to an exposure.

        Parameters
        ----------
        dataId : `DataCoordinate`
            Data ID for an exposure dataset.

        Returns
        -------
        region : `Region`
            `None` is returned if region cannot be determined.
        """
        raise NotImplementedError()


Record = dict[str, Any]


class RecordFactory:
    """Class that implements conversion of dataset information to ObsCore.

    Parameters
    ----------
    config : `ObsCoreConfig`
        Complete configuration specifying conversion options.
    schema : `ObsCoreSchema`
        Description of obscore schema.
    universe : `DimensionUniverse`
        Registry dimensions universe.
    spatial_plugins : `~collections.abc.Collection` of `SpatialObsCorePlugin`
        Spatial plugins.
    exposure_region_factory : `ExposureRegionFactory`, optional
        Manager for Registry dimensions.
    """

    def __init__(
        self,
        config: ObsCoreConfig,
        schema: ObsCoreSchema,
        universe: DimensionUniverse,
        spatial_plugins: Collection[SpatialObsCorePlugin],
        exposure_region_factory: ExposureRegionFactory | None = None,
    ):
        self.config = config
        self.schema = schema
        self.universe = universe
        self.exposure_region_factory = exposure_region_factory
        self.spatial_plugins = spatial_plugins

        # All dimension elements used below.
        self.band = cast(Dimension, universe["band"])
        self.exposure = universe["exposure"]
        self.visit = universe["visit"]
        self.physical_filter = cast(Dimension, universe["physical_filter"])

    def __call__(self, ref: DatasetRef) -> Record | None:
        """Make an ObsCore record from a dataset.

        Parameters
        ----------
        ref : `DatasetRef`
            Dataset ref, its DataId must be in expanded form.
        context : `SqlQueryContext`
            Context used to execute queries for additional dimension metadata.

        Returns
        -------
        record : `dict` [ `str`, `Any` ] or `None`
            ObsCore record represented as a dictionary. `None` is returned if
            dataset does not need to be stored in the obscore table, e.g. when
            dataset type is not in obscore configuration.

        Notes
        -----
        This method filters records by dataset type and returns `None` if
        reference dataset type is not configured. It does not check reference
        run name against configured collections, all runs are acceptable by
        this method.
        """
        # Quick check for dataset type.
        dataset_type_name = ref.datasetType.name
        dataset_config = self.config.dataset_types.get(dataset_type_name)
        if dataset_config is None:
            return None

        dataId = ref.dataId
        # _LOG.debug("New record, dataId=%s", dataId.full)
        # _LOG.debug("New record, records=%s", dataId.records)

        record: dict[str, str | int | float | UUID | None]

        # We need all columns filled, to simplify logic below just pre-fill
        # everything with None.
        record = {field.name: None for field in self.schema.table_spec.fields}

        record["dataproduct_type"] = dataset_config.dataproduct_type
        record["dataproduct_subtype"] = dataset_config.dataproduct_subtype
        record["o_ucd"] = dataset_config.o_ucd
        record["facility_name"] = self.config.facility_name
        record["calib_level"] = dataset_config.calib_level
        if dataset_config.obs_collection is not None:
            record["obs_collection"] = dataset_config.obs_collection
        else:
            record["obs_collection"] = self.config.obs_collection
        record["access_format"] = dataset_config.access_format

        record["instrument_name"] = dataId.get("instrument")
        if self.schema.dataset_fk is not None:
            record[self.schema.dataset_fk.name] = ref.id

        timespan = dataId.timespan
        if timespan is not None:
            if timespan.begin is not None:
                t_min = cast(astropy.time.Time, timespan.begin)
                record["t_min"] = t_min.mjd
            if timespan.end is not None:
                t_max = cast(astropy.time.Time, timespan.end)
                record["t_max"] = t_max.mjd

        region = dataId.region
        if self.exposure.name in dataId:
            if (dimension_record := dataId.records[self.exposure.name]) is not None:
                self._exposure_records(dimension_record, record)
                if self.exposure_region_factory is not None:
                    region = self.exposure_region_factory.exposure_region(dataId)
        elif self.visit.name in dataId and (dimension_record := dataId.records[self.visit.name]) is not None:
            self._visit_records(dimension_record, record)

        # ask each plugin for its values to add to a record.
        try:
            plugin_records = self.make_spatial_records(region)
        except RegionTypeError as exc:
            warnings.warn(
                f"Failed to convert region for obscore dataset {ref.id}: {exc}",
                category=RegionTypeWarning,
                stacklevel=find_outside_stacklevel("lsst.daf.butler"),
            )
        else:
            record.update(plugin_records)

        if self.band.name in dataId:
            em_range = None
            if (label := dataId.get(self.physical_filter.name)) is not None:
                em_range = self.config.spectral_ranges.get(cast(str, label))
            if not em_range:
                band_name = dataId[self.band.name]
                assert isinstance(band_name, str), "Band name must be string"
                em_range = self.config.spectral_ranges.get(band_name)
            if em_range:
                record["em_min"], record["em_max"] = em_range
            else:
                _LOG.warning("could not find spectral range for dataId=%s", dataId)
            record["em_filter_name"] = dataId["band"]

        # Dictionary to use for substitutions when formatting various
        # strings.
        fmt_kws: dict[str, Any] = dict(records=dataId.records)
        fmt_kws.update(dataId.mapping)
        fmt_kws.update(id=ref.id)
        fmt_kws.update(run=ref.run)
        fmt_kws.update(dataset_type=dataset_type_name)
        fmt_kws.update(record)
        if dataset_config.obs_id_fmt:
            record["obs_id"] = dataset_config.obs_id_fmt.format(**fmt_kws)
            fmt_kws["obs_id"] = record["obs_id"]

        if dataset_config.datalink_url_fmt:
            record["access_url"] = dataset_config.datalink_url_fmt.format(**fmt_kws)

        # add extra columns
        extra_columns = {}
        if self.config.extra_columns:
            extra_columns.update(self.config.extra_columns)
        if dataset_config.extra_columns:
            extra_columns.update(dataset_config.extra_columns)
        for key, column_value in extra_columns.items():
            # Try to expand the template with known keys, if expansion
            # fails due to a missing key name then store None.
            if isinstance(column_value, ExtraColumnConfig):
                try:
                    value = column_value.template.format(**fmt_kws)
                    record[key] = _TYPE_CONVERSION[column_value.type](value)
                except KeyError:
                    pass
            else:
                # Just a static value.
                record[key] = column_value

        return record

    def make_spatial_records(self, region: Region | None) -> Record:
        """Make spatial records for a given region.

        Parameters
        ----------
        region : `~lsst.sphgeom.Region` or `None`
            Spacial region to convert to record.

        Returns
        -------
        record : `dict`
            Record items.

        Raises
        ------
        RegionTypeError
            Raised if type of the region is not supported.
        """
        record = Record()
        # ask each plugin for its values to add to a record.
        for plugin in self.spatial_plugins:
            plugin_record = plugin.make_records(region)
            if plugin_record is not None:
                record.update(plugin_record)
        return record

    def _exposure_records(self, dimension_record: DimensionRecord, record: dict[str, Any]) -> None:
        """Extract all needed info from a visit dimension record."""
        record["t_exptime"] = dimension_record.exposure_time
        record["target_name"] = dimension_record.target_name

    def _visit_records(self, dimension_record: DimensionRecord, record: dict[str, Any]) -> None:
        """Extract all needed info from an exposure dimension record."""
        record["t_exptime"] = dimension_record.exposure_time
        record["target_name"] = dimension_record.target_name

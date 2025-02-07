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

__all__ = ["DerivedRegionFactory", "Record", "RecordFactory"]

import logging
import warnings
from abc import abstractmethod
from collections.abc import Callable, Collection, Mapping
from importlib.metadata import entry_points
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID

import astropy.time

from lsst.daf.butler import DataCoordinate, DatasetRef, Dimension, DimensionRecord, DimensionUniverse
from lsst.utils.introspection import find_outside_stacklevel, get_full_type_name

from ._config import ExtraColumnConfig, ExtraColumnType, ObsCoreConfig
from ._spatial import RegionTypeError, RegionTypeWarning

if TYPE_CHECKING:
    from lsst.daf.butler import DimensionGroup
    from lsst.sphgeom import Region

    from ._config import DatasetTypeConfig
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


class DerivedRegionFactory:
    """Abstract interface for a class that returns a Region for a data ID."""

    @abstractmethod
    def derived_region(self, dataId: DataCoordinate) -> Region | None:
        """Return a region for a given DataId that may have been derived.

        Parameters
        ----------
        dataId : `DataCoordinate`
            Data ID for the relevant dataset.

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
    derived_region_factory : `DerivedRegionFactory`, optional
        Factory for handling derived regions that are not directly
        available from the data ID.
    """

    def __init__(
        self,
        config: ObsCoreConfig,
        schema: ObsCoreSchema,
        universe: DimensionUniverse,
        spatial_plugins: Collection[SpatialObsCorePlugin],
        derived_region_factory: DerivedRegionFactory | None = None,
    ):
        self.config = config
        self.schema = schema
        self.universe = universe
        self.derived_region_factory = derived_region_factory
        self.spatial_plugins = spatial_plugins

        # All dimension elements used below.
        self.band = cast(Dimension, universe["band"])
        self.exposure = universe["exposure"]
        self.visit = universe["visit"]
        self.physical_filter = cast(Dimension, universe["physical_filter"])

    @abstractmethod
    def region_dimension(self, dimensions: DimensionGroup) -> tuple[str, str] | tuple[None, None]:
        """Return the dimension to use to obtain a region.

        Parameters
        ----------
        dimensions : `DimensionGroup`
            The dimensions to be examined.

        Returns
        -------
        region_dim : `str` or `None`
            The dimension to use to get the region information. Can be `None`
            if there is no relevant dimension for a region.
        region_metadata : `str` or `None`
            The metadata field for the ``region_dim`` that specifies the
            region itself. Will be `None` if ``region_dim`` is `None`.

        Notes
        -----
        This is universe specific. For example, in the ``daf_butler`` namespace
        the region for an ``exposure`` is obtained by looking for the relevant
        ``visit`` or ``visit_detector_region``.
        """
        raise NotImplementedError()

    def make_generic_records(self, ref: DatasetRef, dataset_config: DatasetTypeConfig) -> Record:
        """Fill record content that is not associated with a specific universe.

        Parameters
        ----------
        ref : `DatasetRef`
            Dataset ref, its DataId must be in expanded form.
        dataset_config : `DatasetTypeConfig`
            The configuration for the dataset type.

        Returns
        -------
        record : `dict` [ `str`, `typing.Any` ]
            Record content with generic content filled in that does not
            depend on a specific universe.
        """
        record: dict[str, str | int | float | UUID | None] = {}

        record["dataproduct_type"] = dataset_config.dataproduct_type
        record["dataproduct_subtype"] = dataset_config.dataproduct_subtype
        record["o_ucd"] = dataset_config.o_ucd
        record["calib_level"] = dataset_config.calib_level
        if dataset_config.obs_collection is not None:
            record["obs_collection"] = dataset_config.obs_collection
        else:
            record["obs_collection"] = self.config.obs_collection
        record["access_format"] = dataset_config.access_format

        dataId = ref.dataId
        dataset_type_name = ref.datasetType.name

        # Dictionary to use for substitutions when formatting various
        # strings from configuration.
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

    @abstractmethod
    def make_universe_records(self, ref: DatasetRef) -> Record:
        """Create universe-specific record content.

        Parameters
        ----------
        ref : `DatasetRef`
            Dataset ref, its DataId must be in expanded form.

        Returns
        -------
        record : `dict` [ `str`, `typing.Any` ]
            Record content populated using algorithms specific to this
            dimension universe.
        """
        raise NotImplementedError()

    def __call__(self, ref: DatasetRef) -> Record | None:
        """Make an ObsCore record from a dataset.

        Parameters
        ----------
        ref : `DatasetRef`
            Dataset ref, its DataId must be in expanded form.

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

        # _LOG.debug("New record, dataId=%s", dataId.full)
        # _LOG.debug("New record, records=%s", dataId.records)

        record: dict[str, str | int | float | UUID | None]

        # We need all columns filled, to simplify logic below just pre-fill
        # everything with None.
        record = {field.name: None for field in self.schema.table_spec.fields}

        record.update(self.make_generic_records(ref, dataset_config))
        record.update(self.make_universe_records(ref))

        return record

    def make_spatial_records(self, region: Region | None, warn: bool = False, msg: str = "") -> Record:
        """Make spatial records for a given region.

        Parameters
        ----------
        region : `~lsst.sphgeom.Region` or `None`
            Spacial region to convert to record.
        warn : `bool`, optional
            If `False`, an exception will be raised if the type of region
            is not supported. If `True` a warning will be issued and an
            empty `dict` returned.
        msg : `str`, optional
            Message to use in warning. Generic message will be used if not
            given. This message will be used to annotate any `RegionTypeError`
            exception raised if defined.

        Returns
        -------
        record : `dict`
            Record items.

        Raises
        ------
        RegionTypeError
            Raised if type of the region is not supported and ``warn`` is
            `False`.
        """
        record = Record()
        try:
            # Ask each plugin for its values to add to a record.
            for plugin in self.spatial_plugins:
                plugin_record = plugin.make_records(region)
                if plugin_record is not None:
                    record.update(plugin_record)
        except RegionTypeError as exc:
            if warn:
                if not msg:
                    msg = "Failed to convert obscore region"
                warnings.warn(
                    f"{msg}: {exc}",
                    category=RegionTypeWarning,
                    stacklevel=find_outside_stacklevel("lsst.daf.butler"),
                )
                # Clear the record.
                record = Record()
            else:
                if msg:
                    exc.add_note(msg)
                raise
        return record

    @classmethod
    def get_record_type_from_universe(cls, universe: DimensionUniverse) -> type[RecordFactory]:
        namespace = universe.namespace
        if namespace == "daf_butler":
            return DafButlerRecordFactory
        # Check for entry points.
        plugins = {p.name: p for p in entry_points(group="butler.obscore_factory")}
        if namespace in plugins:
            func = plugins[namespace].load()
            # The entry point function is required to return the class that
            # should be used for the RecordFactory for this universe.
            record_factory_type = func()
            if not issubclass(record_factory_type, RecordFactory):
                raise TypeError(
                    f"Entry point for universe {namespace} did not return RecordFactory. "
                    f"Returned {get_full_type_name(record_factory_type)}"
                )
            return record_factory_type
        raise ValueError(f"Unable to load record factory dynamically for universe namespace {namespace}")


class DafButlerRecordFactory(RecordFactory):
    """Class that implements conversion of dataset information to ObsCore
    using the daf_butler dimension universe namespace.

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
    derived_region_factory : `DerivedRegionFactory`, optional
        Factory for handling derived regions that are not directly
        available from the data ID.
    """

    def __init__(
        self,
        config: ObsCoreConfig,
        schema: ObsCoreSchema,
        universe: DimensionUniverse,
        spatial_plugins: Collection[SpatialObsCorePlugin],
        derived_region_factory: DerivedRegionFactory | None = None,
    ):
        super().__init__(
            config=config,
            schema=schema,
            universe=universe,
            spatial_plugins=spatial_plugins,
            derived_region_factory=derived_region_factory,
        )

        # All dimension elements used below.
        self.band = cast(Dimension, universe["band"])
        self.exposure = universe["exposure"]
        self.visit = universe["visit"]
        self.physical_filter = cast(Dimension, universe["physical_filter"])

    def make_universe_records(self, ref: DatasetRef) -> Record:
        # Construct records using the daf_butler dimension universe.
        dataId = ref.dataId
        record: dict[str, str | int | float | UUID | None] = {}

        instrument_name = cast(str, dataId.get("instrument"))
        record["instrument_name"] = instrument_name
        if self.schema.dataset_fk is not None:
            record[self.schema.dataset_fk.name] = ref.id

        record["facility_name"] = self.config.facility_map.get(instrument_name, self.config.facility_name)

        timespan = dataId.timespan
        if timespan is not None:
            if timespan.begin is not None:
                t_min = cast(astropy.time.Time, timespan.begin)
                record["t_min"] = float(t_min.mjd)
            if timespan.end is not None:
                t_max = cast(astropy.time.Time, timespan.end)
                record["t_max"] = float(t_max.mjd)

        region = dataId.region
        if self.exposure.name in dataId:
            if (dimension_record := dataId.records[self.exposure.name]) is not None:
                self._exposure_records(dimension_record, record)
                if self.derived_region_factory is not None:
                    region = self.derived_region_factory.derived_region(dataId)
        elif self.visit.name in dataId and (dimension_record := dataId.records[self.visit.name]) is not None:
            self._visit_records(dimension_record, record)

        # Create spatial records.
        record.update(
            self.make_spatial_records(
                region, warn=True, msg=f"Failed to convert region for obscore dataset {ref.id}"
            )
        )

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

        return record

    def _exposure_records(self, dimension_record: DimensionRecord, record: dict[str, Any]) -> None:
        """Extract all needed info from a visit dimension record."""
        record["t_exptime"] = dimension_record.exposure_time
        record["target_name"] = dimension_record.target_name

    def _visit_records(self, dimension_record: DimensionRecord, record: dict[str, Any]) -> None:
        """Extract all needed info from an exposure dimension record."""
        record["t_exptime"] = dimension_record.exposure_time
        record["target_name"] = dimension_record.target_name

    def region_dimension(self, dimensions: DimensionGroup) -> tuple[str, str] | tuple[None, None]:
        # Inherited doc string.
        region_dim = dimensions.region_dimension
        if not region_dim:
            if "exposure" in dimensions:
                if "detector" in dimensions:
                    region_dim = "visit_detector_region"
                else:
                    region_dim = "visit"
            else:
                return None, None
        return region_dim, "region"

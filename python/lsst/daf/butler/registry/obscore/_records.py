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

__all__ = ["RecordFactory"]

import logging
from collections.abc import Mapping
from typing import Any, Callable, Dict, Iterator, Optional, cast
from uuid import UUID

import astropy.time
from lsst.daf.butler import (
    DataCoordinate,
    DataCoordinateIterable,
    DatasetRef,
    Dimension,
    DimensionRecord,
    DimensionUniverse,
)
from lsst.sphgeom import ConvexPolygon, LonLat, Region

from ..interfaces import CollectionManager, CollectionRecord, DimensionRecordStorageManager
from ._config import ObsCoreConfig
from ._schema import ObsCoreSchema

_LOG = logging.getLogger(__name__)

# Map type name to a conversion method that takes string.
_TYPE_CONVERSION: Mapping[str, Callable[[str], Any]] = {
    "bool": lambda x: bool(int(x)),  # expect integer number/string as input.
    "int": int,
    "float": float,
    "string": str,
}


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
    collections : `CollectionManager`
        Manager of Registry collections.
    dimensions: `DimensionRecordStorageManager`
        Manager for Registry dimensions.
    """

    def __init__(
        self,
        config: ObsCoreConfig,
        schema: ObsCoreSchema,
        universe: DimensionUniverse,
        collections: CollectionManager,
        dimensions: DimensionRecordStorageManager,
    ):
        self.config = config
        self.schema = schema
        self.universe = universe
        self.collections = collections
        self.dimensions = dimensions
        self.connection_names = frozenset(self.config.collections or [])

        # All dimension elements used below.
        self.band = cast(Dimension, universe["band"])
        self.exposure = universe["exposure"]
        self.visit = universe["visit"]
        self.physical_filter = cast(Dimension, universe["physical_filter"])

    def __call__(
        self, ref: DatasetRef, collection: Optional[str] = None
    ) -> Optional[Dict[str, str | int | float | UUID | None]]:
        """Make an ObsCore record from a dataset.

        Parameters
        ----------
        ref : `DatasetRef`
            Dataset ref, its DataId must be in expanded form.
        collection : `str`, optional
            Name of a collection. If specified then this collection name is
            used to filter datasets. Original run name of the dataset is always
            used in obscore record (if configuration uses run name in the
            templates of any column).

        Returns
        -------
        record : `dict` [ `str`, `Any` ]
            ObsCore record represented a dictionary. `None` is returned if
            dataset does not need to be stored in the obscore table.
        """
        # Quick check for dataset type.
        dataset_type_name = ref.datasetType.name
        dataset_config = self.config.dataset_types.get(dataset_type_name)
        if dataset_config is None:
            return None

        # Check collection name.
        if self.connection_names:
            if collection is None:
                assert ref.run is not None, "Run cannot be None"
                collection = ref.run
            if not self._check_collections(collection):
                return None

        dataId = ref.dataId
        # _LOG.debug("New record, dataId=%s", dataId.full)
        # _LOG.debug("New record, records=%s", dataId.records)

        record: Dict[str, str | int | float | UUID | None] = {}

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
        if self.exposure in dataId:
            if (dimension_record := dataId.records[self.exposure]) is not None:
                self._exposure_records(dimension_record, record)
                region = self._exposure_region(dataId)
        elif self.visit in dataId:
            if (dimension_record := dataId.records[self.visit]) is not None:
                self._visit_records(dimension_record, record)

        self.region_to_columns(region, record)

        if self.band in dataId:
            em_range = None
            if (label := dataId.get(self.physical_filter)) is not None:
                em_range = self.config.spectral_ranges.get(label)
            if not em_range:
                band_name = dataId[self.band]
                assert isinstance(band_name, str), "Band name must be string"
                em_range = self.config.spectral_ranges.get(band_name)
            if em_range:
                record["em_min"], record["em_max"] = em_range
            else:
                _LOG.warning("could not find spectral range for dataId=%s", dataId.full)
            record["em_filter_name"] = dataId["band"]

        # Dictionary to use for substitutions when formatting various
        # strings.
        fmt_kws: Dict[str, Any] = dict(records=dataId.records)
        fmt_kws.update(dataId.full.byName())
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
            if isinstance(column_value, Mapping):
                try:
                    value = column_value["template"].format(**fmt_kws)
                    record[key] = _TYPE_CONVERSION[column_value["type"]](value)
                except KeyError:
                    pass
            else:
                # Just a static value.
                record[key] = column_value

        return record

    @classmethod
    def region_to_columns(cls, region: Optional[Region], record: Dict[str, Any]) -> None:
        """Fill obscore column values from sphgeom region.

        Parameters
        ----------
        region : `lsst.sphgeom.Region`
            Spatial region, expected to be a ``ConvexPolygon`` instance,
            warning will be logged for other types.
        record : `dict` [ `str`, `Any` ]
            Obscore record that will be expanded with the new columns.

        Notes
        -----
        This method adds ``s_ra``, ``s_dec``, and ``s_fov`` values to the
        record, they are computed from the region bounding circle. If the
        region is a ``ConvexPolygon`` instance, then ``s_region`` value is
        added as well representing the polygon in ADQL format.
        """
        if region is None:
            return

        # Get spatial parameters from the bounding circle.
        circle = region.getBoundingCircle()
        center = LonLat(circle.getCenter())
        record["s_ra"] = center.getLon().asDegrees()
        record["s_dec"] = center.getLat().asDegrees()
        record["s_fov"] = circle.getOpeningAngle().asDegrees() * 2

        if isinstance(region, ConvexPolygon):
            poly = ["POLYGON ICRS"]
            for vertex in region.getVertices():
                lon_lat = LonLat(vertex)
                poly += [
                    f"{lon_lat.getLon().asDegrees():.6f}",
                    f"{lon_lat.getLat().asDegrees():.6f}",
                ]
            record["s_region"] = " ".join(poly)
        else:
            _LOG.warning(f"Unexpected region type: {type(region)}")

    def _exposure_records(self, dimension_record: DimensionRecord, record: Dict[str, Any]) -> None:
        """Extract all needed info from a visit dimension record."""
        record["t_exptime"] = dimension_record.exposure_time
        record["target_name"] = dimension_record.target_name

    def _visit_records(self, dimension_record: DimensionRecord, record: Dict[str, Any]) -> None:
        """Extract all needed info from an exposure dimension record."""
        record["t_exptime"] = dimension_record.exposure_time
        record["target_name"] = dimension_record.target_name

    def _check_collections(self, collection_name: str) -> bool:
        """Check that specified collection (usually RUN or TAGGED) or any
        parent CHAINED collection is in the configured collection list.
        """

        def _all_collections(key: Any) -> Iterator[CollectionRecord]:
            """Return records of all chained collections."""
            for collection_record in self.collections.getParentChains(key):
                yield collection_record
                yield from _all_collections(collection_record.key)

        if collection_name in self.connection_names:
            return True

        collection_record = self.collections.find(collection_name)
        return any(record.name in self.connection_names for record in _all_collections(collection_record.key))

    def _exposure_region(self, dataId: DataCoordinate) -> Optional[Region]:
        """Return a Region for an exposure.

        This code tries to find a matching visit for an exposure and use the
        region from that visit.
        """
        visit_definition_storage = self.dimensions.get(self.universe["visit_definition"])
        if visit_definition_storage is None:
            return None
        exposureDataId = dataId.subset(self.exposure.graph)
        records = visit_definition_storage.fetch(DataCoordinateIterable.fromScalar(exposureDataId))
        # There may be more than one visit per exposure, they should nave the
        # same  region, so we use arbitrary one.
        record = next(iter(records), None)
        if record is None:
            return None
        visit: int = record.visit

        detector = cast(Dimension, self.universe["detector"])
        if detector in dataId:
            visit_detector_region_storage = self.dimensions.get(self.universe["visit_detector_region"])
            if visit_detector_region_storage is None:
                return None
            visitDataId = DataCoordinate.standardize(
                {
                    "instrument": dataId["instrument"],
                    "visit": visit,
                    "detector": dataId["detector"],
                },
                universe=self.universe,
            )
            records = visit_detector_region_storage.fetch(DataCoordinateIterable.fromScalar(visitDataId))
            record = next(iter(records), None)
            if record is not None:
                return record.region

        else:

            visit_storage = self.dimensions.get(self.visit)
            if visit_storage is None:
                return None
            visitDataId = DataCoordinate.standardize(
                {
                    "instrument": dataId["instrument"],
                    "visit": visit,
                },
                universe=self.universe,
            )
            records = visit_storage.fetch(DataCoordinateIterable.fromScalar(visitDataId))
            record = next(iter(records), None)
            if record is not None:
                return record.region

        return None

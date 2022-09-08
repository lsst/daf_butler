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

__all__ = ["ObsCoreLiveTableManager"]

import json
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Iterable, Iterator, List, Optional, Type, cast

import sqlalchemy
from lsst.daf.butler import (
    Config,
    DataCoordinate,
    DataCoordinateIterable,
    DatasetRef,
    Dimension,
    DimensionUniverse,
)
from lsst.sphgeom import Region

from ..interfaces import ObsCoreTableManager, VersionTuple
from ._config import ObsCoreConfig
from ._records import ExposureRegionFactory, RecordFactory
from ._schema import ObsCoreSchema

if TYPE_CHECKING:
    from ..interfaces import (
        CollectionManager,
        CollectionRecord,
        Database,
        DatasetRecordStorageManager,
        DimensionRecordStorageManager,
        StaticTablesContext,
    )

_VERSION = VersionTuple(0, 0, 1)


class _ExposureRegionFactory(ExposureRegionFactory):
    """Find exposure region from a matching visit dimensions records."""

    def __init__(self, dimensions: DimensionRecordStorageManager):
        self.dimensions = dimensions
        self.universe = dimensions.universe
        self.exposure = self.universe["exposure"]
        self.visit = self.universe["visit"]

    def exposure_region(self, dataId: DataCoordinate) -> Optional[Region]:
        # Docstring is inherited from a base class.
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


class ObsCoreLiveTableManager(ObsCoreTableManager):
    """A manager class for ObsCore table, implements methods for updating the
    records in that table.
    """

    def __init__(
        self,
        db: Database,
        table: sqlalchemy.schema.Table,
        schema: ObsCoreSchema,
        universe: DimensionUniverse,
        config: ObsCoreConfig,
        collections: CollectionManager,
        dimensions: DimensionRecordStorageManager,
    ):
        self.db = db
        self.table = table
        self.schema = schema
        self.universe = universe
        self.config = config
        self.collections = collections
        self.connection_names = frozenset(self.config.collections or [])
        exposure_region_factory = _ExposureRegionFactory(dimensions)
        self.record_factory = RecordFactory(config, schema, universe, exposure_region_factory)

    @classmethod
    def initialize(
        cls,
        db: Database,
        context: StaticTablesContext,
        *,
        universe: DimensionUniverse,
        config: Mapping,
        datasets: Type[DatasetRecordStorageManager],
        collections: CollectionManager,
        dimensions: DimensionRecordStorageManager,
    ) -> ObsCoreTableManager:
        # Docstring inherited from base class.
        config_data = Config(config)
        obscore_config = ObsCoreConfig.parse_obj(config_data)

        schema = ObsCoreSchema(config=obscore_config, datasets=datasets)
        table = context.addTable(obscore_config.table_name, schema.table_spec)
        return ObsCoreLiveTableManager(
            db=db,
            table=table,
            schema=schema,
            universe=universe,
            config=obscore_config,
            collections=collections,
            dimensions=dimensions,
        )

    def config_json(self) -> str:
        """Dump configuration in JSON format.

        Returns
        -------
        json : `str`
            Configuration serialized in JSON format.
        """
        return json.dumps(self.config.dict())

    @classmethod
    def currentVersion(cls) -> Optional[VersionTuple]:
        # Docstring inherited from base class.
        return _VERSION

    def schemaDigest(self) -> Optional[str]:
        # Docstring inherited from base class.
        return None

    def add_datasets(self, refs: Iterable[DatasetRef], collection: Optional[str] = None) -> None:
        # Docstring inherited from base class.
        records: List[dict] = []
        for ref in refs:

            # Check dataset run against configured collection list
            if self.connection_names:
                dataset_collection = collection
                if dataset_collection is None:
                    assert ref.run is not None, "Run cannot be None"
                    dataset_collection = ref.run
                if not self._check_dataset_run(dataset_collection):
                    continue

            if (record := self.record_factory(ref)) is not None:
                records.append(record)

        if records:
            # Ignore potential conflicts with existing datasets.
            self.db.ensure(self.table, *records, primary_key_only=True)

    def _check_dataset_run(self, collection_name: str) -> bool:
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

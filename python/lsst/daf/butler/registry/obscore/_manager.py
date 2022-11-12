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
import re
import uuid
from collections import defaultdict
from collections.abc import Collection, Mapping
from typing import TYPE_CHECKING, Dict, Iterable, List, Optional, Type, cast

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
from lsst.utils.iteration import chunk_iterable

from ..interfaces import ObsCoreTableManager, VersionTuple
from ._config import ConfigCollectionType, ObsCoreManagerConfig
from ._records import ExposureRegionFactory, Record, RecordFactory
from ._schema import ObsCoreSchema
from ._spatial import SpatialObsCorePlugin

if TYPE_CHECKING:
    from ..interfaces import (
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
        *,
        db: Database,
        table: sqlalchemy.schema.Table,
        schema: ObsCoreSchema,
        universe: DimensionUniverse,
        config: ObsCoreManagerConfig,
        dimensions: DimensionRecordStorageManager,
        spatial_plugins: Collection[SpatialObsCorePlugin],
    ):
        self.db = db
        self.table = table
        self.schema = schema
        self.universe = universe
        self.config = config
        self.spatial_plugins = spatial_plugins
        exposure_region_factory = _ExposureRegionFactory(dimensions)
        self.record_factory = RecordFactory(
            config, schema, universe, spatial_plugins, exposure_region_factory
        )
        self.tagged_collection: Optional[str] = None
        self.run_patterns: list[re.Pattern] = []
        if config.collection_type is ConfigCollectionType.TAGGED:
            assert (
                config.collections is not None and len(config.collections) == 1
            ), "Exactly one collection name required for tagged type."
            self.tagged_collection = config.collections[0]
        elif config.collection_type is ConfigCollectionType.RUN:
            if config.collections:
                for coll in config.collections:
                    try:
                        self.run_patterns.append(re.compile(coll))
                    except re.error as exc:
                        raise ValueError(f"Failed to compile regex: {coll!r}") from exc
        else:
            raise ValueError(f"Unexpected value of collection_type: {config.collection_type}")

    @classmethod
    def initialize(
        cls,
        db: Database,
        context: StaticTablesContext,
        *,
        universe: DimensionUniverse,
        config: Mapping,
        datasets: Type[DatasetRecordStorageManager],
        dimensions: DimensionRecordStorageManager,
    ) -> ObsCoreTableManager:
        # Docstring inherited from base class.
        config_data = Config(config)
        obscore_config = ObsCoreManagerConfig.parse_obj(config_data)

        # Instantiate all spatial plugins.
        spatial_plugins = SpatialObsCorePlugin.load_plugins(obscore_config.spatial_plugins, db)

        schema = ObsCoreSchema(config=obscore_config, spatial_plugins=spatial_plugins, datasets=datasets)

        # Generate table specification for main obscore table.
        table_spec = schema.table_spec
        for plugin in spatial_plugins:
            plugin.extend_table_spec(table_spec)
        table = context.addTable(obscore_config.table_name, schema.table_spec)

        return ObsCoreLiveTableManager(
            db=db,
            table=table,
            schema=schema,
            universe=universe,
            config=obscore_config,
            dimensions=dimensions,
            spatial_plugins=spatial_plugins,
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

    def add_datasets(self, refs: Iterable[DatasetRef]) -> int:
        # Docstring inherited from base class.

        # Only makes sense for RUN collection types
        if self.config.collection_type is not ConfigCollectionType.RUN:
            return 0

        obscore_refs: Iterable[DatasetRef]
        if self.run_patterns:
            # Check each dataset run against configured run list. We want to
            # reduce number of calls to _check_dataset_run, which may be
            # expensive. Normally references are grouped by run, if there are
            # multiple input references, they should have the same run.
            # Instead of just checking that, we group them by run again.
            refs_by_run: Dict[str, List[DatasetRef]] = defaultdict(list)
            for ref in refs:

                # Record factory will filter dataset types, but to reduce
                # collection checks we also pre-filter it here.
                if ref.datasetType.name not in self.config.dataset_types:
                    continue

                assert ref.run is not None, "Run cannot be None"
                refs_by_run[ref.run].append(ref)

            good_refs: List[DatasetRef] = []
            for run, run_refs in refs_by_run.items():
                if not self._check_dataset_run(run):
                    continue
                good_refs.extend(run_refs)
            obscore_refs = good_refs

        else:

            # Take all refs, no collection check.
            obscore_refs = refs

        return self._populate(obscore_refs)

    def associate(self, refs: Iterable[DatasetRef], collection: CollectionRecord) -> int:
        # Docstring inherited from base class.

        # Only works when collection type is TAGGED
        if self.tagged_collection is None:
            return 0

        if collection.name == self.tagged_collection:
            return self._populate(refs)
        else:
            return 0

    def disassociate(self, refs: Iterable[DatasetRef], collection: CollectionRecord) -> int:
        # Docstring inherited from base class.

        # Only works when collection type is TAGGED
        if self.tagged_collection is None:
            return 0

        count = 0
        if collection.name == self.tagged_collection:

            # Sorting may improve performance
            dataset_ids = sorted(cast(uuid.UUID, ref.id) for ref in refs)
            if dataset_ids:
                fk_field = self.schema.dataset_fk
                assert fk_field is not None, "Cannot be None by construction"
                # There may be too many of them, do it in chunks.
                for ids in chunk_iterable(dataset_ids):
                    where = self.table.columns[fk_field.name].in_(ids)
                    count += self.db.deleteWhere(self.table, where)

        return count

    def _populate(self, refs: Iterable[DatasetRef]) -> int:
        """Populate obscore table with the data from given datasets."""
        records: List[Record] = []
        for ref in refs:
            record = self.record_factory(ref)
            if record is not None:
                records.append(record)

        if records:
            # Ignore potential conflicts with existing datasets.
            return self.db.ensure(self.table, *records, primary_key_only=True)
        else:
            return 0

    def _check_dataset_run(self, run: str) -> bool:
        """Check that specified run collection matches know patterns."""

        if not self.run_patterns:
            # Empty list means take anything.
            return True

        # Try each pattern in turn.
        return any(pattern.fullmatch(run) for pattern in self.run_patterns)

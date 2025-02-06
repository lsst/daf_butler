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

__all__ = ["ObsCoreLiveTableManager"]

import re
import warnings
from collections import defaultdict
from collections.abc import Collection, Iterable, Iterator, Mapping
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

import sqlalchemy

from lsst.daf.butler import Config, DataCoordinate, DatasetRef, DimensionRecordColumnTag, DimensionUniverse
from lsst.daf.relation import Join
from lsst.sphgeom import Region
from lsst.utils.introspection import find_outside_stacklevel
from lsst.utils.iteration import chunk_iterable

from ..._column_type_info import ColumnTypeInfo
from ..interfaces import ObsCoreTableManager, VersionTuple
from ..queries import SqlQueryContext
from ._config import ConfigCollectionType, ObsCoreManagerConfig
from ._records import DerivedRegionFactory, Record, RecordFactory
from ._schema import ObsCoreSchema
from ._spatial import RegionTypeError, RegionTypeWarning, SpatialObsCorePlugin

if TYPE_CHECKING:
    from ..interfaces import (
        CollectionRecord,
        Database,
        DatasetRecordStorageManager,
        DimensionRecordStorageManager,
        StaticTablesContext,
    )

_VERSION = VersionTuple(0, 0, 1)


class _ExposureRegionFactory(DerivedRegionFactory):
    """Find exposure region from a matching visit dimensions records.

    Parameters
    ----------
    dimensions : `DimensionRecordStorageManager`
        The dimension records storage manager.
    """

    def __init__(self, dimensions: DimensionRecordStorageManager, context: SqlQueryContext):
        self.dimensions = dimensions
        self.universe = dimensions.universe
        self.exposure_dimensions = self.universe["exposure"].minimal_group
        self.exposure_detector_dimensions = self.universe.conform(["exposure", "detector"])
        self._context = context

    def derived_region(self, dataId: DataCoordinate) -> Region | None:
        # Docstring is inherited from a base class.
        context = self._context
        # Make a relation that starts with visit_definition (mapping between
        # exposure and visit).
        relation = context.make_initial_relation()
        if "visit_definition" not in self.universe.elements.names:
            return None
        relation = self.dimensions.join("visit_definition", relation, Join(), context)
        # Join in a table with either visit+detector regions or visit regions.
        if "detector" in dataId.dimensions:
            if "visit_detector_region" not in self.universe:
                return None
            relation = self.dimensions.join("visit_detector_region", relation, Join(), context)
            constraint_data_id = dataId.subset(self.exposure_detector_dimensions)
            region_tag = DimensionRecordColumnTag("visit_detector_region", "region")
        else:
            if "visit" not in self.universe:
                return None
            relation = self.dimensions.join("visit", relation, Join(), context)
            constraint_data_id = dataId.subset(self.exposure_dimensions)
            region_tag = DimensionRecordColumnTag("visit", "region")
        # Constrain the relation to match the given exposure and (if present)
        # detector IDs.
        relation = relation.with_rows_satisfying(
            context.make_data_coordinate_predicate(constraint_data_id, full=False)
        )
        # If we get more than one result (because the exposure belongs to
        # multiple visits), just pick an arbitrary one.
        relation = relation[:1]
        # Run the query and extract the region, if the query has any results.
        for row in context.fetch_iterable(relation):
            return row[region_tag]
        return None


class ObsCoreLiveTableManager(ObsCoreTableManager):
    """A manager class for ObsCore table, implements methods for updating the
    records in that table.

    Parameters
    ----------
    db : `Database`
        The active database.
    table : `sqlalchemy.schema.Table`
        The ObsCore table.
    schema : `ObsCoreSchema`
        The relevant schema.
    universe : `DimensionUniverse`
        The dimension universe.
    config : `ObsCoreManagerConfig`
        The config controlling the manager.
    dimensions : `DimensionRecordStorageManager`
        The storage manager for the dimension records.
    spatial_plugins : `~collections.abc.Collection` of `SpatialObsCorePlugin`
        Spatial plugins.
    registry_schema_version : `VersionTuple` or `None`, optional
        Version of registry schema.
    column_type_info : `ColumnTypeInfo`
        Information about column types that can differ between data
        repositories and registry instances.
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
        registry_schema_version: VersionTuple | None = None,
        column_type_info: ColumnTypeInfo,
    ):
        super().__init__(registry_schema_version=registry_schema_version)
        self.db = db
        self.table = table
        self.schema = schema
        self.universe = universe
        self.config = config
        self.spatial_plugins = spatial_plugins
        self._column_type_info = column_type_info
        exposure_region_factory = _ExposureRegionFactory(
            dimensions,
            SqlQueryContext(self.db, column_type_info),
        )
        self.record_factory = RecordFactory.get_record_type_from_universe(universe)(
            config, schema, universe, spatial_plugins, exposure_region_factory
        )
        self.tagged_collection: str | None = None
        self.run_patterns: list[re.Pattern] = []
        if config.collection_type is ConfigCollectionType.TAGGED:
            assert config.collections is not None and len(config.collections) == 1, (
                "Exactly one collection name required for tagged type."
            )
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

    def clone(self, *, db: Database, dimensions: DimensionRecordStorageManager) -> ObsCoreLiveTableManager:
        return ObsCoreLiveTableManager(
            db=db,
            table=self.table,
            schema=self.schema,
            universe=self.universe,
            config=self.config,
            dimensions=dimensions,
            # Current spatial plugins are safe to share without cloning -- they
            # are immutable and do not use their Database object outside of
            # 'initialize'.
            spatial_plugins=self.spatial_plugins,
            registry_schema_version=self._registry_schema_version,
            column_type_info=self._column_type_info,
        )

    @classmethod
    def initialize(
        cls,
        db: Database,
        context: StaticTablesContext,
        *,
        universe: DimensionUniverse,
        config: Mapping,
        datasets: type[DatasetRecordStorageManager],
        dimensions: DimensionRecordStorageManager,
        registry_schema_version: VersionTuple | None = None,
        column_type_info: ColumnTypeInfo,
    ) -> ObsCoreTableManager:
        # Docstring inherited from base class.
        config_data = Config(config)
        obscore_config = ObsCoreManagerConfig.model_validate(config_data)

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
            registry_schema_version=registry_schema_version,
            column_type_info=column_type_info,
        )

    def config_json(self) -> str:
        """Dump configuration in JSON format.

        Returns
        -------
        json : `str`
            Configuration serialized in JSON format.
        """
        return self.config.model_dump_json()

    @classmethod
    def currentVersions(cls) -> list[VersionTuple]:
        # Docstring inherited from base class.
        return [_VERSION]

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
            refs_by_run: dict[str, list[DatasetRef]] = defaultdict(list)
            for ref in refs:
                # Record factory will filter dataset types, but to reduce
                # collection checks we also pre-filter it here.
                if ref.datasetType.name not in self.config.dataset_types:
                    continue

                assert ref.run is not None, "Run cannot be None"
                refs_by_run[ref.run].append(ref)

            good_refs: list[DatasetRef] = []
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
            dataset_ids = sorted(ref.id for ref in refs)
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
        records: list[Record] = []
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

    def update_exposure_regions(self, instrument: str, region_data: Iterable[tuple[int, int, Region]]) -> int:
        # Docstring inherited from base class.
        instrument_column = self.schema.dimension_column("instrument")
        exposure_column = self.schema.dimension_column("exposure")
        detector_column = self.schema.dimension_column("detector")
        if instrument_column is None or exposure_column is None or detector_column is None:
            # Not all needed columns are in the table.
            return 0

        update_rows: list[Record] = []
        for exposure, detector, region in region_data:
            try:
                record = self.record_factory.make_spatial_records(region)
            except RegionTypeError as exc:
                warnings.warn(
                    f"Failed to convert region for exposure={exposure} detector={detector}: {exc}",
                    category=RegionTypeWarning,
                    stacklevel=find_outside_stacklevel("lsst.daf.butler"),
                )
                continue

            record.update(
                {
                    "instrument_column": instrument,
                    "exposure_column": exposure,
                    "detector_column": detector,
                }
            )
            update_rows.append(record)

        where_dict: dict[str, str] = {
            instrument_column: "instrument_column",
            exposure_column: "exposure_column",
            detector_column: "detector_column",
        }

        count = self.db.update(self.table, where_dict, *update_rows)
        return count

    @contextmanager
    def query(
        self, columns: Iterable[str | sqlalchemy.sql.expression.ColumnElement] | None = None, /, **kwargs: Any
    ) -> Iterator[sqlalchemy.engine.CursorResult]:
        # Docstring inherited from base class.
        if columns is not None:
            column_elements: list[sqlalchemy.sql.ColumnElement] = []
            for column in columns:
                if isinstance(column, str):
                    column_elements.append(self.table.columns[column])
                else:
                    column_elements.append(column)
            query = sqlalchemy.sql.select(*column_elements).select_from(self.table)
        else:
            query = self.table.select()

        if kwargs:
            query = query.where(
                sqlalchemy.sql.expression.and_(
                    *[self.table.columns[column] == value for column, value in kwargs.items()]
                )
            )
        with self.db.query(query) as result:
            yield result

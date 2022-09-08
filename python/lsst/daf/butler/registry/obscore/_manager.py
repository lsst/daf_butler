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
from typing import TYPE_CHECKING, Iterable, Optional, Type

import sqlalchemy
from lsst.daf.butler import Config, DatasetRef, DimensionUniverse

from ..interfaces import ObsCoreTableManager, VersionTuple
from ._config import ObsCoreConfig
from ._records import RecordFactory
from ._schema import ObsCoreSchema

if TYPE_CHECKING:
    from ..interfaces import (
        CollectionManager,
        Database,
        DatasetRecordStorageManager,
        DimensionRecordStorageManager,
        StaticTablesContext,
    )

_VERSION = VersionTuple(0, 0, 1)


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
        self.record_factory = RecordFactory(config, schema, universe, collections, dimensions)

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
        records = [self.record_factory(ref, collection) for ref in refs]
        actual_records = [record for record in records if record is not None]
        if records:
            # Ignore potential conflicts with existing datasets.
            self.db.ensure(self.table, *actual_records, primary_key_only=True)

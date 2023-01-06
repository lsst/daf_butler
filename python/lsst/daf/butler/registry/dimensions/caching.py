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

__all__ = ["CachingDimensionRecordStorage"]

from collections.abc import Mapping
from typing import Any

import sqlalchemy
from lsst.daf.relation import Join, Relation
from lsst.utils import doImportType

from ...core import (
    DatabaseDimensionElement,
    DataCoordinate,
    DimensionRecord,
    GovernorDimension,
    NamedKeyMapping,
)
from .. import queries
from ..interfaces import (
    Database,
    DatabaseDimensionRecordStorage,
    GovernorDimensionRecordStorage,
    StaticTablesContext,
)


class CachingDimensionRecordStorage(DatabaseDimensionRecordStorage):
    """A record storage implementation that adds caching to some other nested
    storage implementation.

    Parameters
    ----------
    nested : `DatabaseDimensionRecordStorage`
        The other storage to cache fetches from and to delegate all other
        operations to.
    """

    def __init__(self, nested: DatabaseDimensionRecordStorage):
        self._nested = nested
        self._cache: dict[DataCoordinate, DimensionRecord] | None = None

    @classmethod
    def initialize(
        cls,
        db: Database,
        element: DatabaseDimensionElement,
        *,
        context: StaticTablesContext | None = None,
        config: Mapping[str, Any],
        governors: NamedKeyMapping[GovernorDimension, GovernorDimensionRecordStorage],
        view_target: DatabaseDimensionRecordStorage | None = None,
    ) -> DatabaseDimensionRecordStorage:
        # Docstring inherited from DatabaseDimensionRecordStorage.
        config = config["nested"]
        NestedClass = doImportType(config["cls"])
        if not hasattr(NestedClass, "initialize"):
            raise TypeError(f"Nested class {config['cls']} does not have an initialize() method.")
        nested = NestedClass.initialize(
            db, element, context=context, config=config, governors=governors, view_target=view_target
        )
        if view_target is not None:
            # Caching records that are really a view into another element's
            # records is problematic, because the caching code has no way of
            # intercepting changes to its target's records.  Instead of
            # inventing a callback system to address that directly or dealing
            # with an untrustworthy combination, we just ban this combination.
            # But there's a problem: this is how we've configured the default
            # dimension universe from the beginning, with the 'band' dimension
            # being a cached view into physical_filter, and we don't want to
            # break all those configurations.
            if isinstance(view_target, CachingDimensionRecordStorage):
                # Happily, there's a way out: if the view target's record
                # storage is _also_ cached, then this outer caching is pretty
                # thoroughly unnecessary as well as problematic, and it's
                # reasonable to silently drop it, by returning the nested
                # storage object instead of a new caching wrapper.  And this
                # too is the case with the default dimension configuration.
                return nested
            raise RuntimeError(
                f"Invalid dimension storage configuration: cannot cache dimension element {element} "
                f"that is itself a view of {view_target.element}."
            )
        return cls(nested)

    @property
    def element(self) -> DatabaseDimensionElement:
        # Docstring inherited from DimensionRecordStorage.element.
        return self._nested.element

    def clearCaches(self) -> None:
        # Docstring inherited from DimensionRecordStorage.clearCaches.
        self._cache = None
        self._nested.clearCaches()

    def make_relation(self, context: queries.SqlQueryContext) -> Relation:
        # Docstring inherited.
        return self._nested.make_relation(context)

    def insert(self, *records: DimensionRecord, replace: bool = False, skip_existing: bool = False) -> None:
        # Docstring inherited from DimensionRecordStorage.insert.
        self._nested.insert(*records, replace=replace, skip_existing=skip_existing)
        if self._cache is not None:
            for record in records:
                # We really shouldn't ever get into a situation where the
                # record here differs from the one in the DB, but the last
                # thing we want is to make it harder to debug by making the
                # cache different from the DB.
                if skip_existing:
                    self._cache.setdefault(record.dataId, record)
                else:
                    self._cache[record.dataId] = record

    def sync(self, record: DimensionRecord, update: bool = False) -> bool | dict[str, Any]:
        # Docstring inherited from DimensionRecordStorage.sync.
        inserted_or_updated = self._nested.sync(record, update=update)
        if self._cache is not None and inserted_or_updated:
            self._cache[record.dataId] = record
        return inserted_or_updated

    def fetch_one(self, data_id: DataCoordinate, context: queries.SqlQueryContext) -> DimensionRecord | None:
        # Docstring inherited from DimensionRecordStorage.
        cache = self.get_record_cache(context)
        return cache.get(data_id)

    def get_record_cache(self, context: queries.SqlQueryContext) -> Mapping[DataCoordinate, DimensionRecord]:
        # Docstring inherited.
        if self._cache is None:
            relation = self._nested.join(
                context.make_initial_relation(),
                Join(),
                context,
            )
            reader = queries.DimensionRecordReader(self.element)
            cache: dict[DataCoordinate, DimensionRecord] = {}
            for row in context.fetch_iterable(relation):
                record = reader.read(row)
                cache[record.dataId] = record
            self._cache = cache
        return self._cache

    def digestTables(self) -> list[sqlalchemy.schema.Table]:
        # Docstring inherited from DimensionRecordStorage.digestTables.
        return self._nested.digestTables()

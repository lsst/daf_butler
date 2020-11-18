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

__all__ = ["QueryDimensionRecordStorage"]

from typing import Any, Iterable, Mapping, Optional

import sqlalchemy

from ...core import (
    DatabaseDimension,
    DatabaseDimensionElement,
    DataCoordinateIterable,
    Dimension,
    DimensionElement,
    DimensionRecord,
    GovernorDimension,
    LogicalColumnKey,
    LogicalTable,
    NamedKeyDict,
    NamedKeyMapping,
    NamedValueAbstractSet,
    SelectAdapter,
    SpatialRegionDatabaseRepresentation,
    TimespanDatabaseRepresentation,
)
from ..interfaces import (
    Database,
    DatabaseDimensionRecordStorage,
    GovernorDimensionRecordStorage,
    StaticTablesContext,
)
from ..queries import QueryBuilder


class QueryDimensionRecordStorage(DatabaseDimensionRecordStorage):
    """A read-only record storage implementation backed by SELECT query.

    At present, the only query this class supports is a SELECT DISTNCT over the
    table for some other dimension that has this dimension as an implied
    dependency.  For example, we can use this class to provide access to the
    set of ``band`` names referenced by any ``physical_filter``.

    Parameters
    ----------
    db : `Database`
        Interface to the database engine and namespace that will hold these
        dimension records.
    element : `DatabaseDimensionElement`
        The element whose records this storage will manage.
    """
    def __init__(self, db: Database, element: DatabaseDimensionElement, viewOf: str):
        assert isinstance(element, DatabaseDimension), \
            "An element cannot be a dependency unless it is a dimension."
        self._db = db
        self._element = element
        self._target = element.universe[viewOf]
        self._targetSpec = self._target.RecordClass.fields.makeTableSpec(
            RegionReprClass=self._db.getSpatialRegionRepresentation(),
            TimespanReprClass=self._db.getTimespanRepresentation(),
        )
        self._viewOf = viewOf
        self._query = None  # Constructed on first use.
        if element not in self._target.graph.dimensions:
            raise NotImplementedError("Query-backed dimension must be a dependency of its target.")
        if element.metadata:
            raise NotImplementedError("Cannot use query to back dimension with metadata.")
        if element.implied:
            raise NotImplementedError("Cannot use query to back dimension with implied dependencies.")
        if element.alternateKeys:
            raise NotImplementedError("Cannot use query to back dimension with alternate unique keys.")
        if element.spatial is not None:
            raise NotImplementedError("Cannot use query to back spatial dimension.")
        if element.temporal is not None:
            raise NotImplementedError("Cannot use query to back temporal dimension.")

    @classmethod
    def initialize(
        cls,
        db: Database,
        element: DatabaseDimensionElement, *,
        context: Optional[StaticTablesContext] = None,
        config: Mapping[str, Any],
        governors: NamedKeyMapping[GovernorDimension, GovernorDimensionRecordStorage],
    ) -> DatabaseDimensionRecordStorage:
        # Docstring inherited from DatabaseDimensionRecordStorage.
        viewOf = config["view_of"]
        return cls(db, element, viewOf)

    @property
    def element(self) -> DatabaseDimension:
        # Docstring inherited from DimensionRecordStorage.element.
        return self._element

    def clearCaches(self) -> None:
        # Docstring inherited from DimensionRecordStorage.clearCaches.
        pass

    def _ensureQuery(self) -> None:
        if self._query is None:
            targetTable = self._db.getExistingTable(self._target.name, self._targetSpec)
            assert targetTable is not None
            columns = []
            # The only columns for this dimension are ones for its required
            # dependencies and its own primary key (guaranteed by the checks in
            # the ctor).
            for dimension in self.element.required:
                if dimension == self.element:
                    columns.append(targetTable.columns[dimension.name].label(dimension.primaryKey.name))
                else:
                    columns.append(targetTable.columns[dimension.name].label(dimension.name))
            # This query doesn't do a SELECT DISTINCT, because that's confusing
            # and potentially wasteful if we apply a restrictive WHERE clause,
            # as SelectableDimensionRecordStorage.fetch will do.
            # Instead, we add DISTINCT in join() only.
            self._query = sqlalchemy.sql.select(
                columns, distinct=True
            ).select_from(
                targetTable
            ).alias(
                self.element.name
            )

    def join(
        self,
        builder: QueryBuilder, *,
        regions: Optional[NamedKeyDict[DimensionElement, SpatialRegionDatabaseRepresentation]] = None,
        timespans: Optional[NamedKeyDict[DimensionElement, TimespanDatabaseRepresentation]] = None,
    ) -> None:
        # Docstring inherited from DimensionRecordStorage.
        assert regions is None, "Should be guaranteed by constructor checks."
        assert timespans is None, "Should be guaranteed by constructor checks."
        if self._target in builder.summary.mustHaveKeysJoined:
            # Do nothing; the target dimension is already being included, so
            # joining against a subquery referencing it would just produce a
            # more complicated query that's guaranteed to return the same
            # results.
            return
        self._ensureQuery()
        joinOn = builder.startJoin(self._query, self.element.required,
                                   self.element.RecordClass.fields.required.names)
        builder.finishJoin(self._query, joinOn)
        return self._query

    def insert(self, *records: DimensionRecord) -> None:
        # Docstring inherited from DimensionRecordStorage.insert.
        raise TypeError(f"Cannot insert {self.element.name} records.")

    def sync(self, record: DimensionRecord) -> bool:
        # Docstring inherited from DimensionRecordStorage.sync.
        raise TypeError(f"Cannot sync {self.element.name} records.")

    def fetch(self, dataIds: DataCoordinateIterable) -> Iterable[DimensionRecord]:
        # Docstring inherited from DimensionRecordStorage.fetch.
        RecordClass = self.element.RecordClass
        for dataId in dataIds:
            # Given the restrictions imposed at construction, we know there's
            # nothing to actually fetch: everything we need is in the data ID.
            yield RecordClass(**dataId.byName())

    def digestTables(self) -> Iterable[sqlalchemy.schema.Table]:
        # Docstring inherited from DimensionRecordStorage.digestTables.
        return []

    def makeLogicalTable(self) -> LogicalTable:
        # Docstring inherited from DatabaseDimensionRecordStorage.
        targetTable = self._db.getExistingTable(self._target.name, self._targetSpec)
        return _DimensionSummaryQueryLogicalTable(self._element, targetTable)


class _DimensionSummaryQueryLogicalTable(LogicalTable):

    def __init__(self, element: DatabaseDimension, table: sqlalchemy.schema.Table):
        self._element = element
        self._table = table

    @property
    def name(self) -> str:
        return self._element.name

    @property
    def dimensions(self) -> NamedValueAbstractSet[Dimension]:
        return self._element.dimensions

    def select(
        self,
        columns: Optional[Iterable[LogicalColumnKey]] = None,
        adapter: Optional[SelectAdapter] = None,
    ) -> sqlalchemy.sql.Select:
        # There is actually only one column we can provide (the dimension key),
        # and caller guarantees that any requested columns in `select` or
        # `adapter.needed` are in `self.columns`, so we don't actually need to
        # look at what columns are requested.
        column = self._table.columns[self._element.name]
        sql = sqlalchemy.sql.select(
            [column.label(self._element.name)]
        ).select_from(
            self._table
        ).distinct()
        if adapter is not None:
            sql = adapter.apply(sql, {self._element: column})
        return sql

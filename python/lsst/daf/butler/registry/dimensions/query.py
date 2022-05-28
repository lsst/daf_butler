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

from typing import AbstractSet, Any, Iterable, Mapping, Optional

import sqlalchemy

from ...core import (
    DatabaseDimension,
    DatabaseDimensionElement,
    DataCoordinateIterable,
    DimensionRecord,
    GovernorDimension,
    NamedKeyMapping,
    sql,
)
from ..interfaces import (
    Database,
    DatabaseDimensionRecordStorage,
    GovernorDimensionRecordStorage,
    StaticTablesContext,
)


class QueryDimensionRecordStorage(DatabaseDimensionRecordStorage):
    """A read-only record storage implementation backed by SELECT query.

    At present, the only query this class supports is a SELECT DISTINCT over
    the table for some other dimension that has this dimension as an implied
    dependency.  For example, we can use this class to provide access to the
    set of ``band`` names referenced by any ``physical_filter``.

    Parameters
    ----------
    db : `Database`
        Interface to the database engine and namespace that will hold these
        dimension records.
    element : `DatabaseDimensionElement`
        The element whose records this storage will manage.
    viewOf : `str`
        Name of the dimension element this storage is a view of.
    column_types : `sql.ColumnTypeInfo`
        Information about column types that can differ between data
        repositories and registry instances, including the dimension universe.
    """

    def __init__(
        self,
        db: Database,
        element: DatabaseDimensionElement,
        viewOf: str,
        column_types: sql.ColumnTypeInfo,
    ):
        assert isinstance(
            element, DatabaseDimension
        ), "An element cannot be a dependency unless it is a dimension."
        self._db = db
        self._element = element
        self._target = element.universe[viewOf]
        self._targetSpec = self._target.RecordClass.fields.makeTableSpec(
            RegionReprClass=self._db.getSpatialRegionRepresentation(),
            TimespanReprClass=self._db.getTimespanRepresentation(),
        )
        self._viewOf = viewOf
        self._column_types = column_types
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
        element: DatabaseDimensionElement,
        *,
        context: Optional[StaticTablesContext] = None,
        config: Mapping[str, Any],
        governors: NamedKeyMapping[GovernorDimension, GovernorDimensionRecordStorage],
        column_types: sql.ColumnTypeInfo,
    ) -> DatabaseDimensionRecordStorage:
        # Docstring inherited from DatabaseDimensionRecordStorage.
        viewOf = config["view_of"]
        return cls(db, element, viewOf, column_types)

    @property
    def element(self) -> DatabaseDimension:
        # Docstring inherited from DimensionRecordStorage.element.
        return self._element

    def clearCaches(self) -> None:
        # Docstring inherited from DimensionRecordStorage.clearCaches.
        pass

    def join(
        self,
        relation: sql.Relation,
        columns: Optional[AbstractSet[str]] = None,
    ) -> sql.Relation:
        # Docstring inherited from DimensionRecordStorage.
        target_table = self._db.getExistingTable(self._target.name, self._targetSpec)
        assert target_table is not None
        builder = sql.Relation.build(
            sqlalchemy.sql.select(
                [target_table.columns[name].label(name) for name in self.element.required.names]
            )
            .select_from(target_table)
            .alias(self.element.name),
            self._column_types,
        )
        # The only columns for this dimension are ones for its required
        # dependencies and its own primary key (guaranteed by the checks in
        # the ctor).
        for name in self.element.required.names:
            builder.columns[sql.DimensionKeyColumnTag(name)] = builder.sql_from.columns[name]
        return relation.join(builder.finish().forced_unique())

    def insert(self, *records: DimensionRecord, replace: bool = False, skip_existing: bool = False) -> None:
        # Docstring inherited from DimensionRecordStorage.insert.
        raise TypeError(
            f"Cannot insert {self.element.name} records, define as part of {self._viewOf} instead."
        )

    def sync(self, record: DimensionRecord, update: bool = False) -> bool:
        # Docstring inherited from DimensionRecordStorage.sync.
        raise TypeError(f"Cannot sync {self.element.name} records, define as part of {self._viewOf} instead.")

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

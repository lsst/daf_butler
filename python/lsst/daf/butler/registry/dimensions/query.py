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

__all__ = ["QueryDimensionRecordStorage"]

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

import sqlalchemy
from lsst.daf.relation import Relation

from ..._column_tags import DimensionKeyColumnTag
from ..._named import NamedKeyMapping
from ...dimensions import (
    DatabaseDimension,
    DatabaseDimensionElement,
    DataCoordinate,
    DimensionRecord,
    GovernorDimension,
)
from ..interfaces import (
    Database,
    DatabaseDimensionRecordStorage,
    GovernorDimensionRecordStorage,
    StaticTablesContext,
)

if TYPE_CHECKING:
    from .. import queries


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
    view_target : `DatabaseDimensionRecordStorage`
        Storage object for the element this target's storage is a view of.
    """

    def __init__(
        self, db: Database, element: DatabaseDimensionElement, view_target: DatabaseDimensionRecordStorage
    ):
        assert isinstance(
            element, DatabaseDimension
        ), "An element cannot be a dependency unless it is a dimension."
        self._db = db
        self._element = element
        self._target = view_target
        if element.name not in self._target.element.minimal_group:
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
        context: StaticTablesContext | None = None,
        config: Mapping[str, Any],
        governors: NamedKeyMapping[GovernorDimension, GovernorDimensionRecordStorage],
        view_target: DatabaseDimensionRecordStorage | None = None,
    ) -> DatabaseDimensionRecordStorage:
        # Docstring inherited from DatabaseDimensionRecordStorage.
        assert view_target is not None, f"Storage for '{element}' is a view."
        return cls(db, element, view_target)

    @property
    def element(self) -> DatabaseDimension:
        # Docstring inherited from DimensionRecordStorage.element.
        return self._element

    def clearCaches(self) -> None:
        # Docstring inherited from DimensionRecordStorage.clearCaches.
        pass

    def make_relation(self, context: queries.SqlQueryContext) -> Relation:
        # Docstring inherited from DimensionRecordStorage.
        columns = DimensionKeyColumnTag.generate([self.element.name])
        return (
            self._target.make_relation(context)
            .with_only_columns(
                frozenset(columns),
                preferred_engine=context.preferred_engine,
                require_preferred_engine=True,
            )
            .without_duplicates()
        )

    def insert(self, *records: DimensionRecord, replace: bool = False, skip_existing: bool = False) -> None:
        # Docstring inherited from DimensionRecordStorage.insert.
        raise TypeError(
            f"Cannot insert {self.element.name} records, define as part of {self._target.element} instead."
        )

    def sync(self, record: DimensionRecord, update: bool = False) -> bool:
        # Docstring inherited from DimensionRecordStorage.sync.
        raise TypeError(
            f"Cannot sync {self.element.name} records, define as part of {self._target.element} instead."
        )

    def fetch_one(self, data_id: DataCoordinate, context: queries.SqlQueryContext) -> DimensionRecord | None:
        # Docstring inherited from DimensionRecordStorage.
        # Given the restrictions imposed at construction, we know there's
        # nothing to actually fetch: everything we need is in the data ID.
        return self.element.RecordClass(**data_id.required)

    def digestTables(self) -> list[sqlalchemy.schema.Table]:
        # Docstring inherited from DimensionRecordStorage.digestTables.
        return []

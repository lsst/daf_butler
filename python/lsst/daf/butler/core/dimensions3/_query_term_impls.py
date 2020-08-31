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
#

from __future__ import annotations

from typing import (
    Iterable,
    Mapping,
)

import sqlalchemy

from ..datasets import DatasetType
from ..named import NamedKeyDict, NamedKeyMapping
from ._relationships import (
    RelationshipCategory,
    RelationshipEndpoint,
    RelationshipFamily,
)
from ._elements import (
    Dimension,
    DimensionElement,
    DimensionGroup,
)
from ._queries import (
    LogicalTable,
    LogicalTableSelectParameters,
)
from ...registry import CollectionSearch
from ...registry.interfaces import DatasetRecordStorageManager


class DimensionElementQueryJoinTerm(LogicalTable):

    def __init__(self, element: DimensionElement, table: sqlalchemy.sql.FromClause):
        self.element = element
        self._table = table

    @property
    def dimensions(self) -> NamedKeyMapping[Dimension, str]:
        result: NamedKeyDict[Dimension, str] = NamedKeyDict()
        result.update((d, d.name) for d in self.element.requires)
        if isinstance(self.element, Dimension):
            result[self.element] = self.element.key_field_spec.name
        result.update((d, d.name) for d in self.element.implies)
        return result.freeze()

    @property
    def families(self) -> Mapping[RelationshipCategory, RelationshipFamily]:
        return self.element.families

    def select(self, spec: LogicalTableSelectParameters) -> sqlalchemy.sql.FromClause:
        return self._table


class DatasetTemporalFamily(RelationshipFamily):

    def __init__(self, dataset_type: DatasetType):
        super().__init__(dataset_type.name, RelationshipCategory.TEMPORAL)

    @property
    def minimal_dimensions(self) -> None:
        return None

    def choose(self, endpoints: Iterable[RelationshipEndpoint]) -> RelationshipEndpoint:
        try:
            (endpoint,) = endpoints
        except TypeError:
            raise RuntimeError(
                f"Got multiple temporal endpoints for calibration dataset {self.name}, which should be "
                "impossible.  This is probably a logic bug in query generation."
            )
        return endpoint


class DatasetQueryJoinTerm(LogicalTable):

    def __init__(self, dataset_type: DatasetType, collections: CollectionSearch,
                 manager: DatasetRecordStorageManager):
        self.dataset_type = dataset_type
        self._collections = collections
        self._manager = manager
        self._family = DatasetTemporalFamily(dataset_type) if dataset_type.isCalibration() else None

    @property
    def dimensions(self) -> NamedKeyMapping[Dimension, str]:
        return self.dataset_type.dimensions.required  # type: ignore

    @property
    def families(self) -> Mapping[RelationshipCategory, RelationshipFamily]:
        if not self.dataset_type.isCalibration():
            return {}
        else:
            assert self._family is not None
            return {RelationshipCategory.TEMPORAL: self._family}

    def select(self, spec: LogicalTableSelectParameters) -> sqlalchemy.sql.FromClause:
        raise NotImplementedError("TODO: copy from current QueryBuilder method.")


class DataCoordinateTableJoinTerm(LogicalTable):

    def __init__(self, dimensions: DimensionGroup, table: sqlalchemy.sql.FromClause):
        self._dimensions = dimensions
        self._table = table

    @property
    def dimensions(self) -> NamedKeyMapping[Dimension, str]:
        return NamedKeyDict((d, d.name) for d in self._dimensions)

    @property
    def families(self) -> Mapping[RelationshipCategory, RelationshipFamily]:
        return {}

    def select(self, spec: LogicalTableSelectParameters) -> sqlalchemy.sql.FromClause:
        assert not spec.relationships
        assert not spec.extra
        return self._table

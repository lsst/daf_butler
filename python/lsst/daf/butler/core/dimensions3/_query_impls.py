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

from contextlib import contextmanager
import dataclasses
from typing import (
    Any,
    Generator,
    Iterable,
    Mapping,
    Optional,
    Tuple,
    Union,
)

import sqlalchemy

from ..datasets import DatasetType
from .. import ddl
from ..named import NamedKeyDict, NamedKeyMapping, NamedValueAbstractSet
from ._relationships import (
    RelationshipCategory,
    RelationshipEndpoint,
    RelationshipEndpointKey,
    RelationshipFamily,
)
from ._elements import (
    Dimension,
    DimensionElement,
    DimensionGroup,
)
from ._queries import (
    IntersectAsNeeded,
    LogicalTable,
    LogicalTableSelectParameters,
    ManualLinks,
    QuerySpec,
    QueryWhereExpression,
    SelectableSqlWrapper,
)
from ...registry import CollectionSearch
from ...registry.interfaces import Database, DatasetRecordStorageManager


class DimensionElementQueryLogicalTable(LogicalTable):

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

    def to_sql(self, spec: LogicalTableSelectParameters) -> sqlalchemy.sql.FromClause:
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


class DatasetQueryLogicalTable(LogicalTable):

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

    def to_sql(self, spec: LogicalTableSelectParameters) -> sqlalchemy.sql.FromClause:
        raise NotImplementedError("TODO: copy from current QueryBuilder method.")


class DataCoordinateLogicalTable(LogicalTable):

    def __init__(self, dimensions: DimensionGroup, table: sqlalchemy.sql.FromClause):
        self._dimensions = dimensions
        self._table = table

    @property
    def dimensions(self) -> NamedKeyMapping[Dimension, str]:
        return NamedKeyDict((d, d.name) for d in self._dimensions)

    @property
    def families(self) -> Mapping[RelationshipCategory, RelationshipFamily]:
        return {}

    def to_sql(self, spec: LogicalTableSelectParameters) -> sqlalchemy.sql.FromClause:
        assert not spec.relationships
        assert not spec.extra
        return self._table


class DataCoordinateQuery:

    def __init__(
        self,
        managers: Any,
        *,
        dimensions: Optional[DimensionGroup] = None,
        spec: Optional[QuerySpec] = None,
        unique: bool = False,
        sql: Optional[SelectableSqlWrapper] = None,
        table_spec: Optional[ddl.TableSpec] = None,
    ):
        if spec is None:
            if dimensions is None:
                raise TypeError("Exactly one of 'spec' and 'dimensions' must be provided; got neither.")
            assert sql is None
            assert table_spec is None
            spec = QuerySpec(dimensions)
        elif dimensions is not None:
            raise TypeError("Exactly one of 'spec' and 'dimensions' must be provided; got both.")
        self._spec = spec
        self._is_unique = unique
        self._managers = managers
        self._sql = sql
        self._table_spec = table_spec

    @property
    def dimensions(self) -> DimensionGroup:
        return self._spec.requested_dimensions

    def _ensure_sql(self) -> SelectableSqlWrapper:
        if self._sql is None:
            self._sql = SelectableSqlWrapper.from_query(self._spec.build_sql(self._managers))
            if self._is_unique:
                self._sql = self._sql.unique()
        return self._sql

    def _ensure_table_spec(self) -> ddl.TableSpec:
        if self._table_spec is None:
            self._table_spec = ddl.TableSpec(fields=())
            for dimension in self.dimensions:
                self._table_spec.fields.add(
                    dataclasses.replace(dimension.key_field_spec, name=dimension.name)
                )
        return self._table_spec

    def _as_logical_table(self) -> LogicalTable:
        return DataCoordinateLogicalTable(self.dimensions, self._ensure_sql().as_from_clause())

    def unique(self) -> DataCoordinateQuery:
        if not self._is_unique:
            return DataCoordinateQuery(self._managers, spec=self._spec, unique=self._is_unique,
                                       sql=(self._sql.unique() if self._sql is not None else None),
                                       table_spec=self._table_spec)
        else:
            return self

    @contextmanager
    def materialize(self) -> Generator[DataCoordinateQuery, None, None]:
        db: Database = self._managers.db
        table_spec = self._ensure_table_spec()
        with self._ensure_sql().materialize(table_spec, db) as temp_table_sql:
            yield DataCoordinateQuery(
                self._managers,
                spec=self._spec,
                unique=self._is_unique,
                sql=temp_table_sql,
                table_spec=table_spec
            )

    def for_related_dimensions(self, dimensions: DimensionGroup) -> DataCoordinateQuery:
        if dimensions == self.dimensions:
            return self
        else:
            return DataCoordinateQuery(self._managers, dimensions=dimensions).related_to(self)

    def related_to(self, other: DataCoordinateQuery) -> DataCoordinateQuery:
        spec = dataclasses.replace(self._spec, fixed_tables=list(self._spec.fixed_tables))
        spec.fixed_tables.append(other._as_logical_table())
        return DataCoordinateQuery(self._managers, spec=spec)

    def with_intersection_endpoints(
        self,
        category: RelationshipCategory,
        **kwargs: RelationshipEndpointKey,
    ) -> DataCoordinateQuery:
        spec = dataclasses.replace(self._spec, link_generators=self._spec.link_generators.copy())
        link_generator = IntersectAsNeeded()
        link_generator.overrides.update(**kwargs)
        spec.link_generators[category] = link_generator
        return DataCoordinateQuery(self._managers, spec=spec)

    def with_manual_relationships(
        self,
        category: RelationshipCategory,
        links: Iterable[Tuple[RelationshipEndpointKey, RelationshipEndpointKey]],
    ) -> DataCoordinateQuery:
        spec = dataclasses.replace(self._spec, link_generators=self._spec.link_generators.copy())
        link_generator = ManualLinks()
        link_generator.links.update(links)
        spec.link_generators[category] = link_generator
        return DataCoordinateQuery(self._managers, spec=spec)

    def without_relationships(self, category: RelationshipCategory) -> DataCoordinateQuery:
        return self.with_manual_relationships(category, ())

    def where(self, expression: Union[QueryWhereExpression, str]) -> DataCoordinateQuery:
        if isinstance(expression, str):
            expression = QueryWhereExpression.from_str(expression)
        spec = dataclasses.replace(
            self._spec,
            where_expression=self._spec.where_expression & expression
        )
        return DataCoordinateQuery(self._managers, spec=spec)


class DatasetQuery:

    def __init__(
        self,
        managers: Any,
        parent_dataset_type: DatasetType,
        *,
        spec: Optional[QuerySpec] = None,
        collections: Optional[CollectionSearch] = None,
        components: Iterable[Optional[str]] = (None,),
        sql: Optional[SelectableSqlWrapper] = None,
    ):
        self._managers = managers
        self._parent_dataset_type = parent_dataset_type
        if spec is None:
            if collections is None:
                raise TypeError("Exactly one of 'spec' and 'collections' must be provided; got neither.")
            assert sql is None
            spec = QuerySpec(
                parent_dataset_type.dimensions,  # type: ignore
                fixed_tables=[DatasetQueryLogicalTable(parent_dataset_type, collections, managers.datasets)],
                select_extra={
                    "dataset_id": (parent_dataset_type.name, "id"),
                    "dataset_run": (parent_dataset_type.name, "run"),
                }
            )
        elif collections is not None:
            raise TypeError("Exactly one of 'spec' and 'collections' must be provided; got both.")
        self._spec = spec
        self._components = frozenset(components)
        self._sql = sql

    @property
    def parent_dataset_type(self) -> DatasetType:
        return self._parent_dataset_type

    @property
    def dataset_types(self) -> NamedValueAbstractSet[DatasetType]:
        raise NotImplementedError("TODO")

    def as_search(self) -> DatasetSearch:
        return DatasetSearch(self)

    def related_to(self, other: DataCoordinateQuery) -> DatasetQuery:
        spec = dataclasses.replace(self._spec, fixed_tables=list(self._spec.fixed_tables))
        spec.fixed_tables.append(other._as_logical_table())
        return DatasetQuery(self._managers, self._parent_dataset_type, spec=spec,
                            components=self._components)

    def with_intersection_endpoints(
        self,
        category: RelationshipCategory,
        **kwargs: RelationshipEndpointKey,
    ) -> DatasetQuery:
        spec = dataclasses.replace(self._spec, link_generators=self._spec.link_generators.copy())
        link_generator = IntersectAsNeeded()
        link_generator.overrides.update(**kwargs)
        spec.link_generators[category] = link_generator
        return DatasetQuery(self._managers, self._parent_dataset_type, spec=spec,
                            components=self._components)

    def with_manual_relationships(
        self,
        category: RelationshipCategory,
        links: Iterable[Tuple[RelationshipEndpointKey, RelationshipEndpointKey]],
    ) -> DatasetQuery:
        spec = dataclasses.replace(self._spec, link_generators=self._spec.link_generators.copy())
        link_generator = ManualLinks()
        link_generator.links.update(links)
        spec.link_generators[category] = link_generator
        return DatasetQuery(self._managers, self._parent_dataset_type, spec=spec,
                            components=self._components)

    def without_relationships(self, category: RelationshipCategory) -> DatasetQuery:
        return self.with_manual_relationships(category, ())

    def where(self, expression: QueryWhereExpression) -> DatasetQuery:
        spec = dataclasses.replace(self._spec, where_expression=self._spec.where_expression & where)
        return DatasetQuery(self._managers, self._parent_dataset_type, spec=spec,
                            components=self._components)


class DatasetSearch:

    def __init__(self, query: DatasetQuery):
        self._query = query

    @property
    def parent_dataset_type(self) -> DatasetType:
        return self._parent_dataset_type

    @property
    def dataset_types(self) -> NamedValueAbstractSet[DatasetType]:
        raise NotImplementedError("TODO")

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

from abc import ABC, abstractmethod
from collections import defaultdict
from contextlib import contextmanager
import dataclasses
import itertools
from typing import (
    AbstractSet,
    Callable,
    ContextManager,
    Dict,
    FrozenSet,
    Generator,
    Generic,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
)
import uuid

import sqlalchemy

from .. import ddl
from ..named import NamedKeyDict, NamedKeyMapping, NamedValueAbstractSet, NamedValueSet
from ...registry.interfaces import Database
from ._relationships import (
    RelationshipCategory,
    RelationshipEndpoint,
    RelationshipEndpointDatabaseRepresentation,
    RelationshipEndpointKey,
    RelationshipFamily,
    RelationshipLink,
)
from ._elements import (
    Dimension,
    DimensionElement,
    DimensionGroup,
    DimensionUniverse,
)


@dataclasses.dataclass
class LogicalTableSelectParameters:
    extra: Set[str]
    relationships: Dict[RelationshipCategory, Type[RelationshipEndpointDatabaseRepresentation]]

    def update(self, other: LogicalTableSelectParameters) -> None:
        self.extra.update(other.extra)
        self.relationships.update(other.relationships)


class LogicalTable(RelationshipEndpoint):

    @property
    @abstractmethod
    def dimensions(self) -> NamedKeyMapping[Dimension, str]:
        raise NotImplementedError()

    @property
    @abstractmethod
    def families(self) -> Mapping[RelationshipCategory, RelationshipFamily]:
        return {}

    @abstractmethod
    def to_sql(self, parameters: LogicalTableSelectParameters) -> sqlalchemy.sql.FromClause:
        raise NotImplementedError()


class RelationshipLinkGenerator(ABC):

    @abstractmethod
    def visit(
        self,
        endpoints: Iterable[RelationshipEndpoint],
        category: RelationshipCategory,
        is_needed: Callable[[NamedValueAbstractSet[Dimension]], bool],
    ) -> Iterator[RelationshipLink]:
        raise NotImplementedError()

    @abstractmethod
    def copy(self) -> RelationshipLinkGenerator:
        raise NotImplementedError()


class IntersectAsNeeded(RelationshipLinkGenerator):

    def __init__(self) -> None:
        self.overrides = {}

    def visit(
        self,
        endpoints: Iterable[RelationshipEndpoint],
        category: RelationshipCategory,
        is_needed: Callable[[NamedValueAbstractSet[Dimension]], bool],
    ) -> Iterator[RelationshipLink]:
        # Group endpoints by family.
        endpoints_by_family: NamedKeyDict[RelationshipFamily, NamedValueSet[RelationshipEndpoint]] \
            = NamedKeyDict()
        for endpoint in endpoints:
            family = endpoint.families.get(category)
            if family is not None and family.name not in self.overrides:
                endpoints_by_family.setdefault(family, NamedValueSet()).add(endpoint)
        # Select the best endpoint from each family.
        best_endpoints_by_family: NamedKeyDict[RelationshipFamily, RelationshipEndpoint] = NamedKeyDict()
        for family, endpoints in endpoints_by_family.items():
            override = self.overrides.get(family.name)
            if override is not None:
                best_endpoints_by_family[family] = endpoints[override]
            elif len(endpoints) == 1:
                (best_endpoints_by_family[family],) = endpoints
            else:
                best_endpoints_by_family[family] = family.choose(endpoints)
        # Yield combinatorial links that are needed according to the callback.
        for (f1, e1), (f2, e2) in itertools.combinations(best_endpoints_by_family.items(), 2):
            if f1.minimal_dimensions is not None and f2.minimal_dimensions is not None:
                link_minimal_dimensions = NamedValueSet(f1.minimal_dimensions | f2.minimal_dimensions)
                if is_needed(link_minimal_dimensions):
                    yield RelationshipLink(e1, e2)
            else:
                yield RelationshipLink(e1, e2)

    def copy(self) -> RelationshipLinkGenerator:
        result = IntersectAsNeeded()
        result.overrides.update(self.overrides)
        return result

    overrides: Dict[str, RelationshipEndpointKey]


class ManualLinks(RelationshipLinkGenerator):
    links: Set[Tuple[RelationshipEndpointKey, RelationshipEndpointKey]]

    def __init__(self) -> None:
        self.links = set()

    def visit(
        self,
        endpoints: Iterable[RelationshipEndpoint],
        category: RelationshipCategory,
        is_needed: Callable[[NamedValueAbstractSet[Dimension]], bool],
    ) -> Iterator[RelationshipLink]:
        # Convert input string/endpoint tuples to RelationshipLinks, and use a
        # set to deduplicate them.
        endpoints = NamedValueSet(endpoints)
        standardized_links = {
            RelationshipLink(endpoints[key1], endpoints[key2]) for key1, key2 in self.links
        }
        yield from standardized_links

    def copy(self) -> RelationshipLinkGenerator:
        result = ManualLinks()
        result.links.update(self.links)
        return result


class QueryWhereExpression(ABC):

    @staticmethod
    def from_str(expression: str) -> QueryWhereExpression:
        raise NotImplementedError("TODO")

    @abstractmethod
    def get_referenced_dimensions(self, universe: DimensionUniverse) -> NamedValueAbstractSet[Dimension]:
        """All dimensions referenced by the expression (including those whose
        names appear as keys in the dict returned by
        `get_reference_extra_columns`).
        """
        raise NotImplementedError()

    @abstractmethod
    def get_referenced_extra_columns(self) -> Mapping[str, Set[str]]:
        """All fields referenced by the expression, other than the primary keys
        of dimensions (`Mapping` mapping the name of a `LogicalTable` or
        `DimensionElement` to a `set` of field names).
        """
        raise NotImplementedError()

    def __and__(self, other: QueryWhereExpression) -> QueryWhereExpression:
        raise NotImplementedError("TODO")


class DimensionManager(ABC):

    @abstractmethod
    def make_logical_table_for_element(self, element: DimensionElement) -> Optional[LogicalTable]:
        raise NotImplementedError()

    @abstractmethod
    def make_logical_tables_for_link(
        self,
        edge: RelationshipLink,
        category: RelationshipCategory,
    ) -> Iterator[Tuple[LogicalTable, LogicalTableSelectParameters]]:
        raise NotImplementedError()


K = TypeVar("K")


class SupersetAccumulator(Generic[K]):

    def __init__(self, sets: Iterable[AbstractSet[K]] = ()):
        self._data: Set[FrozenSet[K]] = set()
        for s in sets:
            self.add(s)

    def __contains__(self, s: AbstractSet[K]) -> bool:
        for existing in self._data:
            if existing.issuperset(s):
                return True
            if existing.issubset(s):
                return False
        return False

    def add(self, s: AbstractSet[K]) -> None:
        to_drop: Set[FrozenSet[K]] = set()
        for existing in self._data:
            if existing.issuperset(s):
                return
            if existing.issubset(s):
                to_drop.add(existing)
        self._data -= to_drop
        self._data.add(frozenset(s))

    def __iter__(self) -> Iterator[AbstractSet[K]]:
        yield from self._data


@dataclasses.dataclass
class QuerySpec:

    @property
    def universe(self) -> DimensionUniverse:
        return self.requested_dimensions.universe

    @property
    def full_dimensions(self) -> DimensionGroup:
        names = set(self.requested_dimensions.names)
        names.update(self.where_expression.get_referenced_dimensions(self.universe).names)
        for table in self.fixed_tables:
            names.update(table.dimensions.names)
        return self.universe.group(names)

    def build_sql(self, manager: DimensionManager) -> sqlalchemy.sql.Select:
        # Stage 1: Compute the full set of logical tables that will go into the
        # query, and the columns we need from them.
        stage1 = _QueryBuilderStage1(self.full_dimensions, manager)
        # Fixed tables are always included.
        for table in self.fixed_tables:
            stage1.add_logical_table(table)
        del table
        # Relationships can bring in tables.  Depending on the relationship
        # categories and their representation the database, these can be
        # explicit join tables that represented precomputed relationships, or
        # tables with endpoints (e.g. regions, timespans) to relate on-the-fly.
        for category in RelationshipCategory.__members__.values():
            stage1.add_relationships(category, self.link_generators[category])
        # Ensure any tables with extra (i.e. non-dimension, non-relationship)
        # columns referenced by the WHERE clause are included.  That could be
        # a fixed table already added, or a dimension element table that we'll
        # add here.
        where_extra_columns = self.where_expression.get_referenced_extra_columns()
        for table_name, extra_columns in where_extra_columns.items():
            stage1.ensure_extra_columns(table_name, extra_columns)
        # Similarly ensure any tables with extra columns referenced by the
        # SELECT clause are included.  First group those by table name...
        select_extra_by_table: Dict[str, Set[str]] = defaultdict(set)
        for table_name, extra_column in self.select_extra.values():
            select_extra_by_table[table_name].add(extra_column)
        # ...then ensure those tables are added with those extra columns.
        for table_name, extra_columns in where_extra_columns.items():
            stage1.ensure_extra_columns(table_name, extra_columns)
        # Finally add any dimension elements whose keys or relationships
        # are not already covered by other tables we've included.
        parameters_by_table = stage1.finish()

        # Stage 2: Actually build the query with SQLAlchemy objects.
        # Now that we know the tables we'll join in, this is fairly simple;
        # it's just translating the data structures in the QuerySpec and
        # stage 1 builder into SQL code.
        stage2 = _QueryBuilderStage2()
        for table, parameters in parameters_by_table.items():
            stage2.join(table, parameters)
        return stage2.finish(self.requested_dimensions, self.select_extra, self.where_expression)

    requested_dimensions: DimensionGroup
    link_generators: Dict[RelationshipCategory, RelationshipLinkGenerator] = dataclasses.field(
        default_factory=lambda: defaultdict(IntersectAsNeeded)
    )
    select_extra: Dict[str, Tuple[str, str]] = dataclasses.field(default_factory=dict)
    where_expression: QueryWhereExpression = dataclasses.field(default_factory=QueryWhereExpression)
    fixed_tables: List[LogicalTable] = dataclasses.field(default_factory=list)


class _QueryBuilderStage1:

    def __init__(self, dimensions: DimensionGroup, manager: DimensionManager) -> None:
        self._dimensions = dimensions
        self._manager = manager
        self._parameters_by_table: NamedKeyDict[LogicalTable, LogicalTableSelectParameters] = NamedKeyDict()
        self._dimension_sets: SupersetAccumulator[str] = SupersetAccumulator()

    def add_logical_table(self, table: LogicalTable) -> LogicalTableSelectParameters:
        parameters = self._parameters_by_table.get(table)
        if parameters is None:
            parameters = LogicalTableSelectParameters(
                extra=set(),
                relationships={},
            )
            self._parameters_by_table[table] = parameters
            self._dimension_sets.add(table.dimensions.names)
        return parameters

    def is_dimension_set_needed(self, dimensions: NamedValueAbstractSet[Dimension]) -> bool:
        return dimensions.names in self._dimension_sets

    def add_relationships(
        self,
        category: RelationshipCategory,
        link_generator: RelationshipLinkGenerator
    ) -> None:
        link_iter = link_generator.visit(
            itertools.chain(self._parameters_by_table.keys(), self._dimensions.elements),
            category,
            self.is_dimension_set_needed,
        )
        for link in link_iter:
            for table, parameters in self._manager.make_logical_tables_for_link(link, category):
                self.add_logical_table(table).update(parameters)

    def ensure_extra_columns(self, table_name: str, extra: AbstractSet[str]) -> None:
        if table_name in self._parameters_by_table.names:
            self._parameters_by_table[table_name].extra.update(extra)
        elif table_name in self._dimensions.elements.names:
            element = self._dimensions.elements[table_name]
            element_table = self._manager.make_logical_table_for_element(element)
            if element_table is None:
                raise RuntimeError(f"WHERE clause references {table_name} value(s) {extra}, but there "
                                   "is no table for this element.")
            self.add_logical_table(element_table)

    def finish(self) -> NamedKeyMapping[LogicalTable, LogicalTableSelectParameters]:
        for element in reversed(list(self._dimensions.elements)):
            element_table = self._manager.make_logical_table_for_element(element)
            if element_table is not None and element_table.dimensions.names in self._dimension_sets:
                self.add_logical_table(element_table)
        return self._parameters_by_table.freeze()


class _QueryBuilderStage2:

    def __init__(self) -> None:
        self._dimension_columns: Dict[str, List[sqlalchemy.sql.ColumnElement]] = defaultdict(list)
        self._endpoint_reprs: Dict[RelationshipCategory, List[RelationshipEndpointDatabaseRepresentation]] \
            = defaultdict(list)
        self._table_sql: Dict[str, sqlalchemy.sql.FromClause] = {}
        self._from_clause: Optional[sqlalchemy.sql.FromClause] = None

    def join(self, table: LogicalTable, parameters: LogicalTableSelectParameters) -> None:
        sql = table.to_sql(parameters)
        join_on: List[sqlalchemy.sql.ColumnElement] = []
        for dimension, column_name in table.dimensions.items():
            joined_columns = self._dimension_columns[dimension.name]
            join_on.extend((sql.columns[column_name] == c for c in joined_columns))
            joined_columns.append(sql.columns[column_name])
        for category, endpoint_repr_type in parameters.relationships.items():
            joined_reprs = self._endpoint_reprs[category]
            endpoint_repr = endpoint_repr_type.fromSelectable(sql)
            join_on.extend((endpoint_repr.relate(r) for r in joined_reprs))
            joined_reprs.append(endpoint_repr)
        if self._from_clause is None:
            assert not join_on
            self._from_clause = sql
        else:
            join_on_expr = sqlalchemy.sql.and_(*join_on) if join_on else sqlalchemy.sql.literal(True)
            self._from_clause = self._from_clause.join(
                sql,
                joinon=join_on_expr
            )
        self._table_sql[table.name] = sql

    def finish(
        self,
        dimensions: DimensionGroup,
        select_extra: Mapping[str, Tuple[str, str]],
        where: QueryWhereExpression
    ) -> sqlalchemy.sql.Select:
        select_columns = [
            self._dimension_columns[d.name][-1].label(d.name)
            for d in dimensions
        ]
        select_columns.extend(
            self._table_sql[table_name].columns[column_name].label(label)
            for label, (table_name, column_name) in select_extra.items()
        )
        return sqlalchemy.sql.select(select_columns).select_from(self._from_clause)  # TODO: where clause


class SelectableSqlWrapper(ABC):

    __slots__ = ("_is_unique",)

    def __init__(self, is_unique: bool):
        self._is_unique = is_unique

    @staticmethod
    def from_query(sql: sqlalchemy.sql.Select, *, name: Optional[str] = None) -> SelectableSqlWrapper:
        return _SelectQuerySqlWrapper(sql, name=name)

    @staticmethod
    def from_table(sql: sqlalchemy.schema.Table) -> SelectableSqlWrapper:
        return _TableSqlWrapper(sql)

    @abstractmethod
    def as_from_clause(self) -> sqlalchemy.sql.FromClause:
        raise NotImplementedError()

    @abstractmethod
    def as_select_query(self) -> sqlalchemy.sql.Select:
        raise NotImplementedError()

    def is_unique(self) -> bool:
        return self._is_unique

    def unique(self) -> SelectableSqlWrapper:
        if self._is_unique:
            return self
        else:
            return self.from_query(self.as_select_query().distinct())

    @abstractmethod
    def materialize(
        self,
        table_spec: ddl.TableSpec,
        db: Database
    ) -> ContextManager[SelectableSqlWrapper]:
        raise NotImplementedError()


class _SelectQuerySqlWrapper(SelectableSqlWrapper):

    __slots__ = ("_sql", "_name",)

    def __init__(self, sql: sqlalchemy.sql.Select, *, name: Optional[str] = None,
                 is_unique: bool = False):
        super().__init__(is_unique)
        self._sql = sql
        self._name = name

    def as_from_clause(self) -> sqlalchemy.sql.FromClause:
        if self._name is None:
            name = f"qry_{uuid.uuid4().hex}"
        else:
            name = self._name
        return self._sql.alias(name)

    def as_select_query(self) -> sqlalchemy.sql.Select:
        return self._sql

    @contextmanager
    def materialize(
        self,
        table_spec: ddl.TableSpec,
        db: Database
    ) -> Generator[SelectableSqlWrapper, None, None]:
        table = db.makeTemporaryTable(table_spec, name=self._name)
        db.insert(table, select=self.as_select_query(), names=table_spec.fields.names)
        yield _TableSqlWrapper(table, is_unique=self.is_unique())
        db.dropTemporaryTable(table)


class _TableSqlWrapper(SelectableSqlWrapper):

    __slots__ = ("_sql",)

    def __init__(self, sql: sqlalchemy.schema.Table, is_unique: bool = False):
        super().__init__(is_unique)
        self._sql = sql

    def as_from_clause(self) -> sqlalchemy.sql.FromClause:
        return self._sql

    def as_select_query(self) -> sqlalchemy.sql.Select:
        return self._sql.select()

    @contextmanager
    def materialize(
        self,
        table_spec: ddl.TableSpec,
        db: Database
    ) -> Generator[SelectableSqlWrapper, None, None]:
        yield self

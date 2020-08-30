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
from dataclasses import dataclass
import itertools
from typing import (
    AbstractSet,
    Callable,
    FrozenSet,
    Generic,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Set,
    TypeVar,
)

import sqlalchemy

from ..named import NamedKeyDict, NamedKeyMapping, NamedValueAbstractSet
from ..timespan import DatabaseTimespanRepresentation
from ._relationships import (
    RelationshipCategory,
    RelationshipEndpoint,
    RelationshipFamily,
    RelationshipLink,
)
from ._elements import (
    Dimension,
    DimensionElement,
    DimensionGroup,
    DimensionUniverse,
)


@dataclass
class QueryJoinTermSelectResult:
    selectable: sqlalchemy.sql.FromClause
    dimensions: NamedKeyMapping[Dimension, sqlalchemy.sql.ColumnElement]
    region: Optional[sqlalchemy.sql.ColumnElement]
    timespan: Optional[DatabaseTimespanRepresentation]
    extra: Mapping[str, sqlalchemy.sql.ColumnElement]


@dataclass
class QueryJoinTermSelectSpec:
    extra: Set[str]
    relationships: Set[RelationshipCategory]


class QueryJoinTerm(RelationshipEndpoint):

    @property
    @abstractmethod
    def dimensions(self) -> NamedValueAbstractSet[Dimension]:
        raise NotImplementedError()

    @property
    def families(self) -> Mapping[RelationshipCategory, RelationshipFamily]:
        return {}

    @abstractmethod
    def select(self, spec: QueryJoinTermSelectSpec) -> QueryJoinTermSelectResult:
        raise NotImplementedError()


class RelationshipLinkGenerator(ABC):

    @abstractmethod
    def visit(
        self,
        endpoints: Iterable[RelationshipEndpoint],
        category: RelationshipCategory,
        is_needed: Callable[[RelationshipLink], bool],
    ) -> Iterator[RelationshipLink]:
        raise NotImplementedError()


class IntersectAsNeeded(RelationshipLinkGenerator):
    overrides: NamedKeyMapping[RelationshipFamily, QueryJoinTerm]

    def visit(
        self,
        endpoints: Iterable[RelationshipEndpoint],
        category: RelationshipCategory,
        is_needed: Callable[[RelationshipLink], bool],
    ) -> Iterator[RelationshipLink]:
        # Group endpoints by family.
        endpoints_by_family: NamedKeyDict[RelationshipFamily, List[RelationshipEndpoint]] = NamedKeyDict()
        for endpoint in endpoints:
            family = endpoint.families.get(category)
            if family is not None and family not in self.overrides:
                endpoints_by_family.setdefault(family, []).append(endpoint)
        # Select the best endpoint from each family.
        best_endpoints: List[RelationshipEndpoint] = []
        for family, endpoints in endpoints_by_family.items():
            override = self.overrides.get(family)
            if override is not None:
                best_endpoints.append(override)
            elif len(endpoints) == 1:
                best_endpoints.append(endpoints[0])
            else:
                best_endpoints.append(family.choose(endpoints))
        # Yield combinatorial links that are needed according to the callback.
        for endpoint1, endpoint2 in itertools.combinations(best_endpoints, 2):
            link = RelationshipLink(endpoint1, endpoint2)
            if is_needed(link):
                yield link


class ManualLinks(RelationshipLinkGenerator):
    links: Set[RelationshipLink]

    def visit(
        self,
        endpoints: Iterable[RelationshipEndpoint],
        category: RelationshipCategory,
        is_needed: Callable[[RelationshipLink], bool],
    ) -> Iterator[RelationshipLink]:
        yield from self.links


class QueryWhereExpression:

    dimensions: NamedValueAbstractSet[Dimension]
    """All dimensions referenced by the expression, including dimensions
    recursively referenced by the keys of `metadata` (`NamedValueAbstractSet`
    of `Dimension`).
    """

    extra: NamedKeyMapping[QueryJoinTerm, Set[str]]
    """All fields referenced by the expression, other than the primary keys of
    dimensions (`NamedKeyMapping` mapping `QueryJoinTerm` to a `set` of field
    names).
    """


class DimensionManager(ABC):

    @abstractmethod
    def make_query_join_term_for_element(self, element: DimensionElement) -> Optional[QueryJoinTerm]:
        raise NotImplementedError()

    @abstractmethod
    def make_query_join_terms_for_link(
        self,
        edge: RelationshipLink,
        category: RelationshipCategory,
    ) -> Iterator[QueryJoinTerm]:
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


class QuerySpec:

    @property
    def universe(self) -> DimensionUniverse:
        return self.requested_dimensions.universe

    @property
    def full_dimensions(self) -> DimensionGroup:
        names = set(self.requested_dimensions.names)
        names.update(self.where_expression.dimensions.names)
        for join_term in self.fixed_join_terms:
            names.update(join_term.dimensions.names)
        return self.universe.group(names)

    def compute_select_specs(
        self,
        manager: DimensionManager,
    ) -> NamedKeyMapping[QueryJoinTerm, QueryJoinTermSelectSpec]:
        result: NamedKeyDict[QueryJoinTerm, QueryJoinTermSelectSpec] = NamedKeyDict()
        dimension_sets_seen: SupersetAccumulator[str] = SupersetAccumulator()

        def add_join_term(v: QueryJoinTerm) -> QueryJoinTermSelectSpec:
            spec = result.get(v)
            if spec is None:
                spec = QueryJoinTermSelectSpec(
                    extra=self.where_expression.extra.get(v, ()),
                    relationships=set(),
                )
                result[v] = spec
                dimension_sets_seen.add(v.dimensions.names)
            return spec

        join_term: Optional[QueryJoinTerm]

        for join_term in self.fixed_join_terms:
            add_join_term(join_term)
        del join_term

        for join_term in self.where_expression.extra.keys():
            add_join_term(join_term)
        del join_term

        def is_link_needed(ln: RelationshipLink) -> bool:
            if isinstance(ln[0], DimensionElement) and isinstance(ln[1], DimensionElement):
                link_dimension_set = ln[0].requires.names | ln[1].requires.names
                return link_dimension_set not in dimension_sets_seen
            else:
                return True

        for category in RelationshipCategory.__members__.values():
            link_iter = self.link_generators[category].visit(
                itertools.chain(self.fixed_join_terms, self.full_dimensions.elements),
                category,
                is_link_needed
            )
            for link in link_iter:
                for join_term in manager.make_query_join_terms_for_link(link, category):
                    add_join_term(join_term).relationships.add(category)
        del join_term

        for element in reversed(list(self.full_dimensions.elements)):
            join_term = manager.make_query_join_term_for_element(element)
            if join_term is not None and join_term.dimensions.names not in dimension_sets_seen:
                add_join_term(join_term)
        del join_term

        return result.freeze()

    requested_dimensions: DimensionGroup
    link_generators: Mapping[RelationshipCategory, RelationshipLinkGenerator]
    where_expression: QueryWhereExpression
    fixed_join_terms: NamedValueAbstractSet[QueryJoinTerm]

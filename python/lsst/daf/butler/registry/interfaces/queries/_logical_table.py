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

__all__ = (
    "LogicalTable",
    "LogicalTableFactory",
)

import itertools
from abc import ABC, abstractmethod
from typing import AbstractSet, Iterable, Iterator

from lsst.utils.classes import cached_getter

from ....core import DatasetType, DimensionGraph, DimensionUniverse
from ....core.named import NamedValueAbstractSet, NamedValueSet
from ..._defaults import RegistryDefaults
from ...summaries import GovernorDimensionRestriction
from ._column_tags import ColumnTag
from ._construction_data import QueryConstructionDataRequest, QueryConstructionDataResult


class LogicalTableFactory(ABC):
    @property
    @abstractmethod
    def data_requested(self) -> QueryConstructionDataRequest:
        raise NotImplementedError()

    @abstractmethod
    def make_logical_table(
        self,
        data: QueryConstructionDataResult,
        columns_requested: AbstractSet[ColumnTag],
        *,
        defaults: RegistryDefaults,
        universe: DimensionUniverse,
    ) -> LogicalTable:
        raise NotImplementedError()


class LogicalTable(ABC):
    @staticmethod
    def without_rows(*messages: str, universe: DimensionUniverse) -> LogicalTable:
        return _NoRowsLogicalTable(tuple(messages), universe=universe)

    @property
    @abstractmethod
    def dimensions(self) -> DimensionGraph:
        """The dimensions constrained by this logical table
        (`DimensionGraph`).

        At least the required dimensions are included in the provided columns.
        """
        raise NotImplementedError()

    @property
    def governor_dimension_restriction(self) -> GovernorDimensionRestriction:
        """The governor dimension values required by this logical table
        (`GovernorDimensionRestriction`).
        """
        return GovernorDimensionRestriction.makeFull()

    @property  # type: ignore
    @cached_getter
    def dataset_types(self) -> NamedValueAbstractSet[DatasetType]:
        """Dataset types joined into this table."""
        return NamedValueSet().freeze()

    @property
    def is_doomed(self) -> bool:
        """Whether queries that include this logical table via a join are known
        in advance to return no results (`bool`).
        """
        return False

    def diagnostics(self, verbose: bool = False) -> Iterable[str]:
        """Diagnostic messages that indicate why this logical table is
        doomed.

        Parameters
        ----------
        verbose : `bool`, optional
            If `True` include messages for all filters being applied at
            query-generation time, even if they do not necessarily doom the
            query.  If `False` (default) only include messages that doom the
            query.

        Returns
        -------
        messages : `Iterable` [ `str` ]
            Diagnostic messages.  May be (but need not be) a single-pass
            iterator.
        """
        return ()

    def join(*args: LogicalTable, universe: DimensionUniverse) -> LogicalTable:  # noqa: N805
        """Join logical tables on dimensions they have in common.

        Parameters
        ----------
        *args : `LogicalTable`
            Logical tables to intersect.  If empty (only possible if this
            method is called on the type), a special logical table that has
            one row and no columns is returned.  If only one argument is
            passed, it is returned directly.
        universe : `DimensionUniverse`
            Object managing all dimension definitions and their relationships.

        Returns
        -------
        join : `LogicalTable`
            Join logical table.  Will just be ``self`` if no arguments are
            passed.
        """
        if not args:
            return _NoColumnsLogicalTable(universe)
        first, *rest = itertools.chain.from_iterable(arg.flatten_joins() for arg in args)
        if not rest:
            return first
        any_changed = True
        while any_changed:
            dimensions = first.dimensions.union(*[other.dimensions for other in rest])
            governor_dimension_restriction = first.governor_dimension_restriction.union(
                *[other.governor_dimension_restriction for other in rest]
            )
            restricted_first, any_changed = first.restricted_to(dimensions, governor_dimension_restriction)
            terms = [restricted_first]
            for term in rest:
                restricted_term, this_changed = term.restricted_to(dimensions, governor_dimension_restriction)
                any_changed = any_changed or this_changed
            first, *rest = itertools.chain.from_iterable(term.flatten_joins() for term in terms)
        return _DimensionJoin(first, *rest, universe=universe)

    def join_missing(
        self, columns_requested: AbstractSet[ColumnTag], defaults: RegistryDefaults
    ) -> LogicalTable:
        raise NotImplementedError("TODO")

    def flatten_joins(self) -> Iterator[LogicalTable]:
        yield self

    def insert_join(self, *others: LogicalTable) -> LogicalTable:
        return self.join(*others, universe=self.dimensions.universe)

    def restricted_to(
        self, dimensions: DimensionGraph, restriction: GovernorDimensionRestriction
    ) -> tuple[LogicalTable, bool]:
        return self, False

    @property
    @abstractmethod
    def columns_provided(self) -> AbstractSet[ColumnTag]:
        raise NotImplementedError()


class _NoRowsLogicalTable(LogicalTable):
    def __init__(self, messages: tuple[str, ...], *, universe: DimensionUniverse):
        self._messages = messages
        self._universe = universe

    @property
    def dimensions(self) -> DimensionGraph:
        return self._universe.empty

    @property
    def governor_dimension_restriction(self) -> GovernorDimensionRestriction:
        return GovernorDimensionRestriction.makeFull()

    @property
    def is_doomed(self) -> bool:
        return True

    def diagnostics(self, verbose: bool = False) -> Iterable[str]:
        return self._messages

    @property
    def columns_provided(self) -> AbstractSet[ColumnTag]:
        return frozenset()


class _NoColumnsLogicalTable(LogicalTable):
    def __init__(self, universe: DimensionUniverse):
        self._universe = universe

    @property
    def dimensions(self) -> DimensionGraph:
        return self._universe.empty

    @property
    def governor_dimension_restriction(self) -> GovernorDimensionRestriction:
        return GovernorDimensionRestriction.makeFull()

    @property
    def is_doomed(self) -> bool:
        return False

    def diagnostics(self, verbose: bool = False) -> Iterable[str]:
        return ()

    @property
    def columns_provided(self) -> AbstractSet[ColumnTag]:
        return frozenset()


class _DimensionJoin(LogicalTable):
    def __init__(self, *terms: LogicalTable, universe: DimensionUniverse):
        self._universe = universe
        self._terms = terms

    def flatten_joins(self) -> Iterator[LogicalTable]:
        yield from self._terms

    @property  # type: ignore
    @cached_getter
    def dimensions(self) -> DimensionGraph:
        return self._universe.empty.union(*[t.dimensions for t in self._terms])

    @property  # type: ignore
    @cached_getter
    def governor_dimension_restriction(self) -> GovernorDimensionRestriction:
        return GovernorDimensionRestriction.makeFull().intersection(
            *[t.governor_dimension_restriction for t in self._terms]
        )

    @property  # type: ignore
    @cached_getter
    def dataset_types(self) -> NamedValueAbstractSet[DatasetType]:
        result = NamedValueSet[DatasetType]()
        for term in self._terms:
            result.update(term.dataset_types)
        return result.freeze()

    @property  # type: ignore
    @cached_getter
    def is_doomed(self) -> bool:
        if any(t.is_doomed for t in self._terms):
            return True
        if self.governor_dimension_restriction.dooms_on(self.dimensions):
            return True
        return False

    def diagnostics(self, verbose: bool = False) -> Iterable[str]:
        for t in self._terms:
            yield from t.diagnostics(verbose=verbose)
        for g in self.governor_dimension_restriction.dooms_on(self.dimensions):
            yield f"Query terms put disjoint constraints on {g.name} values:"
            for t in self._terms:
                if (contribution := t.governor_dimension_restriction.get(g)) is not None:
                    yield f"  {t} requires {g.name} in {contribution}."

    @property
    def columns_provided(self) -> AbstractSet[ColumnTag]:
        columns: set[ColumnTag] = set()
        for term in self._terms:
            columns.update(term.columns_provided)
        return frozenset(columns)

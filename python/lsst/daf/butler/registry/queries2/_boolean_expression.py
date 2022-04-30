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
    "BooleanExpression",
    "BooleanExpressionFactory",
)

from typing import AbstractSet, Any, Iterable, Optional

from lsst.utils.classes import cached_getter

from ...core import DataCoordinate, DataId, DatasetType, DimensionGraph, DimensionUniverse
from ...core.named import NamedValueAbstractSet
from .._defaults import RegistryDefaults
from .._exceptions import UserExpressionError
from ..interfaces.queries import (
    ColumnTag,
    DimensionKeyColumnTag,
    DimensionRecordColumnTag,
    LogicalTable,
    LogicalTableFactory,
    QueryConstructionDataRequest,
    QueryConstructionDataResult,
)
from ..queries import expressions as expr
from ..summaries import GovernorDimensionRestriction


class BooleanExpressionFactory(LogicalTableFactory):
    def __init__(
        self,
        expression: str,
        data_id: DataId,
        base: LogicalTableFactory,
        bind: Optional[dict[str, Any]] = None,
    ):
        self._expression = expression
        self._base = base
        self._bind = bind if bind is not None else {}
        self._data_id = data_id

    @property
    def data_requested(self) -> QueryConstructionDataRequest:
        # TODO: include datasets referenced by the expression, in case those
        # aren't in base; we'd need to search RegistryDefaults.collections for
        # those, which means we need a guarantee that those collections are
        # implicitly requested (which seems reasonable).
        return self._base.data_requested

    def make_logical_table(
        self,
        data: QueryConstructionDataResult,
        columns_requested: AbstractSet[ColumnTag],
        *,
        defaults: RegistryDefaults,
        universe: DimensionUniverse,
    ) -> LogicalTable:
        data_id = DataCoordinate.standardize(self._data_id, universe=universe)  # TODO: expand this
        self._check_bind_keys(universe)
        base_logical_table = self._base.make_logical_table(
            data, columns_requested, defaults=defaults, universe=universe
        )
        parser = expr.ParserYacc()  # type: ignore
        tree: expr.Node = parser.parse(self._expression)
        analysis, data_id = self._analyze_expression_tree(
            tree, data_id, base_logical_table, defaults=defaults
        )
        columns_propagated = set(columns_requested)
        columns_propagated.update(DimensionKeyColumnTag.generate(analysis.dimensions.names))
        for element, columns in analysis.columns.items():
            columns_propagated.update(DimensionRecordColumnTag.generate(element.name, columns))
        # TODO: Propagate dataset columns here, too, once the expression
        # analyzer identifies them.  Might want to make the OuterSummary
        # produce a ColumnRequest directly.
        return BooleanExpression(
            tree=tree,
            data_id=data_id,
            analysis=analysis,
            base=base_logical_table.join_missing(columns_propagated, defaults),
            bind=self._bind,
        )

    def _check_bind_keys(self, universe: DimensionUniverse) -> None:
        for identifier in self._bind:
            if identifier in universe.getStaticElements().names:
                raise UserExpressionError(
                    f"Bind parameter key {identifier!r} conflicts with a dimension element."
                )
            table, _, column = identifier.partition(".")
            if column and table in universe.getStaticElements().names:
                raise UserExpressionError(f"Bind parameter key {identifier!r} looks like a dimension column.")

    def _analyze_expression_tree(
        self,
        tree: expr.Node,
        data_id: DataCoordinate,
        base_logical_table: LogicalTable,
        *,
        defaults: RegistryDefaults,
    ) -> tuple[expr.OuterSummary, DataCoordinate]:
        # Convert the expression to disjunctive normal form (ORs of
        # ANDs).  That's potentially super expensive in the general
        # case (where there's a ton of nesting of ANDs and ORs).  That
        # won't be the case for the expressions we expect, and we
        # actually use disjunctive normal instead of conjunctive (i.e.
        # ANDs of ORs) because I think the worst-case is a long list
        # of OR'd-together data IDs, which is already in or very close
        # to disjunctive normal form.
        normalized_tree = expr.NormalFormExpression.fromTree(tree, expr.NormalForm.DISJUNCTIVE)

        # Check the expression for consistency and completeness.
        # TODO: update this to check for expressions on dataset types
        visitor = expr.CheckVisitor(data_id, base_logical_table.dimensions, self._bind, defaults.dataId)
        try:
            summary = normalized_tree.visit(visitor)
        except RuntimeError as err:
            original_str = str(tree)
            normalized_str = str(normalized_tree.toTree())
            if normalized_str == original_str:
                msg = f'Error in query expression "{original_str}": {err}'
            else:
                msg = (
                    f'Error in query expression "{original_str}" '
                    f'(normalized to "{normalized_str}"): {err}'
                )
            raise UserExpressionError(msg) from None
        return summary, visitor.dataId


class BooleanExpression(LogicalTable):
    def __init__(
        self,
        *,
        tree: expr.Node,
        data_id: DataCoordinate,
        base: LogicalTable,
        analysis: expr.OuterSummary,
        bind: dict[str, Any],
    ):
        self._tree = tree
        self._data_id = data_id
        self._base = base
        self._bind = bind
        self._analysis = analysis

    @property
    def dimensions(self) -> DimensionGraph:
        return self._base.dimensions

    @property  # type: ignore
    @cached_getter
    def governor_dimension_restriction(self) -> GovernorDimensionRestriction:
        return self._analysis.governors.intersection(self._base.governor_dimension_restriction)

    @property
    def dataset_types(self) -> NamedValueAbstractSet[DatasetType]:
        return self._base.dataset_types

    @property  # type: ignore
    @cached_getter
    def is_doomed(self) -> bool:
        return self._base.is_doomed or bool(self.governor_dimension_restriction.dooms_on(self.dimensions))

    def diagnostics(self, verbose: bool = False) -> Iterable[str]:
        yield from self._base.diagnostics(verbose)
        for g in self.governor_dimension_restriction.dooms_on(self.dimensions):
            yield (
                f"Boolean expression requires {g.name}={self.governor_dimension_restriction.get(g)}, "
                f"but nested query requires {g.name}={self._base.governor_dimension_restriction.get(g)}."
            )

    def insert_join(self, *others: LogicalTable) -> LogicalTable:
        return BooleanExpression(
            tree=self._tree,
            data_id=self._data_id,
            base=self._base.insert_join(*others),
            analysis=self._analysis,
            bind=self._bind,
        )

    def restricted_to(
        self, dimensions: DimensionGraph, restriction: GovernorDimensionRestriction
    ) -> tuple[LogicalTable, bool]:
        restricted_base, changed = self._base.restricted_to(dimensions, restriction)
        if changed:
            return (
                BooleanExpression(
                    tree=self._tree,
                    data_id=self._data_id,
                    base=restricted_base,
                    bind=self._bind,
                    analysis=self._analysis,
                ),
                True,
            )
        else:
            return self, False

    @property
    def columns_provided(self) -> AbstractSet[ColumnTag]:
        return self._base.columns_provided

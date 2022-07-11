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

__all__ = ("ExpressionPredicate",)

from collections import defaultdict
from collections.abc import Mapping, Set
from typing import Any, cast

import sqlalchemy
from lsst.daf.relation import DictWriter, EngineTag, Predicate, sql
from lsst.utils.sets.ellipsis import Ellipsis
from lsst.utils.sets.unboundable import FrozenUnboundableSet, UnboundableSet

from ....core import ColumnTag, ColumnTypeInfo, DataCoordinate, DataIdValue, DimensionGraph, LogicalColumn
from ..._exceptions import UserExpressionError, UserExpressionSyntaxError
from .check import CheckVisitor
from .convert import convertExpressionToSql
from .normalForm import NormalForm, NormalFormExpression
from .parser import Node, ParserYacc  # type: ignore


class ExpressionPredicate(Predicate[ColumnTag]):
    """A predicate that represents a parsed string expression tree.

    Parameters
    ----------
    string : `str`
        String that was parsed into the expression tree.
    tree : `Node`
        The expression tree itself.
    bind : `Mapping` [ `str`, `Any` ]
        Literal values referenced in the expression.
    dataset_type_name : `str` or `None`
        The name of the dataset type to assume for unqualified dataset columns,
        or `None` if there are no such identifiers.
    columns_required : `~collections.abc.Set` [ `ColumnTag` ]
        All columns referenced by identifiers in the expression, from a
        previous analysis of it.
    dimension_constraints : `Mapping [ `str`, `UnboundableSet` ]
        Struct of pre-execution constraints on dimension values imposed by the
        expression, from a previous analysis of it.
    """

    def __init__(
        self,
        string: str,
        tree: Node | None,
        bind: Mapping[str, Any],
        dataset_type_name: str | None,
        columns_required: Set[ColumnTag],
        dimension_constraints: Mapping[str, UnboundableSet[DataIdValue]],
    ):
        self._string = string
        if tree is None:
            try:
                parser = ParserYacc()
                tree = parser.parse(string)
            except Exception as exc:
                raise UserExpressionSyntaxError(f"Failed to parse user expression {string!r}.") from exc
        self._tree = tree
        self._bind = bind
        self._columns_required = columns_required
        self._dimension_constraints = dimension_constraints
        self._dataset_type_name = dataset_type_name

    @classmethod
    def parse(
        cls,
        string: str,
        dimensions: DimensionGraph,
        *,
        bind: Mapping[str, Any] | None = None,
        data_id: DataCoordinate | None = None,
        defaults: DataCoordinate | None = None,
        dataset_type_name: str | None = None,
        allow_orphans: bool = False,
    ) -> ExpressionPredicate | None:
        """Create a predicate by parsing and analyzing a string expression.

        Parameters
        ----------
        string : `str`
            String to parse.
        dimensions : `DimensionGraph`
            The dimensions the query would include in the absence of this WHERE
            expression.
        bind : `Mapping` [ `str`, `Any` ], optional
            Literal values referenced in the expression.
        data_id : `DataCoordinate`, optional
            A fully-expanded data ID identifying dimensions known in advance.
            If not provided, will be set to an empty data ID.
            ``dataId.hasRecords()`` must return `True`.
        defaults : `DataCoordinate`, optional
            A data ID containing default for governor dimensions.  Ignored
            unless ``check=True``.
        dataset_type_name : `str` or `None`, optional
            The name of the dataset type to assume for unqualified dataset
            columns, or `None` if there are no such identifiers.
        allow_orphans : `bool`, optional
            If `True`, permit expressions to refer to dimensions without
            providing a value for their governor dimensions (e.g. referring to
            a visit without an instrument).  Should be left to default to
            `False` in essentially all new code.

        Returns
        -------
        predicate : `ExpressionPredicate` or `None`
            New predicate derived from the string expression, or `None` if the
            string is empty.
        """
        if not string:
            return None
        try:
            parser = ParserYacc()
            tree = parser.parse(string)
        except Exception as exc:
            raise UserExpressionSyntaxError(f"Failed to parse user expression {string!r}.") from exc
        if bind is None:
            bind = {}
        if bind:
            for identifier in bind:
                if identifier in dimensions.universe.getStaticElements().names:
                    raise RuntimeError(
                        f"Bind parameter key {identifier!r} conflicts with a dimension element."
                    )
                table, _, column = identifier.partition(".")
                if column and table in dimensions.universe.getStaticElements().names:
                    raise RuntimeError(f"Bind parameter key {identifier!r} looks like a dimension column.")
        if data_id is None:
            data_id = DataCoordinate.makeEmpty(dimensions.universe)
        if defaults is None:
            defaults = DataCoordinate.makeEmpty(dimensions.universe)
        # Convert the expression to disjunctive normal form (ORs of ANDs).
        # That's potentially super expensive in the general case (where there's
        # a ton of nesting of ANDs and ORs).  That won't be the case for the
        # expressions we expect, and we actually use disjunctive normal instead
        # of conjunctive (i.e.  ANDs of ORs) because I think the worst-case is
        # a long list of OR'd-together data IDs, which is already in or very
        # close to disjunctive normal form.
        expr = NormalFormExpression.fromTree(tree, NormalForm.DISJUNCTIVE)
        # Check the expression for consistency and completeness.
        visitor = CheckVisitor(data_id, dimensions, bind, defaults, allow_orphans=allow_orphans)
        try:
            summary = expr.visit(visitor)
        except UserExpressionError as err:
            exprOriginal = str(tree)
            exprNormal = str(expr.toTree())
            if exprNormal == exprOriginal:
                msg = f'Error in query expression "{exprOriginal}": {err}'
            else:
                msg = f'Error in query expression "{exprOriginal}" ' f'(normalized to "{exprNormal}"): {err}'
            raise UserExpressionError(msg) from None
        dimension_constraints: defaultdict[str, FrozenUnboundableSet] = defaultdict(
            FrozenUnboundableSet.make_full
        )
        for dimension, values in summary.dimension_constraint.items():
            dimension_constraints[dimension] = FrozenUnboundableSet.coerce(values)
        return ExpressionPredicate(
            string,
            tree,
            bind,
            dataset_type_name=dataset_type_name,
            columns_required=summary.make_column_tag_set(dataset_type_name),
            dimension_constraints=dimension_constraints,
        )

    @property
    def columns_required(self) -> Set[ColumnTag]:
        # Docstring inherited.
        return self._columns_required

    @property
    def dimension_constraints(self) -> Mapping[str, UnboundableSet[DataIdValue]]:
        # Docstring inherited.
        return self._dimension_constraints

    def supports_engine(self, engine: EngineTag) -> bool:
        return isinstance(engine, sql.Engine)

    def serialize(self, writer: DictWriter[ColumnTag]) -> dict[str, Any]:
        return {
            "type": "expression",
            "string": self._string,
            "bind": dict(self._bind),
            "columns_required": writer.write_column_set(self._columns_required),
            "dimension_constraints": {
                dimension_name: sorted(values)
                for dimension_name, unboundable_set in self._dimension_constraints.items()
                if (values := unboundable_set.values) is not Ellipsis
            },
            "dataset_type_name": self._dataset_type_name,
        }

    def to_sql_boolean(
        self,
        columns: Mapping[ColumnTag, LogicalColumn],
        column_types: sql.ColumnTypeInfo[ColumnTag, LogicalColumn],
    ) -> sqlalchemy.sql.ColumnElement:
        # Docstring inherited.
        return convertExpressionToSql(
            self._tree,
            columns=columns,
            bind=self._bind,
            column_types=cast(ColumnTypeInfo, column_types),
            dataset_type_name=self._dataset_type_name,
        )

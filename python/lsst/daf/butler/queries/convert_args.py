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

__all__ = (
    "convert_where_args",
    "convert_order_by_args",
)

from collections.abc import Mapping, Set
from typing import Any

from .._exceptions import InvalidQueryError
from ..dimensions import DataCoordinate, DataId, DimensionGroup
from ._expression_strings import convert_expression_string_to_predicate
from ._identifiers import IdentifierContext, interpret_identifier
from .expression_factory import ExpressionFactory, ExpressionProxy
from .tree import (
    DimensionKeyReference,
    OrderExpression,
    Predicate,
    Reversed,
    make_column_literal,
    validate_order_expression,
)


def convert_where_args(
    dimensions: DimensionGroup,
    datasets: Set[str],
    *args: str | Predicate | DataId,
    bind: Mapping[str, Any] | None = None,
    **kwargs: Any,
) -> Predicate:
    """Convert ``where`` arguments to a sequence of column expressions.

    Parameters
    ----------
    dimensions : `DimensionGroup`
        Dimensions already present in the query this filter is being applied
        to.  Returned predicates may reference dimensions outside this set.
    datasets : `~collections.abc.Set` [ `str` ]
        Dataset types already present in the query this filter is being applied
        to.  Returned predicates may reference datasets outside this set; this
        may be an error at a higher level, but it is not necessarily checked
        here.
    *args : `str`, `Predicate`, `DataCoordinate`, or `~collections.abc.Mapping`
        Expressions to convert into predicates.
    bind : `~collections.abc.Mapping`, optional
        Mapping from identifier to literal value used when parsing string
        expressions.
    **kwargs : `object`
        Additional data ID key-value pairs.

    Returns
    -------
    predicate : `Predicate`
        Standardized predicate object.

    Notes
    -----
    Data ID values are not checked for consistency; they are extracted from
    args and then kwargs and combined, with later extractions taking
    precedence.
    """
    context = IdentifierContext(dimensions, datasets, bind)
    result = Predicate.from_bool(True)
    data_id_dict: dict[str, Any] = {}
    for arg in args:
        match arg:
            case str():
                result = result.logical_and(
                    convert_expression_string_to_predicate(arg, context=context, universe=dimensions.universe)
                )
            case Predicate():
                result = result.logical_and(arg)
            case DataCoordinate():
                data_id_dict.update(arg.mapping)
            case _:
                data_id_dict.update(arg)
    data_id_dict.update(kwargs)
    for k, v in data_id_dict.items():
        result = result.logical_and(
            Predicate.compare(
                DimensionKeyReference.model_construct(dimension=dimensions.universe.dimensions[k]),
                "==",
                make_column_literal(v),
            )
        )
    return result


def convert_order_by_args(
    dimensions: DimensionGroup, datasets: Set[str], *args: str | OrderExpression | ExpressionProxy
) -> tuple[OrderExpression, ...]:
    """Convert ``order_by`` arguments to a sequence of column expressions.

    Parameters
    ----------
    dimensions : `DimensionGroup`
        Dimensions already present in the query whose rows are being sorted.
        Returned expressions may reference dimensions outside this set; this
        may be an error at a higher level, but it is not necessarily checked
        here.
    datasets : `~collections.abc.Set` [ `str` ]
        Dataset types already present in the query whose rows are being sorted.
        Returned expressions may reference datasets outside this set; this may
        be an error at a higher level, but it is not necessarily checked here.
    *args : `OrderExpression`, `str`, or `ExpressionObject`
        Expression or column names to sort by.

    Returns
    -------
    expressions : `tuple` [ `OrderExpression`, ... ]
        Standardized expression objects.
    """
    context = IdentifierContext(dimensions, datasets)
    result: list[OrderExpression] = []
    for arg in args:
        match arg:
            case str():
                reverse = False
                if arg.startswith("-"):
                    reverse = True
                    arg = arg[1:]
                if len(arg) == 0:
                    raise InvalidQueryError("Empty dimension name in ORDER BY")
                arg = interpret_identifier(context, arg)
                if reverse:
                    arg = Reversed(operand=arg)
            case ExpressionProxy():
                arg = ExpressionFactory.unwrap(arg)
        if not hasattr(arg, "expression_type"):
            raise TypeError(f"Unrecognized order-by argument: {arg!r}.")
        result.append(validate_order_expression(arg))
    return tuple(result)

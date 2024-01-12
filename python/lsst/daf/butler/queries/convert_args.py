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

__all__ = ("convert_where_args", "convert_order_by_args")

from collections.abc import Mapping, Set
from typing import Any

from ..dimensions import DataId, DimensionGroup
from .expression_factory import ExpressionProxy
from .tree import OrderExpression, Predicate


def convert_where_args(
    dimensions: DimensionGroup,
    datasets: Set[str],
    *args: str | Predicate | DataId,
    bind: Mapping[str, Any] | None = None,
) -> tuple[Predicate, ...]:
    """Convert ``where`` arguments to a sequence of column expressions.

    Parameters
    ----------
    dimensions : `DimensionGroup`
        Dimensions already present in the query this filter is being applied
        to.  Returned predicates *may* reference dimensions outside this set.
    datasets : `~collections.abc.Set` [ `str` ]
        Dataset types already present in the query this filter is being applied
        to.  Returned predicates may only reference datasets in this set.
    *args : `str`, `Predicate`, `DataCoordinate`, or `~collections.abc.Mapping`
        Expressions to convert into predicates.
    bind : `~collections.abc.Mapping`, optional
        Mapping from identifier to literal value used when parsing string
        expressions.

    Returns
    -------
    predicates : `tuple` [ `Predicate`, ... ]
        Standardized predicates, to be combined via logical AND.
    """
    raise NotImplementedError("TODO: Parse string expression.")


def convert_order_by_args(
    dimensions: DimensionGroup, datasets: Set[str], *args: str | OrderExpression | ExpressionProxy
) -> tuple[OrderExpression, ...]:
    """Convert ``order_by`` arguments to a sequence of column expressions.

    Parameters
    ----------
    dimensions : `DimensionGroup`
        Dimensions already present in the query whose rows are being sorted.
        Returned terms may only reference dimensions in this set.
    datasets : `~collections.abc.Set` [ `str` ]
        Dataset types already present in the query whose rows are being sorted.
        Returned terms may only reference datasets in this set.
    *args : `OrderExpression`, `str`, or `ExpressionObject`
        Expression or column names to sort by.

    Returns
    -------
    expressions : `tuple` [ `OrderExpression`, ... ]
        Standardized expression objects.
    """
    raise NotImplementedError("TODO")

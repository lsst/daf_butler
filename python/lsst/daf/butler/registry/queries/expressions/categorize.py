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

__all__ = ()  # all symbols intentionally private; for internal package use.

import enum
from typing import (
    Optional,
    Tuple,
)

from ....core import (
    DimensionUniverse,
    Dimension,
    DimensionElement,
)


class ExpressionConstant(enum.Enum):
    """Enumeration for constants recognized in all expressions.
    """
    NULL = "null"
    INGEST_DATE = "ingest_date"


def categorizeConstant(name: str) -> Optional[ExpressionConstant]:
    """Categorize an identifier in a parsed expression as one of a few global
    constants.

    Parameters
    ----------
    name : `str`
        Identifier to categorize.  Case-insensitive.

    Returns
    -------
    categorized : `ExpressionConstant` or `None`
        Enumeration value if the string represents a constant, `None`
        otherwise.
    """
    try:
        return ExpressionConstant(name.lower())
    except ValueError:
        return None


def categorizeElementId(universe: DimensionUniverse, name: str) -> Tuple[DimensionElement, Optional[str]]:
    """Categorize an identifier in a parsed expression as either a `Dimension`
    name (indicating the primary key for that dimension) or a non-primary-key
    column in a `DimensionElement` table.

    Parameters
    ----------
    universe : `DimensionUniverse`
        All known dimensions.
    name : `str`
        Identifier to categorize.

    Returns
    -------
    element : `DimensionElement`
        The `DimensionElement` the identifier refers to.
    column : `str` or `None`
        The name of a column in the table for ``element``, or `None` if
        ``element`` is a `Dimension` and the requested column is its primary
        key.

    Raises
    ------
    LookupError
        Raised if the identifier refers to a nonexistent `DimensionElement`
        or column.
    RuntimeError
        Raised if the expression refers to a primary key in an illegal way.
        This exception includes a suggestion for how to rewrite the expression,
        so at least its message should generally be propagated up to a context
        where the error can be interpreted by a human.
    """
    table, sep, column = name.partition('.')
    if column:
        try:
            element = universe[table]
        except KeyError as err:
            raise LookupError(f"No dimension element with name '{table}'.") from err
        if isinstance(element, Dimension) and column == element.primaryKey.name:
            # Allow e.g. "visit.id = x" instead of just "visit = x"; this
            # can be clearer.
            return element, None
        elif column in element.graph.names:
            # User said something like "patch.tract = x" or
            # "tract.tract = x" instead of just "tract = x" or
            # "tract.id = x", which is at least needlessly confusing and
            # possibly not actually a column name, though we can guess
            # what they were trying to do.
            # Encourage them to clean that up and try again.
            raise RuntimeError(
                f"Invalid reference to '{table}.{column}' "  # type: ignore
                f"in expression; please use '{column}' or "
                f"'{column}.{universe[column].primaryKey.name}' instead."
            )
        else:
            return element, column
    else:
        try:
            dimension = universe[table]
        except KeyError as err:
            raise LookupError(f"No dimension with name '{table}'.") from err
        return dimension, None

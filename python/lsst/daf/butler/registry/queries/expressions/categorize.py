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
from typing import Optional, Tuple

from ....core import Dimension, DimensionElement, DimensionGraph, DimensionUniverse


class ExpressionConstant(enum.Enum):
    """Enumeration for constants recognized in all expressions."""

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
    table, sep, column = name.partition(".")
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
            name = universe[column].primaryKey.name  # type: ignore
            raise RuntimeError(
                f"Invalid reference to '{table}.{column}' "
                f"in expression; please use '{column}' or "
                f"'{column}.{name}' instead."
            )
        else:
            return element, column
    else:
        try:
            dimension = universe[table]
        except KeyError as err:
            raise LookupError(f"No dimension with name '{table}'.") from err
        return dimension, None


def categorizeOrderByName(graph: DimensionGraph, name: str) -> Tuple[DimensionElement, Optional[str]]:
    """Categorize an identifier in an ORDER BY clause.

    Parameters
    ----------
    graph : `DimensionGraph`
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
    ValueError
        Raised if element name is not found in a graph, metadata name is not
        recognized, or if there is more than one element has specified
        metadata.

    Notes
    -----
    For ORDER BY identifiers we use slightly different set of rules compared to
    the rules in `categorizeElementId`:

    - Name can be a dimension element name. e.g. ``visit``.
    - Name can be an element name and a metadata name (or key name) separated
      by dot, e.g. ``detector.full_name``.
    - Name can be a metadata name without element name prefix, e.g.
      ``day_obs``; in that case metadata (or key) is searched in all elements
      present in a graph. Exception is raised if name appears in more than one
      element.
    - Two special identifiers ``timespan.begin`` and ``timespan.end`` can be
      used with temporal elements, if element name is not given then a temporal
      element from a graph is used.
    """
    element: DimensionElement
    field_name: Optional[str] = None
    if name in ("timespan.begin", "timespan.end"):
        matches = [element for element in graph.elements if element.temporal]
        if len(matches) == 1:
            element = matches[0]
            field_name = name
        elif len(matches) > 1:
            raise ValueError(
                f"Timespan exists in more than one dimesion element: {matches},"
                " qualify timespan with specific dimension name."
            )
        else:
            raise ValueError(f"Cannot find any temporal dimension element for '{name}'.")
    elif "." not in name:
        # No dot, can be either a dimension name or a field name (in any of
        # the known elements)
        if name in graph.elements.names:
            element = graph.elements[name]
        else:
            # Can be a metadata name or any of unique keys
            matches = [elem for elem in graph.elements if name in elem.metadata.names]
            matches += [dim for dim in graph if name in dim.uniqueKeys.names]
            if len(matches) == 1:
                element = matches[0]
                field_name = name
            elif len(matches) > 1:
                raise ValueError(
                    f"Metadata '{name}' exists in more than one dimension element: {matches},"
                    " qualify metadata name with dimension name."
                )
            else:
                raise ValueError(f"Metadata '{name}' cannot be found in any dimension.")
    else:
        # qualified name, must be a dimension element and a field
        elem_name, _, field_name = name.partition(".")
        if elem_name not in graph.elements.names:
            raise ValueError(f"Unknown dimension element name '{elem_name}'")
        element = graph.elements[elem_name]
        if field_name in ("timespan.begin", "timespan.end"):
            if not element.temporal:
                raise ValueError(f"Cannot use '{field_name}' with non-temporal element '{element}'.")
        elif isinstance(element, Dimension) and field_name == element.primaryKey.name:
            # Primary key is optional
            field_name = None
        else:
            if not (
                field_name in element.metadata.names
                or (isinstance(element, Dimension) and field_name in element.alternateKeys.names)
            ):
                raise ValueError(f"Field '{field_name}' does not exist in '{element}'.")

    return element, field_name


def categorizeElementOrderByName(element: DimensionElement, name: str) -> Optional[str]:
    """Categorize an identifier in an ORDER BY clause for a single element.

    Parameters
    ----------
    element : `DimensionElement`
        Dimension element.
    name : `str`
        Identifier to categorize.

    Returns
    -------
    column : `str` or `None`
        The name of a column in the table for ``element``, or `None` if
        ``element`` is a `Dimension` and the requested column is its primary
        key.

    Raises
    ------
    ValueError
        Raised if name is not recognized.

    Notes
    -----
    For ORDER BY identifiers we use slightly different set of rules compared to
    the rules in `categorizeElementId`:

    - Name can be a dimension element name. e.g. ``visit``.
    - Name can be an element name and a metadata name (or key name) separated
      by dot, e.g. ``detector.full_name``, element name must correspond to
      ``element`` argument
    - Name can be a metadata name without element name prefix, e.g.
      ``day_obs``.
    - Two special identifiers ``timespan.begin`` and ``timespan.end`` can be
      used with temporal elements.
    """
    field_name: Optional[str] = None
    if name in ("timespan.begin", "timespan.end"):
        if element.temporal:
            field_name = name
        else:
            raise ValueError(f"Cannot use '{field_name}' with non-temporal element '{element}'.")
    elif "." not in name:
        # No dot, can be either a dimension name or a field name (in any of
        # the known elements)
        if name == element.name:
            # Must be a dimension element
            if not isinstance(element, Dimension):
                raise ValueError(f"Element '{element}' is not a dimension.")
        else:
            # Can be a metadata name or any of the keys
            if name in element.metadata.names or (
                isinstance(element, Dimension) and name in element.uniqueKeys.names
            ):
                field_name = name
            else:
                raise ValueError(f"Field '{name}' does not exist in '{element}'.")
    else:
        # qualified name, must be a dimension element and a field
        elem_name, _, field_name = name.partition(".")
        if elem_name != element.name:
            raise ValueError(f"Element name mismatch: '{elem_name}' instead of '{element}'")
        if field_name in ("timespan.begin", "timespan.end"):
            if not element.temporal:
                raise ValueError(f"Cannot use '{field_name}' with non-temporal element '{element}'.")
        elif isinstance(element, Dimension) and field_name == element.primaryKey.name:
            # Primary key is optional
            field_name = None
        else:
            if not (
                field_name in element.metadata.names
                or (isinstance(element, Dimension) and field_name in element.alternateKeys.names)
            ):
                raise ValueError(f"Field '{field_name}' does not exist in '{element}'.")

    return field_name

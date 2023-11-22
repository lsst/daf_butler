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

__all__ = ()  # all symbols intentionally private; for internal package use.

import enum
from typing import cast

from ....dimensions import Dimension, DimensionElement, DimensionGroup, DimensionUniverse


class ExpressionConstant(enum.Enum):
    """Enumeration for constants recognized in all expressions."""

    NULL = "null"
    INGEST_DATE = "ingest_date"


def categorizeConstant(name: str) -> ExpressionConstant | None:
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


def categorizeElementId(universe: DimensionUniverse, name: str) -> tuple[DimensionElement, str | None]:
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
    table, _, column = name.partition(".")
    if column:
        try:
            element = universe[table]
        except KeyError:
            if table == "timespan" or table == "datetime" or table == "timestamp":
                raise LookupError(
                    "Dimension element name cannot be inferred in this context; "
                    f"use <dimension>.timespan.{column} instead."
                ) from None
            raise LookupError(f"No dimension element with name {table!r} in {name!r}.") from None
        if isinstance(element, Dimension) and column == element.primaryKey.name:
            # Allow e.g. "visit.id = x" instead of just "visit = x"; this
            # can be clearer.
            return element, None
        elif column in element.dimensions.names:
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


def categorizeOrderByName(dimensions: DimensionGroup, name: str) -> tuple[DimensionElement, str | None]:
    """Categorize an identifier in an ORDER BY clause.

    Parameters
    ----------
    dimensions : `DimensionGroup`
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
        Raised if element name is not found in a dimensions, metadata name is
        not recognized, or if there is more than one element has specified
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
      present in a dimensions. Exception is raised if name appears in more than
      one element.
    - Two special identifiers ``timespan.begin`` and ``timespan.end`` can be
      used with temporal elements, if element name is not given then a temporal
      element from a dimensions is used.
    """
    element: DimensionElement
    field_name: str | None = None
    if name in ("timespan.begin", "timespan.end"):
        matches = [
            element
            for element_name in dimensions.elements
            if (element := dimensions.universe[element_name]).temporal
        ]
        if len(matches) == 1:
            element = matches[0]
            field_name = name
        elif len(matches) > 1:
            raise ValueError(
                "Timespan exists in more than one dimension element "
                f"({', '.join(element.name for element in matches)}); "
                "qualify timespan with specific dimension name."
            )
        else:
            raise ValueError(f"Cannot find any temporal dimension element for '{name}'.")
    elif "." not in name:
        # No dot, can be either a dimension name or a field name (in any of
        # the known elements)
        if name in dimensions.elements:
            element = dimensions.universe[name]
        else:
            # Can be a metadata name or any of unique keys
            match_pairs: list[tuple[DimensionElement, bool]] = [
                (element, False)
                for element_name in dimensions.elements
                if name in (element := dimensions.universe[element_name]).metadata.names
            ]
            match_pairs += [
                (dimension, True)
                for dimension_name in dimensions.names
                if name in (dimension := dimensions.universe.dimensions[dimension_name]).uniqueKeys.names
            ]
            if len(match_pairs) == 1:
                element, is_dimension_key = match_pairs[0]
                if is_dimension_key and name == cast(Dimension, element).primaryKey.name:
                    # Need to treat reference to primary key field as a
                    # reference to the dimension name.
                    return element, None
                field_name = name
            elif len(match_pairs) > 1:
                raise ValueError(
                    f"Metadata '{name}' exists in more than one dimension element "
                    f"({', '.join(element.name for element, _ in match_pairs)}); "
                    "qualify field name with dimension name."
                )
            else:
                raise ValueError(f"Metadata '{name}' cannot be found in any dimension.")
    else:
        # qualified name, must be a dimension element and a field
        elem_name, _, field_name = name.partition(".")
        if elem_name not in dimensions.elements:
            if field_name == "begin" or field_name == "end":
                raise ValueError(
                    f"Unknown dimension element {elem_name!r}; perhaps you meant 'timespan.{field_name}'?"
                )
            raise ValueError(f"Unknown dimension element {elem_name!r}.")
        element = dimensions.universe[elem_name]
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


def categorizeElementOrderByName(element: DimensionElement, name: str) -> str | None:
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
    field_name: str | None = None
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
            # Can be a metadata name or any of the keys, but primary key needs
            # to be treated the same as a reference to the dimension name
            # itself.
            if isinstance(element, Dimension):
                if name == element.primaryKey.name:
                    return None
                elif name in element.uniqueKeys.names:
                    return name
            if name in element.metadata.names:
                return name
            raise ValueError(f"Field '{name}' does not exist in '{element}'.")
    else:
        # qualified name, must be a dimension element and a field
        elem_name, _, field_name = name.partition(".")
        if elem_name != element.name:
            if field_name == "begin" or field_name == "end":
                extra = f"; perhaps you meant 'timespan.{field_name}'?"
            else:
                extra = "."
            raise ValueError(f"Element name mismatch: '{elem_name}' instead of '{element}'{extra}")
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

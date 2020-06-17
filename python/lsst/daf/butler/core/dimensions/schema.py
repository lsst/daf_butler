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
    "addDimensionForeignKey",
    "makeDimensionElementTableSpec",
    "REGION_FIELD_SPEC",
)

import copy

from typing import TYPE_CHECKING

from .. import ddl
from ..named import NamedValueSet
from ..timespan import TIMESPAN_FIELD_SPECS

if TYPE_CHECKING:  # Imports needed only for type annotations; may be circular.
    from .elements import DimensionElement, Dimension


# Most regions are small (they're quadrilaterals), but visit ones can be quite
# large because they have a complicated boundary.  For HSC, about ~1400 bytes.
REGION_FIELD_SPEC = ddl.FieldSpec(name="region", nbytes=2048, dtype=ddl.Base64Region)


def _makeForeignKeySpec(dimension: Dimension) -> ddl.ForeignKeySpec:
    """Make a `ddl.ForeignKeySpec` that references the table for the given
    `Dimension` table.

    Most callers should use the higher-level `addDimensionForeignKey` function
    instead.

    Parameters
    ----------
    dimension : `Dimension`
        The dimension to be referenced.  Caller guarantees that it is actually
        associated with a table.

    Returns
    -------
    spec : `ddl.ForeignKeySpec`
        A database-agnostic foreign key specification.
    """
    source = []
    target = []
    for other in dimension.required:
        if other == dimension:
            target.append(dimension.primaryKey.name)
        else:
            target.append(other.name)
        source.append(other.name)
    return ddl.ForeignKeySpec(table=dimension.name, source=tuple(source), target=tuple(target))


def addDimensionForeignKey(tableSpec: ddl.TableSpec, dimension: Dimension, *,
                           primaryKey: bool, nullable: bool = False, constraint: bool = True
                           ) -> ddl.FieldSpec:
    """Add a field and possibly a foreign key to a table specification that
    reference the table for the given `Dimension`.

    Parameters
    ----------
    tableSpec : `ddl.TableSpec`
        Specification the field and foreign key are to be added to.
    dimension : `Dimension`
        Dimension to be referenced.  If this dimension has required
        dependencies, those must have already been added to the table.  A field
        will be added that correspond to this dimension's primary key, and a
        foreign key constraint will be added only if the dimension is
        associated with a table of its own.
    primaryKey : `bool`
        If `True`, the new field will be added as part of a compound primary
        key for the table.
    nullable : `bool`, optional
        If `False` (default) the new field will be added with a NOT NULL
        constraint.
    constraint : `bool`
        If `False` (`True` is default), just add the field, not the foreign
        key constraint.

    Returns
    -------
    fieldSpec : `ddl.FieldSpec`
        Specification for the field just added.
    """
    # Add the dependency's primary key field, but use the dimension name for
    # the field name to make it unique and more meaningful in this table.
    fieldSpec = copy.copy(dimension.primaryKey)
    fieldSpec.name = dimension.name
    fieldSpec.primaryKey = primaryKey
    fieldSpec.nullable = nullable
    tableSpec.fields.add(fieldSpec)
    # Also add a foreign key constraint on the dependency table, but only if
    # there actually is one and we weren't told not to.
    if dimension.hasTable() and dimension.viewOf is None and constraint:
        tableSpec.foreignKeys.append(_makeForeignKeySpec(dimension))
    return fieldSpec


def makeDimensionElementTableSpec(element: DimensionElement) -> ddl.TableSpec:
    """Create a complete table specification for a `DimensionElement`.

    This combines the foreign key fields from dependencies, unique keys
    for true `Dimension` instances, metadata fields, and region/timestamp
    fields for spatial/temporal elements.

    Most callers should use `DimensionElement.makeTableSpec` or
    `DimensionUniverse.makeSchemaSpec` instead, which account for elements
    that have no table or reference another table.

    Parameters
    ----------
    element : `DimensionElement`
        Element for which to make a table specification.

    Returns
    -------
    spec : `ddl.TableSpec`
        Database-agnostic specification for a table.
    """
    tableSpec = ddl.TableSpec(
        fields=NamedValueSet(),
        unique=set(),
        foreignKeys=[]
    )
    # Add the primary key fields of required dimensions.  These continue to be
    # primary keys in the table for this dimension.
    dependencies = []
    for dimension in element.required:
        if dimension != element:
            addDimensionForeignKey(tableSpec, dimension, primaryKey=True)
            dependencies.append(dimension.name)
        else:
            # A Dimension instance is in its own required dependency graph
            # (always at the end, because of topological ordering).  In this
            # case we don't want to rename the field.
            tableSpec.fields.add(element.primaryKey)   # type: ignore
    # Add fields and foreign keys for implied dimensions.  These are primary
    # keys in their own table, but should not be here.  As with required
    # dependencies, we rename the fields with the dimension name.
    # We use element.implied instead of element.graph.implied because we don't
    # want *recursive* implied dependencies.
    for dimension in element.implied:
        addDimensionForeignKey(tableSpec, dimension, primaryKey=False, nullable=True)
    # Add non-primary unique keys and unique constraints for them.
    for fieldSpec in getattr(element, "alternateKeys", ()):
        tableSpec.fields.add(fieldSpec)
        tableSpec.unique.add(tuple(dependencies) + (fieldSpec.name,))
    # Add metadata fields, temporal timespans, and spatial regions.
    for fieldSpec in element.metadata:
        tableSpec.fields.add(fieldSpec)
    if element.spatial is not None:
        tableSpec.fields.add(REGION_FIELD_SPEC)
    if element.temporal is not None:
        for fieldSpec in TIMESPAN_FIELD_SPECS:
            tableSpec.fields.add(fieldSpec)
    return tableSpec

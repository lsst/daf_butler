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

__all__ = ("makeElementTableSpec", "makeOverlapTableSpec", "Timespan", "REGION_FIELD_SPEC",
           "TIMESPAN_FIELD_SPECS", "OVERLAP_TABLE_NAME_PATTERN")

import copy

from sqlalchemy import DateTime

from ..schema import FieldSpec, TableSpec, ForeignKeySpec, Base64Region
from ..utils import NamedValueSet
from .records import Timespan
from .elements import DimensionElement, Dimension


REGION_FIELD_SPEC = FieldSpec(name="region", dtype=Base64Region)

TIMESPAN_FIELD_SPECS = Timespan(
    begin=FieldSpec(name="datetime_begin", dtype=DateTime),
    end=FieldSpec(name="datetime_end", dtype=DateTime),
)

OVERLAP_TABLE_NAME_PATTERN = "{0}_{1}_overlap"


def makeForeignKeySpec(dimension: Dimension) -> ForeignKeySpec:
    """Make a `ForeignKeySpec` that references the table for the given
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
    spec : `ForeignKeySpec`
        A database-agnostic foreign key specification.
    """
    source = []
    target = []
    for other in dimension.graph.required:
        if other == dimension:
            target.append(dimension.primaryKey.name)
        else:
            target.append(other.name)
        source.append(other.name)
    return ForeignKeySpec(table=dimension.name, source=tuple(source), target=tuple(target))


def addDimensionForeignKey(tableSpec: TableSpec, dimension: Dimension, *,
                           primaryKey: bool, nullable: bool = False):
    """Add a field and possibly a foreign key to a table specification that
    reference the table for the given `Dimension`.

    Parameters
    ----------
    tableSpec : `TableSpec`
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
    """
    # Add the dependency's primary key field, but use the dimension name for
    # the field name to make it unique and more meaningful in this table.
    fieldSpec = copy.copy(dimension.primaryKey)
    fieldSpec.name = dimension.name
    fieldSpec.primaryKey = primaryKey
    fieldSpec.nullable = nullable
    tableSpec.fields.add(fieldSpec)
    # Also add a foreign key constraint on the dependency table, but only if
    # there actually is one.
    if dimension.hasTable() and dimension.viewOf is None:
        tableSpec.foreignKeys.append(makeForeignKeySpec(dimension))


def makeElementTableSpec(element: DimensionElement) -> TableSpec:
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
    spec : `TableSpec`
        Database-agnostic specification for a table.
    """
    tableSpec = TableSpec(
        fields=NamedValueSet(),
        unique=set(),
        foreignKeys=[]
    )
    # Add the primary key fields of required dimensions.  These continue to be
    # primary keys in the table for this dimension.
    dependencies = []
    for dimension in element.graph.required:
        if dimension != element:
            addDimensionForeignKey(tableSpec, dimension, primaryKey=True)
            dependencies.append(dimension.name)
        else:
            # A Dimension instance is in its own required dependency graph
            # (always at the end, because of topological ordering).  In this
            # case we don't want to rename the field.
            tableSpec.fields.add(element.primaryKey)
    # Add fields and foreign keys for implied dimensions.  These are primary
    # keys in their own table, but should not be here.  As with required
    # dependencies, we rename the fields with the dimension name.
    # We use element.implied instead of element.graph.implied because we don't
    # want *recursive* implied dependencies.
    for dimension in element.implied:
        addDimensionForeignKey(tableSpec, dimension, primaryKey=False)
    # Add non-primary unique keys and unique constraints for them.
    for fieldSpec in getattr(element, "alternateKeys", ()):
        tableSpec.fields.add(fieldSpec)
        tableSpec.unique.add(tuple(dependencies) + (fieldSpec.name,))
    # Add metadata fields, temporal timespans, and spatial regions.
    for fieldSpec in element.metadata:
        tableSpec.fields.add(fieldSpec)
    if element.spatial:
        tableSpec.fields.add(REGION_FIELD_SPEC)
    if element.temporal:
        for fieldSpec in TIMESPAN_FIELD_SPECS:
            tableSpec.fields.add(fieldSpec)
    return tableSpec


def makeOverlapTableSpec(a: DimensionElement, b: DimensionElement) -> TableSpec:
    """Create a specification for a table that represents a many-to-many
    relationship between two `DimensionElement` tables.

    Parameters
    ----------
    a : `DimensionElement`
        First element in the relationship.
    b : `DimensionElement`
        Second element in the relationship.

    Returns
    -------
    spec : `TableSpec`
        Database-agnostic specification for a table.
    """
    tableSpec = TableSpec(
        fields=NamedValueSet(),
        unique=set(),
        foreignKeys=[],
    )
    for dimension in a.graph.required:
        addDimensionForeignKey(tableSpec, dimension, primaryKey=True)
    for dimension in b.graph.required:
        addDimensionForeignKey(tableSpec, dimension, primaryKey=True)
    return tableSpec

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

__all__ = ("addDimensionForeignKey",)

import copy
from typing import TYPE_CHECKING, Tuple

from .. import ddl
from ..named import NamedValueSet
from ..timespan import TimespanDatabaseRepresentation

if TYPE_CHECKING:  # Imports needed only for type annotations; may be circular.
    from ._elements import Dimension, DimensionElement


def _makeForeignKeySpec(dimension: Dimension) -> ddl.ForeignKeySpec:
    """Make a `ddl.ForeignKeySpec`.

    This will reference the table for the given `Dimension` table.

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


def addDimensionForeignKey(
    tableSpec: ddl.TableSpec,
    dimension: Dimension,
    *,
    primaryKey: bool,
    nullable: bool = False,
    constraint: bool = True,
) -> ddl.FieldSpec:
    """Add a field and possibly a foreign key to a table specification.

    The field will reference the table for the given `Dimension`.

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


class DimensionElementFields:
    """Class for constructing table schemas for `DimensionElement`.

    This creates an object that constructs the table schema for a
    `DimensionElement` and provides a categorized view of its fields.

    Parameters
    ----------
    element : `DimensionElement`
        Element for which to make a table specification.

    Notes
    -----
    This combines the foreign key fields from dependencies, unique keys
    for true `Dimension` instances, metadata fields, and region/timestamp
    fields for spatial/temporal elements.

    Callers should use `DimensionUniverse.makeSchemaSpec` if they want to
    account for elements that have no table or reference another table; this
    class simply creates a specification for the table an element _would_ have
    without checking whether it does have one.  That can be useful in contexts
    (e.g. `DimensionRecord`) where we want to simulate the existence of such a
    table.
    """

    def __init__(self, element: DimensionElement):
        self.element = element
        self._tableSpec = ddl.TableSpec(fields=())
        # Add the primary key fields of required dimensions.  These continue to
        # be primary keys in the table for this dimension.
        self.required = NamedValueSet()
        self.dimensions = NamedValueSet()
        self.facts = NamedValueSet()
        self.standard = NamedValueSet()
        dependencies = []
        for dimension in element.required:
            if dimension != element:
                fieldSpec = addDimensionForeignKey(self._tableSpec, dimension, primaryKey=True)
                dependencies.append(fieldSpec.name)
            else:
                fieldSpec = element.primaryKey  # type: ignore
                # A Dimension instance is in its own required dependency graph
                # (always at the end, because of topological ordering).  In
                # this case we don't want to rename the field.
                self._tableSpec.fields.add(fieldSpec)
            self.required.add(fieldSpec)
            self.dimensions.add(fieldSpec)
            self.standard.add(fieldSpec)
        # Add fields and foreign keys for implied dimensions.  These are
        # primary keys in their own table, but should not be here.  As with
        # required dependencies, we rename the fields with the dimension name.
        # We use element.implied instead of element.graph.implied because we
        # don't want *recursive* implied dependencies.
        self.implied = NamedValueSet()
        for dimension in element.implied:
            fieldSpec = addDimensionForeignKey(self._tableSpec, dimension, primaryKey=False, nullable=False)
            self.implied.add(fieldSpec)
            self.dimensions.add(fieldSpec)
            self.standard.add(fieldSpec)
        # Add non-primary unique keys and unique constraints for them.
        for fieldSpec in getattr(element, "alternateKeys", ()):
            self._tableSpec.fields.add(fieldSpec)
            self._tableSpec.unique.add(tuple(dependencies) + (fieldSpec.name,))
            self.standard.add(fieldSpec)
            self.facts.add(fieldSpec)
        # Add other metadata fields.
        for fieldSpec in element.metadata:
            self._tableSpec.fields.add(fieldSpec)
            self.standard.add(fieldSpec)
            self.facts.add(fieldSpec)
        names = list(self.standard.names)
        # Add fields for regions and/or timespans.
        if element.spatial is not None:
            names.append("region")
        if element.temporal is not None:
            names.append(TimespanDatabaseRepresentation.NAME)
        self.names = tuple(names)

    def makeTableSpec(
        self,
        TimespanReprClass: type[TimespanDatabaseRepresentation],
    ) -> ddl.TableSpec:
        """Construct a complete specification for a table.

        The table could hold the records of this element.

        Parameters
        ----------
        TimespanReprClass : `type` [ `TimespanDatabaseRepresentation` ]
            Class object that specifies how timespans are represented in the
            database.

        Returns
        -------
        spec : `ddl.TableSpec`
            Specification for a table.
        """
        if self.element.temporal is not None or self.element.spatial is not None:
            spec = ddl.TableSpec(
                fields=NamedValueSet(self._tableSpec.fields),
                unique=self._tableSpec.unique,
                indexes=self._tableSpec.indexes,
                foreignKeys=self._tableSpec.foreignKeys,
            )
            if self.element.spatial is not None:
                spec.fields.add(ddl.FieldSpec.for_region())
            if self.element.temporal is not None:
                spec.fields.update(TimespanReprClass.makeFieldSpecs(nullable=True))
        else:
            spec = self._tableSpec
        return spec

    def __str__(self) -> str:
        lines = [f"{self.element.name}: "]
        lines.extend(f"  {field.name}: {field.getPythonType().__name__}" for field in self.standard)
        if self.element.spatial is not None:
            lines.append("  region: lsst.sphgeom.Region")
        if self.element.temporal is not None:
            lines.append("  timespan: lsst.daf.butler.Timespan")
        return "\n".join(lines)

    element: DimensionElement
    """The dimension element these fields correspond to.

    (`DimensionElement`)
    """

    required: NamedValueSet[ddl.FieldSpec]
    """The required dimension fields of this table.

    They correspond to the element's required
    dimensions, in that order, i.e. `DimensionElement.required`
    (`NamedValueSet` [ `ddl.FieldSpec` ]).
    """

    implied: NamedValueSet[ddl.FieldSpec]
    """The implied dimension fields of this table.

    They correspond to the element's implied
    dimensions, in that order, i.e. `DimensionElement.implied`
    (`NamedValueSet` [ `ddl.FieldSpec` ]).
    """

    dimensions: NamedValueSet[ddl.FieldSpec]
    """The direct and implied dimension fields of this table.

    They correspond to the element's direct
    required and implied dimensions, in that order, i.e.
    `DimensionElement.dimensions` (`NamedValueSet` [ `ddl.FieldSpec` ]).
    """

    facts: NamedValueSet[ddl.FieldSpec]
    """The standard fields of this table that do not correspond to dimensions.

    (`NamedValueSet` [ `ddl.FieldSpec` ]).

    This is equivalent to ``standard - dimensions`` (but possibly in a
    different order).
    """

    standard: NamedValueSet[ddl.FieldSpec]
    """All standard fields that are expected to have the same form.

    They are expected to have the same form in all
    databases; this is all fields other than those that represent a region
    and/or timespan (`NamedValueSet` [ `ddl.FieldSpec` ]).
    """

    names: Tuple[str, ...]
    """The names of all fields in the specification (`tuple` [ `str` ]).

    This includes "region" and/or "timespan" if `element` is spatial and/or
    temporal (respectively).  The actual database representation of these
    quantities may involve multiple fields (or even fields only on a different
    table), but the Python representation of those rows (i.e. `DimensionRecord`
    instances) will always contain exactly these fields.
    """

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

__all__ = ("SpatialRegionDatabaseRepresentation",)

from typing import Any, ClassVar, Dict, Mapping, Optional, Type

import lsst.sphgeom
import sqlalchemy

from . import ddl
from ._topology import TopologicalExtentDatabaseRepresentation, TopologicalSpace


class SpatialRegionDatabaseRepresentation(TopologicalExtentDatabaseRepresentation[lsst.sphgeom.Region]):
    """Class reflecting how spatial regions are represented inside the DB.

    An instance of this class encapsulates how spatial regions on the sky are
    represented in a database engine.

    Instances should be constructed via `fromSelectable`, not by calling the
    constructor directly.

    Parameters
    ----------
    column : `sqlalchemy.sql.ColumnElement`
        Column containing the opaque byte-string, with automatic conversion to
        `lsst.sphgeom.Region` implemented via SQLAlchemy hooks.
    name : `str`
        Name of the column.

    Notes
    -----
    Unlike `TimespanDatabaseRepresentation`, this is a concrete class, because
    we currently do not support any database-native spatial regions, and
    instead rely on precomputed overlaps and opaque (to the database) byte
    string columns.  As a result, it also does not support any in-database
    topological predicates.

    If we add support for database-native regions in the future, this class may
    become an ABC with multiple concrete implementations.
    """

    def __init__(self, column: sqlalchemy.sql.ColumnElement, name: str):
        self.column = column
        self._name = name

    NAME: ClassVar[str] = "region"
    SPACE: ClassVar[TopologicalSpace] = TopologicalSpace.SPATIAL

    @classmethod
    def makeFieldSpecs(
        cls, nullable: bool, name: Optional[str] = None, **kwargs: Any
    ) -> tuple[ddl.FieldSpec, ...]:
        # Docstring inherited.
        if name is None:
            name = cls.NAME
        # Most regions are small (they're quadrilaterals), but visit ones can
        # be quite large because they have a complicated boundary.  For HSC,
        # that's about ~1400 bytes, and I've just rounded up to the nearest
        # power of two.  Given what we now know about variable-length TEXT
        # having no performance penalties in PostgreSQL and SQLite vs.
        # fixed-length strings, there's probably a variable-length bytes type
        # we should be using instead, but that's a schema change and hence
        # something we won't be doing anytime soon.
        return (ddl.FieldSpec(name, nbytes=2048, dtype=ddl.Base64Region),)

    @classmethod
    def getFieldNames(cls, name: Optional[str] = None) -> tuple[str, ...]:
        # Docstring inherited.
        if name is None:
            name = cls.NAME
        return (name,)

    @classmethod
    def update(
        cls,
        extent: Optional[lsst.sphgeom.Region],
        name: Optional[str] = None,
        result: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        # Docstring inherited.
        if name is None:
            name = cls.NAME
        if result is None:
            result = {}
        result[name] = extent
        return result

    @classmethod
    def extract(cls, mapping: Mapping[str, Any], name: Optional[str] = None) -> Optional[lsst.sphgeom.Region]:
        # Docstring inherited.
        if name is None:
            name = cls.NAME
        return mapping[name]

    @classmethod
    def from_columns(
        cls: Type[SpatialRegionDatabaseRepresentation],
        columns: sqlalchemy.sql.ColumnCollection,
        name: Optional[str] = None,
    ) -> SpatialRegionDatabaseRepresentation:
        # Docstring inherited
        if name is None:
            name = cls.NAME
        return cls(columns[name], name)

    @property
    def name(self) -> str:
        # Docstring inherited
        return self._name

    def isNull(self) -> sqlalchemy.sql.ColumnElement:
        # Docstring inherited
        return self.column.is_(None)

    def flatten(self, name: Optional[str] = None) -> tuple[sqlalchemy.sql.ColumnElement, ...]:
        # Docstring inherited
        if name is None:
            name = self.name
        return (self.column.label(name),)

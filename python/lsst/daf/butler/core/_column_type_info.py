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

__all__ = ("ColumnTypeInfo",)

import dataclasses

from .ddl import FieldSpec
from .dimensions import DimensionUniverse
from .timespan import TimespanDatabaseRepresentation


@dataclasses.dataclass(frozen=True, eq=False)
class ColumnTypeInfo:
    """A struct that aggregates information about column types that can differ
    across data repositories due to `Registry` and dimension configuration.
    """

    timespan_cls: type[TimespanDatabaseRepresentation]
    """An abstraction around the column type or types used for timespans by
    this database engine.
    """

    universe: DimensionUniverse
    """Object that manages the definitions of all dimension and dimension
    elements.
    """

    dataset_id_spec: FieldSpec
    """Field specification for the dataset primary key column.
    """

    run_key_spec: FieldSpec
    """Field specification for the `~CollectionType.RUN` primary key column.
    """

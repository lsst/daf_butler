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

__all__ = ("ColumnTypeInfo", "LogicalColumn")

import dataclasses
import datetime

import astropy.time
import sqlalchemy

from . import ddl
from .dimensions import DimensionUniverse
from .timespan_database_representation import TimespanDatabaseRepresentation

LogicalColumn = sqlalchemy.sql.ColumnElement | TimespanDatabaseRepresentation
"""A type alias for the types used to represent columns in SQL relations."""


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

    dataset_id_spec: ddl.FieldSpec
    """Field specification for the dataset primary key column.
    """

    run_key_spec: ddl.FieldSpec
    """Field specification for the `~CollectionType.RUN` primary key column.
    """

    ingest_date_dtype: type[ddl.AstropyTimeNsecTai] | type[sqlalchemy.TIMESTAMP]
    """Type of the ``ingest_date`` column, can be either
    `~lsst.daf.butler.ddl.AstropyTimeNsecTai` or `sqlalchemy.TIMESTAMP`.
    """

    @property
    def ingest_date_pytype(self) -> type:
        """Python type corresponding to ``ingest_date`` column type.

        Returns
        -------
        `type`
            The Python type.
        """
        if self.ingest_date_dtype is ddl.AstropyTimeNsecTai:
            return astropy.time.Time
        elif self.ingest_date_dtype is sqlalchemy.TIMESTAMP:
            return datetime.datetime
        else:
            raise TypeError(f"Unexpected type of ingest_date_dtype: {self.ingest_date_dtype}")

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
from collections.abc import Iterable
from typing import cast

import astropy.time
import sqlalchemy
from lsst.daf.relation import ColumnTag, sql

from . import ddl
from ._column_tags import DatasetColumnTag, DimensionKeyColumnTag, DimensionRecordColumnTag
from .dimensions import Dimension, DimensionUniverse
from .timespan_database_representation import TimespanDatabaseRepresentation

LogicalColumn = sqlalchemy.sql.ColumnElement | TimespanDatabaseRepresentation
"""A type alias for the types used to represent columns in SQL relations.

This is the butler specialization of the `lsst.daf.relation.sql.LogicalColumn`
concept.
"""


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

    def make_relation_table_spec(
        self,
        columns: Iterable[ColumnTag],
        unique_keys: Iterable[Iterable[ColumnTag]] = (),
    ) -> ddl.TableSpec:
        """Create a specification for a table with the given relation columns.

        This is used primarily to create temporary tables for query results.

        Parameters
        ----------
        columns : `~collections.abc.Iterable` [ `ColumnTag` ]
            Iterable of column identifiers.
        unique_keys : `~collections.abc.Iterable` \
                [ `~collections.abc.Iterable` [ `ColumnTag` ] ]
            Unique constraints to add the table, as a nested iterable of
            (first) constraint and (second) the columns within that constraint.

        Returns
        -------
        spec : `ddl.TableSpec`
            Specification for a table.
        """
        result = ddl.TableSpec(fields=())
        columns = list(columns)
        if not columns:
            result.fields.add(
                ddl.FieldSpec(
                    sql.Engine.EMPTY_COLUMNS_NAME,
                    dtype=sql.Engine.EMPTY_COLUMNS_TYPE,
                    nullable=True,
                )
            )
        for tag in columns:
            match tag:
                case DimensionKeyColumnTag(dimension=dimension_name):
                    result.fields.add(
                        dataclasses.replace(
                            cast(Dimension, self.universe[dimension_name]).primaryKey,
                            name=tag.qualified_name,
                            primaryKey=False,
                            nullable=False,
                        )
                    )
                case DimensionRecordColumnTag(column="region"):
                    result.fields.add(ddl.FieldSpec.for_region(tag.qualified_name))
                case DimensionRecordColumnTag(column="timespan") | DatasetColumnTag(column="timespan"):
                    result.fields.update(
                        self.timespan_cls.makeFieldSpecs(nullable=True, name=tag.qualified_name)
                    )
                case DimensionRecordColumnTag(element=element_name, column=column):
                    element = self.universe[element_name]
                    result.fields.add(
                        dataclasses.replace(
                            element.RecordClass.fields.facts[column],
                            name=tag.qualified_name,
                            nullable=True,
                            primaryKey=False,
                        )
                    )
                case DatasetColumnTag(column="dataset_id"):
                    result.fields.add(
                        dataclasses.replace(
                            self.dataset_id_spec, name=tag.qualified_name, primaryKey=False, nullable=False
                        )
                    )
                case DatasetColumnTag(column="run"):
                    result.fields.add(
                        dataclasses.replace(
                            self.run_key_spec, name=tag.qualified_name, primaryKey=False, nullable=False
                        )
                    )
                case DatasetColumnTag(column="ingest_date"):
                    result.fields.add(
                        ddl.FieldSpec(tag.qualified_name, dtype=self.ingest_date_dtype, nullable=False)
                    )
                case _:
                    raise TypeError(f"Unexpected column tag {tag}.")
        for unique_key in unique_keys:
            result.unique.add(tuple(tag.qualified_name for tag in unique_key))
        return result

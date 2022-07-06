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
    "ColumnTag",
    "DatasetColumnTag",
    "DimensionKeyColumnTag",
    "DimensionRecordColumnTag",
)

import dataclasses
from collections.abc import Iterable
from typing import ClassVar, Literal, TypeVar, Union, final

_S = TypeVar("_S")


class _BaseColumnTag:

    __slots__ = ()

    @classmethod
    def filter_from(cls: type[_S], tags: Iterable[ColumnTag]) -> set[_S]:
        return {tag for tag in tags if type(tag) is cls}


@final
@dataclasses.dataclass(frozen=True, slots=True)
class DimensionKeyColumnTag(_BaseColumnTag):
    """An identifier for `~lsst.daf.relation.Relation` columns that represent
    a dimension primary key value.
    """

    dimension: str
    """Name of the dimension (`str`)."""

    def __str__(self) -> str:
        return self.dimension

    is_spatial_region: ClassVar[Literal[False]] = False
    """Whether this column is a spatial region (always `False`)."""

    is_timespan: ClassVar[Literal[False]] = False
    """Whether this column is a timespan (always `False`)."""

    @classmethod
    def generate(cls, dimensions: Iterable[str]) -> set[DimensionKeyColumnTag]:
        """Return an iterator over column tags from an iterable of dimension
        names.

        Parameters
        ----------
        dimensions : `Iterable` [ `str` ]
            Dimension names.

        Returns
        -------
        tags : `set` [ `DimensionKeyColumnTag` ]
            Set of column tags.
        """
        return {cls(d) for d in dimensions}


@final
@dataclasses.dataclass(frozen=True, slots=True)
class DimensionRecordColumnTag(_BaseColumnTag):
    """An identifier for `~lsst.daf.relation.Relation` columns that represent
    non-key columns in a dimension or dimension element record.
    """

    element: str
    """Name of the dimension element (`str`).
    """

    column: str
    """Name of the column (`str`)."""

    def __str__(self) -> str:
        return f"n!{self.element}:{self.column}"

    @property
    def is_spatial_region(self) -> bool:
        """Whether this column is a spatial region (`bool`)."""
        return self.column == "region"

    @property
    def is_timespan(self) -> bool:
        """Whether this column is a timespan (`bool`)."""
        return self.column == "timespan"

    @classmethod
    def generate(cls, element: str, columns: Iterable[str]) -> set[DimensionRecordColumnTag]:
        """Return an iterator over column tags from an iterable of column names
        for a single dimension element.

        Parameters
        ----------
        element : `str`
            Name of the dimension element.
        columns : `Iterable` [ `str` ]
            Column names.

        Returns
        -------
        tags : `set` [ `DimensionRecordColumnTag` ]
            Set of column tags.
        """
        return {cls(element, column) for column in columns}


@final
@dataclasses.dataclass(frozen=True, slots=True)
class DatasetColumnTag(_BaseColumnTag):
    """An identifier for `~lsst.daf.relation.Relation` columns that represent
    columns from a dataset query or subquery.
    """

    dataset_type: str
    """Name of the dataset type (`str`)."""

    column: str
    """Name of the column (`str`).

    Allowed values are:

    - "dataset_id" (autoincrement or UUID primary key)
    - "run" (collection primary key, not collection name)
    - "ingest_date"
    - "timespan" (validity range, or NULL for non-calibration collections)
    - "rank" (collection position in ordered search)
    """

    def __str__(self) -> str:
        return f"t!{self.dataset_type}:{self.column}"

    is_spatial_region: ClassVar[Literal[False]] = False
    """Whether this column is a spatial region (always `False`)."""

    @property
    def is_timespan(self) -> bool:
        """Whether this column is a timespan (`bool`)."""
        return self.column == "timespan"

    @classmethod
    def generate(cls, dataset_type: str, columns: Iterable[str]) -> set[DatasetColumnTag]:
        """Return an iterator over column tags from an iterable of column names
        for a single dataset type.

        Parameters
        ----------
        dataset_type : `str`
            Name of the dataset type.
        columns : `Iterable` [ `str` ]
            Column names.

        Returns
        -------
        tags : `set` [ `DatasetColumnTag` ]
            Set of column tags.
        """
        return {cls(dataset_type, column) for column in columns}


ColumnTag = Union[DimensionKeyColumnTag, DimensionRecordColumnTag, DatasetColumnTag]
"""A type alias for the union of column identifier types used by daf_butler.

This is the butler specialization of the `lsst.daf.relation.ColumnTag`
concept.
"""

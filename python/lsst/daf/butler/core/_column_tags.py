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

__all__ = (
    "DatasetColumnTag",
    "DimensionKeyColumnTag",
    "DimensionRecordColumnTag",
    "is_timespan_column",
)

import dataclasses
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, TypeVar, final

_S = TypeVar("_S")

if TYPE_CHECKING:
    from lsst.daf.relation import ColumnTag


class _BaseColumnTag:
    __slots__ = ()

    @classmethod
    def filter_from(cls: type[_S], tags: Iterable[Any]) -> set[_S]:
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

    @property
    def qualified_name(self) -> str:
        return self.dimension

    @property
    def is_key(self) -> bool:
        return True

    @classmethod
    def generate(cls, dimensions: Iterable[str]) -> list[DimensionKeyColumnTag]:
        """Return a list of column tags from an iterable of dimension
        names.

        Parameters
        ----------
        dimensions : `~collections.abc.Iterable` [ `str` ]
            Dimension names.

        Returns
        -------
        tags : `list` [ `DimensionKeyColumnTag` ]
            List of column tags.
        """
        return [cls(d) for d in dimensions]


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
        return f"{self.element}.{self.column}"

    @property
    def qualified_name(self) -> str:
        return f"n!{self.element}:{self.column}"

    @property
    def is_key(self) -> bool:
        return False

    @classmethod
    def generate(cls, element: str, columns: Iterable[str]) -> list[DimensionRecordColumnTag]:
        """Return a list of column tags from an iterable of column names
        for a single dimension element.

        Parameters
        ----------
        element : `str`
            Name of the dimension element.
        columns : `~collections.abc.Iterable` [ `str` ]
            Column names.

        Returns
        -------
        tags : `list` [ `DimensionRecordColumnTag` ]
            List of column tags.
        """
        return [cls(element, column) for column in columns]


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
        return f"{self.dataset_type}.{self.column}"

    @property
    def qualified_name(self) -> str:
        return f"t!{self.dataset_type}:{self.column}"

    @property
    def is_key(self) -> bool:
        return self.column == "dataset_id" or self.column == "run"

    @classmethod
    def generate(cls, dataset_type: str, columns: Iterable[str]) -> list[DatasetColumnTag]:
        """Return a list of column tags from an iterable of column names
        for a single dataset type.

        Parameters
        ----------
        dataset_type : `str`
            Name of the dataset type.
        columns : `~collections.abc.Iterable` [ `str` ]
            Column names.

        Returns
        -------
        tags : `list` [ `DatasetColumnTag` ]
            List of column tags.
        """
        return [cls(dataset_type, column) for column in columns]


def is_timespan_column(tag: ColumnTag) -> bool:
    """Test whether a column tag is a timespan.

    Parameters
    ----------
    tag : `ColumnTag`
        Column tag to test.

    Returns
    -------
    is_timespan : `bool`
        Whether the given column is a timespan.
    """
    match tag:
        case DimensionRecordColumnTag(column="timespan"):
            return True
        case DatasetColumnTag(column="timespan"):
            return True
    return False

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

__all__ = ("Postprocessing",)

from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING, ClassVar

import sqlalchemy
from lsst.sphgeom import DISJOINT, Region

from .._exceptions import CalibrationLookupError
from ..queries import tree as qt

if TYPE_CHECKING:
    from ..dimensions import DimensionElement


class Postprocessing:
    """A helper object that filters and checks SQL-query result rows to perform
    operations we can't [fully] perform in the SQL query.

    Notes
    -----
    Postprocessing objects are initialized with no parameters to do nothing
    when applied; they are modified as needed in place as the query is built.

    Postprocessing objects evaluate to `True` in a boolean context only when
    they might perform actual row filtering.  They may still perform checks
    when they evaluate to `False`.
    """

    def __init__(self) -> None:
        self.spatial_join_filtering = []
        self.spatial_where_filtering = []
        self.check_validity_match_count: bool = False
        self._limit: int | None = None

    VALIDITY_MATCH_COUNT: ClassVar[str] = "_VALIDITY_MATCH_COUNT"
    """The field name used for the special result column that holds the number
    of matching find-first calibration datasets for each data ID.

    When present, the value of this column must be one for all rows.
    """

    spatial_join_filtering: list[tuple[DimensionElement, DimensionElement]]
    """Pairs of dimension elements whose regions must overlap; rows with
    any non-overlap pair will be filtered out.
    """

    spatial_where_filtering: list[tuple[DimensionElement, Region]]
    """Dimension elements and regions that must overlap; rows with any
    non-overlap pair will be filtered out.
    """

    check_validity_match_count: bool
    """If `True`, result rows will include a special column that counts the
    number of matching datasets in each collection for each data ID, and
    postprocessing should check that the value of this column is one for
    every row (and raise `CalibrationLookupError` if it is not).
    """

    @property
    def limit(self) -> int | None:
        """The maximum number of rows to return, or `None` for no limit.

        This is only set when other postprocess filtering makes it impossible
        to apply directly in SQL.
        """
        return self._limit

    @limit.setter
    def limit(self, value: int | None) -> None:
        if value and not self:
            raise RuntimeError(
                "Postprocessing should only implement 'limit' if it needs to do spatial filtering."
            )
        self._limit = value

    def __bool__(self) -> bool:
        return bool(self.spatial_join_filtering or self.spatial_where_filtering)

    def gather_columns_required(self, columns: qt.ColumnSet) -> None:
        """Add all columns required to perform postprocessing to the given
        column set.

        Parameters
        ----------
        columns : `.queries.tree.ColumnSet`
            Column set to modify in place.
        """
        for element in self.iter_region_dimension_elements():
            columns.update_dimensions(element.minimal_group)
            columns.dimension_fields[element.name].add("region")

    def iter_region_dimension_elements(self) -> Iterator[DimensionElement]:
        """Iterate over the dimension elements whose regions are needed for
        postprocessing.

        Returns
        -------
        elements : `~collections.abc.Iterator` [ `DimensionElement` ]
            Iterator over dimension element objects.
        """
        for a, b in self.spatial_join_filtering:
            yield a
            yield b
        for element, _ in self.spatial_where_filtering:
            yield element

    def iter_missing(self, columns: qt.ColumnSet) -> Iterator[DimensionElement]:
        """Iterate over the columns needed for postprocessing that are not in
        the given `.queries.tree.ColumnSet`.

        Parameters
        ----------
        columns : `.queries.tree.ColumnSet`
            Columns that should not be returned by this method.  These are
            typically the columns included in a query even in the absence of
            postprocessing.

        Returns
        -------
        elements : `~collections.abc.Iterator` [ `DimensionElement` ]
            Iterator over dimension element objects.
        """
        done: set[DimensionElement] = set()
        for element in self.iter_region_dimension_elements():
            if element not in done:
                if "region" not in columns.dimension_fields.get(element.name, frozenset()):
                    yield element
                done.add(element)

    def apply(self, rows: Iterable[sqlalchemy.Row]) -> Iterable[sqlalchemy.Row]:
        """Apply the postprocessing to an iterable of SQL result rows.

        Parameters
        ----------
        rows : `~collections.abc.Iterable` [ `sqlalchemy.Row` ]
            Rows to process.

        Returns
        -------
        processed : `~collections.abc.Iterable` [ `sqlalchemy.Row` ]
            Rows that pass the postprocessing filters and checks.

        Notes
        -----
        This method decreases `limit` in place if it is not `None`, such that
        the same `Postprocessing` instance can be applied to each page in a
        sequence of result pages.  This means a single `Postprocessing` object
        can only be used for a single SQL query, and should be discarded when
        iteration over the results of that query is complete.
        """
        if not (self or self.check_validity_match_count):
            yield from rows
            return
        if self._limit == 0:
            return
        joins = [
            (
                qt.ColumnSet.get_qualified_name(a.name, "region"),
                qt.ColumnSet.get_qualified_name(b.name, "region"),
            )
            for a, b in self.spatial_join_filtering
        ]
        where = [
            (qt.ColumnSet.get_qualified_name(element.name, "region"), region)
            for element, region in self.spatial_where_filtering
        ]

        for row in rows:
            m = row._mapping
            if any(m[a].relate(m[b]) & DISJOINT for a, b in joins) or any(
                m[field].relate(region) & DISJOINT for field, region in where
            ):
                continue
            if self.check_validity_match_count and m[self.VALIDITY_MATCH_COUNT] > 1:
                raise CalibrationLookupError(
                    "Ambiguous calibration validity range match. This usually means a temporal join or "
                    "'where' needs to be added, but it could also mean that multiple validity ranges "
                    "overlap a single output data ID."
                )
            yield row
            if self._limit is not None:
                self._limit -= 1
                if self._limit == 0:
                    return

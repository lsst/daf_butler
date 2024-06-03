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

__all__ = ("DimensionRecordQueryResults",)

from collections.abc import Iterator
from typing import Any, final

from ..dimensions import (
    DimensionElement,
    DimensionGroup,
    DimensionRecord,
    DimensionRecordSet,
    DimensionRecordTable,
)
from ._base import QueryResultsBase
from .driver import QueryDriver
from .result_specs import DimensionRecordResultSpec
from .tree import QueryTree


@final
class DimensionRecordQueryResults(QueryResultsBase):
    """A query for `DimensionRecord` results.

    Parameters
    ----------
    driver : `QueryDriver`
        Implementation object that knows how to actually execute queries.
    tree : `QueryTree`
        Description of the query as a tree of joins and column expressions.
        The instance returned directly by the `Butler._query` entry point
        should be constructed via `make_unit_query_tree`.
    spec : `DimensionRecordResultSpec`
        Specification of the query result rows, including output columns,
        ordering, and slicing.

    Notes
    -----
    This class should never be constructed directly by users; use
    `Query.dimension_records` instead.
    """

    def __init__(self, driver: QueryDriver, tree: QueryTree, spec: DimensionRecordResultSpec):
        spec.validate_tree(tree)
        super().__init__(driver, tree)
        self._spec = spec

    def __iter__(self) -> Iterator[DimensionRecord]:
        for page in self._driver.execute(self._spec, self._tree):
            yield from page.rows

    def iter_table_pages(self) -> Iterator[DimensionRecordTable]:
        """Return an iterator over individual pages of results as table-backed
        collections.

        Yields
        ------
        table : `DimensionRecordTable`
            A table-backed collection of dimension records.
        """
        for page in self._driver.execute(self._spec, self._tree):
            yield page.as_table()

    def iter_set_pages(self) -> Iterator[DimensionRecordSet]:
        """Return an iterator over individual pages of results as set-backed
        collections.

        Yields
        ------
        table : `DimensionRecordSet`
            A set-backed collection of dimension records.
        """
        for page in self._driver.execute(self._spec, self._tree):
            yield page.as_set()

    @property
    def element(self) -> DimensionElement:
        # Docstring inherited.
        return self._spec.element

    @property
    def dimensions(self) -> DimensionGroup:
        # Docstring inherited
        return self._spec.dimensions

    def count(self, *, exact: bool = True, discard: bool = False) -> int:
        # Docstring inherited.
        return self._driver.count(self._tree, self._spec, exact=exact, discard=discard)

    def _copy(self, tree: QueryTree, **kwargs: Any) -> DimensionRecordQueryResults:
        # Docstring inherited.
        return DimensionRecordQueryResults(self._driver, tree, self._spec.model_copy(update=kwargs))

    def _get_datasets(self) -> frozenset[str]:
        # Docstring inherited.
        return frozenset()

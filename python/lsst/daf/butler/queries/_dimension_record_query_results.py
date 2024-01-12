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
from typing import Any

from ..dimensions import DimensionElement, DimensionRecord, DimensionRecordSet, DimensionRecordTable
from ._base import QueryResultsBase
from .driver import QueryDriver
from .result_specs import DimensionRecordResultSpec
from .tree import QueryTree


class DimensionRecordQueryResults(QueryResultsBase):
    """A method-chaining builder for butler queries that return data IDs.

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
    This refines the `DimensionRecordQueryResults` ABC defined in
    `lsst.daf.butler._query_results`, but the intent is to replace that ABC
    with this concrete class, rather than inherit from it.
    """

    def __init__(self, driver: QueryDriver, tree: QueryTree, spec: DimensionRecordResultSpec):
        super().__init__(driver, tree)
        self._spec = spec

    def __iter__(self) -> Iterator[DimensionRecord]:
        page = self._driver.execute(self._tree, self._spec)
        yield from page.rows
        while page.next_key is not None:
            page = self._driver.fetch_next_page(self._spec, page.next_key)
            yield from page.rows

    def iter_table_pages(self) -> Iterator[DimensionRecordTable]:
        page = self._driver.execute(self._tree, self._spec)
        yield page.as_table()
        while page.next_key is not None:
            page = self._driver.fetch_next_page(self._spec, page.next_key)
            yield page.as_table()

    def iter_set_pages(self) -> Iterator[DimensionRecordSet]:
        page = self._driver.execute(self._tree, self._spec)
        yield page.as_set()
        while page.next_key is not None:
            page = self._driver.fetch_next_page(self._spec, page.next_key)
            yield page.as_set()

    @property
    def element(self) -> DimensionElement:
        # Docstring inherited.
        return self._spec.element

    def _copy(self, tree: QueryTree, **kwargs: Any) -> DimensionRecordQueryResults:
        return DimensionRecordQueryResults(self._driver, tree, self._spec.model_copy(update=kwargs))

    def _get_datasets(self) -> frozenset[str]:
        return frozenset()

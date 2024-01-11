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
    "DimensionRecordResultSpec",
    "DimensionRecordResultPage",
)

import dataclasses
from collections.abc import Iterable, Iterator
from typing import Literal

import pydantic

from .._query_results import DimensionRecordQueryResults
from ..dimensions import (
    DimensionElement,
    DimensionGroup,
    DimensionRecord,
    DimensionRecordSet,
    DimensionRecordTable,
)
from .convert_args import convert_order_by_args
from .driver import PageKey, QueryDriver
from .relation_tree import RootRelation


class DimensionRecordResultSpec(pydantic.BaseModel):
    """Specification for a query that yields `DimensionRecord` objects."""

    result_type: Literal["dimension_record"] = "dimension_record"
    element: DimensionElement

    @property
    def dimensions(self) -> DimensionGroup:
        return self.element.minimal_group


@dataclasses.dataclass
class DimensionRecordResultPage:
    """A single page of results from a dimension record query."""

    spec: DimensionRecordResultSpec
    next_key: PageKey | None
    rows: Iterable[DimensionRecord]

    def as_table(self) -> DimensionRecordTable:
        if isinstance(self.rows, DimensionRecordTable):
            return self.rows
        else:
            return DimensionRecordTable(self.spec.element, self.rows)

    def as_set(self) -> DimensionRecordSet:
        if isinstance(self.rows, DimensionRecordSet):
            return self.rows
        else:
            return DimensionRecordSet(self.spec.element, self.rows)


class RelationDimensionRecordQueryResults(DimensionRecordQueryResults):
    """Implementation of DimensionRecordQueryResults for the relation-based
    query system.

    Parameters
    ----------
    driver : `QueryDriver`
        Implementation object that knows how to actually execute queries.
    tree : `RootRelation`
        Description of the query as a tree of relation operations.  The
        instance returned directly by the `Butler._query` entry point should
        be constructed via `make_unit_relation`.
    spec : `DimensionRecordResultSpec`
        Specification for the details of the records to return.

    Notes
    -----
    Ideally this will eventually just be "DimensionRecordQueryResults", because
    we won't need an ABC if this is the only implementation.
    """

    def __init__(self, driver: QueryDriver, tree: RootRelation, spec: DimensionRecordResultSpec):
        self._driver = driver
        self._tree = tree
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

    def run(self) -> DimensionRecordQueryResults:
        # Docstring inherited.
        raise NotImplementedError("TODO: remove this from the base class.")

    def count(self, *, exact: bool = True, discard: bool = False) -> int:
        # Docstring inherited.
        return self._driver.count(self._tree, exact=exact, discard=discard)

    def any(self, *, execute: bool = True, exact: bool = True) -> bool:
        # Docstring inherited.
        return self._driver.any(self._tree, execute=execute, exact=exact)

    def order_by(self, *args: str) -> DimensionRecordQueryResults:
        # Docstring inherited.
        return RelationDimensionRecordQueryResults(
            driver=self._driver,
            tree=self._tree.order_by(*convert_order_by_args(self._tree, *args)),
            spec=self._spec,
        )

    def limit(self, limit: int | None = None, offset: int = 0) -> DimensionRecordQueryResults:
        # Docstring inherited.
        return RelationDimensionRecordQueryResults(
            driver=self._driver,
            tree=self._tree.order_by(limit=limit, offset=offset),
            spec=self._spec,
        )

    def explain_no_results(self, execute: bool = True) -> Iterable[str]:
        # Docstring inherited.
        return self._driver.explain_no_results(self._tree, execute=execute)

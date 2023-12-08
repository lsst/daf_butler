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
    "DataCoordinateResultSpec",
    "DataCoordinateResultPage",
    "RelationDataCoordinateQueryResults",
)

from collections.abc import Iterable, Iterator
from contextlib import AbstractContextManager
from typing import TYPE_CHECKING, Any, Literal

import pydantic

from .._dataset_ref import DatasetRef
from .._dataset_type import DatasetType
from .._query_results import DataCoordinateQueryResults, DatasetQueryResults
from ..dimensions import DataCoordinate, DimensionGroup
from .driver import QueryDriver
from .relation_tree import RootRelation

if TYPE_CHECKING:
    from .driver import PageKey


class DataCoordinateResultSpec(pydantic.BaseModel):
    """Specification for a query that yields `DataCoordinate` objects."""

    result_type: Literal["data_coordinate"] = "data_coordinate"
    dimensions: DimensionGroup
    include_dimension_records: bool


class DataCoordinateResultPage(pydantic.BaseModel):
    """A single page of results from a data coordinate query."""

    spec: DataCoordinateResultSpec
    next_key: PageKey | None

    # TODO: On DM-41114 this will become a custom container that normalizes out
    # attached DimensionRecords and is Pydantic-friendly.  Right now this model
    # isn't actually serializable.
    rows: list[DataCoordinate]


class RelationDataCoordinateQueryResults(DataCoordinateQueryResults):
    """Implementation of DataCoordinateQueryResults for the relation-based
    query system.



    Notes
    -----
    Ideally this will eventually just be "DataCoordinateQueryResults", because
    we won't need an ABC if this is the only implementation.
    """

    def __init__(self, tree: RootRelation, driver: QueryDriver, spec: DataCoordinateResultSpec):
        self._tree = tree
        self._driver = driver
        self._spec = spec

    @property
    def dimensions(self) -> DimensionGroup:
        return self._spec.dimensions

    def __iter__(self) -> Iterator[DataCoordinate]:
        page = self._driver.execute(self._tree, self._spec)
        yield from page.rows
        while page.next_key is not None:
            page = self._driver.fetch_next_page(self._spec, page.next_key)
            yield from page.rows

    def has_full(self) -> bool:  # TODO: since this is always true, we may not need it.
        return True

    def has_records(self) -> bool:  # TODO: should this be a property now?
        return self._spec.include_dimension_records

    def materialize(self) -> AbstractContextManager[DataCoordinateQueryResults]:
        raise NotImplementedError()

    def expanded(self) -> DataCoordinateQueryResults:
        if self.has_records():
            return self
        return RelationDataCoordinateQueryResults(
            tree=self._tree,
            driver=self._driver,
            spec=DataCoordinateResultSpec(dimensions=self._spec.dimensions, include_dimension_records=True),
        )

    def subset(
        self,
        dimensions: DimensionGroup | Iterable[str] | None = None,
        *,
        unique: bool = False,
    ) -> DataCoordinateQueryResults:
        raise NotImplementedError(
            "TODO: Copy with a new result spec and/or DimensionProjection pushed onto tree."
        )

    def find_datasets(
        self, dataset_type: DatasetType | str, collections: Any, *, find_first: bool = True
    ) -> DatasetQueryResults:
        raise NotImplementedError("TODO: Copy with a new result spec and maybe a new DatasetSearch in tree.")

    def find_related_datasets(
        self,
        dataset_type: DatasetType | str,
        collections: Any,
        *,
        find_first: bool = True,
        dimensions: DimensionGroup | Iterable[str] | None = None,
    ) -> Iterable[tuple[DataCoordinate, DatasetRef]]:
        raise NotImplementedError("TODO: drop this in favor of GeneralQueryResults")

    def count(self, *, exact: bool = True, discard: bool = False) -> int:
        return self._driver.count(self._tree, exact=exact, discard=discard)

    def any(self, *, execute: bool = True, exact: bool = True) -> bool:
        return self._driver.any(self._tree, execute=execute, exact=exact)

    def explain_no_results(self, execute: bool = True) -> Iterable[str]:
        return self._driver.explain_no_results(self._tree, execute=execute)

    def order_by(self, *args: str) -> DataCoordinateQueryResults:
        raise NotImplementedError("TODO: Copy with a OrderedSlice pushed onto tree.")

    def limit(self, limit: int, offset: int | None = 0) -> DataCoordinateQueryResults:
        raise NotImplementedError("TODO: Copy with a OrderedSlice pushed onto tree.")

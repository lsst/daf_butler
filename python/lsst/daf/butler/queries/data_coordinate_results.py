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
    "DataCoordinateQueryResults2",
)

from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Literal

import pydantic

from .._dataset_ref import DatasetRef
from .._dataset_type import DatasetType
from .._query_results import DataCoordinateQueryResults, DatasetQueryResults
from ..dimensions import DataCoordinate, DimensionGroup
from .convert_args import convert_order_by_args
from .driver import QueryDriver
from .tree import InvalidQueryTreeError, MaterializationSpec, QueryTree, make_unit_query_tree

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


class DataCoordinateQueryResults2(DataCoordinateQueryResults):
    """Implementation of `DataCoordinateQueryResults` for the tree-based
    query system.

    Parameters
    ----------
    driver : `QueryDriver`
        Implementation object that knows how to actually execute queries.
    tree : `QueryTree`
        Description of the query as a tree of joins and column expressions.
    spec : `DataCoordinateResultSpec`
        Specification for the details of the data IDs to return.

    Notes
    -----
    Ideally this will eventually just be "DataCoordinateQueryResults", because
    we won't need an ABC if this is the only implementation.
    """

    def __init__(self, driver: QueryDriver, tree: QueryTree, spec: DataCoordinateResultSpec):
        self._driver = driver
        self._tree = tree
        self._spec = spec

    @property
    def dimensions(self) -> DimensionGroup:
        # Docstring inherited.
        return self._spec.dimensions

    def __iter__(self) -> Iterator[DataCoordinate]:
        page = self._driver.execute(self._tree, self._spec)
        yield from page.rows
        while page.next_key is not None:
            page = self._driver.fetch_next_page(self._spec, page.next_key)
            yield from page.rows

    def has_full(self) -> bool:  # TODO: since this is always true, we may not need it.
        # Docstring inherited.
        return True

    def has_records(self) -> bool:  # TODO: should this be a property now?
        # Docstring inherited.
        return self._spec.include_dimension_records

    @contextmanager
    def materialize(self) -> Iterator[DataCoordinateQueryResults]:
        # Docstring inherited.
        key, _ = self._driver.materialize(self._tree, dimensions=self.dimensions, datasets=frozenset())
        yield DataCoordinateQueryResults2(
            self._driver,
            tree=make_unit_query_tree(self._driver.universe).join_materialization(
                key,
                MaterializationSpec.model_construct(
                    dimensions=self.dimensions, resolved_datasets=frozenset()
                ),
            ),
            spec=self._spec,
        )
        # TODO: Right now we just rely on the QueryDriver context instead of
        # using this one.  If we want this to remain a context manager, we
        # should make it do something, e.g. by adding QueryDriver method to
        # drop a materialization.

    def expanded(self) -> DataCoordinateQueryResults:
        # Docstring inherited.
        if self.has_records():
            return self
        return DataCoordinateQueryResults2(
            self._driver,
            tree=self._tree,
            spec=DataCoordinateResultSpec.model_construct(
                dimensions=self._spec.dimensions, include_dimension_records=True
            ),
        )

    def subset(
        self,
        dimensions: DimensionGroup | Iterable[str] | None = None,
        *,
        unique: bool = False,
    ) -> DataCoordinateQueryResults:
        # Docstring inherited.
        if dimensions is None:
            dimensions = self.dimensions
        else:
            dimensions = self._driver.universe.conform(dimensions)
            if not dimensions <= self.dimensions:
                raise InvalidQueryTreeError(
                    f"New dimensions {dimensions} are not a subset of the current "
                    f"dimensions {self.dimensions}."
                )
        # TODO: right now I'm assuming we'll deduplicate all query results (per
        # page), even if we have to do that in Python, so the 'unique' argument
        # doesn't do anything.
        return DataCoordinateQueryResults2(
            self._driver,
            tree=self._tree,
            spec=DataCoordinateResultSpec.model_construct(
                dimensions=dimensions, include_dimension_records=self._spec.include_dimension_records
            ),
        )

    def find_datasets(
        self, dataset_type: DatasetType | str, collections: Any, *, find_first: bool = True
    ) -> DatasetQueryResults:
        # Docstring inherited.
        raise NotImplementedError("TODO: Copy with a new result spec and maybe a new DatasetSearch in tree.")

    def find_related_datasets(
        self,
        dataset_type: DatasetType | str,
        collections: Any,
        *,
        find_first: bool = True,
        dimensions: DimensionGroup | Iterable[str] | None = None,
    ) -> Iterable[tuple[DataCoordinate, DatasetRef]]:
        # Docstring inherited.
        raise NotImplementedError("TODO: drop this in favor of GeneralQueryResults")

    def count(self, *, exact: bool = True, discard: bool = False) -> int:
        # Docstring inherited.
        return self._driver.count(
            self._tree, dimensions=self._spec.dimensions, datasets=frozenset(), exact=exact, discard=discard
        )

    def any(self, *, execute: bool = True, exact: bool = True) -> bool:
        # Docstring inherited.
        return self._driver.any(self._tree, execute=execute, exact=exact)

    def explain_no_results(self, execute: bool = True) -> Iterable[str]:
        # Docstring inherited.
        return self._driver.explain_no_results(self._tree, execute=execute)

    def order_by(self, *args: str) -> DataCoordinateQueryResults:
        # Docstring inherited.
        return DataCoordinateQueryResults2(
            driver=self._driver,
            tree=self._tree.order_by(*convert_order_by_args(self._tree, *args)),
            spec=self._spec,
        )

    def limit(self, limit: int | None = None, offset: int = 0) -> DataCoordinateQueryResults:
        # Docstring inherited.
        return DataCoordinateQueryResults2(
            driver=self._driver,
            tree=self._tree.order_by(limit=limit, offset=offset),
            spec=self._spec,
        )

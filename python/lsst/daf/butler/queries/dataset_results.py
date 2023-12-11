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
    "DatasetRefResultSpec",
    "DatasetRefResultPage",
    "RelationSingleTypeDatasetQueryResults",
)

import itertools
from collections.abc import Iterable, Iterator
from contextlib import ExitStack, contextmanager
from typing import Literal

import pydantic

from .._dataset_ref import DatasetRef
from .._dataset_type import DatasetType
from .._query_results import SingleTypeDatasetQueryResults
from .data_coordinate_results import (
    DataCoordinateResultSpec,
    DatasetQueryResults,
    RelationDataCoordinateQueryResults,
)
from .driver import PageKey, QueryDriver
from .relation_tree import Materialization, RootRelation, make_unit_relation


class DatasetRefResultSpec(pydantic.BaseModel):
    """Specification for a query that yields `DatasetRef` objects."""

    result_type: Literal["dataset_ref"] = "dataset_ref"
    dataset_type: DatasetType
    include_dimension_records: bool


class DatasetRefResultPage(pydantic.BaseModel):
    """A single page of results from a dataset ref query."""

    spec: DatasetRefResultSpec
    next_key: PageKey | None

    # TODO: On DM-41115 this will become a custom container that normalizes out
    # attached DimensionRecords and is Pydantic-friendly.  Right now this model
    # isn't actually serializable.
    rows: list[DatasetRef]


class RelationSingleTypeDatasetQueryResults(SingleTypeDatasetQueryResults):
    """Implementation of `SingleTypeDatasetQueryResults` for the relation-based
    query system.

    Parameters
    ----------
    driver : `QueryDriver`
        Implementation object that knows how to actually execute queries.
    tree : `RootRelation`
        Description of the query as a tree of relation operations.  The
        instance returned directly by the `Butler._query` entry point should be
        constructed via `make_unit_relation`.
    spec : `DatasetRefResultSpec`
        Specification for the details of the dataset references to return.

    Notes
    -----
    Ideally this will eventually just be "SingleTypeDatasetQueryResults",
    because we won't need an ABC if this is the only implementation.
    """

    def __init__(self, driver: QueryDriver, tree: RootRelation, spec: DatasetRefResultSpec):
        self._driver = driver
        self._tree = tree
        self._spec = spec

    def __iter__(self) -> Iterator[DatasetRef]:
        page = self._driver.execute(self._tree, self._spec)
        yield from page.rows
        while page.next_key is not None:
            page = self._driver.fetch_next_page(self._spec, page.next_key)
            yield from page.rows

    @contextmanager
    def materialize(self) -> Iterator[RelationSingleTypeDatasetQueryResults]:
        # Docstring inherited from DatasetQueryResults.
        key = self._driver.materialize(self._tree, frozenset())
        yield RelationSingleTypeDatasetQueryResults(
            self._driver,
            tree=make_unit_relation(self._driver.universe).join(
                Materialization.model_construct(
                    key=key, operand=self._tree, dataset_types=frozenset({self.dataset_type.name})
                )
            ),
            spec=self._spec,
        )
        # TODO: Right now we just rely on the QueryDriver context instead of
        # using this one.  If we want this to remain a context manager, we
        # should make it do something, e.g. by adding QueryDriver method to
        # drop a materialization.

    @property
    def dataset_type(self) -> DatasetType:
        # Docstring inherited.
        return self._spec.dataset_type

    @property
    def data_ids(self) -> RelationDataCoordinateQueryResults:
        # Docstring inherited.
        return RelationDataCoordinateQueryResults(
            self._driver,
            tree=self._tree,
            spec=DataCoordinateResultSpec.model_construct(
                dimensions=self.dataset_type.dimensions.as_group(),
                include_dimension_records=self._spec.include_dimension_records,
            ),
        )

    def expanded(self) -> RelationSingleTypeDatasetQueryResults:
        # Docstring inherited.
        if self._spec.include_dimension_records:
            return self
        return RelationSingleTypeDatasetQueryResults(
            self._driver,
            tree=self._tree,
            spec=DatasetRefResultSpec.model_construct(
                dataset_type=self.dataset_type, include_dimension_records=True
            ),
        )

    def by_dataset_type(self) -> Iterator[SingleTypeDatasetQueryResults]:
        # Docstring inherited.
        return iter((self,))

    def count(self, *, exact: bool = True, discard: bool = False) -> int:
        # Docstring inherited.
        return self._driver.count(self._tree, exact=exact, discard=discard)

    def any(self, *, execute: bool = True, exact: bool = True) -> bool:
        # Docstring inherited.
        return self._driver.any(self._tree, execute=execute, exact=exact)

    def explain_no_results(self, execute: bool = True) -> Iterable[str]:
        # Docstring inherited.
        return self._driver.explain_no_results(self._tree, execute=execute)


class ChainedDatasetQueryResults(DatasetQueryResults):
    """Implementation of `DatasetQueryResults` that delegates to a sequence
    of `SingleTypeDatasetQueryResults`.

    Parameters
    ----------
    by_dataset_type : `tuple` [ `SingleTypeDatasetQueryResults` ]
        Tuple of single-dataset-type query result objects to combine.

    Notes
    -----
    Ideally this will eventually just be "DatasetQueryResults", because we
    won't need an ABC if this is the only implementation.
    """

    def __init__(self, by_dataset_type: tuple[SingleTypeDatasetQueryResults, ...]):
        self._by_dataset_type = by_dataset_type

    def __iter__(self) -> Iterator[DatasetRef]:
        return itertools.chain.from_iterable(self._by_dataset_type)

    def by_dataset_type(self) -> Iterator[SingleTypeDatasetQueryResults]:
        # Docstring inherited.
        return iter(self._by_dataset_type)

    @contextmanager
    def materialize(self) -> Iterator[DatasetQueryResults]:
        # Docstring inherited.
        with ExitStack() as exit_stack:
            yield ChainedDatasetQueryResults(
                tuple(
                    [
                        exit_stack.enter_context(single_type_results.materialize())
                        for single_type_results in self._by_dataset_type
                    ]
                )
            )

    def expanded(self) -> ChainedDatasetQueryResults:
        # Docstring inherited.
        return ChainedDatasetQueryResults(
            tuple([single_type_results.expanded() for single_type_results in self._by_dataset_type])
        )

    def count(self, *, exact: bool = True, discard: bool = False) -> int:
        # Docstring inherited.
        return sum(
            single_type_results.count(exact=exact, discard=discard)
            for single_type_results in self._by_dataset_type
        )

    def any(self, *, execute: bool = True, exact: bool = True) -> bool:
        # Docstring inherited.
        return any(
            single_type_results.any(execute=execute, exact=exact)
            for single_type_results in self._by_dataset_type
        )

    def explain_no_results(self, execute: bool = True) -> Iterable[str]:
        # Docstring inherited.
        messages: list[str] = []
        for single_type_results in self._by_dataset_type:
            messages.extend(single_type_results.explain_no_results(execute=execute))
        return messages

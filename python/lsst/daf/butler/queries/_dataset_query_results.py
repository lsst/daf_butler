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
    "DatasetQueryResults",
    "ChainedDatasetQueryResults",
    "SingleTypeDatasetQueryResults",
)

import itertools
from abc import abstractmethod
from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING, Any

from .._dataset_ref import DatasetRef
from .._dataset_type import DatasetType
from ._base import CountableQueryBase, QueryResultsBase
from .driver import QueryDriver
from .result_specs import DatasetRefResultSpec
from .tree import QueryTree

if TYPE_CHECKING:
    from ._data_coordinate_query_results import DataCoordinateQueryResults


class DatasetQueryResults(CountableQueryBase, Iterable[DatasetRef]):
    """An interface for objects that represent the results of queries for
    datasets.
    """

    @abstractmethod
    def by_dataset_type(self) -> Iterator[SingleTypeDatasetQueryResults]:
        """Group results by dataset type.

        Returns
        -------
        iter : `~collections.abc.Iterator` [ `SingleTypeDatasetQueryResults` ]
            An iterator over `DatasetQueryResults` instances that are each
            responsible for a single dataset type.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def has_dimension_records(self) -> bool:
        """Whether all data IDs in this iterable contain dimension records."""
        raise NotImplementedError()

    @abstractmethod
    def with_dimension_records(self) -> DatasetQueryResults:
        """Return a results object for which `has_dimension_records` is
        `True`.
        """
        raise NotImplementedError()


class SingleTypeDatasetQueryResults(DatasetQueryResults, QueryResultsBase):
    """A method-chaining builder for butler queries that return `DatasetRef`
    objects.

    Parameters
    ----------
    driver : `QueryDriver`
        Implementation object that knows how to actually execute queries.
    tree : `QueryTree`
        Description of the query as a tree of joins and column expressions.
        The instance returned directly by the `Butler._query` entry point
        should be constructed via `make_unit_query_tree`.
    spec : `DatasetRefResultSpec`
        Specification of the query result rows, including output columns,
        ordering, and slicing.

    Notes
    -----
    This refines the `SingleTypeDatasetQueryResults` ABC defined in
    `lsst.daf.butler._query_results`, but the intent is to replace that ABC
    with this concrete class, rather than inherit from it.
    """

    def __init__(self, driver: QueryDriver, tree: QueryTree, spec: DatasetRefResultSpec):
        super().__init__(driver, tree)
        self._spec = spec

    def __iter__(self) -> Iterator[DatasetRef]:
        page = self._driver.execute(self._spec, self._tree)
        yield from page.rows
        while page.next_key is not None:
            page = self._driver.fetch_next_page(self._spec, page.next_key)
            yield from page.rows

    @property
    def dataset_type(self) -> DatasetType:
        # Docstring inherited.
        return self._spec.dataset_type

    @property
    def data_ids(self) -> DataCoordinateQueryResults:
        # Docstring inherited.
        from ._data_coordinate_query_results import DataCoordinateQueryResults, DataCoordinateResultSpec

        return DataCoordinateQueryResults(
            self._driver,
            tree=self._tree,
            spec=DataCoordinateResultSpec.model_construct(
                dimensions=self.dataset_type.dimensions.as_group(),
                include_dimension_records=self._spec.include_dimension_records,
            ),
        )

    @property
    def has_dimension_records(self) -> bool:
        # Docstring inherited.
        return self._spec.include_dimension_records

    def with_dimension_records(self) -> SingleTypeDatasetQueryResults:
        # Docstring inherited.
        if self.has_dimension_records:
            return self
        return self._copy(tree=self._tree, include_dimension_records=True)

    def by_dataset_type(self) -> Iterator[SingleTypeDatasetQueryResults]:
        # Docstring inherited.
        return iter((self,))

    def _copy(self, tree: QueryTree, **kwargs: Any) -> SingleTypeDatasetQueryResults:
        return SingleTypeDatasetQueryResults(self._driver, self._tree, self._spec.model_copy(update=kwargs))

    def _get_datasets(self) -> frozenset[str]:
        return frozenset({self.dataset_type.name})


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

    @property
    def has_dimension_records(self) -> bool:
        # Docstring inherited.
        return all(single_type_results.has_dimension_records for single_type_results in self._by_dataset_type)

    def with_dimension_records(self) -> ChainedDatasetQueryResults:
        # Docstring inherited.
        return ChainedDatasetQueryResults(
            tuple(
                [
                    single_type_results.with_dimension_records()
                    for single_type_results in self._by_dataset_type
                ]
            )
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

    def count(self, *, exact: bool = True, discard: bool = False) -> int:
        return sum(
            single_type_results.count(exact=exact, discard=discard)
            for single_type_results in self._by_dataset_type
        )
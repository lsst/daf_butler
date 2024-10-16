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

__all__ = ("GeneralQueryResults", "GeneralResultTuple")

import itertools
from collections.abc import Iterator
from typing import Any, NamedTuple, final

from .._dataset_ref import DatasetRef
from .._dataset_type import DatasetType
from ..dimensions import DataCoordinate, DimensionGroup
from ._base import QueryResultsBase
from .driver import QueryDriver
from .result_specs import GeneralResultSpec
from .tree import QueryTree


class GeneralResultTuple(NamedTuple):
    """Helper class for general result that represents the result row as a
    data coordinate and optionally a set of dataset refs extracted from a row.
    """

    data_id: DataCoordinate
    """Data coordinate for current row."""

    refs: list[DatasetRef]
    """Dataset refs extracted from the current row, the order matches the order
    of arguments in ``iter_tuples`` call."""

    raw_row: dict[str, Any]
    """Original result row, the keys are the names of the dimensions,
    dimension fields (separated from dimension by dot) or dataset type fields
    (separated from dataset type name by dot).
    """


@final
class GeneralQueryResults(QueryResultsBase):
    """A query for `DatasetRef` results with a single dataset type.

    Parameters
    ----------
    driver : `QueryDriver`
        Implementation object that knows how to actually execute queries.
    tree : `QueryTree`
        Description of the query as a tree of joins and column expressions. The
        instance returned directly by the `Butler._query` entry point should be
        constructed via `make_unit_query_tree`.
    spec : `GeneralResultSpec`
        Specification of the query result rows, including output columns,
        ordering, and slicing.

    Notes
    -----
    This class should never be constructed directly by users; use `Query`
    methods instead.
    """

    def __init__(self, driver: QueryDriver, tree: QueryTree, spec: GeneralResultSpec):
        spec.validate_tree(tree)
        super().__init__(driver, tree)
        self._spec = spec

    def __iter__(self) -> Iterator[dict[str, Any]]:
        """Iterate over result rows.

        Yields
        ------
        row_dict : `dict` [`str`, `Any`]
            Result row as dictionary, the keys are the names of the dimensions,
            dimension fields (separated from dimension by dot) or dataset type
            fields (separated from dataset type name by dot).
        """
        for page in self._driver.execute(self._spec, self._tree):
            columns = tuple(str(column) for column in page.spec.get_result_columns())
            for row in page.rows:
                yield dict(zip(columns, row))

    def iter_tuples(self, *dataset_types: DatasetType) -> Iterator[GeneralResultTuple]:
        """Iterate over result rows and return data coordinate, and dataset
        refs constructed from each row, and an original row.

        Parameters
        ----------
        *dataset_types : `DatasetType`
            Zero or more types of the datasets to return.

        Yields
        ------
        row_tuple : `GeneralResultTuple`
            Structure containing data coordinate, refs, and a copy of the row.
        """
        all_dimensions = self._spec.dimensions
        dataset_keys: list[tuple[DatasetType, DimensionGroup, str, str]] = []
        for dataset_type in dataset_types:
            dimensions = dataset_type.dimensions
            id_key = f"{dataset_type.name}.dataset_id"
            run_key = f"{dataset_type.name}.run"
            dataset_keys.append((dataset_type, dimensions, id_key, run_key))
        for row in self:
            values = tuple(
                row[key] for key in itertools.chain(all_dimensions.required, all_dimensions.implied)
            )
            data_coordinate = DataCoordinate.from_full_values(all_dimensions, values)
            refs = []
            for dataset_type, dimensions, id_key, run_key in dataset_keys:
                values = tuple(row[key] for key in itertools.chain(dimensions.required, dimensions.implied))
                data_id = DataCoordinate.from_full_values(dimensions, values)
                refs.append(DatasetRef(dataset_type, data_id, row[run_key], id=row[id_key]))
            yield GeneralResultTuple(data_id=data_coordinate, refs=refs, raw_row=row)

    @property
    def dimensions(self) -> DimensionGroup:
        # Docstring inherited
        return self._spec.dimensions

    def count(self, *, exact: bool = True, discard: bool = False) -> int:
        # Docstring inherited.
        return self._driver.count(self._tree, self._spec, exact=exact, discard=discard)

    def _copy(self, tree: QueryTree, **kwargs: Any) -> GeneralQueryResults:
        # Docstring inherited.
        return GeneralQueryResults(self._driver, tree, self._spec.model_copy(update=kwargs))

    def _get_datasets(self) -> frozenset[str]:
        # Docstring inherited.
        return frozenset(self._spec.dataset_fields)

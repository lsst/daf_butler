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
from ..dimensions import DataCoordinate, DimensionElement, DimensionGroup, DimensionRecord, DimensionRecordSet
from ._base import QueryResultsBase
from .driver import QueryDriver
from .result_specs import GeneralResultSpec
from .tree import QueryTree, ResultColumn


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
                result = dict(zip(columns, row, strict=True))
                if page.dimension_records:
                    records = self._get_cached_dimension_records(result, page.dimension_records)
                    self._add_dimension_records(result, records)
                yield result

    def iter_tuples(self, *dataset_types: DatasetType) -> Iterator[GeneralResultTuple]:
        """Iterate over result rows and return data coordinate, and dataset
        refs constructed from each row, and an original row.

        This object has to include "dataset_id" and "run" columns for each type
        in ``dataset_types``.

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
        for page in self._driver.execute(self._spec, self._tree):
            columns = tuple(str(column) for column in page.spec.get_result_columns())
            for page_row in page.rows:
                row = dict(zip(columns, page_row, strict=True))
                if page.dimension_records:
                    cached_records = self._get_cached_dimension_records(row, page.dimension_records)
                    self._add_dimension_records(row, cached_records)
                else:
                    cached_records = {}
                data_coordinate = self._make_data_id(row, all_dimensions, cached_records)
                refs = []
                for dataset_type, dimensions, id_key, run_key in dataset_keys:
                    data_id = data_coordinate.subset(dimensions)
                    refs.append(DatasetRef(dataset_type, data_id, row[run_key], id=row[id_key]))
                yield GeneralResultTuple(data_id=data_coordinate, refs=refs, raw_row=row)

    @property
    def dimensions(self) -> DimensionGroup:
        # Docstring inherited
        return self._spec.dimensions

    @property
    def has_dimension_records(self) -> bool:
        """Whether all data IDs in this iterable contain dimension records."""
        return self._spec.include_dimension_records

    def with_dimension_records(self) -> GeneralQueryResults:
        """Return a results object for which `has_dimension_records` is
        `True`.
        """
        if self.has_dimension_records:
            return self
        return self._copy(tree=self._tree, include_dimension_records=True)

    def count(self, *, exact: bool = True, discard: bool = False) -> int:
        # Docstring inherited.
        return self._driver.count(self._tree, self._spec, exact=exact, discard=discard)

    def _copy(self, tree: QueryTree, **kwargs: Any) -> GeneralQueryResults:
        # Docstring inherited.
        return GeneralQueryResults(self._driver, tree, self._spec.model_copy(update=kwargs))

    def _get_datasets(self) -> frozenset[str]:
        # Docstring inherited.
        return frozenset(self._spec.dataset_fields)

    def _make_data_id(
        self,
        row: dict[str, Any],
        dimensions: DimensionGroup,
        cached_row_records: dict[DimensionElement, DimensionRecord],
    ) -> DataCoordinate:
        values = tuple(row[key] for key in itertools.chain(dimensions.required, dimensions.implied))
        data_coordinate = DataCoordinate.from_full_values(dimensions, values)
        if self.has_dimension_records:
            records = {}
            for name in dimensions.elements:
                element = dimensions.universe[name]
                record = cached_row_records.get(element)
                if record is None:
                    record = self._make_dimension_record(row, dimensions.universe[name])
                records[name] = record
            data_coordinate = data_coordinate.expanded(records)
        return data_coordinate

    def _make_dimension_record(self, row: dict[str, Any], element: DimensionElement) -> DimensionRecord:
        column_map = list(
            zip(
                element.schema.dimensions.names,
                element.dimensions.names,
            )
        )
        for field in element.schema.remainder.names:
            column_map.append((field, str(ResultColumn(element.name, field))))
        d = {k: row[v] for k, v in column_map}
        record_cls = element.RecordClass
        return record_cls(**d)

    def _get_cached_dimension_records(
        self, row: dict[str, Any], dimension_records: dict[DimensionElement, DimensionRecordSet]
    ) -> dict[DimensionElement, DimensionRecord]:
        """Find cached dimension records matching this row."""
        records = {}
        for element, element_records in dimension_records.items():
            required_values = tuple(row[key] for key in element.required.names)
            records[element] = element_records.find_with_required_values(required_values)
        return records

    def _add_dimension_records(
        self, row: dict[str, Any], records: dict[DimensionElement, DimensionRecord]
    ) -> None:
        """Extend row with the fields from cached dimension records."""
        for element, record in records.items():
            for name, value in record.toDict().items():
                if name not in element.schema.required.names:
                    row[f"{element.name}.{name}"] = value

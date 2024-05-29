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

from collections.abc import Iterable, Iterator
from contextlib import AbstractContextManager

from ..._dataset_ref import DatasetRef
from ..._dataset_type import DatasetType
from ...dimensions import DimensionGroup
from ...queries import DatasetRefQueryResults, Query
from ...registry.queries import DataCoordinateQueryResults, ParentDatasetQueryResults
from ._query_common import CommonQueryArguments, LegacyQueryResultsMixin, QueryFactory


class QueryDriverDatasetRefQueryResults(
    LegacyQueryResultsMixin[DatasetRefQueryResults], ParentDatasetQueryResults
):
    """Implementation of the legacy ``DimensionRecordQueryResults`` interface
    using the new query system.

    Parameters
    ----------
    query_factory : `QueryFactory`
        Function that can be called to access the new query system.
    args : `CommonQueryArguments`
        User-facing arguments forwarded from
        ``registry.queryDatasets``.
    dataset_type : `DatasetType`
        Type of datasets to search for.
    find_first : `bool`
        If `True`, for each result data ID, only yield one `DatasetRef` from
        the first collection in which a dataset of that dataset type appears
        (according to the order of ``collections`` passed in).
    extra_dimensions : `DimensionGroup` | `None`
        Dimensions to include in the query (in addition to those used
        to identify the queried dataset type(s)), either to constrain
        the resulting datasets to those for which a matching dimension
        exists, or to relate the dataset type's dimensions to dimensions
        referenced by the ``dataId`` or ``where`` arguments.
    doomed_by : `list` [ `str` ]
        List of messages explaining reasons why this query might not return
        any results.
    expanded : `bool`
        `True` if the query will generate "expanded" DatasetRefs that include
        dimension records associated with the data IDs.
    """

    def __init__(
        self,
        query_factory: QueryFactory,
        args: CommonQueryArguments,
        *,
        dataset_type: DatasetType,
        find_first: bool,
        extra_dimensions: DimensionGroup | None,
        doomed_by: list[str],
        expanded: bool,
    ) -> None:
        LegacyQueryResultsMixin.__init__(self, query_factory, args)
        ParentDatasetQueryResults.__init__(self)
        self._dataset_type = dataset_type
        self._find_first = find_first
        self._extra_dimensions = extra_dimensions
        self._doomed_by = doomed_by
        self._expanded = expanded

    def _build_result(self, query: Query) -> DatasetRefQueryResults:
        if self._extra_dimensions:
            query = query.join_dimensions(self._extra_dimensions)
        results = query.datasets(self._dataset_type, self._args.collections, find_first=self._find_first)
        if self._expanded:
            return results.with_dimension_records()
        else:
            return results

    def __iter__(self) -> Iterator[DatasetRef]:
        # We have to eagerly fetch the results to prevent
        # leaking the resources associated with QueryDriver.
        return iter(list(self._iterate_rows()))

    def _iterate_rows(self) -> Iterator[DatasetRef]:
        with self._build_query() as result:
            target_storage_class = self._dataset_type.storageClass
            for ref in result:
                if ref.datasetType.storageClass != target_storage_class:
                    yield ref.overrideStorageClass(target_storage_class)
                else:
                    yield ref

    @property
    def parentDatasetType(self) -> DatasetType:
        return self._dataset_type

    @property
    def dataIds(self) -> DataCoordinateQueryResults:
        raise NotImplementedError()

    def byParentDatasetType(self) -> Iterator[ParentDatasetQueryResults]:
        yield self

    def materialize(self) -> AbstractContextManager[QueryDriverDatasetRefQueryResults]:
        raise NotImplementedError()

    def expanded(self) -> QueryDriverDatasetRefQueryResults:
        return QueryDriverDatasetRefQueryResults(
            self._query_factory,
            self._args,
            dataset_type=self._dataset_type,
            find_first=self._find_first,
            extra_dimensions=self._extra_dimensions,
            doomed_by=self._doomed_by,
            expanded=True,
        )

    def explain_no_results(self, execute: bool = True) -> Iterable[str]:
        messages = list(super().explain_no_results(execute))
        messages.extend(self._doomed_by)
        return messages

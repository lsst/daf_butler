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
from contextlib import contextmanager
from typing import Any

from ..._butler import Butler
from ..._dataset_ref import DatasetRef
from ..._dataset_type import DatasetType
from ..._exceptions import DatasetTypeError
from ...dimensions import DataCoordinate, DimensionGroup
from ...queries import DataCoordinateQueryResults, Query
from ._query_common import CommonQueryArguments, LegacyQueryResultsMixin, resolve_collections
from ._query_datasets import QueryDriverDatasetRefQueryResults
from ._results import DataCoordinateQueryResults as LegacyDataCoordinateQueryResults
from ._results import ParentDatasetQueryResults


class QueryDriverDataCoordinateQueryResults(
    LegacyQueryResultsMixin[DataCoordinateQueryResults],
    LegacyDataCoordinateQueryResults,
):
    """Implementation of the legacy ``DimensionRecordQueryResults`` interface
    using the new query system.

    Parameters
    ----------
    butler : `Butler`
        Butler object used to execute queries.
    dimensions : `DimensionGroup` | `None`
        Dimensions of the data IDs to yield from the query.
    expanded : bool
        `True` if the query will also fetch dimension records associated with
        the data IDs.
    args : `CommonQueryArguments`
        User-facing arguments forwarded from
        ``registry.queryDimensionRecords``.
    """

    def __init__(
        self,
        butler: Butler,
        dimensions: DimensionGroup,
        expanded: bool,
        args: CommonQueryArguments,
    ) -> None:
        LegacyQueryResultsMixin.__init__(self, butler, args)
        LegacyDataCoordinateQueryResults.__init__(self)
        self._dimensions = dimensions
        self._expanded = expanded

    def _build_result(self, query: Query) -> DataCoordinateQueryResults:
        results = query.data_ids(self._dimensions)
        if self._expanded:
            return results.with_dimension_records()
        else:
            return results

    @property
    def dimensions(self) -> DimensionGroup:
        return self._dimensions

    def hasFull(self) -> bool:
        return True

    def hasRecords(self) -> bool:
        return self._expanded

    def __iter__(self) -> Iterator[DataCoordinate]:
        with self._build_query() as result:
            # We have to eagerly fetch the results to prevent
            # leaking the resources associated with QueryDriver.
            records = list(result)
        return iter(records)

    @contextmanager
    def materialize(self) -> Iterator[LegacyDataCoordinateQueryResults]:
        yield self

    def expanded(self) -> LegacyDataCoordinateQueryResults:
        return QueryDriverDataCoordinateQueryResults(self._butler, self._dimensions, True, self._args)

    def subset(
        self,
        dimensions: DimensionGroup | Iterable[str] | None = None,
        *,
        unique: bool = False,
    ) -> LegacyDataCoordinateQueryResults:
        # 'unique' parameter is intentionally ignored -- all data ID queries
        # using the new query system are automatically de-duplicated.

        if dimensions is None:
            return self

        dimensions = self.dimensions.universe.conform(dimensions)
        if not dimensions.issubset(self.dimensions):
            raise ValueError(f"{dimensions} is not a subset of {self.dimensions}")
        return QueryDriverDataCoordinateQueryResults(self._butler, dimensions, self._expanded, self._args)

    def findDatasets(
        self,
        datasetType: DatasetType | str,
        collections: Any,
        *,
        findFirst: bool = True,
        components: bool = False,
    ) -> ParentDatasetQueryResults:
        if components is not False:
            raise DatasetTypeError(
                "Dataset component queries are no longer supported by Registry.  Use "
                "DatasetType methods to obtain components from parent dataset types instead."
            )

        if not isinstance(datasetType, DatasetType):
            datasetType = self._butler.get_dataset_type(datasetType)

        doomed_by: list[str] = []
        collections = resolve_collections(self._butler, collections, doomed_by)
        args = self._args.replaceCollections(collections)
        return QueryDriverDatasetRefQueryResults(
            self._butler,
            args,
            dataset_type=datasetType,
            find_first=findFirst,
            extra_dimensions=self.dimensions,
            doomed_by=doomed_by,
            expanded=False,
        )

    def findRelatedDatasets(
        self,
        datasetType: DatasetType | str,
        collections: Any,
        *,
        findFirst: bool = True,
        dimensions: DimensionGroup | Iterable[str] | None = None,
    ) -> Iterable[tuple[DataCoordinate, DatasetRef]]:
        with self._butler.query() as query:
            if not isinstance(datasetType, DatasetType):
                datasetType = self._butler.get_dataset_type(datasetType)

            if dimensions is None:
                dimensions = self.dimensions
            dimensions = DimensionGroup(self._butler.dimensions, dimensions)

            collections = resolve_collections(self._butler, collections, [])
            result = query.join_dataset_search(datasetType, collections).general(
                dimensions,
                dataset_fields={datasetType.name: {"dataset_id", "run"}},
                find_first=findFirst,
            )
            result = self._apply_result_modifiers(result)
            for row in result.iter_tuples(datasetType):
                yield row.data_id.subset(dimensions), row.refs[0]

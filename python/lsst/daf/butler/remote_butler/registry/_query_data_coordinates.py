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

from ..._dataset_ref import DatasetRef
from ..._dataset_type import DatasetType
from ...dimensions import DataCoordinate, DimensionGroup
from ...queries import DataCoordinateQueryResults, Query
from ...registry.queries import DataCoordinateQueryResults as LegacyDataCoordinateQueryResults
from ...registry.queries import ParentDatasetQueryResults
from ._query_common import CommonQueryArguments, LegacyQueryResultsMixin, QueryFactory


class QueryDriverDataCoordinateQueryResults(
    LegacyQueryResultsMixin[DataCoordinateQueryResults],
    LegacyDataCoordinateQueryResults,
):
    """Implementation of the legacy ``DimensionRecordQueryResults`` interface
    using the new query system.

    Parameters
    ----------
    query_factory : `QueryFactory`
        Function that can be called to access the new query system.
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
        query_factory: QueryFactory,
        dimensions: DimensionGroup,
        expanded: bool,
        args: CommonQueryArguments,
    ) -> None:
        LegacyQueryResultsMixin.__init__(self, query_factory, args)
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
        raise NotImplementedError()

    def expanded(self) -> LegacyDataCoordinateQueryResults:
        return QueryDriverDataCoordinateQueryResults(self._query_factory, self._dimensions, True, self._args)

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
        return QueryDriverDataCoordinateQueryResults(
            self._query_factory, dimensions, self._expanded, self._args
        )

    def findDatasets(
        self,
        datasetType: DatasetType | str,
        collections: Any,
        *,
        findFirst: bool = True,
        components: bool = False,
    ) -> ParentDatasetQueryResults:
        raise NotImplementedError()

    def findRelatedDatasets(
        self,
        datasetType: DatasetType | str,
        collections: Any,
        *,
        findFirst: bool = True,
        dimensions: DimensionGroup | Iterable[str] | None = None,
    ) -> Iterable[tuple[DataCoordinate, DatasetRef]]:
        raise NotImplementedError()

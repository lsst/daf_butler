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

__all__ = [
    "DirectDataCoordinateQueryResults",
    "DirectDatasetQueryResults",
    "DirectDimensionRecordQueryResults",
    "DirectSingleTypeDatasetQueryResults",
]

import contextlib
from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING, Any

from ._query_results import (
    DataCoordinateQueryResults,
    DatasetQueryResults,
    DimensionRecordQueryResults,
    SingleTypeDatasetQueryResults,
)
from .registry import queries as registry_queries

if TYPE_CHECKING:
    from ._dataset_ref import DatasetRef
    from ._dataset_type import DatasetType
    from .dimensions import DataCoordinate, DimensionElement, DimensionGroup, DimensionRecord


class DirectDataCoordinateQueryResults(DataCoordinateQueryResults):
    """Implementation of `DataCoordinateQueryResults` using query result
    obtained from registry.

    Parameters
    ----------
    registry_query_result : \
            `~lsst.daf.butler.registry.queries.DataCoordinateQueryResults`
        Query result from Registry.
    """

    def __init__(self, registry_query_result: registry_queries.DataCoordinateQueryResults):
        self._registry_query_result = registry_query_result

    def __iter__(self) -> Iterator[DataCoordinate]:
        return iter(self._registry_query_result)

    @property
    def dimensions(self) -> DimensionGroup:
        # Docstring inherited.
        return self._registry_query_result.dimensions

    def has_full(self) -> bool:
        # Docstring inherited.
        return self._registry_query_result.hasFull()

    def has_records(self) -> bool:
        # Docstring inherited.
        return self._registry_query_result.hasRecords()

    @contextlib.contextmanager
    def materialize(self) -> Iterator[DataCoordinateQueryResults]:
        with self._registry_query_result.materialize() as result:
            yield DirectDataCoordinateQueryResults(result)

    def expanded(self) -> DataCoordinateQueryResults:
        # Docstring inherited.
        if self.has_records():
            return self
        return DirectDataCoordinateQueryResults(self._registry_query_result.expanded())

    def subset(
        self,
        dimensions: DimensionGroup | Iterable[str] | None = None,
        *,
        unique: bool = False,
    ) -> DataCoordinateQueryResults:
        # Docstring inherited.
        return DirectDataCoordinateQueryResults(self._registry_query_result.subset(dimensions, unique=unique))

    def find_datasets(
        self,
        dataset_type: DatasetType | str,
        collections: Any,
        *,
        find_first: bool = True,
    ) -> DatasetQueryResults:
        # Docstring inherited.
        return DirectDatasetQueryResults(
            self._registry_query_result.findDatasets(dataset_type, collections, findFirst=find_first)
        )

    def find_related_datasets(
        self,
        dataset_type: DatasetType | str,
        collections: Any,
        *,
        find_first: bool = True,
        dimensions: DimensionGroup | Iterable[str] | None = None,
    ) -> Iterable[tuple[DataCoordinate, DatasetRef]]:
        # Docstring inherited.
        return self._registry_query_result.findRelatedDatasets(
            dataset_type, collections, findFirst=find_first, dimensions=dimensions
        )

    def count(self, *, exact: bool = True, discard: bool = False) -> int:
        # Docstring inherited.
        return self._registry_query_result.count(exact=exact, discard=discard)

    def any(self, *, execute: bool = True, exact: bool = True) -> bool:
        # Docstring inherited.
        return self._registry_query_result.any(execute=execute, exact=exact)

    def explain_no_results(self, execute: bool = True) -> Iterable[str]:
        # Docstring inherited.
        return self._registry_query_result.explain_no_results(execute=execute)

    def order_by(self, *args: str) -> DataCoordinateQueryResults:
        # Docstring inherited.
        return DirectDataCoordinateQueryResults(self._registry_query_result.order_by(*args))

    def limit(self, limit: int, offset: int | None = 0) -> DataCoordinateQueryResults:
        # Docstring inherited.
        return DirectDataCoordinateQueryResults(self._registry_query_result.limit(limit, offset))


class DirectDatasetQueryResults(DatasetQueryResults):
    """Implementation of `DatasetQueryResults` using query result
    obtained from registry.

    Parameters
    ----------
    registry_query_result : \
            `~lsst.daf.butler.registry.queries.DatasetQueryResults`
        Query result from Registry.
    """

    def __init__(self, registry_query_result: registry_queries.DatasetQueryResults):
        self._registry_query_result = registry_query_result

    def __iter__(self) -> Iterator[DatasetRef]:
        return iter(self._registry_query_result)

    def by_dataset_type(self) -> Iterator[SingleTypeDatasetQueryResults]:
        # Docstring inherited.
        for by_parent in self._registry_query_result.byParentDatasetType():
            yield DirectSingleTypeDatasetQueryResults(by_parent)

    @contextlib.contextmanager
    def materialize(self) -> Iterator[DatasetQueryResults]:
        # Docstring inherited.
        with self._registry_query_result.materialize() as result:
            yield DirectDatasetQueryResults(result)

    def expanded(self) -> DatasetQueryResults:
        # Docstring inherited.
        return DirectDatasetQueryResults(self._registry_query_result.expanded())

    def count(self, *, exact: bool = True, discard: bool = False) -> int:
        # Docstring inherited.
        return self._registry_query_result.count(exact=exact, discard=discard)

    def any(self, *, execute: bool = True, exact: bool = True) -> bool:
        # Docstring inherited.
        return self._registry_query_result.any(execute=execute, exact=exact)

    def explain_no_results(self, execute: bool = True) -> Iterable[str]:
        # Docstring inherited.
        return self._registry_query_result.explain_no_results(execute=execute)


class DirectSingleTypeDatasetQueryResults(SingleTypeDatasetQueryResults):
    """Implementation of `SingleTypeDatasetQueryResults` using query result
    obtained from registry.

    Parameters
    ----------
    registry_query_result : \
            `~lsst.daf.butler.registry.queries.ParentDatasetQueryResults`
        Query result from Registry.
    """

    def __init__(self, registry_query_result: registry_queries.ParentDatasetQueryResults):
        self._registry_query_result = registry_query_result

    def __iter__(self) -> Iterator[DatasetRef]:
        return iter(self._registry_query_result)

    def by_dataset_type(self) -> Iterator[SingleTypeDatasetQueryResults]:
        # Docstring inherited.
        yield self

    @contextlib.contextmanager
    def materialize(self) -> Iterator[SingleTypeDatasetQueryResults]:
        # Docstring inherited.
        with self._registry_query_result.materialize() as result:
            yield DirectSingleTypeDatasetQueryResults(result)

    @property
    def dataset_type(self) -> DatasetType:
        # Docstring inherited.
        return self._registry_query_result.parentDatasetType

    @property
    def data_ids(self) -> DataCoordinateQueryResults:
        # Docstring inherited.
        return DirectDataCoordinateQueryResults(self._registry_query_result.dataIds)

    def expanded(self) -> SingleTypeDatasetQueryResults:
        # Docstring inherited.
        return DirectSingleTypeDatasetQueryResults(self._registry_query_result.expanded())

    def count(self, *, exact: bool = True, discard: bool = False) -> int:
        # Docstring inherited.
        return self._registry_query_result.count(exact=exact, discard=discard)

    def any(self, *, execute: bool = True, exact: bool = True) -> bool:
        # Docstring inherited.
        return self._registry_query_result.any(execute=execute, exact=exact)

    def explain_no_results(self, execute: bool = True) -> Iterable[str]:
        # Docstring inherited.
        return self._registry_query_result.explain_no_results(execute=execute)


class DirectDimensionRecordQueryResults(DimensionRecordQueryResults):
    """Implementation of `DimensionRecordQueryResults` using query result
    obtained from registry.

    Parameters
    ----------
    registry_query_result : \
            `~lsst.daf.butler.registry.queries.DimensionRecordQueryResults`
        Query result from Registry.
    """

    def __init__(self, registry_query_result: registry_queries.DimensionRecordQueryResults):
        self._registry_query_result = registry_query_result

    def __iter__(self) -> Iterator[DimensionRecord]:
        return iter(self._registry_query_result)

    @property
    def element(self) -> DimensionElement:
        # Docstring inherited.
        return self._registry_query_result.element

    def run(self) -> DimensionRecordQueryResults:
        # Docstring inherited.
        return DirectDimensionRecordQueryResults(self._registry_query_result.run())

    def count(self, *, exact: bool = True, discard: bool = False) -> int:
        # Docstring inherited.
        return self._registry_query_result.count(exact=exact, discard=discard)

    def any(self, *, execute: bool = True, exact: bool = True) -> bool:
        # Docstring inherited.
        return self._registry_query_result.any(execute=execute, exact=exact)

    def order_by(self, *args: str) -> DimensionRecordQueryResults:
        # Docstring inherited.
        return DirectDimensionRecordQueryResults(self._registry_query_result.order_by(*args))

    def limit(self, limit: int, offset: int | None = 0) -> DimensionRecordQueryResults:
        # Docstring inherited.
        return DirectDimensionRecordQueryResults(self._registry_query_result.limit(limit, offset))

    def explain_no_results(self, execute: bool = True) -> Iterable[str]:
        # Docstring inherited.
        return self._registry_query_result.explain_no_results(execute=execute)

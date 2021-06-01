# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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

__all__ = ()

from types import MappingProxyType
from typing import Dict, Iterable, Mapping, Type

from ...datasets import DatasetId, DatasetRef, DatasetType
from ...dimensions import DimensionUniverse
from ...named import NamedKeyDict, NamedKeyMapping
from ._by_id_abstract_set import HomogeneousDatasetByIdAbstractSet
from ._by_id_set import DatasetByIdSet
from ._generic_sets import DatasetAbstractSet, GroupingDatasetAbstractSet


class HomogeneousDatasetByIdFrozenSet(HomogeneousDatasetByIdAbstractSet):
    """A concrete immutable container for resolved `DatasetRef` instances of
    a single dataset type that organizes them by dataset ID.

    Parameters
    ----------
    dataset_type : `DatasetType`
        Dataset type for all datasets in this container.
    datasets : `Iterable` [ `DatasetRef` ]
        Initial datasets for this container.

    Raises
    ------
    AmbiguousDatasetError
        Raised if `DatasetRef.id` is `None` for any given dataset.
    TypeError
        Raised if ``ref.datasetType != self.dataset_type`` for any input
        `DatasetRef`.
    """

    def __new__(
        cls, dataset_type: DatasetType, datasets: Iterable[DatasetRef] = ()
    ) -> HomogeneousDatasetByIdFrozenSet:
        if isinstance(datasets, HomogeneousDatasetByIdFrozenSet) and dataset_type == datasets.dataset_type:
            # Just return what we were given; no point copying an immutable
            # container.
            return datasets
        self = super().__new__(cls)
        self._dataset_type = dataset_type
        mapping: Dict[DatasetId, DatasetRef] = {}
        self._update_mapping(datasets, mapping)
        self._as_mapping = MappingProxyType(mapping)
        return self

    __slots__ = ("_dataset_type", "_as_mapping")

    @property
    def dataset_type(self) -> DatasetType:
        # Docstring inherited.
        return self._dataset_type

    @property
    def as_mapping(self) -> Mapping[DatasetId, DatasetRef]:
        # Docstring inherited.
        return self._as_mapping

    @classmethod
    def _from_iterable(
        cls,
        dataset_type: DatasetType,
        iterable: Iterable[DatasetRef],
        all_resolved: bool,
        all_unresolved: bool,
    ) -> HomogeneousDatasetByIdFrozenSet:
        # Docstring inherited.
        return cls(dataset_type, iterable)

    _dataset_type: DatasetType
    _as_mapping: Mapping[DatasetId, DatasetRef]


class DatasetByIdFrozenSet(GroupingDatasetAbstractSet[DatasetId, HomogeneousDatasetByIdFrozenSet]):
    """A concrete immutable container for resolved `DatasetRef` instances of
    potentially multiple dataset types that organizes them by dataset ID.

    Parameters
    ----------
    universe : `DimensionUniverse`
        All dimensions potentially associated with this container.
    datasets : `Iterable` [ `DatasetRef` ]
        Datasets for this container.

    Raises
    ------
    AmbiguousDatasetError
        Raised if `DatasetRef.id` is `None` for any given dataset.
    """

    def __new__(
        cls, universe: DimensionUniverse, datasets: Iterable[DatasetRef] = ()
    ) -> DatasetByIdFrozenSet:
        if isinstance(datasets, DatasetByIdFrozenSet):
            return datasets
        if not isinstance(datasets, DatasetAbstractSet):
            # Inputs are not grouped.  Copy into a similar mutable set for
            # grouping instead of reimplementing it.
            datasets = DatasetByIdSet(universe, datasets)
        by_dataset_type = NamedKeyDict[DatasetType, HomogeneousDatasetByIdFrozenSet](
            (dataset_type, HomogeneousDatasetByIdFrozenSet(dataset_type, refs))
            for dataset_type, refs in datasets.by_dataset_type.items()
        )
        self = super().__new__(cls)
        self._universe = universe
        self._by_dataset_type = by_dataset_type.freeze()
        return self

    __slots__ = ("_universe", "_by_dataset_type")

    @property
    def universe(self) -> DimensionUniverse:
        # Docstring inherited.
        return self._universe

    @property
    def all_resolved(self) -> bool:
        # Docstring inherited.
        return True

    @property
    def all_unresolved(self) -> bool:
        # Docstring inherited.
        return False

    @property
    def by_dataset_type(self) -> NamedKeyMapping[DatasetType, HomogeneousDatasetByIdFrozenSet]:
        # Docstring inherited.
        return self._by_dataset_type

    @classmethod
    def _from_nested(
        cls,
        universe: DimensionUniverse,
        nested: Iterable[HomogeneousDatasetByIdFrozenSet],
        all_resolved: bool,
        all_unresolved: bool,
    ) -> DatasetByIdFrozenSet:
        # Docstring inherited.
        self = super().__new__(cls)
        self._universe = universe
        self._by_dataset_type = NamedKeyDict({h.dataset_type: h for h in nested}).freeze()
        return self

    @classmethod
    def _nested_type(cls) -> Type[HomogeneousDatasetByIdFrozenSet]:
        # Docstring inherited.
        return HomogeneousDatasetByIdFrozenSet

    _universe: DimensionUniverse
    _by_dataset_type: NamedKeyMapping[DatasetType, HomogeneousDatasetByIdFrozenSet]

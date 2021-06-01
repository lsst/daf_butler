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

from typing import Dict, Iterable, Type

from ...datasets import DatasetId, DatasetRef, DatasetType
from ...dimensions import DimensionUniverse
from ...named import NamedKeyDict, NamedKeyMutableMapping
from ._by_id_abstract_set import HomogeneousDatasetByIdAbstractSet
from ._generic_sets import GroupingDatasetMutableSet, HomogeneousDatasetMutableSet


class HomogeneousDatasetByIdSet(HomogeneousDatasetByIdAbstractSet, HomogeneousDatasetMutableSet[DatasetId]):
    """A concrete mutable container for resolved `DatasetRef` instances of
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

    def __init__(self, dataset_type: DatasetType, datasets: Iterable[DatasetRef] = ()):
        self._dataset_type = dataset_type
        self._as_mapping: Dict[DatasetId, DatasetRef] = dict()
        self.update(datasets)

    __slots__ = ("_dataset_type", "_as_mapping")

    @property
    def dataset_type(self) -> DatasetType:
        # Docstring inherited.
        return self._dataset_type

    @property
    def as_mapping(self) -> Dict[DatasetId, DatasetRef]:
        # Docstring inherited.
        return self._as_mapping

    @classmethod
    def _from_iterable(
        cls,
        dataset_type: DatasetType,
        iterable: Iterable[DatasetRef],
        all_resolved: bool,
        all_unresolved: bool,
    ) -> HomogeneousDatasetByIdSet:
        # Docstring inherited.
        return cls(dataset_type, iterable)


class DatasetByIdSet(GroupingDatasetMutableSet[DatasetId, HomogeneousDatasetByIdSet]):
    """A concrete mutable container for resolved `DatasetRef` instances of
    potentially multiple dataset types that organizes them by dataset ID.

    Parameters
    ----------
    universe : `DimensionUniverse`
        All dimensions potentially associated with this container.
    datasets : `Iterable` [ `DatasetRef` ]
        Initial datasets for this container.

    Raises
    ------
    AmbiguousDatasetError
        Raised if `DatasetRef.id` is `None` for any given dataset.
    """

    def __init__(self, universe: DimensionUniverse, datasets: Iterable[DatasetRef] = ()):
        self._universe = universe
        self._by_dataset_type = NamedKeyDict[DatasetType, HomogeneousDatasetByIdSet]()
        self.update(datasets)

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

    def unique_by_id(self) -> DatasetByIdSet:
        # Docstring inherited.
        return self

    @property
    def by_dataset_type(self) -> NamedKeyMutableMapping[DatasetType, HomogeneousDatasetByIdSet]:
        # Docstring inherited.
        return self._by_dataset_type

    @classmethod
    def _from_nested(
        cls,
        universe: DimensionUniverse,
        nested: Iterable[HomogeneousDatasetByIdSet],
        all_resolved: bool,
        all_unresolved: bool,
    ) -> DatasetByIdSet:
        # Docstring inherited.
        self = super().__new__(cls)
        self._universe = universe
        self._by_dataset_type = NamedKeyDict({h.dataset_type: h for h in nested})
        return self

    @classmethod
    def _nested_type(cls) -> Type[HomogeneousDatasetByIdSet]:
        # Docstring inherited.
        return HomogeneousDatasetByIdSet

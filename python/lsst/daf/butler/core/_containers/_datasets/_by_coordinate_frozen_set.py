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

__all__ = (
    "HomogeneousDatasetByCoordinateFrozenSet",
    "DatasetByCoordinateFrozenSet",
)

from types import MappingProxyType
from typing import Dict, Iterable, Mapping, Type

from ...datasets import DatasetRef, DatasetType
from ...dimensions import DataCoordinate, DimensionUniverse
from ...named import NamedKeyDict, NamedKeyMapping
from ._by_coordinate_abstract_set import HomogeneousDatasetByCoordinateAbstractSet
from ._by_coordinate_set import DatasetByCoordinateSet
from ._generic_sets import DatasetAbstractSet, GroupingDatasetAbstractSet


class HomogeneousDatasetByCoordinateFrozenSet(HomogeneousDatasetByCoordinateAbstractSet):
    """A concrete immutable container for resolved `DatasetRef` instances of
    a single dataset type that organizes them by `DataCoordinate`.

    Parameters
    ----------
    dataset_type : `DatasetType`
        Dataset type for all datasets in this container.
    datasets : `Iterable` [ `DatasetRef` ]
        Initial datasets for this container.  If ``all_unresolved`` is `True`,
        an unresolved version of each reference is added.
    all_resolved : `bool`
        Whether all `DatasetRef` instances in this container must be resolved
        (have a `DatasetRef.id` that is not `None`).
    all_unresolved : `bool`
        Whether all `DatasetRef` instances in this container must be unresolved
        (have a `DatasetRef.id` that is `None`).

    Raises
    ------
    AmbiguousDatasetError
        Raised if `DatasetRef.id` is `None` for any given dataset but
        `all_resolved` is `True`.
    TypeError
        Raised if ``ref.datasetType != self.dataset_type`` for any input
        `DatasetRef`.
    """

    def __new__(
        cls,
        dataset_type: DatasetType,
        datasets: Iterable[DatasetRef] = (),
        *,
        all_resolved: bool,
        all_unresolved: bool,
    ) -> HomogeneousDatasetByCoordinateFrozenSet:
        if (
            isinstance(datasets, HomogeneousDatasetByCoordinateFrozenSet)
            and dataset_type == datasets.dataset_type
            and all_resolved == datasets.all_resolved
            and all_unresolved == datasets.all_unresolved
        ):
            # Just return what we were given; no point copying an immutable
            # container.
            return datasets
        self = super().__new__(cls)
        self._dataset_type = dataset_type
        self._all_resolved = all_resolved
        self._all_unresolved = all_unresolved
        mapping: Dict[DataCoordinate, DatasetRef] = {}
        self._update_mapping(datasets, mapping)
        self._as_mapping = MappingProxyType(mapping)
        return self

    __slots__ = ("_dataset_type", "_as_mapping", "_all_resolved", "_all_unresolved")

    @property
    def all_resolved(self) -> bool:
        # Docstring inherited.
        return self._all_resolved

    @property
    def all_unresolved(self) -> bool:
        # Docstring inherited.
        return self._all_unresolved

    @property
    def dataset_type(self) -> DatasetType:
        # Docstring inherited.
        return self._dataset_type

    @property
    def as_mapping(self) -> Mapping[DataCoordinate, DatasetRef]:
        # Docstring inherited.
        return self._as_mapping

    @classmethod
    def _from_iterable(
        cls,
        dataset_type: DatasetType,
        iterable: Iterable[DatasetRef],
        all_resolved: bool,
        all_unresolved: bool,
    ) -> HomogeneousDatasetByCoordinateFrozenSet:
        # Docstring inherited.
        return cls(dataset_type, iterable, all_resolved=all_resolved, all_unresolved=all_unresolved)

    _dataset_type: DatasetType
    _as_mapping: Mapping[DataCoordinate, DatasetRef]
    _all_resolved: bool
    _all_unresolved: bool


class DatasetByCoordinateFrozenSet(
    GroupingDatasetAbstractSet[DataCoordinate, HomogeneousDatasetByCoordinateFrozenSet]
):
    """A concrete immutable container for resolved `DatasetRef` instances of
    potentially multiple dataset types that organizes them by `DataCoordinate`.

    Parameters
    ----------
    universe : `DimensionUniverse`
        All dimensions potentially associated with this container.
    datasets : `Iterable` [ `DatasetRef` ]
        Datasets for this container.
    all_resolved : `bool`
        Whether all `DatasetRef` instances in this container must be resolved
        (have a `DatasetRef.id` that is not `None`).
    all_unresolved : `bool`
        Whether all `DatasetRef` instances in this container must be resolved
        (have a `DatasetRef.id` that is `None`).

    Raises
    ------
    AmbiguousDatasetError
        Raised if `DatasetRef.id` is `None` for any given dataset but
        `all_resolved` is `True`.
    """

    def __new__(
        cls,
        universe: DimensionUniverse,
        datasets: Iterable[DatasetRef],
        *,
        all_resolved: bool,
        all_unresolved: bool,
    ) -> DatasetByCoordinateFrozenSet:
        if isinstance(datasets, DatasetByCoordinateFrozenSet):
            return datasets
        if not isinstance(datasets, DatasetAbstractSet):
            # Inputs are not grouped.  Copy into a similar mutable set for
            # grouping instead of reimplementing it.
            datasets = DatasetByCoordinateSet(
                universe, datasets, all_resolved=all_resolved, all_unresolved=all_unresolved
            )
        by_dataset_type = NamedKeyDict[DatasetType, HomogeneousDatasetByCoordinateFrozenSet](
            (
                dataset_type,
                HomogeneousDatasetByCoordinateFrozenSet(
                    dataset_type, refs, all_resolved=all_resolved, all_unresolved=all_unresolved
                ),
            )
            for dataset_type, refs in datasets.by_dataset_type.items()
        )
        self = super().__new__(cls)
        self._universe = universe
        self._by_dataset_type = by_dataset_type.freeze()
        self._all_resolved = all_resolved
        self._all_unresolved = all_unresolved
        return self

    __slots__ = ("_universe", "_by_dataset_type", "_all_resolved", "_all_unresolved")

    @property
    def universe(self) -> DimensionUniverse:
        # Docstring inherited.
        return self._universe

    @property
    def all_resolved(self) -> bool:
        # Docstring inherited.
        return self._all_resolved

    @property
    def all_unresolved(self) -> bool:
        # Docstring inherited.
        return self._all_unresolved

    def unique_by_coordinate(self) -> DatasetByCoordinateFrozenSet:
        # Docstring inherited.
        return self

    @property
    def by_dataset_type(self) -> NamedKeyMapping[DatasetType, HomogeneousDatasetByCoordinateFrozenSet]:
        # Docstring inherited.
        return self._by_dataset_type

    @classmethod
    def _from_nested(
        cls,
        universe: DimensionUniverse,
        nested: Iterable[HomogeneousDatasetByCoordinateFrozenSet],
        all_resolved: bool,
        all_unresolved: bool,
    ) -> DatasetByCoordinateFrozenSet:
        # Docstring inherited.
        self = super().__new__(cls)
        self._universe = universe
        self._by_dataset_type = NamedKeyDict({h.dataset_type: h for h in nested}).freeze()
        self._all_resolved = all_resolved
        self._all_unresolved = all_unresolved
        return self

    @classmethod
    def _nested_type(cls) -> Type[HomogeneousDatasetByCoordinateFrozenSet]:
        # Docstring inherited.
        return HomogeneousDatasetByCoordinateFrozenSet

    _universe: DimensionUniverse
    _by_dataset_type: NamedKeyMapping[DatasetType, HomogeneousDatasetByCoordinateFrozenSet]
    _all_resolved: bool
    _all_unresolved: bool

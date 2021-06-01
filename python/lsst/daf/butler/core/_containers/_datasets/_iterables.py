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
    "DatasetIterable",
    "HomogeneousDatasetIterable",
)

from abc import abstractmethod
from typing import TYPE_CHECKING, Iterable

from ...datasets import DatasetId, DatasetRef, DatasetType
from ...dimensions import DataCoordinate, DimensionUniverse

if TYPE_CHECKING:
    from ._generic_sets import DatasetAbstractSet, HomogeneousDatasetAbstractSet


class DatasetIterable(Iterable[DatasetRef]):
    """Abstract base class for all custom containers of `DatasetRef`.

    Notes
    -----
    This interface makes no guarantees about uniqueness or iteration order,
    and its instances may support only single-pass iteration.
    """

    __slots__ = ()

    @property
    @abstractmethod
    def universe(self) -> DimensionUniverse:
        """All dimensions potentially associated with this container
        (`DimensionUniverse`).
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def all_resolved(self) -> bool:
        """`True` if all datasets in this container are required to be
        resolved (i.e. have `DatasetRef.id` not `None`), `False` otherwise
        (`bool`).
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def all_unresolved(self) -> bool:
        """`True` if all datasets in this container are required to be
        unresolved (have a `DatasetRef.id` that is `None`), `False` otherwise
        (`bool`).
        """
        raise NotImplementedError()

    def unique_by_id(self) -> DatasetAbstractSet[DatasetId]:
        """Return a container that organizes datasets by their dataset ID,
        removing any duplicates.

        Returns
        -------
        container : `DatasetAbstractSet` [ `DatasetId` ]
            Container whose datasets have unique `DatasetType` + dataset ID
            combinations (note that in the presence of components, the
            dataset ID is not a fully unique identifier).  May not have the
            same size or order as ``self``.  May be``self`` if it is already a
            `DatasetSet` instance organized by `DatasetId`.  If ``self`` is a
            homogeneous dataset container, this container will be, too.

        Raises
        ------
        AmbiguousDatasetError
            Raised if `all_resolved` is not `True`.

        Notes
        -----
        If ``self`` is a single-pass iterable, this may consume it.
        """
        from ._by_id_set import DatasetByIdSet

        return DatasetByIdSet(self.universe, self)

    def unique_by_coordinate(self) -> DatasetAbstractSet[DataCoordinate]:
        """Return a container that organizes datasets by their data coordinate
        (data ID), removing any duplicates.

        Returns
        -------
        container : `DatasetByCoordinateAbstractSet`
            Container whose datasets have unique `DatasetType` +
            `DataCoordinate` combinations.  May not have the same size or order
            as ``self``.  May be``self`` if it is already a
            `DatasetAbstractSet` instance organized by `DataCoordinate`.  If
            ``self`` is a homogeneous dataset container, this container will
            be, too.

        Notes
        -----
        If ``self`` is a single-pass iterable, this may consume it.
        """
        from ._by_coordinate_set import DatasetByCoordinateSet

        return DatasetByCoordinateSet(
            self.universe, self, all_resolved=self.all_resolved, all_unresolved=self.all_unresolved
        )


class HomogeneousDatasetIterable(DatasetIterable):
    """Abstract base class for custom containers of `DatasetRef` that are
    constrained to a single `DatasetType`.

    Notes
    -----
    This interface makes no guarantees about uniqueness or iteration order,
    and its instances may support only single-pass iteration.
    """

    __slots__ = ()

    @property
    def universe(self) -> DimensionUniverse:
        # Docstring inherited.
        return self.dataset_type.dimensions.universe

    def unique_by_id(self) -> HomogeneousDatasetAbstractSet[DatasetId]:
        # Docstring inherited.
        from ._by_id_set import HomogeneousDatasetByIdSet

        return HomogeneousDatasetByIdSet(self.dataset_type, self)

    def unique_by_coordinate(self) -> HomogeneousDatasetAbstractSet[DataCoordinate]:
        # Docstring inherited.
        from ._by_coordinate_set import HomogeneousDatasetByCoordinateSet

        return HomogeneousDatasetByCoordinateSet(
            self.dataset_type, self, all_resolved=self.all_resolved, all_unresolved=self.all_unresolved
        )

    @property
    @abstractmethod
    def dataset_type(self) -> DatasetType:
        """The `DatasetType` shared by all datasets in this iterable."""
        raise NotImplementedError()

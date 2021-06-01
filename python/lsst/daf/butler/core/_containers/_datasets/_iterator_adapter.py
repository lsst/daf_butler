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
    "DatasetIteratorAdapter",
    "HomogeneousDatasetIteratorAdapter",
)

from typing import Callable, Iterator

from ...datasets import DatasetRef, DatasetType
from ...dimensions import DimensionUniverse
from ._iterables import DatasetIterable, HomogeneousDatasetIterable


class DatasetIteratorAdapter(DatasetIterable):
    """A concrete view iterable of `DatasetRef`, backed by a function
    that returns an iterator.

    Parameters
    ----------
    universe : `DimensionUniverse`
        All dimensions potentially associated with this iterable.
    iterator_callback : `Callable`
        Function that takes no arguments and returns an iterator over
        `DatasetRef` objects that satisfy the given `DatasetType` and
        resolution state requirements.
    all_resolved : `bool`
        Whether all `DatasetRef` instances returned are resolved (have a
        `DatasetRef.id` that is not `None`).
    all_unresolved : `bool`
        Whether all `DatasetRef` instances returned are unresolved (have a
        `DatasetRef.id` that is `None`).
    """

    def __init__(
        self,
        universe: DimensionUniverse,
        iterator_callback: Callable[[], Iterator[DatasetRef]],
        *,
        all_resolved: bool,
        all_unresolved: bool
    ):
        self._universe = universe
        self._iterator_callback = iterator_callback
        self._all_resolved = all_resolved
        self._all_unresolved = all_unresolved

    __slots__ = ("_universe", "_iterator_callback", "_all_resolved", "_all_unresolved")

    def __iter__(self) -> Iterator[DatasetRef]:
        # Docstring inherited
        return self._iterator_callback()

    @property
    def universe(self) -> DimensionUniverse:
        # Docstring inherited
        return self._universe

    @property
    def all_resolved(self) -> bool:
        # Docstring inherited
        return self._all_resolved

    @property
    def all_unresolved(self) -> bool:
        # Docstring inherited
        return self._all_unresolved


class HomogeneousDatasetIteratorAdapter(HomogeneousDatasetIterable):
    """A concrete single-`DatasetType` view iterable of `DatasetRef`, backed by
    a function that returns an iterator.

    Parameters
    ----------
    dataset_type : `DatasetType`
        Dataset type for all datasets returned.
    iterator_callback : `Callable`
        Function that takes no arguments and returns an iterator over
        `DatasetRef` objects that satisfy the given `DatasetType` and
        resolution state require
    all_resolved : `bool`
        Whether all `DatasetRef` instances returned are resolved (have a
        `DatasetRef.id` that is not `None`).
    all_unresolved : `bool`
        Whether all `DatasetRef` instances returned are unresolved (have a
        `DatasetRef.id` that is `None`).
    """

    def __init__(
        self,
        dataset_type: DatasetType,
        iterator_callback: Callable[[], Iterator[DatasetRef]],
        *,
        all_resolved: bool,
        all_unresolved: bool
    ):
        self._dataset_type = dataset_type
        self._iterator_callback = iterator_callback
        self._all_resolved = all_resolved
        self._all_unresolved = all_unresolved

    __slots__ = ("_dataset_type", "_iterator_callback", "_all_resolved", "_all_unresolved")

    def __iter__(self) -> Iterator[DatasetRef]:
        # Docstring inherited
        return self._iterator_callback()

    @property
    def all_resolved(self) -> bool:
        # Docstring inherited
        return self._all_resolved

    @property
    def all_unresolved(self) -> bool:
        # Docstring inherited
        return self._all_unresolved

    @property
    def dataset_type(self) -> DatasetType:
        # Docstring inherited.
        return self._dataset_type

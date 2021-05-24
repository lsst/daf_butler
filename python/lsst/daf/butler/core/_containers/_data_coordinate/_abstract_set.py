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

__all__ = ("DataCoordinateAbstractSet",)

from abc import abstractmethod
from typing import AbstractSet, Any, Iterable

from ...dimensions import DataCoordinate
from ._collection import DataCoordinateCollection
from ._iterable import DataCoordinateIterable


class DataCoordinateAbstractSet(DataCoordinateCollection):
    """An abstract base class for homogeneous set-like containers of data IDs.

    Notes
    -----
    `DataCoordinateAbstractSet` does not formally implement the
    `collections.abc.Set` interface, because that requires many binary
    operations to accept any set-like object as the other argument (regardless
    of what its elements might be), and it's much easier to ensure those
    operations never behave surprisingly if we restrict them to
    `DataCoordinateAbstractSet` or (sometimes) `DataCoordinateIterable`, and in
    most cases restrict that they identify the same dimensions and `hasFull` /
    `hasRecords` state.  In particular:

    - a `DataCoordinateAbstractSet` will compare as not equal to any object
      that is not a `DataCoordinateAbstractSet`, even native Python sets
      containing the exact same elements;

    - subset/superset comparison _operators_ (``<``, ``>``, ``<=``, ``>=``)
      require both operands to be `DataCoordinateAbstractSet` instances that
      have the same dimensions (i.e. ``graph`` attribute);

    - operators that create new sets (``&``, ``|``, ``^``, ``-``) require both
      operands to be `DataCoordinateAbstractSet` instances that have the same
      dimensions;
    """

    __slots__ = ()

    @abstractmethod
    def _unwrap(self) -> AbstractSet[DataCoordinate]:
        # Docstring inherited.
        raise NotImplementedError()

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, DataCoordinateAbstractSet):
            return self.graph == other.graph and self._unwrap() == other._unwrap()
        return False

    def __le__(self, other: DataCoordinateAbstractSet) -> bool:
        self._common_state.require_same_dimensions(other._common_state)
        return self._unwrap() <= other._unwrap()

    def __ge__(self, other: DataCoordinateAbstractSet) -> bool:
        self._common_state.require_same_dimensions(other._common_state)
        return self._unwrap() >= other._unwrap()

    def __lt__(self, other: DataCoordinateAbstractSet) -> bool:
        self._common_state.require_same_dimensions(other._common_state)
        return self._unwrap() < other._unwrap()

    def __gt__(self, other: DataCoordinateAbstractSet) -> bool:
        self._common_state.require_same_dimensions(other._common_state)
        return self._unwrap() > other._unwrap()

    def isdisjoint(self, other: DataCoordinateIterable) -> bool:
        """Test whether there are no data IDs in both ``self`` and ``other``.

        Parameters
        ----------
        other : `DataCoordinateIterable`
            An iterable of data IDs with ``other.graph == self.graph``.

        Returns
        -------
        isdisjoint : `bool`
            `True` if there are no data IDs in both ``self`` and ``other``, and
            `False` otherwise.
        """
        self._common_state.require_same_dimensions(other._common_state)
        return self._unwrap().isdisjoint(other._unwrap())

    def __and__(self, other: DataCoordinateAbstractSet) -> DataCoordinateAbstractSet:
        # Containers may have different has_full/has_records state, but in
        # intersection we can choose to only return elements from the container
        # with the most state, and so we do.
        self._common_state.require_same_dimensions(other._common_state)
        if self._common_state == other._common_state:
            # Easy short-circuit case: don't care which container results are
            # drawn from, because they have same state, so we can delegate to
            # underlying set-like object.
            return self._wrap(self._unwrap() & other._unwrap(), self._common_state)
        elif self._common_state >= other._common_state:
            target_state = self._common_state
            return_these = self._unwrap()
            if_they_are_in_this = other._unwrap()
        elif self._common_state <= other._common_state:
            target_state = other._common_state
            return_these = other._unwrap()
            if_they_are_in_this = other._unwrap()
        else:
            raise AssertionError(
                "Logic bug: data coordinate states must satisfy a superset or subset relation "
                "when the dimensions are the same."
            )
        return self._wrap((d for d in return_these if d in if_they_are_in_this), target_state)

    def __or__(self, other: DataCoordinateAbstractSet) -> DataCoordinateAbstractSet:
        iterable: Iterable[DataCoordinate] = self._unwrap() | other._unwrap()
        if self._common_state == other._common_state:
            # Easy case: don't care which container results are drawn from,
            # because they have same state, so we can delegate to underlying
            # set-like object.
            target_state = self._common_state
        else:
            # Containers have different has_full/has_records state, but in
            # union we can get elements from both.  Compute the minimal state,
            # and downgrade all results to that.
            target_state = self._common_state.common_dimensions_intersection(other._common_state)
            iterable = (target_state.downgrade(data_id) for data_id in iterable)
        return self._wrap(iterable, target_state)

    def __xor__(self, other: DataCoordinateAbstractSet) -> DataCoordinateAbstractSet:
        iterable: Iterable[DataCoordinate] = self._unwrap() ^ other._unwrap()
        if self._common_state == other._common_state:
            # Easy case: don't care which container results are drawn from,
            # because they have same state, so we can delegate to underlying
            # set-like object.
            target_state = self._common_state
        else:
            # Containers have different has_full/has_records state, but in
            # symmetric difference we can get elements from both.  Compute the
            # minimal state, and downgrade all results to that.
            target_state = self._common_state.common_dimensions_intersection(other._common_state)
            iterable = (target_state.downgrade(data_id) for data_id in iterable)
        return self._wrap(iterable, target_state)

    def __sub__(self, other: DataCoordinateAbstractSet) -> DataCoordinateAbstractSet:
        # Containers may have different has_full/has_records state, but in
        # difference we always return elements from self.
        return self._wrap(self._unwrap() - other._unwrap(), self._common_state)

    def to_set(self) -> DataCoordinateAbstractSet:
        # Docstring inherited from DataCoordinateIterable.
        return self

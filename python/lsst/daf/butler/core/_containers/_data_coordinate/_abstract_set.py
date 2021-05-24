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
from typing import AbstractSet, Any, Iterable, Optional

from ...dimensions import DataCoordinate, DataId, DimensionGraph
from ._collection import DataCoordinateCollection
from ._iterable import DataCoordinateIterable, DataCoordinateCommonState


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
    most cases restrict that they identify the same dimensions.  In particular:

    - a `DataCoordinateAbstractSet` will compare as not equal to any object
      that is not a `DataCoordinateAbstractSet`, even native Python sets
      containing the exact same elements;

    - subset/superset comparison _operators_ (``<``, ``>``, ``<=``, ``>=``)
      require both operands to be `DataCoordinateAbstractSet` instances that
      have the same dimensions (i.e. ``graph`` attribute);

    - `issubset`, `issuperset`, and `isdisjoint` require the other argument to
      be a `DataCoordinateIterable` with the same dimensions;

    - operators that create new sets (``&``, ``|``, ``^``, ``-``) require both
      operands to be `DataCoordinateAbstractSet` instances that have the same
      dimensions;

    - named methods that create new sets (`intersection`, `union`,
      `symmetric_difference`, `difference`) require the other operand to be a
      `DataCoordinateIterable` with the same dimensions.  In addition, when the
      two operands differ in the return values of `hasFull` and/or
      `hasRecords`, we make no guarantees about what those methods will return
      on the new `DataCoordinateAbstractSet` (other than that they will
      accurately reflect what elements are in the new set - we just don't
      control which elements are contributed by each operand).

    Subclasses must implement `_unwrap` and the `graph` property.  They may
    benefit from implementing other methods for efficiency, especially if
    `_unwrap` requires making any kind of copy.
    """

    __slots__ = ()

    @classmethod
    def standardize(
        cls,
        data_ids: Iterable[DataId],
        graph: DimensionGraph,
        *,
        defaults: Optional[DataCoordinate] = None,
        **kwargs: Any,
    ) -> DataCoordinateAbstractSet:
        """Return a container with standardized versions of the given data IDs.

        Parameters
        ----------
        data_ids : `Iterable` [ `DataId` ]
            Data IDs to standardize.  Each may be a mapping with `str` keys or
            a `NamedKeyMapping` with `Dimension` keys such as a
            `DataCoordinate` instance.
        graph : `DimensionGraph`
            Target dimensions for the standardized data IDs.  Unlike
            `DataCoordinate.standardize`, this must be provided explicitly.
        defaults : `DataCoordinate`, optional
            Default dimension key-value pairs to use when needed.  These are
            ignored if a different value is provided for the same key in
            ``data_ids`` or `**kwargs``.
        **kwargs
            Additional keyword arguments are treated like additional key-value
            pairs in the elements of ``data_ids``, and override any already
            present.

        Returns
        -------
        standardized : `DataCoordinateAbstractSet` subclass instance
            A `DataCoordinateAbstractSet` with ``subset.graph == graph``.
            May be ``data_ids`` if it is already a `DataCoordinateAbstractSet`
            for immutable classes only if ``graph == self.graph``.  Elements
            are equivalent to those that would be created by calling
            `DataCoordinate.standardize` on all elements in ``self``, with
            with deduplication guaranteed but no ordering guarantees.
        """
        return super().standardize(data_ids, graph, default=defaults, **kwargs).toSet()

    @classmethod
    @abstractmethod
    def _wrap(
        cls,
        native: AbstractSet[DataCoordinate],
        common: DataCoordinateCommonState,
    ) -> DataCoordinateAbstractSet:
        """Return a new `DataCoordinateAbstractSet` subclass instance that
        wraps the given native set-like object.

        Parameters
        ----------
        native : `AbstractSet` [ `DataCoordinate` ]
            Built-in set-like object to wrap.
        common : `DataCoordinateCommonState`
            Structure containing the `DimensionGraph` and the possibly-known
            values for `hasFull` and `hasRecords` *at construction*.  Note
            that these values may not be guaranteed to remain true for
            mutable return types.

        Returns
        -------
        wrapped : `DataCoordinateAbstractSet`
            Wrapped set.
        """
        raise NotImplementedError()

    @abstractmethod
    def _unwrap(self) -> AbstractSet[DataCoordinate]:
        # Docstring inherited.
        raise NotImplementedError()

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, DataCoordinateAbstractSet):
            return self.graph == other.graph and self._unwrap() == other._unwrap()
        return False

    def __le__(self, other: DataCoordinateAbstractSet) -> bool:
        self._common_state.check_mix_with(other._common_state)
        return self._unwrap() <= other._unwrap()

    def __ge__(self, other: DataCoordinateAbstractSet) -> bool:
        self._common_state.check_mix_with(other._common_state)
        return self._unwrap() >= other._unwrap()

    def __lt__(self, other: DataCoordinateAbstractSet) -> bool:
        self._common_state.check_mix_with(other._common_state)
        return self._unwrap() < other._unwrap()

    def __gt__(self, other: DataCoordinateAbstractSet) -> bool:
        self._common_state.check_mix_with(other._common_state)
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
        self._common_state.check_mix_with(other._common_state)
        return self._unwrap().isdisjoint(other._unwrap())

    def __and__(self, other: DataCoordinateAbstractSet) -> DataCoordinateAbstractSet:
        return self._wrap(
            self._unwrap() & other._unwrap(),
            self._common_state.mixed_with(other._common_state)
        )

    def __or__(self, other: DataCoordinateAbstractSet) -> DataCoordinateAbstractSet:
        return self._wrap(
            self._unwrap() | other._unwrap(),
            self._common_state.mixed_with(other._common_state)
        )

    def __xor__(self, other: DataCoordinateAbstractSet) -> DataCoordinateAbstractSet:
        return self._wrap(
            self._unwrap() ^ other._unwrap(),
            self._common_state.mixed_with(other._common_state)
        )

    def __sub__(self, other: DataCoordinateAbstractSet) -> DataCoordinateAbstractSet:
        return self._wrap(
            self._unwrap() - other._unwrap(),
            self._common_state.mixed_with(other._common_state)
        )

    def toSet(self) -> DataCoordinateAbstractSet:
        # Docstring inherited from DataCoordinateIterable.
        return self

    def subset(self, graph: DimensionGraph) -> DataCoordinateAbstractSet:
        """Return a subset set.

        This subset set contains data IDs that identify a subset of the
        dimensions that this one's do.

        Parameters
        ----------
        graph : `DimensionGraph`
            Dimensions to be identified by the data IDs in the returned
            set.  Must be a subset of ``self.graph``.

        Returns
        -------
        subset : `DataCoordinateAbstractSet`
            A `DataCoordinateAbstractSet` with ``subset.graph == graph``.
            May be ``self`` for immutable classes only if
            ``graph == self.graph``.  Elements are equivalent to those that
            would be created by calling `DataCoordinate.subset` on all elements
            in ``self``, with deduplication guaranteed but reordering possible.
        """
        common = self._common_state.subset(graph)
        return self._wrap({data_id.subset(graph) for data_id in self._unwrap()}, common)

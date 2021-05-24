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

__all__ = ("DataCoordinateSet",)

from typing import (
    AbstractSet,
    Any,
    Optional,
)

from ...dimensions import DataCoordinate, DimensionGraph
from ._collection import _DataCoordinateCollectionBase
from ._iterable import DataCoordinateIterable


class DataCoordinateSet(_DataCoordinateCollectionBase):
    """Iterable iteration that is set-like.

    A `DataCoordinateIterable` implementation that adds some set-like
    functionality, and is backed by a true set-like object.

    Parameters
    ----------
    dataIds : `collections.abc.Set` [ `DataCoordinate` ]
        A set of `DataCoordinate` instances, with dimensions equal to
        ``graph``.  If this is a mutable object, the caller must be able to
        guarantee that it will not be modified by any other holders.
    graph : `DimensionGraph`
        Dimensions identified by all data IDs in the set.
    hasFull : `bool`, optional
        If `True`, the caller guarantees that `DataCoordinate.hasFull` returns
        `True` for all given data IDs.  If `False`, no such guarantee is made,
        and `DataCoordinateSet.hasFull` will always return `False`.  If `None`
        (default), `DataCoordinateSet.hasFull` will be computed from the given
        data IDs, immediately if ``check`` is `True`, or on first use if
        ``check`` is `False`.
    hasRecords : `bool`, optional
        If `True`, the caller guarantees that `DataCoordinate.hasRecords`
        returns `True` for all given data IDs.  If `False`, no such guarantee
        is made and `DataCoordinateSet.hasRecords` will always return `False`.
        If `None` (default), `DataCoordinateSet.hasRecords` will be computed
        from the given data IDs, immediately if ``check`` is `True`, or on
        first use if ``check`` is `False`.
    check: `bool`, optional
        If `True` (default) check that all data IDs are consistent with the
        given ``graph`` and state flags at construction.  If `False`, no
        checking will occur.

    Notes
    -----
    `DataCoordinateSet` does not formally implement the `collections.abc.Set`
    interface, because that requires many binary operations to accept any
    set-like object as the other argument (regardless of what its elements
    might be), and it's much easier to ensure those operations never behave
    surprisingly if we restrict them to `DataCoordinateSet` or (sometimes)
    `DataCoordinateIterable`, and in most cases restrict that they identify
    the same dimensions.  In particular:

    - a `DataCoordinateSet` will compare as not equal to any object that is
      not a `DataCoordinateSet`, even native Python sets containing the exact
      same elements;

    - subset/superset comparison _operators_ (``<``, ``>``, ``<=``, ``>=``)
      require both operands to be `DataCoordinateSet` instances that have the
      same dimensions (i.e. ``graph`` attribute);

    - `issubset`, `issuperset`, and `isdisjoint` require the other argument to
      be a `DataCoordinateIterable` with the same dimensions;

    - operators that create new sets (``&``, ``|``, ``^``, ``-``) require both
      operands to be `DataCoordinateSet` instances that have the same
      dimensions _and_ the same ``dtype``;

    - named methods that create new sets (`intersection`, `union`,
      `symmetric_difference`, `difference`) require the other operand to be a
      `DataCoordinateIterable` with the same dimensions _and_ the same
      ``dtype``.

    In addition, when the two operands differ in the return values of `hasFull`
    and/or `hasRecords`, we make no guarantees about what those methods will
    return on the new `DataCoordinateSet` (other than that they will accurately
    reflect what elements are in the new set - we just don't control which
    elements are contributed by each operand).
    """

    def __init__(
        self,
        dataIds: AbstractSet[DataCoordinate],
        graph: DimensionGraph,
        *,
        hasFull: Optional[bool] = None,
        hasRecords: Optional[bool] = None,
        check: bool = True,
    ):
        super().__init__(dataIds, graph, hasFull=hasFull, hasRecords=hasRecords, check=check)

    _dataIds: AbstractSet[DataCoordinate]

    __slots__ = ()

    def __str__(self) -> str:
        return str(set(self._dataIds))

    def __repr__(self) -> str:
        return (
            f"DataCoordinateSet({set(self._dataIds)}, {self._graph!r}, "
            f"hasFull={self._hasFull}, hasRecords={self._hasRecords})"
        )

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, DataCoordinateSet):
            return self._graph == other._graph and self._dataIds == other._dataIds
        return False

    def __le__(self, other: DataCoordinateSet) -> bool:
        if self.graph != other.graph:
            raise ValueError(f"Inconsistent dimensions in set comparision: {self.graph} != {other.graph}.")
        return self._dataIds <= other._dataIds

    def __ge__(self, other: DataCoordinateSet) -> bool:
        if self.graph != other.graph:
            raise ValueError(f"Inconsistent dimensions in set comparision: {self.graph} != {other.graph}.")
        return self._dataIds >= other._dataIds

    def __lt__(self, other: DataCoordinateSet) -> bool:
        if self.graph != other.graph:
            raise ValueError(f"Inconsistent dimensions in set comparision: {self.graph} != {other.graph}.")
        return self._dataIds < other._dataIds

    def __gt__(self, other: DataCoordinateSet) -> bool:
        if self.graph != other.graph:
            raise ValueError(f"Inconsistent dimensions in set comparision: {self.graph} != {other.graph}.")
        return self._dataIds > other._dataIds

    def issubset(self, other: DataCoordinateIterable) -> bool:
        """Test whether ``self`` contains all data IDs in ``other``.

        Parameters
        ----------
        other : `DataCoordinateIterable`
            An iterable of data IDs with ``other.graph == self.graph``.

        Returns
        -------
        issubset : `bool`
            `True` if all data IDs in ``self`` are also in ``other``, and
            `False` otherwise.
        """
        if self.graph != other.graph:
            raise ValueError(f"Inconsistent dimensions in set comparision: {self.graph} != {other.graph}.")
        return self._dataIds <= other.toSet()._dataIds

    def issuperset(self, other: DataCoordinateIterable) -> bool:
        """Test whether ``other`` contains all data IDs in ``self``.

        Parameters
        ----------
        other : `DataCoordinateIterable`
            An iterable of data IDs with ``other.graph == self.graph``.

        Returns
        -------
        issuperset : `bool`
            `True` if all data IDs in ``other`` are also in ``self``, and
            `False` otherwise.
        """
        if self.graph != other.graph:
            raise ValueError(f"Inconsistent dimensions in set comparision: {self.graph} != {other.graph}.")
        return self._dataIds >= other.toSet()._dataIds

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
        if self.graph != other.graph:
            raise ValueError(f"Inconsistent dimensions in set comparision: {self.graph} != {other.graph}.")
        return self._dataIds.isdisjoint(other.toSet()._dataIds)

    def __and__(self, other: DataCoordinateSet) -> DataCoordinateSet:
        if self.graph != other.graph:
            raise ValueError(f"Inconsistent dimensions in set operation: {self.graph} != {other.graph}.")
        return DataCoordinateSet(self._dataIds & other._dataIds, self.graph, check=False)

    def __or__(self, other: DataCoordinateSet) -> DataCoordinateSet:
        if self.graph != other.graph:
            raise ValueError(f"Inconsistent dimensions in set operation: {self.graph} != {other.graph}.")
        return DataCoordinateSet(self._dataIds | other._dataIds, self.graph, check=False)

    def __xor__(self, other: DataCoordinateSet) -> DataCoordinateSet:
        if self.graph != other.graph:
            raise ValueError(f"Inconsistent dimensions in set operation: {self.graph} != {other.graph}.")
        return DataCoordinateSet(self._dataIds ^ other._dataIds, self.graph, check=False)

    def __sub__(self, other: DataCoordinateSet) -> DataCoordinateSet:
        if self.graph != other.graph:
            raise ValueError(f"Inconsistent dimensions in set operation: {self.graph} != {other.graph}.")
        return DataCoordinateSet(self._dataIds - other._dataIds, self.graph, check=False)

    def intersection(self, other: DataCoordinateIterable) -> DataCoordinateSet:
        """Return a new set that contains all data IDs from parameters.

        Parameters
        ----------
        other : `DataCoordinateIterable`
            An iterable of data IDs with ``other.graph == self.graph``.

        Returns
        -------
        intersection : `DataCoordinateSet`
            A new `DataCoordinateSet` instance.
        """
        if self.graph != other.graph:
            raise ValueError(f"Inconsistent dimensions in set operation: {self.graph} != {other.graph}.")
        return DataCoordinateSet(self._dataIds & other.toSet()._dataIds, self.graph, check=False)

    def union(self, other: DataCoordinateIterable) -> DataCoordinateSet:
        """Return a new set that contains all data IDs in either parameters.

        Parameters
        ----------
        other : `DataCoordinateIterable`
            An iterable of data IDs with ``other.graph == self.graph``.

        Returns
        -------
        intersection : `DataCoordinateSet`
            A new `DataCoordinateSet` instance.
        """
        if self.graph != other.graph:
            raise ValueError(f"Inconsistent dimensions in set operation: {self.graph} != {other.graph}.")
        return DataCoordinateSet(self._dataIds | other.toSet()._dataIds, self.graph, check=False)

    def symmetric_difference(self, other: DataCoordinateIterable) -> DataCoordinateSet:
        """Return a new set with all data IDs in either parameters, not both.

        Parameters
        ----------
        other : `DataCoordinateIterable`
            An iterable of data IDs with ``other.graph == self.graph``.

        Returns
        -------
        intersection : `DataCoordinateSet`
            A new `DataCoordinateSet` instance.
        """
        if self.graph != other.graph:
            raise ValueError(f"Inconsistent dimensions in set operation: {self.graph} != {other.graph}.")
        return DataCoordinateSet(self._dataIds ^ other.toSet()._dataIds, self.graph, check=False)

    def difference(self, other: DataCoordinateIterable) -> DataCoordinateSet:
        """Return a new set with all data IDs in this that are not in other.

        Parameters
        ----------
        other : `DataCoordinateIterable`
            An iterable of data IDs with ``other.graph == self.graph``.

        Returns
        -------
        intersection : `DataCoordinateSet`
            A new `DataCoordinateSet` instance.
        """
        if self.graph != other.graph:
            raise ValueError(f"Inconsistent dimensions in set operation: {self.graph} != {other.graph}.")
        return DataCoordinateSet(self._dataIds - other.toSet()._dataIds, self.graph, check=False)

    def toSet(self) -> DataCoordinateSet:
        # Docstring inherited from DataCoordinateIterable.
        return self

    def subset(self, graph: DimensionGraph) -> DataCoordinateSet:
        """Return a set whose data IDs identify a subset.

        Parameters
        ----------
        graph : `DimensionGraph`
            Dimensions to be identified by the data IDs in the returned
            iterable.  Must be a subset of ``self.graph``.

        Returns
        -------
        set : `DataCoordinateSet`
            A `DataCoordinateSet` with ``set.graph == graph``.
            Will be ``self`` if ``graph == self.graph``.  Elements are
            equivalent to those that would be created by calling
            `DataCoordinate.subset` on all elements in ``self``, with
            deduplication but and in arbitrary order.
        """
        if graph == self.graph:
            return self
        return DataCoordinateSet(
            {dataId.subset(graph) for dataId in self._dataIds}, graph, **self._subsetKwargs(graph)
        )

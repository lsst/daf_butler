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
    "DataCoordinateIterable",
    "DataCoordinateSet",
    "DataCoordinateSequence",
)

from abc import abstractmethod
from typing import (
    AbstractSet,
    Any,
    Callable,
    Collection,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    overload,
    Sequence,
)

import sqlalchemy

from ..simpleQuery import SimpleQuery
from ._coordinate import DataCoordinate
from ._graph import DimensionGraph
from ._universe import DimensionUniverse


class DataCoordinateIterable(Iterable[DataCoordinate]):
    """An abstract base class for homogeneous iterables of data IDs.

    All elements of a `DataCoordinateIterable` identify the same set of
    dimensions (given by the `graph` property) and generally have the same
    `DataCoordinate.has_full` and `DataCoordinate.has_records` flag values.
    """

    __slots__ = ()

    @staticmethod
    def fromScalar(dataId: DataCoordinate) -> _ScalarDataCoordinateIterable:
        """Return a `DataCoordinateIterable` containing the single data ID.

        Parameters
        ----------
        dataId : `DataCoordinate`
            Data ID to adapt.  Must be a true `DataCoordinate` instance, not
            an arbitrary mapping.  No runtime checking is performed.

        Returns
        -------
        iterable : `DataCoordinateIterable`
            A `DataCoordinateIterable` instance of unspecified (i.e.
            implementation-detail) subclass.  Guaranteed to implement
            the `collections.abc.Sized` (i.e. `__len__`) and
            `collections.abc.Container` (i.e. `__contains__`) interfaces as
            well as that of `DataCoordinateIterable`.
        """
        return _ScalarDataCoordinateIterable(dataId)

    @property
    @abstractmethod
    def graph(self) -> DimensionGraph:
        """Dimensions identified by these data IDs (`DimensionGraph`)."""
        raise NotImplementedError()

    @property
    def universe(self) -> DimensionUniverse:
        """Universe that defines all known compatible dimensions.

        (`DimensionUniverse`).
        """
        return self.graph.universe

    @property
    @abstractmethod
    def has_full(self) -> bool:
        """Whether all data IDs in this iterable identify all dimensions, not
        just required dimensions.

        If `True`, ``all(d.has_full for d in iterable)`` is guaranteed.  If
        `False`, no guarantees are made.
        """
        raise NotImplementedError()

    def hasFull(self) -> bool:
        """Backwards compatibility method getter for `has_full`.

        New code should use the `has_full` property instead.
        """
        return self.has_full

    @property
    @abstractmethod
    def has_records(self) -> bool:
        """Whether all data IDs in this iterable contain records.

        If `True`, ``all(d.has_records for d in iterable)`` is guaranteed. If
        `False`, no guarantees are made.
        """
        raise NotImplementedError()

    def hasRecords(self) -> bool:
        """Backwards compatibility method getter for `has_records`.

        New code should use the `has_records` property instead.
        """
        return self.has_records

    def toSet(self) -> DataCoordinateSet:
        """Transform this iterable into a `DataCoordinateSet`.

        Returns
        -------
        set : `DataCoordinateSet`
            A `DatasetCoordinateSet` instance with the same elements as
            ``self``, after removing any duplicates.  May be ``self`` if it is
            already a `DataCoordinateSet`.
        """
        return DataCoordinateSet(frozenset(self), graph=self.graph,
                                 has_full=self.has_full,
                                 has_records=self.has_records,
                                 check=False)

    def toSequence(self) -> DataCoordinateSequence:
        """Transform this iterable into a `DataCoordinateSequence`.

        Returns
        -------
        seq : `DataCoordinateSequence`
            A new `DatasetCoordinateSequence` with the same elements as
            ``self``, in the same order.  May be ``self`` if it is already a
            `DataCoordinateSequence`.
        """
        return DataCoordinateSequence(tuple(self), graph=self.graph,
                                      has_full=self.has_full,
                                      has_records=self.has_records,
                                      check=False)

    def constrain(self, query: SimpleQuery, columns: Callable[[str], sqlalchemy.sql.ColumnElement]) -> None:
        """Constrain a SQL query to include or relate to only known data IDs.

        Parameters
        ----------
        query : `SimpleQuery`
            Struct that represents the SQL query to constrain, either by
            appending to its WHERE clause, joining a new table or subquery,
            or both.
        columns : `Callable`
            A callable that accepts `str` dimension names and returns
            SQLAlchemy objects representing a column for that dimension's
            primary key value in the query.
        """
        toOrTogether: List[sqlalchemy.sql.ColumnElement] = []
        for dataId in self:
            toOrTogether.append(
                sqlalchemy.sql.and_(*[
                    columns(dimension.name) == dataId[dimension.name]
                    for dimension in self.graph.required
                ])
            )
        query.where.append(sqlalchemy.sql.or_(*toOrTogether))

    @abstractmethod
    def subset(self, graph: DimensionGraph) -> DataCoordinateIterable:
        """Return a subset iterable.

        This subset iterable returns data IDs that identify a subset of the
        dimensions that this one's do.

        Parameters
        ----------
        graph : `DimensionGraph`
            Dimensions to be identified by the data IDs in the returned
            iterable.  Must be a subset of ``self.graph``.

        Returns
        -------
        iterable : `DataCoordinateIterable`
            A `DataCoordinateIterable` with ``iterable.graph == graph``.
            May be ``self`` if ``graph == self.graph``.  Elements are
            equivalent to those that would be created by calling
            `DataCoordinate.subset` on all elements in ``self``, possibly
            with deduplication and/or reordeding (depending on the subclass,
            which may make more specific guarantees).
        """
        raise NotImplementedError()


class _ScalarDataCoordinateIterable(DataCoordinateIterable):
    """An iterable for a single `DataCoordinate`.

    A `DataCoordinateIterable` implementation that adapts a single
    `DataCoordinate` instance.

    This class should only be used directly by other code in the module in
    which it is defined; all other code should interact with it only through
    the `DataCoordinateIterable` interface.

    Parameters
    ----------
    dataId : `DataCoordinate`
        The data ID to adapt.
    """

    def __init__(self, dataId: DataCoordinate):
        self._dataId = dataId

    __slots__ = ("_dataId",)

    def __iter__(self) -> Iterator[DataCoordinate]:
        yield self._dataId

    def __len__(self) -> int:
        return 1

    def __contains__(self, key: Any) -> bool:
        if isinstance(key, DataCoordinate):
            return key == self._dataId
        else:
            return False

    @property
    def graph(self) -> DimensionGraph:
        # Docstring inherited from DataCoordinateIterable.
        return self._dataId.graph

    @property
    def has_full(self) -> bool:
        # Docstring inherited from DataCoordinateIterable.
        return self._dataId.has_full

    @property
    def has_records(self) -> bool:
        # Docstring inherited from DataCoordinateIterable.
        return self._dataId.has_records

    def subset(self, graph: DimensionGraph) -> _ScalarDataCoordinateIterable:
        # Docstring inherited from DataCoordinateIterable.
        return _ScalarDataCoordinateIterable(self._dataId.subset(graph))


class _DataCoordinateCollectionBase(DataCoordinateIterable):
    """A partial iterable implementation backed by native Python collection.

    A partial `DataCoordinateIterable` implementation that is backed by a
    native Python collection.

    This class is intended only to be used as an intermediate base class for
    `DataCoordinateIterables` that assume a more specific type of collection
    and can hence make more informed choices for how to implement some methods.

    Parameters
    ----------
    dataIds : `collections.abc.Collection` [ `DataCoordinate` ]
         A collection of `DataCoordinate` instances, with dimensions equal to
        ``graph``.
    graph : `DimensionGraph`
        Dimensions identified by all data IDs in the set.
    has_full : `bool`, optional
        If `True`, the caller guarantees that `DataCoordinate.has_full` is
        `True` for all given data IDs.  If `False`, no such guarantee is made,
        and `has_full` will always be `False`.  If `None` (default),
        `has_full` will be computed from the given data IDs, immediately if
        ``check`` is `True`, or on first use if ``check`` is `False`.
    has_records : `bool`, optional
         If `True`, the caller guarantees that `DataCoordinate.has_records` is
         `True` for all given data IDs.  If `False`, no such guarantee is made
         and `has_records` will always be `False`.  If `None` (default),
         `has_records` will be computed from the given data IDs, immediately if
         ``check`` is `True`, or on first use if ``check`` is `False`.
    check: `bool`, optional
        If `True` (default) check that all data IDs are consistent with the
        given ``graph`` and state flags at construction.  If `False`, no
        checking will occur.
    """

    def __init__(self, dataIds: Collection[DataCoordinate], graph: DimensionGraph, *,
                 has_full: Optional[bool] = None, has_records: Optional[bool] = None,
                 check: bool = True):
        self._dataIds = dataIds
        self._graph = graph
        if check:
            for dataId in self._dataIds:
                if has_full and not dataId.has_full:
                    raise ValueError(f"{dataId} is not complete, but is required to be.")
                if has_records and not dataId.has_records:
                    raise ValueError(f"{dataId} has no records, but is required to.")
                if dataId.graph != self._graph:
                    raise ValueError(f"Bad dimensions {dataId.graph}; expected {self._graph}.")
            if has_full is None:
                has_full = all(dataId.has_full for dataId in self._dataIds)
            if has_records is None:
                has_records = all(dataId.has_records for dataId in self._dataIds)
        self._has_full = has_full
        self._has_records = has_records

    __slots__ = ("_graph", "_dataIds", "_has_full", "_has_records")

    @property
    def graph(self) -> DimensionGraph:
        # Docstring inherited from DataCoordinateIterable.
        return self._graph

    @property
    def has_full(self) -> bool:
        # Docstring inherited from DataCoordinateIterable.
        if self._has_full is None:
            self._has_full = all(dataId.has_full for dataId in self._dataIds)
        return self._has_full

    @property
    def has_records(self) -> bool:
        # Docstring inherited from DataCoordinateIterable.
        if self._has_records is None:
            self._has_records = all(dataId.has_records for dataId in self._dataIds)
        return self._has_records

    def toSet(self) -> DataCoordinateSet:
        # Docstring inherited from DataCoordinateIterable.
        # Override base class to pass in attributes instead of results of
        # method calls for _has_full and _has_records - those can be None,
        # and hence defer checking if that's what the user originally wanted.
        return DataCoordinateSet(frozenset(self._dataIds), graph=self._graph,
                                 has_full=self._has_full,
                                 has_records=self._has_records,
                                 check=False)

    def toSequence(self) -> DataCoordinateSequence:
        # Docstring inherited from DataCoordinateIterable.
        # Override base class to pass in attributes instead of results of
        # method calls for _has_full and _has_records - those can be None,
        # and hence defer checking if that's what the user originally wanted.
        return DataCoordinateSequence(tuple(self._dataIds), graph=self._graph,
                                      has_full=self._has_full,
                                      has_records=self._has_records,
                                      check=False)

    def __iter__(self) -> Iterator[DataCoordinate]:
        return iter(self._dataIds)

    def __len__(self) -> int:
        return len(self._dataIds)

    def __contains__(self, key: Any) -> bool:
        key = DataCoordinate.standardize(key, universe=self.universe)
        return key in self._dataIds

    def _subsetKwargs(self, graph: DimensionGraph) -> Dict[str, Any]:
        """Return constructor kwargs useful for subclasses implementing subset.

        Parameters
        ----------
        graph : `DimensionGraph`
            Dimensions passed to `subset`.

        Returns
        -------
        kwargs : `dict`
            A dict with `has_full`, `has_records`, and `check` keys, associated
            with the appropriate values for a `subset` operation with the given
            dimensions.
        """
        has_full: Optional[bool]
        if graph.dimensions <= self.graph.required:
            has_full = True
        else:
            has_full = self._has_full
        return dict(has_full=has_full, has_records=self._has_records, check=False)


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
    has_full : `bool`, optional
        If `True`, the caller guarantees that `DataCoordinate.has_full` is
        `True` for all given data IDs.  If `False`, no such guarantee is made,
        and `DataCoordinateSet.has_full` will always be `False`.  If `None`
        (default), `DataCoordinateSet.has_full` will be computed from the given
        data IDs, immediately if ``check`` is `True`, or on first use if
        ``check`` is `False`.
    has_records : `bool`, optional
        If `True`, the caller guarantees that `DataCoordinate.has_records` is
        `True` for all given data IDs.  If `False`, no such guarantee is made
        and `DataCoordinateSet.has_records` will always be `False`.  If `None`
        (default), `DataCoordinateSet.has_records` will be computed from the
        given data IDs, immediately if ``check`` is `True`, or on first use if
        ``check`` is `False`.
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

    In addition, when the two operands differ in the values of `has_full`
    and/or `has_records`, we make no guarantees about what those methods will
    return on the new `DataCoordinateSet` (other than that they will accurately
    reflect what elements are in the new set - we just don't control which
    elements are contributed by each operand).
    """

    def __init__(self, dataIds: AbstractSet[DataCoordinate], graph: DimensionGraph, *,
                 has_full: Optional[bool] = None, has_records: Optional[bool] = None,
                 check: bool = True):
        super().__init__(dataIds, graph, has_full=has_full, has_records=has_records, check=check)

    _dataIds: AbstractSet[DataCoordinate]

    __slots__ = ()

    def __str__(self) -> str:
        return str(set(self._dataIds))

    def __repr__(self) -> str:
        return (f"DataCoordinateSet({set(self._dataIds)}, {self._graph!r}, "
                f"has_full={self._has_full}, has_records={self._has_records})")

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, DataCoordinateSet):
            return (
                self._graph == other._graph
                and self._dataIds == other._dataIds
            )
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
            {dataId.subset(graph) for dataId in self._dataIds},
            graph,
            **self._subsetKwargs(graph)
        )


class DataCoordinateSequence(_DataCoordinateCollectionBase, Sequence[DataCoordinate]):
    """Iterable supporting the full Sequence interface.

    A `DataCoordinateIterable` implementation that supports the full
    `collections.abc.Sequence` interface.

    Parameters
    ----------
    dataIds : `collections.abc.Sequence` [ `DataCoordinate` ]
        A sequence of `DataCoordinate` instances, with dimensions equal to
        ``graph``.
    graph : `DimensionGraph`
        Dimensions identified by all data IDs in the set.
    has_full : `bool`, optional
        If `True`, the caller guarantees that `DataCoordinate.has_full` is
        `True` for all given data IDs.  If `False`, no such guarantee is made,
        and `DataCoordinateSet.has_full` will always be `False`.  If `None`
        (default), `DataCoordinateSet.has_full` will be computed from the given
        data IDs, immediately if ``check`` is `True`, or on first use if
        ``check`` is `False`.
    has_records : `bool`, optional
        If `True`, the caller guarantees that `DataCoordinate.has_records` is
        `True` for all given data IDs.  If `False`, no such guarantee is made
        and `DataCoordinateSet.has_records` will always be `False`.  If `None`
        (default), `DataCoordinateSet.has_records` will be computed from the
        given data IDs, immediately if ``check`` is `True`, or on first use if
        ``check`` is `False`.
    check: `bool`, optional
        If `True` (default) check that all data IDs are consistent with the
        given ``graph`` and state flags at construction.  If `False`, no
        checking will occur.
    """

    def __init__(self, dataIds: Sequence[DataCoordinate], graph: DimensionGraph, *,
                 has_full: Optional[bool] = None, has_records: Optional[bool] = None,
                 check: bool = True):
        super().__init__(tuple(dataIds), graph, has_full=has_full, has_records=has_records, check=check)

    _dataIds: Sequence[DataCoordinate]

    __slots__ = ()

    def __str__(self) -> str:
        return str(tuple(self._dataIds))

    def __repr__(self) -> str:
        return (f"DataCoordinateSequence({tuple(self._dataIds)}, {self._graph!r}, "
                f"has_full={self._has_full}, has_records={self._has_records})")

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, DataCoordinateSequence):
            return (
                self._graph == other._graph
                and self._dataIds == other._dataIds
            )
        return False

    @overload
    def __getitem__(self, index: int) -> DataCoordinate:
        pass

    @overload  # noqa: F811 (FIXME: remove for py 3.8+)
    def __getitem__(self, index: slice) -> DataCoordinateSequence:  # noqa: F811
        pass

    def __getitem__(self, index: Any) -> Any:  # noqa: F811
        r = self._dataIds[index]
        if isinstance(index, slice):
            return DataCoordinateSequence(r, self._graph,
                                          has_full=self._has_full, has_records=self._has_records,
                                          check=False)
        return r

    def toSequence(self) -> DataCoordinateSequence:
        # Docstring inherited from DataCoordinateIterable.
        return self

    def subset(self, graph: DimensionGraph) -> DataCoordinateSequence:
        """Return a sequence whose data IDs identify a subset.

        Parameters
        ----------
        graph : `DimensionGraph`
            Dimensions to be identified by the data IDs in the returned
            iterable.  Must be a subset of ``self.graph``.

        Returns
        -------
        set : `DataCoordinateSequence`
            A `DataCoordinateSequence` with ``set.graph == graph``.
            Will be ``self`` if ``graph == self.graph``.  Elements are
            equivalent to those that would be created by calling
            `DataCoordinate.subset` on all elements in ``self``, in the same
            order and with no deduplication.
        """
        if graph == self.graph:
            return self
        return DataCoordinateSequence(
            tuple(dataId.subset(graph) for dataId in self._dataIds),
            graph,
            **self._subsetKwargs(graph)
        )

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

__all__ = (
    "DataCoordinateIterable",
    "DataCoordinateSet",
    "DataCoordinateSequence",
)

import warnings
from abc import abstractmethod
from collections.abc import Collection, Iterable, Iterator, Sequence, Set
from typing import Any, overload

from deprecated.sphinx import deprecated
from lsst.utils.introspection import find_outside_stacklevel

from ._coordinate import DataCoordinate
from ._graph import DimensionGraph
from ._group import DimensionGroup
from ._universe import DimensionUniverse


class DataCoordinateIterable(Iterable[DataCoordinate]):
    """An abstract base class for homogeneous iterables of data IDs.

    All elements of a `DataCoordinateIterable` identify the same set of
    dimensions (given by the `graph` property) and generally have the same
    `DataCoordinate.hasFull` and `DataCoordinate.hasRecords` flag values.
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

    # TODO: remove on DM-41326.
    @property
    @deprecated(
        "Deprecated in favor of .dimensions; will be removed after v26.",
        category=FutureWarning,
        version="v27",
    )
    def graph(self) -> DimensionGraph:
        """Dimensions identified by these data IDs (`DimensionGraph`)."""
        return self.dimensions._as_graph()

    @property
    @abstractmethod
    def dimensions(self) -> DimensionGroup:
        """Dimensions identified by these data IDs (`DimensionGroup`)."""
        raise NotImplementedError()

    @property
    def universe(self) -> DimensionUniverse:
        """Universe that defines all known compatible dimensions.

        (`DimensionUniverse`).
        """
        return self.dimensions.universe

    @abstractmethod
    def hasFull(self) -> bool:
        """Indicate if all data IDs in this iterable identify all dimensions.

        Not just required dimensions.

        Returns
        -------
        state : `bool`
            If `True`, ``all(d.hasFull() for d in iterable)`` is guaranteed.
            If `False`, no guarantees are made.
        """
        raise NotImplementedError()

    @abstractmethod
    def hasRecords(self) -> bool:
        """Return whether all data IDs in this iterable contain records.

        Returns
        -------
        state : `bool`
            If `True`, ``all(d.hasRecords() for d in iterable)`` is guaranteed.
            If `False`, no guarantees are made.
        """
        raise NotImplementedError()

    def toSet(self) -> DataCoordinateSet:
        """Transform this iterable into a `DataCoordinateSet`.

        Returns
        -------
        set : `DataCoordinateSet`
            A `DatasetCoordinateSet` instance with the same elements as
            ``self``, after removing any duplicates.  May be ``self`` if it is
            already a `DataCoordinateSet`.
        """
        return DataCoordinateSet(
            frozenset(self),
            dimensions=self.dimensions,
            hasFull=self.hasFull(),
            hasRecords=self.hasRecords(),
            check=False,
        )

    def toSequence(self) -> DataCoordinateSequence:
        """Transform this iterable into a `DataCoordinateSequence`.

        Returns
        -------
        seq : `DataCoordinateSequence`
            A new `DatasetCoordinateSequence` with the same elements as
            ``self``, in the same order.  May be ``self`` if it is already a
            `DataCoordinateSequence`.
        """
        return DataCoordinateSequence(
            tuple(self),
            dimensions=self.dimensions,
            hasFull=self.hasFull(),
            hasRecords=self.hasRecords(),
            check=False,
        )

    @abstractmethod
    def subset(self, dimensions: DimensionGraph | DimensionGroup | Iterable[str]) -> DataCoordinateIterable:
        """Return a subset iterable.

        This subset iterable returns data IDs that identify a subset of the
        dimensions that this one's do.

        Parameters
        ----------
        dimensions : `DimensionGraph`, `DimensionGroup`, or \
                `~collections.abc.Iterable` [ `str` ]
            Dimensions to be identified by the data IDs in the returned
            iterable.  Must be a subset of ``self.dimensions``.

        Returns
        -------
        iterable : `DataCoordinateIterable`
            A `DataCoordinateIterable` with
            ``iterable.dimensions == dimensions``.
            May be ``self`` if ``dimensions == self.dimensions``.  Elements are
            equivalent to those that would be created by calling
            `DataCoordinate.subset` on all elements in ``self``, possibly
            with deduplication and/or reordering (depending on the subclass,
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
    def dimensions(self) -> DimensionGroup:
        # Docstring inherited from DataCoordinateIterable.
        return self._dataId.dimensions

    def hasFull(self) -> bool:
        # Docstring inherited from DataCoordinateIterable.
        return self._dataId.hasFull()

    def hasRecords(self) -> bool:
        # Docstring inherited from DataCoordinateIterable.
        return self._dataId.hasRecords()

    def subset(
        self, dimensions: DimensionGraph | DimensionGroup | Iterable[str]
    ) -> _ScalarDataCoordinateIterable:
        # Docstring inherited from DataCoordinateIterable.
        dimensions = self.universe.conform(dimensions)
        return _ScalarDataCoordinateIterable(self._dataId.subset(dimensions))


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
        ``dimensions``.
    graph : `DimensionGraph`, optional
        Dimensions identified by all data IDs in the collection.  Ignored if
        ``dimensions`` is provided, and deprecated with removal after v27.
    dimensions : `~collections.abc.Iterable` [ `str` ], `DimensionGroup`, \
            or `DimensionGraph`, optional
        Dimensions identified by all data IDs in the collection.  Must be
        provided unless ``graph`` is.
    hasFull : `bool`, optional
        If `True`, the caller guarantees that `DataCoordinate.hasFull` returns
        `True` for all given data IDs.  If `False`, no such guarantee is made,
        and `hasFull` will always return `False`.  If `None` (default),
        `hasFull` will be computed from the given data IDs, immediately if
        ``check`` is `True`, or on first use if ``check`` is `False`.
    hasRecords : `bool`, optional
        If `True`, the caller guarantees that `DataCoordinate.hasRecords`
        returns `True` for all given data IDs.  If `False`, no such guarantee
        is made and `hasRecords` will always return `False`.  If `None`
        (default), `hasRecords` will be computed from the given data IDs,
        immediately if ``check`` is `True`, or on first use if ``check`` is
        `False`.
    check: `bool`, optional
        If `True` (default) check that all data IDs are consistent with the
        given ``graph`` and state flags at construction.  If `False`, no
        checking will occur.
    universe : `DimensionUniverse`
        Object that manages all dimension definitions.
    """

    def __init__(
        self,
        dataIds: Collection[DataCoordinate],
        graph: DimensionGraph | None = None,
        *,
        dimensions: Iterable[str] | DimensionGroup | DimensionGraph | None = None,
        hasFull: bool | None = None,
        hasRecords: bool | None = None,
        check: bool = True,
        universe: DimensionUniverse | None = None,
    ):
        universe = (
            universe
            or getattr(dimensions, "universe", None)
            or getattr(graph, "universe", None)
            or getattr(dataIds, "universe", None)
        )
        if universe is None:
            raise TypeError(
                "universe must be provided, either directly or via dimensions, dataIds, or graph."
            )
        if graph is not None:
            warnings.warn(
                "The 'graph' argument to DataCoordinateIterable constructors is deprecated in favor of "
                " passing an iterable of dimension names as the 'dimensions' argument, and wil be removed "
                "after v27.",
                stacklevel=find_outside_stacklevel("lsst.daf.butler"),
                category=FutureWarning,
            )
        if dimensions is not None:
            dimensions = universe.conform(dimensions)
        elif graph is not None:
            dimensions = graph.as_group()
        del graph  # Avoid accidental use later.
        if dimensions is None:
            raise TypeError("Exactly one of 'graph' or (preferably) 'dimensions' must be provided.")
        self._dataIds = dataIds
        self._dimensions = dimensions
        if check:
            for dataId in self._dataIds:
                if hasFull and not dataId.hasFull():
                    raise ValueError(f"{dataId} is not complete, but is required to be.")
                if hasRecords and not dataId.hasRecords():
                    raise ValueError(f"{dataId} has no records, but is required to.")
                if dataId.dimensions != self._dimensions:
                    raise ValueError(f"Bad dimensions {dataId.dimensions}; expected {self._dimensions}.")
            if hasFull is None:
                hasFull = all(dataId.hasFull() for dataId in self._dataIds)
            if hasRecords is None:
                hasRecords = all(dataId.hasRecords() for dataId in self._dataIds)
        self._hasFull = hasFull
        self._hasRecords = hasRecords

    __slots__ = ("_dimensions", "_dataIds", "_hasFull", "_hasRecords")

    @property
    def dimensions(self) -> DimensionGroup:
        # Docstring inherited from DataCoordinateIterable.
        return self._dimensions

    def hasFull(self) -> bool:
        # Docstring inherited from DataCoordinateIterable.
        if self._hasFull is None:
            self._hasFull = all(dataId.hasFull() for dataId in self._dataIds)
        return self._hasFull

    def hasRecords(self) -> bool:
        # Docstring inherited from DataCoordinateIterable.
        if self._hasRecords is None:
            self._hasRecords = all(dataId.hasRecords() for dataId in self._dataIds)
        return self._hasRecords

    def toSet(self) -> DataCoordinateSet:
        # Docstring inherited from DataCoordinateIterable.
        # Override base class to pass in attributes instead of results of
        # method calls for _hasFull and _hasRecords - those can be None,
        # and hence defer checking if that's what the user originally wanted.
        return DataCoordinateSet(
            frozenset(self._dataIds),
            dimensions=self._dimensions,
            hasFull=self._hasFull,
            hasRecords=self._hasRecords,
            check=False,
        )

    def toSequence(self) -> DataCoordinateSequence:
        # Docstring inherited from DataCoordinateIterable.
        # Override base class to pass in attributes instead of results of
        # method calls for _hasFull and _hasRecords - those can be None,
        # and hence defer checking if that's what the user originally wanted.
        return DataCoordinateSequence(
            tuple(self._dataIds),
            dimensions=self._dimensions,
            hasFull=self._hasFull,
            hasRecords=self._hasRecords,
            check=False,
        )

    def __iter__(self) -> Iterator[DataCoordinate]:
        return iter(self._dataIds)

    def __len__(self) -> int:
        return len(self._dataIds)

    def __contains__(self, key: Any) -> bool:
        key = DataCoordinate.standardize(key, universe=self.universe)
        return key in self._dataIds

    def _subsetKwargs(self, dimensions: DimensionGroup) -> dict[str, Any]:
        """Return constructor kwargs useful for subclasses implementing subset.

        Parameters
        ----------
        dimensions : `DimensionGroup`
            Dimensions passed to `subset`.

        Returns
        -------
        **kwargs
            A dict with `hasFull`, `hasRecords`, and `check` keys, associated
            with the appropriate values for a `subset` operation with the given
            dimensions.
        """
        hasFull: bool | None
        if dimensions.names <= self.dimensions.required:
            hasFull = True
        else:
            hasFull = self._hasFull
        return dict(hasFull=hasFull, hasRecords=self._hasRecords, check=False)


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
    graph : `DimensionGraph`, optional
        Dimensions identified by all data IDs in the collection.  Ignored if
        ``dimensions`` is provided, and deprecated with removal after v27.
    dimensions : `~collections.abc.Iterable` [ `str` ], `DimensionGroup`, \
            or `DimensionGraph`, optional
        Dimensions identified by all data IDs in the collection.  Must be
        provided unless ``graph`` is.
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
    universe : `DimensionUniverse`
        Object that manages all dimension definitions.

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
      same dimensions (i.e. `dimensions` attribute);

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
        dataIds: Set[DataCoordinate],
        graph: DimensionGraph | None = None,
        *,
        dimensions: Iterable[str] | DimensionGroup | DimensionGraph | None = None,
        hasFull: bool | None = None,
        hasRecords: bool | None = None,
        check: bool = True,
        universe: DimensionUniverse | None = None,
    ):
        super().__init__(
            dataIds,
            graph,
            dimensions=dimensions,
            hasFull=hasFull,
            hasRecords=hasRecords,
            check=check,
            universe=universe,
        )

    _dataIds: Set[DataCoordinate]

    __slots__ = ()

    def __str__(self) -> str:
        return str(set(self._dataIds))

    def __repr__(self) -> str:
        return (
            f"DataCoordinateSet({set(self._dataIds)}, {self._dimensions!r}, "
            f"hasFull={self._hasFull}, hasRecords={self._hasRecords})"
        )

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, DataCoordinateSet):
            return self._dimensions == other._dimensions and self._dataIds == other._dataIds
        return False

    def __le__(self, other: DataCoordinateSet) -> bool:
        if self.dimensions != other.dimensions:
            raise ValueError(
                f"Inconsistent dimensions in set comparision: {self.dimensions} != {other.dimensions}."
            )
        return self._dataIds <= other._dataIds

    def __ge__(self, other: DataCoordinateSet) -> bool:
        if self.dimensions != other.dimensions:
            raise ValueError(
                f"Inconsistent dimensions in set comparision: {self.dimensions} != {other.dimensions}."
            )
        return self._dataIds >= other._dataIds

    def __lt__(self, other: DataCoordinateSet) -> bool:
        if self.dimensions != other.dimensions:
            raise ValueError(
                f"Inconsistent dimensions in set comparision: {self.dimensions} != {other.dimensions}."
            )
        return self._dataIds < other._dataIds

    def __gt__(self, other: DataCoordinateSet) -> bool:
        if self.dimensions != other.dimensions:
            raise ValueError(
                f"Inconsistent dimensions in set comparision: {self.dimensions} != {other.dimensions}."
            )
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
        if self.dimensions != other.dimensions:
            raise ValueError(
                f"Inconsistent dimensions in set comparision: {self.dimensions} != {other.dimensions}."
            )
        return self._dataIds <= other.toSet()._dataIds

    def issuperset(self, other: DataCoordinateIterable) -> bool:
        """Test whether ``other`` contains all data IDs in ``self``.

        Parameters
        ----------
        other : `DataCoordinateIterable`
            An iterable of data IDs with
            ``other.dimensions == self.dimensions``.

        Returns
        -------
        issuperset : `bool`
            `True` if all data IDs in ``other`` are also in ``self``, and
            `False` otherwise.
        """
        if self.dimensions != other.dimensions:
            raise ValueError(
                f"Inconsistent dimensions in set comparision: {self.dimensions} != {other.dimensions}."
            )
        return self._dataIds >= other.toSet()._dataIds

    def isdisjoint(self, other: DataCoordinateIterable) -> bool:
        """Test whether there are no data IDs in both ``self`` and ``other``.

        Parameters
        ----------
        other : `DataCoordinateIterable`
            An iterable of data IDs with
            ``other._dimensions == self._dimensions``.

        Returns
        -------
        isdisjoint : `bool`
            `True` if there are no data IDs in both ``self`` and ``other``, and
            `False` otherwise.
        """
        if self._dimensions != other.dimensions:
            raise ValueError(
                f"Inconsistent dimensions in set comparision: {self._dimensions} != {other.dimensions}."
            )
        return self._dataIds.isdisjoint(other.toSet()._dataIds)

    def __and__(self, other: DataCoordinateSet) -> DataCoordinateSet:
        if self._dimensions != other.dimensions:
            raise ValueError(
                f"Inconsistent dimensions in set operation: {self._dimensions} != {other.dimensions}."
            )
        return DataCoordinateSet(self._dataIds & other._dataIds, dimensions=self._dimensions, check=False)

    def __or__(self, other: DataCoordinateSet) -> DataCoordinateSet:
        if self._dimensions != other.dimensions:
            raise ValueError(
                f"Inconsistent dimensions in set operation: {self._dimensions} != {other.dimensions}."
            )
        return DataCoordinateSet(self._dataIds | other._dataIds, dimensions=self._dimensions, check=False)

    def __xor__(self, other: DataCoordinateSet) -> DataCoordinateSet:
        if self._dimensions != other.dimensions:
            raise ValueError(
                f"Inconsistent dimensions in set operation: {self._dimensions} != {other.dimensions}."
            )
        return DataCoordinateSet(self._dataIds ^ other._dataIds, dimensions=self._dimensions, check=False)

    def __sub__(self, other: DataCoordinateSet) -> DataCoordinateSet:
        if self._dimensions != other.dimensions:
            raise ValueError(
                f"Inconsistent dimensions in set operation: {self._dimensions} != {other.dimensions}."
            )
        return DataCoordinateSet(self._dataIds - other._dataIds, dimensions=self._dimensions, check=False)

    def intersection(self, other: DataCoordinateIterable) -> DataCoordinateSet:
        """Return a new set that contains all data IDs from parameters.

        Parameters
        ----------
        other : `DataCoordinateIterable`
            An iterable of data IDs with
            ``other.dimensions == self.dimensions``.

        Returns
        -------
        intersection : `DataCoordinateSet`
            A new `DataCoordinateSet` instance.
        """
        if self.dimensions != other.dimensions:
            raise ValueError(
                f"Inconsistent dimensions in set operation: {self.dimensions} != {other.dimensions}."
            )
        return DataCoordinateSet(
            self._dataIds & other.toSet()._dataIds, dimensions=self.dimensions, check=False
        )

    def union(self, other: DataCoordinateIterable) -> DataCoordinateSet:
        """Return a new set that contains all data IDs in either parameters.

        Parameters
        ----------
        other : `DataCoordinateIterable`
            An iterable of data IDs with
            ``other.dimensions == self.dimensions``.

        Returns
        -------
        intersection : `DataCoordinateSet`
            A new `DataCoordinateSet` instance.
        """
        if self.dimensions != other.dimensions:
            raise ValueError(
                f"Inconsistent dimensions in set operation: {self.dimensions} != {other.dimensions}."
            )
        return DataCoordinateSet(
            self._dataIds | other.toSet()._dataIds, dimensions=self.dimensions, check=False
        )

    def symmetric_difference(self, other: DataCoordinateIterable) -> DataCoordinateSet:
        """Return a new set with all data IDs in either parameters, not both.

        Parameters
        ----------
        other : `DataCoordinateIterable`
            An iterable of data IDs with
            ``other.dimensions == self.dimensions``.

        Returns
        -------
        intersection : `DataCoordinateSet`
            A new `DataCoordinateSet` instance.
        """
        if self.dimensions != other.dimensions:
            raise ValueError(
                f"Inconsistent dimensions in set operation: {self.dimensions} != {other.dimensions}."
            )
        return DataCoordinateSet(
            self._dataIds ^ other.toSet()._dataIds, dimensions=self.dimensions, check=False
        )

    def difference(self, other: DataCoordinateIterable) -> DataCoordinateSet:
        """Return a new set with all data IDs in this that are not in other.

        Parameters
        ----------
        other : `DataCoordinateIterable`
            An iterable of data IDs with
            ``other.dimensions == self.dimensions``.

        Returns
        -------
        intersection : `DataCoordinateSet`
            A new `DataCoordinateSet` instance.
        """
        if self.dimensions != other.dimensions:
            raise ValueError(
                f"Inconsistent dimensions in set operation: {self.dimensions} != {other.dimensions}."
            )
        return DataCoordinateSet(
            self._dataIds - other.toSet()._dataIds, dimensions=self.dimensions, check=False
        )

    def toSet(self) -> DataCoordinateSet:
        # Docstring inherited from DataCoordinateIterable.
        return self

    def subset(self, dimensions: DimensionGraph | DimensionGroup | Iterable[str]) -> DataCoordinateSet:
        """Return a set whose data IDs identify a subset.

        Parameters
        ----------
        dimensions : `DimensionGraph`, `DimensionGroup`, or \
                `~collections.abc.Iterable` [ `str` ]
            Dimensions to be identified by the data IDs in the returned
            iterable.  Must be a subset of ``self.dimensions``.

        Returns
        -------
        set : `DataCoordinateSet`
            A `DataCoordinateSet` with ``set.dimensions == dimensions``. Will
            be ``self`` if ``dimensions == self.dimensions``.  Elements are
            equivalent to those that would be created by calling
            `DataCoordinate.subset` on all elements in ``self``, with
            deduplication and in arbitrary order.
        """
        dimensions = self.universe.conform(dimensions)
        if dimensions == self.dimensions:
            return self
        return DataCoordinateSet(
            {dataId.subset(dimensions) for dataId in self._dataIds},
            dimensions=dimensions,
            **self._subsetKwargs(dimensions),
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
    graph : `DimensionGraph`, optional
        Dimensions identified by all data IDs in the collection.  Ignored if
        ``dimensions`` is provided, and deprecated with removal after v27.
    dimensions : `~collections.abc.Iterable` [ `str` ], `DimensionGroup`, \
            `DimensionGraph`, optional
        Dimensions identified by all data IDs in the collection.  Must be
        provided unless ``graph`` is.
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
    universe : `DimensionUniverse`
        Object that manages all dimension definitions.
    """

    def __init__(
        self,
        dataIds: Sequence[DataCoordinate],
        graph: DimensionGraph | None = None,
        *,
        dimensions: Iterable[str] | DimensionGroup | DimensionGraph | None = None,
        hasFull: bool | None = None,
        hasRecords: bool | None = None,
        check: bool = True,
        universe: DimensionUniverse | None = None,
    ):
        super().__init__(
            tuple(dataIds),
            graph,
            dimensions=dimensions,
            hasFull=hasFull,
            hasRecords=hasRecords,
            check=check,
            universe=universe,
        )

    _dataIds: Sequence[DataCoordinate]

    __slots__ = ()

    def __str__(self) -> str:
        return str(tuple(self._dataIds))

    def __repr__(self) -> str:
        return (
            f"DataCoordinateSequence({tuple(self._dataIds)}, {self._dimensions!r}, "
            f"hasFull={self._hasFull}, hasRecords={self._hasRecords})"
        )

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, DataCoordinateSequence):
            return self._dimensions == other._dimensions and self._dataIds == other._dataIds
        return False

    @overload
    def __getitem__(self, index: int) -> DataCoordinate:
        pass

    @overload
    def __getitem__(self, index: slice) -> DataCoordinateSequence:
        pass

    def __getitem__(self, index: Any) -> Any:
        r = self._dataIds[index]
        if isinstance(index, slice):
            return DataCoordinateSequence(
                r,
                dimensions=self._dimensions,
                hasFull=self._hasFull,
                hasRecords=self._hasRecords,
                check=False,
            )
        return r

    def toSequence(self) -> DataCoordinateSequence:
        # Docstring inherited from DataCoordinateIterable.
        return self

    def subset(self, dimensions: DimensionGraph | DimensionGroup | Iterable[str]) -> DataCoordinateSequence:
        """Return a sequence whose data IDs identify a subset.

        Parameters
        ----------
        dimensions : `DimensionGraph`, `DimensionGroup`, \
                or `~collections.abc.Iterable` [ `str` ]
            Dimensions to be identified by the data IDs in the returned
            iterable.  Must be a subset of ``self.dimensions``.

        Returns
        -------
        set : `DataCoordinateSequence`
            A `DataCoordinateSequence` with ``set.graph == graph``.
            Will be ``self`` if ``graph == self.graph``.  Elements are
            equivalent to those that would be created by calling
            `DataCoordinate.subset` on all elements in ``self``, in the same
            order and with no deduplication.
        """
        dimensions = self.universe.conform(dimensions)
        if dimensions == self.dimensions:
            return self
        return DataCoordinateSequence(
            tuple(dataId.subset(dimensions) for dataId in self._dataIds),
            dimensions=dimensions,
            **self._subsetKwargs(dimensions),
        )

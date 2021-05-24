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
    "DataCoordinateCommonState",
)

from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Callable, Iterable, Iterator, Optional, List, TypedDict

import sqlalchemy

from ...dimensions import DataCoordinate, DataId, DimensionGraph, DimensionUniverse
from ...simpleQuery import SimpleQuery

if TYPE_CHECKING:
    from ._abstract_set import DataCoordinateAbstractSet
    from ._sequence import DataCoordinateSequence


class DataCoordinateIterable(Iterable[DataCoordinate]):
    """An abstract base class for homogeneous iterables of data IDs.

    All elements of a `DataCoordinateIterable` identify the same set of
    dimensions (given by the `graph` property) and generally have the same
    `DataCoordinate.hasFull` and `DataCoordinate.hasRecords` flag values.
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
    ) -> DataCoordinateIterable:
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
        standardized : `DataCoordinateIterable` subclass instance
            A `DataCoordinateIterable` with ``subset.graph == graph``.
            May be ``data_ids`` if it is already a `DataCoordinateIterable`
            for immutable classes only if ``graph == self.graph``.  Elements
            are equivalent to those that would be created by calling
            `DataCoordinate.standardize` on all elements in ``self``, possibly
            with deduplication and/or reordering (depending on the subclass,
            which may make more specific guarantees).
        """
        from ._iterator_adaptor import DataCoordinateIteratorAdapter

        def gen() -> Iterator[DataCoordinate]:
            for data_id in data_ids:
                yield DataCoordinate.standardize(data_id, graph=graph, defaults=defaults, **kwargs)

        return DataCoordinateIteratorAdapter(gen, graph=graph)

    @property
    def graph(self) -> DimensionGraph:
        """Dimensions identified by these data IDs (`DimensionGraph`)."""
        return self._common_state.graph

    @property
    def universe(self) -> DimensionUniverse:
        """Universe that defines all known compatible dimensions
        (`DimensionUniverse`).
        """
        return self.graph.universe

    def _unwrap(self) -> Iterable[DataCoordinate]:
        """Return a Python built-in iterable with the same elements as self, if
        possible.

        Returns
        -------
        unwrapped : `Iterable` [ `DataCoordinate` ]
            A Python built-in, if possible, or ``self``, if not.

        Notes
        -----
        This method is conceptually "protected"; it should be called (as well
        as frequently reimplemented) by subclasses, but not other code.  The
        returned object may be internal mutable state (to avoid unnecessary
        copies), but should never actually be modified by the caller.  This
        can be guarded against by always annotating the return type with an
        ABC that has no mutators (e.g. `typing.Sequence` instead of
        `typing.List`).

        Subclasses that define interfaces that mirror specific Python native
        containers should redefine this method with a more specific return
        type, and may make it abstract as well.

        The default implementation simply returns `self`.
        """
        return self

    @property
    @abstractmethod
    def _common_state(self) -> DataCoordinateCommonState:
        """State common to all data IDs (`DataCoordinateCommonState`)."""
        raise NotImplementedError()

    @abstractmethod
    def hasFull(self) -> bool:
        """Indicate if all data IDs in this iterable identify all dimensions,
        not just required dimensions.

        Returns
        -------
        state : `bool`
            If `True`, ``all(d.hasFull() for d in iterable)`` is guaranteed.
            If `False`, no guarantees are made.

        Notes
        -----
        This method does not consume iterables backed by a single pass
        iterator.
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

        Notes
        -----
        This method does not consume iterables backed by a single pass
        iterator.
        """
        raise NotImplementedError()

    def toSet(self) -> DataCoordinateAbstractSet:
        """Transform this iterable into a `DataCoordinateAbstractSet`.

        Returns
        -------
        set : `DataCoordinateAbstractSet`
            A `DataCoordinateAbstractSet` instance with the same elements as
            ``self``, after removing any duplicates.  May be ``self`` if it is
            already a `DataCoordinateAbstractSet`.

        Notes
        -----
        This method may consume iterables backed by a single pass iterator.
        """
        from ._frozen_set import DataCoordinateFrozenSet

        return DataCoordinateFrozenSet(self._unwrap(), check=False, **self._common_state.to_dict())

    def toSequence(self) -> DataCoordinateSequence:
        """Transform this iterable into a `DataCoordinateSequence`.

        Returns
        -------
        seq : `DataCoordinateSequence`
            A new `DatasetCoordinateSequence` with the same elements as
            ``self``, in the same order.  May be ``self`` if it is already a
            `DataCoordinateSequence`.

        Notes
        -----
        This method may consume iterables backed by a single pass iterator.
        """
        from ._tuple import DataCoordinateTuple

        return DataCoordinateTuple(self._unwrap(), check=False, **self._common_state.to_dict())

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

        Notes
        -----
        This method may consume iterables backed by a single pass iterator.
        """
        toOrTogether: List[sqlalchemy.sql.ColumnElement] = []
        for dataId in self:
            toOrTogether.append(
                sqlalchemy.sql.and_(
                    *[columns(dimension.name) == dataId[dimension.name] for dimension in self.graph.required]
                )
            )
        query.where.append(sqlalchemy.sql.or_(*toOrTogether))

    @abstractmethod
    def subset(self, graph: DimensionGraph) -> DataCoordinateIterable:
        """Return a subset iterable.

        This subset iterable contains data IDs that identify a subset of the
        dimensions that this one's do.

        Parameters
        ----------
        graph : `DimensionGraph`
            Dimensions to be identified by the data IDs in the returned
            iterable.  Must be a subset of ``self.graph``.

        Returns
        -------
        subset : `DataCoordinateIterable`
            A `DataCoordinateIterable` with ``subset.graph == graph``.
            May be ``self`` for immutable classes only if
            ``graph == self.graph``.  Elements are equivalent to those that
            would be created by calling `DataCoordinate.subset` on all elements
            in ``self``, possibly with deduplication and/or reordering
            (depending on the subclass, which may make more specific
            guarantees).

        Notes
        -----
        This method may consume iterables backed by a single pass iterator.
        """
        raise NotImplementedError()


class DataCoordinateCommonStateDict(TypedDict):
    """Type annotation for the dict form of `DataCoordinateCommonState`.

    This class does not exist at runtime (it's just a `dict`); it only exists
    to provide static type-checking.
    """

    graph: DimensionGraph
    hasFull: Optional[bool]
    hasRecords: Optional[bool]


class DataCoordinateCommonState:
    """Struct that can provide manangement of state common to all data IDs in
    a container.

    Parameters
    ----------
    graph : `DimensionGraph`
        Dimensions that must be common to all data IDs.
    hasFull : `bool`, optional
        `True` if all data IDs *must* satisfy `DataCoordinate.hasFull` to be
        members of this container; `False` if this cannot be guaranteed, and
        `None` if it is unknown and should be obtained by looking at the data
        IDs themselves.
    hasRecords : `bool`, optional
        Like ``hasFull``, but for `DataCoordinate.hasRecords`.

    Notes
    -----
    `DataCoordinateIterable` implementations do not need to use this class to
    hold their internal state, but it is expected that most will.  The
    functionality its methods provide is not included directly in the base
    class in order to keep it and other ABCs state-free ("use inheritance
    for shared interfaces and composition for shared implementation").
    """

    def __init__(
        self, graph: DimensionGraph, hasFull: Optional[bool] = None, hasRecords: Optional[bool] = None
    ):
        self.graph = graph
        self.hasFull = hasFull
        self.hasRecords = hasRecords
        if not self.graph.implied:
            self.hasFull = True

    __slots__ = ("graph", "hasFull", "hasRecords")

    def to_dict(self) -> DataCoordinateCommonStateDict:
        """Return a dictionary with construction keyword arguments.

        Parameters
        ----------
        dict : `dict`
            Key-value pairs that are identical to construction parameters.
        """
        return dict(graph=self.graph, hasFull=self.hasFull, hasRecords=self.hasRecords)

    def check(self, data_ids: Iterable[DataCoordinate]) -> Iterator[DataCoordinate]:
        """Check all data IDs for consistency with `graph` and saved `hasFull`
        and `hasRecords` values.

        Parameters
        ----------
        data_ids : `Iterable` [ `DataCoordinate` ]
            Data IDs to check.

        Raises
        ------
        ValueError
            Raised if any data ID's dimensions are not equal to `graph`, or
            if internal `hasFull` or `hasRecords` values are `True` but a data
            ID does not satisfy this requirement.

        Returns
        -------
        data_ids : `Iterator` [ `DataCoordinate` ]
            The same data IDs that were passed in.
        """
        for data_id in data_ids:
            if data_id.graph != self.graph:
                raise ValueError(f"Bad dimensions in data ID {data_id}; expected {self.graph}.")
            if self.hasFull and not data_id.hasFull():
                raise ValueError(
                    f"Container expects data IDs to satisfy hasFull() == True, but {data_id} does not."
                )
            if self.hasRecords and not data_id.hasRecords():
                raise ValueError(
                    f"Container expects data IDs to satisfy hasRecords() == True, but {data_id} does not."
                )
            yield data_id

    def computeHasFull(self, data_ids: Iterable[DataCoordinate], *, cache: bool = False) -> bool:
        """Return a `bool` for `hasFull`, computing it from data IDs if
        necessary.

        Parameters
        ----------
        data_ids : `Iterable` [ `DataCoordinate` ]
            Data IDs to check, if ``self.hasFull is None``.
        cache : `bool`, optional
            If `True` (`False` is default), set ``self.hasFull`` to the
            computed value in addition to returning it.

        Returns
        -------
        hasFull : `bool`
            `True` if ``self.hasFull is True``, `False` if
            ``self.hasFull is False``, and whether all data IDs in ``data_ids``
            satisfy `DataCoordinate.hasFull`.
        """
        if self.hasFull is not None:
            return self.hasFull
        result = all(data_id.hasFull() for data_id in data_ids)
        if cache:
            self.hasFull = result
        return result

    def computeHasRecords(self, data_ids: Iterable[DataCoordinate], *, cache: bool = False) -> bool:
        """Return a `bool` for `hasRecords`, computing it from data IDs if
        necessary.

        Parameters
        ----------
        data_ids : `Iterable` [ `DataCoordinate` ]
            Data IDs to check, if ``self.hasRecords is None``.
        cache : `bool`, optional
            If `True` (`False` is default), set ``self.hasRecords`` to the
            computed value in addition to returning it.

        Returns
        -------
        hasRecords : `bool`
            `True` if ``self.hasRecords is True``, `False` if
            ``self.hasRecords is False``, and whether all data IDs in
            ``data_ids`` satisfy `DataCoordinate.hasRecords`.
        """
        if self.hasRecords is not None:
            return self.hasRecords
        result = all(data_id.hasRecords() for data_id in data_ids)
        if cache:
            self.hasRecords = result
        return result

    def subset(self, graph: DimensionGraph) -> DataCoordinateCommonState:
        """Return the common state for the result of a call to
        `DataCoordinateIterable.subset`.

        Parameters
        ----------
        graph : `DimensionGraph`
            New dimensions for data IDs.

        Returns
        -------
        result : `DataCoordinateCommonState`
            Common state for a container of new data IDs.
        """
        if graph == self.graph:
            return DataCoordinateCommonState(**self.to_dict())
        hasFull: Optional[bool] = None
        if self.hasFull or graph.dimensions.names <= self.graph.required.names:
            hasFull = True
        return DataCoordinateCommonState(graph, hasFull=hasFull, hasRecords=self.hasRecords)

    def check_mix_with(self, other: DataCoordinateCommonState) -> None:
        """Test whether the container associated with this state can be
        paired with one with the given state in binary operations.

        Parameters
        ----------
        other : `DataCoordinateCommonState`
            State for the other container in the binary operation.
        """
        if self.graph != other.graph:
            raise ValueError(
                f"Inconsistent dimensions between data ID iterables: {self.graph} != {other.graph}."
            )

    def mixed_with(self, other: DataCoordinateCommonState) -> DataCoordinateCommonState:
        """Return common state for a new container produced in a binary
        operation.

        Parameters
        ----------
        other : `DataCoordinateCommonState`
            State for the other container in the binary operation.

        Returns
        -------
        result : `DataCoordinateCommonState`
            State for the result of the container; computed while assuming its
            constituent data IDs could come from either operand.
        """
        self.check_mix_with(other)
        return DataCoordinateCommonState(
            graph=self.graph,
            hasFull=True if self.hasFull and other.hasFull else None,
            hasRecords=True if self.hasRecords and other.hasRecords else None,
        )

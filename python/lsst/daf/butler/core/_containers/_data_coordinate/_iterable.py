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

__all__ = ("DataCoordinateIterable",)

from abc import abstractmethod
from typing import Callable, Iterable, List, TYPE_CHECKING

import sqlalchemy

from ...simpleQuery import SimpleQuery
from ...dimensions import DataCoordinate, DimensionGraph, DimensionUniverse

if TYPE_CHECKING:
    from ._scalar import _ScalarDataCoordinateIterable
    from ._set import DataCoordinateSet
    from ._sequence import DataCoordinateSequence


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
        from ._scalar import _ScalarDataCoordinateIterable

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
        from ._set import DataCoordinateSet

        return DataCoordinateSet(
            frozenset(self),
            graph=self.graph,
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
        from ._sequence import DataCoordinateSequence

        return DataCoordinateSequence(
            tuple(self), graph=self.graph, hasFull=self.hasFull(), hasRecords=self.hasRecords(), check=False
        )

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
                sqlalchemy.sql.and_(
                    *[columns(dimension.name) == dataId[dimension.name] for dimension in self.graph.required]
                )
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
            with deduplication and/or reordering (depending on the subclass,
            which may make more specific guarantees).
        """
        raise NotImplementedError()

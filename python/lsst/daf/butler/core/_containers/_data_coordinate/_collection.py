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

__all__ = ("DataCoordinateCollection",)

from abc import abstractmethod
from typing import Any, Collection, Iterable, Iterator, Optional

from ...dimensions import DataCoordinate, DataId, DimensionGraph
from ._iterable import DataCoordinateIterable


class DataCoordinateCollection(Collection[DataCoordinate], DataCoordinateIterable):
    """An abstract base class for homogeneous containers of data IDs."""

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
        standardized : `DataCoordinateCollection` subclass instance
            A `DataCoordinateCollection` with ``subset.graph == graph``.
            May be ``data_ids`` if it is already a `DataCoordinateCollection`
            for immutable classes only if ``graph == self.graph``.  Elements
            are equivalent to those that would be created by calling
            `DataCoordinate.standardize` on all elements in ``self``, possibly
            with deduplication and/or reordering (depending on the subclass,
            which may make more specific guarantees).
        """
        return super().standardize(data_ids, graph, default=defaults, **kwargs).toSequence()

    def __iter__(self) -> Iterator[DataCoordinate]:
        return iter(self._unwrap())

    def __len__(self) -> int:
        return len(self._unwrap())

    def __contains__(self, key: Any) -> bool:
        key = DataCoordinate.standardize(key, universe=self.universe)
        return key in self._unwrap()

    def _unwrap(self) -> Collection[DataCoordinate]:
        # Docstring inherited.
        return self

    def hasFull(self) -> bool:
        # Docstring inherited.
        return self._common_state.computeHasFull(self._unwrap())

    def hasRecords(self) -> bool:
        # Docstring inherited.
        return self._common_state.computeHasRecords(self._unwrap())

    @abstractmethod
    def subset(self, graph: DimensionGraph) -> DataCoordinateCollection:
        """Return a subset collection.

        This subset collection contains data IDs that identify a subset of the
        dimensions that this one's do.

        Parameters
        ----------
        graph : `DimensionGraph`
            Dimensions to be identified by the data IDs in the returned
            collection.  Must be a subset of ``self.graph``.

        Returns
        -------
        subset : `DataCoordinateCollection`
            A `DataCoordinateCollection` with ``subset.graph == graph``.
            May be ``self`` for immutable classes only if
            ``graph == self.graph``.  Elements are equivalent to those that
            would be created by calling `DataCoordinate.subset` on all elements
            in ``self``, possibly with deduplication and/or reordering
            (depending on the subclass, which may make more specific
            guarantees).
        """
        raise NotImplementedError()

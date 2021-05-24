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

__all__ = ("DataCoordinateSequence",)

from abc import abstractmethod
from typing import Any, Iterable, Optional, Sequence, overload

from ...dimensions import DataCoordinate, DataId, DimensionGraph
from ._collection import DataCoordinateCollection
from ._iterable import DataCoordinateCommonState


class DataCoordinateSequence(DataCoordinateCollection, Sequence[DataCoordinate]):
    """An abstract base class for homogeneous sequence-like containers of data
    IDs.
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
    ) -> DataCoordinateSequence:
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
        standardized : `Sequence` subclass instance
            A `Sequence` with ``subset.graph == graph``.
            May be ``data_ids`` if it is already a `Sequence`
            for immutable classes only if ``graph == self.graph``.  Elements
            are equivalent to those that would be created by calling
            `DataCoordinate.standardize` on all elements in ``self``, with
            with no reordering but no deduplication.
        """
        return super().standardize(data_ids, graph, default=defaults, **kwargs).toSequence()

    @classmethod
    @abstractmethod
    def _wrap(
        cls, native: Sequence[DataCoordinate],
        common: DataCoordinateCommonState
    ) -> DataCoordinateSequence:
        """Return a new `DataCoordinateSequence` subclass instance that
        wraps the given native sequence.

        Parameters
        ----------
        native : `Sequence` [ `DataCoordinate` ]
            Built-in sequence to wrap.
        common : `DataCoordinateCommonState`
            Structure containing the `DimensionGraph` and the possibly-known
            values for `hasFull` and `hasRecords` *at construction*.  Note
            that these values may not be guaranteed to remain true for
            mutable return types.

        Returns
        -------
        wrapped : `DataCoordinateSequence`
            Wrapped set.
        """
        raise NotImplementedError()

    @abstractmethod
    def _unwrap(self) -> Sequence[DataCoordinate]:
        # Docstring inherited.
        raise NotImplementedError()

    def toSequence(self) -> DataCoordinateSequence:
        # Docstring inherited from DataCoordinateIterable.
        return self

    def subset(self, graph: DimensionGraph) -> DataCoordinateSequence:
        """Return a subset sequence.

        This subset sequence contains data IDs that identify a subset of the
        dimensions that this one's do.

        Parameters
        ----------
        graph : `DimensionGraph`
            Dimensions to be identified by the data IDs in the returned
            sequence.  Must be a subset of ``self.graph``.

        Returns
        -------
        subset : `DataCoordinateSequence`
            A `DataCoordinateSequence` with ``subset.graph == graph``.
            May be ``self`` for immutable classes only if
            ``graph == self.graph``.  Elements are equivalent to those that
            would be created by calling `DataCoordinate.subset` on all elements
            in ``self``, with no deduplication and in the same order.
        """
        common = self._common_state.subset(graph)
        return self._wrap(tuple(data_id.subset(graph) for data_id in self._unwrap()), common)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, DataCoordinateSequence):
            return self.graph == other.graph and self._unwrap() == other._unwrap()
        return False

    @overload
    def __getitem__(self, index: int) -> DataCoordinate:
        pass

    @overload
    def __getitem__(self, index: slice) -> DataCoordinateSequence:  # noqa: F811
        pass

    def __getitem__(self, index: Any) -> Any:  # noqa: F811
        r = self._unwrap()[index]
        if isinstance(index, slice):
            return self._wrap(r, self._common_state)
        return r

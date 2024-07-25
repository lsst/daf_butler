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

__all__ = ("DimensionPacker",)

from abc import ABCMeta, abstractmethod
from typing import TYPE_CHECKING, Any

from ._coordinate import DataCoordinate, DataId
from ._group import DimensionGroup

if TYPE_CHECKING:  # Imports needed only for type annotations; may be circular.
    from ._universe import DimensionUniverse


class DimensionPacker(metaclass=ABCMeta):
    """Class for going from `DataCoordinate` to packed integer ID and back.

    An abstract base class for bidirectional mappings between a
    `DataCoordinate` and a packed integer ID.

    Parameters
    ----------
    fixed : `DataCoordinate`
        Expanded data ID for the dimensions whose values must remain fixed
        (to these values) in all calls to `pack`, and are used in the results
        of calls to `unpack`.  Subclasses may ignore particular dimensions, and
        are permitted to require that ``fixed.hasRecords()`` return `True`.
    dimensions : `DimensionGroup`
        The dimensions of data IDs packed by this instance.
    """

    def __init__(self, fixed: DataCoordinate, dimensions: DimensionGroup):
        self.fixed = fixed
        self._dimensions = self.fixed.universe.conform(dimensions)

    @property
    def universe(self) -> DimensionUniverse:
        """Graph containing all known dimensions (`DimensionUniverse`)."""
        return self.fixed.universe

    @property
    def dimensions(self) -> DimensionGroup:
        """The dimensions of data IDs packed by this instance
        (`DimensionGroup`).
        """
        return self._dimensions

    @property
    @abstractmethod
    def maxBits(self) -> int:
        """Return The maximum number of nonzero bits in the packed ID.

        This packed ID will be returned by
        `~DimensionPacker.pack` (`int`).

        Must be implemented by all concrete derived classes.  May return
        `None` to indicate that there is no maximum.
        """
        raise NotImplementedError()

    @abstractmethod
    def _pack(self, dataId: DataCoordinate) -> int:
        """Abstract implementation for `~DimensionPacker.pack`.

        Must be implemented by all concrete derived classes.

        Parameters
        ----------
        dataId : `DataCoordinate`
            Dictionary-like object identifying (at least) all packed
            dimensions associated with this packer.  Guaranteed to be a true
            `DataCoordinate`, not an informal data ID

        Returns
        -------
        packed : `int`
            Packed integer ID.
        """
        raise NotImplementedError()

    def pack(
        self, dataId: DataId | None = None, *, returnMaxBits: bool = False, **kwargs: Any
    ) -> tuple[int, int] | int:
        """Pack the given data ID into a single integer.

        Parameters
        ----------
        dataId : `DataId`
            Data ID to pack.  Values for any keys also present in the "fixed"
            data ID passed at construction must be the same as the values
            passed at construction, but in general you must still specify
            those keys.
        returnMaxBits : `bool`
            If `True`, return a tuple of ``(packed, self.maxBits)``.
        **kwargs
            Additional keyword arguments are treated like additional key-value
            pairs in ``dataId``.

        Returns
        -------
        packed : `int`
            Packed integer ID.
        maxBits : `int`, optional
            Maximum number of nonzero bits in ``packed``.  Not returned unless
            ``returnMaxBits`` is `True`.

        Notes
        -----
        Should not be overridden by derived class
        (`~DimensionPacker._pack` should be overridden instead).
        """
        dataId = DataCoordinate.standardize(
            dataId, **kwargs, universe=self.fixed.universe, defaults=self.fixed
        )
        if dataId.subset(self.fixed.dimensions) != self.fixed:
            raise ValueError(f"Data ID packer expected a data ID consistent with {self.fixed}, got {dataId}.")
        packed = self._pack(dataId)
        if returnMaxBits:
            return packed, self.maxBits
        else:
            return packed

    @abstractmethod
    def unpack(self, packedId: int) -> DataCoordinate:
        """Unpack an ID produced by `pack` into a full `DataCoordinate`.

        Must be implemented by all concrete derived classes.

        Parameters
        ----------
        packedId : `int`
            The result of a call to `~DimensionPacker.pack` on either
            ``self`` or an identically-constructed packer instance.

        Returns
        -------
        dataId : `DataCoordinate`
            Dictionary-like ID that uniquely identifies all covered
            dimensions.
        """
        raise NotImplementedError()

    # Class attributes below are shadowed by instance attributes, and are
    # present just to hold the docstrings for those instance attributes.

    fixed: DataCoordinate
    """The dimensions provided to the packer at construction
    (`DataCoordinate`)

    The packed ID values are only unique and reversible with these
    dimensions held fixed.
    """

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


__all__ = ("DimensionPacker",)

from abc import ABCMeta, abstractmethod

from lsst.utils import doImport

from ..config import Config
from .graph import DimensionGraph
from .universe import DimensionUniverse
from .coordinate import DataCoordinate, DataId
from .records import ExpandedDataCoordinate


class DimensionPacker(metaclass=ABCMeta):
    """An abstract base class for bidirectional mappings between a
    `DataCoordinate` and a packed integer ID.

    Parameters
    ----------
    fixed : `ExpandedDataCoordinate`
        Expanded data ID for the dimensions whose values must remain fixed
        (to these values) in all calls to `pack`, and are used in the results
        of calls to `unpack`.
    dimensions : `DimensionGraph`
        The dimensions of data IDs packed by this instance.
    """

    def __init__(self, fixed: ExpandedDataCoordinate, dimensions: DimensionGraph):
        self.fixed = fixed
        self.dimensions = dimensions

    @property
    def universe(self) -> DimensionUniverse:
        """A graph containing all known dimensions (`DimensionUniverse`).
        """
        return self.fixed.universe

    @property
    @abstractmethod
    def maxBits(self) -> int:
        """The maximum number of nonzero bits in the packed ID returned by
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

    def pack(self, dataId: DataId, *, returnMaxBits: bool = False, **kwds) -> int:
        """Pack the given data ID into a single integer.

        Parameters
        ----------
        dataId : `DataId`
            Data ID to pack.  Values for any keys also present in the "fixed"
            data ID passed at construction must be the same as the values
            passed at construction.
        returnMaxBits : `bool`
            If `True`, return a tuple of ``(packed, self.maxBits)``.
        kwds
            Additional keyword arguments forwarded to
            `DataCoordinate.standardize`.

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
        dataId = DataCoordinate.standardize(dataId, **kwds)
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

    fixed: ExpandedDataCoordinate
    """The dimensions provided to to the packer at construction
    (`ExpandedDataCoordinate`)

    The packed ID values are only unique and reversible with these
    dimensions held fixed.
    """


class DimensionPackerFactory:
    """A factory class for `DimensionPacker` instances that can be constructed
    from configuration.

    This class is primarily intended for internal use by `DimensionUniverse`.
    """

    def __init__(self, fixed: DimensionGraph, dimensions: DimensionGraph, clsName: str):
        self.fixed = fixed
        self.dimensions = dimensions
        self._clsName = clsName
        self._cls = None

    @classmethod
    def fromConfig(cls, universe: DimensionUniverse, config: Config):
        """Construct a `DimensionPackerFactory` from a piece of dimension
        configuration.

        Parameters
        ----------
        universe : `DimensionGraph`
            All dimension objects known to the `Registry`.
        config : `Config`
            A dict-like `Config` node corresponding to a single entry
            in the ``packers`` section of a `DimensionConfig`.
        """
        fixed = DimensionGraph(universe=universe, names=config["fixed"])
        dimensions = DimensionGraph(universe=universe, names=config["dimensions"])
        clsName = config["cls"]
        return cls(fixed=fixed, dimensions=dimensions, clsName=clsName)

    def __call__(self, fixed: ExpandedDataCoordinate) -> DimensionPacker:
        """Construct a `DimensionPacker` instance for the given fixed data ID.

        Parameters
        ----------
        fixed : `ExpandedDataCoordinate`
            Data ID that provides values for the "fixed" dimensions of the
            packer.  Must be expanded with all metadata known to the
            `Registry`.
        """
        assert fixed.graph.issuperset(self.fixed)
        if self._cls is None:
            self._cls = doImport(self._clsName)
        return self._cls(fixed, self.dimensions)

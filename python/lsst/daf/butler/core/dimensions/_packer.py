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
from typing import TYPE_CHECKING, AbstractSet, Any, Iterable, Optional, Tuple, Type, Union

from lsst.utils import doImportType

from ._coordinate import DataCoordinate, DataId
from ._graph import DimensionGraph
from .construction import DimensionConstructionBuilder, DimensionConstructionVisitor

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
        of calls to `unpack`.  ``fixed.hasRecords()`` must return `True`.
    dimensions : `DimensionGraph`
        The dimensions of data IDs packed by this instance.
    """

    def __init__(self, fixed: DataCoordinate, dimensions: DimensionGraph):
        self.fixed = fixed
        self.dimensions = dimensions

    @property
    def universe(self) -> DimensionUniverse:
        """Graph containing all known dimensions (`DimensionUniverse`)."""
        return self.fixed.universe

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
        self, dataId: DataId, *, returnMaxBits: bool = False, **kwargs: Any
    ) -> Union[Tuple[int, int], int]:
        """Pack the given data ID into a single integer.

        Parameters
        ----------
        dataId : `DataId`
            Data ID to pack.  Values for any keys also present in the "fixed"
            data ID passed at construction must be the same as the values
            passed at construction.
        returnMaxBits : `bool`
            If `True`, return a tuple of ``(packed, self.maxBits)``.
        **kwargs
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
        dataId = DataCoordinate.standardize(dataId, **kwargs)
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
    dimensions held fixed.  ``fixed.hasRecords() is True`` is guaranteed.
    """

    dimensions: DimensionGraph
    """The dimensions of data IDs packed by this instance (`DimensionGraph`).
    """


class DimensionPackerFactory:
    """A factory class for `DimensionPacker` instances.

    Can be constructed from configuration.

    This class is primarily intended for internal use by `DimensionUniverse`.

    Parameters
    ----------
    clsName : `str`
        Fully-qualified name of the packer class this factory constructs.
    fixed : `AbstractSet` [ `str` ]
        Names of dimensions whose values must be provided to the packer when it
        is constructed.  This will be expanded lazily into a `DimensionGraph`
        prior to `DimensionPacker` construction.
    dimensions : `AbstractSet` [ `str` ]
        Names of dimensions whose values are passed to `DimensionPacker.pack`.
        This will be expanded lazily into a `DimensionGraph` prior to
        `DimensionPacker` construction.
    """

    def __init__(
        self,
        clsName: str,
        fixed: AbstractSet[str],
        dimensions: AbstractSet[str],
    ):
        # We defer turning these into DimensionGraph objects until first use
        # because __init__ is called before a DimensionUniverse exists, and
        # DimensionGraph instances can only be constructed afterwards.
        self._fixed: Union[AbstractSet[str], DimensionGraph] = fixed
        self._dimensions: Union[AbstractSet[str], DimensionGraph] = dimensions
        self._clsName = clsName
        self._cls: Optional[Type[DimensionPacker]] = None

    def __call__(self, universe: DimensionUniverse, fixed: DataCoordinate) -> DimensionPacker:
        """Construct a `DimensionPacker` instance for the given fixed data ID.

        Parameters
        ----------
        fixed : `DataCoordinate`
            Data ID that provides values for the "fixed" dimensions of the
            packer.  Must be expanded with all metadata known to the
            `Registry`.  ``fixed.hasRecords()`` must return `True`.
        """
        # Construct DimensionGraph instances if necessary on first use.
        # See related comment in __init__.
        if not isinstance(self._fixed, DimensionGraph):
            self._fixed = universe.extract(self._fixed)
        if not isinstance(self._dimensions, DimensionGraph):
            self._dimensions = universe.extract(self._dimensions)
        assert fixed.graph.issuperset(self._fixed)
        if self._cls is None:
            packer_class = doImportType(self._clsName)
            assert not isinstance(
                packer_class, DimensionPacker
            ), f"Packer class {self._clsName} must be a DimensionPacker."
            self._cls = packer_class
        return self._cls(fixed, self._dimensions)


class DimensionPackerConstructionVisitor(DimensionConstructionVisitor):
    """Builder visitor for a single `DimensionPacker`.

    A single `DimensionPackerConstructionVisitor` should be added to a
    `DimensionConstructionBuilder` for each `DimensionPackerFactory` that
    should be added to a universe.

    Parameters
    ----------
    name : `str`
        Name used to identify this configuration of the packer in a
        `DimensionUniverse`.
    clsName : `str`
        Fully-qualified name of a `DimensionPacker` subclass.
    fixed : `Iterable` [ `str` ]
        Names of dimensions whose values must be provided to the packer when it
        is constructed.  This will be expanded lazily into a `DimensionGraph`
        prior to `DimensionPacker` construction.
    dimensions : `Iterable` [ `str` ]
        Names of dimensions whose values are passed to `DimensionPacker.pack`.
        This will be expanded lazily into a `DimensionGraph` prior to
        `DimensionPacker` construction.
    """

    def __init__(self, name: str, clsName: str, fixed: Iterable[str], dimensions: Iterable[str]):
        super().__init__(name)
        self._fixed = set(fixed)
        self._dimensions = set(dimensions)
        self._clsName = clsName

    def hasDependenciesIn(self, others: AbstractSet[str]) -> bool:
        # Docstring inherited from DimensionConstructionVisitor.
        return False

    def visit(self, builder: DimensionConstructionBuilder) -> None:
        # Docstring inherited from DimensionConstructionVisitor.
        builder.packers[self.name] = DimensionPackerFactory(
            clsName=self._clsName,
            fixed=self._fixed,
            dimensions=self._dimensions,
        )

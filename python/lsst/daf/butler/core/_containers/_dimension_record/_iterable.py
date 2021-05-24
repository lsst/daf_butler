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

__all__ = ("HomogeneousDimensionRecordIterable", "HeterogeneousDimensionRecordIterable")

from abc import abstractmethod
from typing import Iterable

from ...dimensions import DimensionElement, DimensionRecord, DimensionUniverse


class HeterogeneousDimensionRecordIterable(Iterable[DimensionRecord]):
    """An abstract base class for heterogeneous iterables of dimension
    records.
    """

    __slots__ = ()

    @property
    @abstractmethod
    def universe(self) -> DimensionUniverse:
        """All dimensions potentially associated with this container
        (`DimensionUniverse`).
        """
        raise NotImplementedError()


class HomogeneousDimensionRecordIterable(Iterable[DimensionRecord]):
    """An abstract base class for homogeneous iterables of dimension records.

    Notes
    -----
    All elements of a `HomogeneousDimensionRecordIterable` correspond to the
    same `DimensionElement` (i.e. share the same value for their
    `~DimensionRecord.definition` attribute).

    This base class makes no guarantees about duplication or multi-pass
    iteration.  Use `HomogeneousDimensionRecordAbstractSet` if either is
    required.
    """

    __slots__ = ()

    @property
    @abstractmethod
    def definition(self) -> DimensionElement:
        """The `DimensionElement` whose records this iterable contains."""
        raise NotImplementedError()

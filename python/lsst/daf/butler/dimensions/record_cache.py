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

__all__ = ("DimensionRecordCache",)

import copy
from collections.abc import Callable, Iterator, Mapping

from ._record_set import DimensionRecordSet
from ._universe import DimensionUniverse


class DimensionRecordCache(Mapping[str, DimensionRecordSet]):
    """A mapping of cached dimension records.

    This object holds all records for elements where
    `DimensionElement.is_cached` is `True`.

    Parameters
    ----------
    universe : `DimensionUniverse`
        Definitions of all dimensions.
    fetch : `~collections.abc.Callable`
        A callable that takes no arguments and returns a `dict` mapping `str`
        element name to a `DimensionRecordSet` of all records for that element.
        They keys of the returned `dict` must be exactly the elements in
        ``universe`` for which `DimensionElement.is_cached` is `True`.

    Notes
    -----
    The nested `DimensionRecordSet` objects should never be modified in place
    except when returned by the `modifying` context manager.
    """

    def __init__(self, universe: DimensionUniverse, fetch: Callable[[], dict[str, DimensionRecordSet]]):
        self._universe = universe
        self._keys = [element.name for element in universe.elements if element.is_cached]
        self._records: dict[str, DimensionRecordSet] | None = None
        self._fetch = fetch

    def reset(self) -> None:
        """Reset the cache, causing it to be fetched again on next use."""
        self._records = None

    def load_from(self, other: DimensionRecordCache) -> None:
        """Load records from another cache, but do nothing if it doesn't
        currently have any records.

        Parameters
        ----------
        other : `DimensionRecordCache`
            Other cache to potentially copy records from.
        """
        self._records = copy.deepcopy(other._records)

    def preload_cache(self) -> None:
        """Fetch the cache from the DB if it has not already been fetched."""
        if self._records is None:
            self._records = self._fetch()
            assert self._records.keys() == set(self._keys), "Logic bug in fetch callback."

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str):
            return False
        if (element := self._universe.get(key)) is not None:
            return element.is_cached
        return False

    def __getitem__(self, element: str) -> DimensionRecordSet:
        self.preload_cache()
        assert self._records is not None
        return self._records[element]

    def __iter__(self) -> Iterator[str]:
        return iter(self._keys)

    def __len__(self) -> int:
        return len(self._keys)

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

__all__ = ("DatasetTypeCache",)

from collections.abc import Iterable, Iterator
from typing import Generic, TypeVar

from .._dataset_type import DatasetType

_T = TypeVar("_T")


class DatasetTypeCache(Generic[_T]):
    """Cache for dataset types.

    Notes
    -----
    This class caches mapping of dataset type name to a corresponding
    `DatasetType` instance. Registry manager also needs to cache corresponding
    "storage" instance, so this class allows storing additional opaque object
    along with the dataset type.

    In come contexts (e.g. ``resolve_wildcard``) a full list of dataset types
    is needed. To signify that cache content can be used in such contexts,
    cache defines special ``full`` flag that needs to be set by client.
    """

    def __init__(self) -> None:
        self._cache: dict[str, tuple[DatasetType, _T | None]] = {}
        self._full = False

    @property
    def full(self) -> bool:
        """`True` if cache holds all known dataset types (`bool`)."""
        return self._full

    def add(self, dataset_type: DatasetType, extra: _T | None = None) -> None:
        """Add one record to the cache.

        Parameters
        ----------
        dataset_type : `DatasetType`
            Dataset type, replaces any existing dataset type with the same
            name.
        extra : `Any`, optional
            Additional opaque object stored with this dataset type.
        """
        self._cache[dataset_type.name] = (dataset_type, extra)

    def set(self, data: Iterable[DatasetType | tuple[DatasetType, _T | None]], *, full: bool = False) -> None:
        """Replace cache contents with the new set of dataset types.

        Parameters
        ----------
        data : `~collections.abc.Iterable`
            Sequence of `DatasetType` instances or tuples of `DatasetType` and
            an extra opaque object.
        full : `bool`
            If `True` then ``data`` contains all known dataset types.
        """
        self.clear()
        for item in data:
            if isinstance(item, DatasetType):
                item = (item, None)
            self._cache[item[0].name] = item
        self._full = full

    def clear(self) -> None:
        """Remove everything from the cache."""
        self._cache = {}
        self._full = False

    def discard(self, name: str) -> None:
        """Remove named dataset type from the cache.

        Parameters
        ----------
        name : `str`
            Name of the dataset type to remove.
        """
        self._cache.pop(name, None)

    def get(self, name: str) -> tuple[DatasetType | None, _T | None]:
        """Return cached info given dataset type name.

        Parameters
        ----------
        name : `str`
            Dataset type name.

        Returns
        -------
        dataset_type : `DatasetType` or `None`
            Cached dataset type, `None` is returned if the name is not in the
            cache.
        extra : `Any` or `None`
            Cached opaque data, `None` is returned if the name is not in the
            cache or no extra info was stored for this dataset type.
        """
        item = self._cache.get(name)
        if item is None:
            return (None, None)
        return item

    def get_dataset_type(self, name: str) -> DatasetType | None:
        """Return dataset type given its name.

        Parameters
        ----------
        name : `str`
            Dataset type name.

        Returns
        -------
        dataset_type : `DatasetType` or `None`
            Cached dataset type, `None` is returned if the name is not in the
            cache.
        """
        item = self._cache.get(name)
        if item is None:
            return None
        return item[0]

    def items(self) -> Iterator[tuple[DatasetType, _T | None]]:
        """Return iterator for the set of items in the cache, can only be
        used if `full` is true.

        Raises
        ------
        RuntimeError
            Raised if ``self.full`` is `False`.
        """
        if not self._full:
            raise RuntimeError("cannot call items() if cache is not full")
        return iter(self._cache.values())

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

__all__ = ("CollectionRecordCache",)

from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .interfaces import CollectionRecord


class CollectionRecordCache:
    """Cache for collection records.

    Notes
    -----
    This class stores collection records and can retrieve them using either
    collection name or collection key. One complication is that key type can be
    either collection name or a distinct integer value. To optimize storage
    when the key is the same as collection name, this class only stores key to
    record mapping when key is of a non-string type.

    In come contexts (e.g. ``resolve_wildcard``) a full list of collections is
    needed. To signify that cache content can be used in such contexts, cache
    defines special ``full`` flag that needs to be set by client.
    """

    def __init__(self) -> None:
        self._by_name: dict[str, CollectionRecord] = {}
        # This dict is only used for records whose key type is not str.
        self._by_key: dict[Any, CollectionRecord] = {}
        self._full = False

    @property
    def full(self) -> bool:
        """`True` if cache holds all known collection records (`bool`)."""
        return self._full

    def add(self, record: CollectionRecord) -> None:
        """Add one record to the cache.

        Parameters
        ----------
        record : `CollectionRecord`
            Collection record, replaces any existing record with the same name
            or key.
        """
        # In case we replace same record name with different key, find the
        # existing record and drop its key.
        if (old_record := self._by_name.get(record.name)) is not None:
            self._by_key.pop(old_record.key)
        if (old_record := self._by_key.get(record.key)) is not None:
            self._by_name.pop(old_record.name)
        self._by_name[record.name] = record
        if not isinstance(record.key, str):
            self._by_key[record.key] = record

    def set(self, records: Iterable[CollectionRecord], *, full: bool = False) -> None:
        """Replace cache contents with the new set of records.

        Parameters
        ----------
        records : `~collections.abc.Iterable` [`CollectionRecord`]
            Collection records.
        full : `bool`
            If `True` then ``records`` contain all known collection records.
        """
        self.clear()
        for record in records:
            self._by_name[record.name] = record
            if not isinstance(record.key, str):
                self._by_key[record.key] = record
        self._full = full

    def clear(self) -> None:
        """Remove all records from the cache."""
        self._by_name = {}
        self._by_key = {}
        self._full = False

    def discard(self, record: CollectionRecord) -> None:
        """Remove single record from the cache.

        Parameters
        ----------
        record : `CollectionRecord`
            Collection record to remove.
        """
        self._by_name.pop(record.name, None)
        if not isinstance(record.key, str):
            self._by_key.pop(record.key, None)

    def get_by_name(self, name: str) -> CollectionRecord | None:
        """Return collection record given its name.

        Parameters
        ----------
        name : `str`
            Collection name.

        Returns
        -------
        record : `CollectionRecord` or `None`
            Collection record, `None` is returned if the name is not in the
            cache.
        """
        return self._by_name.get(name)

    def get_by_key(self, key: Any) -> CollectionRecord | None:
        """Return collection record given its key.

        Parameters
        ----------
        key : `Any`
            Collection key.

        Returns
        -------
        record : `CollectionRecord` or `None`
            Collection record, `None` is returned if the key is not in the
            cache.
        """
        if isinstance(key, str):
            return self._by_name.get(key)
        return self._by_key.get(key)

    def records(self) -> Iterator[CollectionRecord]:
        """Return iterator for the set of records in the cache, can only be
        used if `full` is true.

        Raises
        ------
        RuntimeError
            Raised if ``self.full`` is `False`.
        """
        if not self._full:
            raise RuntimeError("cannot call records() if cache is not full")
        return iter(self._by_name.values())

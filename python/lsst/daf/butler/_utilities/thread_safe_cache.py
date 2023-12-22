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

import threading
from typing import Generic, TypeVar

TKey = TypeVar("TKey")
TValue = TypeVar("TValue")


class ThreadSafeCache(Generic[TKey, TValue]):
    """A simple thread-safe cache.  Ensures that once a value is stored for
    a key, it does not change.
    """

    def __init__(self) -> None:
        # This mutex may not actually be necessary for the current version of
        # CPython, because both the 'get' and 'set' operation use underlying C
        # functions that don't drop the GIL (at first glance anyway.) This is
        # not documented anywhere that I can find, however, so better safe than
        # sorry.
        self._mutex = threading.Lock()
        self._cache: dict[TKey, TValue] = dict()

    def get(self, key: TKey) -> TValue | None:
        """Return the value associated with the given key, or ``None`` if no
        value has been assigned to that key.

        Parameters
        ----------
        key : ``TKey``
            Key used to look up the value.
        """
        with self._mutex:
            return self._cache.get(key)

    def set_or_get(self, key: TKey, value: TValue) -> TValue:
        """Set a value for a key if the key does not already have a value.

        Parameters
        ----------
        key : ``TKey``
            Key used to look up the value.
        value : ``TValue``
            Value to store in the cache.

        Returns
        -------
        value : ``TValue``
            The existing value stored for the key if it was present, or
            ``value`` if this was a new key.
        """
        with self._mutex:
            return self._cache.setdefault(key, value)

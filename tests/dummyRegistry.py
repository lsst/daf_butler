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

from contextlib import contextmanager


class DummyRegistry:
    """Dummy Registry, for Datastore test purposes.

    TODO refactor StorageInfo out of registry into a separate object,
    that can more easily be dummyfied.
    """
    def __init__(self):
        self._counter = 0
        self._entries = {}

    def addStorageInfo(self, ref, storageInfo):
        # Only set ID if ID is 0 or None
        incrementCounter = True
        if ref.id is None or ref.id == 0:
            ref._id = self._counter
            incrementCounter = False
        self._entries[ref.id] = storageInfo
        if incrementCounter:
            self._counter += 1

    def getStorageInfo(self, ref, datastoreName):
        return self._entries[ref.id]

    def removeStorageInfo(self, datastoreName, ref):
        del self._entries[ref.id]

    def makeDatabaseDict(self, table, types, key, value):
        return dict()

    @contextmanager
    def transaction(self):
        yield

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

from lsst.daf.butler import DimensionUniverse


class DummyRegistry:
    """Dummy Registry, for Datastore test purposes.

    TODO refactor StorageInfo out of registry into a separate object,
    that can more easily be dummyfied.
    """
    def __init__(self):
        self._counter = 0
        self._entries = {}
        self.dimensions = DimensionUniverse()

    def addDatasetLocation(self, ref, datastoreName):
        # Only set ID if ID is 0 or None
        incrementCounter = True
        if ref.id is None or ref.id == 0:
            ref._id = self._counter
            incrementCounter = False
        if ref.id not in self._entries:
            self._entries[ref.id] = set()
        self._entries[ref.id].add(datastoreName)
        if incrementCounter:
            self._counter += 1

    def getDatasetLocations(self, ref):
        return self._entries[ref.id].copy()

    def removeDatasetLocation(self, datastoreName, ref):
        self._entries[ref.id].remove(datastoreName)

    def makeDatabaseDict(self, table, types, key, value, lengths=None):
        return dict()

    @contextmanager
    def transaction(self):
        yield

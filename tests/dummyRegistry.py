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
from typing import Any, Iterator

from lsst.daf.butler import DimensionUniverse, ddl


class DummyRegistry:
    """Dummy Registry, for Datastore test purposes.
    """
    def __init__(self):
        self._counter = 0
        self._entries = {}
        self._externalTableRows = {}
        self._externalTableSpecs = {}
        self.dimensions = DimensionUniverse()

    def registerOpaqueTable(self, name: str, spec: ddl.TableSpec):
        self._externalTableSpecs[name] = spec
        self._externalTableRows[name] = []

    def insertOpaqueData(self, name: str, *data: dict):
        spec = self._externalTableSpecs[name]
        uniqueConstraints = list(spec.unique)
        uniqueConstraints.append(tuple(field.name for field in spec.fields if field.primaryKey))
        for d in data:
            for constraint in uniqueConstraints:
                matching = list(self.fetchOpaqueData(name, **{k: d[k] for k in constraint}))
                if len(matching) != 0:
                    raise RuntimeError(f"Unique constraint {constraint} violation in external table {name}.")
            self._externalTableRows[name].append(d)

    def fetchOpaqueData(self, name: str, **where: Any) -> Iterator[dict]:
        for d in self._externalTableRows[name]:
            if all(d[k] == v for k, v in where.items()):
                yield d

    def deleteOpaqueData(self, name: str, **where: Any):
        kept = []
        for d in self._externalTableRows[name]:
            if not all(d[k] == v for k, v in where.items()):
                kept.append(d)
        self._externalTableRows[name] = kept

    def insertDatasetLocations(self, datastoreName, refs):
        # Only set ID if ID is 0 or None
        for ref in refs:
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

    def makeDatabaseDict(self, table, key, value):
        return dict()

    @contextmanager
    def transaction(self):
        yield

#
# LSST Data Management System
#
# Copyright 2008-2018  AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
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
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <https://www.lsstcorp.org/LegalNotices/>.
#

from .utils import slotValuesAreEqual, slotValuesToHash


class Run(object):
    _currentId = 0

    @classmethod
    def getNewId(cls):
        cls._currentId += 1
        return cls._currentId

    __slots__ = ("_runId", "_registryId", "_collection", "_environment", "_pipeline")
    __eq__ = slotValuesAreEqual
    __hash__ = slotValuesToHash

    def __init__(self, runId, registryId, collection, environment, pipeline):
        self._runId = runId
        self._registryId = registryId
        self._collection = collection
        self._environment = environment
        self._pipeline = pipeline

    @property
    def pkey(self):
        return (self._runId, self.registryId)

    @property
    def runId(self):
        return self._runId

    @property
    def registryId(self):
        return self._registryId

    @property
    def collection(self):
        return self._collection

    @property
    def environment(self):
        return self._environment

    @property
    def pipeline(self):
        return self._pipeline

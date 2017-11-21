# 
# LSST Data Management System
#
# Copyright 2008-2017  AURA/LSST.
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

from .datasets import DatasetRef
from .run import Run


class Quantum:

    __slots__ = ("_quantumId", "_registryId", "_run", "_task", "_predictedInputs", "_actualInputs")

    _currentId = -1

    @classmethod
    def getNewId(cls):
        cls._currentId += 1
        return cls._currentId

    def __init__(self, run, task=None, quantumId=None, registryId=None):
        assert isinstance(run, Run)
        self._quantumId = quantumId
        self._registryId = registryId
        self._run = run
        self._task = task
        self._predictedInputs = {}
        self._actualInputs = {}

    @property
    def pkey(self):
        if self._quantumId is not None and self._registryId is not None:
            return (self._quantumId, self._registryId)
        else:
            return None

    @property
    def quantumId(self):
        return self._quantumId

    @property
    def registryId(self):
        return self._registryId

    @property
    def run(self):
        return self._run

    @property
    def task(self):
        return self._task

    @property
    def predictedInputs(self):
        return self._predictedInputs

    @property
    def actualInputs(self):
        return self._actualInputs

    def addPredictedInput(self, ref):
        assert isinstance(ref, DatasetRef)

        if ref.type.name not in self._actualInputs:
            self._predictedInputs[ref.name] = [ref, ]
        else:
            self._predictedInputs[ref.name].append(ref)

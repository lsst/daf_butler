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

from types import MappingProxyType
from .utils import slotValuesAreEqual, slotValuesToHash
from .storageClass import StorageClass
from .units import DataUnitTypeSet


class DatasetType:

    __slots__ = "_name", "_template", "_units", "_storageClass"
    __eq__ = slotValuesAreEqual
    __hash__ = slotValuesToHash

    @property
    def name(self):
        return self._name
    
    @property
    def template(self):
        return self._template

    @property
    def units(self):
        return self._units

    @property
    def storageClass(self):
        return self._storageClass

    def __init__(self, name, template, units, storageClass):
        assert issubclass(storageClass, StorageClass)
        self._name = name
        self._template = template
        self._units = DataUnitTypeSet(units)
        self._storageClass = storageClass


class DatasetLabel:

    __slots__ = "_name", "_units"
    __eq__ = slotValuesAreEqual

    def __init__(self, name, **units):
        self._name = name
        self._units = units

    @property
    def name(self):
        return self._name

    @property
    def units(self):
        return self._units


class DatasetRef(DatasetLabel):

    __slots__ = ("_type", "_units", "_producer",
                 "_predictedConsumers", "_actualConsumers")

    _currentId = 0

    @classmethod
    def getNewId(cls):
        cls._currentId += 1
        return cls._currentId

    def __init__(self, type, units):
        units = type.units.conform(units)
        super().__init__(
            type.name,
            **{unit.__class__.__name__: unit.value for unit in units}
        )
        self._type = type
        self._units = units
        self._producer = None
        self._predictedConsumers = dict()
        self._actualConsumers = dict()

    @property
    def type(self):
        return self._type

    @property
    def units(self):
        return self._units

    @property
    def producer(self):
        return self._producer

    @property
    def predictedConsumers(self):
        return MappingProxyType(self._predictedConsumers)

    @property
    def actualConsumers(self):
        return MappingProxyType(self._actualConsumers)

    def makePath(self, run, template=None):
        raise NotImplementedError("TODO")


class DatasetHandle(DatasetRef):

    __slots__ = "_datasetId", "_registryId", "_uri", "_components", "_run"

    def __init__(self, datasetId, registryId, ref, uri, components, run):
        super().__init__(ref.type, ref.units)
        self._datasetId = datasetId
        self._registryId = registryId
        self._producer = ref.producer
        self._predictedConsumers.update(ref.predictedConsumers)
        self._actualConsumers.update(ref.actualConsumers)
        self._uri = uri
        self._components = MappingProxyType(components) if components else None
        self._run = run

    @property
    def datasetId(self):
        return self._datasetId

    @property
    def registryId(self):
        return self._registryId

    @property
    def uri(self):
        return self._uri

    @property
    def components(self):
        return self._components

    @property
    def run(self):
        return self._run

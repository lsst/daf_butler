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

import unittest
import datetime
from collections import OrderedDict

import lsst.utils.tests
from lsst.butler.registry import Registry
from lsst.butler.datasets import DatasetType, DatasetRef, DatasetLabel, DatasetHandle
from lsst.butler.run import Run
from lsst.butler.quantum import Quantum
from lsst.butler.units import DataUnitTypeSet, Camera, AbstractFilter, PhysicalFilter, PhysicalSensor, Visit, ObservedSensor, Snap, VisitRange, SkyMap, Tract, Patch
from lsst.butler.storageClass import Image


class RegistryTestCase(lsst.utils.tests.TestCase):

    def setUp(self):
        # Use fixed, but non-default, registryId for testing
        self.registryId = 10

    def _makeDatasetType(self, name="dummy", template=None, units=(Camera, ), storageClass=Image):
        return DatasetType(name, template, units, storageClass)

    def _makeDatasetUnits(self, cameraName, abstractFilterName, physicalFilterName):
        camera = Camera(cameraName)
        abstractFilter = AbstractFilter(abstractFilterName)
        physicalFilter = PhysicalFilter(abstractFilter, camera, physicalFilterName)
        return (camera, abstractFilter, physicalFilter)

    def _populateMinimalRegistry(self, registry, cameraName='dummycam', filters=('g', 'r', 'i', 'z', 'y'), visitNumbers=range(2)):
        """Make a minimal Registry, populated with ObservedSensor (and all dependent DataUnits)
        for the specified *cameraName*, *filters* and *visitNumbers*.
        """
        visitDuration = 30. # seconds

        camera = Camera(cameraName)
        registry.addDataUnit(camera)

        for n, f in enumerate(filters):
            abstractFilter = AbstractFilter(f)
            physicalFilter = PhysicalFilter(abstractFilter, camera, "{0}_{1}".format(cameraName, f))
            physicalSensor = PhysicalSensor(camera, n, name=str(n), group="", purpose="")

            for unit in (abstractFilter, physicalFilter, physicalSensor):
                registry.addDataUnit(unit)

            obsBegin = datetime.datetime(2017, 1, 1)
            for n in visitNumbers:
                visit = Visit(camera, n, physicalFilter, obsBegin, exposureTime=visitDuration, region=None)
                observedSensor = ObservedSensor(camera, visit, physicalSensor, region=None)
                for unit in (visit, observedSensor):
                    registry.addDataUnit(unit)
                obsBegin += datetime.timedelta(seconds=visitDuration)

        run = registry.makeRun("testing")
        datasetType = DatasetType("testdst", template=None, units=(ObservedSensor, ), storageClass=Image)
        registry.registerDatasetType(datasetType)

        return run, datasetType

    def _makeDataUnits(self):
        name = "DummyCam"
        module = "dummycam"
        filterName = "i"
        physicalSensorNumber = 0
        physicalSensorName = str(physicalSensorNumber)
        group = "dummy group"
        purpose = "no purpose"
        visitNumber = 1
        obsBegin = datetime.datetime(2017, 1, 1)
        exposureTime = 60.
        region = None
        snapIndex = 2
        skymapName = "DummyMap"
        skymapModule = "dummyMapModule"
        tractNumber = 3
        patchIndex = 4
        cellX = 5
        cellY = 6

        units = OrderedDict()
        units['Camera'] = Camera(name, module)
        units['AbstractFilter'] = AbstractFilter(filterName)
        units['PhysicalFilter'] = PhysicalFilter(units['AbstractFilter'], units['Camera'], "DummyCam_i")
        units['PhysicalSensor'] = PhysicalSensor(units['Camera'], physicalSensorNumber, physicalSensorName, group, purpose)
        units['Visit'] = Visit(units['Camera'], visitNumber, units['PhysicalFilter'], obsBegin, exposureTime, region)
        units['ObservedSensor'] = ObservedSensor(units['Camera'], units['Visit'], units['PhysicalSensor'], region)
        units['Snap'] = Snap(units['Camera'], units['Visit'], snapIndex, obsBegin, exposureTime)
        units['SkyMap'] = SkyMap(skymapName, skymapModule)
        units['Tract'] = Tract(units['SkyMap'], tractNumber, region)
        units['Patch'] = Patch(units['SkyMap'], units['Tract'], patchIndex, cellX, cellY, region)

        return units

    def testConstructor(self):
        registry = Registry(id=self.registryId)
        self.assertEqual(registry.id, self.registryId)
        from sqlalchemy.engine import Engine
        self.assertIsInstance(registry.engine, Engine)

    def testRegisterDatasetType(self):
        validDatasetType = self._makeDatasetType()
        invalidDatasetType = None

        registry = Registry()
        registry.registerDatasetType(validDatasetType)
        with self.assertRaises(AssertionError):
            registry.registerDatasetType(invalidDatasetType)

    def testGetDatasetType(self):
        inDatasetType = self._makeDatasetType()

        registry = Registry()
        registry.registerDatasetType(inDatasetType)
        outDatasetType = registry.getDatasetType(inDatasetType.name)
        self.assertEqual(inDatasetType, outDatasetType)
        self.assertIsNone(registry.getDatasetType('some_invalid_name'))

    def testAddDataset(self):
        tag = "test_collection"
        datasetTypeName = "test_dataset"
        uri = "http://ls.st/mydummy"
        units = self._makeDatasetUnits(cameraName="DummyCam", abstractFilterName="i", physicalFilterName="dummy_i")

        registry = Registry()
        run = registry.makeRun(tag)
        for unit in units:
            registry.addDataUnit(unit)

        # DatasetType takes a tuple of DataUnit types (classes)
        datasetType = DatasetType(name=datasetTypeName, template=None, units=(type(unit) for unit in units), storageClass=Image)
        registry.registerDatasetType(datasetType)
        # DatasetRef takes a dictionary of name : DataUnit instances
        datasetRef = DatasetRef(datasetType, {unit.__class__.__name__ : unit for unit in units})
        datasetHandle = registry.addDataset(datasetRef, uri, components=None, run=run, producer=None)
        # Should not be able to add the same DatasetRef twice
        with self.assertRaises(ValueError):
            registry.addDataset(datasetRef, uri, components=None, run=run, producer=None)
        datasetHandleOut = registry.find(tag, datasetRef)
        self.assertEqual(datasetHandleOut, datasetHandle)
        # Querying with other tag should return None
        self.assertIsNone(registry.find("unknown_tag", datasetRef))
        # Querying with other DataUnits should return None
        camera2 = Camera("some_other_camera")
        registry.addDataUnit(camera2)
        units2 = {unit.__class__.__name__ : unit for unit in units}
        units2['Camera'] = camera2
        datasetRef2 = DatasetRef(datasetType, units2)
        self.assertIsNone(registry.find(tag, datasetRef2))

        self._testAssociations(registry, datasetRef, tag)

    def _testAssociations(self, registry, datasetRef, originalTag):
        newTag = "some_new_tag"
        datasetHandle = registry.find(originalTag, datasetRef)
        self.assertIsInstance(datasetHandle, DatasetHandle)

        registry.associate(newTag, (datasetHandle, ))
        self.assertEqual(registry.find(newTag, datasetRef), datasetHandle)

        registry.disassociate(newTag, (datasetHandle, ), remove=False)
        self.assertIsNone(registry.find(newTag, datasetRef))

        # Should still be associated with its original tag
        self.assertEqual(registry.find(originalTag, datasetRef), datasetHandle)

    def testMakeRun(self):
        tag = "testing"
        registry = Registry()
        run = registry.makeRun(tag)
        self.assertIsInstance(run, Run)
        self.assertEqual(run.tag, tag)

    def testUpdateRun(self):
        # Not yet implemented in prototype
        pass

    def testGetRun(self):
        tag1 = "testing1"
        tag2 = "testing2"
        registry = Registry()
        run1 = registry.makeRun(tag1)
        run2 = registry.makeRun(tag2)
        self.assertNotEqual(run1, run2)
        self.assertEqual(registry.getRun(run1.pkey), run1)
        self.assertEqual(registry.getRun(run2.pkey), run2)
        self.assertIsNone(registry.getRun((run2.pkey[0], run2.pkey[1]+1)))
        self.assertIsNone(registry.getRun((run2.pkey[0]+1, run2.pkey[1])))

    def testAddQuantum(self):
        registry = Registry(id=self.registryId)
        run = registry.makeRun("testing")
        quantum = Quantum(run)
        registry.addQuantum(quantum)
        self.assertEqual(quantum.pkey, (0, self.registryId))
        # Adding a quantum with a fully specified pkey should fail
        with self.assertRaises(AssertionError):
            registry.addQuantum(quantum)
        with self.assertRaises(AssertionError):
            registry.addQuantum(Quantum(run, quantumId=0, registryId=0))

    def testMarkInputUsed(self):
        cameraName = "dummycam"
        visitNumbers = (0, 1)
        registry = Registry()
        run, datasetType = self._populateMinimalRegistry(registry, cameraName=cameraName, visitNumbers=visitNumbers)
        quantum = Quantum(run)
        registry.addQuantum(quantum)
        handles = []
        for visitNumber in visitNumbers:
            datasetLabel = DatasetLabel(datasetType.name, Camera=cameraName, AbstractFilter='i', PhysicalFilter='dummycam_i', PhysicalSensor=2, Visit=visitNumber)
            datasetRef = registry.expand(datasetLabel)
            datasetHandle = registry.addDataset(datasetRef, uri="", components=None, run=run, producer=None)
            quantum.addPredictedInput(datasetHandle)
            handles.append(datasetHandle)
        for handle in handles:
            registry.markInputUsed(quantum, handle)

    def testAddDataUnit(self):
        registry = Registry()
        for _, unit in self._makeDataUnits().items():
            registry.addDataUnit(unit)

    def testFindDataUnit(self):
        registry = Registry()
        units = self._makeDataUnits()
        unitValues = {unitTypeName : unitInstance.value for unitTypeName, unitInstance in units.items()}
        for _, unitInstance in units.items():
            registry.addDataUnit(unitInstance)
            self.assertEqual(registry.findDataUnit(type(unitInstance), unitValues), unitInstance)

    def testExpand(self):
        name = "dummy"
        units = self._makeDataUnits()
        # DatasetType takes a sequence of DataUnit types
        datasetType = self._makeDatasetType(units=(type(unit) for unit in units.values()))
        registry = Registry()
        registry.registerDatasetType(datasetType)
        # Register units (in dependency order)
        for unit in units.values():
            registry.addDataUnit(unit)
        # DatasetLabel takes name=pkey kwargs, where name is the name of the DataUnit type
        label = DatasetLabel(name, **{unitTypeName : unitInstance.value for unitTypeName, unitInstance in units.items()})
        ref = registry.expand(label)
        self.assertIsInstance(ref, DatasetRef)
        self.assertEqual(ref.name, label.name)
        # Check DataUnits
        self.assertEqual(len(ref.units), len(units))
        for unit in ref.units:
            self.assertEqual(unit, units[unit.__class__.__name__])
        # Test passthrough of DatasetRef
        passthroughRef = registry.expand(ref)
        self.assertIs(passthroughRef, ref)

    def testSubset(self):
        # Not yet implemented in prototype
        pass

    def testMerge(self):
        # Not yet implemented in prototype
        pass

    def testMakeDataGraph(self):
        # Not yet implemented in prototype
        pass

    def testMakeProvenanceGraph(self):
        # Not yet implemented in prototype
        pass

    def testExport(self):
        # Not yet implemented in prototype
        pass

    def testImport_(self):
        # Not yet implemented in prototype
       pass

    def testTransfer(self):
        # Not yet implemented in prototype
        pass


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()


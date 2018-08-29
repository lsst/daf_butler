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

import os
import unittest

import lsst.utils.tests

from lsst.daf.butler import (ButlerConfig, DatasetType, Registry,
                             DatasetOriginInfoDef, StorageClass)
from lsst.sphgeom import Angle, Box, LonLat, NormalizedAngle


class SqlPreFlightTestCase(lsst.utils.tests.TestCase):
    """Test for SqlPreFlight.
    """

    def setUp(self):
        self.testDir = os.path.dirname(__file__)
        self.configFile = os.path.join(self.testDir, "config/basic/butler.yaml")
        # easiest way to make SqlPreFlight instance is to ask SqlRegistry to do it
        self.butlerConfig = ButlerConfig(self.configFile)
        self.registry = Registry.fromConfig(self.butlerConfig)
        self.preFlight = self.registry._preFlight

    def testDatasetOriginInfoDef(self):
        """Test for DatasetOriginInfoDef class"""

        coll = DatasetOriginInfoDef(defaultInputs=["a", "b"], defaultOutput="out")
        self.assertEqual(coll.getInputCollections("ds"), ["a", "b"])
        self.assertEqual(coll.getInputCollections("ds2"), ["a", "b"])
        self.assertEqual(coll.getOutputCollection("ds"), "out")
        self.assertEqual(coll.getOutputCollection("ds2"), "out")

        coll = DatasetOriginInfoDef(defaultInputs=["a", "b"], defaultOutput="out",
                                    inputOverrides=dict(ds2=["c"]),
                                    outputOverrides=dict(ds2="out2"))
        self.assertEqual(coll.getInputCollections("ds"), ["a", "b"])
        self.assertEqual(coll.getInputCollections("ds2"), ["c"])
        self.assertEqual(coll.getOutputCollection("ds"), "out")
        self.assertEqual(coll.getOutputCollection("ds2"), "out2")

    def testPreFlightCameraUnits(self):
        """Test involving only Camera units, no joins to SkyMap"""
        registry = self.registry

        # need a bunch of units and datasets for test
        registry.addDataUnitEntry("Camera", dict(camera="DummyCam"))
        registry.addDataUnitEntry("PhysicalFilter", dict(camera="DummyCam",
                                                         physical_filter="dummy_r",
                                                         abstract_filter="r"))
        registry.addDataUnitEntry("PhysicalFilter", dict(camera="DummyCam",
                                                         physical_filter="dummy_i",
                                                         abstract_filter="i"))
        for sensor in (1, 2, 3, 4, 5):
            registry.addDataUnitEntry("Sensor", dict(camera="DummyCam", sensor=sensor))
        registry.addDataUnitEntry("Visit", dict(camera="DummyCam", visit=10, physical_filter="dummy_i"))
        registry.addDataUnitEntry("Visit", dict(camera="DummyCam", visit=11, physical_filter="dummy_r"))
        registry.addDataUnitEntry("Visit", dict(camera="DummyCam", visit=20, physical_filter="dummy_r"))
        registry.addDataUnitEntry("Exposure", dict(camera="DummyCam", exposure=100, visit=10,
                                                   physical_filter="dummy_i"))
        registry.addDataUnitEntry("Exposure", dict(camera="DummyCam", exposure=101, visit=10,
                                                   physical_filter="dummy_i"))
        registry.addDataUnitEntry("Exposure", dict(camera="DummyCam", exposure=110, visit=11,
                                                   physical_filter="dummy_r"))
        registry.addDataUnitEntry("Exposure", dict(camera="DummyCam", exposure=111, visit=11,
                                                   physical_filter="dummy_r"))
        registry.addDataUnitEntry("Exposure", dict(camera="DummyCam", exposure=200, visit=20,
                                                   physical_filter="dummy_r"))
        registry.addDataUnitEntry("Exposure", dict(camera="DummyCam", exposure=201, visit=20,
                                                   physical_filter="dummy_r"))

        # dataset types
        run = registry.makeRun(collection="test")
        run2 = registry.makeRun(collection="test2")
        storageClass = StorageClass("testDataset")
        registry.storageClasses.registerStorageClass(storageClass)
        rawType = DatasetType(name="RAW", dataUnits=("Camera", "Exposure", "Sensor"),
                              storageClass=storageClass)
        registry.registerDatasetType(rawType)
        calexpType = DatasetType(name="CALEXP", dataUnits=("Camera", "Visit", "Sensor"),
                                 storageClass=storageClass)
        registry.registerDatasetType(calexpType)

        # add pre-existing datasets
        for exposure in (100, 101, 110, 111):
            for sensor in (1, 2, 3):
                # note that only 3 of 5 sensors have datasets
                dataId = dict(camera="DummyCam", exposure=exposure, sensor=sensor)
                ref = registry.addDataset(rawType, dataId=dataId, run=run)
                # Exposures 100 and 101 appear in both collections, 100 has different
                # dataset_id in different collections, for 101 only single dataset_id exists
                if exposure == 100:
                    registry.addDataset(rawType, dataId=dataId, run=run2)
                if exposure == 101:
                    registry.associate(run2.collection, [ref])
        # Add pre-existing datasets to second collection.
        for exposure in (200, 201):
            for sensor in (3, 4, 5):
                # note that only 3 of 5 sensors have datasets
                dataId = dict(camera="DummyCam", exposure=exposure, sensor=sensor)
                registry.addDataset(rawType, dataId=dataId, run=run2)

        # with empty expression
        coll = DatasetOriginInfoDef(defaultInputs=["test"], defaultOutput="test")
        rows = self.preFlight.selectDataUnits(originInfo=coll,
                                              expression="",
                                              neededDatasetTypes=[rawType],
                                              futureDatasetTypes=[calexpType])
        rows = list(rows)
        self.assertEqual(len(rows), 4*3)   # 4 exposures times 3 sensors
        for row in rows:
            self.assertCountEqual(row.dataId.keys(), ("camera", "sensor", "exposure", "visit"))
            self.assertCountEqual(row.datasetRefs.keys(), (rawType, calexpType))
        self.assertCountEqual(set(row.dataId["exposure"] for row in rows),
                              (100, 101, 110, 111))
        self.assertCountEqual(set(row.dataId["visit"] for row in rows), (10, 11))
        self.assertCountEqual(set(row.dataId["sensor"] for row in rows), (1, 2, 3))

        # second collection
        coll = DatasetOriginInfoDef(defaultInputs=["test2"], defaultOutput="test")
        rows = self.preFlight.selectDataUnits(originInfo=coll,
                                              expression="",
                                              neededDatasetTypes=[rawType],
                                              futureDatasetTypes=[calexpType])
        rows = list(rows)
        self.assertEqual(len(rows), 4*3)   # 4 exposures times 3 sensors
        for row in rows:
            self.assertCountEqual(row.dataId.keys(), ("camera", "sensor", "exposure", "visit"))
            self.assertCountEqual(row.datasetRefs.keys(), (rawType, calexpType))
        self.assertCountEqual(set(row.dataId["exposure"] for row in rows),
                              (100, 101, 200, 201))
        self.assertCountEqual(set(row.dataId["visit"] for row in rows), (10, 20))
        self.assertCountEqual(set(row.dataId["sensor"] for row in rows), (1, 2, 3, 4, 5))

        # with two input datasets
        coll = DatasetOriginInfoDef(defaultInputs=["test", "test2"], defaultOutput="test2")
        rows = self.preFlight.selectDataUnits(originInfo=coll,
                                              expression="",
                                              neededDatasetTypes=[rawType],
                                              futureDatasetTypes=[calexpType])
        rows = list(rows)
        self.assertEqual(len(rows), 6*3)   # 6 exposures times 3 sensors
        for row in rows:
            self.assertCountEqual(row.dataId.keys(), ("camera", "sensor", "exposure", "visit"))
            self.assertCountEqual(row.datasetRefs.keys(), (rawType, calexpType))
        self.assertCountEqual(set(row.dataId["exposure"] for row in rows),
                              (100, 101, 110, 111, 200, 201))
        self.assertCountEqual(set(row.dataId["visit"] for row in rows), (10, 11, 20))
        self.assertCountEqual(set(row.dataId["sensor"] for row in rows), (1, 2, 3, 4, 5))

        # limit to single visit
        coll = DatasetOriginInfoDef(defaultInputs=["test"], defaultOutput=None)
        rows = self.preFlight.selectDataUnits(originInfo=coll,
                                              expression="Visit.visit = 10",
                                              neededDatasetTypes=[rawType],
                                              futureDatasetTypes=[calexpType])
        rows = list(rows)
        self.assertEqual(len(rows), 2*3)   # 2 exposures times 3 sensors
        self.assertCountEqual(set(row.dataId["exposure"] for row in rows), (100, 101))
        self.assertCountEqual(set(row.dataId["visit"] for row in rows), (10,))
        self.assertCountEqual(set(row.dataId["sensor"] for row in rows), (1, 2, 3))

        # more limiting expression
        coll = DatasetOriginInfoDef(defaultInputs=["test"], defaultOutput="")
        rows = self.preFlight.selectDataUnits(originInfo=coll,
                                              expression="Visit.visit = 10 and Sensor.sensor > 1",
                                              neededDatasetTypes=[rawType],
                                              futureDatasetTypes=[calexpType])
        rows = list(rows)
        self.assertEqual(len(rows), 2*2)   # 2 exposures times 2 sensors
        self.assertCountEqual(set(row.dataId["exposure"] for row in rows), (100, 101))
        self.assertCountEqual(set(row.dataId["visit"] for row in rows), (10,))
        self.assertCountEqual(set(row.dataId["sensor"] for row in rows), (2, 3))

        # expression excludes everyhting
        rows = self.preFlight.selectDataUnits(originInfo=coll,
                                              expression="Visit.visit > 1000",
                                              neededDatasetTypes=[rawType],
                                              futureDatasetTypes=[calexpType])
        rows = list(rows)
        self.assertEqual(len(rows), 0)

        # Selecting by PhysicalFilter, this is not in the units, but it is
        # a part of the full expression so it should work too.
        rows = self.preFlight.selectDataUnits(originInfo=coll,
                                              expression="PhysicalFilter.physical_filter = 'dummy_r'",
                                              neededDatasetTypes=[rawType],
                                              futureDatasetTypes=[calexpType])
        rows = list(rows)
        self.assertEqual(len(rows), 2*3)   # 2 exposures times 3 sensors
        self.assertCountEqual(set(row.dataId["exposure"] for row in rows), (110, 111))
        self.assertCountEqual(set(row.dataId["visit"] for row in rows), (11,))
        self.assertCountEqual(set(row.dataId["sensor"] for row in rows), (1, 2, 3))

    def testPreFlightSkyMapUnits(self):
        """Test involving only SkyMap units, no joins to Camera"""
        registry = self.registry

        # need a bunch of units and datasets for test, we want "AbstractFilter"
        # in the test so also have to add PhysicalFilter units
        registry.addDataUnitEntry("Camera", dict(camera="DummyCam"))
        registry.addDataUnitEntry("PhysicalFilter", dict(camera="DummyCam",
                                                         physical_filter="dummy_r",
                                                         abstract_filter="r"))
        registry.addDataUnitEntry("PhysicalFilter", dict(camera="DummyCam",
                                                         physical_filter="dummy_i",
                                                         abstract_filter="i"))
        registry.addDataUnitEntry("SkyMap", dict(skymap="DummyMap", hash="sha!"))
        for tract in range(10):
            registry.addDataUnitEntry("Tract", dict(skymap="DummyMap", tract=tract))
            for patch in range(10):
                registry.addDataUnitEntry("Patch", dict(skymap="DummyMap", tract=tract, patch=patch,
                                                        cell_x=0, cell_y=0,
                                                        region=Box(LonLat(NormalizedAngle(0.), Angle(0.)))))

        # dataset types
        run = registry.makeRun(collection="test")
        storageClass = StorageClass("testDataset")
        registry.storageClasses.registerStorageClass(storageClass)
        calexpType = DatasetType(name="deepCoadd_calexp", dataUnits=("SkyMap", "Tract", "Patch",
                                                                     "AbstractFilter"),
                                 storageClass=storageClass)
        registry.registerDatasetType(calexpType)
        mergeType = DatasetType(name="deepCoadd_mergeDet", dataUnits=("SkyMap", "Tract", "Patch"),
                                storageClass=storageClass)
        registry.registerDatasetType(mergeType)
        measType = DatasetType(name="deepCoadd_meas", dataUnits=("SkyMap", "Tract", "Patch",
                                                                 "AbstractFilter"),
                               storageClass=storageClass)
        registry.registerDatasetType(measType)

        # add pre-existing datasets
        for tract in (1, 3, 5):
            for patch in (2, 4, 6, 7):
                dataId = dict(skymap="DummyMap", tract=tract, patch=patch)
                registry.addDataset(mergeType, dataId=dataId, run=run)
                for aFilter in ("i", "r"):
                    dataId = dict(skymap="DummyMap", tract=tract, patch=patch, abstract_filter=aFilter)
                    registry.addDataset(calexpType, dataId=dataId, run=run)

        # with empty expression
        coll = DatasetOriginInfoDef(defaultInputs=["test"], defaultOutput="")
        rows = self.preFlight.selectDataUnits(originInfo=coll,
                                              expression="",
                                              neededDatasetTypes=[calexpType, mergeType],
                                              futureDatasetTypes=[measType])
        rows = list(rows)
        self.assertEqual(len(rows), 3*4*2)   # 4 tracts x 4 patches x 2 filters
        for row in rows:
            self.assertCountEqual(row.dataId.keys(), ("skymap", "tract", "patch", "abstract_filter"))
            self.assertCountEqual(row.datasetRefs.keys(), (calexpType, mergeType, measType))
        self.assertCountEqual(set(row.dataId["tract"] for row in rows), (1, 3, 5))
        self.assertCountEqual(set(row.dataId["patch"] for row in rows), (2, 4, 6, 7))
        self.assertCountEqual(set(row.dataId["abstract_filter"] for row in rows), ("i", "r"))

        # limit to 2 tracts and 2 patches
        rows = self.preFlight.selectDataUnits(originInfo=coll,
                                              expression="Tract.tract IN (1, 5) AND Patch.patch IN (2, 7)",
                                              neededDatasetTypes=[calexpType, mergeType],
                                              futureDatasetTypes=[measType])
        rows = list(rows)
        self.assertEqual(len(rows), 2*2*2)   # 4 tracts x 4 patches x 2 filters
        self.assertCountEqual(set(row.dataId["tract"] for row in rows), (1, 5))
        self.assertCountEqual(set(row.dataId["patch"] for row in rows), (2, 7))
        self.assertCountEqual(set(row.dataId["abstract_filter"] for row in rows), ("i", "r"))

        # limit to single filter
        rows = self.preFlight.selectDataUnits(originInfo=coll,
                                              expression="AbstractFilter.abstract_filter = 'i'",
                                              neededDatasetTypes=[calexpType, mergeType],
                                              futureDatasetTypes=[measType])
        rows = list(rows)
        self.assertEqual(len(rows), 3*4*1)   # 4 tracts x 4 patches x 2 filters
        self.assertCountEqual(set(row.dataId["tract"] for row in rows), (1, 3, 5))
        self.assertCountEqual(set(row.dataId["patch"] for row in rows), (2, 4, 6, 7))
        self.assertCountEqual(set(row.dataId["abstract_filter"] for row in rows), ("i",))

        # expression excludes everyhting, specifying non-existing skymap is not a
        # fatal error, it's operator error
        rows = self.preFlight.selectDataUnits(originInfo=coll,
                                              expression="SkyMap.skymap = 'Mars'",
                                              neededDatasetTypes=[calexpType, mergeType],
                                              futureDatasetTypes=[measType])
        rows = list(rows)
        self.assertEqual(len(rows), 0)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()

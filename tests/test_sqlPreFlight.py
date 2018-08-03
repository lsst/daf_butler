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

from lsst.daf.butler.core.butlerConfig import ButlerConfig
from lsst.daf.butler.core.datasets import DatasetType
from lsst.daf.butler.core.registry import Registry
from lsst.daf.butler.core.storageClass import StorageClass
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
        registry.addDataUnitEntry("Exposure", dict(camera="DummyCam", exposure=100, visit=10,
                                                   physical_filter="dummy_i"))
        registry.addDataUnitEntry("Exposure", dict(camera="DummyCam", exposure=101, visit=10,
                                                   physical_filter="dummy_i"))
        registry.addDataUnitEntry("Exposure", dict(camera="DummyCam", exposure=110, visit=11,
                                                   physical_filter="dummy_r"))
        registry.addDataUnitEntry("Exposure", dict(camera="DummyCam", exposure=111, visit=11,
                                                   physical_filter="dummy_r"))

        # dataset types
        run = registry.makeRun(collection="test")
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
                registry.addDataset(rawType, dataId=dataId, run=run)

        # with empty expression
        headers, rows = self.preFlight.selectDataUnits(collections=["test"], expr="",
                                                       neededDatasetTypes=[rawType],
                                                       futureDatasetTypes=[calexpType])
        # order of columns is not defined
        self.assertCountEqual(headers, (("Camera", "camera"),
                                        ("Sensor", "sensor"),
                                        ("Exposure", "exposure"),
                                        ("Visit", "visit")))
        rows = list(rows)
        self.assertEqual(len(rows), 4*3)   # 4 exposures times 3 sensors

        # limit to single visit
        headers, rows = self.preFlight.selectDataUnits(collections=["test"],
                                                       expr="Visit.visit = 10",
                                                       neededDatasetTypes=[rawType],
                                                       futureDatasetTypes=[calexpType])
        self.assertEqual(len(headers), 4)  # headers must be the same
        rows = list(rows)
        self.assertEqual(len(rows), 2*3)   # 2 exposures times 3 sensors

        # expression excludes everyhting
        headers, rows = self.preFlight.selectDataUnits(collections=["test"],
                                                       expr="Visit.visit = 10 and Sensor.sensor > 1",
                                                       neededDatasetTypes=[rawType],
                                                       futureDatasetTypes=[calexpType])
        self.assertEqual(len(headers), 4)  # headers must be the same
        rows = list(rows)
        self.assertEqual(len(rows), 2*2)   # 2 exposures times 2 sensors

        # expression excludes everyhting
        headers, rows = self.preFlight.selectDataUnits(collections=["test"],
                                                       expr="Visit.visit > 1000",
                                                       neededDatasetTypes=[rawType],
                                                       futureDatasetTypes=[calexpType])
        self.assertEqual(len(headers), 4)  # headers must be the same
        rows = list(rows)
        self.assertEqual(len(rows), 0)

        # Selecting by PhysicalFilter, this should be done via
        # PhysicalFilter.physical_filter but that is not supported currently,
        # instead we can do Visit.physical_filter (or Exposure.physical_filter)
        headers, rows = self.preFlight.selectDataUnits(collections=["test"],
                                                       expr="Visit.physical_filter = 'dummy_r'",
                                                       neededDatasetTypes=[rawType],
                                                       futureDatasetTypes=[calexpType])
        self.assertEqual(len(headers), 4)  # headers must be the same
        rows = list(rows)
        self.assertEqual(len(rows), 2*3)   # 2 exposures times 3 sensors

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
        headers, rows = self.preFlight.selectDataUnits(collections=["test"], expr="",
                                                       neededDatasetTypes=[calexpType, mergeType],
                                                       futureDatasetTypes=[measType])
        # order of columns is not defined
        self.assertCountEqual(headers, (("SkyMap", "skymap"),
                                        ("Tract", "tract"),
                                        ("Patch", "patch"),
                                        ("AbstractFilter", "abstract_filter")))
        rows = list(rows)
        self.assertEqual(len(rows), 3*4*2)   # 4 tracts x 4 patches x 2 filters

        # limit to 2 tracts and 2 patches
        headers, rows = self.preFlight.selectDataUnits(collections=["test"],
                                                       expr="Tract.tract IN (1, 5) AND Patch.patch IN (2, 7)",
                                                       neededDatasetTypes=[calexpType, mergeType],
                                                       futureDatasetTypes=[measType])
        # order of columns is not defined
        self.assertEqual(len(headers), 4)  # headers must be the same
        rows = list(rows)
        self.assertEqual(len(rows), 2*2*2)   # 4 tracts x 4 patches x 2 filters

        # limit to single filter
        headers, rows = self.preFlight.selectDataUnits(collections=["test"],
                                                       expr="AbstractFilter.abstract_filter = 'i'",
                                                       neededDatasetTypes=[calexpType, mergeType],
                                                       futureDatasetTypes=[measType])
        # order of columns is not defined
        self.assertEqual(len(headers), 4)  # headers must be the same
        rows = list(rows)
        self.assertEqual(len(rows), 3*4*1)   # 4 tracts x 4 patches x 2 filters

        # expression excludes everyhting, specifying non-existing skymap is not a
        # fatal error, it's operator error
        headers, rows = self.preFlight.selectDataUnits(collections=["test"],
                                                       expr="SkyMap.skymap = 'Mars'",
                                                       neededDatasetTypes=[calexpType, mergeType],
                                                       futureDatasetTypes=[measType])
        # order of columns is not defined
        self.assertEqual(len(headers), 4)  # headers must be the same
        rows = list(rows)
        self.assertEqual(len(rows), 0)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()

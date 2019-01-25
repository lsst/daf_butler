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

from datetime import datetime, timedelta
import os
import unittest

import lsst.utils.tests

from lsst.daf.butler import (ButlerConfig, DatasetType, Registry, DataId,
                             DatasetOriginInfoDef, StorageClass)
from lsst.daf.butler.registries.sqlPreFlight import SqlPreFlight
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
        self.preFlight = SqlPreFlight(self.registry)

    def testDatasetOriginInfoDef(self):
        """Test for DatasetOriginInfoDef class"""

        originInfo = DatasetOriginInfoDef(defaultInputs=["a", "b"], defaultOutput="out")
        self.assertEqual(originInfo.getInputCollections("ds"), ["a", "b"])
        self.assertEqual(originInfo.getInputCollections("ds2"), ["a", "b"])
        self.assertEqual(originInfo.getOutputCollection("ds"), "out")
        self.assertEqual(originInfo.getOutputCollection("ds2"), "out")

        originInfo = DatasetOriginInfoDef(defaultInputs=["a", "b"], defaultOutput="out",
                                          inputOverrides=dict(ds2=["c"]),
                                          outputOverrides=dict(ds2="out2"))
        self.assertEqual(originInfo.getInputCollections("ds"), ["a", "b"])
        self.assertEqual(originInfo.getInputCollections("ds2"), ["c"])
        self.assertEqual(originInfo.getOutputCollection("ds"), "out")
        self.assertEqual(originInfo.getOutputCollection("ds2"), "out2")

    def testPreFlightInstrumentUnits(self):
        """Test involving only Instrument units, no joins to SkyMap"""
        registry = self.registry

        # need a bunch of units and datasets for test
        registry.addDimensionEntry("Instrument", dict(instrument="DummyCam", visit_max=25, exposure_max=300,
                                                      detector_max=6))
        registry.addDimensionEntry("PhysicalFilter", dict(instrument="DummyCam",
                                                          physical_filter="dummy_r",
                                                          abstract_filter="r"))
        registry.addDimensionEntry("PhysicalFilter", dict(instrument="DummyCam",
                                                          physical_filter="dummy_i",
                                                          abstract_filter="i"))
        for detector in (1, 2, 3, 4, 5):
            registry.addDimensionEntry("Detector", dict(instrument="DummyCam", detector=detector))
        registry.addDimensionEntry("Visit", dict(instrument="DummyCam", visit=10, physical_filter="dummy_i"))
        registry.addDimensionEntry("Visit", dict(instrument="DummyCam", visit=11, physical_filter="dummy_r"))
        registry.addDimensionEntry("Visit", dict(instrument="DummyCam", visit=20, physical_filter="dummy_r"))
        registry.addDimensionEntry("Exposure", dict(instrument="DummyCam", exposure=100, visit=10,
                                                    physical_filter="dummy_i"))
        registry.addDimensionEntry("Exposure", dict(instrument="DummyCam", exposure=101, visit=10,
                                                    physical_filter="dummy_i"))
        registry.addDimensionEntry("Exposure", dict(instrument="DummyCam", exposure=110, visit=11,
                                                    physical_filter="dummy_r"))
        registry.addDimensionEntry("Exposure", dict(instrument="DummyCam", exposure=111, visit=11,
                                                    physical_filter="dummy_r"))
        registry.addDimensionEntry("Exposure", dict(instrument="DummyCam", exposure=200, visit=20,
                                                    physical_filter="dummy_r"))
        registry.addDimensionEntry("Exposure", dict(instrument="DummyCam", exposure=201, visit=20,
                                                    physical_filter="dummy_r"))

        # dataset types
        collection1 = "test"
        collection2 = "test2"
        run = registry.makeRun(collection=collection1)
        run2 = registry.makeRun(collection=collection2)
        storageClass = StorageClass("testDataset")
        registry.storageClasses.registerStorageClass(storageClass)
        rawType = DatasetType(name="RAW", dimensions=("Instrument", "Exposure", "Detector"),
                              storageClass=storageClass)
        registry.registerDatasetType(rawType)
        calexpType = DatasetType(name="CALEXP", dimensions=("Instrument", "Visit", "Detector"),
                                 storageClass=storageClass)
        registry.registerDatasetType(calexpType)

        # add pre-existing datasets
        for exposure in (100, 101, 110, 111):
            for detector in (1, 2, 3):
                # note that only 3 of 5 detectors have datasets
                dataId = dict(instrument="DummyCam", exposure=exposure, detector=detector)
                ref = registry.addDataset(rawType, dataId=dataId, run=run)
                # Exposures 100 and 101 appear in both collections, 100 has different
                # dataset_id in different collections, for 101 only single dataset_id exists
                if exposure == 100:
                    registry.addDataset(rawType, dataId=dataId, run=run2)
                if exposure == 101:
                    registry.associate(run2.collection, [ref])
        # Add pre-existing datasets to second collection.
        for exposure in (200, 201):
            for detector in (3, 4, 5):
                # note that only 3 of 5 detectors have datasets
                dataId = dict(instrument="DummyCam", exposure=exposure, detector=detector)
                registry.addDataset(rawType, dataId=dataId, run=run2)

        # with empty expression
        originInfo = DatasetOriginInfoDef(defaultInputs=[collection1], defaultOutput=collection1)
        rows = self.preFlight.selectDimensions(originInfo=originInfo,
                                               expression="",
                                               neededDatasetTypes=[rawType],
                                               futureDatasetTypes=[calexpType])
        rows = list(rows)
        self.assertEqual(len(rows), 4*3)   # 4 exposures times 3 detectors
        for row in rows:
            self.assertCountEqual(row.dataId.keys(), ("instrument", "detector", "exposure", "visit"))
            self.assertCountEqual(row.datasetRefs.keys(), (rawType, calexpType))
            packer1 = registry.makeDataIdPacker("VisitDetector", row.dataId)
            packer2 = registry.makeDataIdPacker("ExposureDetector", row.dataId)
            self.assertEqual(packer1.unpack(packer1.pack(row.dataId)),
                             DataId(row.dataId, dimensions=packer1.dimensions.required))
            self.assertEqual(packer2.unpack(packer2.pack(row.dataId)),
                             DataId(row.dataId, dimensions=packer2.dimensions.required))
            self.assertNotEqual(packer1.pack(row.dataId), packer2.pack(row.dataId))
        self.assertCountEqual(set(row.dataId["exposure"] for row in rows),
                              (100, 101, 110, 111))
        self.assertCountEqual(set(row.dataId["visit"] for row in rows), (10, 11))
        self.assertCountEqual(set(row.dataId["detector"] for row in rows), (1, 2, 3))

        # second collection
        originInfo = DatasetOriginInfoDef(defaultInputs=[collection2], defaultOutput=collection1)
        rows = self.preFlight.selectDimensions(originInfo=originInfo,
                                               expression="",
                                               neededDatasetTypes=[rawType],
                                               futureDatasetTypes=[calexpType])
        rows = list(rows)
        self.assertEqual(len(rows), 4*3)   # 4 exposures times 3 detectors
        for row in rows:
            self.assertCountEqual(row.dataId.keys(), ("instrument", "detector", "exposure", "visit"))
            self.assertCountEqual(row.datasetRefs.keys(), (rawType, calexpType))
        self.assertCountEqual(set(row.dataId["exposure"] for row in rows),
                              (100, 101, 200, 201))
        self.assertCountEqual(set(row.dataId["visit"] for row in rows), (10, 20))
        self.assertCountEqual(set(row.dataId["detector"] for row in rows), (1, 2, 3, 4, 5))

        # with two input datasets
        originInfo = DatasetOriginInfoDef(defaultInputs=[collection1, collection2], defaultOutput=collection2)
        rows = self.preFlight.selectDimensions(originInfo=originInfo,
                                               expression="",
                                               neededDatasetTypes=[rawType],
                                               futureDatasetTypes=[calexpType])
        rows = list(rows)
        self.assertEqual(len(rows), 6*3)   # 6 exposures times 3 detectors
        for row in rows:
            self.assertCountEqual(row.dataId.keys(), ("instrument", "detector", "exposure", "visit"))
            self.assertCountEqual(row.datasetRefs.keys(), (rawType, calexpType))
        self.assertCountEqual(set(row.dataId["exposure"] for row in rows),
                              (100, 101, 110, 111, 200, 201))
        self.assertCountEqual(set(row.dataId["visit"] for row in rows), (10, 11, 20))
        self.assertCountEqual(set(row.dataId["detector"] for row in rows), (1, 2, 3, 4, 5))

        # limit to single visit
        originInfo = DatasetOriginInfoDef(defaultInputs=[collection1], defaultOutput=None)
        rows = self.preFlight.selectDimensions(originInfo=originInfo,
                                               expression="Visit.visit = 10",
                                               neededDatasetTypes=[rawType],
                                               futureDatasetTypes=[calexpType])
        rows = list(rows)
        self.assertEqual(len(rows), 2*3)   # 2 exposures times 3 detectors
        self.assertCountEqual(set(row.dataId["exposure"] for row in rows), (100, 101))
        self.assertCountEqual(set(row.dataId["visit"] for row in rows), (10,))
        self.assertCountEqual(set(row.dataId["detector"] for row in rows), (1, 2, 3))

        # more limiting expression
        originInfo = DatasetOriginInfoDef(defaultInputs=[collection1], defaultOutput="")
        rows = self.preFlight.selectDimensions(originInfo=originInfo,
                                               expression="Visit.visit = 10 and Detector.detector > 1",
                                               neededDatasetTypes=[rawType],
                                               futureDatasetTypes=[calexpType])
        rows = list(rows)
        self.assertEqual(len(rows), 2*2)   # 2 exposures times 2 detectors
        self.assertCountEqual(set(row.dataId["exposure"] for row in rows), (100, 101))
        self.assertCountEqual(set(row.dataId["visit"] for row in rows), (10,))
        self.assertCountEqual(set(row.dataId["detector"] for row in rows), (2, 3))

        # expression excludes everyhting
        rows = self.preFlight.selectDimensions(originInfo=originInfo,
                                               expression="Visit.visit > 1000",
                                               neededDatasetTypes=[rawType],
                                               futureDatasetTypes=[calexpType])
        rows = list(rows)
        self.assertEqual(len(rows), 0)

        # Selecting by PhysicalFilter, this is not in the units, but it is
        # a part of the full expression so it should work too.
        rows = self.preFlight.selectDimensions(originInfo=originInfo,
                                               expression="PhysicalFilter.physical_filter = 'dummy_r'",
                                               neededDatasetTypes=[rawType],
                                               futureDatasetTypes=[calexpType])
        rows = list(rows)
        self.assertEqual(len(rows), 2*3)   # 2 exposures times 3 detectors
        self.assertCountEqual(set(row.dataId["exposure"] for row in rows), (110, 111))
        self.assertCountEqual(set(row.dataId["visit"] for row in rows), (11,))
        self.assertCountEqual(set(row.dataId["detector"] for row in rows), (1, 2, 3))

    def testPreFlightExposureRange(self):
        """Test involving only ExposureRange unit"""
        registry = self.registry

        # need a bunch of units and datasets for test
        registry.addDimensionEntry("Instrument", dict(instrument="DummyCam"))
        registry.addDimensionEntry("PhysicalFilter", dict(instrument="DummyCam",
                                                          physical_filter="dummy_r",
                                                          abstract_filter="r"))
        for detector in (1, 2, 3, 4, 5):
            registry.addDimensionEntry("Detector", dict(instrument="DummyCam", detector=detector))

        # make few visits/exposures
        now = datetime.now()
        timestamps = []       # list of start/end time of each exposure
        for visit in (10, 11, 20):
            registry.addDimensionEntry("Visit",
                                       dict(instrument="DummyCam", visit=visit, physical_filter="dummy_r"))
            visit_start = now + timedelta(seconds=visit*45)
            for exposure in (0, 1):
                start = visit_start + timedelta(seconds=15*exposure)
                end = start + timedelta(seconds=15)
                registry.addDimensionEntry("Exposure", dict(instrument="DummyCam",
                                                            exposure=visit*10+exposure,
                                                            visit=visit,
                                                            physical_filter="dummy_r",
                                                            datetime_begin=start,
                                                            datetime_end=end))
                timestamps += [(start, end)]
        self.assertEqual(len(timestamps), 6)

        # dataset types
        collection = "test"
        run = registry.makeRun(collection=collection)
        storageClass = StorageClass("testExposureRange")
        registry.storageClasses.registerStorageClass(storageClass)
        rawType = DatasetType(name="RAW", dimensions=("Instrument", "Detector", "Exposure"),
                              storageClass=storageClass)
        registry.registerDatasetType(rawType)
        biasType = DatasetType(name="bias", dimensions=("Instrument", "Detector", "ExposureRange"),
                               storageClass=storageClass)
        registry.registerDatasetType(biasType)
        flatType = DatasetType(name="flat",
                               dimensions=("Instrument", "Detector", "PhysicalFilter", "ExposureRange"),
                               storageClass=storageClass)
        registry.registerDatasetType(flatType)
        calexpType = DatasetType(name="CALEXP", dimensions=("Instrument", "Visit", "Detector"),
                                 storageClass=storageClass)
        registry.registerDatasetType(calexpType)

        # add pre-existing raw datasets
        for visit in (10, 11, 20):
            for exposure in (0, 1):
                for detector in (1, 2, 3, 4, 5):
                    dataId = dict(instrument="DummyCam", exposure=visit*10+exposure, detector=detector)
                    registry.addDataset(rawType, dataId=dataId, run=run)

        # add few bias datasets
        for detector in (1, 2, 3, 4, 5):
            # from before first exposure to the end of second exposure
            dataId = dict(instrument="DummyCam", detector=detector,
                          valid_first=now-timedelta(seconds=3600),
                          valid_last=timestamps[1][1])
            registry.addDataset(biasType, dataId=dataId, run=run)
            # from start of third exposure to the end of last exposure
            dataId = dict(instrument="DummyCam", detector=detector,
                          valid_first=timestamps[2][0],
                          valid_last=timestamps[-1][1])
            registry.addDataset(biasType, dataId=dataId, run=run)

        # add few flat datasets, only for subset of detectors and exposures
        for detector in (1, 2, 3):
            # third and fourth exposures
            dataId = dict(instrument="DummyCam", detector=detector,
                          physical_filter="dummy_r",
                          valid_first=timestamps[2][0],
                          valid_last=timestamps[3][1])
            registry.addDataset(flatType, dataId=dataId, run=run)
            # fifth exposure only
            dataId = dict(instrument="DummyCam", detector=detector,
                          physical_filter="dummy_r",
                          valid_first=timestamps[4][0],
                          valid_last=timestamps[5][0]-timedelta(seconds=1))
            registry.addDataset(flatType, dataId=dataId, run=run)

        # without flat/bias
        originInfo = DatasetOriginInfoDef(defaultInputs=[collection], defaultOutput=collection)
        rows = self.preFlight.selectDimensions(originInfo=originInfo,
                                               expression="",
                                               neededDatasetTypes=[rawType],
                                               futureDatasetTypes=[calexpType],
                                               expandDataIds=False)
        rows = list(rows)
        self.assertEqual(len(rows), 6*5)   # 6 exposures times 5 detectors
        for row in rows:
            self.assertCountEqual(row.dataId.keys(), ("instrument", "detector", "exposure", "visit"))
            self.assertCountEqual(row.datasetRefs.keys(), (rawType, calexpType))

        # use bias
        originInfo = DatasetOriginInfoDef(defaultInputs=[collection], defaultOutput=collection)
        rows = self.preFlight.selectDimensions(originInfo=originInfo,
                                               expression="",
                                               neededDatasetTypes=[rawType, biasType],
                                               futureDatasetTypes=[calexpType],
                                               expandDataIds=False)
        rows = list(rows)
        self.assertEqual(len(rows), 6*5)  # 6 exposures times 5 detectors
        for row in rows:
            self.assertCountEqual(row.dataId.keys(),
                                  ("instrument", "detector", "exposure", "visit"))
            self.assertCountEqual(row.datasetRefs.keys(), (rawType, biasType, calexpType))

        # use flat
        rows = self.preFlight.selectDimensions(originInfo=originInfo,
                                               expression="",
                                               neededDatasetTypes=[rawType, flatType],
                                               futureDatasetTypes=[calexpType],
                                               expandDataIds=False)
        rows = list(rows)
        self.assertEqual(len(rows), 3*3)  # 3 exposures times 3 detectors
        for row in rows:
            self.assertCountEqual(row.dataId.keys(),
                                  ("instrument", "detector", "exposure", "visit", "physical_filter"))
            self.assertCountEqual(row.datasetRefs.keys(), (rawType, flatType, calexpType))

        # use both bias and flat, plus expression
        rows = self.preFlight.selectDimensions(originInfo=originInfo,
                                               expression="Detector.detector IN (1, 3)",
                                               neededDatasetTypes=[rawType, flatType, biasType],
                                               futureDatasetTypes=[calexpType],
                                               expandDataIds=False)
        rows = list(rows)
        self.assertEqual(len(rows), 3*2)  # 3 exposures times 2 detectors
        for row in rows:
            self.assertCountEqual(row.dataId.keys(),
                                  ("instrument", "detector", "exposure", "visit", "physical_filter"))
            self.assertCountEqual(row.datasetRefs.keys(), (rawType, flatType, biasType, calexpType))

        # select single exposure (third) and detector and check datasetRefs
        rows = self.preFlight.selectDimensions(originInfo=originInfo,
                                               expression="Exposure.exposure = 110 AND Detector.detector = 1",
                                               neededDatasetTypes=[rawType, flatType, biasType],
                                               futureDatasetTypes=[calexpType],
                                               expandDataIds=False)
        rows = list(rows)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row.datasetRefs[flatType].dataId,
                         dict(instrument="DummyCam",
                              detector=1,
                              physical_filter="dummy_r",
                              valid_first=timestamps[2][0],
                              valid_last=timestamps[3][1]))
        self.assertEqual(row.datasetRefs[biasType].dataId,
                         dict(instrument="DummyCam",
                              detector=1,
                              valid_first=timestamps[2][0],
                              valid_last=timestamps[-1][1]))

    def testPreFlightSkyMapUnits(self):
        """Test involving only SkyMap units, no joins to Instrument"""
        registry = self.registry

        # need a bunch of units and datasets for test, we want "AbstractFilter"
        # in the test so also have to add PhysicalFilter units
        registry.addDimensionEntry("Instrument", dict(instrument="DummyCam"))
        registry.addDimensionEntry("PhysicalFilter", dict(instrument="DummyCam",
                                                          physical_filter="dummy_r",
                                                          abstract_filter="r"))
        registry.addDimensionEntry("PhysicalFilter", dict(instrument="DummyCam",
                                                          physical_filter="dummy_i",
                                                          abstract_filter="i"))
        registry.addDimensionEntry("SkyMap", dict(skymap="DummyMap", hash="sha!".encode("utf8")))
        for tract in range(10):
            registry.addDimensionEntry("Tract", dict(skymap="DummyMap", tract=tract))
            for patch in range(10):
                registry.addDimensionEntry("Patch", dict(skymap="DummyMap", tract=tract, patch=patch,
                                                         cell_x=0, cell_y=0,
                                                         region=Box(LonLat(NormalizedAngle(0.), Angle(0.)))))

        # dataset types
        collection = "test"
        run = registry.makeRun(collection=collection)
        storageClass = StorageClass("testDataset")
        registry.storageClasses.registerStorageClass(storageClass)
        calexpType = DatasetType(name="deepCoadd_calexp", dimensions=("SkyMap", "Tract", "Patch",
                                                                      "AbstractFilter"),
                                 storageClass=storageClass)
        registry.registerDatasetType(calexpType)
        mergeType = DatasetType(name="deepCoadd_mergeDet", dimensions=("SkyMap", "Tract", "Patch"),
                                storageClass=storageClass)
        registry.registerDatasetType(mergeType)
        measType = DatasetType(name="deepCoadd_meas", dimensions=("SkyMap", "Tract", "Patch",
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
        originInfo = DatasetOriginInfoDef(defaultInputs=[collection], defaultOutput="")
        rows = self.preFlight.selectDimensions(originInfo=originInfo,
                                               expression="",
                                               neededDatasetTypes=[calexpType, mergeType],
                                               futureDatasetTypes=[measType],
                                               expandDataIds=False)
        rows = list(rows)
        self.assertEqual(len(rows), 3*4*2)   # 4 tracts x 4 patches x 2 filters
        for row in rows:
            self.assertCountEqual(row.dataId.keys(), ("skymap", "tract", "patch", "abstract_filter"))
            self.assertCountEqual(row.datasetRefs.keys(), (calexpType, mergeType, measType))
        self.assertCountEqual(set(row.dataId["tract"] for row in rows), (1, 3, 5))
        self.assertCountEqual(set(row.dataId["patch"] for row in rows), (2, 4, 6, 7))
        self.assertCountEqual(set(row.dataId["abstract_filter"] for row in rows), ("i", "r"))

        # limit to 2 tracts and 2 patches
        rows = self.preFlight.selectDimensions(originInfo=originInfo,
                                               expression="Tract.tract IN (1, 5) AND Patch.patch IN (2, 7)",
                                               neededDatasetTypes=[calexpType, mergeType],
                                               futureDatasetTypes=[measType],
                                               expandDataIds=False)
        rows = list(rows)
        self.assertEqual(len(rows), 2*2*2)   # 4 tracts x 4 patches x 2 filters
        self.assertCountEqual(set(row.dataId["tract"] for row in rows), (1, 5))
        self.assertCountEqual(set(row.dataId["patch"] for row in rows), (2, 7))
        self.assertCountEqual(set(row.dataId["abstract_filter"] for row in rows), ("i", "r"))

        # limit to single filter
        rows = self.preFlight.selectDimensions(originInfo=originInfo,
                                               expression="AbstractFilter.abstract_filter = 'i'",
                                               neededDatasetTypes=[calexpType, mergeType],
                                               futureDatasetTypes=[measType],
                                               expandDataIds=False)
        rows = list(rows)
        self.assertEqual(len(rows), 3*4*1)   # 4 tracts x 4 patches x 2 filters
        self.assertCountEqual(set(row.dataId["tract"] for row in rows), (1, 3, 5))
        self.assertCountEqual(set(row.dataId["patch"] for row in rows), (2, 4, 6, 7))
        self.assertCountEqual(set(row.dataId["abstract_filter"] for row in rows), ("i",))

        # expression excludes everyhting, specifying non-existing skymap is not a
        # fatal error, it's operator error
        rows = self.preFlight.selectDimensions(originInfo=originInfo,
                                               expression="SkyMap.skymap = 'Mars'",
                                               neededDatasetTypes=[calexpType, mergeType],
                                               futureDatasetTypes=[measType],
                                               expandDataIds=False)
        rows = list(rows)
        self.assertEqual(len(rows), 0)

    def testPreFlightSpatialMatch(self):
        """Test involving spacial match using join tables.

        Note that realistic test needs a resonably-defined SkyPix and regions
        in registry tables which is hard to implement in this simple test.
        So we do not actually fill registry with any data and all queries will
        return empty result, but this is still useful for coverage of the code
        that generates query.
        """
        registry = self.registry

        # dataset types
        collection = "test"
        registry.makeRun(collection=collection)
        storageClass = StorageClass("testDataset")
        registry.storageClasses.registerStorageClass(storageClass)

        calexpType = DatasetType(name="CALEXP", dimensions=("Instrument", "Visit", "Detector"),
                                 storageClass=storageClass)
        registry.registerDatasetType(calexpType)

        coaddType = DatasetType(name="deepCoadd_calexp", dimensions=("SkyMap", "Tract", "Patch",
                                                                     "AbstractFilter"),
                                storageClass=storageClass)
        registry.registerDatasetType(coaddType)

        # without data this should run OK but return empty set
        originInfo = DatasetOriginInfoDef(defaultInputs=[collection], defaultOutput="")
        rows = self.preFlight.selectDimensions(originInfo=originInfo,
                                               expression="",
                                               neededDatasetTypes=[calexpType],
                                               futureDatasetTypes=[coaddType],
                                               expandDataIds=False)
        rows = list(rows)
        self.assertEqual(len(rows), 0)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()

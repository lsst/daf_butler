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

from lsst.daf.butler import (ButlerConfig, DatasetType, Registry, DataId,
                             DatasetOriginInfoDef, StorageClass)
from lsst.daf.butler.sql import MultipleDatasetQueryBuilder
from lsst.sphgeom import Angle, Box, LonLat, NormalizedAngle


class QueryBuilderTestCase(unittest.TestCase):
    """Tests for QueryBuilders.
    """

    DEFER = False

    def setUp(self):
        self.testDir = os.path.dirname(__file__)
        self.configFile = os.path.join(self.testDir, "config/basic/butler.yaml")
        self.butlerConfig = ButlerConfig(self.configFile)
        self.registry = Registry.fromConfig(self.butlerConfig)

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

    def testInstrumentDimensions(self):
        """Test involving only instrument dimensions, no joins to skymap"""
        registry = self.registry

        # need a bunch of dimensions and datasets for test
        registry.addDimensionEntry("instrument", dict(instrument="DummyCam", visit_max=25, exposure_max=300,
                                                      detector_max=6))
        registry.addDimensionEntry("physical_filter", dict(instrument="DummyCam",
                                                           physical_filter="dummy_r",
                                                           abstract_filter="r"))
        registry.addDimensionEntry("physical_filter", dict(instrument="DummyCam",
                                                           physical_filter="dummy_i",
                                                           abstract_filter="i"))
        for detector in (1, 2, 3, 4, 5):
            registry.addDimensionEntry("detector", dict(instrument="DummyCam", detector=detector))
        registry.addDimensionEntry("visit", dict(instrument="DummyCam", visit=10, physical_filter="dummy_i"))
        registry.addDimensionEntry("visit", dict(instrument="DummyCam", visit=11, physical_filter="dummy_r"))
        registry.addDimensionEntry("visit", dict(instrument="DummyCam", visit=20, physical_filter="dummy_r"))
        registry.addDimensionEntry("exposure", dict(instrument="DummyCam", exposure=100, visit=10,
                                                    physical_filter="dummy_i"))
        registry.addDimensionEntry("exposure", dict(instrument="DummyCam", exposure=101, visit=10,
                                                    physical_filter="dummy_i"))
        registry.addDimensionEntry("exposure", dict(instrument="DummyCam", exposure=110, visit=11,
                                                    physical_filter="dummy_r"))
        registry.addDimensionEntry("exposure", dict(instrument="DummyCam", exposure=111, visit=11,
                                                    physical_filter="dummy_r"))
        registry.addDimensionEntry("exposure", dict(instrument="DummyCam", exposure=200, visit=20,
                                                    physical_filter="dummy_r"))
        registry.addDimensionEntry("exposure", dict(instrument="DummyCam", exposure=201, visit=20,
                                                    physical_filter="dummy_r"))

        # dataset types
        collection1 = "test"
        collection2 = "test2"
        run = registry.makeRun(collection=collection1)
        run2 = registry.makeRun(collection=collection2)
        storageClass = StorageClass("testDataset")
        registry.storageClasses.registerStorageClass(storageClass)
        rawType = DatasetType(name="RAW", dimensions=("instrument", "exposure", "detector"),
                              storageClass=storageClass)
        registry.registerDatasetType(rawType)
        calexpType = DatasetType(name="CALEXP", dimensions=("instrument", "visit", "detector"),
                                 storageClass=storageClass)
        registry.registerDatasetType(calexpType)

        # add pre-existing datasets
        for exposure in (100, 101, 110, 111):
            for detector in (1, 2, 3):
                # note that only 3 of 5 detectors have datasets
                dataId = dict(instrument="DummyCam", exposure=exposure, detector=detector)
                ref = registry.addDataset(rawType, dataId=dataId, run=run)
                # exposures 100 and 101 appear in both collections, 100 has
                # different dataset_id in different collections, for 101 only
                # single dataset_id exists
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
        builder = MultipleDatasetQueryBuilder.fromDatasetTypes(
            self.registry,
            originInfo=originInfo,
            required=[rawType],
            optional=[calexpType],
            defer=self.DEFER
        )
        rows = list(builder.execute())
        self.assertEqual(len(rows), 4*3)   # 4 exposures times 3 detectors
        for row in rows:
            self.assertCountEqual(row.dataId.keys(), ("instrument", "detector", "exposure", "visit",
                                                      "physical_filter", "abstract_filter"))
            self.assertCountEqual(row.datasetRefs.keys(), (rawType, calexpType))
            packer1 = registry.makeDataIdPacker("visit_detector", row.dataId)
            packer2 = registry.makeDataIdPacker("exposure_detector", row.dataId)
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
        builder = MultipleDatasetQueryBuilder.fromDatasetTypes(
            self.registry,
            originInfo=originInfo,
            required=[rawType],
            optional=[calexpType],
            defer=self.DEFER
        )
        rows = list(builder.execute())
        self.assertEqual(len(rows), 4*3)   # 4 exposures times 3 detectors
        for row in rows:
            self.assertCountEqual(row.dataId.keys(), ("instrument", "detector", "exposure", "visit",
                                                      "physical_filter", "abstract_filter"))
            self.assertCountEqual(row.datasetRefs.keys(), (rawType, calexpType))
        self.assertCountEqual(set(row.dataId["exposure"] for row in rows),
                              (100, 101, 200, 201))
        self.assertCountEqual(set(row.dataId["visit"] for row in rows), (10, 20))
        self.assertCountEqual(set(row.dataId["detector"] for row in rows), (1, 2, 3, 4, 5))

        # with two input datasets
        originInfo = DatasetOriginInfoDef(defaultInputs=[collection1, collection2], defaultOutput=collection2)
        builder = MultipleDatasetQueryBuilder.fromDatasetTypes(
            self.registry,
            originInfo=originInfo,
            required=[rawType],
            optional=[calexpType],
            defer=self.DEFER
        )
        rows = list(builder.execute())
        self.assertEqual(len(rows), 6*3)   # 6 exposures times 3 detectors
        for row in rows:
            self.assertCountEqual(row.dataId.keys(), ("instrument", "detector", "exposure", "visit",
                                                      "physical_filter", "abstract_filter"))
            self.assertCountEqual(row.datasetRefs.keys(), (rawType, calexpType))
        self.assertCountEqual(set(row.dataId["exposure"] for row in rows),
                              (100, 101, 110, 111, 200, 201))
        self.assertCountEqual(set(row.dataId["visit"] for row in rows), (10, 11, 20))
        self.assertCountEqual(set(row.dataId["detector"] for row in rows), (1, 2, 3, 4, 5))

        # limit to single visit
        originInfo = DatasetOriginInfoDef(defaultInputs=[collection1], defaultOutput=None)
        builder = MultipleDatasetQueryBuilder.fromDatasetTypes(
            self.registry,
            originInfo=originInfo,
            required=[rawType],
            optional=[calexpType],
            defer=self.DEFER
        )
        builder.whereParsedExpression("visit.visit = 10")
        rows = list(builder.execute())
        self.assertEqual(len(rows), 2*3)   # 2 exposures times 3 detectors
        self.assertCountEqual(set(row.dataId["exposure"] for row in rows), (100, 101))
        self.assertCountEqual(set(row.dataId["visit"] for row in rows), (10,))
        self.assertCountEqual(set(row.dataId["detector"] for row in rows), (1, 2, 3))

        # more limiting expression, using link names instead of Table.column
        originInfo = DatasetOriginInfoDef(defaultInputs=[collection1], defaultOutput="")
        builder = MultipleDatasetQueryBuilder.fromDatasetTypes(
            self.registry,
            originInfo=originInfo,
            required=[rawType],
            optional=[calexpType],
            defer=self.DEFER
        )
        builder.whereParsedExpression("visit = 10 and detector > 1")
        rows = list(builder.execute())
        self.assertEqual(len(rows), 2*2)   # 2 exposures times 2 detectors
        self.assertCountEqual(set(row.dataId["exposure"] for row in rows), (100, 101))
        self.assertCountEqual(set(row.dataId["visit"] for row in rows), (10,))
        self.assertCountEqual(set(row.dataId["detector"] for row in rows), (2, 3))

        # expression excludes everything
        builder = MultipleDatasetQueryBuilder.fromDatasetTypes(
            self.registry,
            originInfo=originInfo,
            required=[rawType],
            optional=[calexpType],
            defer=self.DEFER
        )
        builder.whereParsedExpression("visit.visit > 1000")
        rows = list(builder.execute())
        self.assertEqual(len(rows), 0)

        # Selecting by physical_filter, this is not in the dimensions, but it
        # is a part of the full expression so it should work too.
        builder = MultipleDatasetQueryBuilder.fromDatasetTypes(
            self.registry,
            originInfo=originInfo,
            required=[rawType],
            optional=[calexpType],
            defer=self.DEFER
        )
        builder.whereParsedExpression("physical_filter = 'dummy_r'")
        rows = list(builder.execute())
        self.assertEqual(len(rows), 2*3)   # 2 exposures times 3 detectors
        self.assertCountEqual(set(row.dataId["exposure"] for row in rows), (110, 111))
        self.assertCountEqual(set(row.dataId["visit"] for row in rows), (11,))
        self.assertCountEqual(set(row.dataId["detector"] for row in rows), (1, 2, 3))

    def testCalibrationLabel(self):
        """Test the calibration_label dimension and the
        perDatasetTypeDimensions option to `Registry.selectDimensions`.
        """
        registry = self.registry

        # need a bunch of dimensions and datasets for test
        registry.addDimensionEntry("instrument", dict(instrument="DummyCam"))
        registry.addDimensionEntry("physical_filter", dict(instrument="DummyCam",
                                                           physical_filter="dummy_r",
                                                           abstract_filter="r"))
        for detector in (1, 2, 3, 4, 5):
            registry.addDimensionEntry("detector", dict(instrument="DummyCam", detector=detector))

        # make few visits/exposures
        now = datetime.now()
        timestamps = []       # list of start/end time of each exposure
        for visit in (10, 11, 20):
            registry.addDimensionEntry("visit",
                                       dict(instrument="DummyCam", visit=visit, physical_filter="dummy_r"))
            visit_start = now + timedelta(seconds=visit*45)
            for exposure in (0, 1):
                start = visit_start + timedelta(seconds=20*exposure)
                end = start + timedelta(seconds=15)
                registry.addDimensionEntry("exposure", dict(instrument="DummyCam",
                                                            exposure=visit*10+exposure,
                                                            visit=visit,
                                                            physical_filter="dummy_r",
                                                            datetime_begin=start,
                                                            datetime_end=end))
                timestamps += [(start, end)]
        self.assertEqual(len(timestamps), 6)

        # Add calibration_label dimension entries, with various date ranges.
        # The string calibration_labels are intended to be descriptive for
        # the purposes of this test, by showing ranges of exposure numbers,
        # and do not represent what we expect real-life calibration_labels
        # to look like.

        # From before first exposure (100) to the end of second exposure (101).
        registry.addDimensionEntry("calibration_label",
                                   dict(instrument="DummyCam", calibration_label="..101",
                                        valid_first=now-timedelta(seconds=3600),
                                        valid_last=timestamps[1][1]))
        # From start of third exposure (110) to the end of last exposure (201).
        registry.addDimensionEntry("calibration_label",
                                   dict(instrument="DummyCam", calibration_label="110..201",
                                        valid_first=timestamps[2][0],
                                        valid_last=timestamps[-1][1]))
        # Third (110) and fourth (111) exposures.
        registry.addDimensionEntry("calibration_label",
                                   dict(instrument="DummyCam", calibration_label="110..111",
                                        valid_first=timestamps[2][0],
                                        valid_last=timestamps[3][1]))
        # Fifth exposure (200) only.
        registry.addDimensionEntry("calibration_label",
                                   dict(instrument="DummyCam", calibration_label="200..200",
                                        valid_first=timestamps[4][0],
                                        valid_last=timestamps[5][0]-timedelta(seconds=1)))

        # dataset types
        collection = "test"
        run = registry.makeRun(collection=collection)
        storageClass = StorageClass("testExposureRange")
        registry.storageClasses.registerStorageClass(storageClass)
        rawType = DatasetType(name="RAW", dimensions=("instrument", "detector", "exposure"),
                              storageClass=storageClass)
        registry.registerDatasetType(rawType)
        biasType = DatasetType(name="BIAS", dimensions=("instrument", "detector", "calibration_label"),
                               storageClass=storageClass)
        registry.registerDatasetType(biasType)
        flatType = DatasetType(name="FLAT",
                               dimensions=("instrument", "detector", "physical_filter", "calibration_label"),
                               storageClass=storageClass)
        registry.registerDatasetType(flatType)
        calexpType = DatasetType(name="CALEXP", dimensions=("instrument", "visit", "detector"),
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
            dataId = dict(instrument="DummyCam", detector=detector, calibration_label="..101")
            registry.addDataset(biasType, dataId=dataId, run=run)
            dataId = dict(instrument="DummyCam", detector=detector, calibration_label="110..201")
            registry.addDataset(biasType, dataId=dataId, run=run)

        # add few flat datasets, only for subset of detectors and exposures
        for detector in (1, 2, 3):
            dataId = dict(instrument="DummyCam", detector=detector,
                          physical_filter="dummy_r", calibration_label="110..111")
            registry.addDataset(flatType, dataId=dataId, run=run)
            dataId = dict(instrument="DummyCam", detector=detector,
                          physical_filter="dummy_r", calibration_label="200..200")
            registry.addDataset(flatType, dataId=dataId, run=run)

        # without flat/bias
        originInfo = DatasetOriginInfoDef(defaultInputs=[collection], defaultOutput=collection)
        builder = MultipleDatasetQueryBuilder.fromDatasetTypes(
            self.registry,
            originInfo=originInfo,
            required=[rawType],
            optional=[calexpType],
            perDatasetTypeDimensions=["calibration_label"],
            defer=self.DEFER
        )
        rows = list(builder.execute())
        self.assertEqual(len(rows), 6*5)   # 6 exposures times 5 detectors
        for row in rows:
            self.assertCountEqual(row.dataId.keys(), ("instrument", "detector", "exposure", "visit",
                                                      "physical_filter", "abstract_filter"))
            self.assertCountEqual(row.datasetRefs.keys(), (rawType, calexpType))

        # use bias; we have biases for all raws, so no expression is needed
        originInfo = DatasetOriginInfoDef(defaultInputs=[collection], defaultOutput=collection)
        builder = MultipleDatasetQueryBuilder.fromDatasetTypes(
            self.registry,
            originInfo=originInfo,
            required=[rawType],
            optional=[calexpType],
            prerequisite=[biasType],
            perDatasetTypeDimensions=["calibration_label"],
            defer=self.DEFER
        )
        rows = list(builder.execute())
        self.assertEqual(len(rows), 6*5)  # 6 exposures times 5 detectors
        for row in rows:
            self.assertCountEqual(row.dataId.keys(),
                                  ("instrument", "detector", "exposure", "visit",
                                   "physical_filter", "abstract_filter"))
            self.assertCountEqual(row.datasetRefs.keys(), (rawType, biasType, calexpType))

        # use flat; we lack a flat for some exposures, so this should raise
        builder = MultipleDatasetQueryBuilder.fromDatasetTypes(
            self.registry,
            originInfo=originInfo,
            required=[rawType],
            optional=[calexpType],
            prerequisite=[flatType],
            perDatasetTypeDimensions=["calibration_label"],
            defer=self.DEFER
        )
        with self.assertRaises(LookupError):
            rows = list(builder.execute())

        # use flat, but as a required dataset type instead of prerequisite -
        # this should automatically filter results
        builder = MultipleDatasetQueryBuilder.fromDatasetTypes(
            self.registry,
            originInfo=originInfo,
            required=[rawType, flatType],
            optional=[calexpType],
            perDatasetTypeDimensions=["calibration_label"],
            defer=self.DEFER
        )
        rows = list(builder.execute())
        self.assertEqual(len(rows), 3*3)  # 3 exposures times 3 detectors
        for row in rows:
            self.assertCountEqual(row.dataId.keys(),
                                  ("instrument", "detector", "exposure", "visit",
                                   "physical_filter", "abstract_filter"))
            self.assertCountEqual(row.datasetRefs.keys(), (rawType, flatType, calexpType))

        # use both bias and flat, plus expression
        builder = MultipleDatasetQueryBuilder.fromDatasetTypes(
            self.registry,
            originInfo=originInfo,
            required=[rawType],
            optional=[calexpType],
            prerequisite=[flatType, biasType],
            perDatasetTypeDimensions=["calibration_label"],
            defer=self.DEFER
        )
        builder.whereParsedExpression("detector IN (1, 3) AND exposure NOT IN (100, 101, 201)")
        rows = list(builder.execute())
        self.assertEqual(len(rows), 3*2)  # 3 exposures times 2 detectors
        for row in rows:
            self.assertCountEqual(row.dataId.keys(),
                                  ("instrument", "detector", "exposure", "visit",
                                   "physical_filter", "abstract_filter"))
            self.assertCountEqual(row.datasetRefs.keys(), (rawType, flatType, biasType, calexpType))

        # select single exposure (third) and detector and check datasetRefs
        builder = MultipleDatasetQueryBuilder.fromDatasetTypes(
            self.registry,
            originInfo=originInfo,
            required=[rawType],
            optional=[calexpType],
            prerequisite=[flatType, biasType],
            perDatasetTypeDimensions=["calibration_label"],
            defer=self.DEFER
        )
        builder.whereParsedExpression("exposure.exposure = 110 AND detector.detector = 1")
        rows = list(builder.execute())
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row.datasetRefs[flatType].dataId,
                         dict(instrument="DummyCam",
                              detector=1,
                              physical_filter="dummy_r",
                              calibration_label="110..111"))
        self.assertEqual(row.datasetRefs[biasType].dataId,
                         dict(instrument="DummyCam",
                              detector=1,
                              calibration_label="110..201"))

    def testSkyMapDimensions(self):
        """Test involving only skymap dimensions, no joins to instrument"""
        registry = self.registry

        # need a bunch of dimensions and datasets for test, we want
        # "abstract_filter" in the test so also have to add physical_filter
        # dimensions
        registry.addDimensionEntry("instrument", dict(instrument="DummyCam"))
        registry.addDimensionEntry("physical_filter", dict(instrument="DummyCam",
                                                           physical_filter="dummy_r",
                                                           abstract_filter="r"))
        registry.addDimensionEntry("physical_filter", dict(instrument="DummyCam",
                                                           physical_filter="dummy_i",
                                                           abstract_filter="i"))
        registry.addDimensionEntry("skymap", dict(skymap="DummyMap", hash="sha!".encode("utf8")))
        for tract in range(10):
            registry.addDimensionEntry("tract", dict(skymap="DummyMap", tract=tract))
            for patch in range(10):
                registry.addDimensionEntry("patch", dict(skymap="DummyMap", tract=tract, patch=patch,
                                                         cell_x=0, cell_y=0,
                                                         region=Box(LonLat(NormalizedAngle(0.), Angle(0.)))))

        # dataset types
        collection = "test"
        run = registry.makeRun(collection=collection)
        storageClass = StorageClass("testDataset")
        registry.storageClasses.registerStorageClass(storageClass)
        calexpType = DatasetType(name="deepCoadd_calexp", dimensions=("skymap", "tract", "patch",
                                                                      "abstract_filter"),
                                 storageClass=storageClass)
        registry.registerDatasetType(calexpType)
        mergeType = DatasetType(name="deepCoadd_mergeDet", dimensions=("skymap", "tract", "patch"),
                                storageClass=storageClass)
        registry.registerDatasetType(mergeType)
        measType = DatasetType(name="deepCoadd_meas", dimensions=("skymap", "tract", "patch",
                                                                  "abstract_filter"),
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
        builder = MultipleDatasetQueryBuilder.fromDatasetTypes(
            self.registry,
            originInfo=originInfo,
            required=[calexpType, mergeType],
            optional=[measType],
            defer=self.DEFER
        )
        rows = list(builder.execute())
        self.assertEqual(len(rows), 3*4*2)   # 4 tracts x 4 patches x 2 filters
        for row in rows:
            self.assertCountEqual(row.dataId.keys(), ("skymap", "tract", "patch", "abstract_filter"))
            self.assertCountEqual(row.datasetRefs.keys(), (calexpType, mergeType, measType))
        self.assertCountEqual(set(row.dataId["tract"] for row in rows), (1, 3, 5))
        self.assertCountEqual(set(row.dataId["patch"] for row in rows), (2, 4, 6, 7))
        self.assertCountEqual(set(row.dataId["abstract_filter"] for row in rows), ("i", "r"))

        # limit to 2 tracts and 2 patches
        builder = MultipleDatasetQueryBuilder.fromDatasetTypes(
            self.registry,
            originInfo=originInfo,
            required=[calexpType, mergeType],
            optional=[measType],
            defer=self.DEFER,
        )
        builder.whereParsedExpression("tract IN (1, 5) AND patch.patch IN (2, 7)")
        rows = list(builder.execute())
        self.assertEqual(len(rows), 2*2*2)   # 4 tracts x 4 patches x 2 filters
        self.assertCountEqual(set(row.dataId["tract"] for row in rows), (1, 5))
        self.assertCountEqual(set(row.dataId["patch"] for row in rows), (2, 7))
        self.assertCountEqual(set(row.dataId["abstract_filter"] for row in rows), ("i", "r"))

        # limit to single filter
        builder = MultipleDatasetQueryBuilder.fromDatasetTypes(
            self.registry,
            originInfo=originInfo,
            required=[calexpType, mergeType],
            optional=[measType],
        )
        builder.whereParsedExpression("abstract_filter = 'i'")
        rows = list(builder.execute())
        self.assertEqual(len(rows), 3*4*1)   # 4 tracts x 4 patches x 2 filters
        self.assertCountEqual(set(row.dataId["tract"] for row in rows), (1, 3, 5))
        self.assertCountEqual(set(row.dataId["patch"] for row in rows), (2, 4, 6, 7))
        self.assertCountEqual(set(row.dataId["abstract_filter"] for row in rows), ("i",))

        # expression excludes everything, specifying non-existing skymap is
        # not a fatal error, it's operator error
        builder = MultipleDatasetQueryBuilder.fromDatasetTypes(
            self.registry,
            originInfo=originInfo,
            required=[calexpType, mergeType],
            optional=[measType],
            defer=self.DEFER
        )
        builder.whereParsedExpression("skymap = 'Mars'")
        rows = list(builder.execute())
        self.assertEqual(len(rows), 0)

    def testSpatialMatch(self):
        """Test involving spatial match using join tables.

        Note that realistic test needs a resonably-defined skypix and regions
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

        calexpType = DatasetType(name="CALEXP", dimensions=("instrument", "visit", "detector"),
                                 storageClass=storageClass)
        registry.registerDatasetType(calexpType)

        coaddType = DatasetType(name="deepCoadd_calexp", dimensions=("skymap", "tract", "patch",
                                                                     "abstract_filter"),
                                storageClass=storageClass)
        registry.registerDatasetType(coaddType)

        # without data this should run OK but return empty set
        originInfo = DatasetOriginInfoDef(defaultInputs=[collection], defaultOutput="")
        builder = MultipleDatasetQueryBuilder.fromDatasetTypes(
            self.registry,
            originInfo=originInfo,
            required=[calexpType],
            optional=[coaddType],
            defer=self.DEFER
        )
        rows = list(builder.execute())
        self.assertEqual(len(rows), 0)


class QueryBuilderDeferralTestCase(QueryBuilderTestCase):
    """Trivial subclass of `QueryBuilderTestCase` that runs all tests with
    the ``defer`` option of `MultipleDatasetQueryBuilder.fromDatasetTypes`
    set to `True`.
    """
    DEFER = True


if __name__ == "__main__":
    unittest.main()

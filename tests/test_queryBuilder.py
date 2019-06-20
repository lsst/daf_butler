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

from lsst.daf.butler import (ButlerConfig, DatasetType, Registry, DataId,
                             DatasetOriginInfoDef, StorageClass)
from lsst.daf.butler.sql import DataIdQueryBuilder, SingleDatasetQueryBuilder
from lsst.sphgeom import Angle, Box, LonLat, NormalizedAngle


class QueryBuilderTestCase(unittest.TestCase):
    """Tests for QueryBuilders.
    """

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
        rawType = DatasetType(name="RAW",
                              dimensions=registry.dimensions.extract(("instrument", "exposure", "detector")),
                              storageClass=storageClass)
        registry.registerDatasetType(rawType)
        calexpType = DatasetType(name="CALEXP",
                                 dimensions=registry.dimensions.extract(("instrument", "visit", "detector")),
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

        dimensions = registry.dimensions.empty.union(rawType.dimensions, calexpType.dimensions,
                                                     implied=True)

        # with empty expression
        builder = DataIdQueryBuilder.fromDimensions(registry, dimensions)
        builder.requireDataset(rawType, collections=[collection1])
        rows = list(builder.execute())
        self.assertEqual(len(rows), 4*3)   # 4 exposures times 3 detectors
        for dataId in rows:
            self.assertCountEqual(dataId.keys(), ("instrument", "detector", "exposure", "visit",
                                                  "physical_filter", "abstract_filter"))
            packer1 = registry.makeDataIdPacker("visit_detector", dataId)
            packer2 = registry.makeDataIdPacker("exposure_detector", dataId)
            self.assertEqual(packer1.unpack(packer1.pack(dataId)),
                             DataId(dataId, dimensions=packer1.dimensions.required))
            self.assertEqual(packer2.unpack(packer2.pack(dataId)),
                             DataId(dataId, dimensions=packer2.dimensions.required))
            self.assertNotEqual(packer1.pack(dataId), packer2.pack(dataId))
        self.assertCountEqual(set(dataId["exposure"] for dataId in rows),
                              (100, 101, 110, 111))
        self.assertCountEqual(set(dataId["visit"] for dataId in rows), (10, 11))
        self.assertCountEqual(set(dataId["detector"] for dataId in rows), (1, 2, 3))

        # second collection
        builder = DataIdQueryBuilder.fromDimensions(registry, dimensions)
        builder.requireDataset(rawType, collections=[collection2])
        rows = list(builder.execute())
        self.assertEqual(len(rows), 4*3)   # 4 exposures times 3 detectors
        for dataId in rows:
            self.assertCountEqual(dataId.keys(), ("instrument", "detector", "exposure", "visit",
                                                  "physical_filter", "abstract_filter"))
        self.assertCountEqual(set(dataId["exposure"] for dataId in rows),
                              (100, 101, 200, 201))
        self.assertCountEqual(set(dataId["visit"] for dataId in rows), (10, 20))
        self.assertCountEqual(set(dataId["detector"] for dataId in rows), (1, 2, 3, 4, 5))

        # with two input datasets
        builder = DataIdQueryBuilder.fromDimensions(registry, dimensions)
        builder.requireDataset(rawType, collections=[collection1, collection2])
        rows = list(builder.execute())
        self.assertEqual(len(set(rows)), 6*3)   # 6 exposures times 3 detectors; set needed to de-dupe
        for dataId in rows:
            self.assertCountEqual(dataId.keys(), ("instrument", "detector", "exposure", "visit",
                                                  "physical_filter", "abstract_filter"))
        self.assertCountEqual(set(dataId["exposure"] for dataId in rows),
                              (100, 101, 110, 111, 200, 201))
        self.assertCountEqual(set(dataId["visit"] for dataId in rows), (10, 11, 20))
        self.assertCountEqual(set(dataId["detector"] for dataId in rows), (1, 2, 3, 4, 5))

        # limit to single visit
        builder = DataIdQueryBuilder.fromDimensions(registry, dimensions)
        builder.requireDataset(rawType, collections=[collection1])
        builder.whereParsedExpression("visit.visit = 10")
        rows = list(builder.execute())
        self.assertEqual(len(rows), 2*3)   # 2 exposures times 3 detectors
        self.assertCountEqual(set(dataId["exposure"] for dataId in rows), (100, 101))
        self.assertCountEqual(set(dataId["visit"] for dataId in rows), (10,))
        self.assertCountEqual(set(dataId["detector"] for dataId in rows), (1, 2, 3))

        # more limiting expression, using link names instead of Table.column
        builder = DataIdQueryBuilder.fromDimensions(registry, dimensions)
        builder.requireDataset(rawType, collections=[collection1])
        builder.whereParsedExpression("visit = 10 and detector > 1")
        rows = list(builder.execute())
        self.assertEqual(len(rows), 2*2)   # 2 exposures times 2 detectors
        self.assertCountEqual(set(dataId["exposure"] for dataId in rows), (100, 101))
        self.assertCountEqual(set(dataId["visit"] for dataId in rows), (10,))
        self.assertCountEqual(set(dataId["detector"] for dataId in rows), (2, 3))

        # expression excludes everything
        builder = DataIdQueryBuilder.fromDimensions(registry, dimensions)
        builder.requireDataset(rawType, collections=[collection1])
        builder.whereParsedExpression("visit.visit > 1000")
        rows = list(builder.execute())
        self.assertEqual(len(rows), 0)

        # Selecting by physical_filter, this is not in the dimensions, but it
        # is a part of the full expression so it should work too.
        builder = DataIdQueryBuilder.fromDimensions(registry, dimensions)
        builder.requireDataset(rawType, collections=[collection1])
        builder.whereParsedExpression("physical_filter = 'dummy_r'")
        rows = list(builder.execute())
        self.assertEqual(len(rows), 2*3)   # 2 exposures times 3 detectors
        self.assertCountEqual(set(dataId["exposure"] for dataId in rows), (110, 111))
        self.assertCountEqual(set(dataId["visit"] for dataId in rows), (11,))
        self.assertCountEqual(set(dataId["detector"] for dataId in rows), (1, 2, 3))

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
        calexpType = DatasetType(name="deepCoadd_calexp",
                                 dimensions=registry.dimensions.extract(("skymap", "tract", "patch",
                                                                         "abstract_filter")),
                                 storageClass=storageClass)
        registry.registerDatasetType(calexpType)
        mergeType = DatasetType(name="deepCoadd_mergeDet",
                                dimensions=registry.dimensions.extract(("skymap", "tract", "patch")),
                                storageClass=storageClass)
        registry.registerDatasetType(mergeType)
        measType = DatasetType(name="deepCoadd_meas",
                               dimensions=registry.dimensions.extract(("skymap", "tract", "patch",
                                                                       "abstract_filter")),
                               storageClass=storageClass)
        registry.registerDatasetType(measType)

        dimensions = registry.dimensions.empty.union(calexpType.dimensions, mergeType.dimensions,
                                                     measType.dimensions, implied=True)

        # add pre-existing datasets
        for tract in (1, 3, 5):
            for patch in (2, 4, 6, 7):
                dataId = dict(skymap="DummyMap", tract=tract, patch=patch)
                registry.addDataset(mergeType, dataId=dataId, run=run)
                for aFilter in ("i", "r"):
                    dataId = dict(skymap="DummyMap", tract=tract, patch=patch, abstract_filter=aFilter)
                    registry.addDataset(calexpType, dataId=dataId, run=run)

        # with empty expression
        builder = DataIdQueryBuilder.fromDimensions(registry, dimensions)
        builder.requireDataset(calexpType, collections=[collection])
        builder.requireDataset(mergeType, collections=[collection])
        rows = list(builder.execute())
        self.assertEqual(len(rows), 3*4*2)   # 4 tracts x 4 patches x 2 filters
        for dataId in rows:
            self.assertCountEqual(dataId.keys(), ("skymap", "tract", "patch", "abstract_filter"))
        self.assertCountEqual(set(dataId["tract"] for dataId in rows), (1, 3, 5))
        self.assertCountEqual(set(dataId["patch"] for dataId in rows), (2, 4, 6, 7))
        self.assertCountEqual(set(dataId["abstract_filter"] for dataId in rows), ("i", "r"))

        # limit to 2 tracts and 2 patches
        builder = DataIdQueryBuilder.fromDimensions(registry, dimensions)
        builder.requireDataset(calexpType, collections=[collection])
        builder.requireDataset(mergeType, collections=[collection])
        builder.whereParsedExpression("tract IN (1, 5) AND patch.patch IN (2, 7)")
        rows = list(builder.execute())
        self.assertEqual(len(rows), 2*2*2)   # 4 tracts x 4 patches x 2 filters
        self.assertCountEqual(set(dataId["tract"] for dataId in rows), (1, 5))
        self.assertCountEqual(set(dataId["patch"] for dataId in rows), (2, 7))
        self.assertCountEqual(set(dataId["abstract_filter"] for dataId in rows), ("i", "r"))

        # limit to single filter
        builder = DataIdQueryBuilder.fromDimensions(registry, dimensions)
        builder.requireDataset(calexpType, collections=[collection])
        builder.requireDataset(mergeType, collections=[collection])
        builder.whereParsedExpression("abstract_filter = 'i'")
        rows = list(builder.execute())
        self.assertEqual(len(rows), 3*4*1)   # 4 tracts x 4 patches x 2 filters
        self.assertCountEqual(set(dataId["tract"] for dataId in rows), (1, 3, 5))
        self.assertCountEqual(set(dataId["patch"] for dataId in rows), (2, 4, 6, 7))
        self.assertCountEqual(set(dataId["abstract_filter"] for dataId in rows), ("i",))

        # expression excludes everything, specifying non-existing skymap is
        # not a fatal error, it's operator error
        builder = DataIdQueryBuilder.fromDimensions(registry, dimensions)
        builder.requireDataset(calexpType, collections=[collection])
        builder.requireDataset(mergeType, collections=[collection])
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

        calexpType = DatasetType(name="CALEXP",
                                 dimensions=registry.dimensions.extract(("instrument", "visit", "detector")),
                                 storageClass=storageClass)
        registry.registerDatasetType(calexpType)

        coaddType = DatasetType(name="deepCoadd_calexp",
                                dimensions=registry.dimensions.extract(("skymap", "tract", "patch",
                                                                        "abstract_filter")),
                                storageClass=storageClass)
        registry.registerDatasetType(coaddType)

        dimensions = registry.dimensions.empty.union(calexpType.dimensions, coaddType.dimensions)

        # without data this should run OK but return empty set
        builder = DataIdQueryBuilder.fromDimensions(registry, dimensions)
        builder.requireDataset(calexpType, collections=[collection])

        rows = list(builder.execute())
        self.assertEqual(len(rows), 0)

    def testCalibrationLabelIndirection(self):
        """Test that SingleDatasetQueryBuilder can look up datasets with
        calibration_label dimensions from a data ID with exposure dimensions.
        """
        # exposure <-> calibration_label lookups for master calibrations
        flat = DatasetType(
            "flat",
            self.registry.dimensions.extract(
                ["instrument", "detector", "physical_filter", "calibration_label"]
            ),
            "ImageU"
        )
        builder = SingleDatasetQueryBuilder.fromSingleCollection(self.registry, flat, collection="calib")
        newLinks = builder.relateDimensions(
            self.registry.dimensions.extract(["instrument", "exposure", "detector"], implied=True)
        )
        self.assertEqual(newLinks, set(["exposure"]))
        self.assertIsNotNone(builder.findSelectableByName("exposure_calibration_label_join"))

    def testSkyPixIndirection(self):
        """Test that SingleDatasetQueryBuilder can look up datasets with
        skypix dimensions from a data ID with visit+detector dimensions.
        """
        # exposure <-> calibration_label lookups for master calibrations
        refcat = DatasetType(
            "refcat",
            self.registry.dimensions.extract(["skypix"]),
            "ImageU"
        )
        builder = SingleDatasetQueryBuilder.fromSingleCollection(self.registry, refcat, collection="refcats")
        newLinks = builder.relateDimensions(
            self.registry.dimensions.extract(["instrument", "visit", "detector"], implied=True)
        )
        self.assertEqual(newLinks, set(["instrument", "visit", "detector"]))
        self.assertIsNotNone(builder.findSelectableByName("visit_detector_skypix_join"))


if __name__ == "__main__":
    unittest.main()

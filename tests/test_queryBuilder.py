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
from datetime import datetime

from lsst.daf.butler import (
    ButlerConfig,
    DataCoordinate,
    DatasetRef,
    DatasetType,
    DimensionGraph,
    Registry,
    StorageClass,
)


class QueryBuilderTestCase(unittest.TestCase):
    """Tests for QueryBuilders.
    """

    def setUp(self):
        self.testDir = os.path.dirname(__file__)
        self.configFile = os.path.join(self.testDir, "config/basic/butler.yaml")
        self.butlerConfig = ButlerConfig(self.configFile)
        self.registry = Registry.fromConfig(self.butlerConfig)

    def testInstrumentDimensions(self):
        """Test queries involving only instrument dimensions, with no joins to
        skymap."""
        registry = self.registry

        # need a bunch of dimensions and datasets for test
        registry.insertDimensionData(
            "instrument",
            dict(name="DummyCam", visit_max=25, exposure_max=300, detector_max=6)
        )
        registry.insertDimensionData(
            "physical_filter",
            dict(instrument="DummyCam", name="dummy_r", abstract_filter="r"),
            dict(instrument="DummyCam", name="dummy_i", abstract_filter="i"),
        )
        registry.insertDimensionData(
            "detector",
            *[dict(instrument="DummyCam", id=i, full_name=str(i)) for i in range(1, 6)]
        )
        registry.insertDimensionData(
            "visit",
            dict(instrument="DummyCam", id=10, name="ten", physical_filter="dummy_i"),
            dict(instrument="DummyCam", id=11, name="eleven", physical_filter="dummy_r"),
            dict(instrument="DummyCam", id=20, name="twelve", physical_filter="dummy_r"),
        )
        registry.insertDimensionData(
            "exposure",
            dict(instrument="DummyCam", id=100, name="100", visit=10, physical_filter="dummy_i"),
            dict(instrument="DummyCam", id=101, name="101", visit=10, physical_filter="dummy_i"),
            dict(instrument="DummyCam", id=110, name="110", visit=11, physical_filter="dummy_r"),
            dict(instrument="DummyCam", id=111, name="111", visit=11, physical_filter="dummy_r"),
            dict(instrument="DummyCam", id=200, name="200", visit=20, physical_filter="dummy_r"),
            dict(instrument="DummyCam", id=201, name="201", visit=20, physical_filter="dummy_r"),
        )
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

        dimensions = DimensionGraph(
            registry.dimensions,
            dimensions=(rawType.dimensions.required | calexpType.dimensions.required)
        )

        # with empty expression
        rows = list(registry.queryDimensions(dimensions, datasets={rawType: [collection1]}, expand=True))
        self.assertEqual(len(rows), 4*3)   # 4 exposures times 3 detectors
        for dataId in rows:
            self.assertCountEqual(dataId.keys(), ("instrument", "detector", "exposure"))
            packer1 = registry.dimensions.makePacker("visit_detector", dataId)
            packer2 = registry.dimensions.makePacker("exposure_detector", dataId)
            self.assertEqual(packer1.unpack(packer1.pack(dataId)),
                             DataCoordinate.standardize(dataId, graph=packer1.dimensions))
            self.assertEqual(packer2.unpack(packer2.pack(dataId)),
                             DataCoordinate.standardize(dataId, graph=packer2.dimensions))
            self.assertNotEqual(packer1.pack(dataId), packer2.pack(dataId))
        self.assertCountEqual(set(dataId["exposure"] for dataId in rows),
                              (100, 101, 110, 111))
        self.assertCountEqual(set(dataId["visit"] for dataId in rows), (10, 11))
        self.assertCountEqual(set(dataId["detector"] for dataId in rows), (1, 2, 3))

        # second collection
        rows = list(registry.queryDimensions(dimensions, datasets={rawType: [collection2]}))
        self.assertEqual(len(rows), 4*3)   # 4 exposures times 3 detectors
        for dataId in rows:
            self.assertCountEqual(dataId.keys(), ("instrument", "detector", "exposure"))
        self.assertCountEqual(set(dataId["exposure"] for dataId in rows),
                              (100, 101, 200, 201))
        self.assertCountEqual(set(dataId["visit"] for dataId in rows), (10, 20))
        self.assertCountEqual(set(dataId["detector"] for dataId in rows), (1, 2, 3, 4, 5))

        # with two input datasets
        rows = list(registry.queryDimensions(dimensions, datasets={rawType: [collection1, collection2]}))
        self.assertEqual(len(set(rows)), 6*3)   # 6 exposures times 3 detectors; set needed to de-dupe
        for dataId in rows:
            self.assertCountEqual(dataId.keys(), ("instrument", "detector", "exposure"))
        self.assertCountEqual(set(dataId["exposure"] for dataId in rows),
                              (100, 101, 110, 111, 200, 201))
        self.assertCountEqual(set(dataId["visit"] for dataId in rows), (10, 11, 20))
        self.assertCountEqual(set(dataId["detector"] for dataId in rows), (1, 2, 3, 4, 5))

        # limit to single visit
        rows = list(registry.queryDimensions(dimensions, datasets={rawType: [collection1]},
                                             where="visit = 10"))
        self.assertEqual(len(rows), 2*3)   # 2 exposures times 3 detectors
        self.assertCountEqual(set(dataId["exposure"] for dataId in rows), (100, 101))
        self.assertCountEqual(set(dataId["visit"] for dataId in rows), (10,))
        self.assertCountEqual(set(dataId["detector"] for dataId in rows), (1, 2, 3))

        # more limiting expression, using link names instead of Table.column
        rows = list(registry.queryDimensions(dimensions, datasets={rawType: [collection1]},
                                             where="visit = 10 and detector > 1"))
        self.assertEqual(len(rows), 2*2)   # 2 exposures times 2 detectors
        self.assertCountEqual(set(dataId["exposure"] for dataId in rows), (100, 101))
        self.assertCountEqual(set(dataId["visit"] for dataId in rows), (10,))
        self.assertCountEqual(set(dataId["detector"] for dataId in rows), (2, 3))

        # expression excludes everything
        rows = list(registry.queryDimensions(dimensions, datasets={rawType: [collection1]},
                                             where="visit > 1000"))
        self.assertEqual(len(rows), 0)

        # Selecting by physical_filter, this is not in the dimensions, but it
        # is a part of the full expression so it should work too.
        rows = list(registry.queryDimensions(dimensions, datasets={rawType: [collection1]},
                                             where="physical_filter = 'dummy_r'"))
        self.assertEqual(len(rows), 2*3)   # 2 exposures times 3 detectors
        self.assertCountEqual(set(dataId["exposure"] for dataId in rows), (110, 111))
        self.assertCountEqual(set(dataId["visit"] for dataId in rows), (11,))
        self.assertCountEqual(set(dataId["detector"] for dataId in rows), (1, 2, 3))

    def testSkyMapDimensions(self):
        """Tests involving only skymap dimensions, no joins to instrument."""
        registry = self.registry

        # need a bunch of dimensions and datasets for test, we want
        # "abstract_filter" in the test so also have to add physical_filter
        # dimensions
        registry.insertDimensionData(
            "instrument",
            dict(instrument="DummyCam")
        )
        registry.insertDimensionData(
            "physical_filter",
            dict(instrument="DummyCam", name="dummy_r", abstract_filter="r"),
            dict(instrument="DummyCam", name="dummy_i", abstract_filter="i"),
        )
        registry.insertDimensionData(
            "skymap",
            dict(name="DummyMap", hash="sha!".encode("utf8"))
        )
        for tract in range(10):
            registry.insertDimensionData("tract", dict(skymap="DummyMap", id=tract))
            registry.insertDimensionData(
                "patch",
                *[dict(skymap="DummyMap", tract=tract, id=patch, cell_x=0, cell_y=0)
                  for patch in range(10)]
            )

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

        dimensions = DimensionGraph(
            registry.dimensions,
            dimensions=(calexpType.dimensions.required | mergeType.dimensions.required |
                        measType.dimensions.required)
        )

        # add pre-existing datasets
        for tract in (1, 3, 5):
            for patch in (2, 4, 6, 7):
                dataId = dict(skymap="DummyMap", tract=tract, patch=patch)
                registry.addDataset(mergeType, dataId=dataId, run=run)
                for aFilter in ("i", "r"):
                    dataId = dict(skymap="DummyMap", tract=tract, patch=patch, abstract_filter=aFilter)
                    registry.addDataset(calexpType, dataId=dataId, run=run)

        # with empty expression
        rows = list(registry.queryDimensions(dimensions,
                                             datasets={calexpType: [collection], mergeType: [collection]}))
        self.assertEqual(len(rows), 3*4*2)   # 4 tracts x 4 patches x 2 filters
        for dataId in rows:
            self.assertCountEqual(dataId.keys(), ("skymap", "tract", "patch", "abstract_filter"))
        self.assertCountEqual(set(dataId["tract"] for dataId in rows), (1, 3, 5))
        self.assertCountEqual(set(dataId["patch"] for dataId in rows), (2, 4, 6, 7))
        self.assertCountEqual(set(dataId["abstract_filter"] for dataId in rows), ("i", "r"))

        # limit to 2 tracts and 2 patches
        rows = list(registry.queryDimensions(dimensions,
                                             datasets={calexpType: [collection], mergeType: [collection]},
                                             where="tract IN (1, 5) AND patch IN (2, 7)"))
        self.assertEqual(len(rows), 2*2*2)   # 2 tracts x 2 patches x 2 filters
        self.assertCountEqual(set(dataId["tract"] for dataId in rows), (1, 5))
        self.assertCountEqual(set(dataId["patch"] for dataId in rows), (2, 7))
        self.assertCountEqual(set(dataId["abstract_filter"] for dataId in rows), ("i", "r"))

        # limit to single filter
        rows = list(registry.queryDimensions(dimensions,
                                             datasets={calexpType: [collection], mergeType: [collection]},
                                             where="abstract_filter = 'i'"))
        self.assertEqual(len(rows), 3*4*1)   # 4 tracts x 4 patches x 2 filters
        self.assertCountEqual(set(dataId["tract"] for dataId in rows), (1, 3, 5))
        self.assertCountEqual(set(dataId["patch"] for dataId in rows), (2, 4, 6, 7))
        self.assertCountEqual(set(dataId["abstract_filter"] for dataId in rows), ("i",))

        # expression excludes everything, specifying non-existing skymap is
        # not a fatal error, it's operator error
        rows = list(registry.queryDimensions(dimensions,
                                             datasets={calexpType: [collection], mergeType: [collection]},
                                             where="skymap = 'Mars'"))
        self.assertEqual(len(rows), 0)

    def testSpatialMatch(self):
        """Test involving spatial match using join tables.

        Note that realistic test needs a reasonably-defined skypix and regions
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

        dimensions = DimensionGraph(
            registry.dimensions,
            dimensions=(calexpType.dimensions.required | coaddType.dimensions.required)
        )

        # without data this should run OK but return empty set
        rows = list(registry.queryDimensions(dimensions, datasets={calexpType: [collection]}))
        self.assertEqual(len(rows), 0)

    def checkQueriedDatasets(self, rows, count):
        rows = list(rows)
        self.assertEqual(len(rows), count)
        for row in rows:
            self.assertIsInstance(row, DatasetRef)
            self.assertIsInstance
            self.assertIsNotNone(row.id)
            self.assertEqual(row.datasetType.dimensions, row.dataId.graph)

    def testCalibrationLabelIndirection(self):
        """Test that we can look up datasets with calibration_label dimensions
        from a data ID with exposure dimensions.
        """
        registry = self.registry

        flat = DatasetType(
            "flat",
            self.registry.dimensions.extract(
                ["instrument", "detector", "physical_filter", "calibration_label"]
            ),
            "ImageU"
        )
        registry.registerDatasetType(flat)
        registry.insertDimensionData("instrument", dict(name="DummyCam"))
        registry.insertDimensionData(
            "physical_filter",
            dict(instrument="DummyCam", name="dummy_i", abstract_filter="i"),
        )
        registry.insertDimensionData(
            "detector",
            *[dict(instrument="DummyCam", id=i, full_name=str(i)) for i in (1, 2, 3, 4, 5)]
        )
        registry.insertDimensionData(
            "visit",
            dict(instrument="DummyCam", id=10, name="ten", physical_filter="dummy_i"),
            dict(instrument="DummyCam", id=11, name="eleven", physical_filter="dummy_i"),
        )
        registry.insertDimensionData(
            "exposure",
            dict(instrument="DummyCam", id=100, name="100", visit=10, physical_filter="dummy_i",
                 datetime_begin=datetime(2005, 12, 15, 2), datetime_end=datetime(2005, 12, 15, 3)),
            dict(instrument="DummyCam", id=101, name="101", visit=11, physical_filter="dummy_i",
                 datetime_begin=datetime(2005, 12, 16, 2), datetime_end=datetime(2005, 12, 16, 3)),
        )
        registry.insertDimensionData(
            "calibration_label",
            dict(instrument="DummyCam", name="first_night",
                 datetime_begin=datetime(2005, 12, 15, 1), datetime_end=datetime(2005, 12, 15, 4)),
            dict(instrument="DummyCam", name="second_night",
                 datetime_begin=datetime(2005, 12, 16, 1), datetime_end=datetime(2005, 12, 16, 4)),
            dict(instrument="DummyCam", name="both_nights",
                 datetime_begin=datetime(2005, 12, 15, 1), datetime_end=datetime(2005, 12, 16, 4)),
        )
        # Different flats for different nights for detectors 1-3 in first
        # collection.
        collection1 = "calibs1"
        run1 = registry.makeRun(collection=collection1)
        for detector in (1, 2, 3):
            registry.addDataset(flat, dict(instrument="DummyCam", calibration_label="first_night",
                                           physical_filter="dummy_i", detector=detector),
                                run=run1)
            registry.addDataset(flat, dict(instrument="DummyCam", calibration_label="second_night",
                                           physical_filter="dummy_i", detector=detector),
                                run=run1)
        # The same flat for both nights for detectors 3-5 (so detector 3 has
        # multiple valid flats) in second collection.
        collection2 = "calib2"
        run2 = registry.makeRun(collection=collection2)
        for detector in (3, 4, 5):
            registry.addDataset(flat, dict(instrument="DummyCam", calibration_label="both_nights",
                                           physical_filter="dummy_i", detector=detector),
                                run=run2)
        # Perform queries for individual exposure+detector combinations, which
        # should always return exactly one flat.
        for exposure in (100, 101):
            for detector in (1, 2, 3):
                with self.subTest(exposure=exposure, detector=detector):
                    rows = registry.queryDatasets("flat", collections=[collection1],
                                                  instrument="DummyCam",
                                                  exposure=exposure,
                                                  detector=detector)
                    self.checkQueriedDatasets(rows, 1)
            for detector in (3, 4, 5):
                with self.subTest(exposure=exposure, detector=detector):
                    rows = registry.queryDatasets("flat", collections=[collection2],
                                                  instrument="DummyCam",
                                                  exposure=exposure,
                                                  detector=detector)
                    self.checkQueriedDatasets(rows, 1)
            for detector in (1, 2, 4, 5):
                with self.subTest(exposure=exposure, detector=detector):
                    rows = registry.queryDatasets("flat", collections=[collection1, collection2],
                                                  instrument="DummyCam",
                                                  exposure=exposure,
                                                  detector=detector)
                    self.checkQueriedDatasets(rows, 1)
            for detector in (3,):
                with self.subTest(exposure=exposure, detector=detector):
                    rows = registry.queryDatasets("flat", collections=[collection1, collection2],
                                                  instrument="DummyCam",
                                                  exposure=exposure,
                                                  detector=detector)
                    self.checkQueriedDatasets(rows, 2)


if __name__ == "__main__":
    unittest.main()

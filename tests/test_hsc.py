#
# LSST Data Management System
#
# Copyright 2017 AURA/LSST.
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
# see <http://www.lsstcorp.org/LegalNotices/>.
#

import unittest

import lsst.butler.ci_hsc


class TestDataTestCase(unittest.TestCase):
    """A test case for the ci_hsc Registry test data."""

    def setUp(self):
        self.db = lsst.butler.ci_hsc.ingest.run("bla.db", create=True)

    def tearDown(self):
        self.db.close()

    def query(self, sql, *args):
        result = self.db.execute(sql, *args)
        return result.fetchall()

    RAW_DATASET_SQL = """
        SELECT
            Dataset.dataset_id AS dataset_id,
            Dataset.registry_id AS registry_id,
            Camera.camera_name AS camera_name,
            Visit.visit_number AS visit_number,
            PhysicalFilter.physical_filter_name AS physical_filter_name,
            AbstractFilter.abstract_filter_name AS abstract_filter_name,
            Snap.snap_index AS snap_index,
            PhysicalSensor.physical_sensor_number AS physical_sensor_number
        FROM
            Dataset
            INNER JOIN SnapDatasetJoin ON
                Dataset.dataset_id = SnapDatasetJoin.dataset_id
                AND
                Dataset.registry_id = SnapDatasetJoin.registry_id
            INNER JOIN PhysicalSensorDatasetJoin ON
                Dataset.dataset_id = PhysicalSensorDatasetJoin.dataset_id
                AND
                Dataset.registry_id = PhysicalSensorDatasetJoin.registry_id
            INNER JOIN Camera ON
                PhysicalSensorDatasetJoin.camera_name == Camera.camera_name
                AND
                SnapDatasetJoin.camera_name = Camera.camera_name
            INNER JOIN Visit ON
                SnapDatasetJoin.camera_name = Visit.camera_name
                AND
                SnapDatasetJoin.visit_number = Visit.visit_number
            INNER JOIN PhysicalFilter ON
                Visit.camera_name = PhysicalFilter.camera_name
                AND
                Visit.physical_filter_name = PhysicalFilter.physical_filter_name
            INNER JOIN AbstractFilter ON
                PhysicalFilter.abstract_filter_name = AbstractFilter.abstract_filter_name
            INNER JOIN Snap ON
                SnapDatasetJoin.camera_name = Snap.camera_name
                AND
                SnapDatasetJoin.visit_number = Snap.visit_number
                AND
                SnapDatasetJoin.snap_index = Snap.snap_index
            INNER JOIN PhysicalSensor ON
                PhysicalSensorDatasetJoin.camera_name = PhysicalSensor.camera_name
                AND
                PhysicalSensorDatasetJoin.physical_sensor_number = PhysicalSensor.physical_sensor_number
            INNER JOIN ObservedSensor ON
                Visit.camera_name = ObservedSensor.camera_name
                AND
                Visit.visit_number = ObservedSensor.visit_number
                AND
                PhysicalSensor.camera_name = ObservedSensor.camera_name
                AND
                PhysicalSensor.physical_sensor_number = ObservedSensor.physical_sensor_number
        WHERE
            Dataset.dataset_type_name = 'raw';
    """

    def testRaw(self):
        """Test that joins between the Dataset table and the DataUnit tables
        used to describe the 'raw'` Dataset make sense.
        """
        cameras = set()
        visits = set()
        sensors = set()
        snaps = set()
        datasets = set()
        physicalFilters = set()
        abstractFilters = set()
        for record in self.query(self.RAW_DATASET_SQL):
            cameras.add(record["camera_name"])
            visits.add(record["visit_number"])
            sensors.add((record["visit_number"], record["physical_sensor_number"]))
            physicalFilters.add(record["physical_filter_name"])
            abstractFilters.add(record["abstract_filter_name"])
            snaps.add((record["visit_number"], record["snap_index"]))
            datasets.add((record["dataset_id"]))
        # Test the big inner join doesn't restrict the set of primary keys for each table
        self.assertEqual(
            visits,
            set(record["visit_number"]
                for record in self.query("SELECT visit_number FROM Visit"))
        )
        self.assertEqual(
            snaps,
            set((visit_number, 1) for visit_number in visits)
        )
        self.assertEqual(
            datasets,
            set(record["dataset_id"]
                for record in self.query("SELECT dataset_id FROM Dataset WHERE dataset_type_name = 'raw'"))
        )
        self.assertEqual(len(sensors), len(datasets))
        self.assertEqual(cameras, set(["HSC"]))
        self.assertEqual(physicalFilters, set(["HSC-R", "HSC-I"]))
        self.assertEqual(abstractFilters, set(["r", "i"]))

    def testCalibrationFrameExistence(self):
        """Test that we have exactly one bias, dark, and flat for each raw
        image.
        """
        baseSql = """
            SELECT
                Dataset.dataset_id,
                Dataset.registry_id
            FROM
                Dataset
                INNER JOIN PhysicalSensorDatasetJoin ON
                    Dataset.dataset_id = PhysicalSensorDatasetJoin.dataset_id
                    AND
                    Dataset.registry_id = PhysicalSensorDatasetJoin.registry_id
                INNER JOIN VisitRangeDatasetJoin ON
                    Dataset.dataset_id = VisitRangeDatasetJoin.dataset_id
                    AND
                    Dataset.registry_id = VisitRangeDatasetJoin.registry_id
                INNER JOIN VisitRangeJoin ON
                    VisitRangeDatasetJoin.camera_name = VisitRangeJoin.camera_name
                    AND
                    VisitRangeDatasetJoin.visit_begin = VisitRangeJoin.visit_begin
                    AND
                    VisitRangeDatasetJoin.visit_end = VisitRangeJoin.visit_end
                {extraFrom}
            WHERE
                Dataset.dataset_type_name == '{type}'
                AND
                VisitRangeJoin.visit_number = ?
                AND
                PhysicalSensorDatasetJoin.physical_sensor_number = ?
                {extraWhere}
        """
        flatExtraFrom = """
                INNER JOIN PhysicalFilterDatasetJoin ON
                    Dataset.dataset_id = PhysicalFilterDatasetJoin.dataset_id
                    AND
                    Dataset.registry_id = PhysicalFilterDatasetJoin.registry_id
        """
        flatExtraWhere = """
                AND
                PhysicalFilterDatasetJoin.physical_filter_name = ?
        """
        biasSql = baseSql.format(type="bias", extraFrom="", extraWhere="")
        darkSql = baseSql.format(type="dark", extraFrom="", extraWhere="")
        flatSql = baseSql.format(type="flat", extraFrom=flatExtraFrom, extraWhere=flatExtraWhere)
        for raw in self.query(self.RAW_DATASET_SQL):
            bias = self.query(biasSql, (raw["visit_number"], raw["physical_sensor_number"]))
            self.assertEqual(len(bias), 1)
            dark = self.query(darkSql, (raw["visit_number"], raw["physical_sensor_number"]))
            self.assertEqual(len(dark), 1)
            flat = self.query(
                flatSql,
                (raw["visit_number"], raw["physical_sensor_number"], raw["physical_filter_name"])
            )
            self.assertEqual(len(flat), 1)


if __name__ == "__main__":
    unittest.main()

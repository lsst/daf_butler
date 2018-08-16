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

import unittest

import lsst.utils
import lsst.utils.tests

from lsst.daf.butler.core.schema import SchemaConfig
from lsst.daf.butler.core.dataUnit import DataUnit, DataUnitRegistry


class DataUnitRegistryTestCase(lsst.utils.tests.TestCase):
    """Tests for `DataUnitRegistry`.
    """
    def setUp(self):
        self.config = SchemaConfig()

    def testConstructor(self):
        """Independent check for `Schema` constructor.
        """
        dataUnitRegistry = DataUnitRegistry.fromConfig(self.config)
        self.assertIsInstance(dataUnitRegistry, DataUnitRegistry)
        for dataUnitName, dataUnit in dataUnitRegistry.items():
            self.assertIsInstance(dataUnit, DataUnit)
            for d in dataUnit.dependencies:
                self.assertIsInstance(d, DataUnit)

        # Tests below depend on the schema.yaml definitions as well

        # check all spatial data units names, Sensor is there because
        # pair (Visit, Sensor) is a spatial entity
        self.assertCountEqual(dataUnitRegistry._spatialDataUnits,
                              ["Tract", "Patch", "Visit", "Sensor", "SkyPix"])
        self.assertCountEqual(dataUnitRegistry._dataUnitRegions.keys(),
                              [{'Visit'}, {'SkyPix'}, {'Tract'},
                               {'Patch', 'Tract'}, {'Sensor', 'Visit'}])
        self.assertEqual(len(dataUnitRegistry.joins), 11)
        joins = ((['Exposure'], ['Exposure']),
                 (['Exposure'], ['ExposureRange']),
                 (['Patch'], ['SkyPix']),
                 (['Sensor', 'Visit'], ['Patch']),
                 (['Sensor', 'Visit'], ['SkyPix']),
                 (['Sensor', 'Visit'], ['Tract']),
                 (['Tract'], ['SkyPix']),
                 (['Visit'], ['Patch']),
                 (['Visit'], ['Sensor']),
                 (['Visit'], ['SkyPix']))
        for lhs, rhs in joins:
            self.assertIsNotNone(dataUnitRegistry.getJoin(lhs, rhs))
            self.assertIsNotNone(dataUnitRegistry.getJoin(rhs, lhs))


if __name__ == "__main__":
    unittest.main()

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

from lsst.daf.butler.core.schema import SchemaConfig
from lsst.daf.butler.core.dataUnit import DataUnit, DataUnitRegistry


class DataUnitRegistryTestCase(lsst.utils.tests.TestCase):
    """Tests for `DataUnitRegistry`.
    """
    def setUp(self):
        self.testDir = os.path.dirname(__file__)
        self.schemaFile = os.path.join(self.testDir, "../config/registry/default_schema.yaml")
        self.config = SchemaConfig(self.schemaFile)

    def testConstructor(self):
        """Independent check for `Schema` constructor.
        """
        dataUnitRegistry = DataUnitRegistry.fromConfig(self.config['dataUnits'])
        self.assertIsInstance(dataUnitRegistry, DataUnitRegistry)
        for dataUnitName, dataUnit in dataUnitRegistry.items():
            self.assertIsInstance(dataUnit, DataUnit)
            for d in dataUnit.dependencies:
                self.assertIsInstance(d, DataUnit)


if __name__ == "__main__":
    unittest.main()

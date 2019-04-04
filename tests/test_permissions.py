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

"""Test the permisssions model."""

import unittest

from lsst.daf.butler import Permissions, PermissionsConfig, StorageClass, ValidationError
from datasetsHelper import DatasetTestHelper


class PermissionsTestCase(unittest.TestCase, DatasetTestHelper):

    def setUp(self):
        self.id = 0

        # Create DatasetRefs to test against permissions model
        dimensions = frozenset(("Visit", "PhysicalFilter", "Instrument"))
        sc = StorageClass("DummySC", dict, None)
        self.calexpA = self.makeDatasetRef("calexp", dimensions, sc, {"instrument": "A",
                                                                      "physical_filter": "u"})

        dimensions = frozenset(("Visit", "CalibrationLabel", "Instrument"))
        self.pviA = self.makeDatasetRef("pvi", dimensions, sc, {"instrument": "A",
                                                                "visit": 1})
        self.pviB = self.makeDatasetRef("pvi", dimensions, sc, {"instrument": "B",
                                                                "visit": 2})

    def testSimpleAccept(self):
        config = PermissionsConfig({"accept": ["calexp", "ExposureF"]})
        permissions = Permissions(config)

        self.assertTrue(permissions.hasPermission(self.calexpA))
        self.assertFalse(permissions.hasPermission(self.pviA))

        # Dimension accept
        config = PermissionsConfig({"accept": ["Visit+PhysicalFilter+Instrument", "ExposureF"]})
        permissions = Permissions(config)

        self.assertTrue(permissions.hasPermission(self.calexpA))
        self.assertFalse(permissions.hasPermission(self.pviA))

        config = PermissionsConfig({"accept": ["Visit+CalibrationLabel+Instrument", "ExposureF"]})
        permissions = Permissions(config)

        self.assertFalse(permissions.hasPermission(self.calexpA))
        self.assertTrue(permissions.hasPermission(self.pviA))
        self.assertTrue(permissions.hasPermission(self.pviA))

        # Only accept instrument A pvi
        config = PermissionsConfig({"accept": [{"instrument<A>": ["pvi"]}]})
        permissions = Permissions(config)

        self.assertFalse(permissions.hasPermission(self.calexpA))
        self.assertTrue(permissions.hasPermission(self.pviA))
        self.assertFalse(permissions.hasPermission(self.pviB))

        # Accept PVI for instrument B but not instrument A
        config = PermissionsConfig({"accept": ["calexp",
                                               {"instrument<B>": ["pvi"]}]})
        permissions = Permissions(config)

        self.assertTrue(permissions.hasPermission(self.calexpA))
        self.assertFalse(permissions.hasPermission(self.pviA))
        self.assertTrue(permissions.hasPermission(self.pviB))

    def testSimpleReject(self):
        config = PermissionsConfig({"reject": ["calexp", "ExposureF"]})
        permissions = Permissions(config)

        self.assertFalse(permissions.hasPermission(self.calexpA))
        self.assertTrue(permissions.hasPermission(self.pviA))

    def testAcceptReject(self):
        # Reject everything except calexp
        config = PermissionsConfig({"accept": ["calexp"], "reject": ["all"]})
        permissions = Permissions(config)

        self.assertTrue(permissions.hasPermission(self.calexpA))
        self.assertFalse(permissions.hasPermission(self.pviA))

        # Accept everything except calexp
        config = PermissionsConfig({"reject": ["calexp"], "accept": ["all"]})
        permissions = Permissions(config)

        self.assertFalse(permissions.hasPermission(self.calexpA))
        self.assertTrue(permissions.hasPermission(self.pviA))

        # Reject pvi but explicitly accept pvi for instrument A
        # Reject all instrument A but accept everything else
        # The reject here is superfluous
        config = PermissionsConfig({"accept": [{"instrument<A>": ["pvi"]}], "reject": ["pvi"]})
        permissions = Permissions(config)

        self.assertFalse(permissions.hasPermission(self.calexpA))
        self.assertTrue(permissions.hasPermission(self.pviA))
        self.assertFalse(permissions.hasPermission(self.pviB))

        # Accept everything except pvi from other than instrument A
        config = PermissionsConfig({"accept": ["all", {"instrument<A>": ["pvi"]}], "reject": ["pvi"]})
        permissions = Permissions(config)

        self.assertTrue(permissions.hasPermission(self.calexpA))
        self.assertTrue(permissions.hasPermission(self.pviA))
        self.assertFalse(permissions.hasPermission(self.pviB))

    def testWildcardReject(self):
        # Reject everything
        config = PermissionsConfig({"reject": ["all"]})
        permissions = Permissions(config)

        self.assertFalse(permissions.hasPermission(self.calexpA))
        self.assertFalse(permissions.hasPermission(self.pviA))

        print("TESTING instrument reject")
        # Reject all instrument A but accept everything else
        config = PermissionsConfig({"reject": [{"instrument<A>": ["all"]}]})
        permissions = Permissions(config)

        self.assertFalse(permissions.hasPermission(self.calexpA))
        self.assertFalse(permissions.hasPermission(self.pviA))
        self.assertTrue(permissions.hasPermission(self.pviB))

    def testWildcardAccept(self):
        # Accept everything
        config = PermissionsConfig({})
        permissions = Permissions(config)

        self.assertTrue(permissions.hasPermission(self.calexpA))
        self.assertTrue(permissions.hasPermission(self.pviA))

        # Accept everything
        permissions = Permissions(None)

        self.assertTrue(permissions.hasPermission(self.calexpA))
        self.assertTrue(permissions.hasPermission(self.pviA))

        # Accept everything explicitly
        config = PermissionsConfig({"accept": ["all"]})
        permissions = Permissions(config)

        self.assertTrue(permissions.hasPermission(self.calexpA))
        self.assertTrue(permissions.hasPermission(self.pviA))

        print("TESTING instrument accept")
        # Accept all instrument A but reject everything else
        config = PermissionsConfig({"accept": [{"instrument<A>": ["all"]}]})
        permissions = Permissions(config)

        self.assertTrue(permissions.hasPermission(self.calexpA))
        self.assertTrue(permissions.hasPermission(self.pviA))
        self.assertFalse(permissions.hasPermission(self.pviB))

    def testEdgeCases(self):
        # Accept everything and reject everything
        config = PermissionsConfig({"accept": ["all"], "reject": ["all"]})
        with self.assertRaises(ValidationError):
            Permissions(config)


if __name__ == "__main__":
    unittest.main()

# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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

"""Test the constraints model."""

import unittest

from lsst.daf.butler import DimensionUniverse, StorageClass, ValidationError
from lsst.daf.butler.datastore.constraints import Constraints, ConstraintsConfig
from lsst.daf.butler.tests import DatasetTestHelper


class ConstraintsTestCase(unittest.TestCase, DatasetTestHelper):
    """Test constraints system."""

    def setUp(self):
        self.id = 0

        # Create DatasetRefs to test against constraints model
        self.universe = DimensionUniverse()
        dimensions = self.universe.conform(("visit", "physical_filter", "instrument"))
        sc = StorageClass("DummySC", dict, None)
        self.calexpA = self.makeDatasetRef(
            "calexp",
            dimensions,
            sc,
            {"instrument": "A", "physical_filter": "u", "visit": 3},
        )

        dimensions = self.universe.conform(("visit", "detector", "instrument"))
        self.pviA = self.makeDatasetRef(
            "pvi",
            dimensions,
            sc,
            {"instrument": "A", "visit": 1, "detector": 0},
        )
        self.pviB = self.makeDatasetRef(
            "pvi",
            dimensions,
            sc,
            {"instrument": "B", "visit": 2, "detector": 0},
        )

    def testSimpleAccept(self):
        config = ConstraintsConfig({"accept": ["calexp", "ExposureF"]})
        constraints = Constraints(config, universe=self.universe)

        self.assertTrue(constraints.isAcceptable(self.calexpA))
        self.assertFalse(constraints.isAcceptable(self.pviA))

        # Dimension accept
        config = ConstraintsConfig({"accept": ["visit+physical_filter+instrument", "ExposureF"]})
        constraints = Constraints(config, universe=self.universe)

        self.assertTrue(constraints.isAcceptable(self.calexpA))
        self.assertFalse(constraints.isAcceptable(self.pviA))

        config = ConstraintsConfig({"accept": ["visit+detector+instrument", "ExposureF"]})
        constraints = Constraints(config, universe=self.universe)

        self.assertFalse(constraints.isAcceptable(self.calexpA))
        self.assertTrue(constraints.isAcceptable(self.pviA))
        self.assertTrue(constraints.isAcceptable(self.pviA))

        # Only accept instrument A pvi
        config = ConstraintsConfig({"accept": [{"instrument<A>": ["pvi"]}]})
        constraints = Constraints(config, universe=self.universe)

        self.assertFalse(constraints.isAcceptable(self.calexpA))
        self.assertTrue(constraints.isAcceptable(self.pviA))
        self.assertFalse(constraints.isAcceptable(self.pviB))

        # Accept PVI for instrument B but not instrument A
        config = ConstraintsConfig({"accept": ["calexp", {"instrument<B>": ["pvi"]}]})
        constraints = Constraints(config, universe=self.universe)

        self.assertTrue(constraints.isAcceptable(self.calexpA))
        self.assertFalse(constraints.isAcceptable(self.pviA))
        self.assertTrue(constraints.isAcceptable(self.pviB))

    def testSimpleReject(self):
        config = ConstraintsConfig({"reject": ["calexp", "ExposureF"]})
        constraints = Constraints(config, universe=self.universe)

        self.assertFalse(constraints.isAcceptable(self.calexpA))
        self.assertTrue(constraints.isAcceptable(self.pviA))

    def testAcceptReject(self):
        # Reject everything except calexp
        config = ConstraintsConfig({"accept": ["calexp"], "reject": ["all"]})
        constraints = Constraints(config, universe=self.universe)

        self.assertTrue(constraints.isAcceptable(self.calexpA))
        self.assertFalse(constraints.isAcceptable(self.pviA))

        # Accept everything except calexp
        config = ConstraintsConfig({"reject": ["calexp"], "accept": ["all"]})
        constraints = Constraints(config, universe=self.universe)

        self.assertFalse(constraints.isAcceptable(self.calexpA))
        self.assertTrue(constraints.isAcceptable(self.pviA))

        # Reject pvi but explicitly accept pvi for instrument A
        # Reject all instrument A but accept everything else
        # The reject here is superfluous
        config = ConstraintsConfig({"accept": [{"instrument<A>": ["pvi"]}], "reject": ["pvi"]})
        constraints = Constraints(config, universe=self.universe)

        self.assertFalse(constraints.isAcceptable(self.calexpA))
        self.assertTrue(constraints.isAcceptable(self.pviA))
        self.assertFalse(constraints.isAcceptable(self.pviB))

        # Accept everything except pvi from other than instrument A
        config = ConstraintsConfig({"accept": ["all", {"instrument<A>": ["pvi"]}], "reject": ["pvi"]})
        constraints = Constraints(config, universe=self.universe)

        self.assertTrue(constraints.isAcceptable(self.calexpA))
        self.assertTrue(constraints.isAcceptable(self.pviA))
        self.assertFalse(constraints.isAcceptable(self.pviB))

    def testWildcardReject(self):
        # Reject everything
        config = ConstraintsConfig({"reject": ["all"]})
        constraints = Constraints(config, universe=self.universe)

        self.assertFalse(constraints.isAcceptable(self.calexpA))
        self.assertFalse(constraints.isAcceptable(self.pviA))

        # Reject all instrument A but accept everything else
        config = ConstraintsConfig({"reject": [{"instrument<A>": ["all"]}]})
        constraints = Constraints(config, universe=self.universe)

        self.assertFalse(constraints.isAcceptable(self.calexpA))
        self.assertFalse(constraints.isAcceptable(self.pviA))
        self.assertTrue(constraints.isAcceptable(self.pviB))

    def testWildcardAccept(self):
        # Accept everything
        config = ConstraintsConfig({})
        constraints = Constraints(config, universe=self.universe)

        self.assertTrue(constraints.isAcceptable(self.calexpA))
        self.assertTrue(constraints.isAcceptable(self.pviA))

        # Accept everything
        constraints = Constraints(None, universe=self.universe)

        self.assertTrue(constraints.isAcceptable(self.calexpA))
        self.assertTrue(constraints.isAcceptable(self.pviA))

        # Accept everything explicitly
        config = ConstraintsConfig({"accept": ["all"]})
        constraints = Constraints(config, universe=self.universe)

        self.assertTrue(constraints.isAcceptable(self.calexpA))
        self.assertTrue(constraints.isAcceptable(self.pviA))

        # Accept all instrument A but reject everything else
        config = ConstraintsConfig({"accept": [{"instrument<A>": ["all"]}]})
        constraints = Constraints(config, universe=self.universe)

        self.assertTrue(constraints.isAcceptable(self.calexpA))
        self.assertTrue(constraints.isAcceptable(self.pviA))
        self.assertFalse(constraints.isAcceptable(self.pviB))

    def testEdgeCases(self):
        # Accept everything and reject everything
        config = ConstraintsConfig({"accept": ["all"], "reject": ["all"]})
        with self.assertRaises(ValidationError):
            Constraints(config, universe=self.universe)


if __name__ == "__main__":
    unittest.main()

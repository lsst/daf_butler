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

import os
import unittest

from lsst.daf.butler import DatasetType, DimensionUniverse, StorageClass
from lsst.daf.butler.datastore.composites import CompositesConfig, CompositesMap

TESTDIR = os.path.dirname(__file__)


class TestCompositesConfig(unittest.TestCase):
    """Test composites configuration."""

    @classmethod
    def setUpClass(cls):
        cls.configFile = os.path.join(TESTDIR, "config", "basic", "composites.yaml")

    def testBadConfig(self):
        """Config with bad values in it"""
        with self.assertRaises(ValueError):
            CompositesConfig(os.path.join(TESTDIR, "config", "basic", "composites-bad.yaml"))

    def testConfig(self):
        c = CompositesConfig(self.configFile)
        self.assertIn("default", c)
        # Check merging has worked
        rootKey = "disassembled"
        self.assertIn(f".{rootKey}.dummyTrue", c)
        self.assertIn(f".{rootKey}.StructuredComposite", c)

        # Check that all entries are booleans (this is meant to be enforced
        # internally)
        for k in c[rootKey]:
            self.assertIsInstance(c[f".{rootKey}.{k}"], bool, f"Testing {rootKey}.{k}")

    def testMap(self):
        universe = DimensionUniverse()
        c = CompositesMap(self.configFile, universe=universe)

        # Check that a str is not supported
        with self.assertRaises(ValueError):
            c.shouldBeDisassembled("fred")

        # These will fail (not a composite)
        sc = StorageClass("StructuredDataJson")
        d = DatasetType("dummyTrue", universe.empty, sc)
        self.assertFalse(sc.isComposite())
        self.assertFalse(d.isComposite())
        self.assertFalse(c.shouldBeDisassembled(d), f"Test with DatasetType: {d}")
        self.assertFalse(c.shouldBeDisassembled(sc), f"Test with StorageClass: {sc}")

        # Repeat but this time use a composite storage class
        sccomp = StorageClass("Dummy")
        sc = StorageClass("StructuredDataJson", components={"dummy": sccomp, "dummy2": sccomp})
        d = DatasetType("dummyTrue", universe.empty, sc)
        self.assertTrue(sc.isComposite())
        self.assertTrue(d.isComposite())
        self.assertTrue(c.shouldBeDisassembled(d), f"Test with DatasetType: {d}")
        self.assertFalse(c.shouldBeDisassembled(sc), f"Test with StorageClass: {sc}")

        # Override with False
        d = DatasetType("dummyFalse", universe.empty, sc)
        self.assertFalse(c.shouldBeDisassembled(d), f"Test with DatasetType: {d}")

        # DatasetType that has no explicit entry
        d = DatasetType("dummyFred", universe.empty, sc)
        self.assertFalse(c.shouldBeDisassembled(d), f"Test with DatasetType: {d}")

        # StorageClass that will be disassembled
        sc = StorageClass("StructuredComposite", components={"dummy": sccomp, "dummy2": sccomp})
        d = DatasetType("dummyFred", universe.empty, sc)
        self.assertTrue(c.shouldBeDisassembled(d), f"Test with DatasetType: {d}")

        # Check that we are not allowed a single component in a composite
        with self.assertRaises(ValueError):
            StorageClass("TestSC", components={"dummy": sccomp})


if __name__ == "__main__":
    unittest.main()

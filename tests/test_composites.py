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

from lsst.daf.butler import CompositesConfig, CompositesMap, StorageClass, DatasetType

TESTDIR = os.path.dirname(__file__)


class TestCompositesConfig(unittest.TestCase):

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
        self.assertIn("names.calexp", c)
        self.assertIn("names.dummyTrue", c)
        self.assertIn("names.StructuredComposite", c)
        self.assertIn("names.ExposureF", c)

        # Check that all entries are booleans (this is meant to be enforced
        # internally)
        for k in c["names"]:
            self.assertIsInstance(c[f"names.{k}"], bool, f"Testing names.{k}")

    def testMap(self):
        c = CompositesMap(self.configFile)

        # Check that a str is not supported
        with self.assertRaises(ValueError):
            c.doDisassembly("fred")

        # These will fail (not a composite)
        sc = StorageClass("StructuredDataJson")
        d = DatasetType("dummyTrue", ("a", "b"), sc)
        self.assertFalse(c.doDisassembly(d), f"Test with DatasetType: {d}")
        self.assertFalse(c.doDisassembly(sc), f"Test with StorageClass: {sc}")

        # Repeat but this time use a composite storage class
        sccomp = StorageClass("Dummy")
        sc = StorageClass("StructuredDataJson", components={"dummy": sccomp})
        d = DatasetType("dummyTrue", ("a", "b"), sc)
        self.assertTrue(c.doDisassembly(d), f"Test with DatasetType: {d}")
        self.assertFalse(c.doDisassembly(sc), f"Test with StorageClass: {sc}")

        # Override with False
        d = DatasetType("dummyFalse", ("a", "b"), sc)
        self.assertFalse(c.doDisassembly(d), f"Test with DatasetType: {d}")

        # DatasetType that has no explicit entry
        d = DatasetType("dummyFred", ("a", "b"), sc)
        self.assertFalse(c.doDisassembly(d), f"Test with DatasetType: {d}")

        # StorageClass that will be disassembled
        sc = StorageClass("StructuredComposite", components={"dummy": sccomp})
        d = DatasetType("dummyFred", ("a", "b"), sc)
        self.assertTrue(c.doDisassembly(d), f"Test with DatasetType: {d}")


if __name__ == "__main__":
    unittest.main()

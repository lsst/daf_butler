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

import os.path
import posixpath
import unittest

from lsst.daf.butler import Location, LocationFactory
from lsst.resources import ResourcePath


class LocationTestCase(unittest.TestCase):
    """Tests for Location within datastore"""

    def testFileLocation(self):
        root = os.path.abspath(os.path.curdir)
        factory = LocationFactory(root)
        print(f"Factory created: {factory}")

        pathInStore = "relative/path/file.ext"
        loc1 = factory.fromPath(pathInStore)

        self.assertEqual(loc1.path, os.path.join(root, pathInStore))
        self.assertEqual(loc1.pathInStore.path, pathInStore)
        self.assertTrue(loc1.uri.geturl().startswith("file:///"))
        self.assertTrue(loc1.uri.geturl().endswith("file.ext"))
        loc1.updateExtension("fits")
        self.assertTrue(loc1.uri.geturl().endswith("file.fits"), f"Checking 'fits' extension in {loc1.uri}")
        loc1.updateExtension("fits.gz")
        self.assertEqual(loc1.uri.basename(), "file.fits.gz")
        self.assertTrue(
            loc1.uri.geturl().endswith("file.fits.gz"), f"Checking 'fits.gz' extension in {loc1.uri}"
        )
        self.assertEqual(loc1.getExtension(), ".fits.gz")
        loc1.updateExtension(".jpeg")
        self.assertTrue(loc1.uri.geturl().endswith("file.jpeg"), f"Checking 'jpeg' extension in {loc1.uri}")
        loc1.updateExtension(None)
        self.assertTrue(
            loc1.uri.geturl().endswith("file.jpeg"), f"Checking unchanged extension in {loc1.uri}"
        )
        loc1.updateExtension("")
        self.assertTrue(loc1.uri.geturl().endswith("file"), f"Checking no extension in {loc1.uri}")
        self.assertEqual(loc1.getExtension(), "")

        loc2 = factory.fromPath(pathInStore)
        loc3 = factory.fromPath(pathInStore)
        self.assertEqual(loc2, loc3)

    def testAbsoluteLocations(self):
        """Using a pathInStore that refers to absolute URI."""
        loc = Location(None, "file:///something.txt")
        self.assertEqual(loc.pathInStore.path, "/something.txt")
        self.assertEqual(str(loc.uri), "file:///something.txt")

        with self.assertRaises(ValueError):
            Location(None, "relative.txt")

    def testBadLocations(self):
        with self.assertRaises(ValueError):
            Location([1, 2, 3], "something.txt")

        with self.assertRaises(ValueError):
            Location(ResourcePath("a/b/c", forceAbsolute=False), "d.txt")

        with self.assertRaises(ValueError):
            Location("/a/c", "/a/c")

    def testRelativeRoot(self):
        root = os.path.abspath(os.path.curdir)
        factory = LocationFactory(os.path.curdir)

        pathInStore = "relative/path/file.ext"
        loc1 = factory.fromPath(pathInStore)

        self.assertEqual(loc1.path, os.path.join(root, pathInStore))
        self.assertEqual(loc1.pathInStore.path, pathInStore)
        self.assertEqual(loc1.uri.scheme, "file")

        with self.assertRaises(ValueError):
            factory.fromPath("../something")

    def testQuotedRoot(self):
        """Test we can handle quoted characters."""
        root = "/a/b/c+1/d"
        factory = LocationFactory(root)

        pathInStore = "relative/path/file.ext.gz"

        for pathInStore in (
            "relative/path/file.ext.gz",
            "relative/path+2/file.ext.gz",
            "relative/path+3/file&.ext.gz",
        ):
            loc1 = factory.fromPath(pathInStore)

            self.assertEqual(loc1.pathInStore.path, pathInStore)
            self.assertEqual(loc1.path, os.path.join(root, pathInStore))
            self.assertIn("%", str(loc1.uri))
            self.assertEqual(loc1.getExtension(), ".ext.gz")

    def testHttpLocation(self):
        root = "https://www.lsst.org/butler/datastore"
        factory = LocationFactory(root)
        print(f"Factory created: {factory}")

        pathInStore = "relative/path/file.ext"
        loc1 = factory.fromPath(pathInStore)

        self.assertEqual(loc1.path, posixpath.join("/butler/datastore", pathInStore))
        self.assertEqual(loc1.pathInStore.path, pathInStore)
        self.assertEqual(loc1.uri.scheme, "https")
        self.assertEqual(loc1.uri.basename(), "file.ext")
        loc1.updateExtension("fits")
        self.assertTrue(loc1.uri.basename(), "file.fits")


if __name__ == "__main__":
    unittest.main()

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
import os.path
import posixpath

from lsst.daf.butler import LocationFactory


class LocationTestCase(unittest.TestCase):
    """Tests for Location within datastore
    """

    def testFileLocation(self):
        root = os.path.abspath(os.path.curdir)
        factory = LocationFactory(root)
        print(f"Factory created: {factory}")

        pathInStore = "relative/path/file.ext"
        loc1 = factory.fromPath(pathInStore)

        self.assertEqual(loc1.path, os.path.join(root, pathInStore))
        self.assertEqual(loc1.pathInStore, pathInStore)
        self.assertTrue(loc1.uri.startswith("file:///"))
        self.assertTrue(loc1.uri.endswith("file.ext"))
        loc1.updateExtension("fits")
        self.assertTrue(loc1.uri.endswith("file.fits"))
        loc1.updateExtension(None)
        self.assertTrue(loc1.uri.endswith("file.fits"))
        loc1.updateExtension("")
        self.assertTrue(loc1.uri.endswith("file"))

    def testRelativeRoot(self):
        root = os.path.abspath(os.path.curdir)
        factory = LocationFactory(os.path.curdir)
        print(f"Factory created: {factory}")

        pathInStore = "relative/path/file.ext"
        loc1 = factory.fromPath(pathInStore)

        self.assertEqual(loc1.path, os.path.join(root, pathInStore))
        self.assertEqual(loc1.pathInStore, pathInStore)
        self.assertTrue(loc1.uri.startswith("file:///"))

    def testHttpLocation(self):
        root = "https://www.lsst.org/butler/datastore"
        factory = LocationFactory(root)
        print(f"Factory created: {factory}")

        pathInStore = "relative/path/file.ext"
        loc1 = factory.fromPath(pathInStore)

        self.assertEqual(loc1.path, posixpath.join("/butler/datastore", pathInStore))
        self.assertEqual(loc1.pathInStore, pathInStore)
        self.assertTrue(loc1.uri.startswith("https://"))
        self.assertTrue(loc1.uri.endswith("file.ext"))
        loc1.updateExtension("fits")


if __name__ == "__main__":
    unittest.main()

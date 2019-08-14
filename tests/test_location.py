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

from lsst.daf.butler import LocationFactory, ButlerURI
from lsst.daf.butler.core.location import os2posix, posix2os


class LocationTestCase(unittest.TestCase):
    """Tests for Location within datastore
    """

    def testButlerUri(self):

        # Root to use for relative paths
        testRoot = "/tmp/"

        uriStrings = (
            # Test string, forceAbsolute, scheme, netloc, path
            # These are being tested with forceAbsolute=True
            ("file:///rootDir/absolute/file.ext", True, "file", "", "/rootDir/absolute/file.ext"),
            ("/rootDir/absolute/file.ext", True, "file", "", "/rootDir/absolute/file.ext"),
            ("/rootDir/absolute/file.ext", False, "file", "", "/rootDir/absolute/file.ext"),
            ("/rootDir/absolute/", True, "file", "", "/rootDir/absolute/"),
            ("file:relative/file.ext", True, "file", "", posixpath.join(testRoot, "relative/file.ext")),
            ("file:relative/directory/", True, "file", "", posixpath.join(testRoot, "relative/directory/")),
            ("file://relative/file.ext", True, "file", "relative", "/file.ext"),
            ("file:///absolute/directory/", True, "file", "", "/absolute/directory/"),
            ("relative/file.ext", True, "", "", os.path.join(testRoot, "relative/file.ext")),
            ("relative/file.ext", False, "", "", "relative/file.ext"),
            ("s3://bucketname/rootDir/relative/file.ext", True, "s3", "bucketname",
             "/rootDir/relative/file.ext"),
            ("~/relative/file.ext", True, "file", "", os.path.expanduser("~/relative/file.ext")),
            ("~/relative/file.ext", False, "file", "", os.path.expanduser("~/relative/file.ext")),
            ("test/../relative/file.ext", True, "", "", os.path.join(testRoot, "relative/file.ext")),
            ("test/../relative/file.ext", False, "", "", "relative/file.ext"),
        )

        for uriInfo in uriStrings:
            uri = ButlerURI(uriInfo[0], root=testRoot, forceAbsolute=uriInfo[1])
            with self.subTest(uri=uriInfo[0]):
                self.assertEqual(uri.scheme, uriInfo[2], "test scheme")
                self.assertEqual(uri.netloc, uriInfo[3], "test netloc")
                self.assertEqual(uri.path, uriInfo[4], "test path")

        # File replacement
        uriStrings = (
            ("relative/file.ext", "newfile.fits", "relative/newfile.fits"),
            ("relative/", "newfile.fits", "relative/newfile.fits"),
            ("isThisADirOrFile", "aFile.fits", "aFile.fits"),
            ("https://www.lsst.org/butler/", "butler.yaml", "/butler/butler.yaml"),
            ("s3://amazon/datastore/", "butler.yaml", "/datastore/butler.yaml"),
            ("s3://amazon/datastore/mybutler.yaml", "butler.yaml", "/datastore/butler.yaml"),
        )

        for uriInfo in uriStrings:
            uri = ButlerURI(uriInfo[0], forceAbsolute=False)
            uri.updateFile(uriInfo[1])
            with self.subTest(uri=uriInfo[0]):
                self.assertEqual(uri.path, uriInfo[2])

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
        self.assertTrue(loc1.uri.endswith("file.fits"))

    def testPosix2OS(self):
        """Test round tripping of the posix to os.path conversion helpers."""
        testPaths = ("/a/b/c.e", "a/b", "a/b/", "/a/b", "/a/b/", "a/b/c.e")
        for p in testPaths:
            with self.subTest(path=p):
                self.assertEqual(os2posix(posix2os(p)), p)


if __name__ == "__main__":
    unittest.main()

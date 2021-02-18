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

import copy
import unittest
import os.path
import posixpath
import pickle
import pathlib

from lsst.daf.butler import LocationFactory, Location, ButlerURI
from lsst.daf.butler.core._butlerUri.utils import os2posix, posix2os


class LocationTestCase(unittest.TestCase):
    """Tests for Location within datastore
    """

    def testButlerUri(self):
        """Tests whether ButlerURI instantiates correctly given different
        arguments.
        """
        # Root to use for relative paths
        testRoot = "/tmp/"

        # uriStrings is a list of tuples containing test string, forceAbsolute,
        # forceDirectory as arguments to ButlerURI and scheme, netloc and path
        # as expected attributes. Test asserts constructed equals to expected.
        # 1) no determinable schemes (ensures schema and netloc are not set)
        osRelFilePath = os.path.join(testRoot, "relative/file.ext")
        uriStrings = [
            ("relative/file.ext", True, False, "", "", osRelFilePath),
            ("relative/file.ext", False, False, "", "", "relative/file.ext"),
            ("test/../relative/file.ext", True, False, "", "", osRelFilePath),
            ("test/../relative/file.ext", False, False, "", "", "relative/file.ext"),
            ("relative/dir", False, True, "", "", "relative/dir/")
        ]
        # 2) implicit file scheme, tests absolute file and directory paths
        uriStrings.extend((
            ("/rootDir/absolute/file.ext", True, False, "file", "", '/rootDir/absolute/file.ext'),
            ("~/relative/file.ext", True, False, "file", "", os.path.expanduser("~/relative/file.ext")),
            ("~/relative/file.ext", False, False, "file", "", os.path.expanduser("~/relative/file.ext")),
            ("/rootDir/absolute/", True, False, "file", "", "/rootDir/absolute/"),
            ("/rootDir/absolute", True, True, "file", "", "/rootDir/absolute/"),
            ("~/rootDir/absolute", True, True, "file", "", os.path.expanduser("~/rootDir/absolute/"))
        ))
        # 3) explicit file scheme, absolute and relative file and directory URI
        posixRelFilePath = posixpath.join(testRoot, "relative/file.ext")
        uriStrings.extend((
            ("file:///rootDir/absolute/file.ext", True, False, "file", "", "/rootDir/absolute/file.ext"),
            ("file:relative/file.ext", True, False, "file", "", posixRelFilePath),
            ("file:///absolute/directory/", True, False, "file", "", "/absolute/directory/"),
            ("file:///absolute/directory", True, True, "file", "", "/absolute/directory/")
        ))
        # 4) S3 scheme (ensured Keys as dirs and fully specified URIs work)
        uriStrings.extend((
            ("s3://bucketname/rootDir/", True, False, "s3", "bucketname", "/rootDir/"),
            ("s3://bucketname/rootDir", True, True, "s3", "bucketname", "/rootDir/"),
            ("s3://bucketname/rootDir/relative/file.ext", True, False, "s3",
             "bucketname", "/rootDir/relative/file.ext")
        ))
        # 5) HTTPS scheme
        uriStrings.extend((
            ("https://www.lsst.org/rootDir/", True, False, "https", "www.lsst.org", "/rootDir/"),
            ("https://www.lsst.org/rootDir", True, True, "https", "www.lsst.org", "/rootDir/"),
            ("https://www.lsst.org/rootDir/relative/file.ext", True, False, "https",
             "www.lsst.org", "/rootDir/relative/file.ext")
        ))

        for uriInfo in uriStrings:
            uri = ButlerURI(uriInfo[0], root=testRoot, forceAbsolute=uriInfo[1],
                            forceDirectory=uriInfo[2])
            with self.subTest(uri=uriInfo[0]):
                self.assertEqual(uri.scheme, uriInfo[3], "test scheme")
                self.assertEqual(uri.netloc, uriInfo[4], "test netloc")
                self.assertEqual(uri.path, uriInfo[5], "test path")

        # test root becomes abspath(".") when not specified, note specific
        # file:// scheme case
        uriStrings = (
            ("file://relative/file.ext", True, False, "file", "relative", "/file.ext"),
            ("file:relative/file.ext", False, False, "file", "", os.path.abspath("relative/file.ext")),
            ("file:relative/dir/", True, True, "file", "", os.path.abspath("relative/dir")+"/"),
            ("relative/file.ext", True, False, "", "", os.path.abspath("relative/file.ext"))
        )

        for uriInfo in uriStrings:
            uri = ButlerURI(uriInfo[0], forceAbsolute=uriInfo[1], forceDirectory=uriInfo[2])
            with self.subTest(uri=uriInfo[0]):
                self.assertEqual(uri.scheme, uriInfo[3], "test scheme")
                self.assertEqual(uri.netloc, uriInfo[4], "test netloc")
                self.assertEqual(uri.path, uriInfo[5], "test path")

        # File replacement
        uriStrings = (
            ("relative/file.ext", "newfile.fits", "relative/newfile.fits"),
            ("relative/", "newfile.fits", "relative/newfile.fits"),
            ("https://www.lsst.org/butler/", "butler.yaml", "/butler/butler.yaml"),
            ("s3://amazon/datastore/", "butler.yaml", "/datastore/butler.yaml"),
            ("s3://amazon/datastore/mybutler.yaml", "butler.yaml", "/datastore/butler.yaml")
        )

        for uriInfo in uriStrings:
            uri = ButlerURI(uriInfo[0], forceAbsolute=False)
            uri.updateFile(uriInfo[1])
            with self.subTest(uri=uriInfo[0]):
                self.assertEqual(uri.path, uriInfo[2])

        # Check that schemeless can become file scheme
        schemeless = ButlerURI("relative/path.ext")
        filescheme = ButlerURI("/absolute/path.ext")
        self.assertFalse(schemeless.scheme)
        self.assertEqual(filescheme.scheme, "file")
        self.assertNotEqual(type(schemeless), type(filescheme))

        # Copy constructor
        uri = ButlerURI("s3://amazon/datastore", forceDirectory=True)
        uri2 = ButlerURI(uri)
        self.assertEqual(uri, uri2)
        uri = ButlerURI("file://amazon/datastore/file.txt")
        uri2 = ButlerURI(uri)
        self.assertEqual(uri, uri2)

        # Copy constructor using subclass
        uri3 = type(uri)(uri)
        self.assertEqual(type(uri), type(uri3))

        # Explicit copy
        uri4 = copy.copy(uri3)
        self.assertEqual(uri4, uri3)
        uri4 = copy.deepcopy(uri3)
        self.assertEqual(uri4, uri3)

    def testUriRoot(self):
        osPathRoot = pathlib.Path(__file__).absolute().root
        rootUris = (osPathRoot, "s3://bucket", "file://localhost/", "https://a.b.com")
        for uri_str in rootUris:
            uri = ButlerURI(uri_str, forceDirectory=True)
            self.assertEqual(uri.relativeToPathRoot, "./", f"Testing uri: {uri}")
            self.assertTrue(uri.is_root, f"Testing URI {uri} is a root URI")

        exampleLocalFile = os.path.join(osPathRoot, "a", "b", "c")
        uriStrings = (
            ("file://localhost/file.ext", "file.ext"),
            (exampleLocalFile, os.path.join("a", "b", "c")),
            ("s3://bucket/path/file.ext", "path/file.ext"),
            ("https://host.com/a/b/c.d", "a/b/c.d"),
        )

        for uri_str, result in uriStrings:
            uri = ButlerURI(uri_str)
            self.assertEqual(uri.relativeToPathRoot, result)

    def testUriJoin(self):
        uri = ButlerURI("a/b/c/d", forceDirectory=True, forceAbsolute=False)
        uri2 = uri.join("e/f/g.txt")
        self.assertEqual(str(uri2), "a/b/c/d/e/f/g.txt", f"Checking joined URI {uri} -> {uri2}")

        uri = ButlerURI("a/b/c/d/old.txt", forceAbsolute=False)
        uri2 = uri.join("e/f/g.txt")
        self.assertEqual(str(uri2), "a/b/c/d/e/f/g.txt", f"Checking joined URI {uri} -> {uri2}")

        uri = ButlerURI("a/b/c/d", forceDirectory=True, forceAbsolute=True)
        uri2 = uri.join("e/f/g.txt")
        self.assertTrue(str(uri2).endswith("a/b/c/d/e/f/g.txt"), f"Checking joined URI {uri} -> {uri2}")

        uri = ButlerURI("s3://bucket/a/b/c/d", forceDirectory=True)
        uri2 = uri.join("newpath/newfile.txt")
        self.assertEqual(str(uri2), "s3://bucket/a/b/c/d/newpath/newfile.txt")

        uri = ButlerURI("s3://bucket/a/b/c/d/old.txt")
        uri2 = uri.join("newpath/newfile.txt")
        self.assertEqual(str(uri2), "s3://bucket/a/b/c/d/newpath/newfile.txt")

    def testButlerUriSerialization(self):
        """Test that we can pickle and yaml"""
        uri = ButlerURI("a/b/c/d")
        uri2 = pickle.loads(pickle.dumps(uri))
        self.assertEqual(uri, uri2)
        self.assertFalse(uri2.dirLike)

        uri = ButlerURI("a/b/c/d", forceDirectory=True)
        uri2 = pickle.loads(pickle.dumps(uri))
        self.assertEqual(uri, uri2)
        self.assertTrue(uri2.dirLike)

    def testUriExtensions(self):
        """Test extension extraction."""

        files = (("file.fits.gz", ".fits.gz"),
                 ("file.fits", ".fits"),
                 ("file.fits.xz", ".fits.xz"),
                 ("file.fits.tar", ".tar"),
                 ("file", ""),
                 ("flat_i_sim_1.4_blah.fits.gz", ".fits.gz"),
                 ("flat_i_sim_1.4_blah.txt", ".txt"),
                 ("flat_i_sim_1.4_blah.fits.fz", ".fits.fz"),
                 ("flat_i_sim_1.4_blah.fits.txt", ".txt"),
                 )

        for file, expected in files:
            uri = ButlerURI(f"a/b/{file}")
            self.assertEqual(uri.getExtension(), expected)

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
        self.assertTrue(loc1.uri.geturl().endswith("file.fits"),
                        f"Checking 'fits' extension in {loc1.uri}")
        loc1.updateExtension("fits.gz")
        self.assertEqual(loc1.uri.basename(), "file.fits.gz")
        self.assertTrue(loc1.uri.geturl().endswith("file.fits.gz"),
                        f"Checking 'fits.gz' extension in {loc1.uri}")
        self.assertEqual(loc1.getExtension(), ".fits.gz")
        loc1.updateExtension(".jpeg")
        self.assertTrue(loc1.uri.geturl().endswith("file.jpeg"),
                        f"Checking 'jpeg' extension in {loc1.uri}")
        loc1.updateExtension(None)
        self.assertTrue(loc1.uri.geturl().endswith("file.jpeg"),
                        f"Checking unchanged extension in {loc1.uri}")
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

        for pathInStore in ("relative/path/file.ext.gz",
                            "relative/path+2/file.ext.gz",
                            "relative/path+3/file&.ext.gz"):
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

    def testPosix2OS(self):
        """Test round tripping of the posix to os.path conversion helpers."""
        testPaths = ("/a/b/c.e", "a/b", "a/b/", "/a/b", "/a/b/", "a/b/c.e")
        for p in testPaths:
            with self.subTest(path=p):
                self.assertEqual(os2posix(posix2os(p)), p)

    def testSplit(self):
        """Tests split functionality."""
        testRoot = "/tmp/"

        testPaths = ("/absolute/file.ext", "/absolute/",
                     "file:///absolute/file.ext", "file:///absolute/",
                     "s3://bucket/root/file.ext", "s3://bucket/root/",
                     "https://www.lsst.org/root/file.ext", "https://www.lsst.org/root/",
                     "relative/file.ext", "relative/")

        osRelExpected = os.path.join(testRoot, "relative")
        expected = (("file:///absolute/", "file.ext"), ("file:///absolute/", ""),
                    ("file:///absolute/", "file.ext"), ("file:///absolute/", ""),
                    ("s3://bucket/root/", "file.ext"), ("s3://bucket/root/", ""),
                    ("https://www.lsst.org/root/", "file.ext"), ("https://www.lsst.org/root/", ""),
                    (f"file://{osRelExpected}/", "file.ext"), (f"file://{osRelExpected}/", ""))

        for p, e in zip(testPaths, expected):
            with self.subTest(path=p):
                uri = ButlerURI(p, testRoot)
                head, tail = uri.split()
                self.assertEqual((head.geturl(), tail), e)

        # explicit file scheme should force posixpath, check os.path is ignored
        posixRelFilePath = posixpath.join(testRoot, "relative")
        uri = ButlerURI("file:relative/file.ext", testRoot)
        head, tail = uri.split()
        self.assertEqual((head.geturl(), tail), (f"file://{posixRelFilePath}/", "file.ext"))

        # check head can be empty and we do not get an absolute path back
        uri = ButlerURI("file.ext", forceAbsolute=False)
        head, tail = uri.split()
        self.assertEqual((head.geturl(), tail), ("./", "file.ext"))

        # ensure empty path splits to a directory URL
        uri = ButlerURI("", forceAbsolute=False)
        head, tail = uri.split()
        self.assertEqual((head.geturl(), tail), ("./", ""))

        uri = ButlerURI(".", forceAbsolute=False)
        head, tail = uri.split()
        self.assertEqual((head.geturl(), tail), ("./", ""))


if __name__ == "__main__":
    unittest.main()

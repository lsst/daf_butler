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

import glob
import os
import shutil
import unittest
import urllib.parse
import responses
import pathlib

try:
    import boto3
    import botocore
    from moto import mock_s3
except ImportError:
    boto3 = None

    def mock_s3(cls):
        """A no-op decorator in case moto mock_s3 can not be imported.
        """
        return cls

from lsst.daf.butler import ButlerURI
from lsst.daf.butler.core._butlerUri.s3utils import (setAwsEnvCredentials,
                                                     unsetAwsEnvCredentials)
from lsst.daf.butler.tests.utils import makeTestTempDir, removeTestTempDir

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class FileURITestCase(unittest.TestCase):
    """Concrete tests for local files"""

    def setUp(self):
        # Use a local tempdir because on macOS the temp dirs use symlinks
        # so relsymlink gets quite confused.
        self.tmpdir = makeTestTempDir(TESTDIR)

    def tearDown(self):
        removeTestTempDir(self.tmpdir)

    def testFile(self):
        file = os.path.join(self.tmpdir, "test.txt")
        uri = ButlerURI(file)
        self.assertFalse(uri.exists(), f"{uri} should not exist")
        self.assertEqual(uri.ospath, file)

        path = pathlib.Path(file)
        uri = ButlerURI(path)
        self.assertEqual(uri.ospath, file)

        content = "abcdefghijklmnopqrstuv\n"
        uri.write(content.encode())
        self.assertTrue(os.path.exists(file), "File should exist locally")
        self.assertTrue(uri.exists(), f"{uri} should now exist")
        self.assertEqual(uri.read().decode(), content)
        self.assertEqual(uri.size(), len(content.encode()))

        with self.assertRaises(FileNotFoundError):
            ButlerURI("file/not/there.txt").size()

        # Check that creating a URI from a URI returns the same thing
        uri2 = ButlerURI(uri)
        self.assertEqual(uri, uri2)
        self.assertEqual(id(uri), id(uri2))

        with self.assertRaises(ValueError):
            # Scheme-less URIs are not allowed to support non-file roots
            # at the present time. This may change in the future to become
            # equivalent to ButlerURI.join()
            ButlerURI("a/b.txt", root=ButlerURI("s3://bucket/a/b/"))

    def testExtension(self):
        file = ButlerURI(os.path.join(self.tmpdir, "test.txt"))
        self.assertEqual(file.updatedExtension(None), file)
        self.assertEqual(file.updatedExtension(".txt"), file)
        self.assertEqual(id(file.updatedExtension(".txt")), id(file))

        fits = file.updatedExtension(".fits.gz")
        self.assertEqual(fits.basename(), "test.fits.gz")
        self.assertEqual(fits.updatedExtension(".jpeg").basename(), "test.jpeg")

    def testRelative(self):
        """Check that we can get subpaths back from two URIs"""
        parent = ButlerURI(self.tmpdir, forceDirectory=True, forceAbsolute=True)
        self.assertTrue(parent.isdir())
        child = ButlerURI(os.path.join(self.tmpdir, "dir1", "file.txt"), forceAbsolute=True)

        self.assertEqual(child.relative_to(parent), "dir1/file.txt")

        not_child = ButlerURI("/a/b/dir1/file.txt")
        self.assertIsNone(not_child.relative_to(parent))
        self.assertFalse(not_child.isdir())

        not_directory = ButlerURI(os.path.join(self.tmpdir, "dir1", "file2.txt"))
        self.assertIsNone(child.relative_to(not_directory))

        # Relative URIs
        parent = ButlerURI("a/b/", forceAbsolute=False)
        child = ButlerURI("a/b/c/d.txt", forceAbsolute=False)
        self.assertFalse(child.scheme)
        self.assertEqual(child.relative_to(parent), "c/d.txt")

        # File URI and schemeless URI
        parent = ButlerURI("file:/a/b/c/")
        child = ButlerURI("e/f/g.txt", forceAbsolute=False)

        # If the child is relative and the parent is absolute we assume
        # that the child is a child of the parent unless it uses ".."
        self.assertEqual(child.relative_to(parent), "e/f/g.txt")

        child = ButlerURI("../e/f/g.txt", forceAbsolute=False)
        self.assertIsNone(child.relative_to(parent))

        child = ButlerURI("../c/e/f/g.txt", forceAbsolute=False)
        self.assertEqual(child.relative_to(parent), "e/f/g.txt")

        # Test non-file root with relative path.
        child = ButlerURI("e/f/g.txt", forceAbsolute=False)
        parent = ButlerURI("s3://hello/a/b/c/")
        self.assertEqual(child.relative_to(parent), "e/f/g.txt")

        # Test with different netloc
        child = ButlerURI("http://my.host/a/b/c.txt")
        parent = ButlerURI("http://other.host/a/")
        self.assertIsNone(child.relative_to(parent), f"{child}.relative_to({parent})")

        # Schemeless absolute child.
        # Schemeless absolute URI is constructed using root= parameter.
        parent = ButlerURI("file:///a/b/c/")
        child = ButlerURI("d/e.txt", root=parent)
        self.assertEqual(child.relative_to(parent), "d/e.txt", f"{child}.relative_to({parent})")

        parent = ButlerURI("c/", root="/a/b/")
        self.assertEqual(child.relative_to(parent), "d/e.txt", f"{child}.relative_to({parent})")

        # Absolute schemeless child with relative parent will always fail.
        parent = ButlerURI("d/e.txt", forceAbsolute=False)
        self.assertIsNone(child.relative_to(parent), f"{child}.relative_to({parent})")

    def testParents(self):
        """Test of splitting and parent walking."""
        parent = ButlerURI(self.tmpdir, forceDirectory=True, forceAbsolute=True)
        child_file = parent.join("subdir/file.txt")
        self.assertFalse(child_file.isdir())
        child_subdir, file = child_file.split()
        self.assertEqual(file, "file.txt")
        self.assertTrue(child_subdir.isdir())
        self.assertEqual(child_file.dirname(), child_subdir)
        self.assertEqual(child_file.basename(), file)
        self.assertEqual(child_file.parent(), child_subdir)
        derived_parent = child_subdir.parent()
        self.assertEqual(derived_parent, parent)
        self.assertTrue(derived_parent.isdir())
        self.assertEqual(child_file.parent().parent(), parent)

    def testEnvVar(self):
        """Test that environment variables are expanded."""

        with unittest.mock.patch.dict(os.environ, {"MY_TEST_DIR": "/a/b/c"}):
            uri = ButlerURI("${MY_TEST_DIR}/d.txt")
        self.assertEqual(uri.path, "/a/b/c/d.txt")
        self.assertEqual(uri.scheme, "file")

        # This will not expand
        uri = ButlerURI("${MY_TEST_DIR}/d.txt", forceAbsolute=False)
        self.assertEqual(uri.path, "${MY_TEST_DIR}/d.txt")
        self.assertFalse(uri.scheme)

    def testMkdir(self):
        tmpdir = ButlerURI(self.tmpdir)
        newdir = tmpdir.join("newdir/seconddir")
        newdir.mkdir()
        self.assertTrue(newdir.exists())
        newfile = newdir.join("temp.txt")
        newfile.write("Data".encode())
        self.assertTrue(newfile.exists())

    def testTransfer(self):
        src = ButlerURI(os.path.join(self.tmpdir, "test.txt"))
        content = "Content is some content\nwith something to say\n\n"
        src.write(content.encode())

        for mode in ("copy", "link", "hardlink", "symlink", "relsymlink"):
            dest = ButlerURI(os.path.join(self.tmpdir, f"dest_{mode}.txt"))
            dest.transfer_from(src, transfer=mode)
            self.assertTrue(dest.exists(), f"Check that {dest} exists (transfer={mode})")

            with open(dest.ospath, "r") as fh:
                new_content = fh.read()
            self.assertEqual(new_content, content)

            if mode in ("symlink", "relsymlink"):
                self.assertTrue(os.path.islink(dest.ospath), f"Check that {dest} is symlink")

            # If the source and destination are hardlinks of each other
            # the transfer should work even if overwrite=False.
            if mode in ("link", "hardlink"):
                dest.transfer_from(src, transfer=mode)
            else:
                with self.assertRaises(FileExistsError,
                                       msg=f"Overwrite of {dest} should not be allowed ({mode})"):
                    dest.transfer_from(src, transfer=mode)

            dest.transfer_from(src, transfer=mode, overwrite=True)

            os.remove(dest.ospath)

        b = src.read()
        self.assertEqual(b.decode(), new_content)

        nbytes = 10
        subset = src.read(size=nbytes)
        self.assertEqual(len(subset), nbytes)
        self.assertEqual(subset.decode(), content[:nbytes])

        with self.assertRaises(ValueError):
            src.transfer_from(src, transfer="unknown")

    def testTransferIdentical(self):
        """Test overwrite of identical files."""
        dir1 = ButlerURI(os.path.join(self.tmpdir, "dir1"), forceDirectory=True)
        dir1.mkdir()
        dir2 = os.path.join(self.tmpdir, "dir2")
        os.symlink(dir1.ospath, dir2)

        # Write a test file.
        src_file = dir1.join("test.txt")
        content = "0123456"
        src_file.write(content.encode())

        # Construct URI to destination that should be identical.
        dest_file = ButlerURI(os.path.join(dir2), forceDirectory=True).join("test.txt")
        self.assertTrue(dest_file.exists())
        self.assertNotEqual(src_file, dest_file)

        # Transfer it over itself.
        dest_file.transfer_from(src_file, transfer="symlink", overwrite=True)
        new_content = dest_file.read().decode()
        self.assertEqual(content, new_content)

    def testResource(self):
        u = ButlerURI("resource://lsst.daf.butler/configs/datastore.yaml")
        self.assertTrue(u.exists(), f"Check {u} exists")

        content = u.read().decode()
        self.assertTrue(content.startswith("datastore:"))

        truncated = u.read(size=9).decode()
        self.assertEqual(truncated, "datastore")

        d = ButlerURI("resource://lsst.daf.butler/configs", forceDirectory=True)
        self.assertTrue(u.exists(), f"Check directory {d} exists")

        j = d.join("datastore.yaml")
        self.assertEqual(u, j)
        self.assertFalse(j.dirLike)
        self.assertFalse(j.isdir())
        not_there = d.join("not-there.yaml")
        self.assertFalse(not_there.exists())

        bad = ButlerURI("resource://bad.module/not.yaml")
        multi = ButlerURI.mexists([u, bad, not_there])
        self.assertTrue(multi[u])
        self.assertFalse(multi[bad])
        self.assertFalse(multi[not_there])

    def testEscapes(self):
        """Special characters in file paths"""
        src = ButlerURI("bbb/???/test.txt", root=self.tmpdir, forceAbsolute=True)
        self.assertFalse(src.scheme)
        src.write(b"Some content")
        self.assertTrue(src.exists())

        # abspath always returns a file scheme
        file = src.abspath()
        self.assertTrue(file.exists())
        self.assertIn("???", file.ospath)
        self.assertNotIn("???", file.path)

        file = file.updatedFile("tests??.txt")
        self.assertNotIn("??.txt", file.path)
        file.write(b"Other content")
        self.assertEqual(file.read(), b"Other content")

        src = src.updatedFile("tests??.txt")
        self.assertIn("??.txt", src.path)
        self.assertEqual(file.read(), src.read(), f"reading from {file.ospath} and {src.ospath}")

        # File URI and schemeless URI
        parent = ButlerURI("file:" + urllib.parse.quote("/a/b/c/de/??/"))
        child = ButlerURI("e/f/g.txt", forceAbsolute=False)
        self.assertEqual(child.relative_to(parent), "e/f/g.txt")

        child = ButlerURI("e/f??#/g.txt", forceAbsolute=False)
        self.assertEqual(child.relative_to(parent), "e/f??#/g.txt")

        child = ButlerURI("file:" + urllib.parse.quote("/a/b/c/de/??/e/f??#/g.txt"))
        self.assertEqual(child.relative_to(parent), "e/f??#/g.txt")

        self.assertEqual(child.relativeToPathRoot, "a/b/c/de/??/e/f??#/g.txt")

        # Schemeless so should not quote
        dir = ButlerURI("bbb/???/", root=self.tmpdir, forceAbsolute=True, forceDirectory=True)
        self.assertIn("???", dir.ospath)
        self.assertIn("???", dir.path)
        self.assertFalse(dir.scheme)

        # dir.join() morphs into a file scheme
        new = dir.join("test_j.txt")
        self.assertIn("???", new.ospath, f"Checking {new}")
        new.write(b"Content")

        new2name = "###/test??.txt"
        new2 = dir.join(new2name)
        self.assertIn("???", new2.ospath)
        new2.write(b"Content")
        self.assertTrue(new2.ospath.endswith(new2name))
        self.assertEqual(new.read(), new2.read())

        fdir = dir.abspath()
        self.assertNotIn("???", fdir.path)
        self.assertIn("???", fdir.ospath)
        self.assertEqual(fdir.scheme, "file")
        fnew = dir.join("test_jf.txt")
        fnew.write(b"Content")

        fnew2 = fdir.join(new2name)
        fnew2.write(b"Content")
        self.assertTrue(fnew2.ospath.endswith(new2name))
        self.assertNotIn("###", fnew2.path)

        self.assertEqual(fnew.read(), fnew2.read())

        # Test that children relative to schemeless and file schemes
        # still return the same unquoted name
        self.assertEqual(fnew2.relative_to(fdir), new2name, f"{fnew2}.relative_to({fdir})")
        self.assertEqual(fnew2.relative_to(dir), new2name, f"{fnew2}.relative_to({dir})")
        self.assertEqual(new2.relative_to(fdir), new2name, f"{new2}.relative_to({fdir})")
        self.assertEqual(new2.relative_to(dir), new2name, f"{new2}.relative_to({dir})")

        # Check for double quoting
        plus_path = "/a/b/c+d/"
        with self.assertLogs(level="WARNING"):
            uri = ButlerURI(urllib.parse.quote(plus_path), forceDirectory=True)
        self.assertEqual(uri.ospath, plus_path)

        # Check that # is not escaped for schemeless URIs
        hash_path = "/a/b#/c&d#xyz"
        hpos = hash_path.rfind("#")
        uri = ButlerURI(hash_path)
        self.assertEqual(uri.ospath, hash_path[:hpos])
        self.assertEqual(uri.fragment, hash_path[hpos + 1:])

    def testHash(self):
        """Test that we can store URIs in sets and as keys."""
        uri1 = ButlerURI(TESTDIR)
        uri2 = uri1.join("test/")
        s = {uri1, uri2}
        self.assertIn(uri1, s)

        d = {uri1: "1", uri2: "2"}
        self.assertEqual(d[uri2], "2")

    def testWalk(self):
        """Test ButlerURI.walk()."""
        test_dir_uri = ButlerURI(TESTDIR)

        file = test_dir_uri.join("config/basic/butler.yaml")
        found = list(ButlerURI.findFileResources([file]))
        self.assertEqual(found[0], file)

        # Compare against the full local paths
        expected = set(p for p in glob.glob(os.path.join(TESTDIR, "config", "**"), recursive=True)
                       if os.path.isfile(p))
        found = set(u.ospath for u in ButlerURI.findFileResources([test_dir_uri.join("config")]))
        self.assertEqual(found, expected)

        # Now solely the YAML files
        expected_yaml = set(glob.glob(os.path.join(TESTDIR, "config", "**", "*.yaml"), recursive=True))
        found = set(u.ospath for u in ButlerURI.findFileResources([test_dir_uri.join("config")],
                                                                  file_filter=r".*\.yaml$"))
        self.assertEqual(found, expected_yaml)

        # Now two explicit directories and a file
        expected = set(glob.glob(os.path.join(TESTDIR, "config", "**", "basic", "*.yaml"), recursive=True))
        expected.update(set(glob.glob(os.path.join(TESTDIR, "config", "**", "templates", "*.yaml"),
                                      recursive=True)))
        expected.add(file.ospath)

        found = set(u.ospath for u in ButlerURI.findFileResources([file, test_dir_uri.join("config/basic"),
                                                                   test_dir_uri.join("config/templates")],
                                                                  file_filter=r".*\.yaml$"))
        self.assertEqual(found, expected)

        # Group by directory -- find everything and compare it with what
        # we expected to be there in total. We expect to find 9 directories
        # containing yaml files so make sure we only iterate 9 times.
        found_yaml = set()
        counter = 0
        for uris in ButlerURI.findFileResources([file, test_dir_uri.join("config/")],
                                                file_filter=r".*\.yaml$", grouped=True):
            found = set(u.ospath for u in uris)
            if found:
                counter += 1

            found_yaml.update(found)

        self.assertEqual(found_yaml, expected_yaml)
        self.assertEqual(counter, 9)

        # Grouping but check that single files are returned in a single group
        # at the end
        file2 = test_dir_uri.join("config/templates/templates-bad.yaml")
        found = list(ButlerURI.findFileResources([file, file2, test_dir_uri.join("config/dbAuth")],
                                                 grouped=True))
        self.assertEqual(len(found), 2)
        self.assertEqual(list(found[1]), [file, file2])

        with self.assertRaises(ValueError):
            list(file.walk())

    def testRootURI(self):
        """Test ButlerURI.root_uri()."""
        uri = ButlerURI("https://www.notexist.com:8080/file/test")
        uri2 = ButlerURI("s3://www.notexist.com/file/test")
        self.assertEqual(uri.root_uri().geturl(), "https://www.notexist.com:8080/")
        self.assertEqual(uri2.root_uri().geturl(), "s3://www.notexist.com/")

    def testJoin(self):
        """Test .join method."""

        root_str = "s3://bucket/hsc/payload/"
        root = ButlerURI(root_str)

        self.assertEqual(root.join("b/test.txt").geturl(), f"{root_str}b/test.txt")
        add_dir = root.join("b/c/d/")
        self.assertTrue(add_dir.isdir())
        self.assertEqual(add_dir.geturl(), f"{root_str}b/c/d/")

        up_relative = root.join("../b/c.txt")
        self.assertFalse(up_relative.isdir())
        self.assertEqual(up_relative.geturl(), "s3://bucket/hsc/b/c.txt")

        quote_example = "b&c.t@x#t"
        needs_quote = root.join(quote_example)
        self.assertEqual(needs_quote.unquoted_path, f"/hsc/payload/{quote_example}")

        other = ButlerURI("file://localhost/test.txt")
        self.assertEqual(root.join(other), other)
        self.assertEqual(other.join("b/new.txt").geturl(), "file://localhost/b/new.txt")

        joined = ButlerURI("s3://bucket/hsc/payload/").join(ButlerURI("test.qgraph", forceAbsolute=False))
        self.assertEqual(joined, ButlerURI("s3://bucket/hsc/payload/test.qgraph"))

        with self.assertRaises(ValueError):
            ButlerURI("s3://bucket/hsc/payload/").join(ButlerURI("test.qgraph"))

    def testTemporary(self):
        with ButlerURI.temporary_uri(suffix=".json") as tmp:
            self.assertEqual(tmp.getExtension(), ".json", f"uri: {tmp}")
            self.assertTrue(tmp.isabs(), f"uri: {tmp}")
            self.assertFalse(tmp.exists(), f"uri: {tmp}")
            tmp.write(b"abcd")
            self.assertTrue(tmp.exists(), f"uri: {tmp}")
            self.assertTrue(tmp.isTemporary)
        self.assertFalse(tmp.exists(), f"uri: {tmp}")

        tmpdir = ButlerURI(self.tmpdir, forceDirectory=True)
        with ButlerURI.temporary_uri(prefix=tmpdir, suffix=".yaml") as tmp:
            # Use a specified tmpdir and check it is okay for the file
            # to not be created.
            self.assertFalse(tmp.exists(), f"uri: {tmp}")
        self.assertTrue(tmpdir.exists(), f"uri: {tmpdir} still exists")


@unittest.skipIf(not boto3, "Warning: boto3 AWS SDK not found!")
@mock_s3
class S3URITestCase(unittest.TestCase):
    """Tests involving S3"""

    bucketName = "any_bucket"
    """Bucket name to use in tests"""

    def setUp(self):
        # Local test directory
        self.tmpdir = makeTestTempDir(TESTDIR)

        # set up some fake credentials if they do not exist
        self.usingDummyCredentials = setAwsEnvCredentials()

        # MOTO needs to know that we expect Bucket bucketname to exist
        s3 = boto3.resource("s3")
        s3.create_bucket(Bucket=self.bucketName)

    def tearDown(self):
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(self.bucketName)
        try:
            bucket.objects.all().delete()
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                # the key was not reachable - pass
                pass
            else:
                raise

        bucket = s3.Bucket(self.bucketName)
        bucket.delete()

        # unset any potentially set dummy credentials
        if self.usingDummyCredentials:
            unsetAwsEnvCredentials()

        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def makeS3Uri(self, path):
        return f"s3://{self.bucketName}/{path}"

    def testTransfer(self):
        src = ButlerURI(os.path.join(self.tmpdir, "test.txt"))
        content = "Content is some content\nwith something to say\n\n"
        src.write(content.encode())
        self.assertTrue(src.exists())
        self.assertEqual(src.size(), len(content.encode()))

        dest = ButlerURI(self.makeS3Uri("test.txt"))
        self.assertFalse(dest.exists())

        with self.assertRaises(FileNotFoundError):
            dest.size()

        dest.transfer_from(src, transfer="copy")
        self.assertTrue(dest.exists())

        dest2 = ButlerURI(self.makeS3Uri("copied.txt"))
        dest2.transfer_from(dest, transfer="copy")
        self.assertTrue(dest2.exists())

        local = ButlerURI(os.path.join(self.tmpdir, "copied.txt"))
        local.transfer_from(dest2, transfer="copy")
        with open(local.ospath, "r") as fd:
            new_content = fd.read()
        self.assertEqual(new_content, content)

        with self.assertRaises(ValueError):
            dest2.transfer_from(local, transfer="symlink")

        b = dest.read()
        self.assertEqual(b.decode(), new_content)

        nbytes = 10
        subset = dest.read(size=nbytes)
        self.assertEqual(len(subset), nbytes)  # Extra byte comes back
        self.assertEqual(subset.decode(), content[:nbytes])

        with self.assertRaises(FileExistsError):
            dest.transfer_from(src, transfer="copy")

        dest.transfer_from(src, transfer="copy", overwrite=True)

    def testWalk(self):
        """Test that we can list an S3 bucket"""
        # Files we want to create
        expected = ("a/x.txt", "a/y.txt", "a/z.json", "a/b/w.txt", "a/b/c/d/v.json")
        expected_uris = [ButlerURI(self.makeS3Uri(path)) for path in expected]
        for uri in expected_uris:
            # Doesn't matter what we write
            uri.write("123".encode())

        # Find all the files in the a/ tree
        found = set(uri.path for uri in ButlerURI.findFileResources([ButlerURI(self.makeS3Uri("a/"))]))
        self.assertEqual(found, {uri.path for uri in expected_uris})

        # Find all the files in the a/ tree but group by folder
        found = ButlerURI.findFileResources([ButlerURI(self.makeS3Uri("a/"))],
                                            grouped=True)
        expected = (("/a/x.txt", "/a/y.txt", "/a/z.json"), ("/a/b/w.txt",), ("/a/b/c/d/v.json",))

        for got, expect in zip(found, expected):
            self.assertEqual(tuple(u.path for u in got), expect)

        # Find only JSON files
        found = set(uri.path for uri in ButlerURI.findFileResources([ButlerURI(self.makeS3Uri("a/"))],
                                                                    file_filter=r"\.json$"))
        self.assertEqual(found, {uri.path for uri in expected_uris if uri.path.endswith(".json")})

        # JSON files grouped by directory
        found = ButlerURI.findFileResources([ButlerURI(self.makeS3Uri("a/"))],
                                            file_filter=r"\.json$", grouped=True)
        expected = (("/a/z.json",), ("/a/b/c/d/v.json",))

        for got, expect in zip(found, expected):
            self.assertEqual(tuple(u.path for u in got), expect)

        # Check pagination works with large numbers of files. S3 API limits
        # us to 1000 response per list_objects call so create lots of files
        created = set()
        counter = 1
        n_dir1 = 1100
        while counter <= n_dir1:
            new = ButlerURI(self.makeS3Uri(f"test/file{counter:04d}.txt"))
            new.write(f"{counter}".encode())
            created.add(str(new))
            counter += 1
        counter = 1
        # Put some in a subdirectory to make sure we are looking in a
        # hierarchy.
        n_dir2 = 100
        while counter <= n_dir2:
            new = ButlerURI(self.makeS3Uri(f"test/subdir/file{counter:04d}.txt"))
            new.write(f"{counter}".encode())
            created.add(str(new))
            counter += 1

        found = ButlerURI.findFileResources([ButlerURI(self.makeS3Uri("test/"))])
        self.assertEqual({str(u) for u in found}, created)

        # Again with grouping.
        found = list(ButlerURI.findFileResources([ButlerURI(self.makeS3Uri("test/"))], grouped=True))
        self.assertEqual(len(found), 2)
        dir_1 = list(found[0])
        dir_2 = list(found[1])
        self.assertEqual(len(dir_1), n_dir1)
        self.assertEqual(len(dir_2), n_dir2)

    def testWrite(self):
        s3write = ButlerURI(self.makeS3Uri("created.txt"))
        content = "abcdefghijklmnopqrstuv\n"
        s3write.write(content.encode())
        self.assertEqual(s3write.read().decode(), content)

    def testTemporary(self):
        s3root = ButlerURI(self.makeS3Uri("rootdir"), forceDirectory=True)
        with ButlerURI.temporary_uri(prefix=s3root, suffix=".json") as tmp:
            self.assertEqual(tmp.getExtension(), ".json", f"uri: {tmp}")
            self.assertEqual(tmp.scheme, "s3", f"uri: {tmp}")
            self.assertEqual(tmp.parent(), s3root)
            basename = tmp.basename()
            content = "abcd"
            tmp.write(content.encode())
            self.assertTrue(tmp.exists(), f"uri: {tmp}")
        self.assertFalse(tmp.exists())

        # Again without writing anything, to check that there is no complaint
        # on exit of context manager.
        with ButlerURI.temporary_uri(prefix=s3root, suffix=".json") as tmp:
            self.assertFalse(tmp.exists())
            # Check that the file has a different name than before.
            self.assertNotEqual(tmp.basename(), basename, f"uri: {tmp}")
        self.assertFalse(tmp.exists())

    def testRelative(self):
        """Check that we can get subpaths back from two URIs"""
        parent = ButlerURI(self.makeS3Uri("rootdir"), forceDirectory=True)
        child = ButlerURI(self.makeS3Uri("rootdir/dir1/file.txt"))

        self.assertEqual(child.relative_to(parent), "dir1/file.txt")

        not_child = ButlerURI(self.makeS3Uri("/a/b/dir1/file.txt"))
        self.assertFalse(not_child.relative_to(parent))

        not_s3 = ButlerURI(os.path.join(self.tmpdir, "dir1", "file2.txt"))
        self.assertFalse(child.relative_to(not_s3))

    def testQuoting(self):
        """Check that quoting works."""
        parent = ButlerURI(self.makeS3Uri("rootdir"), forceDirectory=True)
        subpath = "rootdir/dir1+/file?.txt"
        child = ButlerURI(self.makeS3Uri(urllib.parse.quote(subpath)))

        self.assertEqual(child.relative_to(parent), "dir1+/file?.txt")
        self.assertEqual(child.basename(), "file?.txt")
        self.assertEqual(child.relativeToPathRoot, subpath)
        self.assertIn("%", child.path)
        self.assertEqual(child.unquoted_path, "/" + subpath)


# Mock required environment variables during tests
@unittest.mock.patch.dict(os.environ, {"LSST_BUTLER_WEBDAV_AUTH": "TOKEN",
                                       "LSST_BUTLER_WEBDAV_TOKEN_FILE": os.path.join(
                                           TESTDIR, "config/testConfigs/webdav/token"),
                                       "LSST_BUTLER_WEBDAV_CA_BUNDLE": "/path/to/ca/certs"})
class WebdavURITestCase(unittest.TestCase):

    def setUp(self):
        serverRoot = "www.not-exists.orgx"
        existingFolderName = "existingFolder"
        existingFileName = "existingFile"
        notExistingFileName = "notExistingFile"

        self.baseURL = ButlerURI(
            f"https://{serverRoot}", forceDirectory=True)
        self.existingFileButlerURI = ButlerURI(
            f"https://{serverRoot}/{existingFolderName}/{existingFileName}")
        self.notExistingFileButlerURI = ButlerURI(
            f"https://{serverRoot}/{existingFolderName}/{notExistingFileName}")
        self.existingFolderButlerURI = ButlerURI(
            f"https://{serverRoot}/{existingFolderName}", forceDirectory=True)
        self.notExistingFolderButlerURI = ButlerURI(
            f"https://{serverRoot}/{notExistingFileName}", forceDirectory=True)

        # Need to declare the options
        responses.add(responses.OPTIONS,
                      self.baseURL.geturl(),
                      status=200, headers={"DAV": "1,2,3"})

        # Used by ButlerHttpURI.exists()
        responses.add(responses.HEAD,
                      self.existingFileButlerURI.geturl(),
                      status=200, headers={'Content-Length': '1024'})
        responses.add(responses.HEAD,
                      self.notExistingFileButlerURI.geturl(),
                      status=404)

        # Used by ButlerHttpURI.read()
        responses.add(responses.GET,
                      self.existingFileButlerURI.geturl(),
                      status=200,
                      body=str.encode("It works!"))
        responses.add(responses.GET,
                      self.notExistingFileButlerURI.geturl(),
                      status=404)

        # Used by ButlerHttpURI.write()
        responses.add(responses.PUT,
                      self.existingFileButlerURI.geturl(),
                      status=201)

        # Used by ButlerHttpURI.transfer_from()
        responses.add(responses.Response(url=self.existingFileButlerURI.geturl(),
                                         method="COPY",
                                         headers={"Destination": self.existingFileButlerURI.geturl()},
                                         status=201))
        responses.add(responses.Response(url=self.existingFileButlerURI.geturl(),
                                         method="COPY",
                                         headers={"Destination": self.notExistingFileButlerURI.geturl()},
                                         status=201))
        responses.add(responses.Response(url=self.existingFileButlerURI.geturl(),
                                         method="MOVE",
                                         headers={"Destination": self.notExistingFileButlerURI.geturl()},
                                         status=201))

        # Used by ButlerHttpURI.remove()
        responses.add(responses.DELETE,
                      self.existingFileButlerURI.geturl(),
                      status=200)
        responses.add(responses.DELETE,
                      self.notExistingFileButlerURI.geturl(),
                      status=404)

        # Used by ButlerHttpURI.mkdir()
        responses.add(responses.HEAD,
                      self.existingFolderButlerURI.geturl(),
                      status=200, headers={'Content-Length': '1024'})
        responses.add(responses.HEAD,
                      self.baseURL.geturl(),
                      status=200, headers={'Content-Length': '1024'})
        responses.add(responses.HEAD,
                      self.notExistingFolderButlerURI.geturl(),
                      status=404)
        responses.add(responses.Response(url=self.notExistingFolderButlerURI.geturl(),
                                         method="MKCOL",
                                         status=201))
        responses.add(responses.Response(url=self.existingFolderButlerURI.geturl(),
                                         method="MKCOL",
                                         status=403))

    @responses.activate
    def testExists(self):

        self.assertTrue(self.existingFileButlerURI.exists())
        self.assertFalse(self.notExistingFileButlerURI.exists())

        self.assertEqual(self.existingFileButlerURI.size(), 1024)
        with self.assertRaises(FileNotFoundError):
            self.notExistingFileButlerURI.size()

    @responses.activate
    def testRemove(self):

        self.assertIsNone(self.existingFileButlerURI.remove())
        with self.assertRaises(FileNotFoundError):
            self.notExistingFileButlerURI.remove()

    @responses.activate
    def testMkdir(self):

        # The mock means that we can't check this now exists
        self.notExistingFolderButlerURI.mkdir()

        # This should do nothing
        self.existingFolderButlerURI.mkdir()

        with self.assertRaises(ValueError):
            self.notExistingFileButlerURI.mkdir()

    @responses.activate
    def testRead(self):

        self.assertEqual(self.existingFileButlerURI.read().decode(), "It works!")
        self.assertNotEqual(self.existingFileButlerURI.read().decode(), "Nope.")
        with self.assertRaises(FileNotFoundError):
            self.notExistingFileButlerURI.read()

    @responses.activate
    def testWrite(self):

        self.assertIsNone(self.existingFileButlerURI.write(data=str.encode("Some content.")))
        with self.assertRaises(FileExistsError):
            self.existingFileButlerURI.write(data=str.encode("Some content."), overwrite=False)

    @responses.activate
    def testTransfer(self):

        self.assertIsNone(self.notExistingFileButlerURI.transfer_from(
            src=self.existingFileButlerURI))
        self.assertIsNone(self.notExistingFileButlerURI.transfer_from(
            src=self.existingFileButlerURI,
            transfer="move"))
        with self.assertRaises(FileExistsError):
            self.existingFileButlerURI.transfer_from(src=self.existingFileButlerURI)
        with self.assertRaises(ValueError):
            self.notExistingFileButlerURI.transfer_from(
                src=self.existingFileButlerURI,
                transfer="unsupported")

    def testParent(self):

        self.assertEqual(self.existingFolderButlerURI.geturl(),
                         self.notExistingFileButlerURI.parent().geturl())
        self.assertEqual(self.baseURL.geturl(),
                         self.baseURL.parent().geturl())
        self.assertEqual(self.existingFileButlerURI.parent().geturl(),
                         self.existingFileButlerURI.dirname().geturl())


if __name__ == "__main__":
    unittest.main()

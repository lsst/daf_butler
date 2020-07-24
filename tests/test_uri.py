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
import shutil
import tempfile
import unittest

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
from lsst.daf.butler.core.s3utils import (setAwsEnvCredentials,
                                          unsetAwsEnvCredentials)

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class FileURITestCase(unittest.TestCase):
    """Concrete tests for local files"""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def testFile(self):
        file = os.path.join(self.tmpdir, "test.txt")
        uri = ButlerURI(file)
        self.assertFalse(uri.exists(), f"{uri} should not exist")
        self.assertEqual(uri.ospath, file)

        content = "abcdefghijklmnopqrstuv\n"
        uri.write(content.encode())
        self.assertTrue(os.path.exists(file), "File should exist locally")
        self.assertTrue(uri.exists(), f"{uri} should now exist")
        self.assertEqual(uri.read().decode(), content)

    def testRelative(self):
        """Check that we can get subpaths back from two URIs"""
        parent = ButlerURI(self.tmpdir, forceDirectory=True, forceAbsolute=True)
        child = ButlerURI(os.path.join(self.tmpdir, "dir1", "file.txt"), forceAbsolute=True)

        self.assertEqual(child.relative_to(parent), "dir1/file.txt")

        not_child = ButlerURI("/a/b/dir1/file.txt")
        self.assertFalse(not_child.relative_to(parent))

        not_directory = ButlerURI(os.path.join(self.tmpdir, "dir1", "file2.txt"))
        self.assertFalse(child.relative_to(not_directory))

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
        self.assertFalse(child.relative_to(parent))

        child = ButlerURI("../c/e/f/g.txt", forceAbsolute=False)
        self.assertEqual(child.relative_to(parent), "e/f/g.txt")

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
            self.assertTrue(dest.exists())

            with open(dest.ospath, "r") as fh:
                new_content = fh.read()
            self.assertEqual(new_content, content)

            if "symlink" in mode:
                self.assertTrue(os.path.islink(dest.ospath), f"Check that {dest} is symlink")

            os.remove(dest.ospath)

        b = src.read()
        self.assertEqual(b.decode(), new_content)

        nbytes = 10
        subset = src.read(size=nbytes)
        self.assertEqual(len(subset), nbytes)
        self.assertEqual(subset.decode(), content[:nbytes])

        with self.assertRaises(ValueError):
            src.transfer_from(src, transfer="unknown")

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
        self.assertFalse(d.join("not-there.yaml").exists())


@unittest.skipIf(not boto3, "Warning: boto3 AWS SDK not found!")
@mock_s3
class S3URITestCase(unittest.TestCase):
    """Tests involving S3"""

    bucketName = "any_bucket"
    """Bucket name to use in tests"""

    def setUp(self):
        # Local test directory
        self.tmpdir = tempfile.mkdtemp()

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

        dest = ButlerURI(self.makeS3Uri("test.txt"))
        self.assertFalse(dest.exists())
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

    def testWrite(self):
        s3write = ButlerURI(self.makeS3Uri("created.txt"))
        content = "abcdefghijklmnopqrstuv\n"
        s3write.write(content.encode())
        self.assertEqual(s3write.read().decode(), content)

    def testRelative(self):
        """Check that we can get subpaths back from two URIs"""
        parent = ButlerURI(self.makeS3Uri("rootdir"), forceDirectory=True)
        child = ButlerURI(self.makeS3Uri("rootdir/dir1/file.txt"))

        self.assertEqual(child.relative_to(parent), "dir1/file.txt")

        not_child = ButlerURI(self.makeS3Uri("/a/b/dir1/file.txt"))
        self.assertFalse(not_child.relative_to(parent))

        not_s3 = ButlerURI(os.path.join(self.tmpdir, "dir1", "file2.txt"))
        self.assertFalse(child.relative_to(not_s3))


if __name__ == "__main__":
    unittest.main()

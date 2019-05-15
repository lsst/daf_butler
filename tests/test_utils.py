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
import os

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

from lsst.daf.butler.core.utils import iterable, getFullTypeName, Singleton
from lsst.daf.butler.core.s3utils import (bucketExists, parsePathToUriElements,
                                          s3CheckFileExists)
from lsst.daf.butler.core.formatter import Formatter
from lsst.daf.butler import StorageClass


@unittest.skipIf(not boto3, "Warning: boto3 AWS SDK not found!")
@mock_s3
class S3UtilsTestCase(unittest.TestCase):
    """Test for the S3 related utilities.
    """
    bucketName = 'testBucketName'
    fileName = 'testFileName'

    def setUp(self):
        s3 = boto3.client('s3')
        try:
            s3.create_bucket(Bucket=self.bucketName)
            s3.put_object(Bucket=self.bucketName, Key=self.fileName,
                          Body=b'test content')
        except s3.exceptions.BucketAlreadyExists:
            pass

    def tearDown(self):
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.bucketName)
        try:
            bucket.objects.all().delete()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                # the key was not reachable - pass
                pass
            else:
                raise

        bucket = s3.Bucket(self.bucketName)
        bucket.delete()

    def testBucketExists(self):
        self.assertTrue(bucketExists(f's3://{self.bucketName}'))
        self.assertFalse(bucketExists(f's3://{self.bucketName}_NO_EXIST'))

    def testFileExists(self):
        s3 = boto3.client('s3')
        self.assertTrue(s3CheckFileExists(s3, self.bucketName, self.fileName)[0])
        self.assertFalse(s3CheckFileExists(s3, self.bucketName,
                                           self.fileName+'_NO_EXIST')[0])


class IterableTestCase(unittest.TestCase):
    """Tests for `iterable` helper.
    """

    def testNonIterable(self):
        self.assertEqual(list(iterable(0)), [0, ])

    def testString(self):
        self.assertEqual(list(iterable("hello")), ["hello", ])

    def testIterableNoString(self):
        self.assertEqual(list(iterable([0, 1, 2])), [0, 1, 2])
        self.assertEqual(list(iterable(["hello", "world"])), ["hello", "world"])


class SingletonTestCase(unittest.TestCase):
    """Tests of the Singleton metaclass"""

    class IsSingleton(metaclass=Singleton):
        def __init__(self):
            self.data = {}
            self.id = 0

    class IsBadSingleton(IsSingleton):
        def __init__(self, arg):
            """A singleton can not accept any arguments."""
            self.arg = arg

    class IsSingletonSubclass(IsSingleton):
        def __init__(self):
            super().__init__()

    def testSingleton(self):
        one = SingletonTestCase.IsSingleton()
        two = SingletonTestCase.IsSingleton()

        # Now update the first one and check the second
        one.data["test"] = 52
        self.assertEqual(one.data, two.data)
        two.id += 1
        self.assertEqual(one.id, two.id)

        three = SingletonTestCase.IsSingletonSubclass()
        self.assertNotEqual(one.id, three.id)

        with self.assertRaises(TypeError):
            SingletonTestCase.IsBadSingleton(52)


class NamedKeyDictTest(unittest.TestCase):

    def setUp(self):
        self.TestTuple = namedtuple("TestTuple", ("name", "id"))
        self.a = self.TestTuple(name="a", id=1)
        self.b = self.TestTuple(name="b", id=2)
        self.dictionary = {self.a: 10, self.b: 20}
        self.names = {self.a.name, self.b.name}

    def check(self, nkd):
        self.assertEqual(len(nkd), 2)
        self.assertEqual(nkd.names, self.names)
        self.assertEqual(nkd.keys(), self.dictionary.keys())
        self.assertEqual(list(nkd.values()), list(self.dictionary.values()))
        self.assertEqual(list(nkd.items()), list(self.dictionary.items()))
        self.assertEqual(list(nkd.byName().values()), list(self.dictionary.values()))
        self.assertEqual(list(nkd.byName().keys()), list(nkd.names))

    def testConstruction(self):
        self.check(NamedKeyDict(self.dictionary))
        self.check(NamedKeyDict(iter(self.dictionary.items())))

    def testDuplicateNameConstruction(self):
        self.dictionary[self.TestTuple(name="a", id=3)] = 30
        with self.assertRaises(AssertionError):
            NamedKeyDict(self.dictionary)
        with self.assertRaises(AssertionError):
            NamedKeyDict(iter(self.dictionary.items()))

    def testNoNameConstruction(self):
        self.dictionary["a"] = 30
        with self.assertRaises(AttributeError):
            NamedKeyDict(self.dictionary)
        with self.assertRaises(AttributeError):
            NamedKeyDict(iter(self.dictionary.items()))

    def testGetItem(self):
        nkd = NamedKeyDict(self.dictionary)
        self.assertEqual(nkd["a"], 10)
        self.assertEqual(nkd[self.a], 10)
        self.assertEqual(nkd["b"], 20)
        self.assertEqual(nkd[self.b], 20)
        self.assertIn("a", nkd)
        self.assertIn(self.b, nkd)

    def testSetItem(self):
        nkd = NamedKeyDict(self.dictionary)
        nkd[self.a] = 30
        self.assertEqual(nkd["a"], 30)
        nkd["b"] = 40
        self.assertEqual(nkd[self.b], 40)
        with self.assertRaises(KeyError):
            nkd["c"] = 50
        with self.assertRaises(AssertionError):
            nkd[self.TestTuple("a", 3)] = 60

    def testDelItem(self):
        nkd = NamedKeyDict(self.dictionary)
        del nkd[self.a]
        self.assertNotIn("a", nkd)
        del nkd["b"]
        self.assertNotIn(self.b, nkd)
        self.assertEqual(len(nkd), 0)

    def testIter(self):
        self.assertEqual(set(iter(NamedKeyDict(self.dictionary))), set(self.dictionary))

    def testEquality(self):
        nkd = NamedKeyDict(self.dictionary)
        self.assertEqual(nkd, self.dictionary)
        self.assertEqual(self.dictionary, nkd)


class TestButlerUtils(unittest.TestCase):
    """Tests of the simple utilities."""

    def testTypeNames(self):
        # Check types and also an object
        tests = [(Formatter, "lsst.daf.butler.core.formatter.Formatter"),
                 (int, "int"),
                 (StorageClass, "lsst.daf.butler.core.storageClass.StorageClass"),
                 (StorageClass(None), "lsst.daf.butler.core.storageClass.StorageClass")]

        for item, typeName in tests:
            self.assertEqual(getFullTypeName(item), typeName)

    def testParsePathToUriElements(self):
        absPaths = [
            'file:///rootDir/relative/file.ext',
            '/rootDir/relative/file.ext'
        ]
        relPaths = [
            'file://relative/file.ext',
            'relative/file.ext'
        ]
        s3Path = 's3://bucketname/rootDir/relative/file.ext'
        globPath1 = '~/relative/file.ext'
        globPath2 = '../relative/file.ext'
        globPath3 = 'test/../relative/file.ext'

        # absolute paths take precedence over additionaly supplied root paths
        for path in absPaths:
            self.assertEqual(parsePathToUriElements(path),
                             ('file://', '/rootDir/relative', 'file.ext'))
            self.assertEqual(parsePathToUriElements(path, '/<butlerRootDir>'),
                             ('file://', '/rootDir/relative', 'file.ext'))

        self.assertEqual(parsePathToUriElements(globPath1, '/<butlerRoot>/rootDir'),
                         ('file://', os.path.expanduser('~/relative'), 'file.ext'))
        self.assertEqual(parsePathToUriElements(globPath1),
                         ('file://', os.path.expanduser('~/relative'), 'file.ext'))

        # relative paths should not expand, unless root to which they are
        # relative to is also provided
        for path in relPaths:
            self.assertEqual(parsePathToUriElements(path, '/<butlerRoot>'),
                             ('file://', '/<butlerRoot>', 'relative/file.ext'))
            self.assertEqual(parsePathToUriElements(path),
                             ('file://', '', 'relative/file.ext'))

        # basic globbing should work relative to given root or not at all
        self.assertEqual(parsePathToUriElements(globPath2, '/<butlerRoot>/rootDir'),
                         ('file://', '/<butlerRoot>', 'relative/file.ext'))
        self.assertEqual(parsePathToUriElements(globPath3, '/<butlerRoot>'),
                         ('file://', '/<butlerRoot>', 'relative/file.ext'))
        self.assertEqual(parsePathToUriElements(globPath2),
                         ('file://', '', globPath2))

        self.assertEqual(parsePathToUriElements(s3Path),
                         ('s3://', 'bucketname', 'rootDir/relative/file.ext'))
        self.assertEqual(parsePathToUriElements(s3Path, '/<butlerRoot>'),
                         ('s3://', 'bucketname', 'rootDir/relative/file.ext'))


if __name__ == "__main__":
    unittest.main()

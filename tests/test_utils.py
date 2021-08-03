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

from collections import Counter, namedtuple
from glob import glob
import os
import re
import unittest
import logging

from lsst.daf.butler.core.utils import findFileResources, getFullTypeName, globToRegex, iterable, Singleton
from lsst.daf.butler import Formatter, Registry
from lsst.daf.butler import NamedKeyDict, NamedValueSet, StorageClass
from lsst.daf.butler.core.utils import isplit, time_this

TESTDIR = os.path.dirname(__file__)


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


class NamedValueSetTest(unittest.TestCase):

    def setUp(self):
        self.TestTuple = namedtuple("TestTuple", ("name", "id"))
        self.a = self.TestTuple(name="a", id=1)
        self.b = self.TestTuple(name="b", id=2)
        self.c = self.TestTuple(name="c", id=3)

    def testConstruction(self):
        for arg in ({self.a, self.b}, (self.a, self.b)):
            for nvs in (NamedValueSet(arg), NamedValueSet(arg).freeze()):
                self.assertEqual(len(nvs), 2)
                self.assertEqual(nvs.names, {"a", "b"})
                self.assertCountEqual(nvs, {self.a, self.b})
                self.assertCountEqual(nvs.asMapping().items(), [(self.a.name, self.a), (self.b.name, self.b)])

    def testNoNameConstruction(self):
        with self.assertRaises(AttributeError):
            NamedValueSet([self.a, "a"])

    def testGetItem(self):
        nvs = NamedValueSet({self.a, self.b, self.c})
        self.assertEqual(nvs["a"], self.a)
        self.assertEqual(nvs[self.a], self.a)
        self.assertEqual(nvs["b"], self.b)
        self.assertEqual(nvs[self.b], self.b)
        self.assertIn("a", nvs)
        self.assertIn(self.b, nvs)

    def testEquality(self):
        s = {self.a, self.b, self.c}
        nvs = NamedValueSet(s)
        self.assertEqual(nvs, s)
        self.assertEqual(s, nvs)

    def checkOperator(self, result, expected):
        self.assertIsInstance(result, NamedValueSet)
        self.assertEqual(result, expected)

    def testOperators(self):
        ab = NamedValueSet({self.a, self.b})
        bc = NamedValueSet({self.b, self.c})
        self.checkOperator(ab & bc, {self.b})
        self.checkOperator(ab | bc, {self.a, self.b, self.c})
        self.checkOperator(ab ^ bc, {self.a, self.c})
        self.checkOperator(ab - bc, {self.a})


class TestButlerUtils(unittest.TestCase):
    """Tests of the simple utilities."""

    def testTypeNames(self):
        # Check types and also an object
        tests = [(Formatter, "lsst.daf.butler.core.formatter.Formatter"),
                 (int, "int"),
                 (StorageClass, "lsst.daf.butler.core.storageClass.StorageClass"),
                 (StorageClass(None), "lsst.daf.butler.core.storageClass.StorageClass"),
                 (Registry, "lsst.daf.butler.registry.Registry")]

        for item, typeName in tests:
            self.assertEqual(getFullTypeName(item), typeName)

    def testIsplit(self):
        # Test compatibility with str.split
        seps = ("\n", " ", "d")
        input_str = "ab\ncd ef\n"

        for sep in seps:
            for input in (input_str, input_str.encode()):
                test_sep = sep.encode() if isinstance(input, bytes) else sep
                isp = list(isplit(input, sep=test_sep))
                ssp = input.split(test_sep)
                self.assertEqual(isp, ssp)


class FindFileResourcesTestCase(unittest.TestCase):

    def test_getSingleFile(self):
        """Test getting a file by its file name."""
        filename = os.path.join(TESTDIR, "config/basic/butler.yaml")
        self.assertEqual([filename], findFileResources([filename]))

    def test_getAllFiles(self):
        """Test getting all the files by not passing a regex."""
        expected = Counter([p for p in glob(os.path.join(TESTDIR, "config", "**"), recursive=True)
                            if os.path.isfile(p)])
        self.assertNotEqual(len(expected), 0)  # verify some files were found
        files = Counter(findFileResources([os.path.join(TESTDIR, "config")]))
        self.assertEqual(expected, files)

    def test_getAllFilesRegex(self):
        """Test getting all the files with a regex-specified file ending."""
        expected = Counter(glob(os.path.join(TESTDIR, "config", "**", "*.yaml"), recursive=True))
        self.assertNotEqual(len(expected), 0)  # verify some files were found
        files = Counter(findFileResources([os.path.join(TESTDIR, "config")], r"\.yaml\b"))
        self.assertEqual(expected, files)

    def test_multipleInputs(self):
        """Test specifying more than one location to find a files."""
        expected = Counter(glob(os.path.join(TESTDIR, "config", "basic", "**", "*.yaml"), recursive=True))
        expected.update(glob(os.path.join(TESTDIR, "config", "templates", "**", "*.yaml"), recursive=True))
        self.assertNotEqual(len(expected), 0)  # verify some files were found
        files = Counter(findFileResources([os.path.join(TESTDIR, "config", "basic"),
                                           os.path.join(TESTDIR, "config", "templates")],
                                          r"\.yaml\b"))
        self.assertEqual(expected, files)


class GlobToRegexTestCase(unittest.TestCase):

    def testStarInList(self):
        """Test that if a one of the items in the expression list is a star
        (stand-alone) then ``...`` is returned (which implies no restrictions)
        """
        self.assertIs(globToRegex(["foo", "*", "bar"]), ...)

    def testGlobList(self):
        """Test that a list of glob strings converts as expected to a regex and
        returns in the expected list.
        """
        # test an absolute string
        patterns = globToRegex(["bar"])
        self.assertEqual(len(patterns), 1)
        self.assertTrue(bool(re.fullmatch(patterns[0], "bar")))
        self.assertIsNone(re.fullmatch(patterns[0], "boz"))

        # test leading & trailing wildcard in multiple patterns
        patterns = globToRegex(["ba*", "*.fits"])
        self.assertEqual(len(patterns), 2)
        # check the "ba*" pattern:
        self.assertTrue(bool(re.fullmatch(patterns[0], "bar")))
        self.assertTrue(bool(re.fullmatch(patterns[0], "baz")))
        self.assertIsNone(re.fullmatch(patterns[0], "boz.fits"))
        # check the "*.fits" pattern:
        self.assertTrue(bool(re.fullmatch(patterns[1], "bar.fits")))
        self.assertTrue(re.fullmatch(patterns[1], "boz.fits"))
        self.assertIsNone(re.fullmatch(patterns[1], "boz.hdf5"))


class TimerTestCase(unittest.TestCase):

    def testTimer(self):
        with self.assertLogs(level="DEBUG") as cm:
            with time_this():
                pass
        self.assertEqual(cm.records[0].name, "root")
        self.assertEqual(cm.records[0].levelname, "DEBUG")
        self.assertIn("Took", cm.output[0])
        self.assertEqual(cm.records[0].filename, "test_utils.py")

        # Change logging level
        with self.assertLogs(level="INFO") as cm:
            with time_this(log_level=logging.INFO):
                pass
        self.assertEqual(cm.records[0].name, "root")
        self.assertIn("Took", cm.output[0])
        self.assertIn("seconds", cm.output[0])

        # Use a new logger with a message.
        msg = "Test message %d"
        test_num = 42
        logname = "test"
        with self.assertLogs(level="DEBUG") as cm:
            with time_this(log=logging.getLogger(logname),
                           msg=msg, args=(42,)):
                pass
        self.assertEqual(cm.records[0].name, logname)
        self.assertIn("Took", cm.output[0])
        self.assertIn(msg % test_num, cm.output[0])

        # Prefix the logger.
        prefix = "prefix"
        with self.assertLogs(level="DEBUG") as cm:
            with time_this(log_prefix=prefix):
                pass
        self.assertEqual(cm.records[0].name, prefix)
        self.assertIn("Took", cm.output[0])

        # Prefix explicit logger.
        with self.assertLogs(level="DEBUG") as cm:
            with time_this(log=logging.getLogger(logname),
                           log_prefix=prefix):
                pass
        self.assertEqual(cm.records[0].name, f"{prefix}.{logname}")

        # Trigger a problem.
        with self.assertLogs(level="ERROR") as cm:
            with self.assertRaises(RuntimeError):
                with time_this(log=logging.getLogger(logname),
                               log_prefix=prefix):
                    raise RuntimeError("A problem")
        self.assertEqual(cm.records[0].name, f"{prefix}.{logname}")
        self.assertEqual(cm.records[0].levelname, "ERROR")


if __name__ == "__main__":
    unittest.main()

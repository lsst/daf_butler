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
import re
import unittest
from collections import namedtuple

from lsst.daf.butler import NamedKeyDict, NamedValueSet
from lsst.daf.butler.utils import globToRegex

TESTDIR = os.path.dirname(__file__)


class NamedKeyDictTest(unittest.TestCase):
    """Tests for NamedKeyDict."""

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
    """Tests for NamedValueSet."""

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

    def testPop(self):
        # Construct with list for repeatable ordering.
        nvs = NamedValueSet([self.a, self.b, self.c])
        self.assertEqual(nvs.pop("c"), self.c)
        self.assertEqual(nvs.pop(), self.a)
        self.assertEqual(nvs.pop(), self.b)
        self.assertEqual(nvs.pop("d", self.c), self.c)
        with self.assertRaises(KeyError):
            nvs.pop()

    def testRemove(self):
        nvs = NamedValueSet([self.a, self.b, self.c])
        nvs.remove("b")
        self.assertIn("a", nvs)
        self.assertNotIn("b", nvs)
        with self.assertRaises(KeyError):
            nvs.remove("d")
        nvs.discard("d")
        nvs.discard("a")
        self.assertNotIn("a", nvs)


class GlobToRegexTestCase(unittest.TestCase):
    """Tests for glob to regex."""

    def testStarInList(self):
        """Test that if a one of the items in the expression list is a star
        (stand-alone) then ``...`` is returned (which implies no restrictions)
        """
        self.assertIs(globToRegex(["foo", "*", "bar"]), ...)

    def testGlobList(self):
        """Test that a list of glob strings converts as expected to a regex and
        returns in the expected list.
        """
        # These strings should be returned unchanged.
        strings = ["bar", "😺", "ingésτ", "ex]", "[xe", "[!no", "e[x"]
        self.assertEqual(globToRegex(strings), strings)

        # Globs with strings that match the glob and strings that do not.
        tests = (
            ("bar", ["bar"], ["baz"]),
            ("ba*", ["bar", "baz"], ["az"]),
            ("ba[rz]", ["bar", "baz"], ["bat"]),
            ("ba[rz]x[y", ["barx[y", "bazx[y"], ["batx[y"]),
            ("ba[!rz]", ["bat", "baτ"], ["bar", "baz"]),
            ("b?r", ["bor", "bar", "b😺r"], ["bat"]),
            ("*.fits", ["boz.fits"], ["boz.fits.gz", "boz.hdf5"]),
        )

        for glob, matches, no_matches in tests:
            patterns = globToRegex(glob)
            for match in matches:
                self.assertTrue(bool(re.fullmatch(patterns[0], match)))
            for no_match in no_matches:
                self.assertIsNone(re.fullmatch(patterns[0], no_match))


if __name__ == "__main__":
    unittest.main()

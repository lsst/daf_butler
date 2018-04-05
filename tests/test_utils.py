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
import inspect
from itertools import permutations
from random import shuffle

import lsst.utils.tests

from lsst.daf.butler.core.utils import iterable, doImport, getFullTypeName, Singleton, ConnectedSet
from lsst.daf.butler.core.formatter import Formatter
from lsst.daf.butler import StorageClass


class IterableTestCase(lsst.utils.tests.TestCase):
    """Tests for `iterable` helper.
    """

    def testNonIterable(self):
        self.assertEqual(list(iterable(0)), [0, ])

    def testString(self):
        self.assertEqual(list(iterable("hello")), ["hello", ])

    def testIterableNoString(self):
        self.assertEqual(list(iterable([0, 1, 2])), [0, 1, 2])
        self.assertEqual(list(iterable(["hello", "world"])), ["hello", "world"])


class ImportTestCase(unittest.TestCase):
    """Basic tests of doImport."""

    def testDoImport(self):
        c = doImport("lsst.daf.butler.core.formatter.Formatter")
        self.assertEqual(c, Formatter)

        c = doImport("lsst.daf.butler.core.utils.doImport")
        self.assertEqual(type(c), type(doImport))
        self.assertTrue(inspect.isfunction(c))

        c = doImport("lsst.daf.butler")
        self.assertTrue(inspect.ismodule(c))

        c = doImport("lsst.daf.butler.core.config.Config.ppprint")
        self.assertTrue(inspect.isfunction(c))

        with self.assertRaises(AttributeError):
            doImport("lsst.daf.butler.nothere")

        with self.assertRaises(ValueError):
            doImport("missing module")

        with self.assertRaises(TypeError):
            doImport([])


class SingletonTestCase(lsst.utils.tests.TestCase):
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


class ConnectedSetTestCase(lsst.utils.tests.TestCase):
    """Tests of the ConnectedSet class"""

    def testConstructor(self):
        elements = [1, 5, 7, 3, 10, 11]
        connectedSet = ConnectedSet(elements)
        for e in elements:
            self.assertIn(e, connectedSet)

    def testConnect(self):
        elements = ['a', 'd', 'f']
        connectedSet = ConnectedSet(elements)
        # Adding connections should work
        connectedSet.connect('a', 'd')
        connectedSet.connect('a', 'f')
        # Even when adding cycles (unfortunately)
        connectedSet.connect('f', 'a')
        # Adding the same connection again should fail
        with self.assertRaises(ValueError):
            connectedSet.connect('a', 'd')
        # Adding a connection from or to a non existing element should also fail
        with self.assertRaises(KeyError):
            connectedSet.connect('a', 'c')
        with self.assertRaises(KeyError):
            connectedSet.connect('c', 'a')
        with self.assertRaises(KeyError):
            connectedSet.connect('c', 'g')

    def testTopologicalOrdering(self):
        """Iterating over a ConnectedSet should visit the elements
        in the set in topologically sorted order.
        """
        # First check a basic topological ordering
        elements = ['shoes', 'belt', 'trousers']
        for p in permutations(elements):
            connectedSet = ConnectedSet(p)
            connectedSet.connect('belt', 'shoes')
            connectedSet.connect('trousers', 'shoes')
            # Check valid orderings
            self.assertIn(list(connectedSet), [['belt', 'trousers', 'shoes'],
                                               ['trousers', 'belt', 'shoes']])
            # Check invalid orderings (probably redundant)
            self.assertNotIn(list(connectedSet), [['shoes', 'belt', 'trousers'],
                                                  ['shoes', 'trousers', 'belt']])
        # Adding a cycle should cause iteration to fail
        connectedSet.connect('shoes', 'belt')
        with self.assertRaises(ValueError):
            ignore = list(connectedSet)  # noqa F841
        # Now check for a larger number of elements.
        # Here we can't possibly test all possible valid topological orderings,
        # so instead we connect all elements.
        # Thus the topological sort should be equivalent to a regular sort
        # (but less efficient).
        N = 100
        elements = list(range(N))
        unorderedElements = elements.copy()
        shuffle(unorderedElements)
        connectedSet2 = ConnectedSet(unorderedElements)
        for i in range(N-1):
            connectedSet2.connect(i, i+1)
        self.assertEqual(list(connectedSet2), elements)


class TestButlerUtils(lsst.utils.tests.TestCase):
    """Tests of the simple utilities."""

    def testTypeNames(self):
        # Check types and also an object
        tests = [(Formatter, "lsst.daf.butler.core.formatter.Formatter"),
                 (doImport, "lsst.daf.butler.core.utils.doImport"),
                 (int, "builtins.int"),
                 (StorageClass, "lsst.daf.butler.core.storageClass.StorageClass"),
                 (StorageClass(), "lsst.daf.butler.core.storageClass.StorageClass")]

        for item, typeName in tests:
            self.assertEqual(getFullTypeName(item), typeName)


if __name__ == "__main__":
    unittest.main()

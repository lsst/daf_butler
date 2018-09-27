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
import pickle
from itertools import permutations
from random import shuffle

import lsst.utils.tests

from lsst.daf.butler.core.utils import iterable, getFullTypeName, Singleton, TopologicalSet
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


class TopologicalSetTestCase(lsst.utils.tests.TestCase):
    """Tests of the TopologicalSet class"""

    def testConstructor(self):
        elements = [1, 5, 7, 3, 10, 11]
        topologicalSet = TopologicalSet(elements)
        for e in elements:
            self.assertIn(e, topologicalSet)
        self.assertEqual(len(elements), len(topologicalSet))

    def testConnect(self):
        elements = ["a", "d", "f"]
        topologicalSet = TopologicalSet(elements)
        # Adding connections should work
        topologicalSet.connect("a", "d")
        topologicalSet.connect("a", "f")
        # Even when adding cycles (unfortunately)
        topologicalSet.connect("f", "a")
        # Adding a connection from or to a non existing element should also fail
        with self.assertRaises(KeyError):
            topologicalSet.connect("a", "c")
        with self.assertRaises(KeyError):
            topologicalSet.connect("c", "a")
        with self.assertRaises(KeyError):
            topologicalSet.connect("c", "g")
        with self.assertRaises(ValueError):
            topologicalSet.connect("c", "c")

    def testSetOps(self):
        """Test that TopologicalSet behaves like a Set.
        """
        a = TopologicalSet([1, 2, 3, 4])
        b = set([3, 4, 5])
        self.assertNotEqual(a, b)
        self.assertEqual(a & b, {3, 4})
        self.assertEqual(a | b, {1, 2, 3, 4, 5})
        self.assertEqual(a ^ b, {1, 2, 5})
        self.assertEqual(a - b, {1, 2})

    def testTopologicalOrdering(self):
        """Iterating over a TopologicalSet should visit the elements
        in the set in topologically sorted order.
        """
        # First check a basic topological ordering
        elements = ["shoes", "belt", "trousers"]
        for p in permutations(elements):
            topologicalSet = TopologicalSet(p)
            topologicalSet.connect("belt", "shoes")
            topologicalSet.connect("trousers", "shoes")
            # Check valid orderings
            self.assertIn(list(topologicalSet), [["belt", "trousers", "shoes"],
                                                 ["trousers", "belt", "shoes"]])
            # Check invalid orderings (probably redundant)
            self.assertNotIn(list(topologicalSet), [["shoes", "belt", "trousers"],
                                                    ["shoes", "trousers", "belt"]])
        # Adding a cycle should cause iteration to fail
        topologicalSet.connect("shoes", "belt")
        with self.assertRaises(ValueError):
            ignore = list(topologicalSet)  # noqa F841
        # Now check for a larger number of elements.
        # Here we can't possibly test all possible valid topological orderings,
        # so instead we connect all elements.
        # Thus the topological sort should be equivalent to a regular sort
        # (but less efficient).
        N = 100
        elements = list(range(N))
        unorderedElements = elements.copy()
        shuffle(unorderedElements)
        topologicalSet2 = TopologicalSet(unorderedElements)
        for i in range(N-1):
            topologicalSet2.connect(i, i+1)
        self.assertEqual(list(topologicalSet2), elements)

    def testPickle(self):
        """Should be possible to pickle.
        """
        elements = ["a", "d", "f"]
        topologicalSet = TopologicalSet(elements)
        # Adding connections should work
        topologicalSet.connect("a", "d")
        topologicalSet.connect("a", "f")
        out = pickle.loads(pickle.dumps(topologicalSet))
        self.assertEqual(out, topologicalSet)


class TestButlerUtils(lsst.utils.tests.TestCase):
    """Tests of the simple utilities."""

    def testTypeNames(self):
        # Check types and also an object
        tests = [(Formatter, "lsst.daf.butler.core.formatter.Formatter"),
                 (int, "builtins.int"),
                 (StorageClass, "lsst.daf.butler.core.storageClass.StorageClass"),
                 (StorageClass(None), "lsst.daf.butler.core.storageClass.StorageClass")]

        for item, typeName in tests:
            self.assertEqual(getFullTypeName(item), typeName)


if __name__ == "__main__":
    unittest.main()

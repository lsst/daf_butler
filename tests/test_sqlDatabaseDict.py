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
from collections import namedtuple

from lsst.daf.butler.core import RegistryConfig, Registry

"""Tests for SqlDatabaseDict.
"""


class SqlDatabaseDictTestCase(unittest.TestCase):
    """Test for SqlDatabaseDict.
    """

    def setUp(self):
        registryConfig = RegistryConfig()
        registryConfig["db"] = "sqlite:///:memory:"
        self.registry = Registry.fromConfig(registryConfig, create=True)
        self.types = {"x": int, "y": str, "z": float}
        self.key = "x"

    def checkDatabaseDict(self, d, data):
        self.assertEqual(len(d), 0)
        self.assertFalse(d)
        d[0] = data[0]
        self.assertEqual(len(d), 1)
        self.assertTrue(d)
        self.assertIn(0, d)
        self.assertEqual(d[0], data[0])
        d[1] = data[1]
        self.assertEqual(len(d), 2)
        self.assertTrue(d)
        self.assertIn(1, d)
        self.assertEqual(d[1], data[1])
        self.assertCountEqual(d.keys(), data.keys())
        self.assertCountEqual(d.values(), data.values())
        self.assertCountEqual(d.items(), data.items())
        del d[0]
        self.assertNotIn(0, d)
        self.assertEqual(len(d), 1)
        with self.assertRaises(KeyError):
            del d[0]
        with self.assertRaises(KeyError):
            d[0]
        # Test that we can update an existing key
        d[1] = data[0]
        self.assertEqual(len(d), 1)
        self.assertEqual(d[1], data[0])
        del d[1]
        self.assertEqual(len(d), 0)

    def testKeyInValue(self):
        """Test that the key is not permitted to be part of the value."""
        value = namedtuple("TestValue", ["x", "y", "z"])
        with self.assertRaises(ValueError):
            self.registry.makeDatabaseDict(table="test_table", key=self.key, types=self.types, value=value)

    def testKeyNotInValue(self):
        """Test when the value does not include the key."""
        value = namedtuple("TestValue", ["y", "z"])
        data = {
            0: value(y="zero", z=0.0),
            1: value(y="one", z=0.1),
        }
        d = self.registry.makeDatabaseDict(table="test_table", key=self.key, types=self.types, value=value)
        self.checkDatabaseDict(d, data)

    def testLengths(self):
        """Test that when a length is specified that it is actually used."""
        value = namedtuple("TestValue", ["y", "z"])
        data = {
            0: value(y="passes", z=0.0),
            1: value(y="fails too long", z=0.1),
        }
        d = self.registry.makeDatabaseDict(table="test_table", key=self.key, types=self.types, value=value,
                                           lengths={"y": 6})

        # This insert meets the constraint
        d[0] = data[0]
        self.assertEqual(d[0], data[0])

        # This string is too long
        with self.assertRaises(ValueError):
            d[1] = data[1]

    def testBadValueTypes(self):
        """Test that we cannot insert value tuples with the wrong types."""
        value = namedtuple("TestValue", ["y", "z"])
        data = {
            0: value(y=0, z="zero"),
        }
        d = self.registry.makeDatabaseDict(table="test_table", key=self.key, types=self.types, value=value)
        with self.assertRaises(TypeError):
            d[0] = data[0]

    def testBadKeyTypes(self):
        """Test that we cannot insert with the wrong key type."""
        value = namedtuple("TestValue", ["y", "z"])
        data = {
            0: value(y="zero", z=0.0),
        }
        d = self.registry.makeDatabaseDict(table="test_table", key=self.key, types=self.types, value=value)
        d["zero"] = data[0]

    def testExtraFieldsInTable(self):
        """Test when there are fields in the table that not in the value or
        the key.

        These should be completely ignored by the DatabaseDict after the table
        is created (which implies that they must be nullable if
        ``__setitem__`` is expected to work."""
        value = namedtuple("TestValue", ["y"])
        data = {
            0: value(y="zero"),
            1: value(y="one"),
        }
        d = self.registry.makeDatabaseDict(table="test_table", key=self.key, types=self.types, value=value)
        self.checkDatabaseDict(d, data)

    def testExtraFieldsInValue(self):
        """Test that we don't permit the value tuple to have ._fields entries
        that are not in the types argument itself (since we need to know
        their types).
        """
        value = namedtuple("TestValue", ["y", "a"])
        with self.assertRaises(TypeError):
            self.registry.makeDatabaseDict(table="test_table", key=self.key, types=self.types, value=value)


if __name__ == "__main__":
    unittest.main()

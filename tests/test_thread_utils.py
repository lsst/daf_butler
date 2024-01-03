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

import unittest

from lsst.daf.butler._utilities.locked_object import LockedObject
from lsst.daf.butler._utilities.named_locks import NamedLocks
from lsst.daf.butler._utilities.thread_safe_cache import ThreadSafeCache


class ThreadSafeCacheTestCase(unittest.TestCase):
    """Test ThreadSafeCache."""

    def test_cache(self):
        cache = ThreadSafeCache()
        self.assertIsNone(cache.get("unknown"))
        self.assertEqual(cache.set_or_get("key", "a"), "a")
        self.assertEqual(cache.get("key"), "a")
        self.assertEqual(cache.set_or_get("key", "b"), "a")
        self.assertEqual(cache.get("key"), "a")
        self.assertIsNone(cache.get("other"))


class NamedLocksTestCase(unittest.TestCase):
    """Test NamedLocks."""

    def test_named_locks(self):
        locks = NamedLocks()
        lock1 = locks._get_lock("a")
        lock2 = locks._get_lock("b")
        lock3 = locks._get_lock("a")

        self.assertIs(lock1, lock3)
        self.assertIsNot(lock1, lock2)

        self.assertFalse(lock1.locked())
        self.assertFalse(lock2.locked())
        with locks.lock("a"):
            self.assertTrue(lock1.locked())
            self.assertFalse(lock2.locked())
        self.assertFalse(lock1.locked())
        self.assertFalse(lock2.locked())


class LockedObjectTestCase(unittest.TestCase):
    """Test LockedObject."""

    def test_named_locks(self):
        data = object()
        locked_obj = LockedObject(data)
        self.assertFalse(locked_obj._lock.locked())
        with locked_obj.access() as accessed:
            self.assertTrue(locked_obj._lock.locked())
            self.assertIs(data, accessed)
        self.assertFalse(locked_obj._lock.locked())


if __name__ == "__main__":
    unittest.main()

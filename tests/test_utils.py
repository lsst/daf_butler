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

import lsst.utils.tests

from lsst.daf.butler.core.utils import iterable, doImport
from lsst.daf.butler.core.formatter import Formatter


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


if __name__ == "__main__":
    unittest.main()

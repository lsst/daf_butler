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

from lsst.daf.butler.core.utils import doImport
from lsst.daf.butler.core.formatter import Formatter


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

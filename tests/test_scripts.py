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
import tempfile
import shutil
import os
import io

from lsst.daf.butler.script.makeButlerRepo import makeButlerRepo
from lsst.daf.butler.script.validateButlerConfiguration import validateButlerConfiguration
from lsst.daf.butler.script.dumpButlerConfig import dumpButlerConfig

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class ScriptsTestCase(unittest.TestCase):
    """Test script functions."""

    def setUp(self):
        self.root = tempfile.mkdtemp(dir=TESTDIR)

    def tearDown(self):
        if self.root is not None and os.path.exists(self.root):
            shutil.rmtree(self.root, ignore_errors=True)

    def testScriptFlow(self):
        """Call each script in sequence."""
        makeButlerRepo(self.root)
        with io.StringIO() as fh:
            dumpButlerConfig(self.root, subset=".datastore.root", outfh=fh)
            fh.seek(0)
            self.assertIn("<butlerRoot>", fh.readline())

        v = validateButlerConfiguration(self.root)
        self.assertTrue(v)


if __name__ == "__main__":
    unittest.main()

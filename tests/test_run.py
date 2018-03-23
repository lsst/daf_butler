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

import lsst.utils.tests

from lsst.daf.butler.core.run import Run
from lsst.daf.butler.core.execution import Execution

"""Tests for Run.
"""


class RunTestCase(lsst.utils.tests.TestCase):
    """Test for Run.
    """

    def testConstructor(self):
        """Test of constructor.
        """
        execution = Execution()
        collection = "ingest"
        environment = None
        pipeline = None
        run = Run(execution, collection, environment, pipeline)
        self.assertEqual(run.execution, execution)
        self.assertEqual(run.collection, collection)
        self.assertEqual(run.environment, environment)
        self.assertEqual(run.pipeline, pipeline)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()

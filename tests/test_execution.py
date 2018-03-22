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
from datetime import datetime, timedelta

import lsst.utils.tests

from lsst.daf.butler.core.execution import Execution

"""Tests for Execution.
"""


class ExecutionTestCase(lsst.utils.tests.TestCase):
    """Test for Execution.
    """

    def testConstructor(self):
        """Test of constructor.
        """
        id = 0
        startTime = datetime(2018, 1, 1)
        endTime = startTime + timedelta(days=1, hours=5)
        host = "localhost"
        execution = Execution(id, startTime, endTime, host)
        self.assertEqual(execution.id, id)
        self.assertEqual(execution.startTime, startTime)
        self.assertEqual(execution.endTime, endTime)
        self.assertEqual(execution.host, host)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()

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
from datetime import datetime

import lsst.utils.tests

from lsst.daf.butler.core.quantum import Quantum

"""Tests for Quantum.
"""


class QuantumTestCase(lsst.utils.tests.TestCase):
    """Test for Quantum.
    """

    def testConstructor(self):
        """Test of constructor.
        """
        # Quantum specific arguments
        run = None  # TODO add Run
        task = "some.task.object"  # TODO Add a `SuperTask`?
        # Base class arguments
        startTime = datetime(2018, 1, 1)
        endTime = datetime(2018, 1, 2)
        host = "localhost"
        quantum = Quantum(task, run, startTime, endTime, host)
        self.assertEqual(quantum.task, task)
        self.assertEqual(quantum.run, run)
        self.assertEqual(quantum.predictedInputs, dict())
        self.assertEqual(quantum.actualInputs, dict())
        self.assertIsNone(quantum.id)
        self.assertEqual(quantum.startTime, startTime)
        self.assertEqual(quantum.endTime, endTime)
        self.assertEqual(quantum.host, host)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()

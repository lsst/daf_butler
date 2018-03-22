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

from lsst.daf.butler.core.quantum import Quantum

"""Tests for Quantum.
"""


class QuantumTestCase(lsst.utils.tests.TestCase):
    """Test for Quantum.
    """

    def testConstructor(self):
        """Test of constructor.
        """
        execution = None  # TODO add Execution
        run = None  # TODO add Run
        task = "some.task.object"  # TODO Add a `SuperTask`?
        quantum = Quantum(execution, task, run)
        self.assertEqual(quantum.execution, execution)
        self.assertEqual(quantum.task, task)
        self.assertEqual(quantum.run, run)
        self.assertEqual(quantum.predictedInputs, dict())
        self.assertEqual(quantum.actualInputs, dict())


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()

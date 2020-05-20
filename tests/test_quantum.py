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
import astropy.time

from lsst.daf.butler import Quantum, DimensionUniverse, NamedKeyDict, StorageClass, DatasetType, DatasetRef

"""Tests for Quantum.
"""


class QuantumTestCase(unittest.TestCase):
    """Test for Quantum.
    """

    def testConstructor(self):
        """Test of constructor.
        """
        # Quantum specific arguments
        run = None  # TODO add Run
        taskName = "some.task.object"  # can't use a real PipelineTask due to inverted package dependency
        # Base class arguments
        startTime = astropy.time.Time("2018-01-01", format="iso", scale="utc")
        endTime = astropy.time.Time("2018-01-02", format="iso", scale="utc")
        host = "localhost"
        quantum = Quantum(taskName=taskName, run=run, startTime=startTime, endTime=endTime, host=host)
        self.assertEqual(quantum.taskName, taskName)
        self.assertEqual(quantum.run, run)
        self.assertEqual(quantum.predictedInputs, NamedKeyDict())
        self.assertEqual(quantum.actualInputs, NamedKeyDict())
        self.assertIsNone(quantum.dataId)
        self.assertIsNone(quantum.id)
        self.assertEqual(quantum.startTime, startTime)
        self.assertEqual(quantum.endTime, endTime)
        self.assertEqual(quantum.host, host)

    def testAddInputsOutputs(self):
        """Test of addPredictedInput() method.
        """
        quantum = Quantum(taskName="some.task.object", run=None)

        # start with empty
        self.assertEqual(quantum.predictedInputs, dict())
        universe = DimensionUniverse()
        instrument = "DummyCam"
        datasetTypeName = "test_ds"
        storageClass = StorageClass("testref_StructuredData")
        datasetType = DatasetType(datasetTypeName, universe.extract(("instrument", "visit")), storageClass)

        # add one ref
        ref = DatasetRef(datasetType, dict(instrument=instrument, visit=42))
        quantum.addPredictedInput(ref)
        self.assertIn(datasetTypeName, quantum.predictedInputs)
        self.assertEqual(len(quantum.predictedInputs[datasetTypeName]), 1)
        # add second ref
        ref = DatasetRef(datasetType, dict(instrument=instrument, visit=43))
        quantum.addPredictedInput(ref)
        self.assertEqual(len(quantum.predictedInputs[datasetTypeName]), 2)

        # mark last ref as actually used
        self.assertEqual(quantum.actualInputs, dict())
        quantum._markInputUsed(ref)
        self.assertIn(datasetTypeName, quantum.actualInputs)
        self.assertEqual(len(quantum.actualInputs[datasetTypeName]), 1)

        # add couple of outputs too
        self.assertEqual(quantum.outputs, dict())
        ref = DatasetRef(datasetType, dict(instrument=instrument, visit=42))
        quantum.addOutput(ref)
        self.assertIn(datasetTypeName, quantum.outputs)
        self.assertEqual(len(quantum.outputs[datasetTypeName]), 1)

        ref = DatasetRef(datasetType, dict(instrument=instrument, visit=43))
        quantum.addOutput(ref)
        self.assertEqual(len(quantum.outputs[datasetTypeName]), 2)


if __name__ == "__main__":
    unittest.main()

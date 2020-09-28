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
        taskName = "some.task.object"  # can't use a real PipelineTask due to inverted package dependency

        quantum = Quantum(taskName=taskName)
        self.assertEqual(quantum.taskName, taskName)
        self.assertEqual(quantum.initInputs, {})
        self.assertEqual(quantum.inputs, NamedKeyDict())
        self.assertEqual(quantum.outputs, {})
        self.assertIsNone(quantum.dataId)

        universe = DimensionUniverse()
        instrument = "DummyCam"
        datasetTypeName = "test_ds"
        storageClass = StorageClass("testref_StructuredData")
        datasetType = DatasetType(datasetTypeName, universe.extract(("instrument", "visit")), storageClass)
        predictedInputs = {datasetType: [DatasetRef(datasetType, dict(instrument=instrument, visit=42)),
                                         DatasetRef(datasetType, dict(instrument=instrument, visit=43))]}
        outputs = {datasetType: [DatasetRef(datasetType, dict(instrument=instrument, visit=42)),
                                 DatasetRef(datasetType, dict(instrument=instrument, visit=43))]}

        quantum = Quantum(taskName=taskName, inputs=predictedInputs, outputs=outputs)
        self.assertEqual(len(quantum.inputs[datasetType]), 2)
        self.assertEqual(len(quantum.outputs[datasetType]), 2)


if __name__ == "__main__":
    unittest.main()

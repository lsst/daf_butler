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

import json
import unittest
from collections.abc import Iterable

from lsst.daf.butler import (
    DataCoordinate,
    DatasetRef,
    DatasetType,
    DimensionRecordsAccumulator,
    DimensionUniverse,
    NamedKeyDict,
    Quantum,
    SerializedQuantum,
    StorageClass,
)
from lsst.sphgeom import Circle

"""Tests for Quantum.
"""


class MockTask:
    """Mock task for testing."""

    pass


class QuantumTestCase(unittest.TestCase):
    """Test for Quantum."""

    def _buildFullQuantum(self, taskName, addRecords=False) -> tuple[Quantum, Iterable[DatasetType]]:
        universe = DimensionUniverse()
        datasetTypeNameInit = "test_ds_init"
        datasetTypeNameInput = "test_ds_input"
        datasetTypeNameOutput = "test_ds_output"

        storageClass = StorageClass("testref_StructuredData")

        instrument = universe["instrument"]
        instrumentRecord = instrument.RecordClass(name="test")

        band = universe["band"]
        bandRecord = band.RecordClass(name="r")

        physical_filter = universe["physical_filter"]
        physical_filter_record = physical_filter.RecordClass(name="r", instrument="test", band="r")

        day_obs = universe["day_obs"]
        day_obs_record = day_obs.RecordClass(instrument="test", id=20250101)

        visit_system = universe["visit_system"]
        visit_system_record = visit_system.RecordClass(id=9, instrument="test", name="test_visit_system")

        visit = universe["visit"]
        region = Circle()
        # create a synthetic value to mock as a visit hash
        visit_record_42 = visit.RecordClass(
            id=42,
            instrument="test",
            name="test_visit",
            physical_filter="r",
            region=region,
            day_obs=20250101,
        )
        visit_record_43 = visit.RecordClass(
            id=43,
            instrument="test",
            name="test_visit",
            physical_filter="r",
            region=region,
            day_obs=20250101,
        )

        records42 = {
            instrument: instrumentRecord,
            band: bandRecord,
            physical_filter: physical_filter_record,
            visit_system: visit_system_record,
            visit: visit_record_42,
            day_obs: day_obs_record,
        }

        records43 = {
            instrument: instrumentRecord,
            band: bandRecord,
            physical_filter: physical_filter_record,
            visit_system: visit_system_record,
            visit: visit_record_43,
            day_obs: day_obs_record,
        }

        dataId42 = DataCoordinate.standardize(
            dict(instrument="test", visit=42),
            universe=universe,  # type: ignore
        )
        dataId43 = DataCoordinate.standardize(
            dict(instrument="test", visit=43),
            universe=universe,  # type: ignore
        )

        if addRecords:
            dataId42 = dataId42.expanded(records42)  # type: ignore
            dataId43 = dataId43.expanded(records43)  # type: ignore

        datasetTypeInit = DatasetType(
            datasetTypeNameInit, universe.conform(("instrument", "visit")), storageClass
        )
        datasetTypeInput = DatasetType(
            datasetTypeNameInput, universe.conform(("instrument", "visit")), storageClass
        )
        datasetTypeOutput = DatasetType(
            datasetTypeNameOutput, universe.conform(("instrument", "visit")), storageClass
        )
        predictedInputs = {
            datasetTypeInput: [
                DatasetRef(datasetTypeInput, dataId42, run="input"),
                DatasetRef(datasetTypeInput, dataId43, run="input"),
            ]
        }
        outputs = {
            datasetTypeOutput: [
                DatasetRef(datasetTypeOutput, dataId42, run="some_run"),
                DatasetRef(datasetTypeOutput, dataId43, run="other_run"),
            ]
        }
        initInputs = {datasetTypeInit: DatasetRef(datasetTypeInit, dataId42, run="input_run")}

        return Quantum(taskName=taskName, inputs=predictedInputs, outputs=outputs, initInputs=initInputs), [
            datasetTypeInit,
            datasetTypeInput,
            datasetTypeOutput,
        ]

    def testConstructor(self):
        """Test of constructor."""
        # Quantum specific arguments
        taskName = "some.task.object"  # can't use a real PipelineTask due to inverted package dependency

        quantum = Quantum(taskName=taskName)
        self.assertEqual(quantum.taskName, taskName)
        self.assertEqual(quantum.initInputs, {})
        self.assertEqual(quantum.inputs, NamedKeyDict())
        self.assertEqual(quantum.outputs, {})
        self.assertIsNone(quantum.dataId)

        quantum, (_, datasetTypeInput, datasetTypeOutput) = self._buildFullQuantum(taskName)
        self.assertEqual(len(quantum.inputs[datasetTypeInput]), 2)
        self.assertEqual(len(quantum.outputs[datasetTypeOutput]), 2)

    def testSerialization(self):
        taskName = f"{MockTask.__module__}.{MockTask.__qualname__}"
        # from simple w/o records
        quantum, _ = self._buildFullQuantum(taskName)
        serialized = quantum.to_simple()
        self.assertEqual(quantum, quantum.from_simple(serialized, DimensionUniverse()))

        # from simple w/ records
        quantum, _ = self._buildFullQuantum(taskName, addRecords=True)
        serialized = quantum.to_simple()
        self.assertEqual(quantum, quantum.from_simple(serialized, DimensionUniverse()))

        # verify direct works
        jsonVersion = json.loads(serialized.model_dump_json())
        fromDirect = SerializedQuantum.direct(**jsonVersion)
        self.assertEqual(fromDirect, serialized)

        # verify direct with records works
        quantum, _ = self._buildFullQuantum(taskName, addRecords=True)
        serialized = quantum.to_simple()
        jsonVersion = json.loads(serialized.model_dump_json())
        fromDirect = SerializedQuantum.direct(**jsonVersion)
        self.assertEqual(fromDirect, serialized)

        # verify the simple accumulator works
        accumulator = DimensionRecordsAccumulator()
        quantum, _ = self._buildFullQuantum(taskName, addRecords=True)
        serialized = quantum.to_simple(accumulator)
        # verify the accumulator was populated
        recordMapping = accumulator.makeSerializedDimensionRecordMapping()
        self.assertGreater(len(recordMapping), 0)
        # verify the dimension records were not written out
        self.assertEqual(serialized.dimensionRecords, None)
        serialized.dimensionRecords = accumulator.makeSerializedDimensionRecordMapping()  # type: ignore

        self.assertEqual(quantum, quantum.from_simple(serialized, universe=DimensionUniverse()))


if __name__ == "__main__":
    unittest.main()

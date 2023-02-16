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

import json
import os
import unittest
import uuid
from typing import cast

from lsst.daf.butler import (
    Butler,
    Config,
    DatasetRef,
    DatasetType,
    DimensionUniverse,
    Quantum,
    QuantumBackedButler,
    QuantumProvenanceData,
    Registry,
    RegistryConfig,
    StorageClass,
)
from lsst.daf.butler.tests.utils import makeTestTempDir, removeTestTempDir

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class QuantumBackedButlerTestCase(unittest.TestCase):
    """Test case for QuantumBackedButler."""

    def setUp(self) -> None:
        self.root = makeTestTempDir(TESTDIR)
        self.config = Config()
        self.config["root"] = self.root
        self.universe = DimensionUniverse()

        # Make a butler and import dimension definitions.
        registryConfig = RegistryConfig(self.config.get("registry"))
        Registry.createFromConfig(registryConfig, butlerRoot=self.root)
        self.butler = Butler(self.config, writeable=True, run="RUN")
        self.butler.import_(filename=os.path.join(TESTDIR, "data", "registry", "base.yaml"))

        # make all dataset types
        graph = self.universe.extract(("instrument", "detector"))
        storageClass = StorageClass("StructuredDataDict")
        self.datasetTypeInit = DatasetType("test_ds_init", graph, storageClass)
        self.datasetTypeInput = DatasetType("test_ds_input", graph, storageClass)
        self.datasetTypeOutput = DatasetType("test_ds_output", graph, storageClass)
        self.datasetTypeOutput2 = DatasetType("test_ds_output2", graph, storageClass)
        self.datasetTypeExtra = DatasetType("test_ds_extra", graph, storageClass)

        self.dataset_types: dict[str, DatasetType] = {}
        dataset_types = (
            self.datasetTypeInit,
            self.datasetTypeInput,
            self.datasetTypeOutput,
            self.datasetTypeOutput2,
            self.datasetTypeExtra,
        )
        for dataset_type in dataset_types:
            self.butler.registry.registerDatasetType(dataset_type)
            self.dataset_types[dataset_type.name] = dataset_type

        dataIds = [
            self.butler.registry.expandDataId(dict(instrument="Cam1", detector=detector_id))
            for detector_id in (1, 2, 3, 4)
        ]

        # make actual input datasets
        self.input_refs = [
            self.butler.put({"data": dataId["detector"]}, self.datasetTypeInput, dataId) for dataId in dataIds
        ]
        self.init_inputs_refs = [self.butler.put({"data": -1}, self.datasetTypeInit, dataIds[0])]
        self.all_input_refs = self.input_refs + self.init_inputs_refs

        # generate dataset refs for outputs
        self.output_refs = [
            DatasetRef(self.datasetTypeOutput, dataId, id=uuid.uuid4(), run="RUN") for dataId in dataIds
        ]
        self.output_refs2 = [
            DatasetRef(self.datasetTypeOutput2, dataId, id=uuid.uuid4(), run="RUN") for dataId in dataIds
        ]

        self.missing_refs = [
            DatasetRef(self.datasetTypeExtra, dataId, id=uuid.uuid4(), run="RUN") for dataId in dataIds
        ]

    def tearDown(self) -> None:
        removeTestTempDir(self.root)

    def make_quantum(self, step: int = 1) -> Quantum:
        """Make a Quantum which includes datastore records."""

        if step == 1:
            datastore_records = self.butler.datastore.export_records(self.all_input_refs)
            predictedInputs = {self.datasetTypeInput: self.input_refs}
            outputs = {self.datasetTypeOutput: self.output_refs}
            initInputs = {self.datasetTypeInit: self.init_inputs_refs[0]}
        elif step == 2:
            # The result should be empty, this is just to test that it works.
            datastore_records = self.butler.datastore.export_records(self.output_refs)
            predictedInputs = {self.datasetTypeInput: self.output_refs}
            outputs = {self.datasetTypeOutput2: self.output_refs2}
            initInputs = {}
        else:
            raise ValueError(f"unexpected {step} value")

        return Quantum(
            taskName="some.task.name",
            inputs=predictedInputs,
            outputs=outputs,
            initInputs=initInputs,
            datastore_records=datastore_records,
        )

    def test_initialize(self) -> None:
        """Test for initialize factory method"""

        quantum = self.make_quantum()
        qbb = QuantumBackedButler.initialize(
            config=self.config, quantum=quantum, dimensions=self.universe, dataset_types=self.dataset_types
        )
        self._test_factory(qbb)

    def test_from_predicted(self) -> None:
        """Test for from_predicted factory method"""

        datastore_records = self.butler.datastore.export_records(self.all_input_refs)
        qbb = QuantumBackedButler.from_predicted(
            config=self.config,
            predicted_inputs=[ref.getCheckedId() for ref in self.all_input_refs],
            predicted_outputs=[ref.getCheckedId() for ref in self.output_refs],
            dimensions=self.universe,
            datastore_records=datastore_records,
            dataset_types=self.dataset_types,
        )
        self._test_factory(qbb)

    def _test_factory(self, qbb: QuantumBackedButler) -> None:
        """Test state immediately after construction."""

        self.assertTrue(qbb.isWriteable())
        self.assertEqual(qbb._predicted_inputs, set(ref.id for ref in self.all_input_refs))
        self.assertEqual(qbb._predicted_outputs, set(ref.id for ref in self.output_refs))
        self.assertEqual(qbb._available_inputs, set())
        self.assertEqual(qbb._unavailable_inputs, set())
        self.assertEqual(qbb._actual_inputs, set())
        self.assertEqual(qbb._actual_output_refs, set())

    def test_getPutDirect(self) -> None:
        """Test for getDirect/putDirect methods"""

        quantum = self.make_quantum()
        qbb = QuantumBackedButler.initialize(
            config=self.config, quantum=quantum, dimensions=self.universe, dataset_types=self.dataset_types
        )

        # Verify all input data are readable.
        for ref in self.input_refs:
            data = qbb.getDirect(ref)
            self.assertEqual(data, {"data": ref.dataId["detector"]})
        for ref in self.init_inputs_refs:
            data = qbb.getDirect(ref)
            self.assertEqual(data, {"data": -1})
        for ref in self.missing_refs:
            with self.assertRaises(FileNotFoundError):
                data = qbb.getDirect(ref)

        self.assertEqual(qbb._available_inputs, qbb._predicted_inputs)
        self.assertEqual(qbb._actual_inputs, qbb._predicted_inputs)
        self.assertEqual(qbb._unavailable_inputs, set(ref.id for ref in self.missing_refs))

        # Write all expected outputs.
        for ref in self.output_refs:
            qbb.putDirect({"data": cast(int, ref.dataId["detector"]) ** 2}, ref)

        # Must be able to read them back
        for ref in self.output_refs:
            data = qbb.getDirect(ref)
            self.assertEqual(data, {"data": cast(int, ref.dataId["detector"]) ** 2})

        self.assertEqual(qbb._actual_output_refs, set(self.output_refs))

    def test_getDirectDeferred(self) -> None:
        """Test for getDirectDeferred method"""

        quantum = self.make_quantum()
        qbb = QuantumBackedButler.initialize(
            config=self.config, quantum=quantum, dimensions=self.universe, dataset_types=self.dataset_types
        )

        # get some input data
        input_refs = self.input_refs[:2]
        for ref in input_refs:
            data = qbb.getDirectDeferred(ref)
            self.assertEqual(data.get(), {"data": ref.dataId["detector"]})
        for ref in self.init_inputs_refs:
            data = qbb.getDirectDeferred(ref)
            self.assertEqual(data.get(), {"data": -1})
        for ref in self.missing_refs:
            data = qbb.getDirectDeferred(ref)
            with self.assertRaises(FileNotFoundError):
                data.get()

        # _avalable_inputs is not
        self.assertEqual(qbb._available_inputs, set(ref.id for ref in input_refs + self.init_inputs_refs))
        self.assertEqual(qbb._actual_inputs, set(ref.id for ref in input_refs + self.init_inputs_refs))
        self.assertEqual(qbb._unavailable_inputs, set(ref.id for ref in self.missing_refs))

    def test_datasetExistsDirect(self) -> None:
        """Test for datasetExistsDirect method"""

        quantum = self.make_quantum()
        qbb = QuantumBackedButler.initialize(
            config=self.config, quantum=quantum, dimensions=self.universe, dataset_types=self.dataset_types
        )

        # get some input data
        input_refs = self.input_refs[:2]
        for ref in input_refs:
            exists = qbb.datasetExistsDirect(ref)
            self.assertTrue(exists)
        for ref in self.init_inputs_refs:
            exists = qbb.datasetExistsDirect(ref)
            self.assertTrue(exists)
        for ref in self.missing_refs:
            exists = qbb.datasetExistsDirect(ref)
            self.assertFalse(exists)

        # _available_inputs is not
        self.assertEqual(qbb._available_inputs, set(ref.id for ref in input_refs + self.init_inputs_refs))
        self.assertEqual(qbb._actual_inputs, set())
        self.assertEqual(qbb._unavailable_inputs, set())  # this is not consistent with getDirect?

    def test_markInputUnused(self) -> None:
        """Test for markInputUnused method"""

        quantum = self.make_quantum()
        qbb = QuantumBackedButler.initialize(
            config=self.config, quantum=quantum, dimensions=self.universe, dataset_types=self.dataset_types
        )

        # get some input data
        for ref in self.input_refs:
            data = qbb.getDirect(ref)
            self.assertEqual(data, {"data": ref.dataId["detector"]})
        for ref in self.init_inputs_refs:
            data = qbb.getDirect(ref)
            self.assertEqual(data, {"data": -1})

        self.assertEqual(qbb._available_inputs, qbb._predicted_inputs)
        self.assertEqual(qbb._actual_inputs, qbb._predicted_inputs)

        qbb.markInputUnused(self.input_refs[0])
        self.assertEqual(
            qbb._actual_inputs, set(ref.id for ref in self.input_refs[1:] + self.init_inputs_refs)
        )

    def test_pruneDatasets(self) -> None:
        """Test for pruneDatasets methods"""

        quantum = self.make_quantum()
        qbb = QuantumBackedButler.initialize(
            config=self.config, quantum=quantum, dimensions=self.universe, dataset_types=self.dataset_types
        )

        # Write all expected outputs.
        for ref in self.output_refs:
            qbb.putDirect({"data": cast(int, ref.dataId["detector"]) ** 2}, ref)

        # Must be able to read them back
        for ref in self.output_refs:
            data = qbb.getDirect(ref)
            self.assertEqual(data, {"data": cast(int, ref.dataId["detector"]) ** 2})

        # Check for invalid arguments.
        with self.assertRaisesRegex(TypeError, "Cannot pass purge=True without disassociate=True"):
            qbb.pruneDatasets(self.output_refs, disassociate=False, unstore=True, purge=True)
        with self.assertRaisesRegex(TypeError, "Cannot pass purge=True without unstore=True"):
            qbb.pruneDatasets(self.output_refs, disassociate=True, unstore=False, purge=True)
        with self.assertRaisesRegex(TypeError, "Cannot pass disassociate=True without purge=True"):
            qbb.pruneDatasets(self.output_refs, disassociate=True, unstore=True, purge=False)

        # Disassociate only.
        ref = self.output_refs[0]
        qbb.pruneDatasets([ref], disassociate=False, unstore=True, purge=False)
        self.assertFalse(qbb.datasetExistsDirect(ref))
        with self.assertRaises(FileNotFoundError):
            data = qbb.getDirect(ref)

        # can store it again
        qbb.putDirect({"data": cast(int, ref.dataId["detector"]) ** 2}, ref)
        self.assertTrue(qbb.datasetExistsDirect(ref))

        # Purge completely.
        ref = self.output_refs[1]
        qbb.pruneDatasets([ref], disassociate=True, unstore=True, purge=True)
        self.assertFalse(qbb.datasetExistsDirect(ref))
        with self.assertRaises(FileNotFoundError):
            data = qbb.getDirect(ref)
        qbb.putDirect({"data": cast(int, ref.dataId["detector"]) ** 2}, ref)
        self.assertTrue(qbb.datasetExistsDirect(ref))

    def test_extract_provenance_data(self) -> None:
        """Test for extract_provenance_data method"""

        quantum = self.make_quantum()
        qbb = QuantumBackedButler.initialize(
            config=self.config, quantum=quantum, dimensions=self.universe, dataset_types=self.dataset_types
        )

        # read/store everything
        for ref in self.input_refs:
            qbb.getDirect(ref)
        for ref in self.init_inputs_refs:
            qbb.getDirect(ref)
        for ref in self.output_refs:
            qbb.putDirect({"data": cast(int, ref.dataId["detector"]) ** 2}, ref)

        provenance1 = qbb.extract_provenance_data()
        prov_json = provenance1.json()
        provenance2 = QuantumProvenanceData.direct(**json.loads(prov_json))
        for provenance in (provenance1, provenance2):
            input_ids = set(ref.id for ref in self.input_refs + self.init_inputs_refs)
            self.assertEqual(provenance.predicted_inputs, input_ids)
            self.assertEqual(provenance.available_inputs, input_ids)
            self.assertEqual(provenance.actual_inputs, input_ids)
            output_ids = set(ref.id for ref in self.output_refs)
            self.assertEqual(provenance.predicted_outputs, output_ids)
            self.assertEqual(provenance.actual_outputs, output_ids)
            datastore_name = "FileDatastore@<butlerRoot>/datastore"
            self.assertEqual(set(provenance.datastore_records.keys()), {datastore_name})
            datastore_records = provenance.datastore_records[datastore_name]
            self.assertEqual(set(datastore_records.dataset_ids), output_ids)
            class_name = "lsst.daf.butler.core.storedFileInfo.StoredFileInfo"
            table_name = "file_datastore_records"
            self.assertEqual(set(datastore_records.records.keys()), {class_name})
            self.assertEqual(set(datastore_records.records[class_name].keys()), {table_name})
            self.assertEqual(
                set(record["dataset_id"] for record in datastore_records.records[class_name][table_name]),
                output_ids,
            )

    def test_collect_and_transfer(self) -> None:
        """Test for collect_and_transfer method"""

        quantum1 = self.make_quantum(1)
        qbb1 = QuantumBackedButler.initialize(
            config=self.config, quantum=quantum1, dimensions=self.universe, dataset_types=self.dataset_types
        )

        quantum2 = self.make_quantum(2)
        qbb2 = QuantumBackedButler.initialize(
            config=self.config, quantum=quantum2, dimensions=self.universe, dataset_types=self.dataset_types
        )

        # read/store everything
        for ref in self.input_refs:
            qbb1.getDirect(ref)
        for ref in self.init_inputs_refs:
            qbb1.getDirect(ref)
        for ref in self.output_refs:
            qbb1.putDirect({"data": cast(int, ref.dataId["detector"]) ** 2}, ref)

        for ref in self.output_refs:
            qbb2.getDirect(ref)
        for ref in self.output_refs2:
            qbb2.putDirect({"data": cast(int, ref.dataId["detector"]) ** 3}, ref)

        QuantumProvenanceData.collect_and_transfer(
            self.butler,
            [quantum1, quantum2],
            [qbb1.extract_provenance_data(), qbb2.extract_provenance_data()],
        )

        for ref in self.output_refs:
            data = self.butler.getDirect(ref)
            self.assertEqual(data, {"data": cast(int, ref.dataId["detector"]) ** 2})

        for ref in self.output_refs2:
            data = self.butler.getDirect(ref)
            self.assertEqual(data, {"data": cast(int, ref.dataId["detector"]) ** 3})


if __name__ == "__main__":
    unittest.main()

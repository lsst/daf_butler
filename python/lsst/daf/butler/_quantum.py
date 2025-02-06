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

from __future__ import annotations

__all__ = ("DimensionRecordsAccumulator", "Quantum", "SerializedQuantum")

import sys
from collections.abc import Iterable, Mapping, MutableMapping, Sequence
from typing import Any

import pydantic

from lsst.utils import doImportType

from ._dataset_ref import DatasetRef, SerializedDatasetRef
from ._dataset_type import DatasetType, SerializedDatasetType
from ._named import NamedKeyDict, NamedKeyMapping
from .datastore.record_data import DatastoreRecordData, SerializedDatastoreRecordData
from .dimensions import (
    DataCoordinate,
    DimensionRecord,
    DimensionUniverse,
    SerializedDataCoordinate,
    SerializedDimensionRecord,
)


def _reconstructDatasetRef(
    simple: SerializedDatasetRef,
    type_: DatasetType | None,
    ids: Iterable[int],
    dimensionRecords: dict[int, SerializedDimensionRecord] | None,
    universe: DimensionUniverse,
) -> DatasetRef:
    """Reconstruct a DatasetRef stored in a Serialized Quantum."""
    # Reconstruct the dimension records
    # if the dimension record has been loaded previously use that,
    # otherwise load it from the dict of Serialized DimensionRecords
    if dimensionRecords is None and ids:
        raise ValueError("Cannot construct from a SerializedQuantum with no dimension records. ")
    records = {}
    for dId in ids:
        # Ignore typing because it is missing that the above if statement
        # ensures that if there is a loop that dimensionRecords is not None.
        tmpSerialized = dimensionRecords[dId]  # type: ignore
        records[tmpSerialized.definition] = tmpSerialized
    if simple.dataId is not None:
        simple.dataId.records = records or None
    rebuiltDatasetRef = DatasetRef.from_simple(simple, universe, datasetType=type_)
    return rebuiltDatasetRef


class SerializedQuantum(pydantic.BaseModel):
    """Simplified model of a `Quantum` suitable for serialization."""

    taskName: str | None = None
    dataId: SerializedDataCoordinate | None = None
    datasetTypeMapping: Mapping[str, SerializedDatasetType]
    initInputs: Mapping[str, tuple[SerializedDatasetRef, list[int]]]
    inputs: Mapping[str, list[tuple[SerializedDatasetRef, list[int]]]]
    outputs: Mapping[str, list[tuple[SerializedDatasetRef, list[int]]]]
    dimensionRecords: dict[int, SerializedDimensionRecord] | None = None
    datastoreRecords: dict[str, SerializedDatastoreRecordData] | None = None

    @classmethod
    def direct(
        cls,
        *,
        taskName: str | None,
        dataId: dict | None,
        datasetTypeMapping: Mapping[str, dict],
        initInputs: Mapping[str, tuple[dict, list[int]]],
        inputs: Mapping[str, list[tuple[dict, list[int]]]],
        outputs: Mapping[str, list[tuple[dict, list[int]]]],
        dimensionRecords: dict[int, dict] | None,
        datastoreRecords: dict[str, dict] | None,
    ) -> SerializedQuantum:
        """Construct a `SerializedQuantum` directly without validators.

        Parameters
        ----------
        taskName : `str` or `None`
            The name of the task.
        dataId : `dict` or `None`
            The dataId of the quantum.
        datasetTypeMapping : `~collections.abc.Mapping` [`str`, `dict`]
            Dataset type definitions.
        initInputs : `~collections.abc.Mapping`
            The quantum init inputs.
        inputs : `~collections.abc.Mapping`
            The quantum inputs.
        outputs : `~collections.abc.Mapping`
            The quantum outputs.
        dimensionRecords : `dict` [`int`, `dict`] or `None`
            The dimension records.
        datastoreRecords : `dict` [`str`, `dict`] or `None`
            The datastore records.

        Returns
        -------
        quantum : `SerializedQuantum`
            Serializable model of the quantum.

        Notes
        -----
        This differs from the pydantic "construct" method in that the arguments
        are explicitly what the model requires, and it will recurse through
        members, constructing them from their corresponding `direct` methods.

        This method should only be called when the inputs are trusted.
        """
        serialized_dataId = SerializedDataCoordinate.direct(**dataId) if dataId is not None else None
        serialized_datasetTypeMapping = {
            k: SerializedDatasetType.direct(**v) for k, v in datasetTypeMapping.items()
        }
        serialized_initInputs = {
            k: (SerializedDatasetRef.direct(**v), refs) for k, (v, refs) in initInputs.items()
        }
        serialized_inputs = {
            k: [(SerializedDatasetRef.direct(**ref), id) for ref, id in v] for k, v in inputs.items()
        }
        serialized_outputs = {
            k: [(SerializedDatasetRef.direct(**ref), id) for ref, id in v] for k, v in outputs.items()
        }
        serialized_records = (
            {int(k): SerializedDimensionRecord.direct(**v) for k, v in dimensionRecords.items()}
            if dimensionRecords is not None
            else None
        )
        serialized_datastore_records = (
            {k: SerializedDatastoreRecordData.direct(**v) for k, v in datastoreRecords.items()}
            if datastoreRecords is not None
            else None
        )

        node = cls.model_construct(
            taskName=sys.intern(taskName or ""),
            dataId=serialized_dataId,
            datasetTypeMapping=serialized_datasetTypeMapping,
            initInputs=serialized_initInputs,
            inputs=serialized_inputs,
            outputs=serialized_outputs,
            dimensionRecords=serialized_records,
            datastoreRecords=serialized_datastore_records,
        )

        return node


class Quantum:
    """Class representing a discrete unit of work.

    A Quantum may depend on one or more datasets and produce one or more
    datasets.

    Most Quanta will be executions of a particular ``PipelineTask``’s
    ``runQuantum`` method, but they can also be used to represent discrete
    units of work performed manually by human operators or other software
    agents.

    Parameters
    ----------
    taskName : `str`, optional
        Fully-qualified name of the Task class that executed or will execute
        this Quantum.  If not provided, ``taskClass`` must be.
    taskClass : `type`, optional
        The Task class that executed or will execute this Quantum.  If not
        provided, ``taskName`` must be.  Overrides ``taskName`` if both are
        provided.
    dataId : `DataId`, optional
        The dimension values that identify this `Quantum`.
    initInputs : collection of `DatasetRef`, optional
        Datasets that are needed to construct an instance of the Task.  May
        be a flat iterable of `DatasetRef` instances or a mapping from
        `DatasetType` to `DatasetRef`.
    inputs : `~collections.abc.Mapping`, optional
        Inputs identified prior to execution, organized as a mapping from
        `DatasetType` to a list of `DatasetRef`.
    outputs : `~collections.abc.Mapping`, optional
        Outputs from executing this quantum of work, organized as a mapping
        from `DatasetType` to a list of `DatasetRef`.
    datastore_records : `DatastoreRecordData`, optional
        Datastore record data for input or initInput datasets that already
        exist.
    """

    __slots__ = (
        "_taskName",
        "_taskClass",
        "_dataId",
        "_initInputs",
        "_inputs",
        "_outputs",
        "_datastore_records",
    )

    def __init__(
        self,
        *,
        taskName: str | None = None,
        taskClass: type | None = None,
        dataId: DataCoordinate | None = None,
        initInputs: Mapping[DatasetType, DatasetRef] | Iterable[DatasetRef] | None = None,
        inputs: Mapping[DatasetType, Sequence[DatasetRef]] | None = None,
        outputs: Mapping[DatasetType, Sequence[DatasetRef]] | None = None,
        datastore_records: Mapping[str, DatastoreRecordData] | None = None,
    ):
        if taskClass is not None:
            taskName = f"{taskClass.__module__}.{taskClass.__name__}"
        self._taskName = taskName
        self._taskClass = taskClass
        self._dataId = dataId
        if initInputs is None:
            initInputs = {}
        elif not isinstance(initInputs, Mapping):
            initInputs = {ref.datasetType: ref for ref in initInputs}
        if inputs is None:
            inputs = {}
        if outputs is None:
            outputs = {}
        self._initInputs = NamedKeyDict[DatasetType, DatasetRef](initInputs).freeze()
        self._inputs = NamedKeyDict[DatasetType, tuple[DatasetRef]](
            (k, tuple(v)) for k, v in inputs.items()
        ).freeze()
        self._outputs = NamedKeyDict[DatasetType, tuple[DatasetRef]](
            (k, tuple(v)) for k, v in outputs.items()
        ).freeze()
        if datastore_records is None:
            datastore_records = {}
        self._datastore_records = datastore_records

    def to_simple(self, accumulator: DimensionRecordsAccumulator | None = None) -> SerializedQuantum:
        """Convert this class to a simple python type.

        This makes it suitable for serialization.

        Parameters
        ----------
        accumulator : `DimensionRecordsAccumulator`, optional
            This accumulator can be used to aggregate dimension records accross
            multiple Quanta. If this is None, the default, dimension records
            are serialized with this Quantum. If an accumulator is supplied it
            is assumed something else is responsible for serializing the
            records, and they will not be stored with the SerializedQuantum.

        Returns
        -------
        simple : `SerializedQuantum`
            This object converted to a serializable representation.
        """
        typeMapping = {}
        initInputs = {}

        if accumulator is None:
            accumulator = DimensionRecordsAccumulator()
            writeDimensionRecords = True
        else:
            writeDimensionRecords = False

        # collect the init inputs for serialization, recording the types into
        # their own mapping, used throughout to minimize saving the same object
        # multiple times. String name of the type used to index mappings.
        for key, value in self._initInputs.items():
            # add the type to the typeMapping
            typeMapping[key.name] = key.to_simple()
            # convert to a simple DatasetRef representation
            simple = value.to_simple()
            # extract the dimension records
            recIds = []
            if simple.dataId is not None and simple.dataId.records is not None:
                # for each dimension record get a id by adding it to the
                # record accumulator.
                for element_name in value.dataId.dimensions.elements:
                    rec = value.dataId.records[element_name]
                    if rec is not None:
                        recordId = accumulator.addRecord(rec)
                        recIds.append(recordId)
                # Set properties to None to save space
                simple.dataId.records = None
            simple.datasetType = None
            initInputs[key.name] = (simple, recIds)

        # container for all the SerializedDatasetRefs, keyed on the
        # DatasetType name.
        inputs = {}

        # collect the inputs
        for key, values in self._inputs.items():
            # collect type if it is not already in the mapping
            if key.name not in typeMapping:
                typeMapping[key.name] = key.to_simple()
            # for each input type there are a list of inputs, collect them
            tmp = []
            for e in values:
                simp = e.to_simple()
                # This container will hold ids (hashes) that point to all the
                # dimension records within the SerializedDatasetRef dataId
                # These dimension records repeat in almost every DatasetRef
                # So it is hugely wasteful in terms of disk and cpu time to
                # store them over and over again.
                recIds = []
                if simp.dataId is not None and simp.dataId.records is not None:
                    for element_name in e.dataId.dimensions.elements:
                        rec = e.dataId.records[element_name]
                        # for each dimension record get a id by adding it to
                        # the record accumulator.
                        if rec is not None:
                            recordId = accumulator.addRecord(rec)
                            recIds.append(recordId)
                    # Set the records to None to avoid serializing them
                    simp.dataId.records = None
                # Dataset type is the same as the key in _inputs, no need
                # to serialize it out multiple times, set it to None
                simp.datasetType = None
                # append a tuple of the simplified SerializedDatasetRef, along
                # with the list of all the keys for the dimension records
                # needed for reconstruction.
                tmp.append((simp, recIds))
            inputs[key.name] = tmp

        # container for all the SerializedDatasetRefs, keyed on the
        # DatasetType name.
        outputs = {}
        for key, values in self._outputs.items():
            # collect type if it is not already in the mapping
            if key.name not in typeMapping:
                typeMapping[key.name] = key.to_simple()
            # for each output type there are a list of inputs, collect them
            tmp = []
            for e in values:
                simp = e.to_simple()
                # This container will hold ids (hashes) that point to all the
                # dimension records within the SerializedDatasetRef dataId
                # These dimension records repeat in almost every DatasetRef
                # So it is hugely wasteful in terms of disk and cpu time to
                # store them over and over again.
                recIds = []
                if simp.dataId is not None and simp.dataId.records is not None:
                    for element_name in e.dataId.dimensions.elements:
                        rec = e.dataId.records[element_name]
                        # for each dimension record get a id by adding it to
                        # the record accumulator.
                        if rec is not None:
                            recordId = accumulator.addRecord(rec)
                            recIds.append(recordId)
                    # Set the records to None to avoid serializing them
                    simp.dataId.records = None
                # Dataset type is the same as the key in _outputs, no need
                # to serialize it out multiple times, set it to None
                simp.datasetType = None
                # append a tuple of the simplified SerializedDatasetRef, along
                # with the list of all the keys for the dimension records
                # needed for reconstruction.
                tmp.append((simp, recIds))
            outputs[key.name] = tmp

        dimensionRecords: Mapping[int, SerializedDimensionRecord] | None
        if writeDimensionRecords:
            dimensionRecords = accumulator.makeSerializedDimensionRecordMapping()
        else:
            dimensionRecords = None

        datastore_records: dict[str, SerializedDatastoreRecordData] | None = None
        if self.datastore_records is not None:
            datastore_records = {
                datastore_name: record_data.to_simple()
                for datastore_name, record_data in self.datastore_records.items()
            }

        return SerializedQuantum(
            taskName=self._taskName,
            dataId=self.dataId.to_simple() if self.dataId is not None else None,
            datasetTypeMapping=typeMapping,
            initInputs=initInputs,
            inputs=inputs,
            outputs=outputs,
            dimensionRecords=dimensionRecords,
            datastoreRecords=datastore_records,
        )

    @classmethod
    def from_simple(
        cls,
        simple: SerializedQuantum,
        universe: DimensionUniverse,
    ) -> Quantum:
        """Construct a new object from a simplified form.

        Generally this is data returned from the `to_simple` method.

        Parameters
        ----------
        simple : SerializedQuantum
            The value returned by a call to `to_simple`.
        universe : `DimensionUniverse`
            The special graph of all known dimensions.
        """
        initInputs: MutableMapping[DatasetType, DatasetRef] = {}

        # Unpersist all the init inputs
        for key, (value, dimensionIds) in simple.initInputs.items():
            type_ = DatasetType.from_simple(simple.datasetTypeMapping[key], universe=universe)
            # reconstruct the dimension records
            rebuiltDatasetRef = _reconstructDatasetRef(
                value, type_, dimensionIds, simple.dimensionRecords, universe
            )
            initInputs[type_] = rebuiltDatasetRef

        # containers for the dataset refs
        inputs: MutableMapping[DatasetType, list[DatasetRef]] = {}
        outputs: MutableMapping[DatasetType, list[DatasetRef]] = {}

        for container, simpleRefs in ((inputs, simple.inputs), (outputs, simple.outputs)):
            for key, values in simpleRefs.items():
                type_ = DatasetType.from_simple(simple.datasetTypeMapping[key], universe=universe)
                # reconstruct the list of DatasetRefs for this DatasetType
                tmp: list[DatasetRef] = []
                for v, recIds in values:
                    rebuiltDatasetRef = _reconstructDatasetRef(
                        v, type_, recIds, simple.dimensionRecords, universe
                    )
                    tmp.append(rebuiltDatasetRef)
                container[type_] = tmp

        dataId = (
            DataCoordinate.from_simple(simple.dataId, universe=universe)
            if simple.dataId is not None
            else None
        )

        datastore_records: dict[str, DatastoreRecordData] | None = None
        if simple.datastoreRecords is not None:
            datastore_records = {
                datastore_name: DatastoreRecordData.from_simple(record_data)
                for datastore_name, record_data in simple.datastoreRecords.items()
            }

        quant = Quantum(
            taskName=simple.taskName,
            dataId=dataId,
            initInputs=initInputs,
            inputs=inputs,
            outputs=outputs,
            datastore_records=datastore_records,
        )
        return quant

    @property
    def taskClass(self) -> type | None:
        """Task class associated with this `Quantum` (`type`)."""
        if self._taskClass is None:
            if self._taskName is None:
                raise ValueError("No task class defined and task name is None")
            task_class = doImportType(self._taskName)
            self._taskClass = task_class
        return self._taskClass

    @property
    def taskName(self) -> str | None:
        """Return Fully-qualified name of the task associated with `Quantum`.

        (`str`).
        """
        return self._taskName

    @property
    def dataId(self) -> DataCoordinate | None:
        """Return dimension values of the unit of processing (`DataId`)."""
        return self._dataId

    @property
    def initInputs(self) -> NamedKeyMapping[DatasetType, DatasetRef]:
        """Return mapping of datasets used to construct the Task.

        Has `DatasetType` instances as keys (names can also be used for
        lookups) and `DatasetRef` instances as values.
        """
        return self._initInputs

    @property
    def inputs(self) -> NamedKeyMapping[DatasetType, tuple[DatasetRef]]:
        """Return mapping of input datasets that were expected to be used.

        Has `DatasetType` instances as keys (names can also be used for
        lookups) and a list of `DatasetRef` instances as values.

        Notes
        -----
        We cannot use `set` instead of `list` for the nested container because
        `DatasetRef` instances cannot be compared reliably when some have
        integers IDs and others do not.
        """
        return self._inputs

    @property
    def outputs(self) -> NamedKeyMapping[DatasetType, tuple[DatasetRef]]:
        """Return mapping of output datasets (to be) generated by this quantum.

        Has the same form as ``predictedInputs``.

        Notes
        -----
        We cannot use `set` instead of `list` for the nested container because
        `DatasetRef` instances cannot be compared reliably when some have
        integers IDs and others do not.
        """
        return self._outputs

    @property
    def datastore_records(self) -> Mapping[str, DatastoreRecordData]:
        """Tabular data stored with this quantum (`dict`).

        This attribute may be modified in place, but not assigned to.
        """
        return self._datastore_records

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Quantum):
            return False
        for item in ("taskClass", "dataId", "initInputs", "inputs", "outputs"):
            if getattr(self, item) != getattr(other, item):
                return False
        return True

    def __hash__(self) -> int:
        return hash((self.taskClass, self.dataId))

    def __reduce__(self) -> str | tuple[Any, ...]:
        return (
            self._reduceFactory,
            (
                self.taskName,
                self.taskClass,
                self.dataId,
                dict(self.initInputs.items()),
                dict(self.inputs),
                dict(self.outputs),
                self.datastore_records,
            ),
        )

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(taskName={self.taskName}, dataId={self.dataId})"

    @staticmethod
    def _reduceFactory(
        taskName: str | None,
        taskClass: type | None,
        dataId: DataCoordinate | None,
        initInputs: Mapping[DatasetType, DatasetRef] | Iterable[DatasetRef] | None,
        inputs: Mapping[DatasetType, list[DatasetRef]] | None,
        outputs: Mapping[DatasetType, list[DatasetRef]] | None,
        datastore_records: Mapping[str, DatastoreRecordData],
    ) -> Quantum:
        return Quantum(
            taskName=taskName,
            taskClass=taskClass,
            dataId=dataId,
            initInputs=initInputs,
            inputs=inputs,
            outputs=outputs,
            datastore_records=datastore_records,
        )


class DimensionRecordsAccumulator:
    """Class used to accumulate dimension records for serialization.

    This class generates an auto increment key for each unique dimension record
    added to it. This allows serialization of dimension records to occur once
    for each record but be refereed to multiple times.
    """

    def __init__(self) -> None:
        self._counter = 0
        self.mapping: MutableMapping[DimensionRecord, tuple[int, SerializedDimensionRecord]] = {}

    def addRecord(self, record: DimensionRecord) -> int:
        """Add a dimension record to the accumulator if it has not already been
        added. When a record is inserted for the first time it is assigned
        a unique integer key.

        This function returns the key associated with the record (either the
        newly allocated key, or the existing one).

        Parameters
        ----------
        record : `DimensionRecord`
            The record to add to the accumulator.

        Returns
        -------
        accumulatorKey : int
            The key that is associated with the supplied record.
        """
        if (mappingValue := self.mapping.get(record)) is None:
            simple = record.to_simple()
            mappingValue = (self._counter, simple)
            self._counter += 1
            self.mapping[record] = mappingValue
        return mappingValue[0]

    def makeSerializedDimensionRecordMapping(self) -> dict[int, SerializedDimensionRecord]:
        return dict(self.mapping.values())

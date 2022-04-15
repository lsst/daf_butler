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

from __future__ import annotations

__all__ = ("Quantum", "SerializedQuantum", "DimensionRecordsAccumulator")

from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Type, Union

from lsst.utils import doImportType
from pydantic import BaseModel

from .datasets import DatasetRef, DatasetType, SerializedDatasetRef, SerializedDatasetType
from .datastoreRecordData import DatastoreRecordData, SerializedDatastoreRecordData
from .dimensions import (
    DataCoordinate,
    DimensionRecord,
    DimensionUniverse,
    SerializedDataCoordinate,
    SerializedDimensionRecord,
)
from .named import NamedKeyDict, NamedKeyMapping


def _reconstructDatasetRef(
    simple: SerializedDatasetRef,
    type_: Optional[DatasetType],
    ids: Iterable[int],
    dimensionRecords: Optional[Dict[int, SerializedDimensionRecord]],
    reconstitutedDimensions: Dict[int, Tuple[str, DimensionRecord]],
    universe: DimensionUniverse,
) -> DatasetRef:
    """Reconstruct a DatasetRef stored in a Serialized Quantum"""
    # Reconstruct the dimension records
    records = {}
    for dId in ids:
        # if the dimension record has been loaded previously use that,
        # otherwise load it from the dict of Serialized DimensionRecords
        if (recId := reconstitutedDimensions.get(dId)) is None:
            if dimensionRecords is None:
                raise ValueError(
                    "Cannot construct from a SerializedQuantum with no dimension records. "
                    "Reconstituted Dimensions must be supplied and populated in method call."
                )
            tmpSerialized = dimensionRecords[dId]
            reconstructedDim = DimensionRecord.from_simple(tmpSerialized, universe=universe)
            definition = tmpSerialized.definition
            reconstitutedDimensions[dId] = (definition, reconstructedDim)
        else:
            definition, reconstructedDim = recId
        records[definition] = reconstructedDim
    # turn the serialized form into an object and attach the dimension records
    rebuiltDatasetRef = DatasetRef.from_simple(simple, universe, datasetType=type_)
    if records:
        object.__setattr__(rebuiltDatasetRef, "dataId", rebuiltDatasetRef.dataId.expanded(records))
    return rebuiltDatasetRef


class SerializedQuantum(BaseModel):
    """Simplified model of a `Quantum` suitable for serialization."""

    taskName: str
    dataId: Optional[SerializedDataCoordinate]
    datasetTypeMapping: Mapping[str, SerializedDatasetType]
    initInputs: Mapping[str, Tuple[SerializedDatasetRef, List[int]]]
    inputs: Mapping[str, List[Tuple[SerializedDatasetRef, List[int]]]]
    outputs: Mapping[str, List[Tuple[SerializedDatasetRef, List[int]]]]
    dimensionRecords: Optional[Dict[int, SerializedDimensionRecord]] = None
    datastoreRecords: Optional[Dict[str, SerializedDatastoreRecordData]] = None

    @classmethod
    def direct(
        cls,
        *,
        taskName: str,
        dataId: Optional[Dict],
        datasetTypeMapping: Mapping[str, Dict],
        initInputs: Mapping[str, Tuple[Dict, List[int]]],
        inputs: Mapping[str, List[Tuple[Dict, List[int]]]],
        outputs: Mapping[str, List[Tuple[Dict, List[int]]]],
        dimensionRecords: Optional[Dict[int, Dict]],
        datastoreRecords: Optional[Dict[str, Dict]],
    ) -> SerializedQuantum:
        """Construct a `SerializedQuantum` directly without validators.

        This differs from the pydantic "construct" method in that the arguments
        are explicitly what the model requires, and it will recurse through
        members, constructing them from their corresponding `direct` methods.

        This method should only be called when the inputs are trusted.
        """
        node = SerializedQuantum.__new__(cls)
        setter = object.__setattr__
        setter(node, "taskName", taskName)
        setter(node, "dataId", dataId if dataId is None else SerializedDataCoordinate.direct(**dataId))
        setter(
            node,
            "datasetTypeMapping",
            {k: SerializedDatasetType.direct(**v) for k, v in datasetTypeMapping.items()},
        )
        setter(
            node,
            "initInputs",
            {k: (SerializedDatasetRef.direct(**v), refs) for k, (v, refs) in initInputs.items()},
        )
        setter(
            node,
            "inputs",
            {k: [(SerializedDatasetRef.direct(**ref), id) for ref, id in v] for k, v in inputs.items()},
        )
        setter(
            node,
            "outputs",
            {k: [(SerializedDatasetRef.direct(**ref), id) for ref, id in v] for k, v in outputs.items()},
        )
        setter(
            node,
            "dimensionRecords",
            dimensionRecords
            if dimensionRecords is None
            else {int(k): SerializedDimensionRecord.direct(**v) for k, v in dimensionRecords.items()},
        )
        setter(
            node,
            "datastoreRecords",
            datastoreRecords
            if datastoreRecords is None
            else {k: SerializedDatastoreRecordData.direct(**v) for k, v in datastoreRecords.items()},
        )
        setter(
            node,
            "__fields_set__",
            {
                "taskName",
                "dataId",
                "datasetTypeMapping",
                "initInputs",
                "inputs",
                "outputs",
                "dimensionRecords",
                "datastore_records",
            },
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
        "_hash",
        "_datastore_records",
    )

    def __init__(
        self,
        *,
        taskName: Optional[str] = None,
        taskClass: Optional[Type] = None,
        dataId: Optional[DataCoordinate] = None,
        initInputs: Optional[Union[Mapping[DatasetType, DatasetRef], Iterable[DatasetRef]]] = None,
        inputs: Optional[Mapping[DatasetType, List[DatasetRef]]] = None,
        outputs: Optional[Mapping[DatasetType, List[DatasetRef]]] = None,
        datastore_records: Optional[Mapping[str, DatastoreRecordData]] = None,
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
        self._inputs = NamedKeyDict[DatasetType, List[DatasetRef]](inputs).freeze()
        self._outputs = NamedKeyDict[DatasetType, List[DatasetRef]](outputs).freeze()
        if datastore_records is None:
            datastore_records = {}
        self._datastore_records = datastore_records

    def to_simple(self, accumulator: Optional[DimensionRecordsAccumulator] = None) -> SerializedQuantum:
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
                for rec in value.dataId.records.values():
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
                    for rec in e.dataId.records.values():
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
                    for rec in e.dataId.records.values():
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

        dimensionRecords: Optional[Mapping[int, SerializedDimensionRecord]]
        if writeDimensionRecords:
            dimensionRecords = accumulator.makeSerializedDimensionRecordMapping()
        else:
            dimensionRecords = None

        datastore_records: Optional[Dict[str, SerializedDatastoreRecordData]] = None
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
        reconstitutedDimensions: Optional[Dict[int, Tuple[str, DimensionRecord]]] = None,
    ) -> Quantum:
        """Construct a new object from a simplified form.

        Generally this is data returned from the `to_simple` method.

        Parameters
        ----------
        simple : SerializedQuantum
            The value returned by a call to `to_simple`
        universe : `DimensionUniverse`
            The special graph of all known dimensions.
        reconstitutedDimensions : `dict` of `int` to `DimensionRecord` or None
            A mapping of ids to dimension records to be used when populating
            dimensions for this Quantum. If supplied it will be used in place
            of the dimension Records stored with the SerializedQuantum, if a
            required dimension has already been loaded. Otherwise the record
            will be unpersisted from the SerializedQuatnum and added to the
            reconstitutedDimensions dict (if not None). Defaults to None.
        """
        loadedTypes: MutableMapping[str, DatasetType] = {}
        initInputs: MutableMapping[DatasetType, DatasetRef] = {}
        if reconstitutedDimensions is None:
            reconstitutedDimensions = {}

        # Unpersist all the init inputs
        for key, (value, dimensionIds) in simple.initInputs.items():
            # If a datasetType has already been created use that instead of
            # unpersisting.
            if (type_ := loadedTypes.get(key)) is None:
                type_ = loadedTypes.setdefault(
                    key, DatasetType.from_simple(simple.datasetTypeMapping[key], universe=universe)
                )
            # reconstruct the dimension records
            rebuiltDatasetRef = _reconstructDatasetRef(
                value, type_, dimensionIds, simple.dimensionRecords, reconstitutedDimensions, universe
            )
            initInputs[type_] = rebuiltDatasetRef

        # containers for the dataset refs
        inputs: MutableMapping[DatasetType, List[DatasetRef]] = {}
        outputs: MutableMapping[DatasetType, List[DatasetRef]] = {}

        for container, simpleRefs in ((inputs, simple.inputs), (outputs, simple.outputs)):
            for key, values in simpleRefs.items():
                # If a datasetType has already been created use that instead of
                # unpersisting.
                if (type_ := loadedTypes.get(key)) is None:
                    type_ = loadedTypes.setdefault(
                        key, DatasetType.from_simple(simple.datasetTypeMapping[key], universe=universe)
                    )
                # reconstruct the list of DatasetRefs for this DatasetType
                tmp: List[DatasetRef] = []
                for v, recIds in values:
                    rebuiltDatasetRef = _reconstructDatasetRef(
                        v, type_, recIds, simple.dimensionRecords, reconstitutedDimensions, universe
                    )
                    tmp.append(rebuiltDatasetRef)
                container[type_] = tmp

        dataId = (
            DataCoordinate.from_simple(simple.dataId, universe=universe)
            if simple.dataId is not None
            else None
        )

        datastore_records: Optional[Dict[str, DatastoreRecordData]] = None
        if simple.datastoreRecords is not None:
            datastore_records = {
                datastore_name: DatastoreRecordData.from_simple(record_data)
                for datastore_name, record_data in simple.datastoreRecords.items()
            }

        return Quantum(
            taskName=simple.taskName,
            dataId=dataId,
            initInputs=initInputs,
            inputs=inputs,
            outputs=outputs,
            datastore_records=datastore_records,
        )

    @property
    def taskClass(self) -> Optional[Type]:
        """Task class associated with this `Quantum` (`type`)."""
        if self._taskClass is None:
            if self._taskName is None:
                raise ValueError("No task class defined and task name is None")
            task_class = doImportType(self._taskName)
            self._taskClass = task_class
        return self._taskClass

    @property
    def taskName(self) -> Optional[str]:
        """Return Fully-qualified name of the task associated with `Quantum`.

        (`str`).
        """
        return self._taskName

    @property
    def dataId(self) -> Optional[DataCoordinate]:
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
    def inputs(self) -> NamedKeyMapping[DatasetType, List[DatasetRef]]:
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
    def outputs(self) -> NamedKeyMapping[DatasetType, List[DatasetRef]]:
        """Return mapping of output datasets (to be) generated by this quantum.

        Has the same form as `predictedInputs`.

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

    def __reduce__(self) -> Union[str, Tuple[Any, ...]]:
        return (
            self._reduceFactory,
            (
                self.taskName,
                self.taskClass,
                self.dataId,
                dict(self.initInputs.items()),
                dict(self.inputs),
                dict(self.outputs),
            ),
        )

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(taskName={self.taskName}, dataId={self.dataId})"

    @staticmethod
    def _reduceFactory(
        taskName: Optional[str],
        taskClass: Optional[Type],
        dataId: Optional[DataCoordinate],
        initInputs: Optional[Union[Mapping[DatasetType, DatasetRef], Iterable[DatasetRef]]],
        inputs: Optional[Mapping[DatasetType, List[DatasetRef]]],
        outputs: Optional[Mapping[DatasetType, List[DatasetRef]]],
        datastore_records: Optional[Mapping[str, DatastoreRecordData]] = None,
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
        self.mapping: MutableMapping[DimensionRecord, Tuple[int, SerializedDimensionRecord]] = {}

    def addRecord(self, record: DimensionRecord) -> int:
        """Add a dimension record to the accumulator if it has not already been
        added. When a record is inserted for the first time it is assigned
        a unique integer key.

        This function returns the key associated with the record (either the
        newly allocated key, or the existing one)

        Parameters
        ----------
        record : `DimensionRecord`
            The record to add to the accumulator

        Returns
        -------
        accumulatorKey : int
            The key that is associated with the supplied record
        """
        if (mappingValue := self.mapping.get(record)) is None:
            simple = record.to_simple()
            mappingValue = (self._counter, simple)
            self._counter += 1
            self.mapping[record] = mappingValue
        return mappingValue[0]

    def makeSerializedDimensionRecordMapping(self) -> Mapping[int, SerializedDimensionRecord]:
        return {id_: serializeRef for id_, serializeRef in self.mapping.values()}

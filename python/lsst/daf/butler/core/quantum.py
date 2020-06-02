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

__all__ = (
    "ExecutedQuantum",
    "ActiveQuantum",
    "PredictedQuantum",
    "Quantum",
    "QuantumExecutionStatus",
)

from abc import ABC, abstractmethod
import enum
from typing import (
    AbstractSet,
    Any,
    Iterable,
    Mapping,
    MutableSet,
    Optional,
    Set,
    Type,
    TYPE_CHECKING,
)

import astropy.time

from lsst.utils import doImport

from .named import NamedKeyDict, NamedKeyMapping

if TYPE_CHECKING:
    from .dimensions import DataCoordinate
    from .datasets import DatasetRef, DatasetType


class QuantumExecutionStatus(enum.IntEnum):
    """Status flag indicating whether execution was successful, and if not,
    why.
    """

    SUCCEEDED = 0
    """Execution finished successfully.
    """

    INCOMPLETE = 1
    """Execution was started, but is either ongoing or failed for an unknown
    reason in a way that prevented it from being recorded as `FAILED`.
    """

    ABORTED = 2
    """Execution was aborted by the user or some other external signal.
    """

    FAILED_EXCEPTION = 3
    """Execution failed with the task raising an exception.
    """

    FAILED_MISSING_OUTPUTS = 4
    """Execution appeared to succeed, but one or more predicted outputs was
    never written.
    """


class Quantum(ABC):
    """A discrete unit of work that may depend on one or more datasets and
    produces one or more datasets.

    Most Quanta will represent executions of a particular ``PipelineTask``’s
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
    dataId : `DataCoordinate`
        The dimension values that identify this `Quantum`.
    initInputs : `NamedKeyDict` [ `DatasetType`,  `DatasetRef` ]
        Datasets that are needed to construct an instance of the Task.
    predictedInputs : `NamedKeyDict`
        Inputs identified prior to execution, organized as a mapping from
        `DatasetType` to a set of `DatasetRef` instances.
    predictedOutputs : `NamedKeyDict`
        Outputs identified prior to execution, organized as a mapping
        from `DatasetType` to a set of (unresolved) `DatasetRef` instances.
    """
    def __init__(
        self, *,
        taskName: Optional[str] = None,
        taskClass: Optional[Type] = None,
        dataId: DataCoordinate,
        initInputs: NamedKeyDict[DatasetType, DatasetRef],
        predictedInputs: NamedKeyDict[DatasetType, AbstractSet[DatasetRef]],
        predictedOutputs: NamedKeyDict[DatasetType, AbstractSet[DatasetRef]],
        actualInputs: NamedKeyDict[DatasetType, AbstractSet[DatasetRef]],
        actualOutputs: NamedKeyDict[DatasetType, AbstractSet[DatasetRef]],
    ):
        if taskClass is not None:
            taskName = f"{taskClass.__module__}.{taskClass.__name__}"
        self._taskClass = taskClass
        if taskName is None:
            raise ValueError("At least one of 'taskClass' and 'taskName' must be provided.")
        self.taskName = taskName
        self.dataId = dataId
        self.initInputs = initInputs
        self.predictedInputs = predictedInputs
        self.predictedOutputs = predictedOutputs
        self.actualInputs = actualInputs
        self.actualOutputs = actualOutputs

    __slots__ = ("_taskClass", "taskName", "dataId", "initInputs",
                 "predictedInputs", "predictedOutputs",
                 "actualInputs", "actualOutputs")

    @property
    def taskClass(self) -> Type:
        """Task class associated with this `Quantum` (`type`).
        """
        if self._taskClass is None:
            self._taskClass = doImport(self.taskName)
        return self._taskClass

    @abstractmethod
    def reset(self) -> PredictedQuantum:
        raise NotImplementedError()

    @staticmethod
    def predict(
        *,
        taskName: Optional[str] = None,
        taskClass: Optional[Type] = None,
        dataId: DataCoordinate,
        initInputs: Mapping[DatasetType, DatasetRef],
        inputs: Mapping[DatasetType, Iterable[DatasetRef]],
        outputs: Mapping[DatasetType, Iterable[DatasetRef]],
    ) -> PredictedQuantum:
        initInputs = NamedKeyDict(initInputs)
        initInputs.freeze()
        predictedInputs = NamedKeyDict((datasetType, frozenset(refs))
                                       for datasetType, refs in inputs.items())
        predictedInputs.freeze()
        predictedOutputs = NamedKeyDict((datasetType, frozenset(r.unresolved() for r in refs))
                                        for datasetType, refs in outputs.items())
        predictedOutputs.freeze()
        return PredictedQuantum(
            taskName=taskName,
            taskClass=taskClass,
            dataId=dataId,
            initInputs=initInputs,
            predictedInputs=predictedInputs,
            predictedOutputs=predictedOutputs,
            actualInputs=actualInputs,
            actualOutputs=actualOutputs,
        )

    taskName: str
    """Fully-qualified name of the task associated with `Quantum` (`str`).
    """

    dataId: DataCoordinate
    """The dimension values of the unit of processing (`DataCoordinate`).
    """

    initInputs: NamedKeyMapping[DatasetType, DatasetRef]
    """A mapping of datasets used to construct the Task,
    with `DatasetType` instances as keys (names can also be used for
    lookups) and `DatasetRef` instances as values.
    """

    predictedInputs: NamedKeyMapping[DatasetType, AbstractSet[DatasetRef]]
    """A mapping of input datasets that were expected to be used,
    with `DatasetType` instances as keys (names can also be used for
    lookups) and a set of `DatasetRef` instances as values.

    Nested `DatasetRef` instances may be resolved or unresolved.
    """

    predictedOutputs: NamedKeyMapping[DatasetType, AbstractSet[DatasetRef]]
    """A mapping of output datasets expected to be produced by this quantum,
    with the same form as `predictedInputs`.

    Nested `DatasetRef` instances are always unresolved.
    """

    actualInputs: NamedKeyMapping[DatasetType, AbstractSet[DatasetRef]]
    """A mapping of input datasets that were actually used, with the same
    form and keys as `Quantum.predictedInputs`.

    Nested `DatasetRef` instances are always resolved.
    """

    actualOutputs: NamedKeyMapping[DatasetType, AbstractSet[DatasetRef]]
    """A mapping of output datasets that were actually produced, with the same
    form and keys as `Quantum.predictedOutputs`.

    Nested `DatasetRef` instances are always resolved.
    """


class PredictedQuantum(Quantum):
    """A subclass of `Quantum` that should be used for quanta that have not
    been executed (or even started).
    """

    def __init__(
        self, *,
        taskName: Optional[str] = None,
        taskClass: Optional[Type] = None,
        dataId: DataCoordinate,
        initInputs: NamedKeyDict[DatasetType, DatasetRef],
        predictedInputs: NamedKeyDict[DatasetType, AbstractSet[DatasetRef]],
        predictedOutputs: NamedKeyDict[DatasetType, AbstractSet[DatasetRef]],
    ):
        actualInputs = NamedKeyDict((datasetType, frozenset()) for datasetType in predictedInputs)
        actualInputs.freeze()
        actualOutputs = NamedKeyDict((datasetType, frozenset()) for datasetType in predictedOutputs)
        actualOutputs.freeze()
        super().__init__(
            taskName=taskName,
            taskClass=taskClass,
            dataId=dataId,
            initInputs=initInputs,
            predictedInputs=predictedInputs,
            predictedOutputs=predictedOutputs,
            actualInputs=actualInputs,
            actualOutputs=actualOutputs,
        )

    __slots__ = ()

    def reset(self) -> PredictedQuantum:
        return self

    def execute(
        self, *,
        id: int,
        run: str,
        host: Optional[str] = None,
    ) -> ActiveQuantum:
        actualInputs = NamedKeyDict((datasetType, set()) for datasetType in self.predictedInputs)
        actualInputs.freeze()
        actualOutputs = NamedKeyDict((datasetType, set()) for datasetType in self.predictedOutputs)
        actualOutputs.freeze()
        return ActiveQuantum(
            taskName=self.taskName,
            taskClass=self._taskClass,
            dataId=self.dataId,
            initInputs=self.initInputs,
            predictedInputs=self.predictedInputs,
            predictedOutputs=self.predictedOutputs,
            actualInputs=actualInputs,
            actualOutputs=actualOutputs,
            id=id,
            run=run,
            startTime=astropy.time.Time.now(),
            host=host,
        )


class ActiveQuantum(Quantum):

    def __init__(
        self, *,
        id: int,
        run: str,
        startTime: astropy.time.Time,
        host: Optional[str],
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.id = id
        self.run = run
        self.startTime = startTime
        self.host = host

    __slots__ = ("id", "run", "startTime")

    def reset(self) -> PredictedQuantum:
        return PredictedQuantum(
            taskName=self.taskName,
            taskClass=self._taskClass,
            dataId=self.dataId,
            initInputs=self.initInputs,
            predictedInputs=self.predictedInputs,
            predictedOutputs=self.predictedOutputs,
        )

    def finish(
        self, *,
        status: QuantumExecutionStatus,
    ) -> ExecutedQuantum:
        # Make fully-frozen versions of actualInputs and actualOutputs, and
        # validate them in the process.
        actualInputs = NamedKeyDict()
        for datasetType, refs in self.actualInputs.items():
            if not refs <= self.predictedInputs[datasetType]:
                raise RuntimeError(f"Data IDs of actual inputs for {datasetType.name} are not included in "
                                   f"the predicted inputs: "
                                   f"{[ref.dataId for ref in refs - self.predictedInputs[datasetType]]}."
            actualInputs[datasetType] = frozenset(refs)
        actualInputs.freeze()
        outputs = NamedKeyDict()
        for datasetType, refs in self.outputs.items():
            if status is QuantumExecutionStatus.SUCCEEDED and any(ref.id is None for ref in refs):
                status = QuantumExecutionStatus.FAILED_MISSING_OUTPUTS
        outputs.freeze()
        return ExecutedQuantum(
            status=status,
            taskName=self.taskName,
            taskClass=self._taskClass,
            dataId=self.dataId,
            initInputs=self.initInputs,
            predictedInputs=self.predictedInputs,
            actualInputs=actualInputs,
            outputs=outputs,
            id=self.id,
            run=self.run,
            startTime=self.startTime,
            endTime=astropy.time.Time.now(),
            host=self.host,
        )

    actualInputs: NamedKeyMapping[DatasetType, MutableSet[DatasetRef]]
    """A mapping of input datasets that were actually used, with the same
    form and keys as `Quantum.predictedInputs`.

    This is initialized to have empty (mutable) `set` instances as values.
    These should be updated prior to calling `finish`.
    """

    actualOutputs: NamedKeyMapping[DatasetType, MutableSet[DatasetRef]]
    """A mapping of input datasets that were actually used, with the same
    form and keys as `Quantum.predictedOutputs`.

    This is initialized to have empty (mutable) `set` instances as values.
    These should be updated prior to calling `finish`.
    """

    id: int
    """Unique integer for this quantum (`int`).
    """

    run: str
    """The name of the run this Quantum is a part of (`str`).
    """

    startTime: astropy.time.Time
    """Begin timestamp for the execution of this quantum
    (`astropy.time.Time`).
    """

    host: Optional[str]
    """Name of the system on which this quantum was executed (`str`).
    """


class ExecutedQuantum(Quantum):
    """A subclass of `Quantum` that should be used to represent quanta for
    which execution was at least started.

    Parameters
    ----------
    status : `QuantumExecutionStatus`
        Status flag indicating whether execution was successful, and iff not,
        why.
    actualInputs : `NamedKeyDict`
        Inputs actually used during execution, organized as a mapping from
        `DatasetType` to a list of `DatasetRef`.  Must be a subset of
        ``predictedInputs``.
    run : `str`
        The name of the run this Quantum is a part of.
    id : `int`
        Unique integer identifier for this quantum.  Usually set to `None`
        (default) and assigned by `Registry`.
    startTime : `astropy.time.Time`
        The start time for the quantum.
    endTime : `astropy.time.Time`
        The end time for the quantum.
    host : `str`, optional
        The system on this quantum was executed.  May be `None`.
    **kwargs
        All `Quantum` constructor keyword arguments are required, and are
        forwarded directly.
    """
    def __init__(
        self, *,
        status: QuantumExecutionStatus,
        id: int,
        run: str,
        startTime: astropy.time.Time,
        endTime: astropy.time.Time,
        host: Optional[str] = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.status = status
        self.actualInputs = actualInputs
        self.id = id
        self.run = run
        self.startTime = startTime
        self.endTime = endTime
        self.host = host

    __slots__ = ("status", "actualInputs", "id", "run", "startTime", "endTime", "host")

    def reset(self) -> PredictedQuantum:
        # Replace outputs with a version that has unresolved DatasetRefs.
        outputs = NamedKeyDict((datasetType, frozenset(ref.unresolved() for ref in refs))
                               for datasetType, refs in self.outputs.items())
        outputs.freeze()
        return PredictedQuantum(
            taskName=self.taskName,
            taskClass=self._taskClass,
            dataId=self.dataId,
            initInputs=self.initInputs,
            predictedInputs=self.predictedInputs,
            outputs=outputs,
        )

    status: QuantumExecutionStatus
    """Status flag indicating whether execution was successful, and if not,
    why (`QuantumExecutionStatus`).
    """

    id: int
    """Unique integer for this quantum (`int`).
    """

    run: str
    """The name of the run this Quantum is a part of (`str`).
    """

    startTime: astropy.time.Time
    """Begin timestamp for the execution of this quantum
    (`astropy.time.Time`).
    """

    endTime: astropy.time.Time
    """End timestamp for the execution of this quantum
    (`astropy.time.Time`).
    """

    host: Optional[str]
    """Name of the system on which this quantum was executed (`str`).
    """

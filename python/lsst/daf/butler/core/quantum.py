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

import enum
from typing import (
    Any,
    Iterable,
    Optional,
    Type,
    TYPE_CHECKING,
)

import astropy.time

from lsst.utils import doImport

from .datasets import DatasetContainer, DatasetRef, MutableDatasetContainer

if TYPE_CHECKING:
    from .dimensions import DataCoordinate


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


class Quantum:
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
    initInputs : `DatasetContainer`
        Datasets that are needed to construct an instance of the Task.
    predictedInputs : `DatasetContainer`
        Inputs identified prior to execution.
    predictedOutputs : `DatasetContainer`
        Outputs identified prior to execution.
    actualInputs : `DatasetContainer`
        Inputs actually used by the task during execution.
    actualOutputs : `DatasetContainer`
        Outputs actually produced by the task during execution.
    """
    def __init__(
        self, *,
        taskName: Optional[str] = None,
        taskClass: Optional[Type] = None,
        dataId: DataCoordinate,
        initInputs: DatasetContainer,
        predictedInputs: DatasetContainer,
        predictedOutputs: DatasetContainer,
        actualInputs: DatasetContainer,
        actualOutputs: DatasetContainer,
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

    def reset(self) -> PredictedQuantum:
        return PredictedQuantum(
            taskName=self.taskName,
            taskClass=self._taskClass,
            dataId=self.dataId,
            initInputs=self.initInputs,
            predictedInputs=self.predictedInputs,
            predictedOutputs=self.predictedOutputs,
        )

    @staticmethod
    def predict(
        *,
        taskName: Optional[str] = None,
        taskClass: Optional[Type] = None,
        dataId: DataCoordinate,
        initInputs: Iterable[DatasetRef],
        inputs: Iterable[DatasetRef],
        outputs: Iterable[DatasetRef],
    ) -> PredictedQuantum:
        result = PredictedQuantum(
            taskName=taskName,
            taskClass=taskClass,
            dataId=dataId,
            initInputs=DatasetContainer(initInputs),
            predictedInputs=DatasetContainer(inputs),
            predictedOutputs=DatasetContainer(outputs),
        )
        result.predictedInputs.require(unique=True)
        result.predictedOutputs.require(resolved=False, unique=True)
        return result

    taskName: str
    """Fully-qualified name of the task associated with `Quantum` (`str`).
    """

    dataId: DataCoordinate
    """The dimension values of the unit of processing (`DataCoordinate`).
    """

    initInputs: DatasetContainer
    """Datasets that are needed to construct an instance of the Task.
    """

    predictedInputs: DatasetContainer
    """Inputs identified prior to execution.

    Nested `DatasetRef` instances may be resolved or unresolved.
    """

    predictedOutputs: DatasetContainer
    """Outputs identified prior to execution.

    Nested `DatasetRef` instances are always unresolved.
    """

    actualInputs: DatasetContainer
    """Inputs actually used by the task during execution.

    Nested `DatasetRef` instances are always resolved.
    """

    actualOutputs: DatasetContainer
    """Outputs actually produced by the task during execution.

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
        initInputs: DatasetContainer,
        predictedInputs: DatasetContainer,
        predictedOutputs: DatasetContainer,
    ):
        super().__init__(
            taskName=taskName,
            taskClass=taskClass,
            dataId=dataId,
            initInputs=initInputs,
            predictedInputs=predictedInputs,
            predictedOutputs=predictedOutputs,
            actualInputs=DatasetContainer(),
            actualOutputs=DatasetContainer(),
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
        return ActiveQuantum(
            taskName=self.taskName,
            taskClass=self._taskClass,
            dataId=self.dataId,
            initInputs=self.initInputs,
            predictedInputs=self.predictedInputs,
            predictedOutputs=self.predictedOutputs,
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
        super().__init__(
            actualInputs=MutableDatasetContainer(),
            actualOutputs=MutableDatasetContainer(),
            **kwargs
        )
        self.id = id
        self.run = run
        self.startTime = startTime
        self.host = host

    __slots__ = ("id", "run", "startTime")

    def finish(
        self, *,
        status: QuantumExecutionStatus,
    ) -> ExecutedQuantum:
        self.actualInputs.require(resolved=True, unique=True)
        self.actualOutputs.require(resolved=True, unique=True)
        if not (self.actualInputs.unresolved <= self.predictedInputs.unresolved):
            raise ValueError(
                f"actualInputs contains datasets not present in predictedInputs: "
                f"{self.actualInputs.unresolved - self.predictedInputs.unresolved}."
            )
        if status is QuantumExecutionStatus.SUCCEEDED:
            if self.actualOutputs.unresolved != self.predictedOutputs.unresolved:
                raise ValueError(
                    f"Some predicted output datasets were not actually produced: "
                    f"{self.predictedOutputs.unresolved - self.actualOutputs.unresolved}."
                )
        else:
            if not (self.actualOutputs.unresolved <= self.predictedOutputs.unresolved):
                raise ValueError(
                    f"actualOutputs contains datasets not present in predictedOutputs: "
                    f"{self.actualOutputs.unresolved - self.predictedOutputs.unresolved}."
                )
        if (self.actualOutputs.unresolved != self.predictedOutputs.unresolved
                and status is QuantumExecutionStatus.SUCCEEDED):
            status = QuantumExecutionStatus.FAILED_MISSING_OUTPUTS
        return ExecutedQuantum(
            status=status,
            taskName=self.taskName,
            taskClass=self._taskClass,
            dataId=self.dataId,
            initInputs=self.initInputs,
            predictedInputs=self.predictedInputs,
            predictedOutputs=self.predictedOutputs,
            actualInputs=self.actualInputs.intoFrozen(),
            actualOutputs=self.actualOutputs.intoFrozen(),
            id=self.id,
            run=self.run,
            startTime=self.startTime,
            endTime=astropy.time.Time.now(),
            host=self.host,
        )

    actualInputs: MutableDatasetContainer
    """Inputs actually used by the task during execution.
    """

    actualOutputs: MutableDatasetContainer
    """Outputs actually produced by the task during execution.
    """

    id: int
    """Unique integer for this quantum (`int`).
    """

    run: str
    """The name of the run this quantum is a part of (`str`).
    """

    startTime: astropy.time.Time
    """Begin timestamp for the execution of this quantum
    (`astropy.time.Time`).
    """

    host: Optional[str]
    """Name of the system on which this quantum is being executed (`str`).
    """


class ExecutedQuantum(Quantum):
    """A subclass of `Quantum` that should be used to represent quanta for
    which execution was at least started.

    Parameters
    ----------
    status : `QuantumExecutionStatus`
        Status flag indicating whether execution was successful, and if not,
        why.
    run : `str`
        The name of the run this quantum is a part of.
    id : `int`
        Unique integer identifier for this quantum.  Usually set to `None`
        (default) and assigned by `Registry`.
    startTime : `astropy.time.Time`
        Begin timestamp for the execution of this quantum.
    endTime : `astropy.time.Time`
        End timestamp for the execution of this quantum.
    host : `str`, optional
        The name of system on which this quantum was executed.  May be `None`.
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
        self.id = id
        self.run = run
        self.startTime = startTime
        self.endTime = endTime
        self.host = host

    __slots__ = ("status", "id", "run", "startTime", "endTime", "host")

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

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
    "PredictedQuantum",
    "Quantum",
    "QuantumExecutionStatus",
)

import enum
from typing import (
    AbstractSet,
    Any,
    List,
    Mapping,
    Optional,
    Type,
    TYPE_CHECKING,
)

import astropy.time

from lsst.utils import doImport

from .named import NamedKeyDict, NamedKeyMapping

if TYPE_CHECKING:
    from .dimensions import DataCoordinate
    from .datasets import DatasetRef, DatasetType


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
    initInputs : `Mapping` [ `DatasetType`,  `DatasetRef` ]
        Datasets that are needed to construct an instance of the Task.
    predictedInputs : `Mapping` [ `DatasetType`, `Iterable` [ `DatasetRef` ]
        Inputs identified prior to execution, organized as a mapping from
        `DatasetType` to a list of `DatasetRef`.  Must be a superset of
        ``actualInputs``.
    outputs : `Mapping` [ `DatasetType`, `Iterable` [ `DatasetRef` ], optional
        Outputs from executing this quantum of work, organized as a mapping
        from `DatasetType` to a list of `DatasetRef`.
    """
    def __init__(
        self, *,
        taskName: Optional[str] = None,
        taskClass: Optional[Type] = None,
        dataId: DataCoordinate,
        initInputs: Mapping[DatasetType, DatasetRef],
        predictedInputs: Mapping[DatasetType, List[DatasetRef]],
        outputs: Mapping[DatasetType, List[DatasetRef]],
    ):
        if taskClass is not None:
            taskName = f"{taskClass.__module__}.{taskClass.__name__}"
        self._taskClass = taskClass
        if taskName is None:
            raise ValueError("At least one of 'taskClass' and 'taskName' must be provided.")
        self.taskName = taskName
        self.dataId = dataId
        self.initInputs = NamedKeyDict(initInputs)
        self.initInputs.freeze()
        self.predictedInputs = NamedKeyDict(predictedInputs)
        self.predictedInputs.freeze()
        self.outputs = NamedKeyDict(outputs)
        self.outputs.freeze()

    __slots__ = ("_taskClass", "taskName", "dataId",
                 "initInputs", "predictedInputs", "outputs")

    @property
    def taskClass(self) -> Type:
        """Task class associated with this `Quantum` (`type`).
        """
        if self._taskClass is None:
            self._taskClass = doImport(self.taskName)
        return self._taskClass

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
    lookups) and a list of `DatasetRef` instances as values.
    """

    outputs: NamedKeyMapping[DatasetType, AbstractSet[DatasetRef]]
    """A mapping of output datasets (to be) generated for this quantum,
    with the same form as `predictedInputs`.
    """


class PredictedQuantum(Quantum):
    """A subclass of `Quantum` that should be used for quanta that have not
    been executed (or even started).
    """
    pass


class QuantumExecutionStatus(enum.IntEnum):
    """Status flag indicating what whether execution was successful, and if
    not, why.
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


class ExecutedQuantum(Quantum):
    """A subclass of `Quantum` that should be used to represent quanta for
    which execution was at least started.

    Parameters
    ----------
    status : `QuantumExecutionStatus`
        Status flag indicating what whether execution was successful, and if
        not, why.
    actualInputs : `Mapping` [ `DatasetType`, `Iterable` [ `DatasetRef` ]
        Inputs actually used during execution, organized as a mapping from
        `DatasetType` to a list of `DatasetRef`.  Must be a subset of
        ``predictedInputs``.
    run : `str`
        The name of the run this Quantum is a part of.
    id : `int`
        Unique integer identifier for this quantum.  Usually set to `None`
        (default) and assigned by `Registry`.
    startTime : `astropy.time.Time`, optional
        The start time for the quantum.  May be `None`.
    endTime : `astropy.time.Time`. optional
        The end time for the quantum.  May be `None`.
    host : `str`, optional
        The system on this quantum was executed.  May be `None`.
    **kwargs
        All `Quantum` constructor keyword arguments are required, and are
        forwarded directly.
    """
    def __init__(
        self, *,
        status: QuantumExecutionStatus,
        actualInputs: Mapping[DatasetType, List[DatasetRef]],
        id: int,
        run: str,
        startTime: Optional[astropy.time.Time] = None,
        endTime: Optional[astropy.time.Time] = None,
        host: Optional[str] = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.status = status
        self.actualInputs = NamedKeyDict(actualInputs)
        self.actualInputs.freeze()
        self.id = id
        self.run = run
        self.startTime = startTime
        self.endTime = endTime
        self.host = host

    __slots__ = ("status", "actualInputs", "id", "run""startTime", "endTime", "host")

    status: QuantumExecutionStatus
    """Status flag indicating what whether execution was successful, and if
    not, why (`QuantumExecutionStatus`).
    """

    actualInputs: NamedKeyMapping[DatasetType, AbstractSet[DatasetRef]]
    """A mapping of input datasets that were actually used, with the same
    form as `Quantum.predictedInputs`.
    """

    id: int
    """Unique integer for this quantum (`int`).
    """

    run: str
    """The name of the run this Quantum is a part of (`str`).
    """

    startTime: Optional[astropy.time.Time]
    """Begin timestamp for the execution of this quantum
    (`astropy.time.Time`).
    """

    endTime: Optional[astropy.time.Time]
    """End timestamp for the execution of this quantum
    (`astropy.time.Time`).
    """

    host: Optional[str]
    """Name of the system on which this quantum was executed (`str`).
    """

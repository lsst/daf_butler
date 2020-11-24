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

__all__ = ("Quantum",)

from typing import (
    Any,
    Iterable,
    List,
    Mapping,
    Optional,
    Tuple,
    Type,
    Union,
)

from lsst.utils import doImport

from .datasets import DatasetRef, DatasetType
from .dimensions import DataCoordinate
from .named import NamedKeyDict, NamedKeyMapping


class Quantum:
    """A discrete unit of work that may depend on one or more datasets and
    produces one or more datasets.

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
    """

    __slots__ = ("_taskName", "_taskClass", "_dataId", "_initInputs", "_inputs", "_outputs", "_hash")

    def __init__(self, *, taskName: Optional[str] = None,
                 taskClass: Optional[Type] = None,
                 dataId: Optional[DataCoordinate] = None,
                 initInputs: Optional[Union[Mapping[DatasetType, DatasetRef], Iterable[DatasetRef]]] = None,
                 inputs: Optional[Mapping[DatasetType, List[DatasetRef]]] = None,
                 outputs: Optional[Mapping[DatasetType, List[DatasetRef]]] = None,
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

    @property
    def taskClass(self) -> Optional[Type]:
        """Task class associated with this `Quantum` (`type`).
        """
        if self._taskClass is None:
            self._taskClass = doImport(self._taskName)
        return self._taskClass

    @property
    def taskName(self) -> Optional[str]:
        """Fully-qualified name of the task associated with `Quantum` (`str`).
        """
        return self._taskName

    @property
    def dataId(self) -> Optional[DataCoordinate]:
        """The dimension values of the unit of processing (`DataId`).
        """
        return self._dataId

    @property
    def initInputs(self) -> NamedKeyMapping[DatasetType, DatasetRef]:
        """A mapping of datasets used to construct the Task,
        with `DatasetType` instances as keys (names can also be used for
        lookups) and `DatasetRef` instances as values.
        """
        return self._initInputs

    @property
    def inputs(self) -> NamedKeyMapping[DatasetType, List[DatasetRef]]:
        """A mapping of input datasets that were expected to be used,
        with `DatasetType` instances as keys (names can also be used for
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
        """A mapping of output datasets (to be) generated for this quantum,
        with the same form as `predictedInputs`.

        Notes
        -----
        We cannot use `set` instead of `list` for the nested container because
        `DatasetRef` instances cannot be compared reliably when some have
        integers IDs and others do not.
        """
        return self._outputs

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
        return (self._reduceFactory,
                (self.taskName, self.taskClass, self.dataId, dict(self.initInputs.items()),
                 dict(self.inputs), dict(self.outputs)))

    @staticmethod
    def _reduceFactory(taskName: Optional[str],
                       taskClass: Optional[Type],
                       dataId: Optional[DataCoordinate],
                       initInputs: Optional[Union[Mapping[DatasetType, DatasetRef], Iterable[DatasetRef]]],
                       inputs: Optional[Mapping[DatasetType, List[DatasetRef]]],
                       outputs: Optional[Mapping[DatasetType, List[DatasetRef]]]
                       ) -> Quantum:
        return Quantum(taskName=taskName, taskClass=taskClass, dataId=dataId, initInputs=initInputs,
                       inputs=inputs, outputs=outputs)

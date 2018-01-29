#
# LSST Data Management System
#
# Copyright 2008-2018  AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
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
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <https://www.lsstcorp.org/LegalNotices/>.
#


class Quantum(object):
    """A discrete unit of work that may depend on one or more Datasets and produces one or more `Dataset`s.

    Most Quanta will be executions of a particular `SuperTask`’s `runQuantum` method,
    but they can also be used to represent discrete units of work performed manually
    by human operators or other software agents.
    """

    __slots__ = ("_quantumId", "_registryId", "_run", "_task", "_predictedInputs", "_actualInputs")

    _currentId = -1

    @classmethod
    def getNewId(cls):
        """Generate a new Quantum ID number.

        ..todo::
            This is a temporary workaround that will probably disapear in the future,
            when a solution is found to the problem of autoincrement compound primary keys in SQLite.
        """
        cls._currentId += 1
        return cls._currentId

    def __init__(self, run, task=None, quantumId=None, registryId=None):
        """Constructor.

        Parameters correspond directly to attributes.
        """
        self._quantumId = quantumId
        self._registryId = registryId
        self._run = run
        self._task = task
        self._predictedInputs = {}
        self._actualInputs = {}

    @property
    def pkey(self):
        """The `(quantum_id, registry_id)` `tuple` used to uniquely identify this `Run`,
        or `None` if it has not yet been inserted into a `Registry`.
        """
        if self._quantumId is not None and self._registryId is not None:
            return (self._quantumId, self._registryId)
        else:
            return None

    @property
    def quantumId(self):
        return self._quantumId

    @property
    def registryId(self):
        return self._registryId

    @property
    def run(self):
        """The `Run` this `Quantum` is a part of.
        """
        return self._run

    @property
    def task(self):
        """If the `Quantum` is associated with a `SuperTask`, this is the
        `SuperTask` instance that produced and should execute this set of
        inputs and outputs. If not, a human-readable string identifier
        for the operation. Some Registries may permit the value to be
        `None`, but are not required to in general.
        """
        return self._task

    @property
    def predictedInputs(self):
        """A dictionary of input datasets that were expected to be used,
        with `DatasetType` names as keys and a set of `DatasetRef` instances
        as values.

        Input `Datasets` that have already been stored may be
        `DatasetRef`s, and in many contexts may be guaranteed to be.
        Read-only; update via `addPredictedInput()`.
        """
        return self._predictedInputs

    @property
    def actualInputs(self):
        """A `dict` of input datasets that were actually used, with the same
        form as `predictedInputs`.

        All returned sets must be subsets of those in `predictedInputs`.

        Read-only; update via `Registry.markInputUsed()`.
        """
        return self._actualInputs

    def addPredictedInput(self, ref):
        """Add an input `DatasetRef` to the `Quantum`.

        This does not automatically update a `Registry`; all `predictedInputs`
        must be present before a `Registry.addQuantum()` is called.
        """
        datasetTypeName = ref.type.name
        if datasetTypeName not in self._actualInputs:
            self._predictedInputs[datasetTypeName] = [ref, ]
        else:
            self._predictedInputs[datasetTypeName].append(ref)

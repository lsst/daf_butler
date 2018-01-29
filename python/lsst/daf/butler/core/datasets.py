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

from types import MappingProxyType
from .utils import slotValuesAreEqual, slotValuesToHash
from .units import DataUnitSet


def _safeMakeMappingProxyType(data):
    if data is None:
        data = {}
    return MappingProxyType(data)


class DatasetType(object):
    """A named category of Datasets that defines how they are organized,
    related, and stored.

    A concrete, final class whose instances represent `DatasetType`s.
    `DatasetType` instances may be constructed without a `Registry`,
    but they must be registered
    via `Registry.registerDatasetType()` before corresponding `Datasets`
    may be added.
    `DatasetType` instances are immutable.
    """

    __slots__ = ("_name", "_template", "_units", "_storageClass")
    __eq__ = slotValuesAreEqual
    __hash__ = slotValuesToHash

    @property
    def name(self):
        """A string name for the `Dataset`; must correspond to the same
        `DatasetType` across all Registries.
        """
        return self._name

    @property
    def template(self):
        """A string with `str.format`-style replacement patterns that can be
        used to create a path from a `Run`
        (and optionally its associated Collection) and a `DatasetRef`.

        May be `None` to indicate a read-only `Dataset` or one whose templates
        must be provided at a higher level.
        """
        return self._template

    @property
    def units(self):
        """A `DataUnitSet` that defines the `DatasetRef`s corresponding
        to this `DatasetType`.
        """
        return self._units

    @property
    def storageClass(self):
        """A `StorageClass` that defines how this `DatasetType` is persisted.
        """
        return self._storageClass

    def __init__(self, name, template, units, storageClass):
        """Constructor.

        All arguments correspond directly to instance attributes.
        """
        self._name = name
        self._template = template
        self._units = DataUnitSet(units)
        self._storageClass = storageClass


class DatasetRef(object):
    """Reference to a `Dataset` in a `Registry`.

    A `DatasetRef` may point to a `Dataset`s that currently does not yet exist
    (e.g. because it is a predicted input for provenance).
    """

    __slots__ = ("_type", "_producer", "_predictedConsumers", "_actualConsumers")
    _currentId = -1

    @classmethod
    def getNewId(cls):
        """Generate a new Dataset ID number.

        ..todo::
            This is a temporary workaround that will probably disapear in
            the future, when a solution is found to the problem of
            autoincrement compound primary keys in SQLite.
        """
        cls._currentId += 1
        return cls._currentId

    def __init__(self, datasetType, units):
        """Construct a DatasetRef from a DatasetType and a complete tuple
        of DataUnits.

        Parameters
        ----------
        datasetType: `DatasetType`
            The `DatasetType` for this `Dataset`.
        units: `dict`
            Dictionary where the keys are `DataUnit` names and the values are
            `DataUnit` instances.
        """
        units = datasetType.units.conform(units)
        super().__init__(
            datasetType.name,
            **{unit.__class__.__name__: unit.value for unit in units}
        )
        self._datasetType = datasetType
        self._units = units
        self._producer = None
        self._predictedConsumers = dict()
        self._actualConsumers = dict()

    @property
    def datasetType(self):
        """The `DatasetType` associated with the `Dataset` the `DatasetRef`
        points to.
        """
        return self._type

    @property
    def units(self):
        """A `tuple` of `DataUnit` instances that label the `DatasetRef`
        within a Collection.
        """
        return self._units

    @property
    def producer(self):
        """The `Quantum` instance that produced (or will produce) the
        `Dataset`.

        Read-only; update via `Registry.addDataset()`,
        `QuantumGraph.addDataset()`, or `Butler.put()`.
        May be `None` if no provenance information is available.
        """
        return self._producer

    @property
    def predictedConsumers(self):
        """A sequence of `Quantum` instances that list this `Dataset` in their `predictedInputs` attributes.

        Read-only; update via `Quantum.addPredictedInput()`.
        May be an empty list if no provenance information is available.
        """
        return _safeMakeMappingProxyType(self._predictedConsumers)

    @property
    def actualConsumers(self):
        """A sequence of `Quantum` instances that list this `Dataset` in their
        `actualInputs` attributes.

        Read-only; update via `Registry.markInputUsed()`.
        May be an empty list if no provenance information is available.
        """
        return _safeMakeMappingProxyType(self._actualConsumers)

    def makeStorageHint(self, run, template=None):
        """Construct a storage hint by filling in template with the Collection
        collection and the values in the units tuple.

        Although a `Dataset` may belong to multiple Collections, only the one
        corresponding to its `Run` is used.
        """
        if template is None:
            template = self.datasetType.template
        units = {unit.__class__.__name__: unit.value for unit in self.units}
        return template.format(DatasetType=self.datasetType.name, Run=run.collection, **units)

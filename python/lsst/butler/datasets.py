#
# LSST Data Management System
#
# Copyright 2008-2017  AURA/LSST.
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
from .storageClass import StorageClass
from .units import DataUnitTypeSet


class DatasetType:
    """A named category of Datasets that defines how they are organized, related, and stored.

    A concrete, final class whose instances represent `DatasetType`s.
    `DatasetType` instances may be constructed without a `Registry`, but they must be registered
    via `Registry.registerDatasetType()` before corresponding `Datasets` may be added.
    `DatasetType` instances are immutable.
    """

    __slots__ = ("_name", "_template", "_units", "_storageClass")
    __eq__ = slotValuesAreEqual
    __hash__ = slotValuesToHash

    @property
    def name(self):
        """A string name for the `Dataset`; must correspond to the same `DatasetType` across all Registries.
        """
        return self._name

    @property
    def template(self):
        """A string with `str.format`-style replacement patterns that can be used to create a path from a `Run`
        (and optionally its associated Collection) and a `DatasetRef`.

        May be `None` to indicate a read-only `Dataset` or one whose templates must be provided at a higher level.
        """
        return self._template

    @property
    def units(self):
        """A `DataUnitTypeSet` that defines the `DatasetRef`s corresponding to this `DatasetType`.
        """
        return self._units

    @property
    def storageClass(self):
        """A `StorageClass` subclass (not instance) that defines how this `DatasetType` is persisted.
        """
        return self._storageClass

    def __init__(self, name, template, units, storageClass):
        """Constructor.

        All arguments correspond directly to instance attributes.
        """
        assert issubclass(storageClass, StorageClass)
        self._name = name
        self._template = template
        self._units = DataUnitTypeSet(units)
        self._storageClass = storageClass


class DatasetLabel:
    """Opaque label that identifies a `Dataset` in a `Collection`.
    """

    __slots__ = ("_name", "_units")
    __eq__ = slotValuesAreEqual

    def __init__(self, name, **units):
        self._name = name
        self._units = units

    @property
    def name(self):
        """Name of the `DatasetType` associated with the `Dataset`.
        """
        return self._name

    @property
    def units(self):
        """Dictionary with name, value pairs for `DataUnit`s.
        """
        return self._units


class DatasetRef(DatasetLabel):
    """Reference to a `Dataset` in a `Registry`.

    As opposed to a `DatasetLabel`, `DatasetRef` holds actual `DataUnit` instances
    (instead of just their names and primary-key values).
    They can typically only be constructed by calling `Registry.expand`.
    In contrast to `DatasetLabel`s a `DatasetRef` may point to a `Dataset`s that currently do not yet exist
    (e.g. because it is a predicted input for provenance).
    """

    __slots__ = ("_type", "_producer", "_predictedConsumers", "_actualConsumers")
    _currentId = -1

    @classmethod
    def getNewId(cls):
        """Generate a new Dataset ID number.

        ..todo::
            This is a temporary workaround that will probably disapear in the future,
            when a solution is found to the problem of autoincrement compound primary keys in SQLite.
        """
        cls._currentId += 1
        return cls._currentId

    def __init__(self, type, units):
        """Construct a DatasetRef from a DatasetType and a complete tuple of DataUnits.

        Parameters
        ----------
        type: `DatasetType`
            The `DatasetType` for this `Dataset`.
        units: `dict`
            Dictionary where the keys are `DataUnit` names and the values are `DataUnit` instances.
        """
        units = type.units.conform(units)
        super().__init__(
            type.name,
            **{unit.__class__.__name__: unit.value for unit in units}
        )
        self._type = type
        self._units = units
        self._producer = None
        self._predictedConsumers = dict()
        self._actualConsumers = dict()

    @property
    def type(self):
        """The `DatasetType` associated with the `Dataset` the `DatasetRef` points to.
        """
        return self._type

    @property
    def units(self):
        """A `tuple` of `DataUnit` instances that label the `DatasetRef` within a Collection.
        """
        return self._units

    @property
    def producer(self):
        """The `Quantum` instance that produced (or will produce) the `Dataset`.

        Read-only; update via `Registry.addDataset()`, `QuantumGraph.addDataset()`, or `Butler.put()`.
        May be `None` if no provenance information is available.
        """
        return self._producer

    @property
    def predictedConsumers(self):
        """A sequence of `Quantum` instances that list this `Dataset` in their `predictedInputs` attributes.

        Read-only; update via `Quantum.addPredictedInput()`.
        May be an empty list if no provenance information is available.
        """
        return MappingProxyType(self._predictedConsumers)

    @property
    def actualConsumers(self):
        """A sequence of `Quantum` instances that list this `Dataset` in their `actualInputs` attributes.

        Read-only; update via `Registry.markInputUsed()`.
        May be an empty list if no provenance information is available.
        """
        return MappingProxyType(self._actualConsumers)

    def makePath(self, run, template=None):
        """Construct the path part of a URI by filling in template with the Collection tag and the values in the units tuple.

        This is often just a storage hint since the `Datastore` will likely have to deviate from the provided path (in the case of an object-store for instance).
        Although a `Dataset` may belong to multiple Collections, only the first Collection it is added to is used in its path.
        """
        raise NotImplementedError("TODO")


class DatasetHandle(DatasetRef):
    """Handle to a stored `Dataset` in a `Registry`.

    As opposed to a `DatasetLabel`, and like a `DatasetRef`, `DatasetHandle` holds actual `DataUnit` instances
    (instead of just their names and primary-key values).
    In contrast to `DatasetRef`s a `DatasetHandle` only ever points to a `Dataset` that has been stored in a `Datastore`.
    """

    __slots__ = ("_datasetId", "_registryId", "_uri", "_components", "_run")

    def __init__(self, datasetId, registryId, ref, uri, components, run):
        """Constructor.

        Parameters correspond directly to attributes.
        """
        super().__init__(ref.type, ref.units)
        self._datasetId = datasetId
        self._registryId = registryId
        self._producer = ref.producer
        self._predictedConsumers.update(ref.predictedConsumers)
        self._actualConsumers.update(ref.actualConsumers)
        self._uri = uri
        self._components = MappingProxyType(components) if components else None
        self._run = run

    @property
    def datasetId(self):
        """Primary-key identifier for this `Dataset`.
        """
        return self._datasetId

    @property
    def registryId(self):
        """Id of the `Registry` that was used to create this `Dataset`.
        """
        return self._registryId

    @property
    def uri(self):
        """The URI that holds the location of the `Dataset` in a `Datastore`.
        """
        return self._uri

    @property
    def components(self):
        """A `dict` holding `DatasetHandle` instances that correspond to this `Dataset`s named components.

        Empty if the `Dataset` is not a composite.
        """
        return self._components

    @property
    def run(self):
        """The `Run` the `Dataset` was created with.
        """
        return self._run

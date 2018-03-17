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

from types import MappingProxyType
from .utils import slotValuesAreEqual, slotValuesToHash

__all__ = ("DatasetType", "DatasetRef")


def _safeMakeMappingProxyType(data):
    if data is None:
        data = {}
    return MappingProxyType(data)


class DatasetType(object):
    """A named category of Datasets that defines how they are organized,
    related, and stored.

    A concrete, final class whose instances represent `DatasetType`\ s.
    `DatasetType` instances may be constructed without a `Registry`,
    but they must be registered
    via `Registry.registerDatasetType()` before corresponding `Dataset`\ s
    may be added.
    `DatasetType` instances are immutable.

    Parameters
    ----------
    name : `str`
        A string name for the `Dataset`; must correspond to the same
        `DatasetType` across all Registries.
    dataUnits : `iterable` of `str`
        `DataUnit` names that defines the `DatasetRef`\ s corresponding to
        this `DatasetType`.  The input iterable is copied into a `frozenset`.
    storageClass : `str`
        Name of a `StorageClass` that defines how this `DatasetType`
        is persisted.
    """

    __slots__ = ("_name", "_dataUnits", "_storageClass")
    __eq__ = slotValuesAreEqual
    __hash__ = slotValuesToHash

    @property
    def name(self):
        """A string name for the `Dataset`; must correspond to the same
        `DatasetType` across all Registries.
        """
        return self._name

    @property
    def dataUnits(self):
        """A `frozenset` of `DataUnit` names that defines the `DatasetRef`\ s
        corresponding to this `DatasetType`.
        """
        return self._dataUnits

    @property
    def storageClass(self):
        """Name of a `StorageClass` that defines how this `DatasetType`
        is persisted.
        """
        return self._storageClass

    def __init__(self, name, dataUnits, storageClass):
        self._name = name
        self._dataUnits = frozenset(dataUnits)
        self._storageClass = storageClass


class DatasetRef(object):
    """Reference to a `Dataset` in a `Registry`.

    A `DatasetRef` may point to a `Dataset` that currently does not yet exist
    (e.g., because it is a predicted input for provenance).

    Parameters
    ----------
    datasetType : `DatasetType`
        The `DatasetType` for this `Dataset`.
    dataId : `dict`
        Dictionary where the keys are `DataUnit` names and the values are
        `DataUnit` values.
    id : `int`, optional
        A unique identifier.
        Normally set to `None` and assigned by `Registry`
    """

    __slots__ = ("_id", "_datasetType", "_dataId", "_producer", "_predictedConsumers", "_actualConsumers")

    def __init__(self, datasetType, dataId, id=None):
        assert isinstance(datasetType, DatasetType)
        self._id = id
        self._datasetType = datasetType
        self._dataId = dataId
        self._producer = None
        self._predictedConsumers = dict()
        self._actualConsumers = dict()

    @property
    def id(self):
        """Primary key of the dataset (`int`)

        Typically assigned by `Registry`.
        """
        return self._id

    @property
    def datasetType(self):
        """The `DatasetType` associated with the `Dataset` the `DatasetRef`
        points to.
        """
        return self._datasetType

    @property
    def dataId(self):
        """A `dict` of `DataUnit` name, value pairs that label the `DatasetRef`
        within a Collection.
        """
        return self._dataId

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
        """A sequence of `Quantum` instances that list this `Dataset` in their
        `predictedInputs` attributes.

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

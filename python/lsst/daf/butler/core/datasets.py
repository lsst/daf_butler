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

__all__ = ("DatasetType", "DatasetRef")

from copy import deepcopy
import hashlib

from types import MappingProxyType
from .utils import slotValuesAreEqual
from .storageClass import StorageClass, StorageClassFactory
from .dimensions import DimensionGraph, DimensionNameSet, DataId
from .configSupport import LookupKey


def _safeMakeMappingProxyType(data):
    if data is None:
        data = {}
    return MappingProxyType(data)


class DatasetType:
    r"""A named category of Datasets that defines how they are organized,
    related, and stored.

    A concrete, final class whose instances represent `DatasetType`\ s.
    `DatasetType` instances may be constructed without a `Registry`,
    but they must be registered
    via `Registry.registerDatasetType()` before corresponding Datasets
    may be added.
    `DatasetType` instances are immutable.

    Parameters
    ----------
    name : `str`
        A string name for the Dataset; must correspond to the same
        `DatasetType` across all Registries.
    dimensions : `DimensionGraph` or iterable of `str`
        Dimensions used to label and relate instances of this DatasetType,
        or string names thereof.
    storageClass : `StorageClass` or `str`
        Instance of a `StorageClass` or name of `StorageClass` that defines
        how this `DatasetType` is persisted.
    """

    __slots__ = ("_name", "_dimensions", "_storageClass", "_storageClassName")

    @staticmethod
    def nameWithComponent(datasetTypeName, componentName):
        """Form a valid DatasetTypeName from a parent and component.

        No validation is performed.

        Parameters
        ----------
        datasetTypeName : `str`
            Base type name.
        componentName : `str`
            Name of component.

        Returns
        -------
        compTypeName : `str`
            Name to use for component DatasetType.
        """
        return "{}.{}".format(datasetTypeName, componentName)

    def __init__(self, name, dimensions, storageClass):
        self._name = name
        if isinstance(dimensions, (DimensionGraph, DimensionNameSet)):
            self._dimensions = dimensions
        else:
            self._dimensions = DimensionNameSet(names=dimensions)
        assert isinstance(storageClass, (StorageClass, str))
        if isinstance(storageClass, StorageClass):
            self._storageClass = storageClass
            self._storageClassName = storageClass.name
        else:
            self._storageClass = None
            self._storageClassName = storageClass

    def __repr__(self):
        return "DatasetType({}, {}, {})".format(self.name, self.dimensions, self._storageClassName)

    def __eq__(self, other):
        if self._name != other._name:
            return False
        if self._dimensions != other._dimensions:
            return False
        if self._storageClass is not None and other._storageClass is not None:
            return self._storageClass == other._storageClass
        else:
            return self._storageClassName == other._storageClassName

    def __hash__(self):
        """Hash DatasetType instance.

        This only uses StorageClass name which is it consistent with the
        implementation of StorageClass hash method.
        """
        return hash((self._name, self._dimensions, self._storageClassName))

    @property
    def name(self):
        """A string name for the Dataset; must correspond to the same
        `DatasetType` across all Registries.
        """
        return self._name

    @property
    def dimensions(self):
        r"""The `Dimension`\ s that label and relate instances of this
        `DatasetType` (`DimensionGraph` or `DimensionNameSet`).

        If this `DatasetType` was not obtained from or registered with a
        `Registry`, this will typically be a `DimensionNameSet`, with much
        less functionality (just an unsorted ``.names`` and comparison
        operators) than a full `DimensionGraph`.
        """
        return self._dimensions

    @property
    def storageClass(self):
        """`StorageClass` instance that defines how this `DatasetType`
        is persisted. Note that if DatasetType was constructed with a name
        of a StorageClass then Butler has to be initialized before using
        this property.
        """
        if self._storageClass is None:
            self._storageClass = StorageClassFactory().getStorageClass(self._storageClassName)
        return self._storageClass

    @staticmethod
    def splitDatasetTypeName(datasetTypeName):
        """Given a dataset type name, return the root name and the component
        name.

        Parameters
        ----------
        datasetTypeName : `str`
            The name of the dataset type, can include a component using
            a "."-separator.

        Returns
        -------
        rootName : `str`
            Root name without any components.
        componentName : `str`
            The component if it has been specified, else `None`.

        Notes
        -----
        If the dataset type name is ``a.b.c`` this method will return a
        root name of ``a`` and a component name of ``b.c``.
        """
        comp = None
        root = datasetTypeName
        if "." in root:
            # If there is doubt, the component is after the first "."
            root, comp = root.split(".", maxsplit=1)
        return root, comp

    def nameAndComponent(self):
        """Return the root name of this dataset type and the component
        name (if defined).

        Returns
        -------
        rootName : `str`
            Root name for this `DatasetType` without any components.
        componentName : `str`
            The component if it has been specified, else `None`.
        """
        return self.splitDatasetTypeName(self.name)

    def component(self):
        """Component name (if defined)

        Returns
        -------
        comp : `str`
            Name of component part of DatasetType name. `None` if this
            `DatasetType` is not associated with a component.
        """
        _, comp = self.nameAndComponent()
        return comp

    def componentTypeName(self, component):
        """Given a component name, derive the datasetTypeName of that component

        Parameters
        ----------
        component : `str`
            Name of component

        Returns
        -------
        derived : `str`
            Compound name of this `DatasetType` and the component.

        Raises
        ------
        KeyError
            Requested component is not supported by this `DatasetType`.
        """
        if component in self.storageClass.components:
            return self.nameWithComponent(self.name, component)
        raise KeyError("Requested component ({}) not understood by this DatasetType".format(component))

    def isComponent(self):
        """Boolean indicating whether this `DatasetType` refers to a
        component of a composite.

        Returns
        -------
        isComponent : `bool`
            `True` if this `DatasetType` is a component, `False` otherwise.
        """
        if self.component():
            return True
        return False

    def isComposite(self):
        """Boolean indicating whether this `DatasetType` is a composite type.

        Returns
        -------
        isComposite : `bool`
            `True` if this `DatasetType` is a composite type, `False`
            otherwise.
        """
        return self.storageClass.isComposite()

    def _lookupNames(self):
        """Name keys to use when looking up this datasetType in a
        configuration.

        The names are returned in order of priority.

        Returns
        -------
        names : `tuple` of `LookupKey`
            Tuple of the `DatasetType` name and the `StorageClass` name.
            If the name includes a component the name with the component
            is first, then the name without the component and finally
            the storage class name.
        """
        rootName, componentName = self.nameAndComponent()
        lookups = (LookupKey(name=self.name),)
        if componentName is not None:
            lookups = lookups + (LookupKey(name=rootName),)

        if self.dimensions:
            # Dimensions are a lower priority than dataset type name
            lookups = lookups + (LookupKey(dimensions=self.dimensions),)

        return lookups + self.storageClass._lookupNames()

    def __reduce__(self):
        """Support pickling.

        StorageClass instances can not normally be pickled, so we pickle
        StorageClass name instead of instance.
        """
        return (DatasetType, (self.name, self.dimensions, self._storageClassName))

    def __deepcopy__(self, memo):
        """Support for deep copy method.

        Normally ``deepcopy`` will use pickle mechanism to make copies.
        We want to avoid that to support (possibly degenerate) use case when
        DatasetType is constructed with StorageClass instance which is not
        registered with StorageClassFactory (this happens in unit tests).
        Instead we re-implement ``__deepcopy__`` method.
        """
        return DatasetType(name=deepcopy(self.name, memo),
                           dimensions=deepcopy(self.dimensions, memo),
                           storageClass=deepcopy(self._storageClass or self._storageClassName, memo))

    def normalize(self, universe):
        """Ensure the dimensions and storage class name are valid, and make
        ``self.dimensions`` a true `DimensionGraph` instance if it isn't
        already.

        Parameters
        ----------
        universe : `DimensionGraph`
            The set of all known dimensions.

        Raises
        ------
        ValueError
            Raised if the DatasetType is invalid, either because one or more
            dimensions in ``self.dimensions`` is not in ``universe``, or the
            storage class name is not recognized.
        """
        if not isinstance(self._dimensions, DimensionGraph):
            self._dimensions = universe.extract(self._dimensions)
        try:
            # Trigger lookup of StorageClass instance from StorageClass name.
            # KeyError (sort of) makes sense in that context, but it doesn't
            # make as much sense in the context in which normalize() is called,
            # so we translate it to ValueError.
            self.storageClass
        except KeyError:
            raise ValueError(f"Storage class '{self._storageClassName}' not recognized.")


class DatasetRef:
    """Reference to a Dataset in a `Registry`.

    A `DatasetRef` may point to a Dataset that currently does not yet exist
    (e.g., because it is a predicted input for provenance).

    Parameters
    ----------
    datasetType : `DatasetType`
        The `DatasetType` for this Dataset.
    dataId : `dict` or `DataId`
        A `dict` of `Dimension` link fields that labels the Dataset within a
        Collection.
    id : `int`, optional
        A unique identifier.
        Normally set to `None` and assigned by `Registry`
    """

    __slots__ = ("_id", "_datasetType", "_dataId", "_producer", "_run", "_hash",
                 "_predictedConsumers", "_actualConsumers", "_components")

    def __init__(self, datasetType, dataId, id=None, run=None, hash=None, components=None):
        assert isinstance(datasetType, DatasetType)
        # Check the dimensions match if a DataId is provided
        if isinstance(dataId, DataId) and isinstance(datasetType.dimensions, DimensionGraph):
            if dataId.dimensions() != datasetType.dimensions:
                raise ValueError(f"Dimensions mismatch for {dataId} and {datasetType}")

        self._id = id
        self._datasetType = datasetType
        self._dataId = dataId
        self._producer = None
        self._predictedConsumers = dict()
        self._actualConsumers = dict()
        self._components = dict()
        if components is not None:
            self._components.update(components)
        self._run = run
        self._hash = hash

    __eq__ = slotValuesAreEqual

    def __repr__(self):
        return f"DatasetRef({self.datasetType}, {self.dataId}, id={self.id}, run={self.run})"

    @property
    def id(self):
        """Primary key of the dataset (`int`)

        Typically assigned by `Registry`.
        """
        return self._id

    @property
    def hash(self):
        """Secure hash of the `DatasetType` name and `DataId` (`bytes`).
        """
        if self._hash is None:
            message = hashlib.blake2b(digest_size=32)
            message.update(self.datasetType.name.encode("utf8"))
            self.dataId.updateHash(message)
            self._hash = message.digest()
        return self._hash

    @property
    def datasetType(self):
        """The `DatasetType` associated with the Dataset the `DatasetRef`
        points to.
        """
        return self._datasetType

    @property
    def dataId(self):
        """A `dict` of `Dimension` link fields that labels the Dataset
        within a Collection (`dict` or `DataId`).
        """
        return self._dataId

    @property
    def producer(self):
        """The `~lsst.daf.butler.Quantum` instance that produced (or will
        produce) the Dataset.

        Read-only; update via `~lsst.daf.butler.Registry.addDataset()`,
        `~lsst.daf.butler.Quantum.addOutput()`, or
        `~lsst.daf.butler.Butler.put()`.
        May be `None` if no provenance information is available.
        """
        return self._producer

    @property
    def run(self):
        """The `~lsst.daf.butler.Run` instance that produced (or will produce)
        the Dataset.

        Read-only; update via `~lsst.daf.butler.Registry.addDataset()` or
        `~lsst.daf.butler.Butler.put()`.
        """
        return self._run

    @property
    def predictedConsumers(self):
        """A sequence of `Quantum` instances that list this Dataset in their
        `predictedInputs` attributes.

        Read-only; update via `Quantum.addPredictedInput()`.
        May be an empty list if no provenance information is available.
        """
        return _safeMakeMappingProxyType(self._predictedConsumers)

    @property
    def actualConsumers(self):
        """A sequence of `Quantum` instances that list this Dataset in their
        `actualInputs` attributes.

        Read-only; update via `Registry.markInputUsed()`.
        May be an empty list if no provenance information is available.
        """
        return _safeMakeMappingProxyType(self._actualConsumers)

    @property
    def components(self):
        """Named `DatasetRef` components.

        Read-only; update via `Registry.attachComponent()`.
        """
        return _safeMakeMappingProxyType(self._components)

    @property
    def dimensions(self):
        """The dimensions associated with the underlying `DatasetType`
        """
        return self.datasetType.dimensions

    def __str__(self):
        components = ""
        if self.components:
            components = ", components=[" + ", ".join(self.components) + "]"
        return "DatasetRef({}, id={}, dataId={} {})".format(self.datasetType.name,
                                                            self.id, self.dataId, components)

    def detach(self):
        """Obtain a new DatasetRef that is detached from the registry.

        Its ``id`` property will be `None`.  This can be used for transfers
        and similar operations.
        """
        ref = deepcopy(self)
        ref._id = None
        return ref

    def isComponent(self):
        """Boolean indicating whether this `DatasetRef` refers to a
        component of a composite.

        Returns
        -------
        isComponent : `bool`
            `True` if this `DatasetRef` is a component, `False` otherwise.
        """
        return self.datasetType.isComponent()

    def isComposite(self):
        """Boolean indicating whether this `DatasetRef` is a composite type.

        Returns
        -------
        isComposite : `bool`
            `True` if this `DatasetRef` is a composite type, `False`
            otherwise.
        """
        return self.datasetType.isComposite()

    def _lookupNames(self):
        """Name keys to use when looking up this DatasetRef in a configuration.

        The names are returned in order of priority.

        Returns
        -------
        names : `tuple` of `LookupKey`
            Tuple of the `DatasetType` name and the `StorageClass` name.
            If ``instrument`` is defined in the dataId, each of those names
            is added to the start of the tuple with a key derived from the
            value of ``instrument``.
        """
        # Special case the instrument Dimension since we allow configs
        # to include the instrument name in the hierarchy.
        names = self.datasetType._lookupNames()

        if "instrument" in self.dataId:
            names = tuple(n.clone(dataId={"instrument": self.dataId["instrument"]})
                          for n in names) + names

        return names

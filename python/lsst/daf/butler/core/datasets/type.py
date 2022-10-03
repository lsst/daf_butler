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

__all__ = ["DatasetType", "SerializedDatasetType"]

import re
from copy import deepcopy
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, List, Mapping, Optional, Tuple, Type, Union

from pydantic import BaseModel, StrictBool, StrictStr

from ..configSupport import LookupKey
from ..dimensions import DimensionGraph, SerializedDimensionGraph
from ..json import from_json_pydantic, to_json_pydantic
from ..storageClass import StorageClass, StorageClassFactory

if TYPE_CHECKING:
    from ...registry import Registry
    from ..dimensions import Dimension, DimensionUniverse


def _safeMakeMappingProxyType(data: Optional[Mapping]) -> Mapping:
    if data is None:
        data = {}
    return MappingProxyType(data)


class SerializedDatasetType(BaseModel):
    """Simplified model of a `DatasetType` suitable for serialization."""

    name: StrictStr
    storageClass: Optional[StrictStr] = None
    dimensions: Optional[SerializedDimensionGraph] = None
    parentStorageClass: Optional[StrictStr] = None
    isCalibration: StrictBool = False

    @classmethod
    def direct(
        cls,
        *,
        name: str,
        storageClass: Optional[str] = None,
        dimensions: Optional[Dict] = None,
        parentStorageClass: Optional[str] = None,
        isCalibration: bool = False,
    ) -> SerializedDatasetType:
        """Construct a `SerializedDatasetType` directly without validators.

        This differs from PyDantics construct method in that the arguments are
        explicitly what the model requires, and it will recurse through
        members, constructing them from their corresponding `direct` methods.

        This method should only be called when the inputs are trusted.
        """
        node = SerializedDatasetType.__new__(cls)
        setter = object.__setattr__
        setter(node, "name", name)
        setter(node, "storageClass", storageClass)
        setter(
            node,
            "dimensions",
            dimensions if dimensions is None else SerializedDimensionGraph.direct(**dimensions),
        )
        setter(node, "parentStorageClass", parentStorageClass)
        setter(node, "isCalibration", isCalibration)
        setter(
            node,
            "__fields_set__",
            {"name", "storageClass", "dimensions", "parentStorageClass", "isCalibration"},
        )
        return node


class DatasetType:
    r"""A named category of Datasets.

    Defines how they are organized, related, and stored.

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
        `DatasetType` across all Registries.  Names must start with an
        upper or lowercase letter, and may contain only letters, numbers,
        and underscores.  Component dataset types should contain a single
        period separating the base dataset type name from the component name
        (and may be recursive).
    dimensions : `DimensionGraph` or iterable of `Dimension` or `str`
        Dimensions used to label and relate instances of this `DatasetType`.
        If not a `DimensionGraph`, ``universe`` must be provided as well.
    storageClass : `StorageClass` or `str`
        Instance of a `StorageClass` or name of `StorageClass` that defines
        how this `DatasetType` is persisted.
    parentStorageClass : `StorageClass` or `str`, optional
        Instance of a `StorageClass` or name of `StorageClass` that defines
        how the composite parent is persisted.  Must be `None` if this
        is not a component.
    universe : `DimensionUniverse`, optional
        Set of all known dimensions, used to normalize ``dimensions`` if it
        is not already a `DimensionGraph`.
    isCalibration : `bool`, optional
        If `True`, this dataset type may be included in
        `~CollectionType.CALIBRATION` collections.

    See Also
    --------
    :ref:`daf_butler_organizing_datasets`
    """

    __slots__ = (
        "_name",
        "_dimensions",
        "_storageClass",
        "_storageClassName",
        "_parentStorageClass",
        "_parentStorageClassName",
        "_isCalibration",
    )

    _serializedType = SerializedDatasetType

    VALID_NAME_REGEX = re.compile("^[a-zA-Z_][a-zA-Z0-9_]*(\\.[a-zA-Z_][a-zA-Z0-9_]*)*$")

    @staticmethod
    def nameWithComponent(datasetTypeName: str, componentName: str) -> str:
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

    def __init__(
        self,
        name: str,
        dimensions: Union[DimensionGraph, Iterable[Union[Dimension, str]]],
        storageClass: Union[StorageClass, str],
        parentStorageClass: Optional[Union[StorageClass, str]] = None,
        *,
        universe: Optional[DimensionUniverse] = None,
        isCalibration: bool = False,
    ):
        if self.VALID_NAME_REGEX.match(name) is None:
            raise ValueError(f"DatasetType name '{name}' is invalid.")
        self._name = name
        if not isinstance(dimensions, DimensionGraph):
            if universe is None:
                raise ValueError(
                    "If dimensions is not a normalized DimensionGraph, a universe must be provided."
                )
            dimensions = universe.extract(dimensions)
        self._dimensions = dimensions
        if name in self._dimensions.universe.getGovernorDimensions().names:
            raise ValueError(f"Governor dimension name {name} cannot be used as a dataset type name.")
        if not isinstance(storageClass, (StorageClass, str)):
            raise ValueError(f"StorageClass argument must be StorageClass or str. Got {storageClass}")
        self._storageClass: Optional[StorageClass]
        if isinstance(storageClass, StorageClass):
            self._storageClass = storageClass
            self._storageClassName = storageClass.name
        else:
            self._storageClass = None
            self._storageClassName = storageClass

        self._parentStorageClass: Optional[StorageClass] = None
        self._parentStorageClassName: Optional[str] = None
        if parentStorageClass is not None:
            if not isinstance(storageClass, (StorageClass, str)):
                raise ValueError(
                    f"Parent StorageClass argument must be StorageClass or str. Got {parentStorageClass}"
                )

            # Only allowed for a component dataset type
            _, componentName = self.splitDatasetTypeName(self._name)
            if componentName is None:
                raise ValueError(
                    f"Can not specify a parent storage class if this is not a component ({self._name})"
                )
            if isinstance(parentStorageClass, StorageClass):
                self._parentStorageClass = parentStorageClass
                self._parentStorageClassName = parentStorageClass.name
            else:
                self._parentStorageClassName = parentStorageClass

        # Ensure that parent storage class is specified when we have
        # a component and is not specified when we don't
        _, componentName = self.splitDatasetTypeName(self._name)
        if parentStorageClass is None and componentName is not None:
            raise ValueError(
                f"Component dataset type '{self._name}' constructed without parent storage class"
            )
        if parentStorageClass is not None and componentName is None:
            raise ValueError(f"Parent storage class specified by {self._name} is not a composite")
        self._isCalibration = isCalibration

    def __repr__(self) -> str:
        extra = ""
        if self._parentStorageClassName:
            extra = f", parentStorageClass={self._parentStorageClassName}"
        if self._isCalibration:
            extra += ", isCalibration=True"
        return f"DatasetType({self.name!r}, {self.dimensions}, {self._storageClassName}{extra})"

    def _equal_ignoring_storage_class(self, other: Any) -> bool:
        """Check everything is equal except the storage class.

        Parameters
        ----------
        other : Any
            Object to check against this one.

        Returns
        -------
        mostly : `bool`
            Returns `True` if everything except the storage class is equal.
        """
        if not isinstance(other, type(self)):
            return False
        if self._name != other._name:
            return False
        if self._dimensions != other._dimensions:
            return False
        if self._isCalibration != other._isCalibration:
            return False
        if self._parentStorageClass is not None and other._parentStorageClass is not None:
            return self._parentStorageClass == other._parentStorageClass
        else:
            return self._parentStorageClassName == other._parentStorageClassName

    def __eq__(self, other: Any) -> bool:
        mostly_equal = self._equal_ignoring_storage_class(other)
        if not mostly_equal:
            return False

        # Be careful not to force a storage class to import the corresponding
        # python code.
        if self._storageClass is not None and other._storageClass is not None:
            if self._storageClass != other._storageClass:
                return False
        else:
            if self._storageClassName != other._storageClassName:
                return False
        return True

    def is_compatible_with(self, other: DatasetType) -> bool:
        """Determine if the given `DatasetType` is compatible with this one.

        Compatibility requires a matching name and dimensions and a storage
        class for this dataset type that can convert the python type associated
        with the other storage class to this python type.

        Parameters
        ----------
        other : `DatasetType`
            Dataset type to check.

        Returns
        -------
        is_compatible : `bool`
            Returns `True` if the other dataset type is either the same as this
            or the storage class associated with the other can be converted to
            this.
        """
        mostly_equal = self._equal_ignoring_storage_class(other)
        if not mostly_equal:
            return False

        # If the storage class names match then they are compatible.
        if self._storageClassName == other._storageClassName:
            return True

        # Now required to check the full storage class.
        self_sc = self.storageClass
        other_sc = other.storageClass

        return self_sc.can_convert(other_sc)

    def __hash__(self) -> int:
        """Hash DatasetType instance.

        This only uses StorageClass name which is it consistent with the
        implementation of StorageClass hash method.
        """
        return hash((self._name, self._dimensions, self._storageClassName, self._parentStorageClassName))

    def __lt__(self, other: Any) -> bool:
        """Sort using the dataset type name."""
        if not isinstance(other, type(self)):
            return NotImplemented
        return self.name < other.name

    @property
    def name(self) -> str:
        """Return a string name for the Dataset.

        Must correspond to the same `DatasetType` across all Registries.
        """
        return self._name

    @property
    def dimensions(self) -> DimensionGraph:
        r"""Return the `Dimension`\ s fir this dataset type.

        The dimensions label and relate instances of this
        `DatasetType` (`DimensionGraph`).
        """
        return self._dimensions

    @property
    def storageClass(self) -> StorageClass:
        """Return `StorageClass` instance associated with this dataset type.

        The `StorageClass` defines how this `DatasetType`
        is persisted. Note that if DatasetType was constructed with a name
        of a StorageClass then Butler has to be initialized before using
        this property.
        """
        if self._storageClass is None:
            self._storageClass = StorageClassFactory().getStorageClass(self._storageClassName)
        return self._storageClass

    @property
    def storageClass_name(self) -> str:
        """Return the storage class name.

        This will never force the storage class to be imported.
        """
        return self._storageClassName

    @property
    def parentStorageClass(self) -> Optional[StorageClass]:
        """Return the storage class of the composite containing this component.

        Note that if DatasetType was constructed with a name of a
        StorageClass then Butler has to be initialized before using this
        property. Can be `None` if this is not a component of a composite.
        Must be defined if this is a component.
        """
        if self._parentStorageClass is None and self._parentStorageClassName is None:
            return None
        if self._parentStorageClass is None and self._parentStorageClassName is not None:
            self._parentStorageClass = StorageClassFactory().getStorageClass(self._parentStorageClassName)
        return self._parentStorageClass

    def isCalibration(self) -> bool:
        """Return if datasets of this type can be in calibration collections.

        Returns
        -------
        flag : `bool`
            `True` if datasets of this type may be included in calibration
            collections.
        """
        return self._isCalibration

    @staticmethod
    def splitDatasetTypeName(datasetTypeName: str) -> Tuple[str, Optional[str]]:
        """Return the root name and the component from a composite name.

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

    def nameAndComponent(self) -> Tuple[str, Optional[str]]:
        """Return the root name of this dataset type and any component.

        Returns
        -------
        rootName : `str`
            Root name for this `DatasetType` without any components.
        componentName : `str`
            The component if it has been specified, else `None`.
        """
        return self.splitDatasetTypeName(self.name)

    def component(self) -> Optional[str]:
        """Return the component name (if defined).

        Returns
        -------
        comp : `str`
            Name of component part of DatasetType name. `None` if this
            `DatasetType` is not associated with a component.
        """
        _, comp = self.nameAndComponent()
        return comp

    def componentTypeName(self, component: str) -> str:
        """Derive a component dataset type from a composite.

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
        if component in self.storageClass.allComponents():
            return self.nameWithComponent(self.name, component)
        raise KeyError(f"Requested component ({component}) not understood by this DatasetType ({self})")

    def makeCompositeDatasetType(self) -> DatasetType:
        """Return a composite dataset type from the component.

        Returns
        -------
        composite : `DatasetType`
            The composite dataset type.

        Raises
        ------
        RuntimeError
            Raised if this dataset type is not a component dataset type.
        """
        if not self.isComponent():
            raise RuntimeError(f"DatasetType {self.name} must be a component to form the composite")
        composite_name, _ = self.nameAndComponent()
        if self.parentStorageClass is None:
            raise ValueError(
                f"Parent storage class is not set. Unable to create composite type from {self.name}"
            )
        return DatasetType(
            composite_name,
            dimensions=self.dimensions,
            storageClass=self.parentStorageClass,
            isCalibration=self.isCalibration(),
        )

    def makeComponentDatasetType(self, component: str) -> DatasetType:
        """Return a component dataset type from a composite.

        Assumes the same dimensions as the parent.

        Parameters
        ----------
        component : `str`
            Name of component

        Returns
        -------
        datasetType : `DatasetType`
            A new DatasetType instance.
        """
        # The component could be a read/write or read component
        return DatasetType(
            self.componentTypeName(component),
            dimensions=self.dimensions,
            storageClass=self.storageClass.allComponents()[component],
            parentStorageClass=self.storageClass,
            isCalibration=self.isCalibration(),
        )

    def makeAllComponentDatasetTypes(self) -> List[DatasetType]:
        """Return all component dataset types for this composite.

        Returns
        -------
        all : `list` of `DatasetType`
            All the component dataset types. If this is not a composite
            then returns an empty list.
        """
        return [
            self.makeComponentDatasetType(componentName)
            for componentName in self.storageClass.allComponents()
        ]

    def overrideStorageClass(self, storageClass: str | StorageClass) -> DatasetType:
        """Create a new `DatasetType` from this one but with an updated
        `StorageClass`.

        Parameters
        ----------
        storageClass : `str` or `StorageClass`
            The new storage class.

        Returns
        -------
        modified : `DatasetType`
            A new dataset type that is the same as the current one but with
            a different storage class.

        Notes
        -----
        If this is a component dataset type, the parent storage class
        will be retained.
        """
        parent = self._parentStorageClass if self._parentStorageClass else self._parentStorageClassName
        new = DatasetType(
            self.name,
            dimensions=self.dimensions,
            storageClass=storageClass,
            parentStorageClass=parent,
            isCalibration=self.isCalibration(),
        )
        # Check validity.
        if new.is_compatible_with(self) or self.is_compatible_with(new):
            return new
        raise ValueError(
            f"The new storage class ({new.storageClass}) is not compatible with the "
            f"existing storage class ({self.storageClass})."
        )

    def isComponent(self) -> bool:
        """Return whether this `DatasetType` refers to a component.

        Returns
        -------
        isComponent : `bool`
            `True` if this `DatasetType` is a component, `False` otherwise.
        """
        if self.component():
            return True
        return False

    def isComposite(self) -> bool:
        """Return whether this `DatasetType` is a composite.

        Returns
        -------
        isComposite : `bool`
            `True` if this `DatasetType` is a composite type, `False`
            otherwise.
        """
        return self.storageClass.isComposite()

    def _lookupNames(self) -> Tuple[LookupKey, ...]:
        """Return name keys to use for lookups in configurations.

        The names are returned in order of priority.

        Returns
        -------
        names : `tuple` of `LookupKey`
            Tuple of the `DatasetType` name and the `StorageClass` name.
            If the name includes a component the name with the component
            is first, then the name without the component and finally
            the storage class name and the storage class name of the
            composite.
        """
        rootName, componentName = self.nameAndComponent()
        lookups: Tuple[LookupKey, ...] = (LookupKey(name=self.name),)
        if componentName is not None:
            lookups = lookups + (LookupKey(name=rootName),)

        if self.dimensions:
            # Dimensions are a lower priority than dataset type name
            lookups = lookups + (LookupKey(dimensions=self.dimensions),)

        storageClasses = self.storageClass._lookupNames()
        if componentName is not None and self.parentStorageClass is not None:
            storageClasses += self.parentStorageClass._lookupNames()

        return lookups + storageClasses

    def to_simple(self, minimal: bool = False) -> SerializedDatasetType:
        """Convert this class to a simple python type.

        This makes it suitable for serialization.

        Parameters
        ----------
        minimal : `bool`, optional
            Use minimal serialization. Requires Registry to convert
            back to a full type.

        Returns
        -------
        simple : `SerializedDatasetType`
            The object converted to a class suitable for serialization.
        """
        as_dict: Dict[str, Any]
        if minimal:
            # Only needs the name.
            as_dict = {"name": self.name}
        else:
            # Convert to a dict form
            as_dict = {
                "name": self.name,
                "storageClass": self._storageClassName,
                "isCalibration": self._isCalibration,
                "dimensions": self.dimensions.to_simple(),
            }

            if self._parentStorageClassName is not None:
                as_dict["parentStorageClass"] = self._parentStorageClassName
        return SerializedDatasetType(**as_dict)

    @classmethod
    def from_simple(
        cls,
        simple: SerializedDatasetType,
        universe: Optional[DimensionUniverse] = None,
        registry: Optional[Registry] = None,
    ) -> DatasetType:
        """Construct a new object from the simplified form.

        This is usually data returned from the `to_simple` method.

        Parameters
        ----------
        simple : `SerializedDatasetType`
            The value returned by `to_simple()`.
        universe : `DimensionUniverse`
            The special graph of all known dimensions of which this graph will
            be a subset. Can be `None` if a registry is provided.
        registry : `lsst.daf.butler.Registry`, optional
            Registry to use to convert simple name of a DatasetType to
            a full `DatasetType`. Can be `None` if a full description of
            the type is provided along with a universe.

        Returns
        -------
        datasetType : `DatasetType`
            Newly-constructed object.
        """
        if simple.storageClass is None:
            # Treat this as minimalist representation
            if registry is None:
                raise ValueError(
                    f"Unable to convert a DatasetType name '{simple}' to DatasetType without a Registry"
                )
            return registry.getDatasetType(simple.name)

        if universe is None and registry is None:
            raise ValueError("One of universe or registry must be provided.")

        if universe is None and registry is not None:
            # registry should not be none by now but test helps mypy
            universe = registry.dimensions

        if universe is None:
            # this is for mypy
            raise ValueError("Unable to determine a usable universe")

        if simple.dimensions is None:
            # mypy hint
            raise ValueError(f"Dimensions must be specified in {simple}")

        return cls(
            name=simple.name,
            dimensions=DimensionGraph.from_simple(simple.dimensions, universe=universe),
            storageClass=simple.storageClass,
            isCalibration=simple.isCalibration,
            parentStorageClass=simple.parentStorageClass,
            universe=universe,
        )

    to_json = to_json_pydantic
    from_json = classmethod(from_json_pydantic)

    def __reduce__(
        self,
    ) -> Tuple[
        Callable, Tuple[Type[DatasetType], Tuple[str, DimensionGraph, str, Optional[str]], Dict[str, bool]]
    ]:
        """Support pickling.

        StorageClass instances can not normally be pickled, so we pickle
        StorageClass name instead of instance.
        """
        return _unpickle_via_factory, (
            self.__class__,
            (self.name, self.dimensions, self._storageClassName, self._parentStorageClassName),
            {"isCalibration": self._isCalibration},
        )

    def __deepcopy__(self, memo: Any) -> DatasetType:
        """Support for deep copy method.

        Normally ``deepcopy`` will use pickle mechanism to make copies.
        We want to avoid that to support (possibly degenerate) use case when
        DatasetType is constructed with StorageClass instance which is not
        registered with StorageClassFactory (this happens in unit tests).
        Instead we re-implement ``__deepcopy__`` method.
        """
        return DatasetType(
            name=deepcopy(self.name, memo),
            dimensions=deepcopy(self.dimensions, memo),
            storageClass=deepcopy(self._storageClass or self._storageClassName, memo),
            parentStorageClass=deepcopy(self._parentStorageClass or self._parentStorageClassName, memo),
            isCalibration=deepcopy(self._isCalibration, memo),
        )


def _unpickle_via_factory(factory: Callable, args: Any, kwargs: Any) -> DatasetType:
    """Unpickle something by calling a factory.

    Allows subclasses to unpickle using `__reduce__` with keyword
    arguments as well as positional arguments.
    """
    return factory(*args, **kwargs)

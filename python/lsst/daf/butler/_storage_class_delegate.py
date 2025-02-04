# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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

"""Support for reading and writing composite objects."""

from __future__ import annotations

__all__ = ("DatasetComponent", "StorageClassDelegate")

import copy
import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from lsst.utils.introspection import get_full_type_name

if TYPE_CHECKING:
    from lsst.daf.butler import DatasetProvenance, DatasetRef

    from ._storage_class import StorageClass

log = logging.getLogger(__name__)


@dataclass
class DatasetComponent:
    """Component of a dataset and associated information."""

    name: str
    """Name of the component.
    """

    storageClass: StorageClass
    """StorageClass to be used when reading or writing this component.
    """

    component: Any
    """Component extracted from the composite object.
    """


class StorageClassDelegate:
    """Delegate class for StorageClass components and parameters.

    This class delegates the handling of components and parameters for the
    python type associated with a particular `StorageClass`.

    A delegate is required for any storage class that defines components
    (derived or otherwise) or support read parameters. It is used for
    composite disassembly and assembly.

    Attributes
    ----------
    storageClass : `StorageClass`

    Parameters
    ----------
    storageClass : `StorageClass`
        `StorageClass` to be used with this delegate.
    """

    def __init__(self, storageClass: StorageClass):
        assert storageClass is not None
        self.storageClass = storageClass

    def can_accept(self, inMemoryDataset: Any) -> bool:
        """Indicate whether this delegate can accept the specified
        storage class directly.

        Parameters
        ----------
        inMemoryDataset : `object`
            The dataset that is to be stored.

        Returns
        -------
        accepts : `bool`
            If `True` the delegate can handle data of this type without
            requiring datastore to convert it. If `False` the datastore
            will attempt to convert before storage.

        Notes
        -----
        The base class always returns `False` even if the given type is an
        instance of the delegate type. This will result in a storage class
        conversion no-op but also allows mocks with mocked storage classes
        to work properly.
        """
        return False

    @staticmethod
    def _attrNames(componentName: str, getter: bool = True) -> tuple[str, ...]:
        """Return list of suitable attribute names to attempt to use.

        Parameters
        ----------
        componentName : `str`
            Name of component/attribute to look for.
        getter : `bool`
            If true, return getters, else return setters.

        Returns
        -------
        attrs : `tuple(str)`
            Tuple of strings to attempt.
        """
        root = "get" if getter else "set"

        # Capitalized name for getXxx must only capitalize first letter and not
        # downcase the rest. getVisitInfo and not getVisitinfo
        first = componentName[0].upper()
        if len(componentName) > 1:
            tail = componentName[1:]
        else:
            tail = ""
        capitalized = f"{root}{first}{tail}"
        return (componentName, f"{root}_{componentName}", capitalized)

    def assemble(self, components: dict[str, Any], pytype: type | None = None) -> Any:
        """Construct an object from components based on storageClass.

        This generic implementation assumes that instances of objects
        can be created either by passing all the components to a constructor
        or by calling setter methods with the name.

        Parameters
        ----------
        components : `dict`
            Collection of components from which to assemble a new composite
            object. Keys correspond to composite names in the `StorageClass`.
        pytype : `type`, optional
            Override the type from the
            :attr:`StorageClassDelegate.storageClass`
            to use when assembling the final object.

        Returns
        -------
        composite : `object`
            New composite object assembled from components.

        Raises
        ------
        ValueError
            Some components could not be used to create the object or,
            alternatively, some components were not defined in the associated
            StorageClass.
        """
        if pytype is not None:
            cls = pytype
        else:
            cls = self.storageClass.pytype

        # Check that the storage class components are consistent
        understood = set(self.storageClass.components)
        requested = set(components.keys())
        unknown = requested - understood
        if unknown:
            raise ValueError(f"Requested component(s) not known to StorageClass: {unknown}")

        # First try to create an instance directly using keyword args
        try:
            obj = cls(**components)
        except TypeError:
            obj = None

        # Now try to use setters if direct instantiation didn't work
        if not obj:
            obj = cls()

            failed = []
            for name, component in components.items():
                if component is None:
                    continue
                for attr in self._attrNames(name, getter=False):
                    if hasattr(obj, attr):
                        if attr == name:  # Real attribute
                            setattr(obj, attr, component)
                        else:
                            setter = getattr(obj, attr)
                            setter(component)
                        break
                    else:
                        failed.append(name)

            if failed:
                raise ValueError(f"Unhandled components during assembly ({failed})")

        return obj

    def getComponent(self, composite: Any, componentName: str) -> Any:
        """Attempt to retrieve component from composite object by heuristic.

        Will attempt a direct attribute retrieval, or else getter methods of
        the form "get_componentName" and "getComponentName".

        Parameters
        ----------
        composite : `object`
            Item to query for the component.
        componentName : `str`
            Name of component to retrieve.

        Returns
        -------
        component : `object`
            Component extracted from composite.

        Raises
        ------
        AttributeError
            The attribute could not be read from the composite.
        """
        component = None

        if hasattr(composite, "__contains__") and componentName in composite:
            component = composite[componentName]
            return component

        for attr in self._attrNames(componentName, getter=True):
            if hasattr(composite, attr):
                component = getattr(composite, attr)
                if attr != componentName:  # We have a method
                    component = component()
                break
        else:
            raise AttributeError(f"Unable to get component {componentName}")
        return component

    def disassemble(
        self, composite: Any, subset: Iterable | None = None, override: Any | None = None
    ) -> dict[str, DatasetComponent]:
        """Disassembler a composite.

        This is a generic implementation of a disassembler.
        This implementation attempts to extract components from the parent
        by looking for attributes of the same name or getter methods derived
        from the component name.

        Parameters
        ----------
        composite : `object`
            Parent composite object consisting of components to be extracted.
        subset : iterable, optional
            Iterable containing subset of components to extract from composite.
            Must be a subset of those defined in
            `StorageClassDelegate.storageClass`.
        override : `object`, optional
            Object to use for disassembly instead of parent. This can be useful
            when called from subclasses that have composites in a hierarchy.

        Returns
        -------
        components : `dict`
            `dict` with keys matching the components defined in
            `StorageClassDelegate.storageClass`
            and values being `DatasetComponent` instances describing the
            component.

        Raises
        ------
        ValueError
            A requested component can not be found in the parent using generic
            lookups.
        TypeError
            The parent object does not match the supplied
            `StorageClassDelegate.storageClass`.
        """
        if not self.storageClass.isComposite():
            raise TypeError(
                f"Can not disassemble something that is not a composite (storage class={self.storageClass})"
            )

        if not self.storageClass.validateInstance(composite):
            raise TypeError(
                "Unexpected type mismatch between parent and StorageClass "
                f"({type(composite)} != {self.storageClass.pytype})"
            )

        requested = set(self.storageClass.components)

        if subset is not None:
            subset = set(subset)
            diff = subset - requested
            if diff:
                raise ValueError(f"Requested subset is not a subset of supported components: {diff}")
            requested = subset

        if override is not None:
            composite = override

        components = {}
        for c in list(requested):
            # Try three different ways to get a value associated with the
            # component name.
            try:
                component = self.getComponent(composite, c)
            except AttributeError:
                # Defer complaining so we get an idea of how many problems
                # we have
                pass
            else:
                # If we found a match store it in the results dict and remove
                # it from the list of components we are still looking for.
                if component is not None:
                    components[c] = DatasetComponent(c, self.storageClass.components[c], component)
                requested.remove(c)

        if requested:
            raise ValueError(f"Unhandled components during disassembly ({requested})")

        return components

    def add_provenance(
        self, inMemoryDataset: Any, ref: DatasetRef, provenance: DatasetProvenance | None = None
    ) -> Any:
        """Add provenance to the composite dataset.

        Parameters
        ----------
        inMemoryDataset : `object`
            The composite dataset to serialize.
        ref : `DatasetRef`
            The dataset associated with this in-memory dataset.
        provenance : `DatasetProvenance` or `None`, optional
            Any provenance that should be attached to the serialized dataset.
            Can be ignored by a delegate.

        Returns
        -------
        dataset_to_disassemble : `object`
            The dataset to use for serialization and disassembly.
            Can be the same object as given.

        Notes
        -----
        The base class implementation returns the given object unchanged.
        """
        return inMemoryDataset

    def handleParameters(self, inMemoryDataset: Any, parameters: Mapping[str, Any] | None = None) -> Any:
        """Modify the in-memory dataset using the supplied parameters.

        Can return a possibly new object.

        For safety, if any parameters are given to this method an
        exception will be raised.  This is to protect the user from
        thinking that parameters have been applied when they have not been
        applied.

        Parameters
        ----------
        inMemoryDataset : `object`
            Object to modify based on the parameters.
        parameters : `dict`
            Parameters to apply. Values are specific to the parameter.
            Supported parameters are defined in the associated
            `StorageClass`.  If no relevant parameters are specified the
            inMemoryDataset will be return unchanged.

        Returns
        -------
        inMemoryDataset : `object`
            Updated form of supplied in-memory dataset, after parameters
            have been used.

        Raises
        ------
        ValueError
            Parameters have been provided to this default implementation.
        """
        if parameters:
            raise ValueError(f"Parameters ({parameters}) provided to default implementation.")

        return inMemoryDataset

    @classmethod
    def selectResponsibleComponent(cls, derivedComponent: str, fromComponents: set[str | None]) -> str:
        """Select the best component for calculating a derived component.

        Given a possible set of components to choose from, return the
        component that should be used to calculate the requested derived
        component.

        Parameters
        ----------
        derivedComponent : `str`
            The derived component that is being requested.
        fromComponents : `set` of `str`
            The available set of component options from which that derived
            component can be computed. `None` can be included but should
            be ignored.

        Returns
        -------
        required : `str`
            The component that should be used.

        Raises
        ------
        NotImplementedError
            Raised if this delegate refuses to answer the question.
        ValueError
            Raised if this delegate can not determine a relevant component
            from the supplied options.
        """
        raise NotImplementedError("This delegate does not support derived components")

    def copy(self, inMemoryDataset: Any) -> Any:
        """Copy the supplied python type and return the copy.

        Parameters
        ----------
        inMemoryDataset : `object`
            Object to copy.

        Returns
        -------
        copied : `object`
            A copy of the supplied object. Can be the same object if the
            object is known to be read-only.

        Raises
        ------
        NotImplementedError
            Raised if none of the default methods for copying work.

        Notes
        -----
        The default implementation uses `copy.deepcopy()`.
        It is generally expected that this method is the equivalent of a deep
        copy. Subclasses can override this method if they already know the
        optimal approach for deep copying.
        """
        try:
            return copy.deepcopy(inMemoryDataset)
        except Exception as e:
            raise NotImplementedError(
                f"Unable to deep copy the supplied python type ({get_full_type_name(inMemoryDataset)}) "
                f"using default methods ({e})"
            ) from e

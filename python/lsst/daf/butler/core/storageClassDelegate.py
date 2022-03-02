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

"""Support for reading and writing composite objects."""

from __future__ import annotations

__all__ = ("DatasetComponent", "StorageClassDelegate")

import collections.abc
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Iterable, Mapping, Optional, Set, Tuple, Type

if TYPE_CHECKING:
    from .storageClass import StorageClass

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

    @staticmethod
    def _attrNames(componentName: str, getter: bool = True) -> Tuple[str, ...]:
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
        capitalized = "{}{}{}".format(root, first, tail)
        return (componentName, "{}_{}".format(root, componentName), capitalized)

    def assemble(self, components: Dict[str, Any], pytype: Optional[Type] = None) -> Any:
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
            raise ValueError("Requested component(s) not known to StorageClass: {}".format(unknown))

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
                raise ValueError("Unhandled components during assembly ({})".format(failed))

        return obj

    def getValidComponents(self, composite: Any) -> Dict[str, Any]:
        """Extract all non-None components from a composite.

        Parameters
        ----------
        composite : `object`
            Composite from which to extract components.

        Returns
        -------
        comps : `dict`
            Non-None components extracted from the composite, indexed by the
            component name as derived from the
            `StorageClassDelegate.storageClass`.
        """
        components = {}
        if self.storageClass.isComposite():
            for c in self.storageClass.components:
                if isinstance(composite, collections.abc.Mapping):
                    comp = composite[c]
                else:
                    try:
                        comp = self.getComponent(composite, c)
                    except AttributeError:
                        pass
                    else:
                        if comp is not None:
                            components[c] = comp
        return components

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
            raise AttributeError("Unable to get component {}".format(componentName))
        return component

    def disassemble(
        self, composite: Any, subset: Optional[Iterable] = None, override: Optional[Any] = None
    ) -> Dict[str, DatasetComponent]:
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
                "Can not disassemble something that is not a composite"
                f" (storage class={self.storageClass})"
            )

        if not self.storageClass.validateInstance(composite):
            raise TypeError(
                "Unexpected type mismatch between parent and StorageClass"
                " ({} != {})".format(type(composite), self.storageClass.pytype)
            )

        requested = set(self.storageClass.components)

        if subset is not None:
            subset = set(subset)
            diff = subset - requested
            if diff:
                raise ValueError("Requested subset is not a subset of supported components: {}".format(diff))
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
            raise ValueError("Unhandled components during disassembly ({})".format(requested))

        return components

    def handleParameters(self, inMemoryDataset: Any, parameters: Optional[Mapping[str, Any]] = None) -> Any:
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
    def selectResponsibleComponent(cls, derivedComponent: str, fromComponents: Set[Optional[str]]) -> str:
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

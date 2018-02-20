#
# LSST Data Management System
#
# Copyright 2018  AURA/LSST.
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

"""Support for reading and writing composite objects."""

import collections


class DatasetComponent:

    """Component of a dataset and associated information.

    Parameters
    ----------
    name : `str`
        Name of the component.
    storageClass : `StorageClass`
        StorageClass to be used when reading or writing this component.
    component : `object`
        Component extracted from the composite object.

    """
    def __init__(self, name, storageClass, component):
        self.name = name
        self.storageClass = storageClass
        self.component = component


class CompositeAssembler:
    """Class for providing assembler and disassembler support for composites.

    Parameters
    ----------
    storageClass : `StorageClass`
        `StorageClass` to be used with this assembler.
    """

    def __init__(self, storageClass):
        self.storageClass = storageClass

    @staticmethod
    def _attrNames(componentName, getter=True):
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

    def assemble(self, components, pytype=None):
        """Construct an object from components based on storageClass.

        This generic implementation assumes that instances of objects
        can be created either by passing all the components to a constructor
        or by calling setter methods with the name.

        Parameters
        ----------
        components : `dict`
            Collection of components from which to assemble a new composite
            object. Keys correspond to composite names in the `StorageClass`.
        pytype : `class`, optional
            Override the type from the `storageClass` to use when assembling
            the final object.

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

    def getValidComponents(self, composite):
        """Extract all non-None components from a composite.

        Parameters
        ----------
        composite : `object`
            Composite from which to extract components.

        Returns
        -------
        comps : `dict`
            Non-None components extracted from the composite, indexed by the
            component name as derived from the `self.storageClass`.
        """
        components = {}
        if self.storageClass is not None and self.storageClass.components:
            for c in self.storageClass.components:
                if isinstance(composite, collections.Mapping):
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

    def getComponent(self, composite, componentName):
        """Attempt to retrieve component from composite object by heuristic.

        Will attempt a direct attribute retrieval, or else getter methods of the
        form "get_componentName" and "getComponentName".

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
        for attr in self._attrNames(componentName, getter=True):
            if hasattr(composite, attr):
                component = getattr(composite, attr)
                if attr != componentName:  # We have a method
                    component = component()
                break
            elif hasattr(composite, '__contains__') and attr in composite:
                component = composite[attr]
        else:
            raise AttributeError("Unable to get component {}".format(componentName))
        return component

    def disassemble(self, composite, subset=None, override=None):
        """Generic implementation of a disassembler.

        This implementation attempts to extract components from the parent
        by looking for attributes of the same name or getter methods derived
        from the component name.

        Parameters
        ----------
        composite : `object`
            Parent composite object consisting of components to be extracted.
        subset : `iterable`, optional
            Iterable containing subset of components to extract from composite.
            Must be a subset of those defined in `self.storageClass`.
        override : `object`, optional
            Object to use for disassembly instead of parent. This can be useful
            when called from subclasses that have composites in a hierarchy.

        Returns
        -------
        components : `dict`
            `dict` with keys matching the components defined in `storageClass`
            and values being `DatasetComponent` instances describing the
            component. Returns None if this is not a composite `self.storageClass`.

        Raises
        ------
        ValueError
            A requested component can not be found in the parent using generic
            lookups.
        TypeError
            The parent object does not match the supplied `self.storageClass`.
        """
        if self.storageClass.components is None:
            return

        if not self.storageClass.validateInstance(composite):
            raise TypeError("Unexpected type mismatch between parent and StorageClass"
                            " ({} != {})".format(type(composite), self.storageClass.pytype))

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
                # Defer complaining so we get an idea of how many problems we have
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


class CompositeAssemblerMonolithic(CompositeAssembler):
    """Generic assembler class that disables disassembly."""
    disassemble = None

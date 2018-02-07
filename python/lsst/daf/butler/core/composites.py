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


def genericAssembler(storageClass, components):
    """Construct an object from components based on storageClass.

    This generic implementation assumes that instances of objects
    can be created either by passing all the components to a constructor
    or by calling setter methods with the name.

    Parameters
    ----------
    storageClass : `StorageClass`
        `StorageClass` describing the entity to be created from the
        components.
    components : `dict`
        Collection of components from which to assemble a new composite
        object. Keys correspond to composite names in the `StorageClass`.

    Returns
    -------
    composite : `object`
        New composite object assembled from components.

    Raises
    ------
    ValueError
        Some components could not be used to create the object.
    """
    cls = storageClass.pytype

    # Should we check that the storage class components match or are a superset
    # of the items described in the supplied components?

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
            for attr in (name, "set_" + name, "set" + name.capitalize()):
                if hasattr(obj, attr):
                    setattr(obj, attr, component)
                    break
            else:
                failed.append(name)

        if failed:
            raise ValueError("There are unhandled components ({})".format(failed))

    return obj


def genericDisassembler(composite, storageClass):
    """Generic implementation of a disassembler.

    This implementation attempts to extract components from the parent
    by looking for attributes of the same name or getter methods derived
    from the component name.

    Parameters
    ----------
    composite : `object`
        Parent composite object consisting of components to be extracted.
    storageClass : `StorageClass`
        `StorageClass` associated with the parent, with defined components.

    Returns
    -------
    components : `dict`
        `dict` with keys matching the components defined in `storageClass`
        and values being `DatasetComponent` instances describing the component.

    Raises
    ------
    ValueError
        A requested component can not be found in the parent using generic
        lookups.
    TypeError
        The parent object does not match the supplied `StorageClass`.
    """
    if not storageClass.validateInstance(composite):
        raise TypeError("Unexpected type mismatch between parent and StorageClass"
                        " ({} != {})".format(type(composite), storageClass.pytype))

    requested = set(storageClass.components.keys())
    components = {}
    for c in list(requested):
        # Try three different ways to get a value associated with the
        # component name.
        component = None
        for attr in (c, "get_" + c, "get" + c.capitalize()):
            if hasattr(composite, attr):
                component = getattr(composite, attr)
                break

        # If we found a match store it in the results dict and remove
        # it from the list of components we are still looking for.
        if component is not None:
            components[c] = DatasetComponent(c, storageClass.components[c], component)
            requested.remove(c)

    if requested:
        raise ValueError("There are unhandled components ({})".format(requested))

    return components

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

"""Support for Storage Classes."""

import builtins

from lsst.daf.butler.core.utils import doImport
from lsst.daf.butler.core.composites import CompositeAssembler

from .mappingFactory import MappingFactory


class StorageClass:
    """Class describing how a label maps to a particular Python type.
    """

    # Components are fixed per class
    _components = {}

    # These are internal class attributes supporting lazy loading of concrete
    # python types and functions from the string. The lazy loading is only done
    # once the property is requested by an instance. The loading is fixed per
    # class but class access to attribute is not supported.

    # The names are defined when the class is constructed
    _pytypeName = None
    _assemblerClassName = None

    # The types are created on demand and cached
    # We set a default assembler so that a class is guaranteed to support
    # something.
    _pytype = None
    _assembler = CompositeAssembler

    @property
    def components(self):
        """Component names mapped to associated `StorageClass`
        """
        return type(self)._components

    @property
    def pytype(self):
        """Python type associated with this `StorageClass`."""
        cls = type(self)
        if cls._pytype is not None:
            return cls._pytype
        # Handle case where we did get a python type not string
        if not isinstance(cls._pytypeName, str):
            pytype = cls._pytypeName
            cls._pytypeName = cls._pytypeName.__name__
        elif hasattr(builtins, cls._pytypeName):
            pytype = getattr(builtins, cls._pytypeName)
        else:
            pytype = doImport(cls._pytypeName)
        cls._pytype = pytype
        return cls._pytype

    @property
    def assemblerClass(self):
        """Class to use to (dis)assemble an object from components."""
        cls = type(self)
        if cls._assembler is not None:
            return cls._assembler
        if cls._assemblerClassName is None:
            return None
        cls._assembler = doImport(cls._assemblerClassName)
        return cls._assembler

    def assembler(self):
        """Return an instance of an assembler.

        Returns
        -------
        assembler : `CompositeAssembler`
            Instance of the assembler associated with this `StorageClass`
        """
        return self.assemblerClass()

    def validateInstance(self, instance):
        """Check that the supplied instance has the expected Python type

        Parameters
        ----------
        instance : `object`
            Object to check.

        Returns
        -------
        isOk : `bool`
            True is the supplied instance object can be handled by this
            `StorageClass`, False otherwise.
        """
        return isinstance(instance, self.pytype)


def makeNewStorageClass(name, pytype=None, components=None, assembler=None):
    """Create a new Python class as a subclass of `StorageClass`.

    Parameters
    ----------
    name : `str`
        Name to use for this class.
    pytype : `type`
        Python type (or name of type) to associate with the `StorageClass`
    components : `dict`, optional
        `dict` mapping name of a component to another `StorageClass`.
    assembler : `str`, optional
        Fully qualified name of class supporting assembly and disassembly
        of a `pytype` instance.

    Returns
    -------
    newtype : `StorageClass`
        Newly created Python type.
    """
    clsargs = {"name": name,
               "_pytypeName": pytype,
               "components": components}
    # if the assembler is not None also set it and clear the default assembler
    if assembler is not None:
        clsargs["_assemblerClassName"] = assembler
        clsargs["_assembler"] = None

    return type(name, (StorageClass,), clsargs)


class StorageClassFactory:
    """Factory for `StorageClass` instances.
    """

    def __init__(self):
        self._mappingFactory = MappingFactory(StorageClass)

    def getStorageClass(self, storageClassName):
        """Get a StorageClass instance associated with the supplied name.

        Parameter
        ---------
        storageClassName : `str`
            Name of the storage class to retrieve.

        Returns
        -------
        instance : `StorageClass`
            Instance of the correct `StorageClass`.
        """
        return self._mappingFactory.getFromRegistry(storageClassName)

    def registerStorageClass(self, storageClass):
        """Store the `StorageClass` in the factory.

        Will be indexed by `StorageClass.name` and will return instances
        of the supplied `StorageClass`.

        Parameters
        ----------
        storageClass: `StorageClass`
            Type of the Python `StorageClass` to register.

        Raises
        ------
        e : `KeyError`
            If a storage class has already been registered with
            storageClassName.
        """
        self._mappingFactory.placeInRegistry(storageClass.name, storageClass)

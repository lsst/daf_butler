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


class StorageClassMeta(type):

    """Metaclass used by `StorageClass`.

    Implements lazy loading of class attributes, allowing datastores to
    delay loading of external code until it is needed.

    Attributes
    ----------
    pytype
    assembler

    """

    # Caches of imported code objects.
    _pytype = None
    _assembler = CompositeAssembler

    def __init__(self, name, bases, dct):
        super().__init__(name, bases, dct)
        if hasattr(self, "name"):
            StorageClass.subclasses[self.name] = self

    @property
    def pytype(cls):  # noqa N805
        """Python type associated with this `StorageClass`."""
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
    def assembler(cls):  # noqa N805
        """Function to use to assemble an object from components."""
        if cls._assembler is not None:
            return cls._assembler
        if cls._assemblerClassName is None:
            return None
        cls._assembler = doImport(cls._assemblerClassName)
        return cls._assembler


class StorageClass(metaclass=StorageClassMeta):
    """Class describing how a label maps to a particular Python type.

    Attributes
    ----------
    name : `str`
        Name associated with the StorageClass.
    pytype : `type`
        Python type associated with this name.
    components : `dict`
        Dict mapping component names of a composite to an associated
        `StorageClass`.
    assembler : `function`, optional
        Class to use to assemble and disassemble an object of type `pytype`
        from components.
    """

    subclasses = dict()
    components = dict()

    # These are internal class attributes supporting lazy loading of concrete
    # python types and functions from the string. The lazy loading is handled
    # in the metaclass.
    _pytypeName = None
    _assemblerClassName = None

    @property
    def pytype(self):
        """Python type associated with this StorageClass."""
        return type(self).pytype

    @property
    def assembler(self):
        """Function object to use to create a type from components."""
        return type(self).assembler

    @classmethod
    def validateInstance(cls, instance):
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
        return isinstance(instance, cls.pytype)


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

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

from .mappingFactory import MappingFactory


class StorageClassMeta(type):

    """Metaclass used by `StorageClass`.

    Implements lazy loading of class attributes, allowing datastores to
    delay loading of external code until it is needed.

    Attributes
    ----------
    pytype
    assembler
    disassembler

    """

    # Caches of imported code objects.
    _pytype = None
    _assembler = None
    _disassembler = None

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
        if cls._assemblerName is None:
            return None
        cls._assembler = doImport(cls._assemblerName)
        return cls._assembler

    @property
    def disassembler(cls):  # noqa N805
        """Function to use to split an object into components."""
        if cls._disassembler is not None:
            return cls._disassembler
        if cls._disassemblerName is None:
            return None
        cls._disassembler = doImport(cls._disassemblerName)
        return cls._disassembler


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
        Function to call to assemble an object of type `pytype` from
        components.
    disassembler : `function`, optional
        Function to call to disassemble an object of type `pytype` into
        its components.
        Takes a Python instance object to be disassembled and the
        `StorageClass` being used.

    """

    subclasses = dict()
    components = dict()

    # These are internal class attributes supporting lazy loading of concrete
    # python types and functions from the string. The lazy loading is handled
    # in the metaclass.
    _pytypeName = None
    _assemblerName = None
    _disassemblerName = None

    @property
    def pytype(self):
        """Python type associated with this StorageClass."""
        return type(self).pytype

    @property
    def assembler(self):
        """Function object to use to create a type from components."""
        return type(self).assembler

    @property
    def disassembler(self):
        """Function object to use to disassembler a type into components."""
        return type(self).disassembler

    @classmethod
    def assemble(cls, parent, components):
        return parent


def makeNewStorageClass(name, pytype=None, components=None, assembler=None, disassembler=None):
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
        Fully qualified name of function to join components together to form a
        `pytype` instance. Takes a dict with values being instances of objects
        to be used to contruct `pytype`. Keys of the dict map those defined
        in `components`.
    disassembler : `str`, optional
        Fully qualified name of function to split `pytype` instance into
        components. Takes a Python instance object to be disassembled and the
        `StorageClass` being used.
        Returns dict with keys matching the entries in `components`
        and values being the component instances extracted from `pytype`.


    Returns
    -------
    newtype : `StorageClass`
        Newly created Python type.
    """
    return type(name, (StorageClass,), {"name": name,
                                        "_pytypeName": pytype,
                                        "_assemblerName": assembler,
                                        "_disassemblerName": disassembler,
                                        "components": components})


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

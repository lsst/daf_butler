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

from lsst.daf.butler.core.utils import doImport

from .mappingFactory import MappingFactory


class StorageClassMeta(type):

    # Caches of imported code objects.
    _pytype = None

    def __init__(self, name, bases, dct):
        super().__init__(name, bases, dct)
        if hasattr(self, "name"):
            StorageClass.subclasses[self.name] = self

    @property
    def pytype(cls):
        if cls._pytype is not None:
            return cls._pytype
        # Handle case where we did get a python type not string
        if not isinstance(cls._pytypeName, str):
            pytype = cls._pytypeName
            cls._pytypeName = cls._pytypeName.__name__
        elif cls._pytypeName == "dict":
            pytype = dict
        elif cls._pytypeName == "list":
            pytype = list
        else:
            pytype = doImport(cls._pytypeName)
        cls._pytype = pytype
        return cls._pytype


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

    """

    subclasses = dict()
    components = dict()

    # These are internal class attributes supporting lazy loading of concrete
    # python types and functions from the string. The lazy loading is handled
    # in the metaclass.
    _pytypeName = None

    @property
    def pytype(self):
        """Python type associated with this StorageClass."""
        return type(self).pytype

    @classmethod
    def assemble(cls, parent, components):
        return parent


def makeNewStorageClass(name, pytype=None, components=None):
    """Create a new Python class as a subclass of `StorageClass`.

    parameters
    ----------
    name : `str`
        Name to use for this class.
    pytype : `type`
        Python type (or name of type) to associate with the `StorageClass`
    components : `dict`, optional
        `dict` mapping name of a component to another `StorageClass`.

    Returns
    -------
    newtype : `StorageClass`
        Newly created Python type.
    """
    return type(name, (StorageClass,), {"name": name,
                                        "_pytypeName": pytype,
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

    def registerStorageClass(self, storageClassName, classAttr):
        """Create a `StorageClass` subclass with the supplied properties
        and associate it with the storageClassName in the registry.

        Parameters
        ----------
        storageClassName : `str`
            Name of new storage class to be created.
        classAttr : `dict`
            Dict containing StorageClass parameters. Supported keys:
            - pytype: `str`
                Name of python type associated with this StorageClass
            - components: `dict`
                Map of storageClassName to `storageClass` for components.

        Raises
        ------
        e : `KeyError`
            If a storage class has already been registered with
            storageClassName.
        """
        newtype = makeNewStorageClass(storageClassName, **classAttr)
        self._mappingFactory.placeInRegistry(storageClassName, newtype)

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

    def __init__(self, name, bases, dct):
        super().__init__(name, bases, dct)
        if hasattr(self, "name"):
            StorageClass.subclasses[self.name] = self


class StorageClass(metaclass=StorageClassMeta):

    subclasses = dict()

    components = dict()

    @classmethod
    def assemble(cls, parent, components):
        return parent


def makeNewStorageClass(name, pytype, components=None):
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
    if isinstance(pytype, str):
        # Special case Python native dict type for testing
        if pytype == "dict":
            pytype = dict
        else:
            pytype = doImport(pytype)
    return type(name, (StorageClass,), {"name": name,
                                        "type": pytype,
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

    def registerStorageClass(self, storageClassName, pytype, components=None):
        """Create a `StorageClass` subclass with the supplied properties
        and associate it with the storageClassName in the registry.

        Parameters
        ----------
        storageClassName : `str`
            Name of new storage class to be created.
        pytype : `str` or Python class.
            Python type to be associated with this storage class.
        components : `dict`
            Map of storageClassName to `storageClass` for components.

        Raises
        ------
        e : `KeyError`
            If a storage class has already been registered with
            storageClassName.
        """
        newtype = makeNewStorageClass(storageClassName, pytype, components)
        self._mappingFactory.placeInRegistry(storageClassName, newtype)

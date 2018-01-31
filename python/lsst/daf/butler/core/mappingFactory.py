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
from lsst.daf.persistence import doImport


class MappingFactory:
    """
    Register the mapping of some key to a python type and retrieve instances.

    Enables instances of these classes to be retrieved from the factory later.
    The class can be specified as an object, class or string.
    If the key is an object it is converted to a string by accessing
    a `name` attribute.
    """

    refType = None
    """Type of instances expected to be returned from factory."""

    def __init__(self):
        self._registry = {}

    def getFromRegistry(self, targetClass, override=None):
        """Get a new instance of the object stored in the registry.

        Parameters
        ----------
        targetClass : `str` or object supporting `.name` attribute
            Get item from registry associated with this target class, unless
        override : `str` or object supporting `.name` attribute, optional
            If given, look if an override has been specified for this and,
            if so return that instead.

        Returns
        -------
        instance : `object`
            Instance of class stored in registry associated with the target.
        """
        for t in (override, targetClass):
            if t is not None:
                try:
                    typeName = self._registry[self._getName(t)]
                except KeyError:
                    pass
                else:
                    return self._getInstanceOf(typeName)
        raise KeyError("Unable to find item in registry with keys {} or {}".format(targetClass, override))

    def placeInRegistry(self, registryKey, typeName):
        """Register a class name with the associated type.
        The type name provided is validated against the reference
        class, `refType`, if defined.

        Parameters
        ----------
        registryKey : `str` or object supporting `.name` attribute.
            Item to associate with the provided type.
        typeName : `str` or Python type
            Identifies a class to associate with the provided key.

        Raises
        ------
        e : `ValueError`
            If instance of class is not of the expected type.
        """
        if not self._isValidStr(typeName):
            raise ValueError("Not a valid class string: {}".format(typeName))
        keyString = self._getName(registryKey)
        if keyString in self._registry:
            raise ValueError("Item with key {} already registered".format(keyString))

        self._registry[keyString] = typeName

    @staticmethod
    def _getName(typeOrName):
        """Extract name of supplied object as string.

        Parameters
        ----------
        typeOrName : `str` or object supporting `.name` attribute.
            Item from which to extract a name.

        Returns
        -------
        name : `str`
            Extracted name as a string.
        """
        if isinstance(typeOrName, str):
            return typeOrName
        elif hasattr(typeOrName, 'name'):
            return typeOrName.name
        else:
            raise ValueError("Cannot extract name from type")

    @staticmethod
    def _getInstanceOf(typeOrName):
        """Given the type name or a type, instantiate an object of that type.

        If a type name is given, an attempt will be made to import the type.

        Parameters
        ----------
        typeOrName : `str` or Python class
            A string describing the Python class to load or a Python type.
        """
        if isinstance(typeOrName, str):
            cls = doImport(typeOrName)
        else:
            cls = typeOrName
        return cls()

    @classmethod
    def _isValidStr(cls, typeName):
        """Validate that the class name provided does create instances of
        objects that are of the expected type, as stored in the class
        `refType` attribute.
        """
        if cls.refType is None:
            return True
        try:
            c = cls._getInstanceOf(typeName)
        except (ImportError, TypeError, AttributeError):
            return False
        else:
            return isinstance(c, cls.refType)

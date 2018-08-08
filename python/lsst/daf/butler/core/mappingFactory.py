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

from .utils import getInstanceOf

__all__ = ("MappingFactory", )


class MappingFactory:
    """
    Register the mapping of some key to a python type and retrieve instances.

    Enables instances of these classes to be retrieved from the factory later.
    The class can be specified as an object, class or string.
    If the key is an object it is converted to a string by accessing
    a ``name`` attribute.

    Parameters
    ----------
    refType : `type`
        Python reference `type` to use to ensure that items stored in the
        registry create instance objects of the correct class. Subclasses
        of this type are allowed. Using `None` disables the check.

    """

    def __init__(self, refType):
        self._registry = {}
        self.refType = refType

    def getFromRegistry(self, *targetClasses):
        """Get a new instance of the object stored in the registry.

        Parameters
        ----------
        *targetClasses : `str` or objects supporting ``name`` attribute
            Each item is tested in turn until a match is found in the registry.
            Items with `None` value are skipped.

        Returns
        -------
        instance : `object`
            Instance of class stored in registry associated with the first
            matching target class.

        Raises
        ------
        KeyError
            None of the supplied target classes match an item in the registry.
        """
        attempts = []
        for t in (targetClasses):
            if t is None:
                attempts.append(t)
            else:
                key = self._getName(t)
                attempts.append(key)
                try:
                    typeName = self._registry[key]
                except KeyError:
                    pass
                else:
                    return getInstanceOf(typeName)
        raise KeyError("Unable to find item in registry with keys: {}".format(attempts))

    def placeInRegistry(self, registryKey, typeName):
        """Register a class name with the associated type.

        The type name provided is validated against the reference
        class, `refType` attribute, if defined.

        Parameters
        ----------
        registryKey : `str` or object supporting ``name`` attribute.
            Item to associate with the provided type.
        typeName : `str` or Python type
            Identifies a class to associate with the provided key.

        Raises
        ------
        ValueError
            If instance of class is not of the expected type.
        KeyError
            If item is already registered and has different value.
        """
        if not self._isValidStr(typeName):
            raise ValueError("Not a valid class string: {}".format(typeName))
        keyString = self._getName(registryKey)
        if keyString in self._registry:
            # Compare the class strings since dynamic classes can be the
            # same thing but be different.
            if str(self._registry[keyString]) == str(typeName):
                return

            raise KeyError("Item with key {} already registered with different value"
                           " ({} != {})".format(keyString, self._registry[keyString], typeName))

        self._registry[keyString] = typeName

    @staticmethod
    def _getName(typeOrName):
        """Extract name of supplied object as string.

        Parameters
        ----------
        typeOrName : `str` or object supporting ``name`` attribute.
            Item from which to extract a name.

        Returns
        -------
        name : `str`
            Extracted name as a string.
        """
        if isinstance(typeOrName, str):
            return typeOrName
        elif hasattr(typeOrName, "name"):
            return typeOrName.name
        else:
            raise ValueError("Cannot extract name from type")

    def _isValidStr(self, typeName):
        """Validate that the class type name provided does create instances of
        objects that are of the expected type, as stored in the `refType`
        attribute.
        """
        if self.refType is None:
            return True
        try:
            c = getInstanceOf(typeName)
        except (ImportError, TypeError, AttributeError):
            return False
        else:
            return isinstance(c, self.refType)

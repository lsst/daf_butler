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

from __future__ import annotations

__all__ = ("MappingFactory",)

from typing import Any, Dict, Iterable, List, Set, Tuple, Type, Union

from lsst.utils.introspection import get_class_of

from .config import Config
from .configSupport import LookupKey


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

    def __init__(self, refType: Type):
        self._registry: Dict[LookupKey, Dict[str, Any]] = {}
        self.refType = refType

    def __contains__(self, key: Any) -> bool:
        """Indicate whether the supplied key is present in the factory.

        Parameters
        ----------
        key : `LookupKey`, `str` or objects with ``name`` attribute
            Key to use to lookup whether a corresponding element exists
            in this factory.

        Returns
        -------
        in : `bool`
            `True` if the supplied key is present in the factory.
        """
        key = self._getNameKey(key)
        return key in self._registry

    def getLookupKeys(self) -> Set[LookupKey]:
        """Retrieve the look up keys for all the registry entries.

        Returns
        -------
        keys : `set` of `LookupKey`
            The keys available for matching in the registry.
        """
        return set(self._registry)

    def getClassFromRegistryWithMatch(
        self, targetClasses: Iterable[Any]
    ) -> Tuple[LookupKey, Type, Dict[Any, Any]]:
        """Get the class stored in the registry along with the matching key.

        Parameters
        ----------
        targetClasses : `LookupKey`, `str` or objects with ``name`` attribute
            Each item is tested in turn until a match is found in the registry.
            Items with `None` value are skipped.

        Returns
        -------
        matchKey : `LookupKey`
            The key that resulted in the successful match.
        cls : `type`
            Class stored in registry associated with the first
            matching target class.
        kwargs: `dict`
            Keyword arguments to be given to constructor.

        Raises
        ------
        KeyError
            Raised if none of the supplied target classes match an item in the
            registry.
        """
        attempts: List[Any] = []
        for t in targetClasses:
            if t is None:
                attempts.append(t)
            else:
                key = self._getNameKey(t)
                attempts.append(key)
                try:
                    entry = self._registry[key]
                except KeyError:
                    pass
                else:
                    return key, get_class_of(entry["type"]), entry["kwargs"]

        # Convert list to a string for error reporting
        msg = ", ".join(str(k) for k in attempts)
        plural = "" if len(attempts) == 1 else "s"
        raise KeyError(f"Unable to find item in registry with key{plural}: {msg}")

    def getClassFromRegistry(self, targetClasses: Iterable[Any]) -> Type:
        """Get the matching class stored in the registry.

        Parameters
        ----------
        targetClasses : `LookupKey`, `str` or objects with ``name`` attribute
            Each item is tested in turn until a match is found in the registry.
            Items with `None` value are skipped.

        Returns
        -------
        cls : `type`
            Class stored in registry associated with the first
            matching target class.

        Raises
        ------
        KeyError
            Raised if none of the supplied target classes match an item in the
            registry.
        """
        _, cls, _ = self.getClassFromRegistryWithMatch(targetClasses)
        return cls

    def getFromRegistryWithMatch(
        self, targetClasses: Iterable[Any], *args: Any, **kwargs: Any
    ) -> Tuple[LookupKey, Any]:
        """Get a new instance of the registry object along with matching key.

        Parameters
        ----------
        targetClasses : `LookupKey`, `str` or objects with ``name`` attribute
            Each item is tested in turn until a match is found in the registry.
            Items with `None` value are skipped.
        args : `tuple`
            Positional arguments to use pass to the object constructor.
        **kwargs
            Keyword arguments to pass to object constructor.

        Returns
        -------
        matchKey : `LookupKey`
            The key that resulted in the successful match.
        instance : `object`
            Instance of class stored in registry associated with the first
            matching target class.

        Raises
        ------
        KeyError
            Raised if none of the supplied target classes match an item in the
            registry.
        """
        key, cls, registry_kwargs = self.getClassFromRegistryWithMatch(targetClasses)

        # Supplied keyword args must overwrite registry defaults
        # We want this overwriting to happen recursively since we expect
        # some of these keyword arguments to be dicts.
        # Simplest to use Config for this
        config_kwargs = Config(registry_kwargs)
        config_kwargs.update(kwargs)
        merged_kwargs = config_kwargs.toDict()

        return key, cls(*args, **merged_kwargs)

    def getFromRegistry(self, targetClasses: Iterable[Any], *args: Any, **kwargs: Any) -> Any:
        """Get a new instance of the object stored in the registry.

        Parameters
        ----------
        targetClasses : `LookupKey`, `str` or objects with ``name`` attribute
            Each item is tested in turn until a match is found in the registry.
            Items with `None` value are skipped.
        args : `tuple`
            Positional arguments to use pass to the object constructor.
        **kwargs
            Keyword arguments to pass to object constructor.

        Returns
        -------
        instance : `object`
            Instance of class stored in registry associated with the first
            matching target class.

        Raises
        ------
        KeyError
            Raised if none of the supplied target classes match an item in the
            registry.
        """
        _, instance = self.getFromRegistryWithMatch(targetClasses, *args, **kwargs)
        return instance

    def placeInRegistry(
        self, registryKey: Any, typeName: Union[str, Type], overwrite: bool = False, **kwargs: Any
    ) -> None:
        """Register a class name with the associated type.

        Parameters
        ----------
        registryKey : `LookupKey`, `str` or object with ``name`` attribute.
            Item to associate with the provided type.
        typeName : `str` or Python type
            Identifies a class to associate with the provided key.
        overwrite : `bool`, optional
            If `True`, an existing entry will be overwritten.  This option
            is expected to be used to simplify test suites.
            Default is `False`.
        **kwargs
            Keyword arguments to always pass to object constructor when
            retrieved.

        Raises
        ------
        KeyError
            Raised if item is already registered and has different value and
            ``overwrite`` is `False`.
        """
        key = self._getNameKey(registryKey)
        if key in self._registry and not overwrite:
            # Compare the class strings since dynamic classes can be the
            # same thing but be different.
            if str(self._registry[key]) == str(typeName):
                return

            raise KeyError(
                "Item with key {} already registered with different value"
                " ({} != {})".format(key, self._registry[key], typeName)
            )

        self._registry[key] = {
            "type": typeName,
            "kwargs": dict(**kwargs),
        }

    @staticmethod
    def _getNameKey(typeOrName: Any) -> LookupKey:
        """Extract name of supplied object as entity suitable for key use.

        Parameters
        ----------
        typeOrName : `LookupKey, `str` or object supporting ``name`` attribute.
            Item from which to extract a name.

        Returns
        -------
        name : `LookupKey`
            Extracted name as a string or
        """
        if isinstance(typeOrName, LookupKey):
            return typeOrName

        if isinstance(typeOrName, str):
            name = typeOrName
        elif hasattr(typeOrName, "name"):
            name = typeOrName.name
        else:
            raise ValueError("Cannot extract name from type")

        return LookupKey(name=name)

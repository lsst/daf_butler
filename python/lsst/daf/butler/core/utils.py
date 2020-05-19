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

__all__ = (
    "allSlots",
    "getClassOf",
    "getFullTypeName",
    "getInstanceOf",
    "immutable",
    "iterable",
    "Singleton",
    "stripIfNotNone",
    "transactional",
)

import builtins
import functools
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    Mapping,
    Optional,
    Type,
    Union,
)

from lsst.utils import doImport


def iterable(a: Any) -> Iterable[Any]:
    """Make input iterable.

    There are three cases, when the input is:

    - iterable, but not a `str`  or Mapping -> iterate over elements
      (e.g. ``[i for i in a]``)
    - a `str` -> return single element iterable (e.g. ``[a]``)
    - a Mapping -> return single element iterable
    - not iterable -> return single elment iterable (e.g. ``[a]``).

    Parameters
    ----------
    a : iterable or `str` or not iterable
        Argument to be converted to an iterable.

    Returns
    -------
    i : `generator`
        Iterable version of the input value.
    """
    if isinstance(a, str):
        yield a
        return
    if isinstance(a, Mapping):
        yield a
        return
    try:
        yield from a
    except Exception:
        yield a


def allSlots(self: Any) -> Iterator[str]:
    """
    Return combined ``__slots__`` for all classes in objects mro.

    Parameters
    ----------
    self : `object`
        Instance to be inspected.

    Returns
    -------
    slots : `itertools.chain`
        All the slots as an iterable.
    """
    from itertools import chain
    return chain.from_iterable(getattr(cls, "__slots__", []) for cls in self.__class__.__mro__)


def getFullTypeName(cls: Any) -> str:
    """Return full type name of the supplied entity.

    Parameters
    ----------
    cls : `type` or `object`
        Entity from which to obtain the full name. Can be an instance
        or a `type`.

    Returns
    -------
    name : `str`
        Full name of type.

    Notes
    -----
    Builtins are returned without the ``builtins`` specifier included.  This
    allows `str` to be returned as "str" rather than "builtins.str".
    """
    # If we have an instance we need to convert to a type
    if not hasattr(cls, "__qualname__"):
        cls = type(cls)
    if hasattr(builtins, cls.__qualname__):
        # Special case builtins such as str and dict
        return cls.__qualname__
    return cls.__module__ + "." + cls.__qualname__


def getClassOf(typeOrName: Union[Type, str]) -> Type:
    """Given the type name or a type, return the python type.

    If a type name is given, an attempt will be made to import the type.

    Parameters
    ----------
    typeOrName : `str` or Python class
        A string describing the Python class to load or a Python type.

    Returns
    -------
    type_ : `type`
        Directly returns the Python type if a type was provided, else
        tries to import the given string and returns the resulting type.

    Notes
    -----
    This is a thin wrapper around `~lsst.utils.doImport`.
    """
    if isinstance(typeOrName, str):
        cls = doImport(typeOrName)
    else:
        cls = typeOrName
    return cls


def getInstanceOf(typeOrName: Union[Type, str], *args: Any, **kwargs: Any) -> Any:
    """Given the type name or a type, instantiate an object of that type.

    If a type name is given, an attempt will be made to import the type.

    Parameters
    ----------
    typeOrName : `str` or Python class
        A string describing the Python class to load or a Python type.
    args : `tuple`
        Positional arguments to use pass to the object constructor.
    kwargs : `dict`
        Keyword arguments to pass to object constructor.

    Returns
    -------
    instance : `object`
        Instance of the requested type, instantiated with the provided
        parameters.
    """
    cls = getClassOf(typeOrName)
    return cls(*args, **kwargs)


class Singleton(type):
    """Metaclass to convert a class to a Singleton.

    If this metaclass is used the constructor for the singleton class must
    take no arguments. This is because a singleton class will only accept
    the arguments the first time an instance is instantiated.
    Therefore since you do not know if the constructor has been called yet it
    is safer to always call it with no arguments and then call a method to
    adjust state of the singleton.
    """

    _instances: Dict[Type, Any] = {}

    # Signature is intentionally not substitutable for type.__call__ (no *args,
    # **kwargs) to require classes that use this metaclass to have no
    # constructor arguments.
    def __call__(cls) -> Any:  # type: ignore
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__()
        return cls._instances[cls]


def transactional(func: Callable) -> Callable:
    """Decorator that wraps a method and makes it transactional.

    This depends on the class also defining a `transaction` method
    that takes no arguments and acts as a context manager.
    """
    @functools.wraps(func)
    def inner(self: Any, *args: Any, **kwargs: Any) -> Any:
        with self.transaction():
            return func(self, *args, **kwargs)
    return inner


def stripIfNotNone(s: Optional[str]) -> Optional[str]:
    """Strip leading and trailing whitespace if the given object is not None.

    Parameters
    ----------
    s : `str`, optional
        Input string.

    Returns
    -------
    r : `str` or `None`
        A string with leading and trailing whitespace stripped if `s` is not
        `None`, or `None` if `s` is `None`.
    """
    if s is not None:
        s = s.strip()
    return s


def immutable(cls: Type) -> Type:
    """A class decorator that simulates a simple form of immutability for
    the decorated class.

    A class decorated as `immutable` may only set each of its attributes once
    (by convention, in ``__new__``); any attempts to set an already-set
    attribute will raise `AttributeError`.

    Because this behavior interferes with the default implementation for
    the ``pickle`` and ``copy`` modules, `immutable` provides implementations
    of ``__getstate__`` and ``__setstate__`` that override this behavior.
    Immutable classes can them implement pickle/copy via ``__getnewargs__``
    only (other approaches such as ``__reduce__`` and ``__deepcopy__`` may
    also be used).
    """
    def __setattr__(self: Any, name: str, value: Any) -> None:  # noqa: N807
        if hasattr(self, name):
            raise AttributeError(f"{cls.__name__} instances are immutable.")
        object.__setattr__(self, name, value)
    # mypy says the variable here has signature (str, Any) i.e. no "self";
    # I think it's just confused by descriptor stuff.
    cls.__setattr__ = __setattr__  # type: ignore

    def __getstate__(self: Any) -> dict:  # noqa: N807
        # Disable default state-setting when unpickled.
        return {}
    cls.__getstate__ = __getstate__

    def __setstate__(self: Any, state: Any) -> None:  # noqa: N807
        # Disable default state-setting when copied.
        # Sadly what works for pickle doesn't work for copy.
        assert not state
    cls.__setstate__ = __setstate__
    return cls

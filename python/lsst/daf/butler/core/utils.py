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
    "safeMakeDir",
    "Singleton",
    "stripIfNotNone",
    "transactional",
)

import errno
import os
import builtins
import fnmatch
import functools
import logging
import re
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Pattern,
    Type,
    TypeVar,
    TYPE_CHECKING,
    Union,
)

from lsst.utils import doImport

if TYPE_CHECKING:
    from ..registry.wildcards import Ellipsis, EllipsisType


_LOG = logging.getLogger(__name__)


def safeMakeDir(directory: str) -> None:
    """Make a directory in a manner avoiding race conditions."""
    if directory != "" and not os.path.exists(directory):
        try:
            os.makedirs(directory)
        except OSError as e:
            # Don't fail if directory exists due to race
            if e.errno != errno.EEXIST:
                raise e


def iterable(a: Any) -> Iterable[Any]:
    """Make input iterable.

    There are three cases, when the input is:

    - iterable, but not a `str`  or Mapping -> iterate over elements
      (e.g. ``[i for i in a]``)
    - a `str` -> return single element iterable (e.g. ``[a]``)
    - a Mapping -> return single element iterable
    - not iterable -> return single element iterable (e.g. ``[a]``).

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
    allows `str` to be returned as "str" rather than "builtins.str". Any
    parts of the path that start with a leading underscore are removed
    on the assumption that they are an implementation detail and the
    entity will be hoisted into the parent namespace.
    """
    # If we have an instance we need to convert to a type
    if not hasattr(cls, "__qualname__"):
        cls = type(cls)
    if hasattr(builtins, cls.__qualname__):
        # Special case builtins such as str and dict
        return cls.__qualname__

    real_name = cls.__module__ + "." + cls.__qualname__

    # Remove components with leading underscores
    cleaned_name = ".".join(c for c in real_name.split(".") if not c.startswith("_"))

    # Consistency check
    if real_name != cleaned_name:
        try:
            test = doImport(cleaned_name)
        except Exception:
            # Could not import anything so return the real name
            return real_name

        # The thing we imported should match the class we started with
        # despite the clean up. If it does not we return the real name
        if test is not cls:
            return real_name

    return cleaned_name


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


F = TypeVar("F", bound=Callable)


def transactional(func: F) -> F:
    """Decorate a method and makes it transactional.

    This depends on the class also defining a `transaction` method
    that takes no arguments and acts as a context manager.
    """
    @functools.wraps(func)
    def inner(self: Any, *args: Any, **kwargs: Any) -> Any:
        with self.transaction():
            return func(self, *args, **kwargs)
    return inner  # type: ignore


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


_T = TypeVar("_T", bound="Type")


def immutable(cls: _T) -> _T:
    """Decorate a class to simulates a simple form of immutability.

    A class decorated as `immutable` may only set each of its attributes once;
    any attempts to set an already-set attribute will raise `AttributeError`.

    Notes
    -----
    Subclasses of classes marked with ``@immutable`` are also immutable.

    Because this behavior interferes with the default implementation for the
    ``pickle`` modules, `immutable` provides implementations of
    ``__getstate__`` and ``__setstate__`` that override this behavior.
    Immutable classes can then implement pickle via ``__reduce__`` or
    ``__getnewargs__``.

    Following the example of Python's built-in immutable types, such as `str`
    and `tuple`, the `immutable` decorator provides a ``__copy__``
    implementation that just returns ``self``, because there is no reason to
    actually copy an object if none of its shared owners can modify it.

    Similarly, objects that are recursively (i.e. are themselves immutable and
    have only recursively immutable attributes) should also reimplement
    ``__deepcopy__`` to return ``self``.  This is not done by the decorator, as
    it has no way of checking for recursive immutability.
    """
    def __setattr__(self: _T, name: str, value: Any) -> None:  # noqa: N807
        if hasattr(self, name):
            raise AttributeError(f"{cls.__name__} instances are immutable.")
        object.__setattr__(self, name, value)
    # mypy says the variable here has signature (str, Any) i.e. no "self";
    # I think it's just confused by descriptor stuff.
    cls.__setattr__ = __setattr__  # type: ignore

    def __getstate__(self: _T) -> dict:  # noqa: N807
        # Disable default state-setting when unpickled.
        return {}
    cls.__getstate__ = __getstate__

    def __setstate__(self: _T, state: Any) -> None:  # noqa: N807
        # Disable default state-setting when copied.
        # Sadly what works for pickle doesn't work for copy.
        assert not state
    cls.__setstate__ = __setstate__

    def __copy__(self: _T) -> _T:  # noqa: N807
        return self
    cls.__copy__ = __copy__
    return cls


_S = TypeVar("_S")
_R = TypeVar("_R")


def cached_getter(func: Callable[[_S], _R]) -> Callable[[_S], _R]:
    """Decorate a method to caches the result.

    Only works on methods that take only ``self``
    as an argument, and returns the cached result on subsequent calls.

    Notes
    -----
    This is intended primarily as a stopgap for Python 3.8's more sophisticated
    ``functools.cached_property``, but it is also explicitly compatible with
    the `immutable` decorator, which may not be true of ``cached_property``.

    `cached_getter` guarantees that the cached value will be stored in
    an attribute named ``_cached_{name-of-decorated-function}``.  Classes that
    use `cached_getter` are responsible for guaranteeing that this name is not
    otherwise used, and is included if ``__slots__`` is defined.
    """
    attribute = f"_cached_{func.__name__}"

    @functools.wraps(func)
    def inner(self: _S) -> _R:
        if not hasattr(self, attribute):
            object.__setattr__(self, attribute, func(self))
        return getattr(self, attribute)

    return inner


def findFileResources(values: Iterable[str], regex: Optional[str] = None) -> List[str]:
    """Scan the supplied directories and return all matching files.

    Get the files from a list of values. If a value is a file it is added to
    the list of returned files. If a value is a directory, all the files in
    the directory (recursively) that match the regex will be returned.

    Parameters
    ----------
    values : iterable [`str`]
        The files to return and directories in which to look for files to
        return.
    regex : `str`
        The regex to use when searching for files within directories. Optional,
        by default returns all the found files.

    Returns
    -------
    resources: `list` [`str`]
        The passed-in files and files found in passed-in directories.
    """
    fileRegex = None if regex is None else re.compile(regex)
    resources = []

    # Find all the files of interest
    for location in values:
        if os.path.isdir(location):
            for root, dirs, files in os.walk(location):
                for name in files:
                    path = os.path.join(root, name)
                    if os.path.isfile(path) and (fileRegex is None or fileRegex.search(name)):
                        resources.append(path)
        else:
            resources.append(location)
    return resources


def globToRegex(expressions: List[str]) -> Union[List[Pattern[str]], EllipsisType]:
    """Translate glob-style search terms to regex.

    If a stand-alone '*' is found in ``expressions``, or expressions is empty,
    then the special value ``...`` will be returned, indicating that any string
    will match.

    Parameters
    ----------
    expressions : `list` [`str`]
        A list of glob-style pattern strings to convert.

    Returns
    -------
    expressions : `list` [`str`] or ``...``
        A list of regex Patterns
    """
    if not expressions or "*" in expressions:
        return Ellipsis
    return [re.compile(fnmatch.translate(e)) for e in expressions]

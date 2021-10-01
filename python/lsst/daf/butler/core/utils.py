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
    "immutable",
    "safeMakeDir",
    "Singleton",
    "stripIfNotNone",
    "transactional",
)

import errno
import os
import fnmatch
import functools
import logging
import re
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Pattern,
    Type,
    TypeVar,
    TYPE_CHECKING,
    Union,
)

from lsst.utils.iteration import iterable

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


def globToRegex(expressions: Union[str, EllipsisType, None,
                                   List[str]]) -> Union[List[Union[str, Pattern]], EllipsisType]:
    """Translate glob-style search terms to regex.

    If a stand-alone '``*``' is found in ``expressions``, or expressions is
    empty or `None`, then the special value ``...`` will be returned,
    indicating that any string will match.

    Parameters
    ----------
    expressions : `str` or `list` [`str`]
        A list of glob-style pattern strings to convert.

    Returns
    -------
    expressions : `list` [`str` or `re.Pattern`] or ``...``
        A list of regex Patterns or simple strings. Returns ``...`` if
        the provided expressions would match everything.
    """
    if expressions is Ellipsis or expressions is None:
        return Ellipsis
    expressions = list(iterable(expressions))
    if not expressions or "*" in expressions:
        return Ellipsis

    nomagic = re.compile(r"^[\w/\.\-]+$", re.ASCII)

    # Try not to convert simple string to a regex.
    results: List[Union[str, Pattern]] = []
    for e in expressions:
        res: Union[str, Pattern]
        if nomagic.match(e):
            res = e
        else:
            res = re.compile(fnmatch.translate(e))
        results.append(res)
    return results

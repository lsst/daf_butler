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

__all__ = ("iterable", "allSlots", "slotValuesAreEqual", "slotValuesToHash",
           "getFullTypeName", "getInstanceOf", "Singleton", "transactional",
           "getObjectSize", "stripIfNotNone", "PrivateConstructorMeta",
           "NamedKeyDict", "NamedValueSet", "IndexedTupleDict",
           "immutable")

import builtins
import sys
import functools
from typing import (TypeVar, MutableMapping, Iterator, KeysView, ValuesView, ItemsView, Dict, Union,
                    MutableSet, Iterable, Mapping, Tuple)
from types import MappingProxyType

from lsst.utils import doImport


def iterable(a):
    """Make input iterable.

    There are three cases, when the input is:

    - iterable, but not a `str` -> iterate over elements
      (e.g. ``[i for i in a]``)
    - a `str` -> return single element iterable (e.g. ``[a]``)
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
    try:
        yield from a
    except Exception:
        yield a


def allSlots(self):
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


def slotValuesAreEqual(self, other):
    """
    Test for equality by the contents of all slots, including those of its
    parents.

    Parameters
    ----------
    self : `object`
        Reference instance.
    other : `object`
        Comparison instance.

    Returns
    -------
    equal : `bool`
        Returns True if all the slots are equal in both arguments.
    """
    return all((getattr(self, slot) == getattr(other, slot) for slot in allSlots(self)))


def slotValuesToHash(self):
    """
    Generate a hash from slot values.

    Parameters
    ----------
    self : `object`
        Instance to be hashed.

    Returns
    -------
    h : `int`
        Hashed value generated from the slot values.
    """
    return hash(tuple(getattr(self, slot) for slot in allSlots(self)))


def getFullTypeName(cls):
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


def getClassOf(typeOrName):
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


def getInstanceOf(typeOrName, *args, **kwargs):
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

    _instances = {}

    def __call__(cls):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__()
        return cls._instances[cls]


def transactional(func):
    """Decorator that wraps a method and makes it transactional.

    This depends on the class also defining a `transaction` method
    that takes no arguments and acts as a context manager.
    """
    @functools.wraps(func)
    def inner(self, *args, **kwargs):
        with self.transaction():
            return func(self, *args, **kwargs)
    return inner


def getObjectSize(obj, seen=None):
    """Recursively finds size of objects.

    Only works well for pure python objects. For example it does not work for
    ``Exposure`` objects where all the content is behind getter methods.

    Parameters
    ----------
    obj : `object`
       Instance for which size is to be calculated.
    seen : `set`, optional
       Used internally to keep track of objects already sized during
       recursion.

    Returns
    -------
    size : `int`
       Size in bytes.

    See Also
    --------
    sys.getsizeof

    Notes
    -----
    See https://goshippo.com/blog/measure-real-size-any-python-object/
    """
    size = sys.getsizeof(obj)
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    # Important mark as seen *before* entering recursion to gracefully handle
    # self-referential objects
    seen.add(obj_id)
    if isinstance(obj, dict):
        size += sum([getObjectSize(v, seen) for v in obj.values()])
        size += sum([getObjectSize(k, seen) for k in obj.keys()])
    elif hasattr(obj, "__dict__"):
        size += getObjectSize(obj.__dict__, seen)
    elif hasattr(obj, "__iter__") and not isinstance(obj, (str, bytes, bytearray)):
        size += sum([getObjectSize(i, seen) for i in obj])

    return size


def stripIfNotNone(s):
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


class PrivateConstructorMeta(type):
    """A metaclass that disables regular construction syntax.

    A class that uses PrivateConstructorMeta may have an ``__init__`` and/or
    ``__new__`` method, but these can't be invoked by "calling" the class
    (that will always raise `TypeError`).  Instead, such classes can be called
    by calling the metaclass-provided `_construct` class method with the same
    arguments.

    As is usual in Python, there are no actual prohibitions on what code can
    call `_construct`; the purpose of this metaclass is just to prevent
    instances from being created normally when that can't do what users would
    expect.

    ..note::

        Classes that inherit from PrivateConstructorMeta also inherit
        the hidden-constructor behavior.  If you just want to disable
        construction of the base class, `abc.ABCMeta` may be a better
        option.

    Examples
    --------
    Given this class definition::
        class Hidden(metaclass=PrivateConstructorMeta):

            def __init__(self, a, b):
                self.a = a
                self.b = b

    This doesn't work:

        >>> instance = Hidden(a=1, b="two")
        TypeError: Hidden objects cannot be constructed directly.

    But this does:

        >>> instance = Hidden._construct(a=1, b="two")

    """

    def __call__(cls, *args, **kwds):
        """Disabled class construction interface; always raises `TypeError.`
        """
        raise TypeError(f"{cls.__name__} objects cannot be constructed directly.")

    def _construct(cls, *args, **kwds):
        """Private class construction interface.

        All arguments are forwarded to ``__init__`` and/or ``__new__``
        in the usual way.
        """
        return type.__call__(cls, *args, **kwds)


K = TypeVar("K")
V = TypeVar("V")


class NamedKeyDict(MutableMapping[K, V]):
    """A dictionary wrapper that require keys to have a ``.name`` attribute,
    and permits lookups using either key objects or their names.

    Names can be used in place of keys when updating existing items, but not
    when adding new items.

    It is assumed (but asserted) that all name equality is equivalent to key
    equality, either because the key objects define equality this way, or
    because different objects with the same name are never included in the same
    dictionary.

    Parameters
    ----------
    args
        All positional constructor arguments are forwarded directly to `dict`.
        Keyword arguments are not accepted, because plain strings are not valid
        keys for `NamedKeyDict`.

    Raises
    ------
    AttributeError
        Raised when an attempt is made to add an object with no ``.name``
        attribute to the dictionary.
    AssertionError
        Raised when multiple keys have the same name.
    """

    __slots__ = ("_dict", "_names",)

    def __init__(self, *args):
        self._dict = dict(*args)
        self._names = {key.name: key for key in self._dict}
        assert len(self._names) == len(self._dict), "Duplicate names in keys."

    @property
    def names(self) -> KeysView[str]:
        """The set of names associated with the keys, in the same order
        (`~collections.abc.KeysView`).
        """
        return self._names.keys()

    def byName(self) -> Dict[str, V]:
        """Return a `dict` with names as keys and the same values as ``self``.
        """
        return dict(zip(self._names.keys(), self._dict.values()))

    def __len__(self) -> int:
        return len(self._dict)

    def __iter__(self) -> Iterator[K]:
        return iter(self._dict)

    def __str__(self) -> str:
        return "{{{}}}".format(", ".join(f"{str(k)}: {str(v)}" for k, v in self.items()))

    def __repr__(self) -> str:
        return "NamedKeyDict({{{}}})".format(", ".join(f"{repr(k)}: {repr(v)}" for k, v in self.items()))

    def __getitem__(self, key: Union[str, K]) -> V:
        if hasattr(key, "name"):
            return self._dict[key]
        else:
            return self._dict[self._names[key]]

    def __setitem__(self, key: Union[str, K], value: V):
        if hasattr(key, "name"):
            assert self._names.get(key.name, key) == key, "Name is already associated with a different key."
            self._dict[key] = value
            self._names[key.name] = key
        else:
            self._dict[self._names[key]] = value

    def __delitem__(self, key: Union[str, K]):
        if hasattr(key, "name"):
            del self._dict[key]
            del self._names[key.name]
        else:
            del self._dict[self._names[key]]
            del self._names[key]

    def keys(self) -> KeysView[K]:
        return self._dict.keys()

    def values(self) -> ValuesView[V]:
        return self._dict.values()

    def items(self) -> ItemsView[K, V]:
        return self._dict.items()

    def copy(self) -> NamedKeyDict[K, V]:
        result = NamedKeyDict.__new__(NamedKeyDict)
        result._dict = dict(self._dict)
        result._names = dict(self._names)
        return result

    def freeze(self):
        """Disable all mutators, effectively transforming ``self`` into
        an immutable mapping.
        """
        if not isinstance(self._dict, MappingProxyType):
            self._dict = MappingProxyType(self._dict)


T = TypeVar("T")


class NamedValueSet(MutableSet[T]):
    """A custom mutable set class that requires elements to have a ``.name``
    attribute, which can then be used as keys in `dict`-like lookup.

    Names and elements can both be used with the ``in`` and ``del``
    operators, `remove`, and `discard`.  Names (but not elements)
    can be used with ``[]``-based element retrieval (not assignment)
    and the `get` method.  `pop` can be used in either its `MutableSet`
    form (no arguments; an arbitrary element is returned) or its
    `MutableMapping` form (one or two arguments for the name and
    optional default value, respectively).

    Parameters
    ----------
    elements : `iterable`
        Iterable over elements to include in the set.

    Raises
    ------
    AttributeError
        Raised if one or more elements do not have a ``.name`` attribute.

    Notes
    -----
    Iteration order is guaranteed to be the same as insertion order (with
    the same general behavior as `dict` ordering).
    Like `dicts`, sets with the same elements will compare as equal even if
    their iterator order is not the same.
    """

    __slots__ = ("_dict",)

    def __init__(self, elements: Iterable[T] = ()):
        self._dict = {element.name: element for element in elements}

    @property
    def names(self) -> KeysView[str]:
        """The set of element names, in the same order
        (`~collections.abc.KeysView`).
        """
        return self._dict.keys()

    def asDict(self) -> Mapping[str, T]:
        """Return a mapping view with names as keys.

        Returns
        -------
        dict : `Mapping`
            A dictionary-like view with ``values() == self``.
        """
        return self._dict

    def __contains__(self, key: Union[str, T]) -> bool:
        return getattr(key, "name", key) in self._dict

    def __len__(self) -> int:
        return len(self._dict)

    def __iter__(self) -> Iterator[T]:
        return iter(self._dict.values())

    def __str__(self) -> str:
        return "{{{}}}".format(", ".join(str(element) for element in self))

    def __repr__(self) -> str:
        return "NamedValueSet({{{}}})".format(", ".join(repr(element) for element in self))

    def __eq__(self, other):
        try:
            return self._dict.keys() == other._dict.keys()
        except AttributeError:
            return NotImplemented

    def __hash__(self):
        return hash(frozenset(self._dict.keys()))

    # As per Set's docs, overriding just __le__ and __ge__ for performance will
    # cover the other comparisons, too.

    def __le__(self, other: NamedValueSet[T]) -> bool:
        try:
            return self._dict.keys() <= other._dict.keys()
        except AttributeError:
            return NotImplemented

    def __ge__(self, other: NamedValueSet[T]) -> bool:
        try:
            return self._dict.keys() >= other._dict.keys()
        except AttributeError:
            return NotImplemented

    def issubset(self, other):
        return self <= other

    def issuperset(self, other):
        return self >= other

    def __getitem__(self, name: str) -> T:
        return self._dict[name]

    def get(self, name: str, default=None):
        """Return the element with the given name, or ``default`` if
        no such element is present.
        """
        return self._dict.get(name, default)

    def __delitem__(self, name: str):
        del self._dict[name]

    def add(self, element: T):
        """Add an element to the set.

        Raises
        ------
        AttributeError
            Raised if the element does not have a ``.name`` attribute.
        """
        self._dict[element.name] = element

    def remove(self, element: Union[str, T]):
        """Remove an element from the set.

        Parameters
        ----------
        element : `object` or `str`
            Element to remove or the string name thereof.  Assumed to be an
            element if it has a ``.name`` attribute.

        Raises
        ------
        KeyError
            Raised if an element with the given name does not exist.
        """
        del self._dict[getattr(element, "name", element)]

    def discard(self, element: Union[str, T]):
        """Remove an element from the set if it exists.

        Does nothing if no matching element is present.

        Parameters
        ----------
        element : `object` or `str`
            Element to remove or the string name thereof.  Assumed to be an
            element if it has a ``.name`` attribute.
        """
        try:
            self.remove(element)
        except KeyError:
            pass

    def pop(self, *args):
        """Remove and return an element from the set.

        Parameters
        ----------
        name : `str`, optional
            Name of the element to remove and return.  Must be passed
            positionally.  If not provided, an arbitrary element is
            removed and returned.
        default : `object`, optional
            Value to return if ``name`` is provided but no such element
            exists.

        Raises
        ------
        KeyError
            Raised if ``name`` is provided but ``default`` is not, and no
            matching element exists.
        """
        if not args:
            return super().pop()
        else:
            return self._dict.pop(*args)

    def copy(self) -> NamedValueSet[T]:
        result = NamedValueSet.__new__(NamedValueSet)
        result._dict = dict(self._dict)
        return result

    def freeze(self):
        """Disable all mutators, effectively transforming ``self`` into
        an immutable set.
        """
        if not isinstance(self._dict, MappingProxyType):
            self._dict = MappingProxyType(self._dict)


class IndexedTupleDict(Mapping[K, V]):
    """An immutable mapping that combines a tuple of values with a (possibly
    shared) mapping from key to tuple index.

    Parameters
    ----------
    indices: `~collections.abc.Mapping`
        Mapping from key to integer index in the values tuple.  This mapping
        is used as-is, not copied or converted to a true `dict`, which means
        that the caller must guarantee that it will not be modified by other
        (shared) owners in the future.  If it is a `NamedKeyDict`, both names
        and key instances will be usable as keys in the `IndexedTupleDict`.
    values: `tuple`
        Tuple of values for the dictionary.
    """

    __slots__ = ("_indices", "_values")

    def __new__(cls, indices: Mapping[K, int], values: Tuple[V, ...]):
        self = super().__new__(cls)
        self._indices = indices
        self._values = values
        return self

    def __getitem__(self, key: K) -> V:
        return self._values[self._indices[key]]

    def __iter__(self) -> Iterator[K]:
        return iter(self._indices)

    def __len__(self) -> int:
        return len(self._indices)

    def __str__(self) -> str:
        return "{{{}}}".format(", ".join(f"{str(k)}: {str(v)}" for k, v in self.items()))

    def __repr__(self) -> str:
        return "IndexedTupleDict({{{}}})".format(", ".join(f"{repr(k)}: {repr(v)}" for k, v in self.items()))

    def __contains__(self, key: K) -> bool:
        return key in self._indices

    def keys(self) -> KeysView[K]:
        return self._indices.keys()

    def values(self) -> Tuple[V, ...]:
        return self._values

    # Let Mapping base class provide items(); we can't do it any more
    # efficiently ourselves.


def immutable(cls):
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
    def __setattr__(self, name, value):  # noqa N807
        if hasattr(self, name):
            raise AttributeError(f"{cls.__name__} instances are immutable.")
        object.__setattr__(self, name, value)
    cls.__setattr__ = __setattr__
    def __getstate__(self) -> dict:  # noqa N807
        # Disable default state-setting when unpickled.
        return {}
    cls.__getstate__ = __getstate__
    def __setstate__(self, state):  # noqa N807
        # Disable default state-setting when copied.
        # Sadly what works for pickle doesn't work for copy.
        assert not state
    cls.__setstate__ = __setstate__
    return cls

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
    "IndexedTupleDict",
    "Named",
    "NamedKeyDict",
    "NamedValueSet",
)

from abc import ABC
from typing import (
    AbstractSet,
    Any,
    cast,
    Dict,
    Generic,
    ItemsView,
    Iterable,
    Iterator,
    KeysView,
    Mapping,
    MutableMapping,
    MutableSet,
    Tuple,
    TypeVar,
    Union,
    ValuesView,
)
from types import MappingProxyType


class Named(ABC):
    """A base class for objects that have a ``.name`` attribute.

    Notes
    -----
    In Python 3.8, this would probably just be a `typing.Protocol`.
    """
    def __init__(self, name: str):
        self.name = name

    def __eq__(self, other: Any) -> Union[bool, NotImplemented]:
        try:
            return self.name == other.name
        except AttributeError:
            return NotImplemented

    name: str
    """Name for this object (`str`).
    """


K = TypeVar("K", bound=Named)
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
        if isinstance(key, Named):
            return self._dict[key]
        else:
            return self._dict[self._names[key]]

    def __setitem__(self, key: Union[str, K], value: V):
        if isinstance(key, Named):
            assert self._names.get(key.name, key) == key, "Name is already associated with a different key."
            self._dict[key] = value
            self._names[key.name] = key
        else:
            self._dict[self._names[key]] = value

    def __delitem__(self, key: Union[str, K]):
        if isinstance(key, Named):
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


class NamedValueSet(MutableSet[K]):
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

    def __init__(self, elements: Iterable[K] = ()):
        self._dict = {element.name: element for element in elements}

    @property
    def names(self) -> KeysView[str]:
        """The set of element names, in the same order
        (`~collections.abc.KeysView`).
        """
        return self._dict.keys()

    def asDict(self) -> Mapping[str, K]:
        """Return a mapping view with names as keys.

        Returns
        -------
        dict : `Mapping`
            A dictionary-like view with ``values() == self``.
        """
        return self._dict

    def __contains__(self, key: Any) -> bool:
        return getattr(key, "name", key) in self._dict

    def __len__(self) -> int:
        return len(self._dict)

    def __iter__(self) -> Iterator[K]:
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

    def __le__(self, other: AbstractSet[Any]) -> Union[bool, NotImplemented]:
        if isinstance(other, NamedValueSet):
            return self._dict.keys() <= other._dict.keys()
        else:
            return NotImplemented

    def __ge__(self, other: AbstractSet[Any]) -> Union[bool, NotImplemented]:
        if isinstance(other, NamedValueSet):
            return self._dict.keys() >= other._dict.keys()
        else:
            return NotImplemented

    def issubset(self, other: AbstractSet[Any]) -> Union[bool, NotImplemented]:
        return self <= other

    def issuperset(self, other: AbstractSet[Any]) -> Union[bool, NotImplemented]:
        return self >= other

    def __getitem__(self, name: str) -> K:
        return self._dict[name]

    def get(self, name: str, default: Any = None) -> Any:
        """Return the element with the given name, or ``default`` if
        no such element is present.
        """
        return self._dict.get(name, default)

    def __delitem__(self, name: str):
        del self._dict[name]

    def add(self, element: K):
        """Add an element to the set.

        Raises
        ------
        AttributeError
            Raised if the element does not have a ``.name`` attribute.
        """
        self._dict[element.name] = element

    def remove(self, element: Union[str, K]):
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

    def discard(self, element: Union[str, K]):
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

    def copy(self) -> NamedValueSet[K]:
        result = NamedValueSet.__new__(NamedValueSet)
        result._dict = dict(self._dict)
        return result

    def freeze(self):
        """Disable all mutators, effectively transforming ``self`` into
        an immutable set.
        """
        if not isinstance(self._dict, MappingProxyType):
            self._dict = MappingProxyType(self._dict)


@ValuesView.register
class IndexedTupleValuesView(Generic[V]):
    """ValuesView implementation for `IndexedTupleDict`.

    Notes
    -----
    This class only exists because `collections.abc.ValuesView` (and its
    `typing` counterpart) aren't actually ABCs; they're concrete classes that
    nevertheless inherit from `abc.ABCMeta` and have state.

    It'd make sense for `IndexedTupleDict.values()` to just return a `tuple`,
    but it can't because - even though it satisfies all requirements for
    `ValuesView` - it isn't a `ValuesView` subclass, according to either
    `isinstance` (at runtime) or static typing (i.e. mypy).

    This classes is registered as a "virtual subclass" to take care of runtime
    checks, and we just use `typing.cast` in the implementation of
    `IndexedTupleDict.values()` so it can return `ValuesView[V]` directly
    (since that's covariant).
    """

    def __init__(self, t: Tuple[V, ...]):
        self._tuple = t

    __slots__ = ("_tuple",)

    def __iter__(self) -> Iterator[V]:
        return iter(self._tuple)

    def __contains__(self, v: Any) -> bool:
        return v in self._tuple

    def __len__(self) -> int:
        return len(self._tuple)


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
        The caller is also responsible for guaranteeing that the indices in
        the mapping are all valid for the given tuple.
    values: `tuple`
        Tuple of values for the dictionary.  The caller is responsible for
        guaranteeing that this has the same number of elements as ``indices``
        and that they are in the same order.
    """

    __slots__ = ("_indices", "_values")

    def __new__(cls, indices: NamedKeyDict[K, int], values: Tuple[V, ...]):
        assert len(indices) == len(values)
        self = super().__new__(cls)
        self._indices = indices
        self._values = values
        return self

    @property
    def names(self) -> KeysView[str]:
        """The set of names associated with the keys, in the same order
        (`~collections.abc.KeysView`).
        """
        return self._indices.names

    def byName(self) -> Dict[str, V]:
        """Return a `dict` with names as keys and the same values as ``self``.
        """
        return dict(zip(self.names, self._values))

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

    def __contains__(self, key: Any) -> bool:
        return key in self._indices

    def keys(self) -> KeysView[K]:
        return self._indices.keys()

    def values(self) -> ValuesView[V]:
        return cast(ValuesView[V], IndexedTupleValuesView(self._values))

    # Let Mapping base class provide items(); we can't do it any more
    # efficiently ourselves.

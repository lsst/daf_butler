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
    "NamedKeyDict",
    "NamedKeyMapping",
    "NamedValueSet",
)

from abc import abstractmethod
from typing import (
    AbstractSet,
    Any,
    Dict,
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
try:
    # If we're running mypy, we should have typing_extensions.
    # If we aren't running mypy, we shouldn't assume we do.
    # When we're safely on Python 3.8, we can import Protocol
    # from typing and avoid all of this.
    from typing_extensions import Protocol

    class Named(Protocol):
        @property
        def name(self) -> str:
            pass

except ImportError:
    Named = Any  # type: ignore


K = TypeVar("K", bound=Named)
V = TypeVar("V")


class NamedKeyMapping(Mapping[K, V]):
    """An abstract base class for custom mappings whose keys are objects with
    a `str` ``name`` attribute, for which lookups on the name as well as the
    object are permitted.

    Notes
    -----
    In addition to the new `names` property and `byName` method, this class
    simply redefines the type signature for `__getitem__` and `get` that would
    otherwise be inherited from `Mapping`. That is only relevant for static
    type checking; the actual Python runtime doesn't care about types at all.
    """

    @property
    @abstractmethod
    def names(self) -> AbstractSet[str]:
        """The set of names associated with the keys, in the same order
        (`AbstractSet` [ `str` ]).
        """
        raise NotImplementedError()

    def byName(self) -> Dict[str, V]:
        """Return a `Mapping` with names as keys and the same values as
        ``self``.

        Returns
        -------
        dictionary : `dict`
            A dictionary with the same values (and iteration order) as
            ``self``, with `str` names as keys.  This is always a new object,
            not a view.
        """
        return dict(zip(self.names, self.values()))

    @abstractmethod
    def __getitem__(self, key: Union[str, K]) -> V:
        raise NotImplementedError()

    def get(self, key: Union[str, K], default: Any = None) -> Any:
        # Delegating to super is not allowed by typing, because it doesn't
        # accept str, but we know it just delegates to __getitem__, which does.
        return super().get(key, default)  # type: ignore


class NamedKeyMutableMapping(NamedKeyMapping[K, V], MutableMapping[K, V]):
    """An abstract base class that adds mutation to `NamedKeyMapping`.
    """

    @abstractmethod
    def __setitem__(self, key: Union[str, K], value: V) -> None:
        raise NotImplementedError()

    @abstractmethod
    def __delitem__(self, key: Union[str, K]) -> None:
        raise NotImplementedError()

    def pop(self, key: Union[str, K], default: Any = None) -> Any:
        # See comment in `NamedKeyMapping.get`; same logic applies here.
        return super().pop(key, default)  # type: ignore


class NamedKeyDict(NamedKeyMutableMapping[K, V]):
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

    def __init__(self, *args: Any):
        self._dict: Dict[K, V] = dict(*args)
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
        if isinstance(key, str):
            return self._dict[self._names[key]]
        else:
            return self._dict[key]

    def __setitem__(self, key: Union[str, K], value: V) -> None:
        if isinstance(key, str):
            self._dict[self._names[key]] = value
        else:
            assert self._names.get(key.name, key) == key, "Name is already associated with a different key."
            self._dict[key] = value
            self._names[key.name] = key

    def __delitem__(self, key: Union[str, K]) -> None:
        if isinstance(key, str):
            del self._dict[self._names[key]]
            del self._names[key]
        else:
            del self._dict[key]
            del self._names[key.name]

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

    def freeze(self) -> None:
        """Disable all mutators, effectively transforming ``self`` into
        an immutable mapping.
        """
        if not isinstance(self._dict, MappingProxyType):
            self._dict = MappingProxyType(self._dict)  # type: ignore


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

    def __eq__(self, other: Any) -> Union[bool, NotImplemented]:
        if isinstance(other, NamedValueSet):
            return self._dict.keys() == other._dict.keys()
        else:
            return NotImplemented

    def __hash__(self) -> int:
        return hash(frozenset(self._dict.keys()))

    # As per Set's docs, overriding just __le__ and __ge__ for performance will
    # cover the other comparisons, too.

    def __le__(self, other: AbstractSet[K]) -> Union[bool, NotImplemented]:
        if isinstance(other, NamedValueSet):
            return self._dict.keys() <= other._dict.keys()
        else:
            return NotImplemented

    def __ge__(self, other: AbstractSet[K]) -> Union[bool, NotImplemented]:
        if isinstance(other, NamedValueSet):
            return self._dict.keys() >= other._dict.keys()
        else:
            return NotImplemented

    def issubset(self, other: AbstractSet[K]) -> bool:
        return self <= other

    def issuperset(self, other: AbstractSet[K]) -> bool:
        return self >= other

    def __getitem__(self, name: str) -> K:
        return self._dict[name]

    def get(self, name: str, default: Any = None) -> Any:
        """Return the element with the given name, or ``default`` if
        no such element is present.
        """
        return self._dict.get(name, default)

    def __delitem__(self, name: str) -> None:
        del self._dict[name]

    def add(self, element: K) -> None:
        """Add an element to the set.

        Raises
        ------
        AttributeError
            Raised if the element does not have a ``.name`` attribute.
        """
        self._dict[element.name] = element

    def remove(self, element: Union[str, K]) -> Any:
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

    def discard(self, element: Union[str, K]) -> Any:
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

    def pop(self, *args: str) -> K:
        """Remove and return an element from the set.

        Parameters
        ----------
        name : `str`, optional
            Name of the element to remove and return.  Must be passed
            positionally.  If not provided, an arbitrary element is
            removed and returned.

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

    def freeze(self) -> None:
        """Disable all mutators, effectively transforming ``self`` into
        an immutable set.
        """
        if not isinstance(self._dict, MappingProxyType):
            self._dict = MappingProxyType(self._dict)  # type: ignore


class IndexedTupleDict(NamedKeyMapping[K, V]):
    """An immutable mapping that combines a tuple of values with a (possibly
    shared) mapping from key to tuple index.

    Parameters
    ----------
    indices: `NamedKeyDict`
        Mapping from key to integer index in the values tuple.  This mapping
        is used as-is, not copied or converted to a true `dict`, which means
        that the caller must guarantee that it will not be modified by other
        (shared) owners in the future.
        The caller is also responsible for guaranteeing that the indices in
        the mapping are all valid for the given tuple.
    values: `tuple`
        Tuple of values for the dictionary.  This may have a length greater
        than the length of indices; these values are not considered part of
        the mapping.
    """

    __slots__ = ("_indices", "_values")

    def __init__(self, indices: NamedKeyDict[K, int], values: Tuple[V, ...]):
        assert tuple(indices.values()) == tuple(range(len(values)))
        self._indices = indices
        self._values = values

    @property
    def names(self) -> KeysView[str]:
        return self._indices.names

    def byName(self) -> Dict[str, V]:
        return dict(zip(self.names, self._values))

    def __getitem__(self, key: Union[str, K]) -> V:
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

    # Tuple meets all requirements of ValuesView, but the Python typing system
    # doesn't recognize it as substitutable, perhaps because it only really is
    # for immutable mappings where there's no need to worry about the view
    # being updated because the mapping changed.
    def values(self) -> Tuple[V, ...]:   # type: ignore
        return self._values

    # Let Mapping base class provide items(); we can't do it any more
    # efficiently ourselves.

    # These private attributes need to have types annotated outside __new__
    # because mypy hasn't learned (yet) how to infer instance attribute types
    # there they way it can with __init__.
    _indices: NamedKeyDict[K, int]
    _values: Tuple[V, ...]

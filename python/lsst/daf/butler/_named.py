# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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
    "NamedKeyDict",
    "NamedKeyMapping",
    "NamedValueAbstractSet",
    "NamedValueMutableSet",
    "NamedValueSet",
    "NameLookupMapping",
    "NameMappingSetView",
)

import contextlib
from abc import abstractmethod
from collections.abc import (
    ItemsView,
    Iterable,
    Iterator,
    KeysView,
    Mapping,
    MutableMapping,
    MutableSet,
    Set,
    ValuesView,
)
from types import MappingProxyType
from typing import Any, Protocol, TypeVar, overload


class Named(Protocol):
    """Protocol for objects with string name.

    A non-inheritance interface for objects that have a string name that
    maps directly to their equality comparisons.
    """

    @property
    def name(self) -> str:
        pass


K = TypeVar("K", bound=Named)
K_co = TypeVar("K_co", bound=Named, covariant=True)
V = TypeVar("V")
V_co = TypeVar("V_co", covariant=True)


class NamedKeyMapping(Mapping[K, V_co]):
    """Custom mapping class.

    An abstract base class for custom mappings whose keys are objects with
    a `str` ``name`` attribute, for which lookups on the name as well as the
    object are permitted.

    Notes
    -----
    In addition to the new `names` property and `byName` method, this class
    simply redefines the type signature for `__getitem__` and `get` that would
    otherwise be inherited from `~collections.abc.Mapping`. That is only
    relevant for static type checking; the actual Python runtime doesn't
    care about types at all.
    """

    __slots__ = ()

    @property
    @abstractmethod
    def names(self) -> Set[str]:
        """Return the set of names associated with the keys, in the same order.

        (`~collections.abc.Set` [ `str` ]).
        """
        raise NotImplementedError()

    def byName(self) -> dict[str, V_co]:
        """Return a `~collections.abc.Mapping` with names as keys and the
        ``self`` values.

        Returns
        -------
        dictionary : `dict`
            A dictionary with the same values (and iteration order) as
            ``self``, with `str` names as keys.  This is always a new object,
            not a view.
        """
        return dict(zip(self.names, self.values(), strict=True))

    @abstractmethod
    def keys(self) -> NamedValueAbstractSet[K]:  # type: ignore
        # TODO: docs
        raise NotImplementedError()

    @abstractmethod
    def __getitem__(self, key: str | K) -> V_co:
        raise NotImplementedError()

    @overload
    def get(self, key: object) -> V_co | None:
        ...

    @overload
    def get(self, key: object, default: V) -> V_co | V:
        ...

    def get(self, key: Any, default: Any = None) -> Any:
        return super().get(key, default)


NameLookupMapping = NamedKeyMapping[K, V_co] | Mapping[str, V_co]
"""A type annotation alias for signatures that want to use ``mapping[s]``
(or ``mapping.get(s)``) where ``s`` is a `str`, and don't care whether
``mapping.keys()`` returns named objects or direct `str` instances.
"""


class NamedKeyMutableMapping(NamedKeyMapping[K, V], MutableMapping[K, V]):
    """An abstract base class that adds mutation to `NamedKeyMapping`."""

    __slots__ = ()

    @abstractmethod
    def __setitem__(self, key: str | K, value: V) -> None:
        raise NotImplementedError()

    @abstractmethod
    def __delitem__(self, key: str | K) -> None:
        raise NotImplementedError()

    def pop(self, key: str | K, default: Any = None) -> Any:
        # See comment in `NamedKeyMapping.get`; same logic applies here.
        return super().pop(key, default)  # type: ignore


class NamedKeyDict(NamedKeyMutableMapping[K, V]):
    """Dictionary wrapper for named keys.

    Requires keys to have a ``.name`` attribute,
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

    __slots__ = (
        "_dict",
        "_names",
    )

    def __init__(self, *args: Any):
        self._dict: dict[K, V] = dict(*args)
        self._names = {key.name: key for key in self._dict}
        assert len(self._names) == len(self._dict), "Duplicate names in keys."

    @property
    def names(self) -> KeysView[str]:
        """Return set of names associated with the keys, in the same order.

        (`~collections.abc.KeysView`).
        """
        return self._names.keys()

    def byName(self) -> dict[str, V]:
        """Return a `dict` with names as keys and the ``self`` values."""
        return dict(zip(self._names.keys(), self._dict.values(), strict=True))

    def __len__(self) -> int:
        return len(self._dict)

    def __iter__(self) -> Iterator[K]:
        return iter(self._dict)

    def __str__(self) -> str:
        return "{{{}}}".format(", ".join(f"{str(k)}: {str(v)}" for k, v in self.items()))

    def __repr__(self) -> str:
        return "NamedKeyDict({{{}}})".format(", ".join(f"{repr(k)}: {repr(v)}" for k, v in self.items()))

    def __getitem__(self, key: str | K) -> V:
        if isinstance(key, str):
            return self._dict[self._names[key]]
        else:
            return self._dict[key]

    def __setitem__(self, key: str | K, value: V) -> None:
        if isinstance(key, str):
            self._dict[self._names[key]] = value
        else:
            assert self._names.get(key.name, key) == key, "Name is already associated with a different key."
            self._dict[key] = value
            self._names[key.name] = key

    def __delitem__(self, key: str | K) -> None:
        if isinstance(key, str):
            del self._dict[self._names[key]]
            del self._names[key]
        else:
            del self._dict[key]
            del self._names[key.name]

    def keys(self) -> NamedValueAbstractSet[K]:  # type: ignore
        return NameMappingSetView(self._names)

    def values(self) -> ValuesView[V]:
        return self._dict.values()

    def items(self) -> ItemsView[K, V]:
        return self._dict.items()

    def copy(self) -> NamedKeyDict[K, V]:
        """Return a new `NamedKeyDict` with the same elements."""
        result = NamedKeyDict.__new__(NamedKeyDict)
        result._dict = dict(self._dict)
        result._names = dict(self._names)
        return result

    def freeze(self) -> NamedKeyMapping[K, V]:
        """Disable all mutators.

        Effectively transforms ``self`` into an immutable mapping.

        Returns
        -------
        self : `NamedKeyMapping`
            While ``self`` is modified in-place, it is also returned with a
            type annotation that reflects its new, frozen state; assigning it
            to a new variable (and considering any previous references
            invalidated) should allow for more accurate static type checking.
        """
        if not isinstance(self._dict, MappingProxyType):  # type: ignore[unreachable]
            self._dict = MappingProxyType(self._dict)  # type: ignore
        return self


class NamedValueAbstractSet(Set[K_co]):
    """Custom sets with named elements.

    An abstract base class for custom sets whose elements are objects with
    a `str` ``name`` attribute, allowing some dict-like operations and
    views to be supported.
    """

    __slots__ = ()

    @property
    @abstractmethod
    def names(self) -> Set[str]:
        """Return set of names associated with the keys, in the same order.

        (`~collections.abc.Set` [ `str` ]).
        """
        raise NotImplementedError()

    @abstractmethod
    def asMapping(self) -> Mapping[str, K_co]:
        """Return a mapping view with names as keys.

        Returns
        -------
        dict : `~collections.abc.Mapping`
            A dictionary-like view with ``values() == self``.
        """
        raise NotImplementedError()

    @abstractmethod
    def __getitem__(self, key: str | K_co) -> K_co:
        raise NotImplementedError()

    @overload
    def get(self, key: object) -> K_co | None:
        ...

    @overload
    def get(self, key: object, default: V) -> K_co | V:
        ...

    def get(self, key: Any, default: Any = None) -> Any:
        """Return the element with the given name.

        Returns ``default`` if no such element is present.
        """
        try:
            return self[key]
        except KeyError:
            return default

    @classmethod
    def _from_iterable(cls, iterable: Iterable[K_co]) -> NamedValueSet[K_co]:
        """Construct class from an iterable.

        Hook to ensure that inherited `collections.abc.Set` operators return
        `NamedValueSet` instances, not something else (see `collections.abc`
        documentation for more information).

        Note that this behavior can only be guaranteed when both operands are
        `NamedValueAbstractSet` instances.
        """
        return NamedValueSet(iterable)


class NameMappingSetView(NamedValueAbstractSet[K_co]):
    """A lightweight implementation of `NamedValueAbstractSet`.

    Backed by a mapping from name to named object.

    Parameters
    ----------
    mapping : `~collections.abc.Mapping` [ `str`, `object` ]
        Mapping this object will provide a view of.
    """

    def __init__(self, mapping: Mapping[str, K_co]):
        self._mapping = mapping

    __slots__ = ("_mapping",)

    @property
    def names(self) -> Set[str]:
        # Docstring inherited from NamedValueAbstractSet.
        return self._mapping.keys()

    def asMapping(self) -> Mapping[str, K_co]:
        # Docstring inherited from NamedValueAbstractSet.
        return self._mapping

    def __getitem__(self, key: str | K_co) -> K_co:
        if isinstance(key, str):
            return self._mapping[key]
        else:
            return self._mapping[key.name]

    def __contains__(self, key: Any) -> bool:
        return getattr(key, "name", key) in self._mapping

    def __len__(self) -> int:
        return len(self._mapping)

    def __iter__(self) -> Iterator[K_co]:
        return iter(self._mapping.values())

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, NamedValueAbstractSet):
            return self.names == other.names
        else:
            return set(self._mapping.values()) == other

    def __le__(self, other: Set[K]) -> bool:
        if isinstance(other, NamedValueAbstractSet):
            return self.names <= other.names
        else:
            return set(self._mapping.values()) <= other

    def __ge__(self, other: Set[K]) -> bool:
        if isinstance(other, NamedValueAbstractSet):
            return self.names >= other.names
        else:
            return set(self._mapping.values()) >= other

    def __str__(self) -> str:
        return "{{{}}}".format(", ".join(str(element) for element in self))

    def __repr__(self) -> str:
        return f"NameMappingSetView({self._mapping})"


class NamedValueMutableSet(NamedValueAbstractSet[K], MutableSet[K]):
    """Mutable variant of `NamedValueAbstractSet`.

    Methods that can add new elements to the set are unchanged from their
    `~collections.abc.MutableSet` definitions, while those that only remove
    them can generally accept names or element instances.  `pop` can be used
    in either its `~collections.abc.MutableSet` form (no arguments; an
    arbitrary element is returned) or its `~collections.abc.MutableMapping`
    form (one or two arguments for the name and optional default value,
    respectively).  A `~collections.abc.MutableMapping`-like `__delitem__`
    interface is also included, which takes only names (like
    `NamedValueAbstractSet.__getitem__`).
    """

    __slots__ = ()

    @abstractmethod
    def __delitem__(self, name: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def remove(self, element: str | K) -> Any:
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
        raise NotImplementedError()

    @abstractmethod
    def discard(self, element: str | K) -> Any:
        """Remove an element from the set if it exists.

        Does nothing if no matching element is present.

        Parameters
        ----------
        element : `object` or `str`
            Element to remove or the string name thereof.  Assumed to be an
            element if it has a ``.name`` attribute.
        """
        raise NotImplementedError()

    @abstractmethod
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
        raise NotImplementedError()


class NamedValueSet(NameMappingSetView[K], NamedValueMutableSet[K]):
    """Custom mutable set class.

    A custom mutable set class that requires elements to have a ``.name``
    attribute, which can then be used as keys in `dict`-like lookup.

    Names and elements can both be used with the ``in`` and ``del``
    operators, `remove`, and `discard`.  Names (but not elements)
    can be used with ``[]``-based element retrieval (not assignment)
    and the `get` method.

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

    def __init__(self, elements: Iterable[K] = ()):
        super().__init__({element.name: element for element in elements})

    def __repr__(self) -> str:
        return "NamedValueSet({{{}}})".format(", ".join(repr(element) for element in self))

    def issubset(self, other: Set[K]) -> bool:
        return self <= other

    def issuperset(self, other: Set[K]) -> bool:
        return self >= other

    def __delitem__(self, name: str) -> None:
        del self._mapping[name]

    def add(self, element: K) -> None:
        """Add an element to the set.

        Raises
        ------
        AttributeError
            Raised if the element does not have a ``.name`` attribute.
        """
        self._mapping[element.name] = element

    def clear(self) -> None:
        # Docstring inherited.
        self._mapping.clear()

    def remove(self, element: str | K) -> Any:
        # Docstring inherited.
        k = element.name if not isinstance(element, str) else element
        del self._mapping[k]

    def discard(self, element: str | K) -> Any:
        # Docstring inherited.
        with contextlib.suppress(KeyError):
            self.remove(element)

    def pop(self, *args: str) -> K:
        # Docstring inherited.
        if not args:
            # Parent is abstract method and we cannot call MutableSet
            # implementation directly. Instead follow MutableSet and
            # choose first element from iteration.
            it = iter(self._mapping)
            try:
                value = next(it)
            except StopIteration:
                raise KeyError from None
            args = (value,)

        return self._mapping.pop(*args)

    def update(self, elements: Iterable[K]) -> None:
        """Add multiple new elements to the set.

        Parameters
        ----------
        elements : `~collections.abc.Iterable`
            Elements to add.
        """
        for element in elements:
            self.add(element)

    def copy(self) -> NamedValueSet[K]:
        """Return a new `NamedValueSet` with the same elements."""
        result = NamedValueSet.__new__(NamedValueSet)
        result._mapping = dict(self._mapping)
        return result

    def freeze(self) -> NamedValueAbstractSet[K]:
        """Disable all mutators.

        Effectively transforming ``self`` into an immutable set.

        Returns
        -------
        self : `NamedValueAbstractSet`
            While ``self`` is modified in-place, it is also returned with a
            type annotation that reflects its new, frozen state; assigning it
            to a new variable (and considering any previous references
            invalidated) should allow for more accurate static type checking.
        """
        if not isinstance(self._mapping, MappingProxyType):  # type: ignore[unreachable]
            self._mapping = MappingProxyType(self._mapping)  # type: ignore
        return self

    _mapping: dict[str, K]

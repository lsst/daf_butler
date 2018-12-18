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

__all__ = ("DimensionSet", "DimensionNameSet", "toNameSet")

from abc import ABCMeta, abstractmethod
from collections.abc import Set, MutableSet
from collections import OrderedDict
from .elements import DimensionElement


def toNameSet(container):
    """Transform an iterable of `DimensionElement` or names thereof into a
    `DimensionNameSet`.

    Also accepts objects with a ``.names`` attribute that are not themselves
    iterable, such as `DimensionNameSet` itself.
    """
    if isinstance(container, str):
        # Friendlier error for easy mistake, e.g. union("Name") instead of
        # union(["Name"])
        raise TypeError("Argument must be an *iterable* over `DimensionElement` or `str`; got `str`")
    try:
        return DimensionNameSet(container.names)
    except AttributeError:
        pass
    return DimensionNameSet(element.name if isinstance(element, DimensionElement) else element
                            for element in container)


class DimensionSetBase(metaclass=ABCMeta):
    """Abstract base class to share common implementations between
    `DimensionSet` and `DimensionNameSet`.
    """

    @property
    @abstractmethod
    def names(self):
        """The names of all elements (`set`-like, immutable).

        The order of the names is consistent with the iteration order of the
        set itself.
        """
        raise NotImplementedError()

    def __len__(self):
        return len(self.names)

    def __str__(self):
        return "{{{}}}".format(", ".join(self.names))

    def __hash__(self):
        # self.names can be either a frozenset or OrderedDict keys view,
        # make sure that hash is always consistent by converting to frozenset
        return hash(frozenset(self.names))

    def __eq__(self, other):
        return self.names == toNameSet(other).names

    def __le__(self, other):
        return self.names <= toNameSet(other).names

    def __lt__(self, other):
        return self.names < toNameSet(other).names

    def __ge__(self, other):
        return self.names >= toNameSet(other).names

    def __gt__(self, other):
        return self.names > toNameSet(other).names

    def issubset(self, other):
        """Return `True` if all elements in ``self`` are also in ``other``.

        The empty set is a subset of all sets (including the empty set).
        """
        return self <= other

    def issuperset(self, other):
        """Return `True` if all elements in ``other`` are also in ``self``,
        and `False` otherwise.

        All sets (including the empty set) are supersets of the empty set.
        """
        return self >= other

    def isdisjoint(self, other):
        """Return `True` if there are no elements in both ``self`` and
        ``other``, and `False` otherwise.

        All sets (including the empty set) are disjoint with the empty set.
        """
        return self.names.isdisjoint(toNameSet(other).names)

    def union(self, *others):
        """Return a new set containing all elements that are in ``self`` or
        any of the other given sets.

        Parameters
        ----------
        *others : iterable over `DimensionElement` or `str`.
            Other sets whose elements should be included in the result.

        Returns
        -------
        result : `DimensionNameSet` or `DimensionSet`
            A new set containing all elements in any input set.  A full
            `DimensionSet` is returned if any argument is a full
            `DimensionSet` or `DimensionGraph`.
        """
        names = set(self.names)
        universe = getattr(self, "universe", None)
        for other in others:
            names |= toNameSet(other).names
            universe = getattr(other, "universe", universe)
        if universe is not None:
            return DimensionSet(universe, names)
        else:
            return DimensionNameSet(names)

    def intersection(self, *others):
        """Return a new set containing all elements that are in both  ``self``
        and all of the other given sets.

        Parameters
        ----------
        others : iterable over `DimensionElement` or `str`.
            Other sets whose elements may be included in the result.

        Returns
        -------
        result : `DimensionNameSet` or `DimensionSet`
            A new set containing any elements in all input sets.  A full
            `DimensionSet` is returned if any argument is a full `DimensionSet`
            or `DimensionGraph`.
        """
        names = set(self.names)
        universe = getattr(self, "universe", None)
        for other in others:
            names &= toNameSet(other).names
            universe = getattr(other, "universe", universe)
        if universe is not None:
            return DimensionSet(universe, names)
        else:
            return DimensionNameSet(names)

    def symmetric_difference(self, other):
        """Return a new set containing all elements that are in either ``self``
        or other, but not both.

        Parameters
        ----------
        other : iterable of `DimensionElement` or `str`.
            The other set from which to draw potential result elements.

        Returns
        -------
        result : `DimensionNameSet` or `DimensionSet`
            A new set containing elements ``self`` or ``other``, but not both.
            A full `DimensionSet` is returned if any argument is a full
            `DimensionSet` or `DimensionGraph`.
        """
        names = self.names ^ toNameSet(other).names
        universe = getattr(self, "universe", None)
        universe = getattr(other, "universe", universe)
        if universe is not None:
            return DimensionSet(universe, names)
        else:
            return DimensionNameSet(names)

    def difference(self, other):
        """Return a new set containing all elements that are in ``self``
        but not other.

        Parameters
        ----------
        other : iterable of `DimensionElement` or `str`.
            The other set containing elements that should not be included
            in the result.

        Returns
        -------
        result : `DimensionNameSet` or `DimensionSet`
            A new set containing elements in ``self`` but not ``other``.
            A full `DimensionSet` is returned if any argument is a full
            `DimensionSet` or `DimensionGraph`.
        """
        names = self.names - toNameSet(other).names
        universe = getattr(self, "universe", None)
        universe = getattr(other, "universe", universe)
        if universe is not None:
            return DimensionSet(universe, names)
        else:
            return DimensionNameSet(names)

    # Operators that return sets are only enabled when operands on both sides
    # have the same type, to avoid confusion about return types.

    def __or__(self, other):
        if type(self) == type(other):
            return self.union(other)
        return NotImplemented

    def __and__(self, other):
        if isinstance(other, DimensionSet):
            return self.intersection(other)
        return NotImplemented

    def __xor__(self, other):
        if isinstance(other, DimensionSet):
            return self.symmetric_difference(other)
        return NotImplemented

    def __sub__(self, other):
        if isinstance(other, DimensionSet):
            return self.difference(other)
        return NotImplemented


class DimensionSet(DimensionSetBase, Set):
    r"""A custom set/dict hybrid class for collections of `DimensionElement`\s.

    `DimensionSet` objects implement the full (immutable)
    `collections.abc.Set` interface.  In addition, like `frozenset`, they are
    immutable and hashable, and also provide named-method versions of most
    operators.  Unlike Python sets, they are deterministically sorted.

    Parameters
    ----------
    universe : `DimensionGraph`
        Ultimate-parent `DimensionGraph` that constructed the elements in
        this set.
    elements : iterable of `DimensionElement` or `str`
        Elements to include in the set, or names thereof.
    expand : `bool`
        If `True`, recursively expand the set to include dependencies.
    optional : `bool`
        If `True`, include optional dependencies in expansion.  Ignored
        if ``expand`` is `False`.

    Notes
    -----
    `DimensionSet` comparison operators and named-method relation operations
    accept other set-like objects and iterables containing either
    `DimensionElement` instances or their string names; because
    `DimensionElement`\s cannot be directly constructed, APIs that accept them
    should generally accept a name as an alternative when the transformation
    to a `DimensionElement` can be done internally.  Operators that return
    new sets (`|`, `&`, `^`, and `-`) do require `DimensionSet` operands on
    both sides to avoid surprises in return types.

    Because the `DimensionElement` objects they hold always have a name,
    `DimensionSet`\s also supports some `dict`-like operations: including
    regular square-bracket indexing (`__getitem__`), `get`, and the ``in``
    operator (`__contains__`).  Both names and `DimensionElement` objects can
    be passed to any of these. The `names` attribute can also be used to
    obtain a `set`-like object containing those names.

    `DimensionSet` instances cannot be constructed directly; they can only be
    obtained (possibly indirectly) from a special "universe" `DimensionGraph`
    loaded from configuration.
    """
    def __init__(self, universe, elements, expand=False, optional=False):
        self._universe = universe
        self._elements = OrderedDict()

        def toPairs(elems):
            for elem in elems:
                if not isinstance(elem, DimensionElement):
                    elem = self._universe.elements[elem]
                yield (elem.name, elem)
                if expand:
                    yield from toPairs(elem.dependencies(optional=optional))

        names = dict(toPairs(elements))
        if names:
            # We iterate over the elements in the universe to maintain the
            # careful topological+lexicographical ordering established there.
            # This makes set construction scale with the size of the universe,
            # but that's okay in this context because Dimension universes are
            # always small.  It'd be a bad idea to copy logic this into a more
            # general-purpose sorted set, though.
            for candidate in self._universe.elements:
                if names.pop(candidate.name, None):
                    self._elements[candidate.name] = candidate

    @property
    def universe(self):
        """The graph of all dimensions compatible with self (`DimensionGraph`).
        """
        return self._universe

    def __iter__(self):
        return iter(self._elements.values())

    def __repr__(self):
        return f"DimensionSet({self})"

    def __contains__(self, key):
        key = getattr(key, "name", key)
        return key in self._elements

    def __getitem__(self, key):
        key = getattr(key, "name", key)
        return self._elements[key]

    def get(self, key, default=None):
        """Return the element with the given name, or ``default`` if it
        does not exist.

        ``key`` may also be a `DimensionElement`, in which case an equivalent
        object will be returned if it is present in the set.
        """
        key = getattr(key, "name", key)
        return self._elements.get(key, default)

    @property
    def names(self):
        """The names of all elements (`set`-like, immutable).

        The order of the names is consistent with the iteration order of the
        `DimensionSet` itself.
        """
        return self._elements.keys()

    @property
    def links(self):
        """The names of all fields that uniquely identify these dimensions in
        a data ID dict (`frozenset` of `str`).
        """
        return frozenset().union(*(d.primaryKey for d in self))

    def expanded(self, optional=False):
        """Return a new `DimensionSet` that has been expanded to include
        dependencies.

        Parameters
        ----------
        optional : `bool`
            Whether to include optional as well as required dependencies.
        """
        return DimensionSet(self.universe, self, expand=True, optional=optional)

    def findIf(self, predicate, default=None):
        """Return the element in ``self`` that matches the given predicate.

        Parameters
        ----------
        predicate : callable
            Callable that takes a single `DimensionElement` argument and
            returns a `bool`, indicating whether the given value should be
            returned.
        default : `DimensionElement`, optional
            Object to return if no matching elements are found.

        Returns
        -------
        matching : `DimensionElement`
            Element matching the given predicate.

        Raises
        ------
        ValueError
            Raised if multiple elements match the given predicate.
        """
        t = tuple(element for element in self if predicate(element))
        if len(t) > 1:
            raise ValueError(f"Multiple matches: {t}")
        elif len(t) == 0:
            return default
        return t[0]


class DimensionNameSet(DimensionSetBase):
    r"""An incomplete, name-only stand-in for `DimensionSet` or
    `DimensionGraph`.

    Parameters
    ----------
    names : iterable of `str`
        The names of elements to conceptually include in the set.

    Notes
    -----
    Because true `DimensionSet`\s and `DimensionGraph`\s cannot be constructed
    without access to a "universe" `DimensionGraph` loaded from config,
    requiring one of these classes in API also makes that API more difficult
    to use.  `DimensionNameSet` partially solves that problem by being easy to
    construct (only the names of the `DimensionElement`\s are needed, and no
    sorting or checking is done) and behaving as much like a `DimensionSet` or
    `DimensionGraph` as possible.  This enables the following pattern:

     - Accept either `DimensionNameSet` as well as `DimensionSet` and/or
       `DimensionGraph` when construting objects that need a container of
       `DimensionElement`\s.  This may limit the functionality of the
       constructed object if only a `DimensionNameSet` is passed, of course.

     - "Upgrade" from `DimensionNameSet` to one of the more complete classes
       when the object is rendezvouzed with a "universe" `DimensionGraph`.
       This upgrade process also serves to validate the names.

    The `DatasetType` class provides an example of this pattern;
    `DatasetType`\s may be constructed with only the names of `Dimension`\s,
    but are modified transparently by `Registry` operations to hold actual
    `Dimension` objects.
    """

    def __init__(self, names):
        if not isinstance(names, Set) or isinstance(names, MutableSet):
            # Usually want to ensure this is a frozenset, but we also accept
            # the keys view of an OrderedDict, and when get that we don't want
            # to transform it as that would discard the ordering.
            names = frozenset(names)
        self._names = names

    def __repr__(self):
        return f"DimensionNameSet({self.names})"

    @property
    def names(self):
        """The names of all elements (`set`-like, immutable).

        Unlike a real `DimensionElement` container, these names are *not*
        topologically sorted.
        """
        return self._names

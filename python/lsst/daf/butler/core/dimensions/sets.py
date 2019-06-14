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

__all__ = ("DimensionSet", "conformSet")

from collections.abc import Set
from collections import OrderedDict
from .elements import DimensionElement
from ..exceptions import ValidationError


def conformSet(container, universe):
    """Transform an iterable of `DimensionElement` or names thereof into an
    object with a set-like ``.names`` attribute.
    """
    from .graph import DimensionGraph
    if isinstance(container, (DimensionGraph, DimensionSet)):
        return container
    elif isinstance(container, str):
        # Friendlier error for easy mistake, e.g. union("Name") instead of
        # union(["Name"])
        raise TypeError("Argument must be an *iterable* over `DimensionElement` or `str`; got `str`")
    return DimensionSet(universe, container)


class DimensionSet(Set):
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
    implied : `bool`
        If `True`, include implied dependencies in expansion.  Ignored
        if ``expand`` is `False`.

    Raises
    ------
    ValidationError
        Raised if a Dimension is not part of the Universe.

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
    def __init__(self, universe, elements, expand=False, implied=False):
        self._universe = universe
        self._elements = OrderedDict()
        self._links = None

        def toPairs(elems):
            for elem in elems:
                if not isinstance(elem, DimensionElement):
                    try:
                        elem = self._universe.elements[elem]
                    except KeyError as e:
                        raise ValidationError(f"Dimension '{elem}' is not part of Universe") from e
                yield (elem.name, elem)
                if expand:
                    yield from toPairs(elem.dependencies(implied=implied))

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

    def links(self):
        """Return the names of all fields that uniquely identify these
        dimensions in a data ID dict.

        Returns
        -------
        links : `frozenset` of `str`
        """
        if self._links is None:
            self._links = frozenset().union(*(d.links() for d in self))
        return self._links

    def expanded(self, implied=False):
        """Return a new `DimensionSet` that has been expanded to include
        dependencies.

        Parameters
        ----------
        implied : `bool`
            Whether to include implied as well as required dependencies.
        """
        return DimensionSet(self.universe, self, expand=True, implied=implied)

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

    def __len__(self):
        return len(self._elements)

    def __str__(self):
        return "{{{}}}".format(", ".join(self._elements.keys()))

    def __hash__(self):
        # Keys are guaranteed to be deterministically sorted (topological then
        # lexicographical) at construction.
        return hash(tuple(self._elements.keys()))

    def __eq__(self, other):
        return self.names == conformSet(other, self._universe).names

    def __le__(self, other):
        return self.names <= conformSet(other, self._universe).names

    def __lt__(self, other):
        return self.names < conformSet(other, self._universe).names

    def __ge__(self, other):
        return self.names >= conformSet(other, self._universe).names

    def __gt__(self, other):
        return self.names > conformSet(other, self._universe).names

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
        return self.names.isdisjoint(conformSet(other, self._universe).names)

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
        for other in others:
            names |= conformSet(other, self._universe).names
        return DimensionSet(self.universe, names)

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
        for other in others:
            names &= conformSet(other, self._universe).names
        return DimensionSet(self.universe, names)

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
        names = self.names ^ conformSet(other, self._universe).names
        return DimensionSet(self.universe, names)

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
        names = self.names - conformSet(other, self._universe).names
        return DimensionSet(self.universe, names)

    # Operators that return sets are only enabled when operands on both sides
    # have the same type, to avoid confusion about return types.

    def __or__(self, other):
        if isinstance(other, DimensionSet):
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

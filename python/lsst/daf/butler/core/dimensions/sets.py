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

__all__ = ("DimensionSet", "DimensionUnitSet", "DimensionJoinSet", "UniverseMismatchError")

from abc import abstractmethod
from collections.abc import Set
from collections import OrderedDict


class UniverseMismatchError(Exception):
    """Exception thrown when trying to relate dimension objects from one
    universe to those from another.

    This almost always indicates a logic bug that should be fixed instead of
    handled at runtime, and hence should usually be allowed to propagate up.
    """

    @classmethod
    def check(cls, a, b):
        """Raise if the given dimension objects are not from the same universe.

        Parameters
        ----------
        a, b : `Dimension`, `DimensionSet`, or `DimensionGraph`
            Dimension objects to compare.
        """
        if a.universe is not b.universe:
            raise cls(
                f"Cannot compare dimensions from different universes: {id(a.universe)} != {id(b.universe)}"
            )


class DimensionSet(Set):
    r"""A special set/dict hybrid base class for `Dimension` objects.

    This class is an abstract base class and cannot be constructed or used
    directly.  See the `DimensionUnitSet` and `DimensionJoinSet` subclasses
    for more information.

    `DimensionSet` objects implement at least most of the
    `collections.abc.Set` interface (not all subclasses implement
    `symmetric_difference` and `difference`). Like `frozenset`, they are
    immutable and hashable, and also provide named-method versions of most
    operators.  Unlike Python sets, they are deterministically sorted.

    Because the `Dimension` objects they hold always have a name,
    `DimensionSet` also supports some more `dict`-like operations: elements
    can be retrieved by name using regular square-bracket indexing
    (`__getitem__`) or `get`, and the ``in`` operator (`__contains__`) can be
    used with both `Dimension` instances and their string names. The `names`
    attribute can also be used to obtain a `set`-like object containing those
    names.

    Like all other dimension objects, `DimensionSet`\s are associated with a
    special "universe" `DimensionGraph`.  All `Dimension`\s in the set must be
    associated with the same universe.

    Parameters
    ----------
    universe : `DimensionGraph`
        Special parent graph to which all `Dimension` instances in this set
        belong.
    names : sequence of `str`
        The names of the `Dimension` elements to include in this set.
    """

    def __init__(self, universe, names=frozenset(), **kwds):
        self._universe = universe
        self._elements = OrderedDict()
        if names:
            self._populate(names, **kwds)

    @property
    def universe(self):
        """The graph of all dimensions compatible with self (`DimensionGraph`).
        """
        return self._universe

    def __len__(self):
        return len(self._elements)

    def __iter__(self):
        return iter(self._elements.values())

    def __str__(self):
        return "{{{}}}".format(", ".join(self._elements.keys()))

    def __contains__(self, key):
        try:
            return key.name in self._elements
        except AttributeError:
            return key in self._elements

    def __getitem__(self, key):
        return self._elements[key]

    def get(self, key, default=None):
        return self._elements.get(key, default)

    @property
    def names(self):
        """The names of all elements (`set`-like, immutable).

        The order of the names is consistent with the iteration order of the
        `DimensionSet` itself.
        """
        return self._elements.keys()

    @property
    def primaryKey(self):
        """The names of all fields that uniquely identify these dimensions in
        a data ID dict (`frozenset` of `str).
        """
        return frozenset().union(*(d.primaryKey for d in self))

    def __hash__(self):
        return hash(self.names)

    # We directly implement the full Set interface below instead of relying
    # on Set's mixin methods because we can do it much more efficiently
    # (by just delegating to set operations on _elements.keys()).

    def __eq__(self, other):
        try:
            UniverseMismatchError.check(self, other)
            return self._elements.keys() == other._elements.keys()
        except AttributeError:
            return NotImplemented

    def __le__(self, other):
        try:
            UniverseMismatchError.check(self, other)
            return self._elements.keys() <= other._elements.keys()
        except AttributeError:
            return NotImplemented

    def __lt__(self, other):
        try:
            UniverseMismatchError.check(self, other)
            return self._elements.keys() < other._elements.keys()
        except AttributeError:
            return NotImplemented

    def __ge__(self, other):
        try:
            UniverseMismatchError.check(self, other)
            return self._elements.keys() >= other._elements.keys()
        except AttributeError:
            return NotImplemented

    def __gt__(self, other):
        try:
            UniverseMismatchError.check(self, other)
            return self._elements.keys() > other._elements.keys()
        except AttributeError:
            return NotImplemented

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
        try:
            UniverseMismatchError.check(self, other)
            return self.names.isdisjoint(other.names)
        except AttributeError:
            return other.isdisjoint(self)

    def union(self, *others, **kwds):
        """Return a new set containing all elements that are in ``self`` or
        any of the other given sets.

        Parameters
        ----------
        *others : `DimensionSet`
            Other sets whose elements should be included in the result.
        **kwds : unspecified
            Additional subclass-specific arguments.

        Returns
        -------
        result : instance of a subclass of `DimensionSet`
            A new set of the same type as ``self`` containing all elements
            in any input set.
        """
        names = set(self.names)
        for other in others:
            UniverseMismatchError.check(self, other)
            names |= other.names
        return type(self)(self.universe, names, **kwds)

    def intersection(self, *others, **kwds):
        """Return a new set containing all elements that are in both  ``self``
        and all of the other given sets.

        Parameters
        ----------
        *others : `DimensionSet`
            Other sets whose elements may be included in the result.
        **kwds : unspecified
            Additional subclass-specific arguments.

        Returns
        -------
        result : instance of a subclass of `DimensionSet`
            A new set of the same type as ``self`` containing any elements
            in all input sets.
        """
        names = set(self.names)
        for other in others:
            UniverseMismatchError.check(self, other)
            names &= other.names
        return type(self)(self.universe, names, **kwds)

    def __or__(self, other):
        return self.union(other)

    def __and__(self, other):
        return self.intersection(other)

    def __xor__(self, other):
        return self.symmetric_difference(other)

    def __sub__(self, other):
        return self.difference(other)

    def copy(self):
        """Return a shallow copy of this set.

        Because `Dimension` instances are immutable, there should be no need
        to ever perform a deep copy.
        """
        result = type(self)(self.universe)
        result._elements = self._elements.copy()
        return result

    @abstractmethod
    def _populate(self, names, **kwds):
        r"""Extract the `Dimension`\s with the given names from
        ``self.universe`` (in order) and add them to ``self._elements``.

        Must be implemented by subclasses, which may opt to also include
        other `Dimension`\s not in ``names``.

        Additional keyword arguments are forwarded directly by ``union``,
        ``intersection``, and the class constructor.
        Subclasses must support at least the case where no additional keywords
        arguments are provided.
        """
        raise NotImplementedError()

    def findMany(self, where):
        """Return the elements in ``self`` that match the given predicate.

        Parameters
        ----------
        where : callable
            Callable that takes a single `DimensionBase` argument and returns
            a `bool`, indicating whether the given value should be returned.

        Yields
        ------
        matching : `DimensionBase`
            Elements matching the given predicate.
        """
        for element in self:
            if where(element):
                yield element

    def findOne(self, where, default=None):
        """Return the element in ``self`` that matches the given predicate.

        Parameters
        ----------
        where : callable
            Callable that takes a single `DimensionBase` argument and returns
            a `bool`, indicating whether the given value should be returned.
        default : `DimensionBase`, optional
            Object to return if no matching elements are found.

        Returns
        -------
        matching : `DimensionBase`
            Element matching the given predicate.

        Raises
        ------
        ValueError
            Raised if multiple elements match the given predicate.
        """
        t = tuple(self.findMany(where))
        if len(t) > 1:
            raise ValueError(f"Multiple matches: {t}")
        elif len(t) == 0:
            return default
        return t[0]


class DimensionUnitSet(DimensionSet):
    r"""A concrete `DimensionSet` for `DimensionUnit` instances.

    `DimensionUnitSet`\s always include all required dependencies of all
    of the `DimensionUnit`\s they contain, and are sorted topologically
    (with an unspecified but guaranteed deterministic ordering to break
    topological ties).

    Automatic expansion to include required dependencies means that set
    operations that remove elements (`symmetric_difference` and `difference`)
    may not work as naively expected; the direct operation may remove a
    `DimensionUnit` from the result while retaining one that depends on it,
    which would then cause it to be added back into the result set.

    The constructor and `union` and `intersection` methods for
    `DimensionUnitSet` take an extra ``optional`` argument that controls
    whether optional dependencies of all elements are also included in
    the returned set.  This defaults to `False`.
    """

    def __repr__(self):
        return f"DimensionUnitSet({self})"

    def _populate(self, names, optional=False):
        # recursive local function to add dependencies of dependencies
        def visit(units):
            for unit in units:
                yield unit.name
                yield from visit(unit.dependencies(optional=optional))

        expanded = set(visit(self.universe.units[unitName] for unitName in names))

        for dimension in self.universe.units:
            if dimension.name in expanded:
                self._elements[dimension.name] = dimension
                expanded.remove(dimension.name)
        assert not expanded

    def symmetric_difference(self, other):
        """Symmetric difference (XOR) is disabled for `DimensionUnitSet`.

        A `DimensionUnitSet` must always include all required dependencies of
        any of its elements, which rules out a standard symmetric difference.
        Returning a standard symmetric difference along with needed
        dependencies that should have been removed would be confusing in at
        least some contexts, and we don't have a clear use case for that
        anyway.
        """
        raise TypeError("DimensionUnitSet cannot safely implement "
                        "symmetric_difference.")

    def difference(self, other):
        """Set difference is disabled for `DimensionUnitSet`.

        A `DimensionUnitSet` must always include all required dependencies of
        any of its elements, which rules out a standard difference. Returning
        a standard difference along with needed dependencies that should have
        been removed would be confusing in at least some contexts, and we
        don't have a clear use case for that anyway.
        """
        raise TypeError("DimensionUnitSet cannot safely implement "
                        "set difference.")


class DimensionJoinSet(DimensionSet):
    r"""A concrete `DimensionSet` for `DimensionJoin` instances.

    `DimensionJoinSet`\s are indirectly topologically sorted according to the
    `DimensionUnit`\s they depend on: a `DimensionJoin` "A" appears before "B"
    if the last of its (topologically sorted) dependencies appear before
    the last of B's dependencies.  Ties are guaranteed to be broken
    deterministically.
    """

    def __repr__(self):
        return f"DimensionJoinSet({self})"

    def _populate(self, names):
        names = set(names)
        for dimension in self.universe._joins:
            if dimension.name in names:
                self._elements[dimension.name] = dimension
                names.remove(dimension.name)
        if names:
            raise LookupError(f"Unrecognized name(s): {names}")

    def symmetric_difference(self, other):
        """Return a new set containing all elements that are in either ``self``
        or other, but not both.

        Parameters
        ----------
        other : `DimensionJoinSet`
            The other set from which to draw potential result elements.

        Returns
        -------
        result : `DimensionJoinSet`
            A new set containing the XOR of ``self`` and ``other``.
        """
        names = self.names ^ other.names
        return type(self)(self.universe, names)

    def difference(self, other):
        """Return a new set containing all elements that are in ``self``
        but not other.

        Parameters
        ----------
        other : `DimensionJoinSet`
            The other set containing elements that should not be included
            in the result.

        Returns
        -------
        result : `DimensionJoinSet`
            A new set containing elements in ``self`` but not ``other``.
        """
        names = self.names - other.names
        return type(self)(self.universe, names)

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

__all__ = ("DimensionSet",)

from collections.abc import Set
from collections import OrderedDict
from .elements import DimensionElement
from ..utils import PrivateConstructorMeta


def toNameSet(container):
    if isinstance(container, str):
        # Friendly error for easy mistake, e.g. union("Name") instead of union(["Name"])
        raise TypeError("Argument must be an *iterable* over `DimensionElement` or `str`; got `str`")
    try:
        return container.names
    except AttributeError:
        pass
    return frozenset(element.name if isinstance(element, DimensionElement) else element
                     for element in container)


@Set.register
class DimensionSet(metaclass=PrivateConstructorMeta):
    r"""A special set/dict hybrid base class for `DimensionElement` objects.

    This class is an abstract base class and cannot be constructed or used
    directly.  See the `DimensionUnitSet` and `DimensionJoinSet` subclasses
    for more information.

    `DimensionSet` objects implement at least most of the
    `collections.abc.Set` interface (not all subclasses implement
    `symmetric_difference` and `difference`). Like `frozenset`, they are
    immutable and hashable, and also provide named-method versions of most
    operators.  Unlike Python sets, they are deterministically sorted.

    Because the `DimensionElement` objects they hold always have a name,
    `DimensionSet` also supports some more `dict`-like operations: elements
    can be retrieved by name using regular square-bracket indexing
    (`__getitem__`) or `get`, and the ``in`` operator (`__contains__`) can be
    used with both `Dimension` instances and their string names. The `names`
    attribute can also be used to obtain a `set`-like object containing those
    names.

    Like all other dimension objects, `DimensionSet`\s are associated with a
    special "universe" `DimensionGraph`.  All `Dimension`\s in the set must be
    associated with the same universe.

    """

    def __init__(self, parent, elements, expand=False, optional=False):
        self._parent = parent if parent is not None else self
        self._elements = OrderedDict()

        def toPairs(elems):
            for elem in elems:
                if not isinstance(elem, DimensionElement):
                    elem = self._parent[elem]
                yield (elem.name, elem)
                if expand:
                    yield from toPairs(elem.dependencies(optional=optional))

        names = dict(toPairs(elements))
        for candidate in self._parent:
            if names.pop(candidate.name, None):
                self._elements[candidate.name] = candidate

    def __len__(self):
        return len(self._elements)

    def __iter__(self):
        return iter(self._elements.values())

    def __str__(self):
        return "{{{}}}".format(", ".join(self._elements.keys()))

    def __repr__(self):
        return f"DimensionSet({self})"

    def __contains__(self, key):
        try:
            return key.name in self._elements
        except AttributeError:
            return key in self._elements

    def __getitem__(self, name):
        return self._elements[name]

    def get(self, name, default=None):
        """Return the element with the given ``name``, or ``default`` if it
        does not exist.
        """
        return self._elements.get(name, default)

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

    def expanded(self, optional=False):
        """Return a new `DimensionSet` that has been expanded to include
        dependencies.
        """
        return DimensionSet(self._parent, self, expand=True, optional=optional)

    def __hash__(self):
        return hash(self.names)

    def __eq__(self, other):
        return self.names == toNameSet(other)

    def __le__(self, other):
        return self.names <= toNameSet(other)

    def __lt__(self, other):
        return self.names < toNameSet(other)

    def __ge__(self, other):
        return self.names >= toNameSet(other)

    def __gt__(self, other):
        return self.names > toNameSet(other)

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
        return self.names.isdisjoint(toNameSet(other))

    def union(self, *others):
        """Return a new set containing all elements that are in ``self`` or
        any of the other given sets.

        Parameters
        ----------
        *others : iterable over `Dimension` or `str` name.
            Other sets whose elements should be included in the result.

        Returns
        -------
        result : instance of a subclass of `DimensionSet`
            A new set of the same type as ``self`` containing all elements
            in any input set.
        """
        names = set(self.names)
        for other in others:
            names |= toNameSet(other)
        return DimensionSet._construct(self._parent, names)

    def intersection(self, *others):
        """Return a new set containing all elements that are in both  ``self``
        and all of the other given sets.

        Parameters
        ----------
        *others : iterable over `Dimension` or `str` name.
            Other sets whose elements may be included in the result.

        Returns
        -------
        result : instance of a subclass of `DimensionSet`
            A new set of the same type as ``self`` containing any elements
            in all input sets.
        """
        names = set(self.names)
        for other in others:
            names &= toNameSet(other)
        return DimensionSet._construct(self._parent, names)

    def symmetric_difference(self, other):
        """Return a new set containing all elements that are in either ``self``
        or other, but not both.

        Parameters
        ----------
        *others : iterable over `Dimension` or `str` name.
            The other set from which to draw potential result elements.

        Returns
        -------
        result : `DimensionSet`
            A new set containing the XOR of ``self`` and ``other``.
        """
        names = self.names ^ toNameSet(other)
        return DimensionSet._construct(self._parent, names)

    def difference(self, other):
        """Return a new set containing all elements that are in ``self``
        but not other.

        Parameters
        ----------
        *others : iterable over `Dimension` or `str` name.
            The other set containing elements that should not be included
            in the result.

        Returns
        -------
        result : `DimensionSet`
            A new set containing elements in ``self`` but not ``other``.
        """
        names = self.names - toNameSet(other)
        return DimensionSet._construct(self._parent, names)

    # Operators that return sets are only enabled for DimensionSet operands on
    # both sides, to avoid confusion about return types.

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

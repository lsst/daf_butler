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

__all__ = ["DimensionGroup"]

import itertools
from collections.abc import Iterable, Iterator, Mapping, Set
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

from lsst.utils.classes import cached_getter, immutable

from .._named import NamedValueAbstractSet, NamedValueSet
from .._topology import TopologicalFamily, TopologicalSpace

if TYPE_CHECKING:  # Imports needed only for type annotations; may be circular.
    from ._elements import DimensionElement
    from ._graph import DimensionGraph
    from ._universe import DimensionUniverse


class SortedSequenceSet(Set[str]):
    """A set-like interface wrapper around a tuple.

    This delegates directly to ``tuple.__contains__``, so there is an implicit
    assumption that `len` is small and hence O(N) lookups are not a problem, as
    is the case for sets of dimension names.
    """

    def __init__(self, seq: tuple[str, ...]):
        self._seq = seq

    __slots__ = ("_seq",)

    def __contains__(self, x: object) -> bool:
        return x in self._seq

    def __iter__(self) -> Iterator[str]:
        return iter(self._seq)

    def __len__(self) -> int:
        return len(self._seq)

    def __hash__(self) -> int:
        return hash(self._seq)

    def __eq__(self, other: object) -> bool:
        if seq := getattr(other, "_seq", None):
            return seq == self._seq
        return super().__eq__(other)

    @classmethod
    def _from_iterable(cls, iterable: Iterable[str]) -> set[str]:
        # This is used by collections.abc.Set mixin methods when they need
        # to return a new object (e.g. in `__and__`).
        return set(iterable)

    def __repr__(self) -> str:
        return f"{{{', '.join(str(k) for k in self._seq)}}}"

    def as_tuple(self) -> tuple[str, ...]:
        """Return the underlying tuple."""
        return self._seq

    @property
    def names(self) -> Set[str]:
        """An alias to ``self``.

        This is a backwards-compatibility API that allows `DimensionGroup`
        to mimic the `DimensionGraph` object it is intended to replace, by
        permitting expressions like ``x.required.names`` when ``x`` can be
        an object of either type.
        """
        return self


@immutable
class DimensionGroup:
    """An immutable, dependency-complete collection of dimensions.

    `DimensionGroup` behaves in many respects like a set of `str` dimension
    names that maintains several special subsets and supersets of related
    dimension elements.  It does not fully implement the `collections.abc.Set`
    interface, because it defines a few different iteration orders and does not
    privilege any one of them by implementing ``__iter__``.

    Parameters
    ----------
    universe : `DimensionUniverse`
        Object that manages all known dimensions.
    names : iterable of `str`, optional
        An iterable of the names of dimensions that must be included in the
        group.  All (recursive) dependencies of these dimensions will also be
        included.  At most one of ``dimensions`` and ``names`` must be
        provided.
    _conform : `bool`, optional
        If `True` (default), expand to include dependencies.  `False` should
        only be used for callers that can guarantee that other arguments are
        already correctly expanded, and is for internal use only.

    Notes
    -----
    `DimensionGroup` should be used instead of other collections in most
    contexts where a collection of dimensions is required and a
    `DimensionUniverse` is available.  Exceptions include cases where order
    matters (and is different from the consistent ordering defined by the
    `DimensionUniverse`), or complete `~collection.abc.Set` semantics are
    required.
    """

    def __new__(
        cls,
        universe: DimensionUniverse,
        names: Iterable[str] | DimensionGroup = frozenset(),
        _conform: bool = True,
    ) -> DimensionGroup:
        if isinstance(names, DimensionGroup):
            if names.universe is universe:
                return names
            else:
                names = names.names
        if _conform:
            # Expand dimension names to include all required and implied
            # dependencies.
            to_expand = set(names)
            names = set()
            while to_expand:
                dimension = universe[to_expand.pop()]
                names.add(dimension.name)
                to_expand.update(dimension.required.names)
                to_expand.update(dimension.implied.names)
                to_expand.difference_update(names)
        else:
            names = frozenset(names)
        # Look in the cache of existing groups, with the expanded set of names.
        cache_key = frozenset(names)
        self = universe._cached_groups.get(cache_key, None)
        if self is not None:
            return self
        # This is apparently a new group.  Create it, and add it to the cache.
        self = super().__new__(cls)
        universe._cached_groups[cache_key] = self
        self.universe = universe
        # Reorder dimensions by iterating over the universe (which is
        # ordered already) and extracting the ones in the set.
        self.names = SortedSequenceSet(tuple(d.name for d in universe.sorted(names)))
        # Make a set that includes both the dimensions and any
        # DimensionElements whose dependencies are in self.dimensions.
        self.elements = SortedSequenceSet(
            tuple(e.name for e in universe.elements if e.required.names <= self.names)
        )
        self.governors = SortedSequenceSet(
            tuple(d for d in self.names if d in universe.governor_dimensions.names)
        )
        self.skypix = SortedSequenceSet(tuple(d for d in self.names if d in universe.skypix_dimensions.names))
        # Split dependencies up into "required" and "implied" subsets.
        # Note that a dimension may be required in one group and implied in
        # another.
        required: list[str] = []
        implied: list[str] = []
        for dim1 in self.names:
            for dim2 in self.names:
                if dim1 in universe[dim2].implied.names:
                    implied.append(dim1)
                    break
            else:
                # If no other dimension implies dim1, it's required.
                required.append(dim1)
        self.required = SortedSequenceSet(tuple(required))
        self.implied = SortedSequenceSet(tuple(implied))

        self._space_families = MappingProxyType(
            {
                space: NamedValueSet(
                    universe[e].topology[space] for e in self.elements if space in universe[e].topology
                ).freeze()
                for space in TopologicalSpace.__members__.values()
            }
        )

        # Build mappings from dimension to index; this is really for
        # DataCoordinate, but we put it in DimensionGroup because many (many!)
        # DataCoordinates will share the same DimensionGroup, and we want them
        # to be lightweight.  The order here is what's convenient for
        # DataCoordinate: all required dimensions before all implied
        # dimensions.
        self._data_coordinate_indices = {
            name: i for i, name in enumerate(itertools.chain(self.required, self.implied))
        }
        return self

    def __getnewargs__(self) -> tuple:
        return (self.universe, self.names._seq, False)

    def __deepcopy__(self, memo: dict) -> DimensionGroup:
        # DimensionGroup is recursively immutable; see note in @immutable
        # decorator.
        return self

    def __len__(self) -> int:
        return len(self.names)

    def __contains__(self, element: str) -> bool:
        if element in self.elements:
            return True
        else:
            from ._elements import DimensionElement

            if isinstance(element, DimensionElement):  # type: ignore[unreachable]
                raise TypeError(
                    "DimensionGroup does not support membership tests using DimensionElement "
                    "instances; use their names instead."
                )
            return False

    def __str__(self) -> str:
        return str(self.names)

    def __repr__(self) -> str:
        return f"DimensionGroup({self.names})"

    def as_group(self) -> DimensionGroup:
        """Return ``self``.

        This is a backwards-compatibility API that allows both `DimensionGraph`
        and `DimensionGroup` to be coerced to the latter.
        """
        return self

    @cached_getter
    def _as_graph(self) -> DimensionGraph:
        """Return a view of ``self`` as a `DimensionGraph`.

        This is provided as a convenience for methods and properties that must
        return a `DimensionGraph` for backwards compatibility (until v27).  It
        is the only way of making a `DimensionGraph` that does not produce
        a warning.
        """
        from ._graph import DimensionGraph

        result = object.__new__(DimensionGraph)
        result._group = self
        return result

    def isdisjoint(self, other: DimensionGroup) -> bool:
        """Test whether the intersection of two groups is empty.

        Returns `True` if either operand is the empty.
        """
        return self.names.isdisjoint(other.names)

    def issubset(self, other: DimensionGroup) -> bool:
        """Test whether all dimensions in ``self`` are also in ``other``.

        Returns `True` if ``self`` is empty.
        """
        return self.names <= other.names

    def issuperset(self, other: DimensionGroup) -> bool:
        """Test whether all dimensions in ``other`` are also in ``self``.

        Returns `True` if ``other`` is empty.
        """
        return self.names >= other.names

    def __eq__(self, other: Any) -> bool:
        from ._graph import DimensionGraph

        # TODO: Drop DimensionGraph support here on DM-41326.
        if isinstance(other, (DimensionGroup, DimensionGraph)):
            return self.names == other.names
        else:
            return False

    def __hash__(self) -> int:
        return hash(self.required._seq)

    def __le__(self, other: DimensionGroup) -> bool:
        return self.names <= other.names

    def __ge__(self, other: DimensionGroup) -> bool:
        return self.names >= other.names

    def __lt__(self, other: DimensionGroup) -> bool:
        return self.names < other.names

    def __gt__(self, other: DimensionGroup) -> bool:
        return self.names > other.names

    def union(self, *others: DimensionGroup) -> DimensionGroup:
        """Construct a new group with all dimensions in any of the operands.

        The elements of the returned group may exceed the naive union of their
        elements, as some dimension elements are included in groups whenever
        multiple dimensions are present, and those dependency dimensions could
        have been provided by different operands.
        """
        names = set(self.names).union(*[other.names for other in others])
        return DimensionGroup(self.universe, names)

    def intersection(self, *others: DimensionGroup) -> DimensionGroup:
        """Construct a new group with only dimensions in all of the operands.

        See also `union`.
        """
        names = set(self.names).intersection(*[other.names for other in others])
        return DimensionGroup(self.universe, names=names)

    def __or__(self, other: DimensionGroup) -> DimensionGroup:
        return self.union(other)

    def __and__(self, other: DimensionGroup) -> DimensionGroup:
        return self.intersection(other)

    @property
    def data_coordinate_keys(self) -> Set[str]:
        """A set of dimensions ordered like `DataCoordinate.mapping`.

        This order is defined as all required dimensions followed by all
        implied dimensions.
        """
        return self._data_coordinate_indices.keys()

    @property
    @cached_getter
    def lookup_order(self) -> tuple[str, ...]:
        """A tuple of all elements in the order needed to find their records.

        Unlike the table definition/topological order (which is what
        `DimensionUniverse.sorted` gives you), when dimension A implies
        dimension B, dimension A appears first.
        """
        done: set[str] = set()
        order: list[str] = []

        def add_to_order(element: DimensionElement) -> None:
            if element.name in done:
                return
            predecessors = set(element.required.names)
            predecessors.discard(element.name)
            if not done.issuperset(predecessors):
                return
            order.append(element.name)
            done.add(element.name)
            for other in element.implied:
                add_to_order(other)

        while not done.issuperset(self.required):
            for dimension in self.required:
                add_to_order(self.universe[dimension])

        order.extend(element for element in self.elements if element not in done)
        return tuple(order)

    @property
    def spatial(self) -> NamedValueAbstractSet[TopologicalFamily]:
        """Families represented by the spatial elements in this graph."""
        return self._space_families[TopologicalSpace.SPATIAL]

    @property
    def temporal(self) -> NamedValueAbstractSet[TopologicalFamily]:
        """Families represented by the temporal elements in this graph."""
        return self._space_families[TopologicalSpace.TEMPORAL]

    # Class attributes below are shadowed by instance attributes, and are
    # present just to hold the docstrings for those instance attributes.

    universe: DimensionUniverse
    """The set of all known dimensions, of which this group is a subset
    (`DimensionUniverse`).
    """

    names: SortedSequenceSet
    """A true `~collections.abc.Set` of the dimension names.

    Iteration order is consist with `DimensionUniverse.sorted`: each dimension
    is preceded by its required and implied dependencies.
    """

    elements: SortedSequenceSet
    """A true `~collections.abc.Set` of all dimension element names in the
    group; a superset of `dimensions`.
    """

    governors: SortedSequenceSet
    """A true `~collections.abc.Set` of all governor dimension names in the
    group.
    """

    skypix: SortedSequenceSet
    """A true `~collections.abc.Set` of all skypix dimension names in the "
    group.
    """

    required: SortedSequenceSet
    """The dimensions that must be directly identified via their primary keys
    in a data ID in order to identify the rest of the elements in the group.
    """

    implied: SortedSequenceSet
    """The dimensions that need not be directly identified via their primary
    keys in a data ID.
    """

    _space_families: Mapping[TopologicalSpace, NamedValueAbstractSet[TopologicalFamily]]
    """Families of elements in this graph that exist in topological spaces
    relationships (`~collections.abc.Mapping` from `TopologicalSpace` to
    `NamedValueAbstractSet` of `TopologicalFamily`).
    """

    _data_coordinate_indices: dict[str, int]

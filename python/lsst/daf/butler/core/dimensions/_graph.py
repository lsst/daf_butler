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

__all__ = ["DimensionGraph", "SerializedDimensionGraph"]

import itertools
from types import MappingProxyType
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
    Union,
)

from lsst.utils.classes import cached_getter, immutable
from pydantic import BaseModel

from .._topology import TopologicalFamily, TopologicalSpace
from ..json import from_json_pydantic, to_json_pydantic
from ..named import NamedValueAbstractSet, NamedValueSet

if TYPE_CHECKING:  # Imports needed only for type annotations; may be circular.
    from ...registry import Registry
    from ._elements import Dimension, DimensionElement
    from ._governor import GovernorDimension
    from ._universe import DimensionUniverse


class SerializedDimensionGraph(BaseModel):
    """Simplified model of a `DimensionGraph` suitable for serialization."""

    names: List[str]

    @classmethod
    def direct(cls, *, names: List[str]) -> SerializedDimensionGraph:
        """Construct a `SerializedDimensionGraph` directly without validators.

        This differs from the pydantic "construct" method in that the arguments
        are explicitly what the model requires, and it will recurse through
        members, constructing them from their corresponding `direct` methods.

        This method should only be called when the inputs are trusted.
        """
        node = SerializedDimensionGraph.__new__(cls)
        object.__setattr__(node, "names", names)
        object.__setattr__(node, "__fields_set__", {"names"})
        return node


@immutable
class DimensionGraph:
    """An immutable, dependency-complete collection of dimensions.

    `DimensionGraph` behaves in many respects like a set of `Dimension`
    instances that maintains several special subsets and supersets of
    related `DimensionElement` instances.  It does not fully implement the
    `collections.abc.Set` interface, as its automatic expansion of dependencies
    would make set difference and XOR operations behave surprisingly.

    It also provides dict-like lookup of `DimensionElement` instances from
    their names.

    Parameters
    ----------
    universe : `DimensionUniverse`
        The special graph of all known dimensions of which this graph will be
        a subset.
    dimensions : iterable of `Dimension`, optional
        An iterable of `Dimension` instances that must be included in the
        graph.  All (recursive) dependencies of these dimensions will also
        be included.  At most one of ``dimensions`` and ``names`` must be
        provided.
    names : iterable of `str`, optional
        An iterable of the names of dimensions that must be included in the
        graph.  All (recursive) dependencies of these dimensions will also
        be included.  At most one of ``dimensions`` and ``names`` must be
        provided.
    conform : `bool`, optional
        If `True` (default), expand to include dependencies.  `False` should
        only be used for callers that can guarantee that other arguments are
        already correctly expanded, and is primarily for internal use.

    Notes
    -----
    `DimensionGraph` should be used instead of other collections in most
    contexts where a collection of dimensions is required and a
    `DimensionUniverse` is available.  Exceptions include cases where order
    matters (and is different from the consistent ordering defined by the
    `DimensionUniverse`), or complete `~collection.abc.Set` semantics are
    required.
    """

    _serializedType = SerializedDimensionGraph

    def __new__(
        cls,
        universe: DimensionUniverse,
        dimensions: Optional[Iterable[Dimension]] = None,
        names: Optional[Iterable[str]] = None,
        conform: bool = True,
    ) -> DimensionGraph:
        conformedNames: Set[str]
        if names is None:
            if dimensions is None:
                conformedNames = set()
            else:
                try:
                    # Optimize for NamedValueSet/NamedKeyDict, though that's
                    # not required.
                    conformedNames = set(dimensions.names)  # type: ignore
                except AttributeError:
                    conformedNames = set(d.name for d in dimensions)
        else:
            if dimensions is not None:
                raise TypeError("Only one of 'dimensions' and 'names' may be provided.")
            conformedNames = set(names)
        if conform:
            universe.expandDimensionNameSet(conformedNames)
        # Look in the cache of existing graphs, with the expanded set of names.
        cacheKey = frozenset(conformedNames)
        self = universe._cache.get(cacheKey, None)
        if self is not None:
            return self
        # This is apparently a new graph.  Create it, and add it to the cache.
        self = super().__new__(cls)
        universe._cache[cacheKey] = self
        self.universe = universe
        # Reorder dimensions by iterating over the universe (which is
        # ordered already) and extracting the ones in the set.
        self.dimensions = NamedValueSet(universe.sorted(conformedNames)).freeze()
        # Make a set that includes both the dimensions and any
        # DimensionElements whose dependencies are in self.dimensions.
        self.elements = NamedValueSet(
            e for e in universe.getStaticElements() if e.required.names <= self.dimensions.names
        ).freeze()
        self._finish()
        return self

    def _finish(self) -> None:
        # Make a set containing just the governor dimensions in this graph.
        # Need local import to avoid cycle.
        from ._governor import GovernorDimension

        self.governors = NamedValueSet(
            d for d in self.dimensions if isinstance(d, GovernorDimension)
        ).freeze()
        # Split dependencies up into "required" and "implied" subsets.
        # Note that a dimension may be required in one graph and implied in
        # another.
        required: NamedValueSet[Dimension] = NamedValueSet()
        implied: NamedValueSet[Dimension] = NamedValueSet()
        for i1, dim1 in enumerate(self.dimensions):
            for i2, dim2 in enumerate(self.dimensions):
                if dim1.name in dim2.implied.names:
                    implied.add(dim1)
                    break
            else:
                # If no other dimension implies dim1, it's required.
                required.add(dim1)
        self.required = required.freeze()
        self.implied = implied.freeze()

        self.topology = MappingProxyType(
            {
                space: NamedValueSet(e.topology[space] for e in self.elements if space in e.topology).freeze()
                for space in TopologicalSpace.__members__.values()
            }
        )

        # Build mappings from dimension to index; this is really for
        # DataCoordinate, but we put it in DimensionGraph because many
        # (many!) DataCoordinates will share the same DimensionGraph, and
        # we want them to be lightweight.  The order here is what's convenient
        # for DataCoordinate: all required dimensions before all implied
        # dimensions.
        self._dataCoordinateIndices: Dict[str, int] = {
            name: i for i, name in enumerate(itertools.chain(self.required.names, self.implied.names))
        }

    def __getnewargs__(self) -> tuple:
        return (self.universe, None, tuple(self.dimensions.names), False)

    def __deepcopy__(self, memo: dict) -> DimensionGraph:
        # DimensionGraph is recursively immutable; see note in @immutable
        # decorator.
        return self

    @property
    def names(self) -> AbstractSet[str]:
        """Set of the names of all dimensions in the graph (`KeysView`)."""
        return self.dimensions.names

    def to_simple(self, minimal: bool = False) -> SerializedDimensionGraph:
        """Convert this class to a simple python type.

        This type is suitable for serialization.

        Parameters
        ----------
        minimal : `bool`, optional
            Use minimal serialization. Has no effect on for this class.

        Returns
        -------
        names : `list`
            The names of the dimensions.
        """
        # Names are all we can serialize.
        return SerializedDimensionGraph(names=list(self.names))

    @classmethod
    def from_simple(
        cls,
        names: SerializedDimensionGraph,
        universe: Optional[DimensionUniverse] = None,
        registry: Optional[Registry] = None,
    ) -> DimensionGraph:
        """Construct a new object from the simplified form.

        This is assumed to support data data returned from the `to_simple`
        method.

        Parameters
        ----------
        names : `list` of `str`
            The names of the dimensions.
        universe : `DimensionUniverse`
            The special graph of all known dimensions of which this graph will
            be a subset. Can be `None` if `Registry` is provided.
        registry : `lsst.daf.butler.Registry`, optional
            Registry from which a universe can be extracted. Can be `None`
            if universe is provided explicitly.

        Returns
        -------
        graph : `DimensionGraph`
            Newly-constructed object.
        """
        if universe is None and registry is None:
            raise ValueError("One of universe or registry is required to convert names to a DimensionGraph")
        if universe is None and registry is not None:
            universe = registry.dimensions
        if universe is None:
            # this is for mypy
            raise ValueError("Unable to determine a usable universe")

        return cls(names=names.names, universe=universe)

    to_json = to_json_pydantic
    from_json = classmethod(from_json_pydantic)

    def __iter__(self) -> Iterator[Dimension]:
        """Iterate over all dimensions in the graph.

        (and true `Dimension` instances only).
        """
        return iter(self.dimensions)

    def __len__(self) -> int:
        """Return the number of dimensions in the graph.

        (and true `Dimension` instances only).
        """
        return len(self.dimensions)

    def __contains__(self, element: Union[str, DimensionElement]) -> bool:
        """Return `True` if the given element or element name is in the graph.

        This test covers all `DimensionElement` instances in ``self.elements``,
        not just true `Dimension` instances).
        """
        return element in self.elements

    def __getitem__(self, name: str) -> DimensionElement:
        """Return the element with the given name.

        This lookup covers all `DimensionElement` instances in
        ``self.elements``, not just true `Dimension` instances).
        """
        return self.elements[name]

    def get(self, name: str, default: Any = None) -> DimensionElement:
        """Return the element with the given name.

        This lookup covers all `DimensionElement` instances in
        ``self.elements``, not just true `Dimension` instances).
        """
        return self.elements.get(name, default)

    def __str__(self) -> str:
        return str(self.dimensions)

    def __repr__(self) -> str:
        return f"DimensionGraph({str(self)})"

    def isdisjoint(self, other: DimensionGraph) -> bool:
        """Test whether the intersection of two graphs is empty.

        Returns `True` if either operand is the empty.
        """
        return self.dimensions.isdisjoint(other.dimensions)

    def issubset(self, other: DimensionGraph) -> bool:
        """Test whether all dimensions in ``self`` are also in ``other``.

        Returns `True` if ``self`` is empty.
        """
        return self.dimensions <= other.dimensions

    def issuperset(self, other: DimensionGraph) -> bool:
        """Test whether all dimensions in ``other`` are also in ``self``.

        Returns `True` if ``other`` is empty.
        """
        return self.dimensions >= other.dimensions

    def __eq__(self, other: Any) -> bool:
        """Test the arguments have exactly the same dimensions & elements."""
        if isinstance(other, DimensionGraph):
            return self.dimensions == other.dimensions
        else:
            return False

    def __hash__(self) -> int:
        return hash(tuple(self.dimensions.names))

    def __le__(self, other: DimensionGraph) -> bool:
        """Test whether ``self`` is a subset of ``other``."""
        return self.dimensions <= other.dimensions

    def __ge__(self, other: DimensionGraph) -> bool:
        """Test whether ``self`` is a superset of ``other``."""
        return self.dimensions >= other.dimensions

    def __lt__(self, other: DimensionGraph) -> bool:
        """Test whether ``self`` is a strict subset of ``other``."""
        return self.dimensions < other.dimensions

    def __gt__(self, other: DimensionGraph) -> bool:
        """Test whether ``self`` is a strict superset of ``other``."""
        return self.dimensions > other.dimensions

    def union(self, *others: DimensionGraph) -> DimensionGraph:
        """Construct a new graph with all dimensions in any of the operands.

        The elements of the returned graph may exceed the naive union of
        their elements, as some `DimensionElement` instances are included
        in graphs whenever multiple dimensions are present, and those
        dependency dimensions could have been provided by different operands.
        """
        names = set(self.names).union(*[other.names for other in others])
        return DimensionGraph(self.universe, names=names)

    def intersection(self, *others: DimensionGraph) -> DimensionGraph:
        """Construct a new graph with only dimensions in all of the operands.

        See also `union`.
        """
        names = set(self.names).intersection(*[other.names for other in others])
        return DimensionGraph(self.universe, names=names)

    def __or__(self, other: DimensionGraph) -> DimensionGraph:
        """Construct a new graph with all dimensions in any of the operands.

        See `union`.
        """
        return self.union(other)

    def __and__(self, other: DimensionGraph) -> DimensionGraph:
        """Construct a new graph with only dimensions in all of the operands.

        See `intersection`.
        """
        return self.intersection(other)

    @property
    @cached_getter
    def primaryKeyTraversalOrder(self) -> Tuple[DimensionElement, ...]:
        """Return a tuple of all elements in specific order.

        The order allows records to be
        found given their primary keys, starting from only the primary keys of
        required dimensions (`tuple` [ `DimensionRecord` ]).

        Unlike the table definition/topological order (which is what
        DimensionUniverse.sorted gives you), when dimension A implies
        dimension B, dimension A appears first.
        """
        done: Set[str] = set()
        order = []

        def addToOrder(element: DimensionElement) -> None:
            if element.name in done:
                return
            predecessors = set(element.required.names)
            predecessors.discard(element.name)
            if not done.issuperset(predecessors):
                return
            order.append(element)
            done.add(element.name)
            for other in element.implied:
                addToOrder(other)

        while not done.issuperset(self.required):
            for dimension in self.required:
                addToOrder(dimension)

        order.extend(element for element in self.elements if element.name not in done)
        return tuple(order)

    @property
    def spatial(self) -> NamedValueAbstractSet[TopologicalFamily]:
        """Families represented by the spatial elements in this graph."""
        return self.topology[TopologicalSpace.SPATIAL]

    @property
    def temporal(self) -> NamedValueAbstractSet[TopologicalFamily]:
        """Families represented by the temporal elements in this graph."""
        return self.topology[TopologicalSpace.TEMPORAL]

    # Class attributes below are shadowed by instance attributes, and are
    # present just to hold the docstrings for those instance attributes.

    universe: DimensionUniverse
    """The set of all known dimensions, of which this graph is a subset
    (`DimensionUniverse`).
    """

    dimensions: NamedValueAbstractSet[Dimension]
    """A true `~collections.abc.Set` of all true `Dimension` instances in the
    graph (`NamedValueAbstractSet` of `Dimension`).

    This is the set used for iteration, ``len()``, and most set-like operations
    on `DimensionGraph` itself.
    """

    elements: NamedValueAbstractSet[DimensionElement]
    """A true `~collections.abc.Set` of all `DimensionElement` instances in the
    graph; a superset of `dimensions` (`NamedValueAbstractSet` of
    `DimensionElement`).

    This is the set used for dict-like lookups, including the ``in`` operator,
    on `DimensionGraph` itself.
    """

    governors: NamedValueAbstractSet[GovernorDimension]
    """A true `~collections.abc.Set` of all true `GovernorDimension` instances
    in the graph (`NamedValueAbstractSet` of `GovernorDimension`).
    """

    required: NamedValueAbstractSet[Dimension]
    """The subset of `dimensions` whose elements must be directly identified
    via their primary keys in a data ID in order to identify the rest of the
    elements in the graph (`NamedValueAbstractSet` of `Dimension`).
    """

    implied: NamedValueAbstractSet[Dimension]
    """The subset of `dimensions` whose elements need not be directly
    identified via their primary keys in a data ID (`NamedValueAbstractSet` of
    `Dimension`).
    """

    topology: Mapping[TopologicalSpace, NamedValueAbstractSet[TopologicalFamily]]
    """Families of elements in this graph that can participate in topological
    relationships (`Mapping` from `TopologicalSpace` to
    `NamedValueAbstractSet` of `TopologicalFamily`).
    """

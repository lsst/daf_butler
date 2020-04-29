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

__all__ = ["DimensionGraph"]

from typing import Optional, Iterable, Iterator, KeysView, Union, Any, Tuple, TYPE_CHECKING

from ..utils import NamedValueSet, NamedKeyDict, immutable

if TYPE_CHECKING:  # Imports needed only for type annotations; may be circular.
    from .universe import DimensionUniverse
    from .elements import DimensionElement, Dimension


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
        An iterable of the names of dimensiosn that must be included in the
        graph.  All (recursive) dependencies of these dimensions will also
        be included.  At most one of ``dimensions`` and ``names`` must be
        provided.
    conform : `bool`, optional
        If `True` (default), expand to include dependencies.  `False` should
        only be used for callers that can guarantee that other arguments are
        already correctly expanded, and is primarily for internal use.

    Notes
    -----
    `DimensionGraph` should be used instead of other collections in any context
    where a collection of dimensions is required and a `DimensionUniverse` is
    available.

    While `DimensionUniverse` inherits from `DimensionGraph`, it should
    otherwise not be used as a base class.
    """

    def __new__(cls, universe: DimensionUniverse,
                dimensions: Optional[Iterable[Dimension]] = None,
                names: Optional[Iterable[str]] = None,
                conform: bool = True) -> DimensionGraph:
        if names is None:
            if dimensions is None:
                names = ()
            else:
                try:
                    names = set(dimensions.names)
                except AttributeError:
                    names = set(d.name for d in dimensions)
        else:
            if dimensions is not None:
                raise TypeError("Only one of 'dimensions' and 'names' may be provided.")
            names = set(names)
        if conform:
            # Expand given dimensions to include all dependencies.
            for name in tuple(names):  # iterate over a temporary copy so we can modify the original
                names.update(universe[name]._related.dependencies)
        # Look in the cache of existing graphs, with the expanded set of names.
        cacheKey = frozenset(names)
        self = universe._cache.get(cacheKey, None)
        if self is not None:
            return self
        # This is apparently a new graph.  Create it, and add it to the cache.
        self = super().__new__(cls)
        universe._cache[cacheKey] = self
        self.universe = universe
        # Reorder dimensions by iterating over the universe (which is
        # ordered already) and extracting the ones in the set.
        self.dimensions = NamedValueSet(d for d in universe.dimensions if d.name in names)
        # Make a set that includes both the dimensions and any
        # DimensionElements whose dependencies are in self.dimensions.
        self.elements = NamedValueSet(e for e in universe.elements
                                      if e._shouldBeInGraph(self.dimensions.names))
        self._finish()
        return self

    def _finish(self):
        """Complete construction of the graph.

        This is intended for internal use by `DimensionGraph` and
        `DimensionUniverse` only.
        """
        # Freeze the sets the constructor is responsible for populating.
        self.dimensions.freeze()
        self.elements.freeze()

        # Split dependencies up into "required" and "implied" subsets.
        # Note that a dimension may be required in one graph and implied in
        # another.
        self.required = NamedValueSet()
        self.implied = NamedValueSet()
        for i1, dim1 in enumerate(self.dimensions):
            for i2, dim2 in enumerate(self.dimensions):
                if dim1.name in dim2._related.implied:
                    self.implied.add(dim1)
                    break
            else:
                # If no other dimension implies dim1, it's required.
                self.required.add(dim1)
        self.required.freeze()
        self.implied.freeze()

        # Compute sets of spatial and temporal elements.
        # This contain the values of the `.spatial` and `.temporal` attributes
        # of all elements, unless those attributes are not in the graph.
        # In that case, the element whose attribute is not in the graph is
        # added instead.  This ensures that these sets contain the
        # most-specific spatial and temporal elements, not the summary elements
        # that aggregate them, unless the summaries are all that we have.
        self.spatial = NamedValueSet()
        self.temporal = NamedValueSet()
        for element in self.elements:
            if element.spatial is not None:
                if element.spatial in self.elements:
                    self.spatial.add(element.spatial)
                else:
                    self.spatial.add(element)
            if element.temporal is not None:
                if element.temporal in self.elements:
                    self.temporal.add(element.temporal)
                else:
                    self.temporal.add(element)
        self.spatial.freeze()
        self.temporal.freeze()

        # Build mappings from dimension to index; this is really for
        # DataCoordinate, but we put it in DimensionGraph because many
        # (many!) DataCoordinates will share the same DimensionGraph, and
        # we want them to be lightweight.
        self._requiredIndices = NamedKeyDict({dimension: i for i, dimension in enumerate(self.required)})
        self._dimensionIndices = NamedKeyDict({dimension: i for i, dimension in enumerate(self.dimensions)})
        self._elementIndices = NamedKeyDict({element: i for i, element in enumerate(self.elements)})

    def __getnewargs__(self) -> tuple:
        return (self.universe, None, tuple(self.dimensions.names), False)

    @property
    def names(self) -> KeysView[str]:
        """A set of the names of all dimensions in the graph (`KeysView`).
        """
        return self.dimensions.names

    def __iter__(self) -> Iterator[Dimension]:
        """Iterate over all dimensions in the graph (and true `Dimension`
        instances only).
        """
        return iter(self.dimensions)

    def __len__(self) -> int:
        """Return the number of dimensions in the graph (and true `Dimension`
        instances only).
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

    @classmethod
    def decode(cls, encoded: bytes, *, universe: DimensionUniverse) -> DimensionGraph:
        """Construct a `DimensionGraph` from its encoded representation.

        Parameters
        ----------
        encoded : `bytes`
            Byte string produced by `DimensionGraph.encode`.
        universe : `DimensionUniverse`
            Universe the new graph is a part of.  Must have the same dimensions
            as the original universe.

        Returns
        -------
        graph : `DimensionGraph`
            A new (or possibly cached) `DimensionGraph` instance matching the
            given encoding.
        """
        dimensions = []
        mask = int.from_bytes(encoded, "big")
        for dimension in universe.dimensions:
            index = universe._dimensionIndices[dimension]
            if mask & (1 << index):
                dimensions.append(dimension)
        return cls(universe, dimensions=dimensions, conform=False)

    def encode(self) -> bytes:
        """Encode a `DimensionGraph` into a byte string.

        Returns
        -------
        encoded : `bytes`
            Encoded representation of the graph.  Length is guaranteed to be
            equal to `DimensionUniverse.getEncodeLength`.
        """
        mask = 0
        for dimension in self.dimensions:
            index = self.universe._dimensionIndices[dimension]
            mask |= (1 << index)
        return mask.to_bytes(self.universe.getEncodeLength(), byteorder="big")

    def isdisjoint(self, other: DimensionGraph) -> bool:
        """Test whether the intersection of two graphs is empty.

        Returns `True` if either operand is the empty.
        """
        return self.dimensions.isdisjoint(other.dimensions)

    def issubset(self, other: DimensionGraph) -> bool:
        """Test whether all dimensions in ``self`` are also in ``other``.

        Returns `True` if ``self`` is empty.
        """
        return self.dimensions.issubset(other.dimensions)

    def issuperset(self, other: DimensionGraph) -> bool:
        """Test whether all dimensions in ``other`` are also in ``self``.

        Returns `True` if ``other`` is empty.
        """
        return self.dimensions.issuperset(other.dimensions)

    def __eq__(self, other: DimensionGraph) -> bool:
        """Test whether ``self`` and ``other`` have exactly the same dimensions
        and elements.
        """
        return self.dimensions == other.dimensions

    def __hash__(self) -> int:
        return hash(tuple(self.dimensions.names))

    def __le__(self, other: DimensionGraph) -> bool:
        """Test whether ``self`` is a subset of ``other``.
        """
        return self.dimensions <= other.dimensions

    def __ge__(self, other: DimensionGraph) -> bool:
        """Test whether ``self`` is a superset of ``other``.
        """
        return self.dimensions >= other.dimensions

    def __lt__(self, other: DimensionGraph) -> bool:
        """Test whether ``self`` is a strict subset of ``other``.
        """
        return self.dimensions < other.dimensions

    def __gt__(self, other: DimensionGraph) -> bool:
        """Test whether ``self`` is a strict superset of ``other``.
        """
        return self.dimensions > other.dimensions

    def union(self, *others: DimensionGraph):
        """Construct a new graph containing all dimensions in any of the
        operands.

        The elements of the returned graph may exceed the naive union of
        their elements, as some `DimensionElement` instances are included
        in graphs whenever multiple dimensions are present, and those
        dependency dimensions could have been provided by different operands.
        """
        names = set(self.names).union(*[other.names for other in others])
        return DimensionGraph(self.universe, names=names)

    def intersection(self, *others: DimensionGraph):
        """Construct a new graph containing only dimensions in all of the
        operands.
        """
        names = set(self.names).intersection(*[other.names for other in others])
        return DimensionGraph(self.universe, names=names)

    def __or__(self, other):
        """Construct a new graph containing all dimensions in any of the
        operands.

        See `union`.
        """
        return self.union(other)

    def __and__(self, other):
        """Construct a new graph containing only dimensions in all of the
        operands.
        """
        return self.intersection(other)

    @property
    def primaryKeyTraversalOrder(self) -> Tuple[DimensionElement]:
        """Return a tuple of all elements in an order allows records to be
        found given their primary keys, starting from only the primary keys of
        required dimensions (`tuple` [ `DimensionRecord` ]).

        Unlike the table definition/topological order (which is what
        DimensionUniverse.sorted gives you), when dimension A implies
        dimension B, dimension A appears first.
        """
        order = getattr(self, "_primaryKeyTraversalOrder", None)
        if order is None:
            done = set()
            order = []

            def addToOrder(element) -> bool:
                if element.name in done:
                    return
                predecessors = set(element.required.names)
                predecessors.discard(element.name)
                if not done.issuperset(predecessors):
                    return
                order.append(element)
                done.add(element)
                for other in element.implied:
                    addToOrder(other)

            while not done.issuperset(self.required):
                for dimension in self.required:
                    addToOrder(dimension)

            order.extend(element for element in self.elements if element.name not in done)
            order = tuple(order)
            self._primaryKeyTraversalOrder = order
        return order

    # Class attributes below are shadowed by instance attributes, and are
    # present just to hold the docstrings for those instance attributes.

    universe: DimensionUniverse
    """The set of all known dimensions, of which this graph is a subset
    (`DimensionUniverse`).
    """

    dimensions: NamedValueSet[Dimension]
    """A true `~collections.abc.Set` of all true `Dimension` instances in the
    graph (`NamedValueSet` of `Dimension`).

    This is the set used for iteration, ``len()``, and most set-like operations
    on `DimensionGraph` itself.
    """

    elements: NamedValueSet[DimensionElement]
    """A true `~collections.abc.Set` of all `DimensionElement` instances in the
    graph; a superset of `dimensions` (`NamedValueSet` of `DimensionElement`).

    This is the set used for dict-like lookups, including the ``in`` operator,
    on `DimensionGraph` itself.
    """

    required: NamedValueSet[Dimension]
    """The subset of `dimensions` whose elments must be directly identified via
    their primary keys in a data ID in order to identify the rest of the
    elements in the graph (`NamedValueSet` of `Dimension`).
    """

    implied: NamedValueSet[Dimension]
    """The subset of `dimensions` whose elements need not be directly
    identified via their primary keys in a data ID (`NamedValueSet` of
    `Dimension`).
    """

    spatial: NamedValueSet[DimensionElement]
    """Elements that are associated with independent spatial regions
    (`NamedValueSet` of `DimensionElement`).
    """

    temporal: NamedValueSet[DimensionElement]
    """Elements that are associated with independent spatial regions
    (`NamedValueSet` of `DimensionElement`).
    """

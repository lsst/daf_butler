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

__all__ = ["DimensionGraph", "SerializedDimensionGraph"]

import warnings
from collections.abc import Iterable, Iterator, Mapping, Set
from typing import TYPE_CHECKING, Any, ClassVar, TypeVar, cast

from deprecated.sphinx import deprecated
from lsst.daf.butler._compat import _BaseModelCompat
from lsst.utils.classes import cached_getter, immutable
from lsst.utils.introspection import find_outside_stacklevel

from .._named import NamedValueAbstractSet, NameMappingSetView
from .._topology import TopologicalFamily, TopologicalSpace
from ..json import from_json_pydantic, to_json_pydantic
from ._group import DimensionGroup

if TYPE_CHECKING:  # Imports needed only for type annotations; may be circular.
    from ..registry import Registry
    from ._elements import Dimension, DimensionElement
    from ._governor import GovernorDimension
    from ._skypix import SkyPixDimension
    from ._universe import DimensionUniverse


class SerializedDimensionGraph(_BaseModelCompat):
    """Simplified model of a `DimensionGraph` suitable for serialization."""

    names: list[str]

    @classmethod
    def direct(cls, *, names: list[str]) -> SerializedDimensionGraph:
        """Construct a `SerializedDimensionGraph` directly without validators.

        This differs from the pydantic "construct" method in that the arguments
        are explicitly what the model requires, and it will recurse through
        members, constructing them from their corresponding `direct` methods.

        This method should only be called when the inputs are trusted.
        """
        return cls.model_construct(names=names)


_T = TypeVar("_T", bound="DimensionElement", covariant=True)


# TODO: Remove on DM-41326.
_NVAS_DEPRECATION_MSG = """DimensionGraph is deprecated in favor of
DimensionGroup, which uses sets of str names instead of NamedValueAbstractSets
of Dimension or DimensionElement instances.  Support for the
NamedValueAbstractSet interfaces on this object will be dropped after v27.
"""


class _DimensionGraphNamedValueSet(NameMappingSetView[_T]):
    def __init__(self, keys: Set[str], universe: DimensionUniverse):
        super().__init__({k: cast(_T, universe[k]) for k in keys})

    # TODO: Remove on DM-41326.
    @deprecated(
        _NVAS_DEPRECATION_MSG
        + "Use a dict comprehension and DimensionUniverse indexing to construct a mapping when needed.",
        version="v27",
        category=FutureWarning,
    )
    def asMapping(self) -> Mapping[str, _T]:
        return super().asMapping()

    # TODO: Remove on DM-41326.
    @deprecated(
        _NVAS_DEPRECATION_MSG + "Use DimensionUniverse for DimensionElement lookups.",
        version="v27",
        category=FutureWarning,
    )
    def __getitem__(self, key: str | _T) -> _T:
        return super().__getitem__(key)

    def __contains__(self, key: Any) -> bool:
        from ._elements import DimensionElement

        if isinstance(key, DimensionElement):
            warnings.warn(
                _NVAS_DEPRECATION_MSG + "'in' expressions must use str keys.",
                category=FutureWarning,
                stacklevel=find_outside_stacklevel("lsst.daf.butler."),
            )
        return super().__contains__(key)

    def __iter__(self) -> Iterator[_T]:
        # TODO: Remove on DM-41326.
        warnings.warn(
            _NVAS_DEPRECATION_MSG
            + (
                "In the future, iteration will yield str names; for now, use .names "
                "to do the same without triggering this warning."
            ),
            category=FutureWarning,
            stacklevel=find_outside_stacklevel("lsst.daf.butler."),
        )
        return super().__iter__()

    def __eq__(self, other: Any) -> bool:
        # TODO: Remove on DM-41326.
        warnings.warn(
            _NVAS_DEPRECATION_MSG
            + (
                "In the future, set-equality will assume str keys; for now, use .names "
                "to do the same without triggering this warning."
            ),
            category=FutureWarning,
            stacklevel=find_outside_stacklevel("lsst.daf.butler."),
        )
        return super().__eq__(other)

    def __le__(self, other: Set[Any]) -> bool:
        # TODO: Remove on DM-41326.
        warnings.warn(
            _NVAS_DEPRECATION_MSG
            + (
                "In the future, subset tests will assume str keys; for now, use .names "
                "to do the same without triggering this warning."
            ),
            category=FutureWarning,
            stacklevel=find_outside_stacklevel("lsst.daf.butler."),
        )
        return super().__le__(other)

    def __ge__(self, other: Set[Any]) -> bool:
        # TODO: Remove on DM-41326.
        warnings.warn(
            _NVAS_DEPRECATION_MSG
            + (
                "In the future, superset tests will assume str keys; for now, use .names "
                "to do the same without triggering this warning."
            ),
            category=FutureWarning,
            stacklevel=find_outside_stacklevel("lsst.daf.butler."),
        )
        return super().__ge__(other)


# TODO: Remove on DM-41326.
@deprecated(
    "DimensionGraph is deprecated in favor of DimensionGroup and will be removed after v27.",
    category=FutureWarning,
    version="v27",
)
@immutable
class DimensionGraph:
    """An immutable, dependency-complete collection of dimensions.

    `DimensionGraph` is deprecated in favor of `DimensionGroup` and will be
    removed after v27.  The two types have very similar interfaces, but
    `DimensionGroup` does not support direct iteration and its set-like
    attributes are of dimension element names, not `DimensionElement`
    instances.  `DimensionGraph` objects are still returned by certain
    non-deprecated methods and properties (most prominently
    `DatasetType.dimensions`), and to handle these cases deprecation warnings
    are only emitted for operations on `DimensionGraph` that are not
    supported by `DimensionGroup` as well.

    Parameters
    ----------
    universe : `DimensionUniverse`
        The special graph of all known dimensions of which this graph will be a
        subset.
    dimensions : iterable of `Dimension`, optional
        An iterable of `Dimension` instances that must be included in the
        graph.  All (recursive) dependencies of these dimensions will also be
        included.  At most one of ``dimensions`` and ``names`` must be
        provided.
    names : iterable of `str`, optional
        An iterable of the names of dimensions that must be included in the
        graph.  All (recursive) dependencies of these dimensions will also be
        included.  At most one of ``dimensions`` and ``names`` must be
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
        dimensions: Iterable[Dimension] | None = None,
        names: Iterable[str] | None = None,
        conform: bool = True,
    ) -> DimensionGraph:
        if names is None:
            if dimensions is None:
                group = DimensionGroup(universe)
            else:
                group = DimensionGroup(universe, {d.name for d in dimensions}, _conform=conform)
        else:
            if dimensions is not None:
                raise TypeError("Only one of 'dimensions' and 'names' may be provided.")
            group = DimensionGroup(universe, names, _conform=conform)
        return group._as_graph()

    @property
    def universe(self) -> DimensionUniverse:
        """Object that manages all known dimensions."""
        return self._group.universe

    @property
    @deprecated(
        _NVAS_DEPRECATION_MSG + "Use '.names' instead of '.dimensions' or '.dimensions.names'.",
        version="v27",
        category=FutureWarning,
    )
    @cached_getter
    def dimensions(self) -> NamedValueAbstractSet[Dimension]:
        """A true `~collections.abc.Set` of all true `Dimension` instances in
        the graph.
        """
        return _DimensionGraphNamedValueSet(self._group.names, self._group.universe)

    @property
    @cached_getter
    def elements(self) -> NamedValueAbstractSet[DimensionElement]:
        """A true `~collections.abc.Set` of all `DimensionElement` instances in
        the graph; a superset of `dimensions` (`NamedValueAbstractSet` of
        `DimensionElement`).
        """
        return _DimensionGraphNamedValueSet(self._group.elements, self._group.universe)

    @property
    @cached_getter
    def governors(self) -> NamedValueAbstractSet[GovernorDimension]:
        """A true `~collections.abc.Set` of all `GovernorDimension` instances
        in the graph.
        """
        return _DimensionGraphNamedValueSet(self._group.governors, self._group.universe)

    @property
    @cached_getter
    def skypix(self) -> NamedValueAbstractSet[SkyPixDimension]:
        """A true `~collections.abc.Set` of all `SkyPixDimension` instances
        in the graph.
        """
        return _DimensionGraphNamedValueSet(self._group.skypix, self._group.universe)

    @property
    @cached_getter
    def required(self) -> NamedValueAbstractSet[Dimension]:
        """The subset of `dimensions` whose elements must be directly
        identified via their primary keys in a data ID in order to identify the
        rest of the elements in the graph.
        """
        return _DimensionGraphNamedValueSet(self._group.required, self._group.universe)

    @property
    @cached_getter
    def implied(self) -> NamedValueAbstractSet[Dimension]:
        """The subset of `dimensions` whose elements need not be directly
        identified via their primary keys in a data ID.
        """
        return _DimensionGraphNamedValueSet(self._group.implied, self._group.universe)

    def __getnewargs__(self) -> tuple:
        return (self.universe, None, tuple(self._group.names), False)

    def __deepcopy__(self, memo: dict) -> DimensionGraph:
        # DimensionGraph is recursively immutable; see note in @immutable
        # decorator.
        return self

    @property
    def names(self) -> Set[str]:
        """Set of the names of all dimensions in the graph."""
        return self._group.names

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
        universe: DimensionUniverse | None = None,
        registry: Registry | None = None,
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
    from_json: ClassVar = classmethod(from_json_pydantic)

    def __iter__(self) -> Iterator[Dimension]:
        """Iterate over all dimensions in the graph.

        (and true `Dimension` instances only).
        """
        return iter(self.dimensions)

    def __len__(self) -> int:
        """Return the number of dimensions in the graph.

        (and true `Dimension` instances only).
        """
        return len(self._group)

    def __contains__(self, element: str | DimensionElement) -> bool:
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
        return str(self.as_group())

    def __repr__(self) -> str:
        return f"DimensionGraph({str(self)})"

    def as_group(self) -> DimensionGroup:
        """Return a `DimensionGroup` that represents the same set of
        dimensions.
        """
        return self._group

    def isdisjoint(self, other: DimensionGroup | DimensionGraph) -> bool:
        """Test whether the intersection of two graphs is empty.

        Returns `True` if either operand is the empty.
        """
        return self._group.isdisjoint(other.as_group())

    def issubset(self, other: DimensionGroup | DimensionGraph) -> bool:
        """Test whether all dimensions in ``self`` are also in ``other``.

        Returns `True` if ``self`` is empty.
        """
        return self._group <= other.as_group()

    def issuperset(self, other: DimensionGroup | DimensionGraph) -> bool:
        """Test whether all dimensions in ``other`` are also in ``self``.

        Returns `True` if ``other`` is empty.
        """
        return self._group >= other.as_group()

    def __eq__(self, other: Any) -> bool:
        """Test the arguments have exactly the same dimensions & elements."""
        if isinstance(other, (DimensionGraph, DimensionGroup)):
            return self._group == other.as_group()
        return False

    def __hash__(self) -> int:
        return hash(self.as_group())

    def __le__(self, other: DimensionGroup | DimensionGraph) -> bool:
        """Test whether ``self`` is a subset of ``other``."""
        return self._group <= other.as_group()

    def __ge__(self, other: DimensionGroup | DimensionGraph) -> bool:
        """Test whether ``self`` is a superset of ``other``."""
        return self._group >= other.as_group()

    def __lt__(self, other: DimensionGroup | DimensionGraph) -> bool:
        """Test whether ``self`` is a strict subset of ``other``."""
        return self._group < other.as_group()

    def __gt__(self, other: DimensionGroup | DimensionGraph) -> bool:
        """Test whether ``self`` is a strict superset of ``other``."""
        return self._group > other.as_group()

    def union(self, *others: DimensionGroup | DimensionGraph) -> DimensionGraph:
        """Construct a new graph with all dimensions in any of the operands.

        The elements of the returned graph may exceed the naive union of
        their elements, as some `DimensionElement` instances are included
        in graphs whenever multiple dimensions are present, and those
        dependency dimensions could have been provided by different operands.
        """
        names = set(self.names).union(*[other.names for other in others])
        return self.universe.conform(names)._as_graph()

    def intersection(self, *others: DimensionGroup | DimensionGraph) -> DimensionGraph:
        """Construct a new graph with only dimensions in all of the operands.

        See also `union`.
        """
        names = set(self.names).intersection(*[other.names for other in others])
        return self.universe.conform(names)._as_graph()

    def __or__(self, other: DimensionGroup | DimensionGraph) -> DimensionGraph:
        """Construct a new graph with all dimensions in any of the operands.

        See `union`.
        """
        return self.union(other)

    def __and__(self, other: DimensionGroup | DimensionGraph) -> DimensionGraph:
        """Construct a new graph with only dimensions in all of the operands.

        See `intersection`.
        """
        return self.intersection(other)

    # TODO: Remove on DM-41326.
    @property
    @deprecated(
        "DimensionGraph is deprecated in favor of DimensionGroup, which does not have this attribute; "
        "use .lookup_order.  DimensionGraph will be removed after v27.",
        category=FutureWarning,
        version="v27",
    )
    def primaryKeyTraversalOrder(self) -> tuple[DimensionElement, ...]:
        """A tuple of all elements in specific order.

        The order allows records to be found given their primary keys, starting
        from only the primary keys of required dimensions (`tuple` [
        `DimensionRecord` ]).

        Unlike the table definition/topological order (which is what
        DimensionUniverse.sorted gives you), when dimension A implies dimension
        B, dimension A appears first.
        """
        return tuple(self.universe[element_name] for element_name in self._group.lookup_order)

    @property
    def spatial(self) -> NamedValueAbstractSet[TopologicalFamily]:
        """Families represented by the spatial elements in this graph."""
        return self._group.spatial

    @property
    def temporal(self) -> NamedValueAbstractSet[TopologicalFamily]:
        """Families represented by the temporal elements in this graph."""
        return self._group.temporal

    # TODO: Remove on DM-41326.
    @property
    @deprecated(
        "DimensionGraph is deprecated in favor of DimensionGroup, which does not have this attribute; "
        "use .spatial or .temporal.  DimensionGraph will be removed after v27.",
        category=FutureWarning,
        version="v27",
    )
    def topology(self) -> Mapping[TopologicalSpace, NamedValueAbstractSet[TopologicalFamily]]:
        """Families of elements in this graph that can participate in
        topological relationships.
        """
        return self._group._space_families

    _group: DimensionGroup

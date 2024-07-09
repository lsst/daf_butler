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

__all__ = ["DimensionUniverse"]

import logging
import pickle
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar, TypeVar, overload

from lsst.utils.classes import cached_getter, immutable

from .._config import Config
from .._named import NamedValueAbstractSet, NamedValueSet
from .._topology import TopologicalFamily, TopologicalSpace
from .._utilities.thread_safe_cache import ThreadSafeCache
from ._config import _DEFAULT_NAMESPACE, DimensionConfig
from ._database import DatabaseDimensionElement
from ._elements import Dimension, DimensionElement
from ._governor import GovernorDimension
from ._group import DimensionGroup
from ._skypix import SkyPixDimension, SkyPixSystem

if TYPE_CHECKING:  # Imports needed only for type annotations; may be circular.
    from .construction import DimensionConstructionBuilder


E = TypeVar("E", bound=DimensionElement)
_LOG = logging.getLogger(__name__)


@immutable
class DimensionUniverse:  # numpydoc ignore=PR02
    """Self-consistent set of dimensions.

    A parent class that represents a complete, self-consistent set of
    dimensions and their relationships.

    `DimensionUniverse` is not a class-level singleton, but all instances are
    tracked in a singleton map keyed by the version number and namespace
    in the configuration they were loaded from.  Because these universes
    are solely responsible for constructing `DimensionElement` instances,
    these are also indirectly tracked by that singleton as well.

    Parameters
    ----------
    config : `Config`, optional
        Configuration object from which dimension definitions can be extracted.
        Ignored if ``builder`` is provided, or if ``version`` is provided and
        an instance with that version already exists.
    version : `int`, optional
        Integer version for this `DimensionUniverse`.  If not provided, a
        version will be obtained from ``builder`` or ``config``.
    namespace : `str`, optional
        Namespace of this `DimensionUniverse`, combined with the version
        to provide universe safety for registries that use different
        dimension definitions.
    builder : `DimensionConstructionBuilder`, optional
        Builder object used to initialize a new instance.  Ignored if
        ``version`` is provided and an instance with that version already
        exists.  Should not have had `~DimensionConstructionBuilder.finish`
        called; this will be called if needed by `DimensionUniverse`.
    use_cache : `bool`, optional
        If `True` or not provided, cache `DimensionUniverse` instances globally
        to avoid creating more than one `DimensionUniverse` instance for a
        given configuration.
    """

    _instances: ClassVar[ThreadSafeCache[tuple[int, str], DimensionUniverse]] = ThreadSafeCache()
    """Singleton dictionary of all instances, keyed by version.

    For internal use only.
    """

    def __new__(
        cls,
        config: Config | None = None,
        *,
        version: int | None = None,
        namespace: str | None = None,
        builder: DimensionConstructionBuilder | None = None,
        use_cache: bool = True,
    ) -> DimensionUniverse:
        # Try to get a version first, to look for existing instances; try to
        # do as little work as possible at this stage.
        if version is None:
            if builder is None:
                config = DimensionConfig(config)
                version = config["version"]
            else:
                version = builder.version

        # Then a namespace.
        if namespace is None:
            if builder is None:
                config = DimensionConfig(config)
                namespace = config.get("namespace", _DEFAULT_NAMESPACE)
            else:
                namespace = builder.namespace
        # if still None use the default
        if namespace is None:
            namespace = _DEFAULT_NAMESPACE

        # See if an equivalent instance already exists.
        if use_cache:
            existing_instance = cls._instances.get((version, namespace))
            if existing_instance is not None:
                return existing_instance

        # Ensure we have a builder, building one from config if necessary.
        if builder is None:
            config = DimensionConfig(config)
            builder = config.makeBuilder()

        # Delegate to the builder for most of the construction work.
        builder.finish()

        # Create the universe instance and create core attributes, mostly
        # copying from builder.
        self: DimensionUniverse = object.__new__(cls)
        assert self is not None
        self._cached_groups = ThreadSafeCache()
        self._dimensions = builder.dimensions
        self._elements = builder.elements
        self._topology = builder.topology
        self.dimensionConfig = builder.config
        commonSkyPix = self._dimensions[builder.commonSkyPixName]
        assert isinstance(commonSkyPix, SkyPixDimension)
        self.commonSkyPix = commonSkyPix

        # Attach self to all elements.
        for element in self._elements:
            element.universe = self

        # Add attribute for special subsets of the graph.
        self._empty = DimensionGroup(self, (), _conform=False)

        # Use the version number and namespace from the config as a key in
        # the singleton dict containing all instances; that will let us
        # transfer dimension objects between processes using pickle without
        # actually going through real initialization, as long as a universe
        # with the same version and namespace has already been constructed in
        # the receiving process.
        self._version = version
        self._namespace = namespace

        # Build mappings from element to index.  These are used for
        # topological-sort comparison operators in DimensionElement itself.
        self._elementIndices = {name: i for i, name in enumerate(self._elements.names)}
        # Same for dimension to index, sorted topologically across required
        # and implied.  This is used for encode/decode.
        self._dimensionIndices = {name: i for i, name in enumerate(self._dimensions.names)}

        self._populates = defaultdict(NamedValueSet)
        for element in self._elements:
            if element.populated_by is not None:
                self._populates[element.populated_by.name].add(element)

        if use_cache:
            return cls._instances.set_or_get((self._version, self._namespace), self)
        else:
            return self

    @property
    def version(self) -> int:
        """The version number of this universe.

        Returns
        -------
        version : `int`
            An integer representing the version number of this universe.
            Uniquely defined when combined with the `namespace`.
        """
        return self._version

    @property
    def namespace(self) -> str:
        """The namespace associated with this universe.

        Returns
        -------
        namespace : `str`
            The namespace. When combined with the `version` can uniquely
            define this universe.
        """
        return self._namespace

    def isCompatibleWith(self, other: DimensionUniverse) -> bool:
        """Check compatibility between this `DimensionUniverse` and another.

        Parameters
        ----------
        other : `DimensionUniverse`
            The other `DimensionUniverse` to check for compatibility.

        Returns
        -------
        results : `bool`
            If the other `DimensionUniverse` is compatible with this one return
            `True`, else `False`.
        """
        # Different namespaces mean that these universes cannot be compatible.
        if self.namespace != other.namespace:
            return False
        if self.version != other.version:
            _LOG.info(
                "Universes share a namespace %r but have differing versions (%d != %d). "
                " This could be okay but may be responsible for dimension errors later.",
                self.namespace,
                self.version,
                other.version,
            )

        # For now assume compatibility if versions differ.
        return True

    def __repr__(self) -> str:
        return f"DimensionUniverse({self._version}, {self._namespace})"

    def __getitem__(self, name: str) -> DimensionElement:
        return self._elements[name]

    def __contains__(self, name: Any) -> bool:
        return name in self._elements

    def get(self, name: str, default: DimensionElement | None = None) -> DimensionElement | None:
        """Return the `DimensionElement` with the given name or a default.

        Parameters
        ----------
        name : `str`
            Name of the element.
        default : `DimensionElement`, optional
            Element to return if the named one does not exist.  Defaults to
            `None`.

        Returns
        -------
        element : `DimensionElement`
            The named element.
        """
        return self._elements.get(name, default)

    def getStaticElements(self) -> NamedValueAbstractSet[DimensionElement]:
        """Return a set of all static elements in this universe.

        Non-static elements that are created as needed may also exist, but
        these are guaranteed to have no direct relationships to other elements
        (though they may have spatial or temporal relationships).

        Returns
        -------
        elements : `NamedValueAbstractSet` [ `DimensionElement` ]
            A frozen set of `DimensionElement` instances.
        """
        return self._elements

    def getStaticDimensions(self) -> NamedValueAbstractSet[Dimension]:
        """Return a set of all static dimensions in this universe.

        Non-static dimensions that are created as needed may also exist, but
        these are guaranteed to have no direct relationships to other elements
        (though they may have spatial or temporal relationships).

        Returns
        -------
        dimensions : `NamedValueAbstractSet` [ `Dimension` ]
            A frozen set of `Dimension` instances.
        """
        return self._dimensions

    def getGovernorDimensions(self) -> NamedValueAbstractSet[GovernorDimension]:
        """Return a set of all `GovernorDimension` instances in this universe.

        Returns
        -------
        governors : `NamedValueAbstractSet` [ `GovernorDimension` ]
            A frozen set of `GovernorDimension` instances.
        """
        return self.governor_dimensions

    def getDatabaseElements(self) -> NamedValueAbstractSet[DatabaseDimensionElement]:
        """Return set of all `DatabaseDimensionElement` instances in universe.

        This does not include `GovernorDimension` instances, which are backed
        by the database but do not inherit from `DatabaseDimensionElement`.

        Returns
        -------
        elements : `NamedValueAbstractSet` [ `DatabaseDimensionElement` ]
            A frozen set of `DatabaseDimensionElement` instances.
        """
        return self.database_elements

    @property
    def elements(self) -> NamedValueAbstractSet[DimensionElement]:
        """All dimension elements defined in this universe."""
        return self._elements

    @property
    def dimensions(self) -> NamedValueAbstractSet[Dimension]:
        """All dimensions defined in this universe."""
        return self._dimensions

    @property
    @cached_getter
    def governor_dimensions(self) -> NamedValueAbstractSet[GovernorDimension]:
        """All governor dimensions defined in this universe.

        Governor dimensions serve as special required dependencies of other
        dimensions, with special handling in dimension query expressions and
        collection summaries.  Governor dimension records are stored in the
        database but the set of such values is expected to be small enough
        for all values to be cached by all clients.
        """
        return NamedValueSet(d for d in self._dimensions if isinstance(d, GovernorDimension)).freeze()

    @property
    @cached_getter
    def skypix_dimensions(self) -> NamedValueAbstractSet[SkyPixDimension]:
        """All skypix dimensions defined in this universe.

        Skypix dimension records are always generated on-the-fly rather than
        stored in the database, and they always represent a tiling of the sky
        with no overlaps.
        """
        result = NamedValueSet[SkyPixDimension]()
        for system in self.skypix:
            result.update(system)
        return result.freeze()

    @property
    @cached_getter
    def database_elements(self) -> NamedValueAbstractSet[DatabaseDimensionElement]:
        """All dimension elements whose records are stored in the database,
        except governor dimensions.
        """
        return NamedValueSet(d for d in self._elements if isinstance(d, DatabaseDimensionElement)).freeze()

    @property
    @cached_getter
    def skypix(self) -> NamedValueAbstractSet[SkyPixSystem]:
        """All skypix systems known to this universe.

        (`NamedValueAbstractSet` [ `SkyPixSystem` ]).
        """
        return NamedValueSet(
            [
                family
                for family in self._topology[TopologicalSpace.SPATIAL]
                if isinstance(family, SkyPixSystem)
            ]
        ).freeze()

    def getElementIndex(self, name: str) -> int:
        """Return the position of the named dimension element.

        The position is in this universe's sorting of all elements.

        Parameters
        ----------
        name : `str`
            Name of the element.

        Returns
        -------
        index : `int`
            Sorting index for this element.
        """
        return self._elementIndices[name]

    def getDimensionIndex(self, name: str) -> int:
        """Return the position of the named dimension.

        This position is in this universe's sorting of all dimensions.

        Parameters
        ----------
        name : `str`
            Name of the dimension.

        Returns
        -------
        index : `int`
            Sorting index for this dimension.

        Notes
        -----
        The dimension sort order for a universe is consistent with the element
        order (all dimensions are elements), and either can be used to sort
        dimensions if used consistently.  But there are also some contexts in
        which contiguous dimension-only indices are necessary or at least
        desirable.
        """
        return self._dimensionIndices[name]

    def conform(
        self,
        dimensions: Iterable[str] | str | DimensionGroup,
        /,
    ) -> DimensionGroup:
        """Construct a dimension group from an iterable of dimension names.

        Parameters
        ----------
        dimensions : `~collections.abc.Iterable` [ `str` ], `str`, or \
                `DimensionGroup`
            Dimensions that must be included in the returned group; their
            dependencies will be as well.

        Returns
        -------
        group : `DimensionGroup`
            A `DimensionGroup` instance containing all given dimensions.
        """
        match dimensions:
            case DimensionGroup():
                return dimensions
            case str() as name:
                return self[name].minimal_group
            case iterable:
                return DimensionGroup(self, set(iterable))

    @overload
    def sorted(self, elements: Iterable[Dimension], *, reverse: bool = False) -> Sequence[Dimension]: ...

    @overload
    def sorted(
        self, elements: Iterable[DimensionElement | str], *, reverse: bool = False
    ) -> Sequence[DimensionElement]: ...

    def sorted(self, elements: Iterable[Any], *, reverse: bool = False) -> list[Any]:
        """Return a sorted version of the given iterable of dimension elements.

        The universe's sort order is topological (an element's dependencies
        precede it), with an unspecified (but deterministic) approach to
        breaking ties.

        Parameters
        ----------
        elements : iterable of `DimensionElement`
            Elements to be sorted.
        reverse : `bool`, optional
            If `True`, sort in the opposite order.

        Returns
        -------
        sorted : `~collections.abc.Sequence` [ `Dimension` or \
                `DimensionElement` ]
            A sorted sequence containing the same elements that were given.
        """
        s = set(elements)
        result = [element for element in self._elements if element in s or element.name in s]
        if reverse:
            result.reverse()
        return result

    def get_elements_populated_by(self, dimension: Dimension) -> NamedValueAbstractSet[DimensionElement]:
        """Return the set of `DimensionElement` objects whose
        `~DimensionElement.populated_by` attribute is the given dimension.

        Parameters
        ----------
        dimension : `Dimension`
            The dimension of interest.

        Returns
        -------
        populated_by : `NamedValueAbstractSet` [ `DimensionElement` ]
            The set of elements who say they are populated by the given
            dimension.
        """
        return self._populates[dimension.name]

    @property
    def empty(self) -> DimensionGroup:
        """The `DimensionGroup` that contains no dimensions."""
        return self._empty

    @classmethod
    def _unpickle(cls, version: int, namespace: str | None = None) -> DimensionUniverse:
        """Return an unpickled dimension universe.

        Callable used for unpickling.

        For internal use only.
        """
        if namespace is None:
            # Old pickled universe.
            namespace = _DEFAULT_NAMESPACE
        instance = cls._instances.get((version, namespace))
        if instance is None:
            raise pickle.UnpicklingError(
                f"DimensionUniverse with version '{version}' and namespace {namespace!r} "
                "not found.  Note that DimensionUniverse objects are not "
                "truly serialized; when using pickle to transfer them "
                "between processes, an equivalent instance with the same "
                "version must already exist in the receiving process."
            )
        return instance

    def __reduce__(self) -> tuple:
        return (self._unpickle, (self._version, self._namespace))

    def __deepcopy__(self, memo: dict) -> DimensionUniverse:
        # DimensionUniverse is recursively immutable; see note in @immutable
        # decorator.
        return self

    # Class attributes below are shadowed by instance attributes, and are
    # present just to hold the docstrings for those instance attributes.

    commonSkyPix: SkyPixDimension
    """The special skypix dimension that is used to relate all other spatial
    dimensions in the `Registry` database (`SkyPixDimension`).
    """

    dimensionConfig: DimensionConfig
    """The configuration used to create this Universe (`DimensionConfig`)."""

    _cached_groups: ThreadSafeCache[frozenset[str], DimensionGroup]

    _dimensions: NamedValueAbstractSet[Dimension]

    _elements: NamedValueAbstractSet[DimensionElement]

    _empty: DimensionGroup

    _topology: Mapping[TopologicalSpace, NamedValueAbstractSet[TopologicalFamily]]

    _dimensionIndices: dict[str, int]

    _elementIndices: dict[str, int]

    _populates: defaultdict[str, NamedValueSet[DimensionElement]]

    _version: int

    _namespace: str

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

__all__ = ["DimensionUniverse"]

import logging
import math
import pickle
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    FrozenSet,
    Iterable,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)

from lsst.utils.classes import cached_getter, immutable

from .._topology import TopologicalFamily, TopologicalSpace
from ..config import Config
from ..named import NamedValueAbstractSet, NamedValueSet
from ._config import _DEFAULT_NAMESPACE, DimensionConfig
from ._database import DatabaseDimensionElement
from ._elements import Dimension, DimensionElement
from ._governor import GovernorDimension
from ._graph import DimensionGraph
from ._skypix import SkyPixDimension, SkyPixSystem

if TYPE_CHECKING:  # Imports needed only for type annotations; may be circular.
    from ._coordinate import DataCoordinate
    from ._packer import DimensionPacker, DimensionPackerFactory
    from .construction import DimensionConstructionBuilder


E = TypeVar("E", bound=DimensionElement)
_LOG = logging.getLogger(__name__)


@immutable
class DimensionUniverse:
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
    """

    _instances: ClassVar[Dict[Tuple[int, str], DimensionUniverse]] = {}
    """Singleton dictionary of all instances, keyed by version.

    For internal use only.
    """

    def __new__(
        cls,
        config: Optional[Config] = None,
        *,
        version: Optional[int] = None,
        namespace: Optional[str] = None,
        builder: Optional[DimensionConstructionBuilder] = None,
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
        self: Optional[DimensionUniverse] = cls._instances.get((version, namespace))
        if self is not None:
            return self

        # Ensure we have a builder, building one from config if necessary.
        if builder is None:
            config = DimensionConfig(config)
            builder = config.makeBuilder()

        # Delegate to the builder for most of the construction work.
        builder.finish()

        # Create the universe instance and create core attributes, mostly
        # copying from builder.
        self = object.__new__(cls)
        assert self is not None
        self._cache = {}
        self._dimensions = builder.dimensions
        self._elements = builder.elements
        self._topology = builder.topology
        self._packers = builder.packers
        self.dimensionConfig = builder.config
        commonSkyPix = self._dimensions[builder.commonSkyPixName]
        assert isinstance(commonSkyPix, SkyPixDimension)
        self.commonSkyPix = commonSkyPix

        # Attach self to all elements.
        for element in self._elements:
            element.universe = self

        # Add attribute for special subsets of the graph.
        self.empty = DimensionGraph(self, (), conform=False)

        # Use the version number and namespace from the config as a key in
        # the singleton dict containing all instances; that will let us
        # transfer dimension objects between processes using pickle without
        # actually going through real initialization, as long as a universe
        # with the same version and namespace has already been constructed in
        # the receiving process.
        self._version = version
        self._namespace = namespace
        cls._instances[self._version, self._namespace] = self

        # Build mappings from element to index.  These are used for
        # topological-sort comparison operators in DimensionElement itself.
        self._elementIndices = {name: i for i, name in enumerate(self._elements.names)}
        # Same for dimension to index, sorted topologically across required
        # and implied.  This is used for encode/decode.
        self._dimensionIndices = {name: i for i, name in enumerate(self._dimensions.names)}

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
        """Check compatibility between this `DimensionUniverse` and another

        Parameters
        ----------
        other : `DimensionUniverse`
            The other `DimensionUniverse` to check for compatibility

        Returns
        -------
        results : `bool`
            If the other `DimensionUniverse` is compatible with this one return
            `True`, else `False`
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

    def get(self, name: str, default: Optional[DimensionElement] = None) -> Optional[DimensionElement]:
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

    @cached_getter
    def getGovernorDimensions(self) -> NamedValueAbstractSet[GovernorDimension]:
        """Return a set of all `GovernorDimension` instances in this universe.

        Returns
        -------
        governors : `NamedValueAbstractSet` [ `GovernorDimension` ]
            A frozen set of `GovernorDimension` instances.
        """
        return NamedValueSet(d for d in self._dimensions if isinstance(d, GovernorDimension)).freeze()

    @cached_getter
    def getDatabaseElements(self) -> NamedValueAbstractSet[DatabaseDimensionElement]:
        """Return set of all `DatabaseDimensionElement` instances in universe.

        This does not include `GovernorDimension` instances, which are backed
        by the database but do not inherit from `DatabaseDimensionElement`.

        Returns
        -------
        elements : `NamedValueAbstractSet` [ `DatabaseDimensionElement` ]
            A frozen set of `DatabaseDimensionElement` instances.
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

    def expandDimensionNameSet(self, names: Set[str]) -> None:
        """Expand a set of dimension names in-place.

        Includes recursive dependencies.

        This is an advanced interface for cases where constructing a
        `DimensionGraph` (which also expands required dependencies) is
        impossible or undesirable.

        Parameters
        ----------
        names : `set` [ `str` ]
            A true `set` of dimension names, to be expanded in-place.
        """
        # Keep iterating until the set of names stops growing.  This is not as
        # efficient as it could be, but we work pretty hard cache
        # DimensionGraph instances to keep actual construction rare, so that
        # shouldn't matter.
        oldSize = len(names)
        while True:
            # iterate over a temporary copy so we can modify the original
            for name in tuple(names):
                names.update(self._dimensions[name].required.names)
                names.update(self._dimensions[name].implied.names)
            if oldSize == len(names):
                break
            else:
                oldSize = len(names)

    def extract(self, iterable: Iterable[Union[Dimension, str]]) -> DimensionGraph:
        """Construct graph from iterable.

        Constructs a `DimensionGraph` from a possibly-heterogenous iterable
        of `Dimension` instances and string names thereof.

        Constructing `DimensionGraph` directly from names or dimension
        instances is slightly more efficient when it is known in advance that
        the iterable is not heterogenous.

        Parameters
        ----------
        iterable: iterable of `Dimension` or `str`
            Dimensions that must be included in the returned graph (their
            dependencies will be as well).

        Returns
        -------
        graph : `DimensionGraph`
            A `DimensionGraph` instance containing all given dimensions.
        """
        names = set()
        for item in iterable:
            try:
                names.add(item.name)  # type: ignore
            except AttributeError:
                names.add(item)
        return DimensionGraph(universe=self, names=names)

    def sorted(self, elements: Iterable[Union[E, str]], *, reverse: bool = False) -> List[E]:
        """Return a sorted version of the given iterable of dimension elements.

        The universe's sort order is topological (an element's dependencies
        precede it), with an unspecified (but deterministic) approach to
        breaking ties.

        Parameters
        ----------
        elements : iterable of `DimensionElement`.
            Elements to be sorted.
        reverse : `bool`, optional
            If `True`, sort in the opposite order.

        Returns
        -------
        sorted : `list` of `DimensionElement`
            A sorted list containing the same elements that were given.
        """
        s = set(elements)
        result = [element for element in self._elements if element in s or element.name in s]
        if reverse:
            result.reverse()
        # mypy thinks this can return DimensionElements even if all the user
        # passed it was Dimensions; we know better.
        return result  # type: ignore

    def makePacker(self, name: str, dataId: DataCoordinate) -> DimensionPacker:
        """Make a dimension packer.

        Constructs a `DimensionPacker` that can pack data ID dictionaries
        into unique integers.

        Parameters
        ----------
        name : `str`
            Name of the packer, matching a key in the "packers" section of the
            dimension configuration.
        dataId : `DataCoordinate`
            Fully-expanded data ID that identifies the at least the "fixed"
            dimensions of the packer (i.e. those that are assumed/given,
            setting the space over which packed integer IDs are unique).
            ``dataId.hasRecords()`` must return `True`.
        """
        return self._packers[name](self, dataId)

    def getEncodeLength(self) -> int:
        """Return encoded size of graph.

        Returns the size (in bytes) of the encoded size of `DimensionGraph`
        instances in this universe.

        See `DimensionGraph.encode` and `DimensionGraph.decode` for more
        information.
        """
        return math.ceil(len(self._dimensions) / 8)

    @classmethod
    def _unpickle(cls, version: int, namespace: Optional[str] = None) -> DimensionUniverse:
        """Return an unpickled dimension universe.

        Callable used for unpickling.

        For internal use only.
        """
        if namespace is None:
            # Old pickled universe.
            namespace = _DEFAULT_NAMESPACE
        try:
            return cls._instances[version, namespace]
        except KeyError as err:
            raise pickle.UnpicklingError(
                f"DimensionUniverse with version '{version}' and namespace {namespace!r} "
                f"not found.  Note that DimensionUniverse objects are not "
                f"truly serialized; when using pickle to transfer them "
                f"between processes, an equivalent instance with the same "
                f"version must already exist in the receiving process."
            ) from err

    def __reduce__(self) -> tuple:
        return (self._unpickle, (self._version, self._namespace))

    def __deepcopy__(self, memo: dict) -> DimensionUniverse:
        # DimensionUniverse is recursively immutable; see note in @immutable
        # decorator.
        return self

    # Class attributes below are shadowed by instance attributes, and are
    # present just to hold the docstrings for those instance attributes.

    empty: DimensionGraph
    """The `DimensionGraph` that contains no dimensions (`DimensionGraph`).
    """

    commonSkyPix: SkyPixDimension
    """The special skypix dimension that is used to relate all other spatial
    dimensions in the `Registry` database (`SkyPixDimension`).
    """

    dimensionConfig: DimensionConfig
    """The configuration used to create this Universe (`DimensionConfig`)."""

    _cache: Dict[FrozenSet[str], DimensionGraph]

    _dimensions: NamedValueAbstractSet[Dimension]

    _elements: NamedValueAbstractSet[DimensionElement]

    _topology: Mapping[TopologicalSpace, NamedValueAbstractSet[TopologicalFamily]]

    _dimensionIndices: Dict[str, int]

    _elementIndices: Dict[str, int]

    _packers: Dict[str, DimensionPackerFactory]

    _version: int

    _namespace: str

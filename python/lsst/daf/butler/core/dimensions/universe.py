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

import math
import pickle
from typing import (
    ClassVar,
    Dict,
    FrozenSet,
    Iterable,
    List,
    Optional,
    TYPE_CHECKING,
    TypeVar,
    Union,
)

from ..config import Config
from ..named import NamedValueSet
from ..utils import immutable
from .elements import Dimension, DimensionElement, SkyPixDimension
from .graph import DimensionGraph
from .config import processElementsConfig, processSkyPixConfig, DimensionConfig
from .packer import DimensionPackerFactory

if TYPE_CHECKING:  # Imports needed only for type annotations; may be circular.
    from .coordinate import DataCoordinate
    from .packer import DimensionPacker


E = TypeVar("E", bound=DimensionElement)


@immutable
class DimensionUniverse:
    """A parent class that represents a complete, self-consistent set of
    dimensions and their relationships.

    `DimensionUniverse` is not a class-level singleton, but all instances are
    tracked in a singleton map keyed by the version number in the configuration
    they were loaded from.  Because these universes are solely responsible for
    constructing `DimensionElement` instances, these are also indirectly
    tracked by that singleton as well.

    Parameters
    ----------
    config : `Config`, optional
        Configuration describing the dimensions and their relationships.  If
        not provided, default configuration (from ``dimensions.yaml`` in
        this package's ``configs`` resource directory) will be loaded.
    """

    _instances: ClassVar[Dict[int, DimensionUniverse]] = {}
    """Singleton dictionary of all instances, keyed by version.

    For internal use only.
    """

    def __new__(cls, config: Optional[Config] = None) -> DimensionUniverse:
        # Normalize the config and apply defaults.
        config = DimensionConfig(config)

        # First see if an equivalent instance already exists.
        version = config["version"]
        self: Optional[DimensionUniverse] = cls._instances.get(version)
        if self is not None:
            return self

        # Create the universe instance and add core attributes.
        self = object.__new__(cls)
        assert self is not None
        self._cache = {}
        self._dimensions = NamedValueSet()
        self._elements = NamedValueSet()

        # Read the skypix dimensions from config.
        skyPixDimensions, self.commonSkyPix = processSkyPixConfig(config["skypix"])
        # Add the skypix dimensions to the universe after sorting
        # lexicographically (no topological sort because skypix dimensions
        # never have any dependencies).
        for name in sorted(skyPixDimensions):
            skyPixDimensions[name]._finish(self, {})

        # Read the other dimension elements from config.
        elementsToDo = processElementsConfig(config["elements"])
        # Add elements to the universe in topological order by identifying at
        # each outer iteration which elements have already had all of their
        # dependencies added.
        while elementsToDo:
            unblocked = [name for name, element in elementsToDo.items()
                         if element._related.dependencies.isdisjoint(elementsToDo.keys())]
            unblocked.sort()  # Break ties lexicographically.
            if not unblocked:
                raise RuntimeError(f"Cycle detected in dimension elements: {elementsToDo.keys()}.")
            for name in unblocked:
                # Finish initialization of the element with steps that
                # depend on those steps already having been run for all
                # dependencies.
                # This includes adding the element to self.elements and
                # (if appropriate) self.dimensions.
                elementsToDo.pop(name)._finish(self, elementsToDo)

        # Add attributes for special subsets of the graph.
        self.empty = DimensionGraph(self, (), conform=False)

        # Set up factories for dataId packers as defined by config.
        # MyPy is totally confused by us setting attributes on self in __new__.
        self._packers = {}
        for name, subconfig in config.get("packers", {}).items():
            self._packers[name] = DimensionPackerFactory.fromConfig(universe=self,  # type: ignore
                                                                    config=subconfig)

        # Use the version number from the config as a key in the singleton
        # dict containing all instances; that will let us transfer dimension
        # objects between processes using pickle without actually going
        # through real initialization, as long as a universe with the same
        # version has already been constructed in the receiving process.
        self._version = version
        cls._instances[self._version] = self

        # Build mappings from element to index.  These are used for
        # topological-sort comparison operators in DimensionElement itself.
        self._elementIndices = {
            name: i for i, name in enumerate(self._elements.names)
        }
        # Same for dimension to index, sorted topologically across required
        # and implied.  This is used for encode/decode.
        self._dimensionIndices = {
            name: i for i, name in enumerate(self._dimensions.names)
        }

        # Freeze internal sets so we can return them in methods without
        # worrying about modifications.
        self._elements.freeze()
        self._dimensions.freeze()
        return self

    def __repr__(self) -> str:
        return f"DimensionUniverse({self._version})"

    def __getitem__(self, name: str) -> DimensionElement:
        return self._elements[name]

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

    def getStaticElements(self) -> NamedValueSet[DimensionElement]:
        """Return a set of all static elements in this universe.

        Non-static elements that are created as needed may also exist, but
        these are guaranteed to have no direct relationships to other elements
        (though they may have spatial or temporal relationships).

        Returns
        -------
        elements : `NamedValueSet` [ `DimensionElement` ]
            A frozen set of `DimensionElement` instances.
        """
        return self._elements

    def getStaticDimensions(self) -> NamedValueSet[Dimension]:
        """Return a set of all static dimensions in this universe.

        Non-static dimensions that are created as needed may also exist, but
        these are guaranteed to have no direct relationships to other elements
        (though they may have spatial or temporal relationships).

        Returns
        -------
        dimensions : `NamedValueSet` [ `Dimension` ]
            A frozen set of `Dimension` instances.
        """
        return self._dimensions

    def getElementIndex(self, name: str) -> int:
        """Return the position of the named dimension element in this
        universe's sorting of all elements.

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
        """Return the position of the named dimension in this universe's
        sorting of all dimensions.

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

    def extract(self, iterable: Iterable[Union[Dimension, str]]) -> DimensionGraph:
        """Construct a `DimensionGraph` from a possibly-heterogenous iterable
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
        """Construct a `DimensionPacker` that can pack data ID dictionaries
        into unique integers.

        Parameters
        ----------
        name : `str`
            Name of the packer, matching a key in the "packers" section of the
            dimension configuration.
        dataId : `DataCoordinate`
            Fully-expanded data ID that identfies the at least the "fixed"
            dimensions of the packer (i.e. those that are assumed/given,
            setting the space over which packed integer IDs are unique).
            ``dataId.hasRecords()`` must return `True`.
        """
        return self._packers[name](dataId)

    def getEncodeLength(self) -> int:
        """Return the size (in bytes) of the encoded size of `DimensionGraph`
        instances in this universe.

        See `DimensionGraph.encode` and `DimensionGraph.decode` for more
        information.
        """
        return math.ceil(len(self._dimensions)/8)

    @classmethod
    def _unpickle(cls, version: int) -> DimensionUniverse:
        """Callable used for unpickling.

        For internal use only.
        """
        try:
            return cls._instances[version]
        except KeyError as err:
            raise pickle.UnpicklingError(
                f"DimensionUniverse with version '{version}' "
                f"not found.  Note that DimensionUniverse objects are not "
                f"truly serialized; when using pickle to transfer them "
                f"between processes, an equivalent instance with the same "
                f"version must already exist in the receiving process."
            ) from err

    def __reduce__(self) -> tuple:
        return (self._unpickle, (self._version,))

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

    _cache: Dict[FrozenSet[str], DimensionGraph]

    _dimensions: NamedValueSet[Dimension]

    _elements: NamedValueSet[DimensionElement]

    _dimensionIndices: Dict[str, int]

    _elementIndices: Dict[str, int]

    _packers: Dict[str, DimensionPackerFactory]

    _version: int

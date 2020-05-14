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
from typing import Optional, Iterable, List, Union, TYPE_CHECKING

from ..config import Config
from ..named import NamedValueSet
from ..utils import immutable
from .elements import Dimension, DimensionElement, SkyPixDimension
from .graph import DimensionGraph
from .config import processElementsConfig, processSkyPixConfig, DimensionConfig
from .packer import DimensionPackerFactory

if TYPE_CHECKING:  # Imports needed only for type annotations; may be circular.
    from .coordinate import ExpandedDataCoordinate
    from .packer import DimensionPacker


@immutable
class DimensionUniverse(DimensionGraph):
    """A special `DimensionGraph` that constructs and manages a complete set of
    compatible dimensions.

    `DimensionUniverse` is not a class-level singleton, but all instances are
    tracked in a singleton map keyed by the version number in the configuration
    they were loaded from.  Because these universes are solely responsible for
    constructing `DimensionElement` instances, these are also indirectly
    tracked by that singleton as well.

    Parameters
    ----------
    config : `Config`, optional
        Configuration describing the dimensions and their relationships.  If
        not provided, default configuration (from
        ``daf_butler/config/dimensions.yaml``) wil be loaded.
    """

    _instances = {}
    """Singleton dictionary of all instances, keyed by version.

    For internal use only.
    """

    def __new__(cls, config: Optional[Config] = None) -> DimensionUniverse:
        # Normalize the config and apply defaults.
        config = DimensionConfig(config)

        # First see if an equivalent instance already exists.
        version = config["version"]
        self = cls._instances.get(version)
        if self is not None:
            return self

        # Create the universe instance and add core attributes.
        # We don't want any of what DimensionGraph.__new__ does, so we just go
        # straight to object.__new__.  The C++ side of my brain is offended by
        # this, but I think it's the right approach in Python, where we don't
        # have the option of having multiple constructors with different roles.
        self = object.__new__(cls)
        self.universe = self
        self._cache = {}
        self.dimensions = NamedValueSet()
        self.elements = NamedValueSet()

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
        self._finish()

        # Set up factories for dataId packers as defined by config.
        self._packers = {}
        for name, subconfig in config.get("packers", {}).items():
            self._packers[name] = DimensionPackerFactory.fromConfig(universe=self, config=subconfig)

        # Use the version number from the config as a key in the singleton
        # dict containing all instances; that will let us transfer dimension
        # objects between processes using pickle without actually going
        # through real initialization, as long as a universe with the same
        # version has already been constructed in the receiving process.
        self._version = version
        cls._instances[self._version] = self
        return self

    def __repr__(self) -> str:
        return f"DimensionUniverse({self})"

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
                names.add(item.name)
            except AttributeError:
                names.add(item)
        return DimensionGraph(universe=self, names=names)

    def sorted(self, elements: Iterable[DimensionElement], *, reverse=False) -> List[DimensionElement]:
        """Return a sorted version of the given iterable of dimension elements.

        The universe's sort order is topological (an element's dependencies
        precede it), starting with skypix dimensions (which never have
        dependencies) and then sorting lexicographically to break ties.

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
        result = [element for element in self.elements if element in s or element.name in s]
        if reverse:
            result.reverse()
        return result

    def makePacker(self, name: str, dataId: ExpandedDataCoordinate) -> DimensionPacker:
        """Construct a `DimensionPacker` that can pack data ID dictionaries
        into unique integers.

        Parameters
        ----------
        name : `str`
            Name of the packer, matching a key in the "packers" section of the
            dimension configuration.
        dataId : `ExpandedDataCoordinate`
            Fully-expanded data ID that identfies the at least the "fixed"
            dimensions of the packer (i.e. those that are assumed/given,
            setting the space over which packed integer IDs are unique).
        """
        return self._packers[name](dataId)

    def getEncodeLength(self) -> int:
        """Return the size (in bytes) of the encoded size of `DimensionGraph`
        instances in this universe.

        See `DimensionGraph.encode` and `DimensionGraph.decode` for more
        information.
        """
        return math.ceil(len(self.dimensions)/8)

    @classmethod
    def _unpickle(cls, version: bytes) -> DimensionUniverse:
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

    # Class attributes below are shadowed by instance attributes, and are
    # present just to hold the docstrings for those instance attributes.

    empty: DimensionGraph
    """The `DimensionGraph` that contains no dimensions (`DimensionGraph`).
    """

    commonSkyPix: SkyPixDimension
    """The special skypix dimension that is used to relate all other spatial
    dimensions in the `Registry` database (`SkyPixDimension`).
    """

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

__all__ = ["DimensionElement", "Dimension", "SkyPixDimension"]

from typing import Optional, Iterable, AbstractSet, TYPE_CHECKING

from sqlalchemy import Integer

from lsst.sphgeom import Pixelization
from ..utils import NamedValueSet, immutable
from ..schema import FieldSpec, TableSpec
from .records import _subclassDimensionRecord
from .schema import makeElementTableSpec
from .graph import DimensionGraph

if TYPE_CHECKING:  # Imports needed only for type annotations; may be circular.
    from .universe import DimensionUniverse


@immutable
class DimensionElement:
    """A named data-organization concept that defines a label and/or metadata
    in the dimensions system.

    A `DimensionElement` instance typically corresponds to a table in the
    `Registry`; the rows in that table are represented by instances of a
    `DimensionRecord` subclass.  Most `DimensionElement` instances are
    instances of its `Dimension` subclass, which is used for elements that can
    be used as data ID keys.  The base class itself can be used for other
    dimension tables that provide metadata keyed by true dimensions.

    Parameters
    ----------
    name : `str`
        Name of the element.  Used as at least part of the table name, if
        the dimension in associated with a database table.
    directDependencyNames : iterable of `str`
        The names of all dimensions this elements depends on directly,
        including both required dimensions (those needed to identify a
        record of this element) an implied dimensions.
    impliedDependencyNames : iterable of `str`
        The names of all dimensions that are identified by records of this
        element, but are not needed to identify it.
    spatial : `bool`
        Whether records of this element are associated with a region on the
        sky.
    temporal : `bool`
        Whether records of this element are associated with a timespan.
    metadata : iterable of `FieldSpec`
        Additional metadata fields included in this element's table.
    cached : `bool`
        Whether `Registry` should cache records of this element in-memory.
    viewOf : `str`, optional
        Name of another table this element's records should be drawn from.  The
        fields of this table must be a superset of the fields of the element.

    Notes
    -----
    `DimensionElement` instances should always be constructed by and retreived
    from a `DimensionUniverse`.  They are immutable after they are fully
    constructed, and should never be copied.

    Pickling a `DimensionElement` just records its name and universe;
    unpickling one actually just looks up the element via the singleton
    dictionary of all universes.  This allows pickle to be used to transfer
    elements between processes, but only when each process initializes its own
    instance of the same `DimensionUniverse`.
    """

    def __init__(self, name: str, *,
                 directDependencyNames: Iterable[str] = (),
                 impliedDependencyNames: Iterable[str] = (),
                 spatial: bool = False,
                 temporal: bool = False,
                 metadata: Iterable[FieldSpec] = (),
                 cached: bool = False,
                 viewOf: Optional[str] = None):
        self.name = name
        self._directDependencyNames = frozenset(directDependencyNames)
        self._impliedDependencyNames = frozenset(impliedDependencyNames)
        self.spatial = spatial
        self.temporal = temporal
        self.metadata = NamedValueSet(metadata)
        self.metadata.freeze()
        self.cached = cached
        self.viewOf = viewOf

    def _finish(self, universe: DimensionUniverse):
        """Finish construction of the element and add it to the given universe.

        For internal use by `DimensionUniverse` only.
        """
        # Attach set self.universe and add self to universe attributes;
        # let subclasses override which attributes by calling a separate
        # method.
        self._attachToUniverse(universe)
        # Expand direct dependencies into recursive dependencies.
        expanded = set(self._directDependencyNames)
        for name in self._directDependencyNames:
            expanded.update(universe[name]._recursiveDependencyNames)
        self._recursiveDependencyNames = frozenset(expanded)
        # Define self.implied, a public, sorted version of
        # self._impliedDependencyNames.
        self.implied = NamedValueSet(universe.sorted(self._impliedDependencyNames))
        self.implied.freeze()
        # Attach a DimensionGraph that provides the public API for getting
        # at requirements.  Again delegate to subclasses.
        self._attachGraph()
        # Create and attach a DimensionRecord subclass to hold values of this
        # dimension type.
        self.RecordClass = _subclassDimensionRecord(self)

    def _attachToUniverse(self, universe: DimensionUniverse):
        """Add the element to the given universe.

        Called only by `_finish`, but may be overridden by subclasses.
        """
        self.universe = universe
        self.universe.elements.add(self)

    def _attachGraph(self):
        """Initialize the `graph` attribute for this element.

        Called only by `_finish`, but may be overridden by subclasses.
        """
        from .graph import DimensionGraph
        self.graph = DimensionGraph(self.universe, names=self._recursiveDependencyNames, conform=False)

    def _shouldBeInGraph(self, dimensionNames: AbstractSet[str]):
        """Return `True` if this element should be included in `DimensionGraph`
        that includes the named dimensions.

        For internal use by `DimensionGraph` only.
        """
        return self._directDependencyNames.issubset(dimensionNames)

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.name})"

    def __eq__(self, other) -> bool:
        try:
            return self.name == other.name
        except AttributeError:
            return self.name == other

    def __hash__(self) -> int:
        return hash(self.name)

    def __lt__(self, other) -> bool:
        try:
            return self.universe._elementIndices[self] < self.universe._elementIndices[other]
        except KeyError:
            return NotImplemented

    def __le__(self, other) -> bool:
        try:
            return self.universe._elementIndices[self] <= self.universe._elementIndices[other]
        except KeyError:
            return NotImplemented

    def __gt__(self, other) -> bool:
        try:
            return self.universe._elementIndices[self] > self.universe._elementIndices[other]
        except KeyError:
            return NotImplemented

    def __ge__(self, other) -> bool:
        try:
            return self.universe._elementIndices[self] >= self.universe._elementIndices[other]
        except KeyError:
            return NotImplemented

    def hasTable(self) -> bool:
        """Return `True` if this element is associated with a table
        (even if that table "belongs" to another element).

        Instances of the `DimensionElement` base class itself are always
        associated with tables.
        """
        return True

    def makeTableSpec(self) -> Optional[TableSpec]:
        """Return a specification of the schema for the table corresponding
        to this element.

        This programmatically generates the primary and foreign key fields from
        the element's dependencies and then appends any metadata fields.

        Returns
        -------
        spec : `TableSpec` or `None`
            Database-agnostic specification of the fields in this table.
        """
        if not self.hasTable():
            return None
        return makeElementTableSpec(self)

    @classmethod
    def _unpickle(cls, universe: DimensionUniverse, name: str) -> DimensionElement:
        """Callable used for unpickling.

        For internal use only.
        """
        return universe.elements[name]

    def __reduce__(self) -> tuple:
        return (self._unpickle, (self.universe, self.name))

    # Class attributes below are shadowed by instance attributes, and are
    # present just to hold the docstrings for those instance attributes.

    universe: DimensionUniverse
    """The universe of all compatible dimensions with which this element is
    associated (`DimensionUniverse`).
    """

    name: str
    """Unique name for this dimension element (`str`).
    """

    graph: DimensionGraph
    """Minimal graph that includes this element (`DimensionGraph`).

    ``self.graph.required`` includes all dimensions whose primary key values
    must be provided in order to uniquely identify ``self`` (including ``self``
    if ``isinstance(self, Dimension)`.  ``self.graph.implied`` includes all
    dimensions also identified (possibly recursively) by this set.
    """

    implied: NamedValueSet[Dimension]
    """Other dimensions that are uniquely identified directly by a record of
    this dimension.

    Unlike ``self.graph.implied``, this set is not expanded recursively.
    """

    spatial: bool
    """Whether records of this element are associated with a region on the sky
    (`bool`).
    """

    temporal: bool
    """Whether records of this element are associated with a timespan (`bool`).
    """

    metadata: NamedValueSet[FieldSpec]
    """Additional metadata fields included in this element's table
    (`NamedValueSet` of `FieldSpec`).
    """

    RecordClass: type
    """The `DimensionRecord` subclass used to hold records for this element
    (`type`).

    Because `DimensionRecord` subclasses are generated dynamically, this type
    cannot be imported directly and hence canonly be obtained from this
    attribute.
    """

    cached: bool
    """Whether `Registry` should cache records of this element in-memory
    (`bool`).
    """

    viewOf: Optional[str]
    """Name of another table this elements records are drawn from (`str` or
    `None`).
    """


@immutable
class Dimension(DimensionElement):
    """A named data-organization concept that can be used as a key in a data
    ID.

    Parameters
    ----------
    name : `str`
        Name of the dimension.  Used as at least part of the table name, if
        the dimension in associated with a database table, and as an alternate
        key (instead of the instance itself) in data IDs.
    uniqueKeys : iterable of `FieldSpec`
        Fields that can *each* be used to uniquely identify this dimension
        (once all required dependencies are also identified).  The first entry
        will be used as the table's primary key and as a foriegn key field in
        the fields of dependent tables.
    kwds
        Additional keyword arguments are forwarded to the `DimensionElement`
        constructor.
    """

    def __init__(self, name: str, *, uniqueKeys: Iterable[FieldSpec], **kwds):
        super().__init__(name, **kwds)
        self.uniqueKeys = NamedValueSet(uniqueKeys)
        self.uniqueKeys.freeze()
        self.primaryKey, *alternateKeys = uniqueKeys
        self.alternateKeys = NamedValueSet(alternateKeys)
        self.alternateKeys.freeze()

    def _attachToUniverse(self, universe: DimensionUniverse):
        # Docstring inherited from DimensionElement._attachToUniverse.
        super()._attachToUniverse(universe)
        universe.dimensions.add(self)

    def _attachGraph(self):
        # Docstring inherited from DimensionElement._attachGraph.
        self.graph = DimensionGraph(self.universe,
                                    names=self._recursiveDependencyNames.union([self.name]),
                                    conform=False)

    def _shouldBeInGraph(self, dimensionNames: AbstractSet[str]):
        # Docstring inherited from DimensionElement._shouldBeInGraph.
        return self.name in dimensionNames

    # Class attributes below are shadowed by instance attributes, and are
    # present just to hold the docstrings for those instance attributes.

    uniqueKeys: NamedValueSet[FieldSpec]
    """All fields that can individually be used to identify records of this
    element, given the primary keys of all required dependencies
    (`NamedValueSet` of `FieldSpec`).
    """

    primaryKey: FieldSpec
    """The primary key field for this dimension (`FieldSpec`).

    Note that the database primary keys for dimension tables are in general
    compound; this field is the only field in the database primary key that is
    not also a foreign key (to a required dependency dimension table).
    """

    alternateKeys: NamedValueSet[FieldSpec]
    """Additional unique key fields for this dimension that are not the the
    primary key (`NamedValueSet` of `FieldSpec`).
    """


@immutable
class SkyPixDimension(Dimension):
    """A special `Dimension` subclass for hierarchical pixelizations of the
    sky.

    Unlike most other dimensions, skypix dimension records are not stored in
    the database, as these records only contain an integer pixel ID and a
    region on the sky, and each of these can be computed directly from the
    other.

    Parameters
    ----------
    name : `str`
        Name of the dimension.  By convention, this is a lowercase string
        abbreviation for the pixelization followed by its integer level,
        such as "htm7".
    pixelization : `sphgeom.Pixelization`
        Pixelization instance that can compute regions from IDs and IDs from
        points.
    """

    def __init__(self, name: str, pixelization: Pixelization):
        uniqueKeys = [FieldSpec(name="id", dtype=Integer, primaryKey=True, nullable=False)]
        super().__init__(name, uniqueKeys=uniqueKeys, spatial=True)
        self.pixelization = pixelization

    def hasTable(self) -> bool:
        # Docstring inherited from DimensionElement.hasTable.
        return False

    # Class attributes below are shadowed by instance attributes, and are
    # present just to hold the docstrings for those instance attributes.

    pixelization: Pixelization
    """Pixelization instance that can compute regions from IDs and IDs from
    points (`sphgeom.Pixelization`).
    """

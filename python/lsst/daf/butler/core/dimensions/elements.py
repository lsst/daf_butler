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

from typing import (
    Any,
    AbstractSet,
    Dict,
    Iterable,
    Optional,
    Set,
    Type,
    TYPE_CHECKING,
)

from sqlalchemy import BigInteger

from lsst.sphgeom import Pixelization
from ..utils import immutable
from ..named import NamedValueSet
from .. import ddl

if TYPE_CHECKING:  # Imports needed only for type annotations; may be circular.
    from .universe import DimensionUniverse
    from .graph import DimensionGraph
    from .records import DimensionRecord


class RelatedDimensions:
    """A semi-internal struct containing the names of the dimension elements
    related to the one holding the instance of this struct.

    This object is used as the type of `DimensionElement._related`, which is
    considered private _to the dimensions subpackage_, rather that private or
    protected within `DimensionElement` itself.

    Parameters
    ----------
    required : `set` [ `str` ]
        The names of other dimensions that are used to form the (compound)
        primary key for this element, as well as foreign keys.
    implied : `set` [ `str` ]
        The names of other dimensions that are used to define foreign keys
        for this element, but not primary keys.
    spatial : `str`, optional
        The name of a `DimensionElement` whose spatial regions this element's
        region aggregates, or the name of this element if it has a region
        that is not an aggregate.  `None` (default) if this element is not
        associated with region.
    temporal : `str`, optional
        The name of a `DimensionElement` whose timespans this element's
        timespan aggregates, or the name of this element if it has a timespan
        that is not an aggregate.  `None` (default) if this element is not
        associated with timespan.
    """
    def __init__(self, required: Set[str], implied: Set[str],
                 spatial: Optional[str] = None, temporal: Optional[str] = None):
        self.required = required
        self.implied = implied
        self.spatial = spatial
        self.temporal = temporal
        self.dependencies = set(self.required | self.implied)

    __slots__ = ("required", "implied", "spatial", "temporal", "dependencies")

    def expand(self, universe: DimensionUniverse) -> None:
        """Expand ``required`` and ``dependencies`` recursively.

        Parameters
        ----------
        universe : `DimensionUniverse`
            Object containing all other dimension elements.
        """
        for req in tuple(self.required):
            other = universe.elements[req]._related
            self.required.update(other.required)
            self.dependencies.update(other.dependencies)
        for dep in self.implied:
            other = universe.elements[dep]._related
            self.dependencies.update(other.dependencies)

    required: Set[str]
    """The names of dimensions that are used to form the (compound) primary key
    for this element, as well as foreign keys.

    For true `Dimension` instances, this should be constructed without the
    dimension's own name, with the `Dimension` itself adding it later (after
    `dependencies` is defined).
    """

    implied: Set[str]
    """The names of other dimensions that are used to define foreign keys for
    this element, but not primary keys.
    """

    dependencies: Set[str]
    """The names of all dimensions in `required` or `implied`.

    Immediately after construction, this is equal to the union of `required`
    `implied`.  After `expand` is called, this may not be true, as this will
    include the required and implied dependencies (recursively) of implied
    dependencies, while `implied` is not expanded recursively.

    For true `Dimension` instances, this never includes the dimension's own
    name.
    """

    spatial: Optional[str]
    """The name of a dimension element to which this element delegates spatial
    region handling, if any (see `DimensionElement.spatial`).
    """

    temporal: Optional[str]
    """The name of a dimension element to which this element delegates
    timespan handling, if any (see `DimensionElement.temporal`).
    """


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
    related : `RelatedDimensions`
        Struct containing the names of related dimensions.
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
                 related: RelatedDimensions,
                 metadata: Iterable[ddl.FieldSpec] = (),
                 cached: bool = False,
                 viewOf: Optional[str] = None,
                 alwaysJoin: bool = False):
        self.name = name
        self._related = related
        self.metadata = NamedValueSet(metadata)
        self.metadata.freeze()
        self.cached = cached
        self.viewOf = viewOf
        self.alwaysJoin = alwaysJoin

    def _finish(self, universe: DimensionUniverse, elementsToDo: Dict[str, DimensionElement]) -> None:
        """Finish construction of the element and add it to the given universe.

        For internal use by `DimensionUniverse` only.

        Parameters
        ----------
        universe : `DimensionUniverse`
            The under-construction dimension universe.  It can be relied upon
            to contain all dimensions that this element requires or implies.
        elementsToDo : `dict` [ `str`, `DimensionElement` ]
            A dictionary containing all dimension elements that have not yet
            been added to ``universe``, keyed by name.
        """
        # Attach set self.universe and add self to universe attributes;
        # let subclasses override which attributes by calling a separate
        # method.
        self._attachToUniverse(universe)
        # Expand dependencies.
        self._related.expand(universe)
        # Define public DimensionElement versions of some of the name sets
        # in self._related.
        self.required = NamedValueSet(universe.sorted(self._related.required))
        self.required.freeze()
        self.implied = NamedValueSet(universe.sorted(self._related.implied))
        self.implied.freeze()
        # Set self.spatial and self.temporal to DimensionElement instances from
        # the private _related versions of those.
        for s in ("spatial", "temporal"):
            targetName = getattr(self._related, s, None)
            if targetName is None:
                target = None
            elif targetName == self.name:
                target = self
            else:
                target = universe.elements.get(targetName)
                if target is None:
                    try:
                        target = elementsToDo[targetName]
                    except KeyError as err:
                        raise LookupError(
                            f"Could not find {s} provider {targetName} for {self.name}."
                        ) from err
            setattr(self, s, target)
        # Attach a DimensionGraph that provides the public API for getting
        # at requirements.  Again delegate to subclasses.
        self._attachGraph()
        # Create and attach a DimensionRecord subclass to hold values of this
        # dimension type.
        from .records import _subclassDimensionRecord
        self.RecordClass = _subclassDimensionRecord(self)

    def _attachToUniverse(self, universe: DimensionUniverse) -> None:
        """Add the element to the given universe.

        Called only by `_finish`, but may be overridden by subclasses.
        """
        self.universe = universe
        self.universe.elements.add(self)

    def _attachGraph(self) -> None:
        """Initialize the `graph` attribute for this element.

        Called only by `_finish`, but may be overridden by subclasses.
        """
        from .graph import DimensionGraph
        self.graph = DimensionGraph(self.universe, names=self._related.dependencies, conform=False)

    def _shouldBeInGraph(self, dimensionNames: AbstractSet[str]) -> bool:
        """Return `True` if this element should be included in `DimensionGraph`
        that includes the named dimensions.

        For internal use by `DimensionGraph` only.
        """
        return self._related.required.issubset(dimensionNames)

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.name})"

    def __eq__(self, other: Any) -> bool:
        try:
            return self.name == other.name
        except AttributeError:
            return self.name == other

    def __hash__(self) -> int:
        return hash(self.name)

    def __lt__(self, other: DimensionElement) -> bool:
        try:
            return self.universe._elementIndices[self.name] < self.universe._elementIndices[other.name]
        except KeyError:
            return NotImplemented

    def __le__(self, other: DimensionElement) -> bool:
        try:
            return self.universe._elementIndices[self.name] <= self.universe._elementIndices[other.name]
        except KeyError:
            return NotImplemented

    def __gt__(self, other: DimensionElement) -> bool:
        try:
            return self.universe._elementIndices[self.name] > self.universe._elementIndices[other.name]
        except KeyError:
            return NotImplemented

    def __ge__(self, other: DimensionElement) -> bool:
        try:
            return self.universe._elementIndices[self.name] >= self.universe._elementIndices[other.name]
        except KeyError:
            return NotImplemented

    def hasTable(self) -> bool:
        """Return `True` if this element is associated with a table
        (even if that table "belongs" to another element).

        Instances of the `DimensionElement` base class itself are always
        associated with tables.
        """
        return True

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
    are sufficient (often necessary) to uniquely identify ``self``
    (including ``self` if ``isinstance(self, Dimension)``.
    ``self.graph.implied`` includes all dimensions also identified (possibly
    recursively) by this set.
    """

    required: NamedValueSet[Dimension]
    """Dimensions that are sufficient (often necessary) to uniquely identify
    a record of this dimension element.

    For elements with a database representation, these dimension are exactly
    those used to form the (possibly compound) primary key, and all dimensions
    here that are not ``self`` are also used to form foreign keys.

    For `Dimension` instances, this should be exactly the same as
    ``graph.required``, but that may not be true for `DimensionElement`
    instances in general.  When they differ, there are multiple combinations
    of dimensions that uniquely identify this element, but this one is more
    direct.
    """

    implied: NamedValueSet[Dimension]
    """Other dimensions that are uniquely identified directly by a record of
    this dimension element.

    For elements with a database representation, these are exactly the
    dimensions used to form foreign key constraints whose fields are not
    (wholly) also part of the primary key.

    Unlike ``self.graph.implied``, this set is not expanded recursively.
    """

    spatial: Optional[DimensionElement]
    """A `DimensionElement` whose spatial regions this element's region
    aggregates, or ``self`` if it has a region that is not an aggregate
    (`DimensionElement` or `None`).
    """

    temporal: Optional[DimensionElement]
    """A `DimensionElement` whose timespans this element's timespan
    aggregates, or ``self`` if it has a timespan that is not an aggregate
    (`DimensionElement` or `None`).
    """

    metadata: NamedValueSet[ddl.FieldSpec]
    """Additional metadata fields included in this element's table
    (`NamedValueSet` of `FieldSpec`).
    """

    RecordClass: Type[DimensionRecord]
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
    related : `RelatedDimensions`
        Struct containing the names of related dimensions.
    uniqueKeys : iterable of `FieldSpec`
        Fields that can *each* be used to uniquely identify this dimension
        (once all required dependencies are also identified).  The first entry
        will be used as the table's primary key and as a foriegn key field in
        the fields of dependent tables.
    kwds
        Additional keyword arguments are forwarded to the `DimensionElement`
        constructor.
    """

    def __init__(self, name: str, *, related: RelatedDimensions, uniqueKeys: Iterable[ddl.FieldSpec],
                 **kwargs: Any):
        related.required.add(name)
        super().__init__(name, related=related, **kwargs)
        self.uniqueKeys = NamedValueSet(uniqueKeys)
        self.uniqueKeys.freeze()
        self.primaryKey, *alternateKeys = uniqueKeys
        self.alternateKeys = NamedValueSet(alternateKeys)
        self.alternateKeys.freeze()

    def _attachToUniverse(self, universe: DimensionUniverse) -> None:
        # Docstring inherited from DimensionElement._attachToUniverse.
        super()._attachToUniverse(universe)
        universe.dimensions.add(self)

    def _attachGraph(self) -> None:
        # Docstring inherited from DimensionElement._attachGraph.
        from .graph import DimensionGraph
        self.graph = DimensionGraph(self.universe,
                                    names=self._related.dependencies.union([self.name]),
                                    conform=False)

    def _shouldBeInGraph(self, dimensionNames: AbstractSet[str]) -> bool:
        # Docstring inherited from DimensionElement._shouldBeInGraph.
        return self.name in dimensionNames

    # Class attributes below are shadowed by instance attributes, and are
    # present just to hold the docstrings for those instance attributes.

    uniqueKeys: NamedValueSet[ddl.FieldSpec]
    """All fields that can individually be used to identify records of this
    element, given the primary keys of all required dependencies
    (`NamedValueSet` of `FieldSpec`).
    """

    primaryKey: ddl.FieldSpec
    """The primary key field for this dimension (`FieldSpec`).

    Note that the database primary keys for dimension tables are in general
    compound; this field is the only field in the database primary key that is
    not also a foreign key (to a required dependency dimension table).
    """

    alternateKeys: NamedValueSet[ddl.FieldSpec]
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
        related = RelatedDimensions(required=set(), implied=set(), spatial=name)
        uniqueKeys = [ddl.FieldSpec(name="id", dtype=BigInteger, primaryKey=True, nullable=False)]
        super().__init__(name, related=related, uniqueKeys=uniqueKeys)
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

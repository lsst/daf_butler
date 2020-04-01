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

__all__ = ("DataCoordinate", "ExpandedDataCoordinate", "DataId")

import numbers
from typing import Any, Tuple, Mapping, Optional, Dict, Union, TYPE_CHECKING

from lsst.sphgeom import Region
from ..utils import IndexedTupleDict, immutable
from ..timespan import Timespan
from .graph import DimensionGraph

if TYPE_CHECKING:  # Imports needed only for type annotations; may be circular.
    from .elements import DimensionElement, Dimension
    from .universe import DimensionUniverse
    from .records import DimensionRecord


@immutable
class DataCoordinate(IndexedTupleDict):
    """An immutable data ID dictionary that guarantees that its key-value pairs
    identify all required dimensions in a `DimensionGraph`.

    `DataCoordinate` instances should usually be constructed via the
    `standardize` class method; the constructor is reserved for callers that
    can guarantee that the ``values`` tuple has exactly the right elements.

    Parameters
    ----------
    graph : `DimensionGraph`
        The dimensions identified by this instance.
    values : `tuple`
        Tuple of primary key values for the given dimensions.

    Notes
    -----
    Like any data ID class, `DataCoordinate` behaves like a dictionary,
    mostly via methods inherited from `IndexedTupleDict`.  Like `NamedKeyDict`,
    both `Dimension` instances and `str` names thereof may be used as keys in
    lookup operations.

    Subclasses are permitted to support lookup for any dimension in
    ``self.graph.dimensions``, but the base class only supports lookup for
    those in ``self.graph.required``, which is the minimal set needed to
    identify all others in a `Registry`.  Both the base class and subclasses
    define comparisons, iterators, and the `keys`, `values`, and `items` views
    to just the ``self.graph.required`` subset in order to guarantee true
    (i.e. Liskov) substitutability.
    """

    __slots__ = ("graph",)

    def __new__(cls, graph: DimensionGraph, values: Tuple[Any, ...]):
        self = super().__new__(cls, graph._requiredIndices, values)
        self.graph = graph
        return self

    @staticmethod
    def standardize(mapping: Optional[Mapping[str, Any]] = None, *,
                    graph: Optional[DimensionGraph] = None,
                    universe: Optional[DimensionUniverse] = None,
                    **kwds) -> DataCoordinate:
        """Adapt an arbitrary mapping and/or additional arguments into a true
        `DataCoordinate`, or augment an existing one.

        Parameters
        ----------
        mapping : `~collections.abc.Mapping`, optional
            An informal data ID that maps dimension names to their primary key
            values (may also be a true `DataCoordinate`).
        graph : `DimensionGraph`
            The dimensions to be identified by the new `DataCoordinate`.
            If not provided, will be inferred from the keys of ``mapping``,
            and ``universe`` must be provided unless ``mapping`` is already a
            `DataCoordinate`.
        universe : `DimensionUniverse`
            All known dimensions and their relationships; used to expand
            and validate dependencies when ``graph`` is not provided.
        kwds
            Additional keyword arguments are treated like additional key-value
            pairs in ``mapping``.

        Returns
        -------
        coordinate : `DataCoordinate`
            A validated `DataCoordinate` instance.  May be a subclass instance
            if and only if ``mapping`` is a subclass instance and ``graph``
            is a subset of ``mapping.graph``.

        Raises
        ------
        TypeError
            Raised if the set of optional arguments provided is not supported.
        KeyError
            Raised if a key-value pair for a required dimension is missing.

        Notes
        -----
        Because `DataCoordinate` stores only values for required dimensions,
        key-value pairs for other related dimensions will be ignored and
        excluded from the result.  This means that a `DataCoordinate` may
        contain *fewer* key-value pairs than the informal data ID dictionary
        it was constructed from.
        """
        if isinstance(mapping, DataCoordinate):
            if graph is None:
                if not kwds:
                    # Already standardized to exactly what we want.
                    return mapping
            elif mapping.graph.issuperset(graph):
                # Already standardized; just return the relevant subset.
                return mapping.subset(graph)
            assert universe is None or universe == mapping.universe
            universe = mapping.universe
        if kwds:
            if mapping:
                try:
                    d = dict(mapping.byName(), **kwds)
                except AttributeError:
                    d = dict(mapping, **kwds)
            else:
                d = kwds
        elif mapping:
            try:
                d = mapping.byName()
            except AttributeError:
                d = mapping
        else:
            d = {}
        if graph is None:
            if universe is None:
                raise TypeError("universe must be provided if graph is not.")
            graph = DimensionGraph(universe, names=d.keys())
        try:
            values = tuple(d[name] for name in graph.required.names)
            # some backends cannot handle numpy.int64 type which is
            # a subclass of numbers.Integral, convert that to int.
            values = tuple(int(val) if isinstance(val, numbers.Integral) else val for val in values)
        except KeyError as err:
            raise KeyError(f"No value in data ID ({mapping}) for required dimension {err}.") from err
        return DataCoordinate(graph, values)

    def byName(self) -> Dict[str, Any]:
        """Return a true `dict` keyed by `str` dimension name and the same
        values as ``self``.
        """
        return {k.name: v for k, v in self.items()}

    def __getnewargs__(self) -> tuple:
        # Implements pickle support (in addition to methods provided by
        # @immutable decorator).
        return (self.graph, self.values())

    def __hash__(self) -> int:
        return hash((self.graph, self.values()))

    def __eq__(self, other: DataCoordinate) -> bool:
        try:
            # Optimized code path for DataCoordinate comparisons.
            return self.graph == other.graph and self.values() == other.values()
        except AttributeError:
            # Also support comparison with informal data ID dictionaries that
            # map dimension name to value.
            return self.byName() == other

    def __str__(self):
        return f"{self.byName()}"

    def __repr__(self):
        return f"DataCoordinate({self.graph}, {self.values()})"

    def fingerprint(self, update):
        """Update a secure hash function with the values in this data ID.

        Parameters
        ----------
        update : `~collections.abc.Callable`
            Callable that accepts a single `bytes` argument to update
            the hash; usually the ``update`` method of an instance from
            the ``hashlib`` module.
        """
        for k, v in self.items():
            update(k.name.encode("utf8"))
            if isinstance(v, numbers.Integral):
                update(int(v).to_bytes(64, "big", signed=False))
            elif isinstance(v, str):
                update(v.encode("utf8"))
            else:
                raise TypeError(f"Only `int` and `str` are allowed as dimension keys, not {v} ({type(v)}).")

    def matches(self, other: DataCoordinate) -> bool:
        """Test whether the values of all keys in both coordinates are equal.

        Parameters
        ----------
        other : `DataCoordinate`
            The other coordinate to compare to.

        Returns
        -------
        consistent : `bool`
            `True` if all keys that are in in both ``other`` and ``self``
            are associated with the same values, and `False` otherwise.
            `True` if there are no keys in common.
        """
        d = getattr(other, "full", other)
        return all(self[k] == d[k] for k in (self.keys() & d.keys()))

    def subset(self, graph: DimensionGraph) -> DataCoordinate:
        """Return a new `DataCoordinate` whose graph is a subset of
        ``self.graph``.

        Subclasses may override this method to return a subclass instance.

        Parameters
        ----------
        graph : `DimensionGraph`
            The dimensions identified by the returned `DataCoordinate`.

        Returns
        -------
        coordinate : `DataCoordinate`
            A `DataCoordinate` instance that identifies only the given
            dimensions.

        Raises
        ------
        KeyError
            Raised if ``graph`` is not a subset of ``self.graph``, and hence
            one or more dimensions has no associated primary key value.
        """
        return DataCoordinate(graph, tuple(self[dimension] for dimension in graph.required))

    @property
    def universe(self) -> DimensionUniverse:
        """The universe that defines all known dimensions compatible with
        this coordinate (`DimensionUniverse`).
        """
        return self.graph.universe

    # Class attributes below are shadowed by instance attributes, and are
    # present just to hold the docstrings for those instance attributes.

    graph: DimensionGraph
    """The dimensions identified by this data ID (`DimensionGraph`).

    Note that values are only required to be present for dimensions in
    ``self.graph.required``; all others may be retrieved (from a `Registry`)
    given these.
    """


DataId = Union[DataCoordinate, Mapping[str, Any]]
"""A type-annotation alias for signatures that accept both informal data ID
dictionaries and validated `DataCoordinate` instances.
"""


def _intersectRegions(*args: Region) -> Optional[Region]:
    """Return the intersection of several regions.

    For internal use by `ExpandedDataCoordinate` only.

    If no regions are provided, returns `None`.

    This is currently a placeholder; it actually returns `NotImplemented`
    (it does *not* raise an exception) when multiple regions are given, which
    propagates to `ExpandedDataCoordinate`.  This reflects the fact that we
    don't want to fail to construct an `ExpandedDataCoordinate` entirely when
    we can't compute its region, and at present we don't have a high-level use
    case for the regions of these particular data IDs.
    """
    if len(args) == 0:
        return None
    elif len(args) == 1:
        return args[0]
    else:
        return NotImplemented


@immutable
class ExpandedDataCoordinate(DataCoordinate):
    """A data ID that has been expanded to include all relevant metadata.

    Instances should usually be obtained by calling `Registry.expandDataId`.

    Parameters
    ----------
    graph : `DimensionGraph`
        The dimensions identified by this instance.
    values : `tuple`
        Tuple of primary key values for the given dimensions.
    records : `~collections.abc.Mapping`
        Dictionary mapping `DimensionElement` to `DimensionRecord`.
    full : `~collections.abc.Mapping`
        Dictionary mapping dimensions to their primary key values for all
        dimensions in the graph, not just required ones.  Ignored unless
        ``conform`` is `False.`
    region : `sphgeom.Region`, optional
        Region on the sky associated with this data ID, or `None` if there
        are no spatial dimensions.  At present, this may be the special value
        `NotImplemented` if there multiple spatial dimensions identified; in
        the future this will be replaced with the intersection.  Ignored unless
        ``conform`` is `False`.Timespan
    timespan : `Timespan`, optionalTimespan
        Timespan associated with this data ID, or `None` if there are no
        temporal dimensions.
        Ignored unless ``conform`` is `False`.
    conform : `bool`, optional
        If `True` (default), adapt arguments from arbitrary mappings to the
        custom dictionary types and check that all expected key-value pairs are
        present.  `False` is only for internal use.

    Notes
    -----
    To maintain Liskov substitutability with `DataCoordinate`,
    `ExpandedDataCoordinate` mostly acts like a mapping that contains only
    values for its graph's required dimensions, even though it also contains
    values for all implied dimensions - its length, iteration, and
    keys/values/items views reflect only required dimensions.  Values for
    the primary keys of implied dimensions can be obtained from the `full`
    attribute, and are also accessible in dict lookups and the ``in`` operator.
    """

    __slots__ = ("records", "full", "region", "timespan")

    def __new__(cls, graph: DimensionGraph, values: Tuple[Any, ...], *,
                records: Mapping[DimensionElement, DimensionRecord],
                full: Optional[Mapping[Dimension, Any]] = None,
                region: Optional[Region] = None,
                timespan: Optional[Timespan] = None,
                conform: bool = True):
        self = super().__new__(cls, graph, values)
        if conform:
            self.records = IndexedTupleDict(
                indices=graph._elementIndices,
                values=tuple(records[element] for element in graph.elements)
            )
            self.full = IndexedTupleDict(
                indices=graph._dimensionIndices,
                values=tuple(getattr(self.records[dimension], dimension.primaryKey.name, None)
                             for dimension in graph.dimensions)
            )
            regions = []
            for element in self.graph.spatial:
                record = self.records[element.name]
                if record is None or record.region is None:
                    self.region = None
                    break
                else:
                    regions.append(record.region)
            else:
                self.region = _intersectRegions(*regions)
            timespans = []
            for element in self.graph.temporal:
                record = self.records[element.name]
                if record is None or record.timespan is None:
                    self.timespan = None
                    break
                else:
                    timespans.append(record.timespan)
            else:
                self.timespan = Timespan.intersection(*timespans)
        else:
            self.records = records
            self.full = full
            self.region = region
            self.timespan = timespan
        return self

    def __contains__(self, key: Union[DimensionElement, str]) -> bool:
        return key in self.full

    def __getitem__(self, key: Union[DimensionElement, str]) -> Any:
        return self.full[key]

    def __repr__(self):
        return f"ExpandedDataCoordinate({self.graph}, {self.values()})"

    def pack(self, name: str, *, returnMaxBits: bool = False) -> int:
        """Pack this data ID into an integer.

        Parameters
        ----------
        name : `str`
            Name of the `DimensionPacker` algorithm (as defined in the
            dimension configuration).
        returnMaxBits : `bool`, optional
            If `True` (`False` is default), return the maximum number of
            nonzero bits in the returned integer across all data IDs.

        Returns
        -------
        packed : `int`
            Integer ID.  This ID is unique only across data IDs that have
            the same values for the packer's "fixed" dimensions.
        maxBits : `int`, optional
            Maximum number of nonzero bits in ``packed``.  Not returned unless
            ``returnMaxBits`` is `True`.
        """
        return self.universe.makePacker(name, self).pack(self, returnMaxBits=returnMaxBits)

    def matches(self, other) -> bool:
        # Docstring inherited from DataCoordinate.matches.
        d = getattr(other, "full", other)
        return all(self[k] == d[k] for k in (self.full.keys() & d.keys()))

    def subset(self, graph: DimensionGraph) -> ExpandedDataCoordinate:
        # Docstring inherited from DataCoordinate.subset.
        return ExpandedDataCoordinate(
            graph,
            tuple(self[dimension] for dimension in graph.required),
            records=self.records,
            conform=True
        )

    def __getnewargs_ex__(self) -> Tuple(tuple, dict):
        return (
            (self.graph, self.values()),
            dict(
                records=self.records,
                full=self.full,
                region=self.region,
                timespan=self.timespan,
                conform=False,
            )
        )

    # Class attributes below are shadowed by instance attributes, and are
    # present just to hold the docstrings for those instance attributes.

    full: IndexedTupleDict[Dimension, Any]
    """Dictionary mapping dimensions to their primary key values for all
    dimensions in the graph, not just required ones (`IndexedTupleDict`).

    Like `DataCoordinate` itself, this dictionary can be indexed by `str` name
    as well as `Dimension` instance.
    """

    records: IndexedTupleDict[DimensionElement, DimensionRecord]
    """Dictionary mapping `DimensionElement` to the associated
    `DimensionRecord` (`IndexedTupleDict`).

    Like `DataCoordinate` itself, this dictionary can be indexed by `str` name
    as well as `DimensionElement` instance.
    """

    region: Optional[Region]
    """Region on the sky associated with this data ID, or `None` if there
    are no spatial dimensions (`sphgeom.Region`).

    At present, this may be the special value `NotImplemented` if there
    multiple spatial dimensions identified; in the future this will be replaced
    with the intersection.
    """

    timespan: Optional[Timespan]
    """Timespan associated with this data ID, or `None` if there are no
    temporal dimensions (`TimeSpan`).
    """

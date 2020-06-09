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
from typing import (
    Any,
    Mapping,
    Optional,
    Tuple,
    TYPE_CHECKING,
    Union,
)

from lsst.sphgeom import Region
from ..named import IndexedTupleDict, NamedKeyMapping
from ..timespan import Timespan
from .elements import Dimension
from .graph import DimensionGraph

if TYPE_CHECKING:  # Imports needed only for type annotations; may be circular.
    from .elements import DimensionElement
    from .universe import DimensionUniverse
    from .records import DimensionRecord


class DataCoordinate(IndexedTupleDict[Dimension, Any]):
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

    __slots__ = ("_graph",)

    def __init__(self, graph: DimensionGraph, values: Tuple[Any, ...]):
        super().__init__(graph._requiredIndices, values)
        self._graph = graph

    @staticmethod
    def standardize(mapping: Optional[Union[Mapping[str, Any], NamedKeyMapping[Dimension, Any]]] = None, *,
                    graph: Optional[DimensionGraph] = None,
                    universe: Optional[DimensionUniverse] = None,
                    **kwargs: Any) -> DataCoordinate:
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
        **kwargs
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
                if not kwargs:
                    # Already standardized to exactly what we want.
                    return mapping
            elif mapping.graph.issuperset(graph) and kwargs.keys().isdisjoint(graph.names):
                # Already standardized; just return the relevant subset.
                return mapping.subset(graph)
            assert universe is None or universe == mapping.universe
            universe = mapping.universe
        d: Mapping[str, Any]
        if kwargs:
            if mapping:
                if isinstance(mapping, NamedKeyMapping):
                    d = dict(mapping.byName(), **kwargs)
                else:
                    d = dict(mapping, **kwargs)
            else:
                d = kwargs
        elif mapping:
            if isinstance(mapping, NamedKeyMapping):
                d = mapping.byName()
            else:
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

    def __hash__(self) -> int:
        return hash((self.graph, self.values()))

    def __eq__(self, other: Any) -> bool:
        try:
            # Optimized code path for DataCoordinate comparisons.
            return self.graph == other.graph and self.values() == other.values()
        except AttributeError:
            # We can't reliably compare to informal data ID dictionaries
            # we don't know if any extra keys they might have are consistent
            # with an `ExpandedDataCoordinate` version of ``self`` (which
            # should compare as equal) or something else (which should
            # compare as not equal).
            # We don't even want to return `NotImplemented` and tell Python
            # to delegate to ``other.__eq__``, because that could also be
            # misleading.  We raise TypeError instead.
            raise TypeError("Cannot compare DataCoordinate instances to other objects without potentially "
                            "misleading results.") from None

    def __repr__(self) -> str:
        # We can't make repr yield something that could be exec'd here without
        # printing out the whole DimensionUniverse the graph is derived from.
        # So we print something that mostly looks like a dict, but doesn't
        # quote it's keys: that's both more compact and something that can't
        # be mistaken for an actual dict or something that could be exec'd.
        return "{{{}}}".format(', '.join(f"{k.name}: {v!r}" for k, v in self.items()))

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

    @property
    def graph(self) -> DimensionGraph:
        """The dimensions identified by this data ID (`DimensionGraph`).

        Note that values are only required to be present for dimensions in
        ``self.graph.required``; all others may be retrieved (from a
        `Registry`) given these.
        """
        return self._graph


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

    __slots__ = ("_records", "_full", "_region", "_timespan")

    def __init__(self, graph: DimensionGraph, values: Tuple[Any, ...], *,
                 records: NamedKeyMapping[DimensionElement, Optional[DimensionRecord]],
                 full: Optional[NamedKeyMapping[Dimension, Any]] = None,
                 region: Optional[Region] = None,
                 timespan: Optional[Timespan] = None,
                 conform: bool = True):
        super().__init__(graph, values)
        if conform:
            self._records = IndexedTupleDict(
                indices=graph._elementIndices,
                values=tuple(records[element.name] for element in graph.elements)
            )
            self._full = IndexedTupleDict(
                indices=graph._dimensionIndices,
                values=tuple(getattr(self.records[dimension], dimension.primaryKey.name, None)
                             for dimension in graph.dimensions)
            )
            regions = []
            for element in self.graph.spatial:
                record = self.records[element.name]
                # DimensionRecord subclasses for spatial elements always have a
                # .region, but they're dynamic so this can't be type-checked.
                if record is None or record.region is None:  # type: ignore
                    self._region = None
                    break
                else:
                    regions.append(record.region)  # type:ignore
            else:
                self._region = _intersectRegions(*regions)
            timespans = []
            for element in self.graph.temporal:
                record = self.records[element.name]
                # DimensionRecord subclasses for temporal elements always have
                # .timespan, but they're dynamic so this can't be type-checked.
                if record is None or record.timespan is None:  # type:ignore
                    self._timespan = None
                    break
                else:
                    timespans.append(record.timespan)  # type:ignore
            else:
                self._timespan = Timespan.intersection(*timespans)
        else:
            # User has declared that the types are correct; ignore them.
            self._records = records  # type: ignore
            self._full = full  # type: ignore
            self._region = region
            self._timespan = timespan

    def __contains__(self, key: Any) -> bool:
        return key in self.full

    def __getitem__(self, key: Union[Dimension, str]) -> Any:
        return self.full[key]

    def __repr__(self) -> str:
        # See DataCoordinate.__repr__ comment for reasoning behind this form.
        # The expanded version just includes key-value pairs for implied
        # dimensions.
        return "{{{}}}".format(', '.join(f"{k.name}: {v!r}" for k, v in self.full.items()))

    def pack(self, name: str, *, returnMaxBits: bool = False) -> Union[Tuple[int, int], int]:
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

    def subset(self, graph: DimensionGraph) -> ExpandedDataCoordinate:
        # Docstring inherited from DataCoordinate.subset.
        return ExpandedDataCoordinate(
            graph,
            tuple(self[dimension] for dimension in graph.required),
            records=self.records,
            conform=True
        )

    @property
    def full(self) -> NamedKeyMapping[Dimension, Any]:
        """Dictionary mapping dimensions to their primary key values for all
        dimensions in the graph, not just required ones (`NamedKeyMapping`).

        Like `DataCoordinate` itself, this dictionary can be indexed by `str`
        name as well as `Dimension` instance.
        """
        return self._full

    @property
    def records(self) -> NamedKeyMapping[DimensionElement, Optional[DimensionRecord]]:
        """Dictionary mapping `DimensionElement` to the associated
        `DimensionRecord` (`NamedKeyMapping`).

        Like `DataCoordinate` itself, this dictionary can be indexed by `str`
        name as well as `DimensionElement` instance.
        """
        return self._records

    @property
    def region(self) -> Optional[Region]:
        """Region on the sky associated with this data ID, or `None` if there
        are no spatial dimensions (`sphgeom.Region`).

        At present, this may be the special value `NotImplemented` if there
        multiple spatial dimensions identified; in the future this will be
        replaced with the intersection.
        """
        return self._region

    @property
    def timespan(self) -> Optional[Timespan]:
        """Timespan associated with this data ID, or `None` if there are no
        temporal dimensions (`TimeSpan`).
        """
        return self._timespan

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

__all__ = ("DimensionRecord", "ExpandedDataCoordinate")

from typing import Dict, Any, Optional, Type, Tuple, Mapping, Union

from lsst.sphgeom import Region
from ..timespan import Timespan
from ..utils import IndexedTupleDict, immutable
from .elements import DimensionElement, Dimension
from .graph import DimensionGraph
from .coordinate import DataCoordinate


def _reconstructDimensionRecord(definition, *args):
    """Unpickle implementation for `DimensionRecord` subclasses.

    For internal use by `DimensionRecord`.
    """
    return definition.RecordClass(*args)


def _makeTimespanFromRecord(record: DimensionRecord):
    """Extract a `Timespan` object from the appropriate endpoint attributes.

    For internal use by `DimensionRecord`.
    """
    from .schema import TIMESPAN_FIELD_SPECS
    return Timespan(
        begin=getattr(record, TIMESPAN_FIELD_SPECS.begin.name),
        end=getattr(record, TIMESPAN_FIELD_SPECS.end.name),
    )


def _subclassDimensionRecord(definition: DimensionElement) -> Type[DimensionRecord]:
    """Create a dynamic subclass of `DimensionRecord` for the given
    `DimensionElement`.

    For internal use by `DimensionRecord`.
    """
    from .schema import makeElementTableSpec
    d = {
        "definition": definition,
        "__slots__": tuple(makeElementTableSpec(definition).fields.names)
    }
    if definition.temporal:
        d["timespan"] = property(_makeTimespanFromRecord)
    return type(definition.name + ".RecordClass", (DimensionRecord,), d)


class DimensionRecord:
    """Base class for the Python representation of database records for
    a `DimensionElement`.

    Parameters
    ----------
    args
        Field values for this record, ordered to match ``__slots__``.

    Notes
    -----
    `DimensionRecord` subclasses are created dynamically for each
    `DimensionElement` in a `DimensionUniverse`, and are accessible via the
    `DimensionElement.RecordClass` attribute.  The `DimensionRecord` base class
    itself is pure abstract, but does not use the `abc` module to indicate this
    because it does not have overridable methods.

    Record classes have attributes that correspond exactly to the fields in the
    related database table, a few additional methods inherited from the
    `DimensionRecord` base class, and two additional injected attributes:

     - ``definition`` is a class attribute that holds the `DimensionElement`;

     - ``timespan`` is a property that returns a `Timespan`, present only
       on record classes that correspond to temporal elements.

    The field attributes are defined via the ``__slots__`` mechanism, and the
    ``__slots__`` tuple itself is considered the public interface for obtaining
    the list of fields.  It is guaranteed to be equal to
    ``DimensionElement.makeTableSpec().fields.names`` when ``makeTableSpec``
    does not return `None`.

    Instances are usually obtained from a `Registry`, but in the rare cases
    where they are constructed directly in Python (usually for insertion into
    a `Registry`), the `fromDict` method should generally be used.

    `DimensionRecord` instances are immutable.
    """

    # Derived classes are required to define __slots__ as well, and it's those
    # derived-class slots that other methods on the base class expect to see
    # when they access self.__slots__.
    __slots__ = ("dataId",)

    def __init__(self, *args):
        for attrName, value in zip(self.__slots__, args):
            object.__setattr__(self, attrName, value)
        dataId = DataCoordinate(
            self.definition.graph,
            args[:len(self.definition.graph.required.names)]
        )
        object.__setattr__(self, "dataId", dataId)

    @classmethod
    def fromDict(cls, mapping: Mapping[str, Any]):
        """Construct a `DimensionRecord` subclass instance from a mapping
        of field values.

        Parameters
        ----------
        mapping : `~collections.abc.Mapping`
            Field values, keyed by name.  The keys must match those in
            ``__slots__``, with the exception that a dimension name
            may be used in place of the primary key name - for example,
            "tract" may be used instead of "id" for the "id" primary key
            field of the "tract" dimension.

        Returns
        -------
        record : `DimensionRecord`
            An instance of this subclass of `DimensionRecord`.
        """
        # If the name of the dimension is present in the given dict, use it
        # as the primary key value instead of expecting the field name.
        # For example, allow {"instrument": "HSC", ...} instead of
        # {"name": "HSC", ...} when building a record for instrument dimension.
        primaryKey = mapping.get(cls.definition.name)
        if primaryKey is not None:
            d = dict(mapping)
            d[cls.definition.primaryKey.name] = primaryKey
        else:
            d = mapping
        values = tuple(d.get(k) for k in cls.__slots__)
        return cls(*values)

    def __reduce__(self):
        args = tuple(getattr(self, name) for name in self.__slots__)
        return (_reconstructDimensionRecord, (self.definition,) + args)

    def toDict(self) -> Dict[str, Any]:
        """Return a vanilla `dict` representation of this record.
        """
        return {name: getattr(self, name) for name in self.__slots__}

    # Class attributes below are shadowed by instance attributes, and are
    # present just to hold the docstrings for those instance attributes.

    dataId: DataCoordinate
    """A dict-like identifier for this record's primary keys
    (`DataCoordinate`).
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
    full : `~collections.abc.Mapping`
        Dictionary mapping dimensions to their primary key values for all
        dimensions in the graph, not just required ones.
    records : `~collections.abc.Mapping`
        Dictionary mapping `DimensionElement` to `DimensionRecord`.
    region : `sphgeom.Region`, optional
        Region on the sky associated with this data ID, or `None` if there
        are no spatial dimensions.  At present, this may be the special value
        `NotImplemented` if there multiple spatial dimensions identified; in
        the future this will be replaced with the intersection.
    timespan : `Timespan`, optional
        Timespan associated with this data ID, or `None` if there are no
        temporal dimensions.
    conform : `bool`, optional
        If `True` (default) adapt arguments from arbitrary mappings to
        `NamedKeyDict` and check that all expected key-value pairs are
        present.  `False` is primarily for internal use, or at least
        callers that can independently guarantee correctness.

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
                values=tuple(getattr(self.records[dimension], dimension.primaryKey.name)
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
            )
        )

    # Class attributes below are shadowed by instance attributes, and are
    # present just to hold the docstrings for those instance attributes.

    full: IndexedTupleDict[Dimension, Any]
    """Dictionary mapping dimensions to their primary key values for all
    dimensions in the graph, not just required ones (`NamedKeyDict`).
    """

    records: IndexedTupleDict[DimensionElement, DimensionRecord]
    """Dictionary mapping `DimensionElement` to the associated
    `DimensionRecord` (`NamedKeyDict`).
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

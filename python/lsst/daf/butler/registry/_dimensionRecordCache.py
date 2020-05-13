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

__all__ = ("ConsistentDataIds", "DimensionRecordCache",)

from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List, MutableMapping, Optional, TYPE_CHECKING

import lsst.sphgeom
from ..core import (
    DataCoordinate,
    DataId,
    DimensionElement,
    DimensionGraph,
    DimensionRecord,
    ExpandedDataCoordinate,
)
from ..core.utils import NamedKeyDict
from ._exceptions import InconsistentDataIdError

if TYPE_CHECKING:
    from ..core import Timespan
    from .interfaces import DimensionRecordStorageManager


@dataclass
class ConsistentDataIds:
    """A struct used to report relationships between data IDs by
    `Registry.relateDataIds`.

    If an instance of this class is returned (instead of `None`), the data IDs
    are "not inconsistent" - any keys they have in common have the same value,
    and any spatial or temporal relationships they have at least might involve
    an overlap.  To capture this, any instance of `ConsistentDataIds` coerces
    to `True` in boolean contexts.
    """

    overlaps: bool
    """If `True`, the data IDs have at least one key in common, associated with
    the same value.

    Note that data IDs are not inconsistent even if overlaps is `False` - they
    may simply have no keys in common, which means they cannot have
    inconsistent values for any keys.  They may even be equal, in the case that
    both data IDs are empty.

    This field does _not_ indicate whether a spatial or temporal overlap
    relationship exists.
    """

    contains: bool
    """If `True`, all keys in the first data ID are in the second, and are
    associated with the same values.

    This includes case where the first data ID is empty.
    """

    within: bool
    """If `True`, all keys in the second data ID are in the first, and are
    associated with the same values.

    This includes case where the second data ID is empty.
    """

    @property
    def equal(self) -> bool:
        """If `True`, the two data IDs are the same.

        Data IDs are equal if they have both a `contains` and a `within`
        relationship.
        """
        return self.contains and self.within

    @property
    def disjoint(self) -> bool:
        """If `True`, the two data IDs have no keys in common.

        This is simply the oppose of `overlaps`.  Disjoint datasets are by
        definition not inconsistent.
        """
        return not self.overlaps

    def __bool__(self) -> bool:
        return True


class DimensionRecordCache:
    """A helper class that caches the `DimensionRecord` objects fetched by
    `expandDataId` and `relateDataIds` operations.

    Instances of this class should only be obtained by user code via calls to
    `Registry.cachedDimensions`.

    Parameters
    ----------
    dimensions : `DimensionRecordStorageManager`
        Helper object that manages dimensions for `Registry`.
    """
    def __init__(self, dimensions: DimensionRecordStorageManager):
        self._dimensions = dimensions
        self._cache: MutableMapping[str, Dict[DataCoordinate, DimensionRecord]] = defaultdict(dict)

    def relateDataIds(self, a: DataId, b: DataId) -> Optional[ConsistentDataIds]:
        """Compare the keys and values of a pair of data IDs for consistency.

        See `ConsistentDataIds` for more information.

        Parameters
        ----------
        a : `dict` or `DataCoordinate`
            First data ID to be compared.
        b : `dict` or `DataCoordinate`
            Second data ID to be compared.

        Returns
        -------
        relationship : `ConsistentDataIds` or `None`
            Relationship information.  This is not `None` and coerces to
            `True` in boolean contexts if and only if the data IDs are
            consistent in terms of all common key-value pairs, all many-to-many
            join tables, and all spatial andtemporal relationships.
        """
        a = DataCoordinate.standardize(a, universe=self._dimensions.universe)
        b = DataCoordinate.standardize(b, universe=self._dimensions.universe)
        aFull = getattr(a, "full", None)
        bFull = getattr(b, "full", None)
        aBest = aFull if aFull is not None else a
        bBest = bFull if bFull is not None else b
        jointKeys = aBest.keys() & bBest.keys()
        # If any common values are not equal, we know they are inconsistent.
        if any(aBest[k] != bBest[k] for k in jointKeys):
            return None
        # If the graphs are equal, we know the data IDs are.
        if a.graph == b.graph:
            return ConsistentDataIds(contains=True, within=True, overlaps=bool(jointKeys))
        # Result is still inconclusive.  Try to expand a data ID containing
        # keys from both; that will fail if they are inconsistent.
        # First, if either input was already an ExpandedDataCoordinate, extract
        # its records so we don't have to query for them.
        records = {}
        if hasattr(a, "records"):
            records.update(a.records)
        if hasattr(b, "records"):
            records.update(b.records)
        try:
            self.expandDataId({**a, **b}, graph=(a.graph | b.graph), records=records)
        except InconsistentDataIdError:
            return None
        # We know the answer is not `None`; time to figure out what it is.
        return ConsistentDataIds(
            contains=(a.graph >= b.graph),
            within=(a.graph <= b.graph),
            overlaps=bool(a.graph & b.graph),
        )

    def expandDataId(self, dataId: Optional[DataId] = None, *, graph: Optional[DimensionGraph] = None,
                     records: Optional[NamedKeyDict[DimensionElement, DimensionRecord]] = None,
                     **kwargs: Any):
        """Expand a dimension-based data ID to include additional information.

        Parameters
        ----------
        dataId : `DataCoordinate` or `dict`, optional
            Data ID to be expanded; augmented and overridden by ``kwds``.
        graph : `DimensionGraph`, optional
            Set of dimensions for the expanded ID.  If `None`, the dimensions
            will be inferred from the keys of ``dataId`` and ``kwds``.
            Dimensions that are in ``dataId`` or ``kwds`` but not in ``graph``
            are silently ignored, providing a way to extract and expand a
            subset of a data ID.
        records : mapping [`DimensionElement`, `DimensionRecord`], optional
            Dimension record data to use before querying the database for that
            data.
        **kwds
            Additional keywords are treated like additional key-value pairs for
            ``dataId``, extending and overriding

        Returns
        -------
        expanded : `ExpandedDataCoordinate`
            A data ID that includes full metadata for all of the dimensions it
            identifieds.
        """
        standardized = DataCoordinate.standardize(dataId, graph=graph, universe=self._dimensions.universe,
                                                  **kwargs)
        if isinstance(standardized, ExpandedDataCoordinate):
            return standardized
        elif isinstance(dataId, ExpandedDataCoordinate):
            records = NamedKeyDict(records) if records is not None else NamedKeyDict()
            records.update(dataId.records)
        else:
            records = NamedKeyDict(records) if records is not None else NamedKeyDict()
        keys = dict(standardized)
        regions: List[lsst.sphgeom.Region] = []
        timespans: List[Timespan] = []
        for element in standardized.graph.primaryKeyTraversalOrder:
            # Try to find the record for this element.
            # Start by pulling out the nested dictionary and assembling the key
            # for this record in the cache; we'll need them regardless.
            cacheForElement = self._cache.setdefault(element.name, {})
            dataIdForElement = DataCoordinate.standardize(keys, graph=element.graph)
            # First see if already in the records he user gave us or that we
            # extracted from an existing ExpandedDataCoordinate.
            record = records.get(element.name, ...)  # Use ... to mean not found; None might mean NULL
            if record is ...:
                # Not there; next see if it's in the cache.
                record = cacheForElement.get(dataIdForElement, ...)
                if record is ...:
                    # No choice but to delegate to storage to fetch it, which
                    # is _probably_ a database query.
                    storage = self._dimensions[element]
                    record = storage.fetch(keys)
                    # Update cache for next time.
                    cacheForElement[dataIdForElement] = record
                # Update the records dictionary that we're building.
                records[element] = record
            else:
                # We did find this in the given records dictionary.  Make sure
                # it's in the cache.
                cacheForElement[dataIdForElement] = record

            if record is not None:
                # If we did get a record, check that its keys are consistent
                # with any others we already have.
                for d in element.implied:
                    value = getattr(record, d.name)
                    if keys.setdefault(d, value) != value:
                        raise InconsistentDataIdError(f"Data ID {standardized} has {d.name}={keys[d]!r}, "
                                                      f"but {element.name} implies {d.name}={value!r}.")
                if element in standardized.graph.spatial and record.region is not None:
                    if any(record.region.relate(r) & lsst.sphgeom.DISJOINT for r in regions):
                        raise InconsistentDataIdError(f"Data ID {standardized}'s region for {element.name} "
                                                      f"is disjoint with those for other elements.")
                    regions.append(record.region)
                if element in standardized.graph.temporal:
                    if any(not record.timespan.overlaps(t) for t in timespans):
                        raise InconsistentDataIdError(f"Data ID {standardized}'s timespan for {element.name}"
                                                      f" is disjoint with those for other elements.")
                    timespans.append(record.timespan)
            else:
                # If we didn't get a record, check that that's okay.
                if element in standardized.graph.required:
                    raise LookupError(
                        f"Could not fetch record for required dimension {element.name} via keys {keys}."
                    )
                if element.alwaysJoin:
                    raise InconsistentDataIdError(
                        f"Could not fetch record for element {element.name} via keys {keys}, ",
                        "but it is marked alwaysJoin=True; this means one or more dimensions are not "
                        "related."
                    )
                records.update((d, None) for d in element.implied)
        # Pass all found records to ExpandedDataCoordinate, which will
        # select only the ones it actually needs.
        return ExpandedDataCoordinate(standardized.graph, standardized.values(), records=records)

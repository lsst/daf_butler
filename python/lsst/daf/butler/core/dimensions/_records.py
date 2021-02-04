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

__all__ = ("DimensionRecord",)

from typing import (
    Any,
    ClassVar,
    Dict,
    Optional,
    TYPE_CHECKING,
    Type,
)

import lsst.sphgeom

from .._topology import SpatialRegionDatabaseRepresentation
from ..timespan import Timespan, TimespanDatabaseRepresentation
from ..utils import immutable
from ._elements import Dimension, DimensionElement
from ..json import from_json_generic, to_json_generic

if TYPE_CHECKING:  # Imports needed only for type annotations; may be circular.
    from ._coordinate import DataCoordinate
    from ._schema import DimensionElementFields
    from ._graph import DimensionUniverse
    from ...registry import Registry


def _reconstructDimensionRecord(definition: DimensionElement, mapping: Dict[str, Any]) -> DimensionRecord:
    """Unpickle implementation for `DimensionRecord` subclasses.

    For internal use by `DimensionRecord`.
    """
    return definition.RecordClass(**mapping)


def _subclassDimensionRecord(definition: DimensionElement) -> Type[DimensionRecord]:
    """Create a dynamic subclass of `DimensionRecord` for the given
    `DimensionElement`.

    For internal use by `DimensionRecord`.
    """
    from ._schema import DimensionElementFields
    fields = DimensionElementFields(definition)
    slots = list(fields.standard.names)
    if definition.spatial:
        slots.append(SpatialRegionDatabaseRepresentation.NAME)
    if definition.temporal:
        slots.append(TimespanDatabaseRepresentation.NAME)
    d = {
        "definition": definition,
        "__slots__": tuple(slots),
        "fields": fields
    }
    return type(definition.name + ".RecordClass", (DimensionRecord,), d)


@immutable
class DimensionRecord:
    """Base class for the Python representation of database records for
    a `DimensionElement`.

    Parameters
    ----------
    **kwargs
        Field values for this record.  Unrecognized keys are ignored.  If this
        is the record for a `Dimension`, its primary key value may be provided
        with the actual name of the field (e.g. "id" or "name"), the name of
        the `Dimension`, or both.  If this record class has a "timespan"
        attribute, "datetime_begin" and "datetime_end" keyword arguments may
        be provided instead of a single "timespan" keyword argument (but are
        ignored if a "timespan" argument is provided).

    Notes
    -----
    `DimensionRecord` subclasses are created dynamically for each
    `DimensionElement` in a `DimensionUniverse`, and are accessible via the
    `DimensionElement.RecordClass` attribute.  The `DimensionRecord` base class
    itself is pure abstract, but does not use the `abc` module to indicate this
    because it does not have overridable methods.

    Record classes have attributes that correspond exactly to the
    `~DimensionElementFields.standard` fields in the related database table,
    plus "region" and "timespan" attributes for spatial and/or temporal
    elements (respectively).

    Instances are usually obtained from a `Registry`, but can be constructed
    directly from Python as well.

    `DimensionRecord` instances are immutable.
    """

    # Derived classes are required to define __slots__ as well, and it's those
    # derived-class slots that other methods on the base class expect to see
    # when they access self.__slots__.
    __slots__ = ("dataId",)

    def __init__(self, **kwargs: Any):
        # Accept either the dimension name or the actual name of its primary
        # key field; ensure both are present in the dict for convenience below.
        if isinstance(self.definition, Dimension):
            v = kwargs.get(self.definition.primaryKey.name)
            if v is None:
                v = kwargs.get(self.definition.name)
                if v is None:
                    raise ValueError(
                        f"No value provided for {self.definition.name}.{self.definition.primaryKey.name}."
                    )
                kwargs[self.definition.primaryKey.name] = v
            else:
                v2 = kwargs.setdefault(self.definition.name, v)
                if v != v2:
                    raise ValueError(
                        f"Multiple inconsistent values for "
                        f"{self.definition.name}.{self.definition.primaryKey.name}: {v!r} != {v2!r}."
                    )
        for name in self.__slots__:
            object.__setattr__(self, name, kwargs.get(name))
        if self.definition.temporal is not None:
            if self.timespan is None:
                object.__setattr__(
                    self,
                    "timespan",
                    Timespan(
                        kwargs.get("datetime_begin"),
                        kwargs.get("datetime_end"),
                    )
                )

        from ._coordinate import DataCoordinate
        object.__setattr__(
            self,
            "dataId",
            DataCoordinate.fromRequiredValues(
                self.definition.graph,
                tuple(kwargs[dimension] for dimension in self.definition.required.names)
            )
        )

    def __eq__(self, other: Any) -> bool:
        if type(other) != type(self):
            return False
        return self.dataId == other.dataId

    def __hash__(self) -> int:
        return hash(self.dataId)

    def __str__(self) -> str:
        lines = [f"{self.definition.name}:"]
        lines.extend(f"  {name}: {getattr(self, name)!r}" for name in self.__slots__)
        return "\n".join(lines)

    def __repr__(self) -> str:
        return "{}.RecordClass({})".format(
            self.definition.name,
            ", ".join(f"name={getattr(self, name)!r}" for name in self.__slots__)
        )

    def __reduce__(self) -> tuple:
        mapping = {name: getattr(self, name) for name in self.__slots__}
        return (_reconstructDimensionRecord, (self.definition, mapping))

    def to_simple(self, minimal: bool = False) -> Dict[str, Any]:
        """Convert this class to a simple python type suitable for
        serialization.

        Parameters
        ----------
        minimal : `bool`, optional
            Use minimal serialization. Has no effect on for this class.

        Returns
        -------
        names : `list`
            The names of the dimensions.
        """
        if minimal:
            # The DataId is sufficient if you are willing to do a deferred
            # query. This may not be overly useful since to reconstruct
            # a collection of records will require repeated registry queries.
            simple = self.dataId.to_simple()
            # Need some means of indicating this is not a full record
            simple["element"] = self.definition.name
            return simple

        mapping = {name: getattr(self, name) for name in self.__slots__}
        # If the item in mapping supports simplification update it
        for k, v in mapping.items():
            try:
                mapping[k] = v.to_simple(minimal=minimal)
            except AttributeError:
                if isinstance(v, lsst.sphgeom.Region):
                    # YAML serialization specifies the class when it
                    # doesn't have to. This is partly for explicitness
                    # and also history. Here use a different approach.
                    # This code needs to be migrated to sphgeom
                    mapping[k] = {"encoded_region": v.encode().hex()}
        definition = self.definition.to_simple(minimal=minimal)

        return {"definition": definition,
                "record": mapping}

    @classmethod
    def from_simple(cls, simple: Dict[str, Any],
                    universe: Optional[DimensionUniverse] = None,
                    registry: Optional[Registry] = None) -> DimensionRecord:
        """Construct a new object from the data returned from the `to_simple`
        method.

        Parameters
        ----------
        simple : `dict` of `str`
            Value return from `to_simple`.
        universe : `DimensionUniverse`
            The special graph of all known dimensions of which this graph will
            be a subset. Can be `None` if `Registry` is provided.
        registry : `lsst.daf.butler.Registry`, optional
            Registry from which a universe can be extracted. Can be `None`
            if universe is provided explicitly.

        Returns
        -------
        graph : `DimensionGraph`
            Newly-constructed object.
        """
        # Minimal representation requires a registry
        if "element" in simple:
            if registry is None:
                raise ValueError("Registry is required to decode minimalist form of dimensions record")
            element = simple.pop("element")
            records = list(registry.queryDimensionRecords(element, dataId=simple))
            if (n := len(records)) != 1:
                raise RuntimeError(f"Unexpectedly got {n} records for element {element} dataId {simple}")
            return records[0]

        if universe is None and registry is None:
            raise ValueError("One of universe or registry is required to convert names to a DimensionGraph")
        if universe is None and registry is not None:
            universe = registry.dimensions
        if universe is None:
            # this is for mypy
            raise ValueError("Unable to determine a usable universe")

        definition = DimensionElement.from_simple(simple["definition"], universe=universe)

        # Timespan and region have to be converted to native form
        # for now assume that those keys are special
        rec = simple["record"]
        if (ts := "timespan") in rec:
            rec[ts] = Timespan.from_simple(rec[ts], universe=universe, registry=registry)
        if (reg := "region") in rec:
            encoded = bytes.fromhex(rec[reg]["encoded_region"])
            rec[reg] = lsst.sphgeom.Region.decode(encoded)

        return _reconstructDimensionRecord(definition, simple["record"])

    to_json = to_json_generic
    from_json = classmethod(from_json_generic)

    def toDict(self, splitTimespan: bool = False) -> Dict[str, Any]:
        """Return a vanilla `dict` representation of this record.

        Parameters
        ----------
        splitTimespan : `bool`, optional
            If `True` (`False` is default) transform any "timespan" key value
            from a `Timespan` instance into a pair of regular
            ("datetime_begin", "datetime_end") fields.
        """
        results = {name: getattr(self, name) for name in self.__slots__}
        if splitTimespan:
            timespan = results.pop("timespan", None)
            if timespan is not None:
                results["datetime_begin"] = timespan.begin
                results["datetime_end"] = timespan.end
        return results

    # DimensionRecord subclasses are dynamically created, so static type
    # checkers can't know about them or their attributes.  To avoid having to
    # put "type: ignore", everywhere, add a dummy __getattr__ that tells type
    # checkers not to worry about missing attributes.
    def __getattr__(self, name: str) -> Any:
        raise AttributeError(name)

    # Class attributes below are shadowed by instance attributes, and are
    # present just to hold the docstrings for those instance attributes.

    dataId: DataCoordinate
    """A dict-like identifier for this record's primary keys
    (`DataCoordinate`).
    """

    definition: ClassVar[DimensionElement]
    """The `DimensionElement` whose records this class represents
    (`DimensionElement`).
    """

    fields: ClassVar[DimensionElementFields]
    """A categorized view of the fields in this class
    (`DimensionElementFields`).
    """

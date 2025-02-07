# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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

__all__ = ("DimensionRecord", "SerializedDimensionRecord")

from collections.abc import Hashable
from typing import TYPE_CHECKING, Any, ClassVar

from pydantic import BaseModel, Field, StrictBool, StrictFloat, StrictInt, StrictStr, create_model

import lsst.sphgeom
from lsst.utils.classes import immutable

from .._timespan import Timespan
from ..json import from_json_pydantic, to_json_pydantic
from ..persistence_context import PersistenceContextVars
from ._elements import Dimension, DimensionElement

if TYPE_CHECKING:  # Imports needed only for type annotations; may be circular.
    from ..registry import Registry
    from ._coordinate import DataCoordinate
    from ._schema import DimensionElementFields
    from ._universe import DimensionUniverse


def _reconstructDimensionRecord(definition: DimensionElement, mapping: dict[str, Any]) -> DimensionRecord:
    """Unpickle implementation for `DimensionRecord` subclasses.

    For internal use by `DimensionRecord`.
    """
    return definition.RecordClass(**mapping)


def _subclassDimensionRecord(definition: DimensionElement) -> type[DimensionRecord]:
    """Create a dynamic subclass of `DimensionRecord` for the given element.

    For internal use by `DimensionRecord`.
    """
    from ._schema import DimensionElementFields

    fields = DimensionElementFields(definition)
    slots = list(fields.standard.names)
    if definition.spatial:
        slots.append("region")
    if definition.temporal:
        slots.append("timespan")
    d = {"definition": definition, "__slots__": tuple(slots), "fields": fields}
    return type(definition.name + ".RecordClass", (DimensionRecord,), d)


class SpecificSerializedDimensionRecord(BaseModel, extra="forbid"):
    """Base model for a specific serialized record content."""


_SIMPLE_RECORD_CLASS_CACHE: dict[
    tuple[DimensionElement, DimensionUniverse], type[SpecificSerializedDimensionRecord]
] = {}


def _createSimpleRecordSubclass(definition: DimensionElement) -> type[SpecificSerializedDimensionRecord]:
    from ._schema import DimensionElementFields

    # Cache on the definition (which hashes as the name) and the
    # associated universe.
    cache_key = (definition, definition.universe)
    if cache_key in _SIMPLE_RECORD_CLASS_CACHE:
        return _SIMPLE_RECORD_CLASS_CACHE[cache_key]

    fields = DimensionElementFields(definition)
    members = {}
    # Prefer strict typing for external data
    type_map = {
        str: StrictStr,
        float: StrictFloat,
        bool: StrictBool,
        int: StrictInt,
    }

    for field in fields.standard:
        field_type = field.getPythonType()
        field_type = type_map.get(field_type, field_type)
        if field.nullable:
            field_type = field_type | None  # type: ignore
        members[field.name] = (field_type, ...)
    if definition.temporal:
        members["timespan"] = (Timespan | None, ...)  # type: ignore
    if definition.spatial:
        members["region"] = (str | None, ...)  # type: ignore

    # For the new derived class name need to convert to camel case.
    # so "day_obs" -> "DayObs".
    derived_name = "".join([part.capitalize() for part in definition.name.split("_")])

    model = create_model(
        f"SpecificSerializedDimensionRecord{derived_name}",
        __base__=SpecificSerializedDimensionRecord,
        **members,  # type: ignore
    )

    _SIMPLE_RECORD_CLASS_CACHE[cache_key] = model
    return model


# While supporting pydantic v1 and v2 keep this outside the model.
_serialized_dimension_record_schema_extra = {
    "examples": [
        {
            "definition": "detector",
            "record": {
                "instrument": "HSC",
                "id": 72,
                "full_name": "0_01",
                "name_in_raft": "01",
                "raft": "0",
                "purpose": "SCIENCE",
            },
        }
    ]
}


class SerializedDimensionRecord(BaseModel):
    """Simplified model for serializing a `DimensionRecord`."""

    definition: str = Field(
        ...,
        title="Name of dimension associated with this record.",
        examples=["exposure"],
    )

    # Use strict types to prevent casting
    record: dict[str, None | StrictBool | StrictInt | StrictFloat | StrictStr | Timespan] = Field(
        ...,
        title="Dimension record keys and values.",
        examples=[
            {
                "definition": "exposure",
                "record": {
                    "instrument": "LATISS",
                    "exposure": 2021050300044,
                    "obs_id": "AT_O_20210503_00044",
                },
            }
        ],
    )

    model_config = {
        "json_schema_extra": _serialized_dimension_record_schema_extra,  # type: ignore[typeddict-item]
    }

    @classmethod
    def direct(
        cls,
        *,
        definition: str,
        record: dict[str, Any],
    ) -> SerializedDimensionRecord:
        """Construct a `SerializedDimensionRecord` directly without validators.

        Parameters
        ----------
        definition : `str`
            The name of the record.
        record : `dict`
            A dictionary representation of the record content.

        Returns
        -------
        rec : `SerializedDimensionRecord`
            A model representing the dimension records.

        Notes
        -----
        This differs from the pydantic "construct" method in that the arguments
        are explicitly what the model requires, and it will recurse through
        members, constructing them from their corresponding `direct` methods.

        This method should only be called when the inputs are trusted.
        """
        # This method requires tuples as values of the mapping, but JSON
        # readers will read things in as lists. Be kind and transparently
        # transform to tuples.
        _recItems = {
            k: (v if type(v) is not list else Timespan(begin=None, end=None, _nsec=tuple(v)))  # type: ignore
            for k, v in record.items()
        }

        # Type ignore because the ternary statement seems to confuse mypy
        # based on conflicting inferred types of v.
        key = (
            definition,
            frozenset(_recItems.items()),
        )
        cache = PersistenceContextVars.serializedDimensionRecordMapping.get()
        if cache is not None and (result := cache.get(key)) is not None:
            return result

        node = cls.model_construct(definition=definition, record=_recItems)  # type: ignore

        if cache is not None:
            cache[key] = node
        return node


@immutable
class DimensionRecord:
    """Base class for the Python representation of database records.

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

    _serializedType: ClassVar[type[BaseModel]] = SerializedDimensionRecord

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
                        "Multiple inconsistent values for "
                        f"{self.definition.name}.{self.definition.primaryKey.name}: {v!r} != {v2!r}."
                    )

        from ._coordinate import DataCoordinate

        object.__setattr__(
            self,
            "dataId",
            DataCoordinate.from_required_values(
                self.definition.minimal_group,
                tuple(kwargs[dimension] for dimension in self.definition.required.names),
            ),
        )
        # Don't need the primary key value aliased to the dimension name
        # anymore.
        kwargs.pop(self.definition.name, None)

        for name in self.__slots__:
            # Note that we remove from kwargs as we go, to make sure there's
            # nothing left at the end.
            object.__setattr__(self, name, kwargs.pop(name, None))
        # Support 'datetime_begin' and 'datetime_end' instead of 'timespan' for
        # backwards compatibility, but if one is provided both must be.
        if self.definition.temporal is not None and self.timespan is None and "datetime_begin" in kwargs:
            object.__setattr__(
                self,
                "timespan",
                Timespan(
                    kwargs.pop("datetime_begin"),
                    kwargs.pop("datetime_end"),
                ),
            )

        if kwargs:
            raise TypeError(f"Invalid fields for {self.definition} dimension record: {set(kwargs.keys())}.")

    def __eq__(self, other: Any) -> bool:
        if type(other) is not type(self):
            return False
        return self.dataId == other.dataId

    def __hash__(self) -> int:
        return hash(self.dataId.required_values)

    def __str__(self) -> str:
        lines = [f"{self.definition.name}:"]
        lines.extend(f"  {name}: {getattr(self, name)!r}" for name in self.__slots__)
        return "\n".join(lines)

    def __repr__(self) -> str:
        return "{}.RecordClass({})".format(
            self.definition.name, ", ".join(f"{name}={getattr(self, name)!r}" for name in self.__slots__)
        )

    def __reduce__(self) -> tuple:
        mapping = {name: getattr(self, name) for name in self.__slots__}
        return (_reconstructDimensionRecord, (self.definition, mapping))

    def _repr_html_(self) -> str:
        """Override the default representation in IPython/Jupyter notebooks.

        This gives a more readable output that understands embedded newlines.
        """
        return f"<pre>{self}<pre>"

    def to_simple(self, minimal: bool = False) -> SerializedDimensionRecord:
        """Convert this class to a simple python type.

        This makes it suitable for serialization.

        Parameters
        ----------
        minimal : `bool`, optional
            Use minimal serialization. Has no effect on for this class.

        Returns
        -------
        names : `list`
            The names of the dimensions.
        """
        # The DataId is sufficient if you are willing to do a deferred
        # query. This may not be overly useful since to reconstruct
        # a collection of records will require repeated registry queries.
        # For now do not implement minimal form.
        key = (id(self.definition), self.dataId)
        cache = PersistenceContextVars.serializedDimensionRecordMapping.get()
        if cache is not None and (result := cache.get(key)) is not None:
            return result

        mapping = {name: getattr(self, name) for name in self.__slots__}
        for k, v in mapping.items():
            if isinstance(v, lsst.sphgeom.Region):
                # YAML serialization specifies the class when it
                # doesn't have to. This is partly for explicitness
                # and also history. Here use a different approach.
                # This code needs to be migrated to sphgeom
                mapping[k] = v.encode().hex()
            if isinstance(v, bytes):
                # We actually can't handle serializing out to bytes for
                # hash objects, encode it here to a hex string
                mapping[k] = v.hex()
        definition = self.definition.to_simple(minimal=minimal)
        dimRec = SerializedDimensionRecord(definition=definition, record=mapping)
        if cache is not None:
            cache[key] = dimRec
        return dimRec

    @classmethod
    def from_simple(
        cls,
        simple: SerializedDimensionRecord,
        universe: DimensionUniverse | None = None,
        registry: Registry | None = None,
        cacheKey: Hashable | None = None,
    ) -> DimensionRecord:
        """Construct a new object from the simplified form.

        This is generally data returned from the `to_simple`
        method.

        Parameters
        ----------
        simple : `SerializedDimensionRecord`
            Value return from `to_simple`.
        universe : `DimensionUniverse`
            The special graph of all known dimensions of which this graph will
            be a subset. Can be `None` if `Registry` is provided.
        registry : `lsst.daf.butler.Registry`, optional
            Registry from which a universe can be extracted. Can be `None`
            if universe is provided explicitly.
        cacheKey : `Hashable` or `None`
            If this is not None, it will be used as a key for any cached
            reconstruction instead of calculating a value from the serialized
            format.

        Returns
        -------
        record : `DimensionRecord`
            Newly-constructed object.
        """
        if universe is None and registry is None:
            raise ValueError("One of universe or registry is required to convert names to a DimensionGroup")
        if universe is None and registry is not None:
            universe = registry.dimensions
        if universe is None:
            # this is for mypy
            raise ValueError("Unable to determine a usable universe")
        # Type ignore because the ternary statement seems to confuse mypy
        # based on conflicting inferred types of v.
        key = cacheKey or (
            simple.definition,
            frozenset(simple.record.items()),  # type: ignore
        )
        cache = PersistenceContextVars.dimensionRecords.get()
        if cache is not None and (result := cache.get(key)) is not None:
            return result

        definition = DimensionElement.from_simple(simple.definition, universe=universe)

        # Create a specialist subclass model with type validation.
        # This allows us to do simple checks of external data (possibly
        # sent as JSON) since for now _reconstructDimensionRecord does not
        # do any validation.
        record_model_cls = _createSimpleRecordSubclass(definition)
        record_model = record_model_cls(**simple.record)

        # Region and hash have to be converted to native form; for now assume
        # that the keys are special.  We make the mapping we need to pass to
        # the DimensionRecord constructor via getattr, because we don't
        # model_dump re-disassembling things like Timespans that we've already
        # assembled.
        mapping = {k: getattr(record_model, k) for k in definition.schema.names}

        if mapping.get("region") is not None:
            mapping["region"] = lsst.sphgeom.Region.decode(bytes.fromhex(mapping["region"]))
        if "hash" in mapping:
            mapping["hash"] = bytes.fromhex(mapping["hash"].decode())

        dimRec = _reconstructDimensionRecord(definition, mapping)
        if cache is not None:
            cache[key] = dimRec
        return dimRec

    to_json = to_json_pydantic
    from_json: ClassVar = classmethod(from_json_pydantic)

    def toDict(self, splitTimespan: bool = False) -> dict[str, Any]:
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

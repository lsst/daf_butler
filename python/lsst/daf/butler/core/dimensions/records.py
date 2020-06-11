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
    Mapping,
    TYPE_CHECKING,
    Type,
)

from ..timespan import Timespan
from .coordinate import DataCoordinate
from .elements import Dimension

if TYPE_CHECKING:  # Imports needed only for type annotations; may be circular.
    import astropy.time
    from .elements import DimensionElement


def _reconstructDimensionRecord(definition: DimensionElement, *args: Any) -> DimensionRecord:
    """Unpickle implementation for `DimensionRecord` subclasses.

    For internal use by `DimensionRecord`.
    """
    return definition.RecordClass(*args)


def _makeTimespanFromRecord(record: DimensionRecord) -> Timespan[astropy.time.Time]:
    """Extract a `Timespan` object from the appropriate endpoint attributes.

    For internal use by `DimensionRecord`.
    """
    from ..timespan import TIMESPAN_FIELD_SPECS
    return Timespan(
        begin=getattr(record, TIMESPAN_FIELD_SPECS.begin.name),
        end=getattr(record, TIMESPAN_FIELD_SPECS.end.name),
    )


def _subclassDimensionRecord(definition: DimensionElement) -> Type[DimensionRecord]:
    """Create a dynamic subclass of `DimensionRecord` for the given
    `DimensionElement`.

    For internal use by `DimensionRecord`.
    """
    from .schema import makeDimensionElementTableSpec
    d = {
        "definition": definition,
        "__slots__": tuple(makeDimensionElementTableSpec(definition).fields.names)
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
    the list of fields.

    Instances are usually obtained from a `Registry`, but in the rare cases
    where they are constructed directly in Python (usually for insertion into
    a `Registry`), the `fromDict` method should generally be used.

    `DimensionRecord` instances are immutable.
    """

    # Derived classes are required to define __slots__ as well, and it's those
    # derived-class slots that other methods on the base class expect to see
    # when they access self.__slots__.
    __slots__ = ("dataId",)

    def __init__(self, *args: Any):
        for attrName, value in zip(self.__slots__, args):
            object.__setattr__(self, attrName, value)
        if self.definition.required.names == self.definition.graph.required.names:
            dataId = DataCoordinate(
                self.definition.graph,
                args[:len(self.definition.required.names)]
            )
        else:
            assert not isinstance(self.definition, Dimension)
            dataId = DataCoordinate(
                self.definition.graph,
                tuple(getattr(self, name) for name in self.definition.required.names)
            )
        object.__setattr__(self, "dataId", dataId)

    @classmethod
    def fromDict(cls, mapping: Mapping[str, Any]) -> DimensionRecord:
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
        d: Mapping[str, Any]
        if primaryKey is not None and isinstance(cls.definition, Dimension):
            d = dict(mapping)
            d[cls.definition.primaryKey.name] = primaryKey
        else:
            d = mapping
        values = tuple(d.get(k) for k in cls.__slots__)
        return cls(*values)

    def __reduce__(self) -> tuple:
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

    definition: ClassVar[DimensionElement]

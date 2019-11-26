from __future__ import annotations

__all__ = ["CollectionType", "Collection", "Run"]

import enum
from typing import Optional

from ..core.dimensions.schema import TIMESPAN_FIELD_SPECS
from ..core.timespan import Timespan


class CollectionType(enum.IntEnum):
    RUN = 1
    TAGGED = 2
    CALIBRATION = 3


class Collection:

    __slots__ = ("_name", "_type", "id", "origin")

    def __init__(self, name: str, *, id: Optional[int] = None, origin: Optional[int] = None,
                 type: CollectionType = CollectionType.TAGGED):
        if type is CollectionType.RUN and not isinstance(self, Run):
            raise TypeError("Use the Run class for RUN collections.")
        self._name = name
        self._type = type
        self.id = id
        self.origin = origin

    @property
    def name(self) -> str:
        return self._name

    @property
    def type(self) -> CollectionType:
        return self._type

    id: Optional[int]
    origin: Optional[int]


class Run(Collection):

    __slots__ = (TIMESPAN_FIELD_SPECS.begin.name, TIMESPAN_FIELD_SPECS.end.name, "host")

    # TODO: make chainable

    def __init__(self, name: str, *, id: Optional[int] = None, origin: Optional[int] = None,
                 host: Optional[str] = None, **kwds):
        super().__init__(name=name, id=id, origin=origin, type=CollectionType.RUN)
        self.host = host
        for spec in TIMESPAN_FIELD_SPECS:
            value = kwds.pop(spec.name)
            if value is not None:
                setattr(self, spec.name, value)
        assert not kwds

    host: Optional[str]

    @property
    def timespan(self) -> Timespan:
        return Timespan(
            getattr(self, TIMESPAN_FIELD_SPECS.begin.name),
            getattr(self, TIMESPAN_FIELD_SPECS.end.name),
        )

    @timespan.setter
    def timespan(self, value: Timespan):
        if value.begin is not None:
            setattr(self, TIMESPAN_FIELD_SPECS.begin.name, value.begin)
        if value.end is not None:
            setattr(self, TIMESPAN_FIELD_SPECS.end.name, value.end)

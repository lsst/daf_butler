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

__all__ = ("to_json_generic", "from_json_generic", "to_json_pydantic", "from_json_pydantic")

import json
from typing import TYPE_CHECKING, Any, Optional, Protocol, Type

if TYPE_CHECKING:
    from ..registry import Registry
    from .dimensions import DimensionUniverse


class SupportsSimple(Protocol):
    _serializedType: Type

    def to_simple(self, minimal: bool) -> Any:
        ...

    @classmethod
    def from_simple(
        cls, simple: Any, universe: Optional[DimensionUniverse] = None, registry: Optional[Registry] = None
    ) -> SupportsSimple:
        ...


def to_json_pydantic(self: SupportsSimple, minimal: bool = False) -> str:
    """Convert this class to JSON assuming that the ``to_simple()`` returns
    a pydantic model.

    """
    return self.to_simple(minimal=minimal).json(exclude_defaults=True, exclude_unset=True)


def from_json_pydantic(
    cls: Type[SupportsSimple],
    json_str: str,
    universe: Optional[DimensionUniverse] = None,
    registry: Optional[Registry] = None,
) -> SupportsSimple:
    """Convert from JSON to a pydantic model."""
    simple = cls._serializedType.parse_raw(json_str)
    try:
        return cls.from_simple(simple, universe=universe, registry=registry)
    except AttributeError as e:
        raise AttributeError(f"JSON deserialization requires {cls} has a from_simple() class method") from e


def to_json_generic(self: SupportsSimple, minimal: bool = False) -> str:
    """Convert this class to JSON form.

    The class type is not recorded in the JSON so the JSON decoder
    must know which class is represented.

    Parameters
    ----------
    minimal : `bool`, optional
        Use minimal serialization. Requires Registry to convert
        back to a full type.

    Returns
    -------
    json : `str`
        The class in JSON string format.
    """
    # For now use the core json library to convert a dict to JSON
    # for us.
    return json.dumps(self.to_simple(minimal=minimal))


def from_json_generic(
    cls: Type[SupportsSimple],
    json_str: str,
    universe: Optional[DimensionUniverse] = None,
    registry: Optional[Registry] = None,
) -> SupportsSimple:
    """Return new class from JSON string.

    Converts a JSON string created by `to_json` and return
    something of the supplied class.

    Parameters
    ----------
    json_str : `str`
        Representation of the dimensions in JSON format as created
        by `to_json()`.
    universe : `DimensionUniverse`, optional
        The special graph of all known dimensions. Passed directly
        to `from_simple()`.
    registry : `lsst.daf.butler.Registry`, optional
        Registry to use to convert simple name of a DatasetType to
        a full `DatasetType`. Passed directly to `from_simple()`.

    Returns
    -------
    constructed : Any
        Newly-constructed object.
    """
    simple = json.loads(json_str)
    try:
        return cls.from_simple(simple, universe=universe, registry=registry)
    except AttributeError as e:
        raise AttributeError(f"JSON deserialization requires {cls} has a from_simple() class method") from e

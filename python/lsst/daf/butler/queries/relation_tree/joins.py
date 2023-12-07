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

__all__ = ("JoinTuple", "JoinArg", "JoinHelper")

from typing import Annotated, Any, ClassVar, Literal, TypeAlias, TypeVar, Union

import pydantic

from ...dimensions import DimensionGroup
from ._base import InvalidRelationError

_T = TypeVar("_T")


def _reorder_join_tuple(original: tuple[str, str]) -> tuple[str, str]:
    if original[0] < original[1]:
        return original
    if original[0] > original[1]:
        return original[::-1]
    raise ValueError("Join tuples may not represent self-joins.")


JoinTuple: TypeAlias = Annotated[tuple[str, str], pydantic.AfterValidator(_reorder_join_tuple)]


JoinArg: TypeAlias = Union[Literal["auto"], JoinTuple, frozenset[JoinTuple]]


class JoinHelper:
    """Helper class for validating and automatic spatial and temporal joins."""

    @classmethod
    def standardize(
        cls,
        arg: JoinArg,
        a_dimensions: DimensionGroup,
        b_dimensions: DimensionGroup,
        kind: Literal["spatial", "temporal"],
    ) -> frozenset[JoinTuple]:
        match arg:
            case "auto":
                a_families = getattr(a_dimensions, kind) - getattr(b_dimensions, kind)
                b_families = getattr(b_dimensions, kind) - getattr(a_dimensions, kind)
                if not a_families or not b_families:
                    # If either side doesn't have a dimension participant,
                    # don't create any automatic join.  This means
                    # validity-range joins with dimensions on one side and a
                    # CALIBRATION collection on the other are never automatic.
                    return frozenset()
                try:
                    (a_family,) = a_families
                except ValueError:
                    raise InvalidRelationError(
                        f"Cannot automate spatial join between {a_dimensions} and {b_dimensions}: "
                        f"could use any of {{}}."
                    )
                try:
                    (b_family,) = b_families
                except ValueError:
                    raise InvalidRelationError(
                        f"Cannot automate spatial join between {a_dimensions} and {b_dimensions}: "
                        f"could use any of {{}}.",
                    )
                a = a_family.choose(a_dimensions.elements, a_dimensions.universe).name
                b = b_family.choose(b_dimensions.elements, b_dimensions.universe).name
                return frozenset({_reorder_join_tuple((a, b))})
            case (str(a), str(b)):
                return frozenset({_reorder_join_tuple((a, b))})
        if cls._set_arg_type_adapter is None:
            cls._set_arg_type_adapter = pydantic.TypeAdapter(frozenset[JoinTuple])
        return cls._set_arg_type_adapter.validate_python(arg)

    _set_arg_type_adapter: ClassVar[pydantic.TypeAdapter[Any] | None] = None

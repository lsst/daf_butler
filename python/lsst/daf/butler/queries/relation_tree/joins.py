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

__all__ = ("JoinTuple", "JoinArg", "compute_joins")

from collections.abc import Iterable
from typing import TYPE_CHECKING, Annotated, Literal, TypeAlias, Union

import pydantic

from ._base import InvalidRelationError

if TYPE_CHECKING:
    from ._relation import Relation


def _reorder_join_tuple(original: tuple[str, str]) -> tuple[str, str]:
    if original[0] < original[1]:
        return original
    if original[0] > original[1]:
        return original[::-1]
    raise ValueError("Join tuples may not represent self-joins.")


JoinTuple: TypeAlias = Annotated[tuple[str, str], pydantic.AfterValidator(_reorder_join_tuple)]


JoinArg: TypeAlias = Union[Literal["auto"], tuple[str, str], Iterable[tuple[str, str]]]


def compute_joins(
    arg: JoinArg,
    a_operand: Relation,
    b_operand: Relation,
    kind: Literal["spatial", "temporal"],
) -> frozenset[JoinTuple]:
    """Compute the spatial or temporal joins between a pair of relations."""
    result: set[JoinTuple] = set()
    result.update(getattr(a_operand, f"{kind}_joins", frozenset()))
    result.update(getattr(b_operand, f"{kind}_joins", frozenset()))
    match arg:
        case "auto":
            a_families = getattr(a_operand.dimensions, kind) - getattr(b_operand.dimensions, kind)
            b_families = getattr(b_operand.dimensions, kind) - getattr(a_operand.dimensions, kind)
            if not a_families or not b_families:
                # If either side doesn't have a dimension participant,
                # don't create any automatic join.  This means
                # validity-range joins with dimensions on one side and a
                # CALIBRATION collection on the other are never automatic.
                return frozenset(result)
            try:
                (a_family,) = a_families
            except ValueError:
                raise InvalidRelationError(
                    f"Cannot automate spatial join between {a_operand.dimensions} and "
                    f"{b_operand.dimensions}: could use any of {{}}."
                )
            try:
                (b_family,) = b_families
            except ValueError:
                raise InvalidRelationError(
                    f"Cannot automate spatial join between {a_operand.dimensions} and "
                    f"{b_operand.dimensions}: could use any of {{}}.",
                )
            a = a_family.choose(a_operand.dimensions.elements, a_operand.dimensions.universe).name
            b = b_family.choose(b_operand.dimensions.elements, b_operand.dimensions.universe).name
            result.add(_reorder_join_tuple((a, b)))
        # MyPy is not smart enough to figure out how the following match arms
        # narrow the type.
        case (str(), str()) as pair:
            result.add(_reorder_join_tuple(pair))  # type: ignore[arg-type]
        case iterable:
            for pair in iterable:  # type: ignore[assignment]
                match pair:
                    case (str(), str()):
                        result.add(_reorder_join_tuple(pair))  # type: ignore[arg-type]
                    case _:
                        raise ValueError(f"Invalid argument {arg} for {kind} join.")
    return frozenset(result)

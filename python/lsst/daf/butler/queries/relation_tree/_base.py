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

__all__ = (
    "RelationBase",
    "ColumnExpressionBase",
    "PredicateBase",
    "StringOrWildcard",
    "DatasetFieldName",
    "InvalidRelationError",
)

from types import EllipsisType
from typing import TYPE_CHECKING, Annotated, Literal, TypeAlias

import pydantic

if TYPE_CHECKING:
    from ._column_reference import ColumnReference
    from ._predicate import Predicate


StringOrWildcard = Annotated[
    str | EllipsisType,
    pydantic.PlainSerializer(lambda x: "..." if x is ... else x, return_type=str),
    pydantic.BeforeValidator(lambda x: ... if x == "..." else x),
    pydantic.GetPydanticSchema(lambda _s, h: h(str)),
]


DatasetFieldName: TypeAlias = Literal[
    "dataset_id", "dataset_type", "ingest_date", "run", "collection", "rank"
]


class InvalidRelationError(ValueError):
    """Exception raised when a query's relation tree would be invalid."""

    pass


class RelationTreeBase(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(frozen=True, extra="forbid", strict=True)


class RelationBase(RelationTreeBase):
    pass


class ColumnExpressionBase(RelationTreeBase):
    def gather_required_columns(self) -> set[ColumnReference]:
        return set()


class PredicateBase(RelationTreeBase):
    def gather_required_columns(self) -> set[ColumnReference]:
        return set()

    def _flatten_and(self) -> tuple[Predicate, ...]:
        return (self,)  # type: ignore[return-value]

    def _flatten_or(self) -> tuple[Predicate, ...]:
        return (self,)  # type: ignore[return-value]

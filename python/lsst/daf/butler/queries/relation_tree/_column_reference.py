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

__all__ = ("ColumnReference", "DimensionKeyReference", "DimensionFieldReference", "DatasetFieldReference")

from typing import Annotated, Literal, TypeAlias, Union

import pydantic

from ._base import ColumnExpressionBase, DatasetFieldName, StringOrWildcard


class DimensionKeyReference(ColumnExpressionBase):
    """A column expression that references a dimension primary key column."""

    expression_type: Literal["dimension_key"] = "dimension_key"
    dimension: str

    def gather_required_columns(self) -> set[ColumnReference]:
        return {self}

    @property
    def precedence(self) -> int:
        return 0

    def __str__(self) -> str:
        return self.dimension


class DimensionFieldReference(ColumnExpressionBase):
    """A column expression that references a dimension record column that is
    not a primary ket.
    """

    expression_type: Literal["dimension_field"] = "dimension_field"
    element: str
    field: str

    def gather_required_columns(self) -> set[ColumnReference]:
        return {self}

    @property
    def precedence(self) -> int:
        return 0

    def __str__(self) -> str:
        return f"{self.element}.{self.field}"


class DatasetFieldReference(ColumnExpressionBase):
    """A column expression that references a column associated with a dataset
    type.
    """

    expression_type: Literal["dataset_field"] = "dataset_field"
    dataset_type: StringOrWildcard
    field: DatasetFieldName

    def gather_required_columns(self) -> set[ColumnReference]:
        return {self}

    @property
    def precedence(self) -> int:
        return 0

    def __str__(self) -> str:
        if self.dataset_type is ...:
            return self.field
        else:
            return f"{self.dataset_type}.{self.field}"


_ColumnReference: TypeAlias = Union[
    DimensionKeyReference,
    DimensionFieldReference,
    DatasetFieldReference,
]

ColumnReference: TypeAlias = Annotated[_ColumnReference, pydantic.Field(discriminator="expression_type")]

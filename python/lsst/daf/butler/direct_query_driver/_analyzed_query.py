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

__all__ = ("AnalyzedQuery", "AnalyzedDatasetSearch", "DataIdExtractionVisitor")

import dataclasses
from collections.abc import Iterator
from typing import Any

from ..dimensions import DataIdValue, DimensionElement, DimensionGroup, DimensionUniverse
from ..queries import tree as qt
from ..queries.visitors import ColumnExpressionVisitor, PredicateVisitFlags, SimplePredicateVisitor
from ..registry.interfaces import CollectionRecord
from ._postprocessing import Postprocessing


@dataclasses.dataclass
class AnalyzedDatasetSearch:
    name: str
    shrunk: str
    dimensions: DimensionGroup
    collection_records: list[CollectionRecord] = dataclasses.field(default_factory=list)
    messages: list[str] = dataclasses.field(default_factory=list)
    is_calibration_search: bool = False


@dataclasses.dataclass
class AnalyzedQuery:
    predicate: qt.Predicate
    postprocessing: Postprocessing
    base_columns: qt.ColumnSet
    projection_columns: qt.ColumnSet
    final_columns: qt.ColumnSet
    find_first_dataset: str | None
    materializations: dict[qt.MaterializationKey, DimensionGroup] = dataclasses.field(default_factory=dict)
    datasets: dict[str, AnalyzedDatasetSearch] = dataclasses.field(default_factory=dict)
    messages: list[str] = dataclasses.field(default_factory=list)
    constraint_data_id: dict[str, DataIdValue] = dataclasses.field(default_factory=dict)
    data_coordinate_uploads: dict[qt.DataCoordinateUploadKey, DimensionGroup] = dataclasses.field(
        default_factory=dict
    )
    needs_dimension_distinct: bool = False
    needs_find_first_resolution: bool = False
    projection_region_aggregates: list[DimensionElement] = dataclasses.field(default_factory=list)

    @property
    def universe(self) -> DimensionUniverse:
        return self.base_columns.dimensions.universe

    @property
    def needs_projection(self) -> bool:
        return self.needs_dimension_distinct or self.postprocessing.check_validity_match_count

    def iter_mandatory_base_elements(self) -> Iterator[DimensionElement]:
        for element_name in self.base_columns.dimensions.elements:
            element = self.universe[element_name]
            if self.base_columns.dimension_fields[element_name]:
                # We need to get dimension record fields for this element, and
                # its table is the only place to get those.
                yield element
            elif element.defines_relationships:
                # We als need to join in DimensionElements tables that define
                # one-to-many and many-to-many relationships, but data
                # coordinate uploads, materializations, and datasets can also
                # provide these relationships. Data coordinate uploads and
                # dataset tables only have required dimensions, and can hence
                # only provide relationships involving those.
                if any(
                    element.minimal_group.names <= upload_dimensions.required
                    for upload_dimensions in self.data_coordinate_uploads.values()
                ):
                    continue
                if any(
                    element.minimal_group.names <= dataset_spec.dimensions.required
                    for dataset_spec in self.datasets.values()
                ):
                    continue
                # Materializations have all key columns for their dimensions.
                if any(
                    element in materialization_dimensions.names
                    for materialization_dimensions in self.materializations.values()
                ):
                    continue
                yield element


class DataIdExtractionVisitor(
    SimplePredicateVisitor,
    ColumnExpressionVisitor[tuple[str, None] | tuple[None, Any] | tuple[None, None]],
):
    def __init__(self, data_id: dict[str, DataIdValue], messages: list[str]):
        self.data_id = data_id
        self.messages = messages

    def visit_comparison(
        self,
        a: qt.ColumnExpression,
        operator: qt.ComparisonOperator,
        b: qt.ColumnExpression,
        flags: PredicateVisitFlags,
    ) -> None:
        if flags & PredicateVisitFlags.HAS_OR_SIBLINGS:
            return None
        if flags & PredicateVisitFlags.INVERTED:
            if operator == "!=":
                operator = "=="
            else:
                return None
        if operator != "==":
            return None
        k_a, v_a = a.visit(self)
        k_b, v_b = b.visit(self)
        if k_a is not None and v_b is not None:
            key = k_a
            value = v_b
        elif k_b is not None and v_a is not None:
            key = k_b
            value = v_a
        else:
            return None
        if (old := self.data_id.setdefault(key, value)) != value:
            self.messages.append(f"'where' expression requires both {key}={value!r} and {key}={old!r}.")
        return None

    def visit_binary_expression(self, expression: qt.BinaryExpression) -> tuple[None, None]:
        return None, None

    def visit_unary_expression(self, expression: qt.UnaryExpression) -> tuple[None, None]:
        return None, None

    def visit_literal(self, expression: qt.ColumnLiteral) -> tuple[None, Any]:
        return None, expression.get_literal_value()

    def visit_dimension_key_reference(self, expression: qt.DimensionKeyReference) -> tuple[str, None]:
        return expression.dimension.name, None

    def visit_dimension_field_reference(self, expression: qt.DimensionFieldReference) -> tuple[None, None]:
        return None, None

    def visit_dataset_field_reference(self, expression: qt.DatasetFieldReference) -> tuple[None, None]:
        return None, None

    def visit_reversed(self, expression: qt.Reversed) -> tuple[None, None]:
        raise AssertionError("No Reversed expressions in predicates.")

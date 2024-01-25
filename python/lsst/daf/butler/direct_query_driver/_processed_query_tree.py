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

__all__ = ("ProcessedQueryTree",)

import dataclasses
from typing import Any

from ..dimensions import DataIdValue
from ..queries import tree as qt
from ..queries.driver import QueryDriver
from ..queries.visitors import ColumnExpressionVisitor, PredicateVisitFlags, SimplePredicateVisitor
from ..registry.interfaces import CollectionRecord


@dataclasses.dataclass
class ProcessedQueryTree:
    tree: qt.QueryTree
    data_id: dict[str, DataIdValue] = dataclasses.field(default_factory=dict)
    resolved_collections: dict[str, list[CollectionRecord]] = dataclasses.field(default_factory=dict)
    messages: list[str] = dataclasses.field(default_factory=list)

    @classmethod
    def process(cls, tree: qt.QueryTree, driver: QueryDriver) -> ProcessedQueryTree:
        result = cls(tree)
        tree.predicate.visit(_DataIdExtractionVisitor(result.data_id, result.messages))
        where_columns = qt.ColumnSet(driver.universe.empty.as_group())
        tree.predicate.gather_required_columns(where_columns)
        for governor in where_columns.dimensions.governors:
            if governor not in result.data_id:
                raise qt.InvalidQueryTreeError(
                    f"Query 'where' expression references a dimension dependent on {governor} without "
                    "constraining it directly."
                )
        for dataset_type_name, dataset_search in result.tree.datasets.items():
            resolved_dataset_type = driver.get_dataset_type(dataset_type_name)
            # Check dataset type dimensions: this is also done when the dataset
            # is joined into a Query, but we might have deserialized a Query
            # we don't trust on a server.
            if resolved_dataset_type.dimensions.as_group() != dataset_search.dimensions:
                raise qt.InvalidQueryTreeError(
                    f"Dataset type {dataset_type_name!r} has dimensions {dataset_search.dimensions} "
                    f"in query tree, but {resolved_dataset_type.dimensions} in the repository."
                )
            dataset_messages: list[str] = []
            resolved_collections: list[CollectionRecord] = []
            for collection_record, collection_summary in driver.resolve_collection_path(
                dataset_search.collections
            ):
                rejected: bool = False
                if dataset_type_name not in collection_summary.dataset_types.names:
                    dataset_messages.append(
                        f"No datasets of type {dataset_type_name!r} in collection {collection_record.name}."
                    )
                    rejected = True
                for governor in result.data_id.keys() & collection_summary.governors.keys():
                    if result.data_id[governor] not in collection_summary.governors[governor]:
                        dataset_messages.append(
                            f"No datasets with {governor}={result.data_id[governor]!r} in collection "
                            f"{collection_record.name}."
                        )
                        rejected = True
                if not rejected:
                    resolved_collections.append(collection_record)
            result.resolved_collections[dataset_type_name] = resolved_collections
            if not resolved_collections:
                result.messages.append(f"Search for dataset type {dataset_type_name!r} is doomed to fail.")
                result.messages.extend(dataset_messages)
        return result


class _DataIdExtractionVisitor(
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

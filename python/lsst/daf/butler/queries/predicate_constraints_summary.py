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

from typing import Any

from .._exceptions import InvalidQueryError
from ..dimensions import DataCoordinate, DataIdValue, DimensionGroup, DimensionUniverse
from ..queries import tree as qt
from ..queries.visitors import ColumnExpressionVisitor, PredicateVisitFlags, SimplePredicateVisitor


class PredicateConstraintsSummary:
    """Summarizes information about the constraints on data ID values implied
    by a Predicate.

    Parameters
    ----------
    predicate : `Predicate`
        Predicate to summarize.
    """

    predicate: qt.Predicate
    """The predicate examined by this summary."""

    constraint_data_id: dict[str, DataIdValue]
    """Data ID values that will be identical in all result rows due to query
    constraints.
    """

    messages: list[str]
    """Diagnostic messages that report reasons the query may not return any
    rows.
    """

    def __init__(self, predicate: qt.Predicate) -> None:
        self.predicate = predicate
        self.constraint_data_id = {}
        self.messages = []
        # Governor dimensions referenced directly in the predicate, but not
        # necessarily constrained to the same value in all logic branches.
        self._governors_referenced: set[str] = set()

        self.predicate.visit(
            _DataIdExtractionVisitor(self.constraint_data_id, self.messages, self._governors_referenced)
        )

    def apply_default_data_id(
        self, default_data_id: DataCoordinate, query_dimensions: DimensionGroup
    ) -> None:
        """Augment the predicate and summary by adding missing constraints for
        governor dimensions using a default data ID.

        Parameters
        ----------
        default_data_id : `DataCoordinate`
            Data ID values that will be used to constrain the query if governor
            dimensions have not already been constrained by the predicate.

        query_dimensions : `DimensionGroup`
            The set of dimensions returned in result rows from the query.
        """
        # Find governor dimensions required by the predicate.
        # If these are not constrained by the predicate or the default data ID,
        # we will raise an exception.
        where_governors: set[str] = set()
        self.predicate.gather_governors(where_governors)

        # Add in governor dimensions that are returned in result rows.
        # We constrain these using a default data ID if one is available,
        # but it's not an error to omit the constraint.
        governors_used_by_query = where_governors | query_dimensions.governors

        # For each governor dimension needed by the query, add a constraint
        # from the default data ID if the existing predicate does not
        # constrain it.
        for governor in governors_used_by_query:
            if governor not in self.constraint_data_id and governor not in self._governors_referenced:
                if governor in default_data_id.dimensions:
                    data_id_value = default_data_id[governor]
                    self.constraint_data_id[governor] = data_id_value
                    self._governors_referenced.add(governor)
                    self.predicate = self.predicate.logical_and(
                        _create_data_id_predicate(governor, data_id_value, query_dimensions.universe)
                    )
                elif governor in where_governors:
                    # Check that the predicate doesn't reference any dimensions
                    # without constraining their governor dimensions, since
                    # that's a particularly easy mistake to make and it's
                    # almost never intentional.
                    raise InvalidQueryError(
                        f"Query 'where' expression references a dimension dependent on {governor} without "
                        "constraining it directly."
                    )


def _create_data_id_predicate(
    dimension_name: str, value: DataIdValue, universe: DimensionUniverse
) -> qt.Predicate:
    """Create a Predicate that tests whether the given dimension primary key is
    equal to the given literal value.
    """
    dimension = universe.dimensions[dimension_name]
    return qt.Predicate.compare(
        qt.DimensionKeyReference(dimension=dimension), "==", qt.make_column_literal(value)
    )


class _DataIdExtractionVisitor(
    SimplePredicateVisitor,
    ColumnExpressionVisitor[tuple[str, None] | tuple[None, Any] | tuple[None, None]],
):
    """A column-expression visitor that extracts quality constraints on
    dimensions that are not OR'd with anything else.

    Parameters
    ----------
    data_id : `dict`
        Dictionary to populate in place.
    messages : `list` [ `str` ]
        List of diagnostic messages to populate in place.
    governor_references : `set` [ `str` ]
        Set of the names of governor dimension names that were referenced
        directly.  This includes dimensions that were constrained to different
        values in different logic branches, and hence not included in
        ``data_id``.
    """

    def __init__(self, data_id: dict[str, DataIdValue], messages: list[str], governor_references: set[str]):
        self.data_id = data_id
        self.messages = messages
        self.governor_references = governor_references

    def visit_comparison(
        self,
        a: qt.ColumnExpression,
        operator: qt.ComparisonOperator,
        b: qt.ColumnExpression,
        flags: PredicateVisitFlags,
    ) -> None:
        k_a, v_a = a.visit(self)
        k_b, v_b = b.visit(self)
        if flags & PredicateVisitFlags.HAS_OR_SIBLINGS:
            return None
        if flags & PredicateVisitFlags.INVERTED:
            if operator == "!=":
                operator = "=="
            else:
                return None
        if operator != "==":
            return None
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
        expression.a.visit(self)
        expression.b.visit(self)
        return None, None

    def visit_unary_expression(self, expression: qt.UnaryExpression) -> tuple[None, None]:
        expression.operand.visit(self)
        return None, None

    def visit_literal(self, expression: qt.ColumnLiteral) -> tuple[None, Any]:
        return None, expression.get_literal_value()

    def visit_dimension_key_reference(self, expression: qt.DimensionKeyReference) -> tuple[str, None]:
        if expression.dimension.governor is expression.dimension:
            self.governor_references.add(expression.dimension.name)
        return expression.dimension.name, None

    def visit_dimension_field_reference(self, expression: qt.DimensionFieldReference) -> tuple[None, None]:
        if (
            expression.element.governor is expression.element
            and expression.field in expression.element.alternate_keys.names
        ):
            self.governor_references.add(expression.element.name)
        return None, None

    def visit_dataset_field_reference(self, expression: qt.DatasetFieldReference) -> tuple[None, None]:
        return None, None

    def visit_reversed(self, expression: qt.Reversed) -> tuple[None, None]:
        raise AssertionError("No Reversed expressions in predicates.")

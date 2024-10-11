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

__all__ = ("SqlColumnVisitor",)

import warnings
from typing import TYPE_CHECKING, Any

import erfa
import sqlalchemy

from .. import ddl
from .._exceptions import InvalidQueryError
from ..queries import tree as qt
from ..queries.visitors import ColumnExpressionVisitor, PredicateVisitFlags, PredicateVisitor
from ..timespan_database_representation import TimespanDatabaseRepresentation

if TYPE_CHECKING:
    from ._driver import DirectQueryDriver
    from ._sql_builders import SqlColumns


class SqlColumnVisitor(
    ColumnExpressionVisitor[sqlalchemy.ColumnElement[Any] | TimespanDatabaseRepresentation],
    PredicateVisitor[
        sqlalchemy.ColumnElement[bool], sqlalchemy.ColumnElement[bool], sqlalchemy.ColumnElement[bool]
    ],
):
    """A column expression visitor that constructs a `sqlalchemy.ColumnElement`
    expression tree.

    Parameters
    ----------
    columns : `QueryColumns`
        `QueryColumns` that provides SQL columns for column-reference
        expressions.
    driver : `QueryDriver`
        Driver used to construct nested queries for "in query" predicates.
    """

    def __init__(self, columns: SqlColumns, driver: DirectQueryDriver):
        self._driver = driver
        self._columns = columns

    def visit_literal(
        self, expression: qt.ColumnLiteral
    ) -> sqlalchemy.ColumnElement[Any] | TimespanDatabaseRepresentation:
        # Docstring inherited.
        if expression.column_type == "timespan":
            return self._driver.db.getTimespanRepresentation().fromLiteral(expression.get_literal_value())
        return sqlalchemy.literal(
            expression.get_literal_value(), type_=ddl.VALID_CONFIG_COLUMN_TYPES[expression.column_type]
        )

    def visit_dimension_key_reference(
        self, expression: qt.DimensionKeyReference
    ) -> sqlalchemy.ColumnElement[int | str]:
        # Docstring inherited.
        return self._columns.dimension_keys[expression.dimension.name][0]

    def visit_dimension_field_reference(
        self, expression: qt.DimensionFieldReference
    ) -> sqlalchemy.ColumnElement[Any] | TimespanDatabaseRepresentation:
        # Docstring inherited.
        if expression.column_type == "timespan":
            return self._columns.timespans[expression.element.name]
        return self._columns.fields[expression.element.name][expression.field]

    def visit_dataset_field_reference(
        self, expression: qt.DatasetFieldReference
    ) -> sqlalchemy.ColumnElement[Any] | TimespanDatabaseRepresentation:
        # Docstring inherited.
        if expression.column_type == "timespan":
            return self._columns.timespans[expression.dataset_type]
        return self._columns.fields[expression.dataset_type][expression.field]

    def visit_unary_expression(self, expression: qt.UnaryExpression) -> sqlalchemy.ColumnElement[Any]:
        # Docstring inherited.
        match expression.operator:
            case "-":
                return -self.expect_scalar(expression.operand)
            case "begin_of":
                return self.expect_timespan(expression.operand).lower()
            case "end_of":
                return self.expect_timespan(expression.operand).upper()
        raise AssertionError(f"Invalid unary expression operator {expression.operator!r}.")

    def visit_binary_expression(self, expression: qt.BinaryExpression) -> sqlalchemy.ColumnElement[Any]:
        # Docstring inherited.
        a = self.expect_scalar(expression.a)
        b = self.expect_scalar(expression.b)
        match expression.operator:
            case "+":
                return a + b
            case "-":
                return a - b
            case "*":
                return a * b
            case "/":
                return a / b
            case "%":
                return a % b
        raise AssertionError(f"Invalid binary expression operator {expression.operator!r}.")

    def visit_reversed(self, expression: qt.Reversed) -> sqlalchemy.ColumnElement[Any]:
        # Docstring inherited.
        return self.expect_scalar(expression.operand).desc()

    def visit_boolean_wrapper(
        self, value: qt.ColumnExpression, flags: PredicateVisitFlags
    ) -> sqlalchemy.ColumnElement[bool]:
        return self.expect_scalar(value)

    def visit_comparison(
        self,
        a: qt.ColumnExpression,
        operator: qt.ComparisonOperator,
        b: qt.ColumnExpression,
        flags: PredicateVisitFlags,
    ) -> sqlalchemy.ColumnElement[bool]:
        # Docstring inherited.
        if operator == "overlaps":
            if values := qt.is_one_timespan_and_one_datetime(a, b):
                return self.expect_timespan(values.timespan).contains(self.expect_scalar(values.datetime))
            elif a.column_type == "timespan" and b.column_type == "timespan":
                return self.expect_timespan(a).overlaps(self.expect_timespan(b))
            else:
                # Spatial overlaps should be transformed away by now.
                raise AssertionError(
                    f"Unexpected types {a.column_type},{b.column_type} in overlaps operator."
                )

        lhs = self.expect_scalar(a)
        rhs = self.expect_scalar(b)
        # Special case to handle awkward situation where ingest_date is not
        # always the same type as other datetime columns.
        if qt.is_one_datetime_and_one_ingest_date(a, b):
            if a.column_type == "datetime":
                lhs = self._convert_datetime_to_ingest_date(a)
            elif b.column_type == "datetime":
                rhs = self._convert_datetime_to_ingest_date(b)

        match operator:
            case "==":
                return lhs == rhs
            case "!=":
                return lhs != rhs
            case "<":
                return lhs < rhs
            case ">":
                return lhs > rhs
            case "<=":
                return lhs <= rhs
            case ">=":
                return lhs >= rhs
        raise AssertionError(f"Invalid comparison operator {operator!r}.")

    def visit_is_null(
        self, operand: qt.ColumnExpression, flags: PredicateVisitFlags
    ) -> sqlalchemy.ColumnElement[bool]:
        # Docstring inherited.
        if operand.column_type == "timespan":
            return self.expect_timespan(operand).isNull()
        return self.expect_scalar(operand) == sqlalchemy.null()

    def visit_in_container(
        self,
        member: qt.ColumnExpression,
        container: tuple[qt.ColumnExpression, ...],
        flags: PredicateVisitFlags,
    ) -> sqlalchemy.ColumnElement[bool]:
        # Docstring inherited.
        return self.expect_scalar(member).in_([self.expect_scalar(item) for item in container])

    def visit_in_range(
        self, member: qt.ColumnExpression, start: int, stop: int | None, step: int, flags: PredicateVisitFlags
    ) -> sqlalchemy.ColumnElement[bool]:
        # Docstring inherited.
        sql_member = self.expect_scalar(member)
        if stop is None:
            target = sql_member >= sqlalchemy.literal(start)
        else:
            stop_inclusive = stop - 1
            if start == stop_inclusive:
                return sql_member == sqlalchemy.literal(start)
            else:
                target = sqlalchemy.sql.between(
                    sql_member,
                    sqlalchemy.literal(start),
                    sqlalchemy.literal(stop_inclusive),
                )
        if step != 1:
            return sqlalchemy.sql.and_(
                *[
                    target,
                    sql_member % sqlalchemy.literal(step) == sqlalchemy.literal(start % step),
                ]
            )
        else:
            return target

    def visit_in_query_tree(
        self,
        member: qt.ColumnExpression,
        column: qt.ColumnExpression,
        query_tree: qt.QueryTree,
        flags: PredicateVisitFlags,
    ) -> sqlalchemy.ColumnElement[bool]:
        # Docstring inherited.
        columns = qt.ColumnSet(self._driver.universe.empty)
        column.gather_required_columns(columns)
        builder = self._driver.build_query(query_tree, columns)
        if builder.postprocessing:
            raise NotImplementedError(
                "Right-hand side subquery in IN expression would require postprocessing."
            )
        select_builder = builder.finish_nested()
        subquery_visitor = SqlColumnVisitor(select_builder.joins, self._driver)
        select_builder.joins.special["_MEMBER"] = subquery_visitor.expect_scalar(column)
        select_builder.columns = qt.ColumnSet(self._driver.universe.empty)
        subquery_select = select_builder.select(postprocessing=None)
        sql_member = self.expect_scalar(member)
        return sql_member.in_(subquery_select)

    def apply_logical_and(
        self, originals: qt.PredicateOperands, results: tuple[sqlalchemy.ColumnElement[bool], ...]
    ) -> sqlalchemy.ColumnElement[bool]:
        # Docstring inherited.
        match len(results):
            case 0:
                return sqlalchemy.true()
            case 1:
                return results[0]
            case _:
                return sqlalchemy.and_(*results)

    def apply_logical_or(
        self,
        originals: tuple[qt.PredicateLeaf, ...],
        results: tuple[sqlalchemy.ColumnElement[bool], ...],
        flags: PredicateVisitFlags,
    ) -> sqlalchemy.ColumnElement[bool]:
        # Docstring inherited.
        match len(results):
            case 0:
                return sqlalchemy.false()
            case 1:
                return results[0]
            case _:
                return sqlalchemy.or_(*results)

    def apply_logical_not(
        self, original: qt.PredicateLeaf, result: sqlalchemy.ColumnElement[bool], flags: PredicateVisitFlags
    ) -> sqlalchemy.ColumnElement[bool]:
        # Docstring inherited.
        return sqlalchemy.not_(result)

    def expect_scalar(self, expression: qt.OrderExpression) -> sqlalchemy.ColumnElement[Any]:
        result = expression.visit(self)
        assert isinstance(result, sqlalchemy.ColumnElement)
        return result

    def _convert_datetime_to_ingest_date(self, expression: qt.ColumnExpression) -> sqlalchemy.ColumnElement:
        assert expression.column_type == "datetime"
        if self._driver.managers.datasets.ingest_date_dtype() == sqlalchemy.TIMESTAMP:
            # Datasets manager v1 schema has "datasets" table's "ingest_date"
            # column as TIMESTAMP, but the rest of the database schema and
            # query system uses integer TAI nanoseconds.  So we have to convert
            # the nanoseconds value to a timestamp.
            # Note that this loses precision.
            if expression.expression_type != "datetime":
                # Conversion between TAI and UTC can't be done in the database,
                # so we are only able to handle literal values here.
                raise InvalidQueryError("Only literal date-time values can be compared with ingest date.")

            # The conversion from TAI to UTC can trigger a warning for dates
            # in the future so catch those warnings.
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=erfa.ErfaWarning)
                dt = expression.value.utc.to_datetime()
            return sqlalchemy.literal(dt)
        else:
            # For v2 schema, ingest_date uses TAI nanoseconds like everything
            # else, so no conversion is required.
            return self.expect_scalar(expression)

    def expect_timespan(self, expression: qt.ColumnExpression) -> TimespanDatabaseRepresentation:
        result = expression.visit(self)
        assert isinstance(result, TimespanDatabaseRepresentation)
        return result

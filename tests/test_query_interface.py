# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""Tests for the public Butler._query interface and the Pydantic models that
back it using a mock column-expression visitor and a mock QueryDriver
implementation.

These tests are entirely independent of which kind of butler or database
backend we're using.

This is a very large test file because a lot of tests make use of those mocks,
but they're not so generally useful that I think they're worth putting in the
library proper.
"""

from __future__ import annotations

import dataclasses
import itertools
import unittest
import uuid
from collections.abc import Iterable, Iterator, Mapping, Set
from typing import Any

import astropy.time
from lsst.daf.butler import (
    CollectionType,
    DataCoordinate,
    DataIdValue,
    DatasetRef,
    DatasetType,
    DimensionGroup,
    DimensionRecord,
    DimensionRecordSet,
    DimensionUniverse,
    InvalidQueryError,
    MissingDatasetTypeError,
    NamedValueSet,
    NoDefaultCollectionError,
    Timespan,
)
from lsst.daf.butler.queries import (
    DataCoordinateQueryResults,
    DatasetRefQueryResults,
    DimensionRecordQueryResults,
    Query,
)
from lsst.daf.butler.queries import driver as qd
from lsst.daf.butler.queries import result_specs as qrs
from lsst.daf.butler.queries import tree as qt
from lsst.daf.butler.queries.expression_factory import ExpressionFactory
from lsst.daf.butler.queries.tree._column_expression import UnaryExpression
from lsst.daf.butler.queries.tree._predicate import PredicateLeaf, PredicateOperands
from lsst.daf.butler.queries.visitors import ColumnExpressionVisitor, PredicateVisitFlags, PredicateVisitor
from lsst.daf.butler.registry import CollectionSummary, DatasetTypeError
from lsst.daf.butler.registry.interfaces import ChainedCollectionRecord, CollectionRecord, RunRecord
from lsst.sphgeom import DISJOINT, Mq3cPixelization


class _TestVisitor(PredicateVisitor[bool, bool, bool], ColumnExpressionVisitor[Any]):
    """Test visitor for column expressions.

    This visitor evaluates column expressions using regular Python logic.

    Parameters
    ----------
    dimension_keys : `~collections.abc.Mapping`, optional
        Mapping from dimension name to the value it should be assigned by the
        visitor.
    dimension_fields : `~collections.abc.Mapping`, optional
        Mapping from ``(dimension element name, field)`` tuple to the value it
        should be assigned by the visitor.
    dataset_fields : `~collections.abc.Mapping`, optional
        Mapping from ``(dataset type name, field)`` tuple to the value it
        should be assigned by the visitor.
    query_tree_items : `~collections.abc.Set`, optional
        Set that should be used as the right-hand side of element-in-query
        predicates.
    """

    def __init__(
        self,
        dimension_keys: Mapping[str, Any] | None = None,
        dimension_fields: Mapping[tuple[str, str], Any] | None = None,
        dataset_fields: Mapping[tuple[str, str], Any] | None = None,
        query_tree_items: Set[Any] = frozenset(),
    ):
        self.dimension_keys = dimension_keys or {}
        self.dimension_fields = dimension_fields or {}
        self.dataset_fields = dataset_fields or {}
        self.query_tree_items = query_tree_items

    def visit_binary_expression(self, expression: qt.BinaryExpression) -> Any:
        match expression.operator:
            case "+":
                return expression.a.visit(self) + expression.b.visit(self)
            case "-":
                return expression.a.visit(self) - expression.b.visit(self)
            case "*":
                return expression.a.visit(self) * expression.b.visit(self)
            case "/":
                match expression.column_type:
                    case "int":
                        return expression.a.visit(self) // expression.b.visit(self)
                    case "float":
                        return expression.a.visit(self) / expression.b.visit(self)
            case "%":
                return expression.a.visit(self) % expression.b.visit(self)

    def visit_comparison(
        self,
        a: qt.ColumnExpression,
        operator: qt.ComparisonOperator,
        b: qt.ColumnExpression,
        flags: PredicateVisitFlags,
    ) -> bool:
        match operator:
            case "==":
                return a.visit(self) == b.visit(self)
            case "!=":
                return a.visit(self) != b.visit(self)
            case "<":
                return a.visit(self) < b.visit(self)
            case ">":
                return a.visit(self) > b.visit(self)
            case "<=":
                return a.visit(self) <= b.visit(self)
            case ">=":
                return a.visit(self) >= b.visit(self)
            case "overlaps":
                return not (a.visit(self).relate(b.visit(self)) & DISJOINT)

    def visit_dataset_field_reference(self, expression: qt.DatasetFieldReference) -> Any:
        return self.dataset_fields[expression.dataset_type, expression.field]

    def visit_dimension_field_reference(self, expression: qt.DimensionFieldReference) -> Any:
        return self.dimension_fields[expression.element.name, expression.field]

    def visit_dimension_key_reference(self, expression: qt.DimensionKeyReference) -> Any:
        return self.dimension_keys[expression.dimension.name]

    def visit_in_container(
        self,
        member: qt.ColumnExpression,
        container: tuple[qt.ColumnExpression, ...],
        flags: PredicateVisitFlags,
    ) -> bool:
        return member.visit(self) in [item.visit(self) for item in container]

    def visit_in_range(
        self, member: qt.ColumnExpression, start: int, stop: int | None, step: int, flags: PredicateVisitFlags
    ) -> bool:
        return member.visit(self) in range(start, stop, step)

    def visit_in_query_tree(
        self,
        member: qt.ColumnExpression,
        column: qt.ColumnExpression,
        query_tree: qt.QueryTree,
        flags: PredicateVisitFlags,
    ) -> bool:
        return member.visit(self) in self.query_tree_items

    def visit_is_null(self, operand: qt.ColumnExpression, flags: PredicateVisitFlags) -> bool:
        return operand.visit(self) is None

    def visit_literal(self, expression: qt.ColumnLiteral) -> Any:
        return expression.get_literal_value()

    def visit_reversed(self, expression: qt.Reversed) -> Any:
        return _TestReversed(expression.operand.visit(self))

    def visit_unary_expression(self, expression: UnaryExpression) -> Any:
        match expression.operator:
            case "-":
                return -expression.operand.visit(self)
            case "begin_of":
                return expression.operand.visit(self).begin
            case "end_of":
                return expression.operand.visit(self).end

    def apply_logical_and(self, originals: PredicateOperands, results: tuple[bool, ...]) -> bool:
        return all(results)

    def apply_logical_not(self, original: PredicateLeaf, result: bool, flags: PredicateVisitFlags) -> bool:
        return not result

    def apply_logical_or(
        self, originals: tuple[PredicateLeaf, ...], results: tuple[bool, ...], flags: PredicateVisitFlags
    ) -> bool:
        return any(results)


@dataclasses.dataclass
class _TestReversed:
    """Struct used by _TestVisitor" to mark an expression as reversed in sort
    order.
    """

    operand: Any


class _TestQueryExecution(BaseException):
    """Exception raised by _TestQueryDriver.execute to communicate its args
    back to the caller.
    """

    def __init__(self, result_spec: qrs.ResultSpec, tree: qt.QueryTree, driver: _TestQueryDriver) -> None:
        self.result_spec = result_spec
        self.tree = tree
        self.driver = driver


class _TestQueryCount(BaseException):
    """Exception raised by _TestQueryDriver.count to communicate its args
    back to the caller.
    """

    def __init__(
        self,
        result_spec: qrs.ResultSpec,
        tree: qt.QueryTree,
        driver: _TestQueryDriver,
        exact: bool,
        discard: bool,
    ) -> None:
        self.result_spec = result_spec
        self.tree = tree
        self.driver = driver
        self.exact = exact
        self.discard = discard


class _TestQueryAny(BaseException):
    """Exception raised by _TestQueryDriver.any to communicate its args
    back to the caller.
    """

    def __init__(
        self,
        tree: qt.QueryTree,
        driver: _TestQueryDriver,
        exact: bool,
        execute: bool,
    ) -> None:
        self.tree = tree
        self.driver = driver
        self.exact = exact
        self.execute = execute


class _TestQueryExplainNoResults(BaseException):
    """Exception raised by _TestQueryDriver.explain_no_results to communicate
    its args back to the caller.
    """

    def __init__(
        self,
        tree: qt.QueryTree,
        driver: _TestQueryDriver,
        execute: bool,
    ) -> None:
        self.tree = tree
        self.driver = driver
        self.execute = execute


class _TestQueryDriver(qd.QueryDriver):
    """Mock implementation of `QueryDriver` that mostly raises exceptions that
    communicate the arguments its methods were called with.

    Parameters
    ----------
    default_collections : `tuple` [ `str`, ... ], optional
        Default collection the query or parent butler is imagined to have been
        constructed with.
    collection_info : `~collections.abc.Mapping`, optional
        Mapping from collection name to its record and summary, simulating the
        collections present in the data repository.
    dataset_types : `~collections.abc.Mapping`, optional
        Mapping from dataset type to its definition, simulating the dataset
        types registered in the data repository.
    result_rows : `tuple` [ `~collections.abc.Iterable`, ... ], optional
        A tuple of iterables of arbitrary type to use as result rows any time
        `execute` is called, with each nested iterable considered a separate
        page.  The result type is not checked for consistency with the result
        spec.  If this is not provided, `execute` will instead raise
        `_TestQueryExecution`, and `fetch_page` will not do anything useful.
    """

    def __init__(
        self,
        default_collections: tuple[str, ...] | None = None,
        collection_info: Mapping[str, tuple[CollectionRecord, CollectionSummary]] | None = None,
        dataset_types: Mapping[str, DatasetType] | None = None,
        result_rows: tuple[Iterable[Any], ...] | None = None,
    ) -> None:
        self._universe = DimensionUniverse()
        # Mapping of the arguments passed to materialize, keyed by the UUID
        # that that each call returned.
        self.materializations: dict[
            qd.MaterializationKey, tuple[qt.QueryTree, DimensionGroup, frozenset[str]]
        ] = {}
        # Mapping of the arguments passed to upload_data_coordinates, keyed by
        # the UUID that that each call returned.
        self.data_coordinate_uploads: dict[
            qd.DataCoordinateUploadKey, tuple[DimensionGroup, list[tuple[DataIdValue, ...]]]
        ] = {}
        self._default_collections = default_collections
        self._collection_info = collection_info or {}
        self._dataset_types = dataset_types or {}
        self._executions: list[tuple[qrs.ResultSpec, qt.QueryTree]] = []
        self._result_rows = result_rows

    @property
    def universe(self) -> DimensionUniverse:
        return self._universe

    def __enter__(self) -> None:
        pass

    def __exit__(self, *args: Any, **kwargs: Any) -> None:
        pass

    def execute(self, result_spec: qrs.ResultSpec, tree: qt.QueryTree) -> Iterator[qd.ResultPage]:
        if self._result_rows is None:
            raise _TestQueryExecution(result_spec, tree, self)

        for rows in self._result_rows:
            yield self._make_next_page(result_spec, rows)

    def _make_next_page(self, result_spec: qrs.ResultSpec, current_rows: Iterable[Any]) -> qd.ResultPage:
        match result_spec:
            case qrs.DataCoordinateResultSpec():
                return qd.DataCoordinateResultPage(spec=result_spec, rows=current_rows)
            case qrs.DimensionRecordResultSpec():
                return qd.DimensionRecordResultPage(spec=result_spec, rows=current_rows)
            case qrs.DatasetRefResultSpec():
                return qd.DatasetRefResultPage(spec=result_spec, rows=current_rows)
            case _:
                raise NotImplementedError("Other query types not yet supported.")

    def materialize(
        self,
        tree: qt.QueryTree,
        dimensions: DimensionGroup,
        datasets: frozenset[str],
    ) -> qd.MaterializationKey:
        key = uuid.uuid4()
        self.materializations[key] = (tree, dimensions, datasets)
        return key

    def upload_data_coordinates(
        self, dimensions: DimensionGroup, rows: Iterable[tuple[DataIdValue, ...]]
    ) -> qd.DataCoordinateUploadKey:
        key = uuid.uuid4()
        self.data_coordinate_uploads[key] = (dimensions, frozenset(rows))
        return key

    def count(
        self,
        tree: qt.QueryTree,
        result_spec: qrs.ResultSpec,
        *,
        exact: bool,
        discard: bool,
    ) -> int:
        raise _TestQueryCount(result_spec, tree, self, exact, discard)

    def any(self, tree: qt.QueryTree, *, execute: bool, exact: bool) -> bool:
        raise _TestQueryAny(tree, self, exact, execute)

    def explain_no_results(self, tree: qt.QueryTree, execute: bool) -> Iterable[str]:
        raise _TestQueryExplainNoResults(tree, self, execute)

    def get_default_collections(self) -> tuple[str, ...]:
        if self._default_collections is None:
            raise NoDefaultCollectionError()
        return self._default_collections

    def get_dataset_type(self, name: str) -> DatasetType:
        try:
            return self._dataset_types[name]
        except KeyError:
            raise MissingDatasetTypeError(name)


class ColumnExpressionsTestCase(unittest.TestCase):
    """Tests for column expression objects in lsst.daf.butler.queries.tree."""

    def setUp(self) -> None:
        self.universe = DimensionUniverse()
        self.x = ExpressionFactory(self.universe)

    def query(self, **kwargs: Any) -> Query:
        """Make an initial Query object with the given kwargs used to
        initialize the _TestQueryDriver.
        """
        return Query(_TestQueryDriver(**kwargs), qt.make_identity_query_tree(self.universe))

    def test_int_literals(self) -> None:
        expr = self.x.unwrap(self.x.literal(5))
        self.assertEqual(expr.value, 5)
        self.assertEqual(expr.get_literal_value(), 5)
        self.assertEqual(expr.expression_type, "int")
        self.assertEqual(expr.column_type, "int")
        self.assertEqual(str(expr), "5")
        self.assertTrue(expr.is_literal)
        columns = qt.ColumnSet(self.universe.empty)
        expr.gather_required_columns(columns)
        self.assertFalse(columns)
        self.assertEqual(expr.visit(_TestVisitor()), 5)

    def test_string_literals(self) -> None:
        expr = self.x.unwrap(self.x.literal("five"))
        self.assertEqual(expr.value, "five")
        self.assertEqual(expr.get_literal_value(), "five")
        self.assertEqual(expr.expression_type, "string")
        self.assertEqual(expr.column_type, "string")
        self.assertEqual(str(expr), "'five'")
        self.assertTrue(expr.is_literal)
        columns = qt.ColumnSet(self.universe.empty)
        expr.gather_required_columns(columns)
        self.assertFalse(columns)
        self.assertEqual(expr.visit(_TestVisitor()), "five")

    def test_float_literals(self) -> None:
        expr = self.x.unwrap(self.x.literal(0.5))
        self.assertEqual(expr.value, 0.5)
        self.assertEqual(expr.get_literal_value(), 0.5)
        self.assertEqual(expr.expression_type, "float")
        self.assertEqual(expr.column_type, "float")
        self.assertEqual(str(expr), "0.5")
        self.assertTrue(expr.is_literal)
        columns = qt.ColumnSet(self.universe.empty)
        expr.gather_required_columns(columns)
        self.assertFalse(columns)
        self.assertEqual(expr.visit(_TestVisitor()), 0.5)

    def test_hash_literals(self) -> None:
        expr = self.x.unwrap(self.x.literal(b"eleven"))
        self.assertEqual(expr.value, b"eleven")
        self.assertEqual(expr.get_literal_value(), b"eleven")
        self.assertEqual(expr.expression_type, "hash")
        self.assertEqual(expr.column_type, "hash")
        self.assertEqual(str(expr), "(bytes)")
        self.assertTrue(expr.is_literal)
        columns = qt.ColumnSet(self.universe.empty)
        expr.gather_required_columns(columns)
        self.assertFalse(columns)
        self.assertEqual(expr.visit(_TestVisitor()), b"eleven")

    def test_uuid_literals(self) -> None:
        value = uuid.uuid4()
        expr = self.x.unwrap(self.x.literal(value))
        self.assertEqual(expr.value, value)
        self.assertEqual(expr.get_literal_value(), value)
        self.assertEqual(expr.expression_type, "uuid")
        self.assertEqual(expr.column_type, "uuid")
        self.assertEqual(str(expr), str(value))
        self.assertTrue(expr.is_literal)
        columns = qt.ColumnSet(self.universe.empty)
        expr.gather_required_columns(columns)
        self.assertFalse(columns)
        self.assertEqual(expr.visit(_TestVisitor()), value)

    def test_datetime_literals(self) -> None:
        value = astropy.time.Time("2020-01-01T00:00:00", format="isot", scale="tai")
        expr = self.x.unwrap(self.x.literal(value))
        self.assertEqual(expr.value, value)
        self.assertEqual(expr.get_literal_value(), value)
        self.assertEqual(expr.expression_type, "datetime")
        self.assertEqual(expr.column_type, "datetime")
        self.assertEqual(str(expr), "2020-01-01T00:00:00")
        self.assertTrue(expr.is_literal)
        columns = qt.ColumnSet(self.universe.empty)
        expr.gather_required_columns(columns)
        self.assertFalse(columns)
        self.assertEqual(expr.visit(_TestVisitor()), value)

    def test_timespan_literals(self) -> None:
        begin = astropy.time.Time("2020-01-01T00:00:00", format="isot", scale="tai")
        end = astropy.time.Time("2020-01-01T00:01:00", format="isot", scale="tai")
        value = Timespan(begin, end)
        expr = self.x.unwrap(self.x.literal(value))
        self.assertEqual(expr.value, value)
        self.assertEqual(expr.get_literal_value(), value)
        self.assertEqual(expr.expression_type, "timespan")
        self.assertEqual(expr.column_type, "timespan")
        self.assertEqual(str(expr), "[2020-01-01T00:00:00, 2020-01-01T00:01:00)")
        self.assertTrue(expr.is_literal)
        columns = qt.ColumnSet(self.universe.empty)
        expr.gather_required_columns(columns)
        self.assertFalse(columns)
        self.assertEqual(expr.visit(_TestVisitor()), value)

    def test_region_literals(self) -> None:
        pixelization = Mq3cPixelization(10)
        value = pixelization.quad(12058870)
        expr = self.x.unwrap(self.x.literal(value))
        self.assertEqual(expr.value, value)
        self.assertEqual(expr.get_literal_value(), value)
        self.assertEqual(expr.expression_type, "region")
        self.assertEqual(expr.column_type, "region")
        self.assertEqual(str(expr), "(region)")
        self.assertTrue(expr.is_literal)
        columns = qt.ColumnSet(self.universe.empty)
        expr.gather_required_columns(columns)
        self.assertFalse(columns)
        self.assertEqual(expr.visit(_TestVisitor()), value)

    def test_invalid_literal(self) -> None:
        with self.assertRaisesRegex(TypeError, "Invalid type 'complex' of value 5j for column literal."):
            self.x.literal(5j)

    def test_dimension_key_reference(self) -> None:
        expr = self.x.unwrap(self.x.detector)
        self.assertIsNone(expr.get_literal_value())
        self.assertEqual(expr.expression_type, "dimension_key")
        self.assertEqual(expr.column_type, "int")
        self.assertEqual(str(expr), "detector")
        self.assertFalse(expr.is_literal)
        columns = qt.ColumnSet(self.universe.empty)
        expr.gather_required_columns(columns)
        self.assertEqual(columns.dimensions, self.universe.conform(["detector"]))
        self.assertEqual(expr.visit(_TestVisitor(dimension_keys={"detector": 3})), 3)

    def test_dimension_field_reference(self) -> None:
        expr = self.x.unwrap(self.x.detector.purpose)
        self.assertIsNone(expr.get_literal_value())
        self.assertEqual(expr.expression_type, "dimension_field")
        self.assertEqual(expr.column_type, "string")
        self.assertEqual(str(expr), "detector.purpose")
        self.assertFalse(expr.is_literal)
        columns = qt.ColumnSet(self.universe.empty)
        expr.gather_required_columns(columns)
        self.assertEqual(columns.dimensions, self.universe.conform(["detector"]))
        self.assertEqual(columns.dimension_fields["detector"], {"purpose"})
        with self.assertRaises(InvalidQueryError):
            qt.DimensionFieldReference(element=self.universe.dimensions["detector"], field="region")
        self.assertEqual(
            expr.visit(_TestVisitor(dimension_fields={("detector", "purpose"): "science"})), "science"
        )

    def test_dataset_field_reference(self) -> None:
        expr = self.x.unwrap(self.x["raw"].ingest_date)
        self.assertIsNone(expr.get_literal_value())
        self.assertEqual(expr.expression_type, "dataset_field")
        self.assertEqual(str(expr), "raw.ingest_date")
        self.assertFalse(expr.is_literal)
        columns = qt.ColumnSet(self.universe.empty)
        expr.gather_required_columns(columns)
        self.assertEqual(columns.dimensions, self.universe.empty)
        self.assertEqual(columns.dataset_fields["raw"], {"ingest_date"})
        self.assertEqual(qt.DatasetFieldReference(dataset_type="raw", field="dataset_id").column_type, "uuid")
        self.assertEqual(
            qt.DatasetFieldReference(dataset_type="raw", field="collection").column_type, "string"
        )
        self.assertEqual(qt.DatasetFieldReference(dataset_type="raw", field="run").column_type, "string")
        self.assertEqual(
            qt.DatasetFieldReference(dataset_type="raw", field="ingest_date").column_type, "ingest_date"
        )
        self.assertEqual(
            qt.DatasetFieldReference(dataset_type="raw", field="timespan").column_type, "timespan"
        )
        value = astropy.time.Time("2020-01-01T00:00:00", format="isot", scale="tai")
        self.assertEqual(expr.visit(_TestVisitor(dataset_fields={("raw", "ingest_date"): value})), value)

    def test_unary_negation(self) -> None:
        expr = self.x.unwrap(-self.x.visit.exposure_time)
        self.assertIsNone(expr.get_literal_value())
        self.assertEqual(expr.expression_type, "unary")
        self.assertEqual(expr.column_type, "float")
        self.assertEqual(str(expr), "-visit.exposure_time")
        self.assertFalse(expr.is_literal)
        columns = qt.ColumnSet(self.universe.empty)
        expr.gather_required_columns(columns)
        self.assertEqual(columns.dimensions, self.universe.conform(["visit"]))
        self.assertEqual(columns.dimension_fields["visit"], {"exposure_time"})
        self.assertEqual(expr.visit(_TestVisitor(dimension_fields={("visit", "exposure_time"): 2.0})), -2.0)
        with self.assertRaises(InvalidQueryError):
            qt.UnaryExpression(
                operand=qt.DimensionFieldReference(
                    element=self.universe.dimensions["detector"], field="purpose"
                ),
                operator="-",
            )

    def test_unary_timespan_begin(self) -> None:
        expr = self.x.unwrap(self.x.visit.timespan.begin)
        self.assertIsNone(expr.get_literal_value())
        self.assertEqual(expr.expression_type, "unary")
        self.assertEqual(expr.column_type, "datetime")
        self.assertEqual(str(expr), "visit.timespan.begin")
        self.assertFalse(expr.is_literal)
        columns = qt.ColumnSet(self.universe.empty)
        expr.gather_required_columns(columns)
        self.assertEqual(columns.dimensions, self.universe.conform(["visit"]))
        self.assertEqual(columns.dimension_fields["visit"], {"timespan"})
        begin = astropy.time.Time("2020-01-01T00:00:00", format="isot", scale="tai")
        end = astropy.time.Time("2020-01-01T00:01:00", format="isot", scale="tai")
        value = Timespan(begin, end)
        self.assertEqual(
            expr.visit(_TestVisitor(dimension_fields={("visit", "timespan"): value})), value.begin
        )
        with self.assertRaises(InvalidQueryError):
            qt.UnaryExpression(
                operand=qt.DimensionFieldReference(
                    element=self.universe.dimensions["detector"], field="purpose"
                ),
                operator="begin_of",
            )

    def test_unary_timespan_end(self) -> None:
        expr = self.x.unwrap(self.x.visit.timespan.end)
        self.assertIsNone(expr.get_literal_value())
        self.assertEqual(expr.expression_type, "unary")
        self.assertEqual(expr.column_type, "datetime")
        self.assertEqual(str(expr), "visit.timespan.end")
        self.assertFalse(expr.is_literal)
        columns = qt.ColumnSet(self.universe.empty)
        expr.gather_required_columns(columns)
        self.assertEqual(columns.dimensions, self.universe.conform(["visit"]))
        self.assertEqual(columns.dimension_fields["visit"], {"timespan"})
        begin = astropy.time.Time("2020-01-01T00:00:00", format="isot", scale="tai")
        end = astropy.time.Time("2020-01-01T00:01:00", format="isot", scale="tai")
        value = Timespan(begin, end)
        self.assertEqual(expr.visit(_TestVisitor(dimension_fields={("visit", "timespan"): value})), value.end)
        with self.assertRaises(InvalidQueryError):
            qt.UnaryExpression(
                operand=qt.DimensionFieldReference(
                    element=self.universe.dimensions["detector"], field="purpose"
                ),
                operator="end_of",
            )

    def test_binary_expression_float(self) -> None:
        for proxy, string, value in [
            (self.x.visit.exposure_time + 15.0, "visit.exposure_time + 15.0", 45.0),
            (self.x.visit.exposure_time - 10.0, "visit.exposure_time - 10.0", 20.0),
            (self.x.visit.exposure_time * 6.0, "visit.exposure_time * 6.0", 180.0),
            (self.x.visit.exposure_time / 30.0, "visit.exposure_time / 30.0", 1.0),
            (15.0 + -self.x.visit.exposure_time, "15.0 + (-visit.exposure_time)", -15.0),
            (10.0 - -self.x.visit.exposure_time, "10.0 - (-visit.exposure_time)", 40.0),
            (6.0 * -self.x.visit.exposure_time, "6.0 * (-visit.exposure_time)", -180.0),
            (30.0 / -self.x.visit.exposure_time, "30.0 / (-visit.exposure_time)", -1.0),
            ((self.x.visit.exposure_time + 15.0) * 6.0, "(visit.exposure_time + 15.0) * 6.0", 270.0),
            ((self.x.visit.exposure_time + 15.0) + 45.0, "(visit.exposure_time + 15.0) + 45.0", 90.0),
            ((self.x.visit.exposure_time + 15.0) / 5.0, "(visit.exposure_time + 15.0) / 5.0", 9.0),
            # We don't need the parentheses we generate in the next one, but
            # they're not a problem either.
            ((self.x.visit.exposure_time + 15.0) - 60.0, "(visit.exposure_time + 15.0) - 60.0", -15.0),
            (6.0 * (-self.x.visit.exposure_time - 15.0), "6.0 * ((-visit.exposure_time) - 15.0)", -270.0),
            (60.0 + (-self.x.visit.exposure_time - 15.0), "60.0 + ((-visit.exposure_time) - 15.0)", 15.0),
            (90.0 / (-self.x.visit.exposure_time - 15.0), "90.0 / ((-visit.exposure_time) - 15.0)", -2.0),
            (60.0 - (-self.x.visit.exposure_time - 15.0), "60.0 - ((-visit.exposure_time) - 15.0)", 105.0),
        ]:
            with self.subTest(string=string):
                expr = self.x.unwrap(proxy)
                self.assertIsNone(expr.get_literal_value())
                self.assertEqual(expr.expression_type, "binary")
                self.assertEqual(expr.column_type, "float")
                self.assertEqual(str(expr), string)
                self.assertFalse(expr.is_literal)
                columns = qt.ColumnSet(self.universe.empty)
                expr.gather_required_columns(columns)
                self.assertEqual(columns.dimensions, self.universe.conform(["visit"]))
                self.assertEqual(columns.dimension_fields["visit"], {"exposure_time"})
                self.assertEqual(
                    expr.visit(_TestVisitor(dimension_fields={("visit", "exposure_time"): 30.0})), value
                )

    def test_binary_modulus(self) -> None:
        for proxy, string, value in [
            (self.x.visit.id % 2, "visit % 2", 1),
            (52 % self.x.visit, "52 % visit", 2),
        ]:
            with self.subTest(string=string):
                expr = self.x.unwrap(proxy)
                self.assertIsNone(expr.get_literal_value())
                self.assertEqual(expr.expression_type, "binary")
                self.assertEqual(expr.column_type, "int")
                self.assertEqual(str(expr), string)
                self.assertFalse(expr.is_literal)
                columns = qt.ColumnSet(self.universe.empty)
                expr.gather_required_columns(columns)
                self.assertEqual(columns.dimensions, self.universe.conform(["visit"]))
                self.assertFalse(columns.dimension_fields["visit"])
                self.assertEqual(expr.visit(_TestVisitor(dimension_keys={"visit": 5})), value)

    def test_binary_expression_validation(self) -> None:
        with self.assertRaises(InvalidQueryError):
            # No arithmetic operators on strings (we do not interpret + as
            # concatenation).
            self.x.instrument + "suffix"
        with self.assertRaises(InvalidQueryError):
            # Mixed types are not supported, even when they both support the
            # operator.
            self.x.visit.exposure_time + self.x.detector
        with self.assertRaises(InvalidQueryError):
            # No modulus for floats.
            self.x.visit.exposure_time % 5.0

    def test_reversed(self) -> None:
        expr = self.x.detector.desc
        self.assertIsNone(expr.get_literal_value())
        self.assertEqual(expr.expression_type, "reversed")
        self.assertEqual(expr.column_type, "int")
        self.assertEqual(str(expr), "detector DESC")
        self.assertFalse(expr.is_literal)
        columns = qt.ColumnSet(self.universe.empty)
        expr.gather_required_columns(columns)
        self.assertEqual(columns.dimensions, self.universe.conform(["detector"]))
        self.assertFalse(columns.dimension_fields["detector"])
        self.assertEqual(expr.visit(_TestVisitor(dimension_keys={"detector": 5})), _TestReversed(5))

    def test_trivial_predicate(self) -> None:
        """Test logical operations on trivial True/False predicates."""
        yes = qt.Predicate.from_bool(True)
        no = qt.Predicate.from_bool(False)
        maybe: qt.Predicate = self.x.detector == 5
        for predicate in [
            yes,
            yes.logical_or(no),
            no.logical_or(yes),
            yes.logical_and(yes),
            no.logical_not(),
            yes.logical_or(maybe),
            maybe.logical_or(yes),
        ]:
            self.assertEqual(predicate.column_type, "bool")
            self.assertEqual(str(predicate), "True")
            self.assertTrue(predicate.visit(_TestVisitor()))
            self.assertEqual(predicate.operands, ())
        for predicate in [
            no,
            yes.logical_and(no),
            no.logical_and(yes),
            no.logical_or(no),
            yes.logical_not(),
            no.logical_and(maybe),
            maybe.logical_and(no),
        ]:
            self.assertEqual(predicate.column_type, "bool")
            self.assertEqual(str(predicate), "False")
            self.assertFalse(predicate.visit(_TestVisitor()))
            self.assertEqual(predicate.operands, ((),))
        for predicate in [
            maybe,
            yes.logical_and(maybe),
            no.logical_or(maybe),
            maybe.logical_not().logical_not(),
        ]:
            self.assertEqual(predicate.column_type, "bool")
            self.assertEqual(str(predicate), "detector == 5")
            self.assertTrue(predicate.visit(_TestVisitor(dimension_keys={"detector": 5})))
            self.assertFalse(predicate.visit(_TestVisitor(dimension_keys={"detector": 4})))
            self.assertEqual(len(predicate.operands), 1)
            self.assertEqual(len(predicate.operands[0]), 1)
            self.assertIs(predicate.operands[0][0], maybe.operands[0][0])

    def test_comparison(self) -> None:
        predicate: qt.Predicate
        string: str
        value: bool
        for detector in (4, 5, 6):
            for predicate, string, value in [
                (self.x.detector == 5, "detector == 5", detector == 5),
                (self.x.detector != 5, "detector != 5", detector != 5),
                (self.x.detector < 5, "detector < 5", detector < 5),
                (self.x.detector > 5, "detector > 5", detector > 5),
                (self.x.detector <= 5, "detector <= 5", detector <= 5),
                (self.x.detector >= 5, "detector >= 5", detector >= 5),
                (self.x.detector == 5, "detector == 5", detector == 5),
                (self.x.detector != 5, "detector != 5", detector != 5),
                (self.x.detector < 5, "detector < 5", detector < 5),
                (self.x.detector > 5, "detector > 5", detector > 5),
                (self.x.detector <= 5, "detector <= 5", detector <= 5),
                (self.x.detector >= 5, "detector >= 5", detector >= 5),
            ]:
                with self.subTest(string=string, detector=detector):
                    self.assertEqual(predicate.column_type, "bool")
                    self.assertEqual(str(predicate), string)
                    columns = qt.ColumnSet(self.universe.empty)
                    predicate.gather_required_columns(columns)
                    self.assertEqual(columns.dimensions, self.universe.conform(["detector"]))
                    self.assertFalse(columns.dimension_fields["detector"])
                    self.assertEqual(
                        predicate.visit(_TestVisitor(dimension_keys={"detector": detector})), value
                    )
                    inverted = predicate.logical_not()
                    self.assertEqual(inverted.column_type, "bool")
                    self.assertEqual(str(inverted), f"NOT {string}")
                    self.assertEqual(
                        inverted.visit(_TestVisitor(dimension_keys={"detector": detector})), not value
                    )
                    columns = qt.ColumnSet(self.universe.empty)
                    inverted.gather_required_columns(columns)
                    self.assertEqual(columns.dimensions, self.universe.conform(["detector"]))
                    self.assertFalse(columns.dimension_fields["detector"])

    def test_overlap_comparison(self) -> None:
        pixelization = Mq3cPixelization(10)
        region1 = pixelization.quad(12058870)
        predicate = self.x.visit.region.overlaps(region1)
        self.assertEqual(predicate.column_type, "bool")
        self.assertEqual(str(predicate), "visit.region OVERLAPS (region)")
        columns = qt.ColumnSet(self.universe.empty)
        predicate.gather_required_columns(columns)
        self.assertEqual(columns.dimensions, self.universe.conform(["visit"]))
        self.assertEqual(columns.dimension_fields["visit"], {"region"})
        region2 = pixelization.quad(12058857)
        self.assertFalse(predicate.visit(_TestVisitor(dimension_fields={("visit", "region"): region2})))
        inverted = predicate.logical_not()
        self.assertEqual(inverted.column_type, "bool")
        self.assertEqual(str(inverted), "NOT visit.region OVERLAPS (region)")
        self.assertTrue(inverted.visit(_TestVisitor(dimension_fields={("visit", "region"): region2})))
        columns = qt.ColumnSet(self.universe.empty)
        inverted.gather_required_columns(columns)
        self.assertEqual(columns.dimensions, self.universe.conform(["visit"]))
        self.assertEqual(columns.dimension_fields["visit"], {"region"})

    def test_invalid_comparison(self) -> None:
        # Mixed type comparisons.
        with self.assertRaises(InvalidQueryError):
            self.x.visit > "three"
        with self.assertRaises(InvalidQueryError):
            self.x.visit > 3.0
        # Invalid operator for type.
        with self.assertRaises(InvalidQueryError):
            self.x["raw"].dataset_id < uuid.uuid4()

    def test_is_null(self) -> None:
        predicate = self.x.visit.region.is_null
        self.assertEqual(predicate.column_type, "bool")
        self.assertEqual(str(predicate), "visit.region IS NULL")
        columns = qt.ColumnSet(self.universe.empty)
        predicate.gather_required_columns(columns)
        self.assertEqual(columns.dimensions, self.universe.conform(["visit"]))
        self.assertEqual(columns.dimension_fields["visit"], {"region"})
        self.assertTrue(predicate.visit(_TestVisitor(dimension_fields={("visit", "region"): None})))
        inverted = predicate.logical_not()
        self.assertEqual(inverted.column_type, "bool")
        self.assertEqual(str(inverted), "NOT visit.region IS NULL")
        self.assertFalse(inverted.visit(_TestVisitor(dimension_fields={("visit", "region"): None})))
        inverted.gather_required_columns(columns)
        self.assertEqual(columns.dimensions, self.universe.conform(["visit"]))
        self.assertEqual(columns.dimension_fields["visit"], {"region"})

    def test_in_container(self) -> None:
        predicate: qt.Predicate = self.x.visit.in_iterable([3, 4, self.x.exposure.id])
        self.assertEqual(predicate.column_type, "bool")
        self.assertEqual(str(predicate), "visit IN [3, 4, exposure]")
        columns = qt.ColumnSet(self.universe.empty)
        predicate.gather_required_columns(columns)
        self.assertEqual(columns.dimensions, self.universe.conform(["visit", "exposure"]))
        self.assertFalse(columns.dimension_fields["visit"])
        self.assertFalse(columns.dimension_fields["exposure"])
        self.assertTrue(predicate.visit(_TestVisitor(dimension_keys={"visit": 2, "exposure": 2})))
        self.assertFalse(predicate.visit(_TestVisitor(dimension_keys={"visit": 2, "exposure": 5})))
        inverted = predicate.logical_not()
        self.assertEqual(inverted.column_type, "bool")
        self.assertEqual(str(inverted), "NOT visit IN [3, 4, exposure]")
        self.assertFalse(inverted.visit(_TestVisitor(dimension_keys={"visit": 2, "exposure": 2})))
        self.assertTrue(inverted.visit(_TestVisitor(dimension_keys={"visit": 2, "exposure": 5})))
        columns = qt.ColumnSet(self.universe.empty)
        inverted.gather_required_columns(columns)
        self.assertEqual(columns.dimensions, self.universe.conform(["visit", "exposure"]))
        self.assertFalse(columns.dimension_fields["visit"])
        self.assertFalse(columns.dimension_fields["exposure"])
        with self.assertRaises(InvalidQueryError):
            # Regions (and timespans) not allowed in IN expressions, since that
            # suggests topological logic we're not actually doing.  We can't
            # use ExpressionFactory because it prohibits this case with typing.
            pixelization = Mq3cPixelization(10)
            region = pixelization.quad(12058870)
            qt.Predicate.in_container(self.x.unwrap(self.x.visit.region), [qt.make_column_literal(region)])
        with self.assertRaises(InvalidQueryError):
            # Mismatched types.
            self.x.visit.in_iterable([3.5, 2.1])

    def test_in_range(self) -> None:
        predicate: qt.Predicate = self.x.visit.in_range(2, 8, 2)
        self.assertEqual(predicate.column_type, "bool")
        self.assertEqual(str(predicate), "visit IN 2:8:2")
        columns = qt.ColumnSet(self.universe.empty)
        predicate.gather_required_columns(columns)
        self.assertEqual(columns.dimensions, self.universe.conform(["visit"]))
        self.assertFalse(columns.dimension_fields["visit"])
        self.assertTrue(predicate.visit(_TestVisitor(dimension_keys={"visit": 2})))
        self.assertFalse(predicate.visit(_TestVisitor(dimension_keys={"visit": 8})))
        inverted = predicate.logical_not()
        self.assertEqual(inverted.column_type, "bool")
        self.assertEqual(str(inverted), "NOT visit IN 2:8:2")
        self.assertFalse(inverted.visit(_TestVisitor(dimension_keys={"visit": 2})))
        self.assertTrue(inverted.visit(_TestVisitor(dimension_keys={"visit": 8})))
        columns = qt.ColumnSet(self.universe.empty)
        inverted.gather_required_columns(columns)
        self.assertEqual(columns.dimensions, self.universe.conform(["visit"]))
        self.assertFalse(columns.dimension_fields["visit"])
        with self.assertRaises(InvalidQueryError):
            # Only integer fields allowed.
            self.x.visit.exposure_time.in_range(2, 4)
        with self.assertRaises(InvalidQueryError):
            # Step must be positive.
            self.x.visit.in_range(2, 4, -1)
        with self.assertRaises(InvalidQueryError):
            # Stop must be >= start.
            self.x.visit.in_range(2, 0)

    def test_in_query(self) -> None:
        query = self.query().join_dimensions(["visit", "tract"]).where(skymap="s", tract=3)
        predicate: qt.Predicate = self.x.exposure.in_query(self.x.visit, query)
        self.assertEqual(predicate.column_type, "bool")
        self.assertEqual(str(predicate), "exposure IN (query).visit")
        columns = qt.ColumnSet(self.universe.empty)
        predicate.gather_required_columns(columns)
        self.assertEqual(columns.dimensions, self.universe.conform(["exposure"]))
        self.assertFalse(columns.dimension_fields["exposure"])
        self.assertTrue(
            predicate.visit(_TestVisitor(dimension_keys={"exposure": 2}, query_tree_items={1, 2, 3}))
        )
        self.assertFalse(
            predicate.visit(_TestVisitor(dimension_keys={"exposure": 8}, query_tree_items={1, 2, 3}))
        )
        inverted = predicate.logical_not()
        self.assertEqual(inverted.column_type, "bool")
        self.assertEqual(str(inverted), "NOT exposure IN (query).visit")
        self.assertFalse(
            inverted.visit(_TestVisitor(dimension_keys={"exposure": 2}, query_tree_items={1, 2, 3}))
        )
        self.assertTrue(
            inverted.visit(_TestVisitor(dimension_keys={"exposure": 8}, query_tree_items={1, 2, 3}))
        )
        columns = qt.ColumnSet(self.universe.empty)
        inverted.gather_required_columns(columns)
        self.assertEqual(columns.dimensions, self.universe.conform(["exposure"]))
        self.assertFalse(columns.dimension_fields["exposure"])
        with self.assertRaises(InvalidQueryError):
            # Regions (and timespans) not allowed in IN expressions, since that
            # suggests topological logic we're not actually doing.  We can't
            # use ExpressionFactory because it prohibits this case with typing.
            qt.Predicate.in_query(
                self.x.unwrap(self.x.visit.region), self.x.unwrap(self.x.tract.region), query._tree
            )
        with self.assertRaises(InvalidQueryError):
            # Mismatched types.
            self.x.exposure.in_query(self.x.visit.exposure_time, query)
        with self.assertRaises(InvalidQueryError):
            # Query column requires dimensions that are not in the query.
            self.x.exposure.in_query(self.x.patch, query)
        with self.assertRaises(InvalidQueryError):
            # Query column requires dataset type that is not in the query.
            self.x["raw"].dataset_id.in_query(self.x["raw"].dataset_id, query)

    def test_complex_predicate(self) -> None:
        """Test that predicates are converted to conjunctive normal form and
        get parentheses in the right places when stringified.
        """
        visitor = _TestVisitor(dimension_keys={"instrument": "i", "detector": 3, "visit": 6, "band": "r"})
        a: qt.Predicate = self.x.visit > 5  # will evaluate to True
        b: qt.Predicate = self.x.detector != 3  # will evaluate to False
        c: qt.Predicate = self.x.instrument == "i"  # will evaluate to True
        d: qt.Predicate = self.x.band == "g"  # will evaluate to False
        predicate: qt.Predicate
        for predicate, string, value in [
            (a.logical_or(b), f"{a} OR {b}", True),
            (a.logical_or(c), f"{a} OR {c}", True),
            (b.logical_or(d), f"{b} OR {d}", False),
            (a.logical_and(b), f"{a} AND {b}", False),
            (a.logical_and(c), f"{a} AND {c}", True),
            (b.logical_and(d), f"{b} AND {d}", False),
            (self.x.any(a, b, c, d), f"{a} OR {b} OR {c} OR {d}", True),
            (self.x.all(a, b, c, d), f"{a} AND {b} AND {c} AND {d}", False),
            (a.logical_or(b).logical_and(c), f"({a} OR {b}) AND {c}", True),
            (a.logical_and(b.logical_or(d)), f"{a} AND ({b} OR {d})", False),
            (a.logical_and(b).logical_or(c), f"({a} OR {c}) AND ({b} OR {c})", True),
            (
                a.logical_and(b).logical_or(c.logical_and(d)),
                f"({a} OR {c}) AND ({a} OR {d}) AND ({b} OR {c}) AND ({b} OR {d})",
                False,
            ),
            (a.logical_or(b).logical_not(), f"NOT {a} AND NOT {b}", False),
            (a.logical_or(c).logical_not(), f"NOT {a} AND NOT {c}", False),
            (b.logical_or(d).logical_not(), f"NOT {b} AND NOT {d}", True),
            (a.logical_and(b).logical_not(), f"NOT {a} OR NOT {b}", True),
            (a.logical_and(c).logical_not(), f"NOT {a} OR NOT {c}", False),
            (b.logical_and(d).logical_not(), f"NOT {b} OR NOT {d}", True),
            (
                self.x.not_(a.logical_or(b).logical_and(c)),
                f"(NOT {a} OR NOT {c}) AND (NOT {b} OR NOT {c})",
                False,
            ),
            (
                a.logical_and(b.logical_or(d)).logical_not(),
                f"(NOT {a} OR NOT {b}) AND (NOT {a} OR NOT {d})",
                True,
            ),
        ]:
            with self.subTest(string=string):
                self.assertEqual(str(predicate), string)
                self.assertEqual(predicate.visit(visitor), value)

    def test_proxy_misc(self) -> None:
        """Test miscellaneous things on various ExpressionFactory proxies."""
        self.assertEqual(str(self.x.visit_detector_region), "visit_detector_region")
        self.assertEqual(str(self.x.visit.instrument), "instrument")
        self.assertEqual(str(self.x["raw"]), "raw")
        self.assertEqual(str(self.x["raw.ingest_date"]), "raw.ingest_date")
        self.assertEqual(
            str(self.x.visit.timespan.overlaps(self.x["raw"].timespan)),
            "visit.timespan OVERLAPS raw.timespan",
        )
        self.assertGreater(
            set(dir(self.x["raw"])), {"dataset_id", "ingest_date", "collection", "run", "timespan"}
        )
        self.assertGreater(set(dir(self.x.exposure)), {"seq_num", "science_program", "timespan"})
        with self.assertRaises(AttributeError):
            self.x["raw"].seq_num
        with self.assertRaises(AttributeError):
            self.x.visit.horse


class QueryTestCase(unittest.TestCase):
    """Tests for Query and *QueryResults objects in lsst.daf.butler.queries."""

    def setUp(self) -> None:
        self.maxDiff = None
        self.universe = DimensionUniverse()
        # We use ArrowTable as the storage class for all dataset types because
        # it's got conversions that only require third-party packages we
        # already require.
        self.raw = DatasetType(
            "raw", dimensions=self.universe.conform(["detector", "exposure"]), storageClass="ArrowTable"
        )
        self.refcat = DatasetType(
            "refcat", dimensions=self.universe.conform(["htm7"]), storageClass="ArrowTable"
        )
        self.bias = DatasetType(
            "bias",
            dimensions=self.universe.conform(["detector"]),
            storageClass="ArrowTable",
            isCalibration=True,
        )
        self.default_collections: list[str] | None = ["DummyCam/defaults"]
        self.collection_info: dict[str, tuple[CollectionRecord, CollectionSummary]] = {
            "DummyCam/raw/all": (
                RunRecord[int](1, name="DummyCam/raw/all"),
                CollectionSummary(NamedValueSet({self.raw}), governors={"instrument": {"DummyCam"}}),
            ),
            "DummyCam/calib": (
                CollectionRecord[int](2, name="DummyCam/calib", type=CollectionType.CALIBRATION),
                CollectionSummary(NamedValueSet({self.bias}), governors={"instrument": {"DummyCam"}}),
            ),
            "refcats": (
                RunRecord[int](3, name="refcats"),
                CollectionSummary(NamedValueSet({self.refcat}), governors={}),
            ),
            "DummyCam/defaults": (
                ChainedCollectionRecord[int](
                    4, name="DummyCam/defaults", children=("DummyCam/raw/all", "DummyCam/calib", "refcats")
                ),
                CollectionSummary(
                    NamedValueSet({self.raw, self.refcat, self.bias}), governors={"instrument": {"DummyCam"}}
                ),
            ),
        }
        self.dataset_types = {"raw": self.raw, "refcat": self.refcat, "bias": self.bias}

    def query(self, **kwargs: Any) -> Query:
        """Make an initial Query object with the given kwargs used to
        initialize the _TestQueryDriver.

        The given kwargs override the test-case-attribute defaults.
        """
        kwargs.setdefault("default_collections", self.default_collections)
        kwargs.setdefault("collection_info", self.collection_info)
        kwargs.setdefault("dataset_types", self.dataset_types)
        return Query(_TestQueryDriver(**kwargs), qt.make_identity_query_tree(self.universe))

    def test_dataset_join(self) -> None:
        """Test queries that have had a dataset search explicitly joined in via
        Query.join_dataset_search.

        Since this kind of query has a moderate amount of complexity, this is
        where we get a lot of basic coverage that applies to all kinds of
        queries, including:

         - getting data ID and dataset results (but not iterating over them);
         - the 'any' and 'explain_no_results' methods;
         - adding 'where' filters (but not expanding dimensions accordingly);
         - materializations.
        """

        def check(
            query: Query,
            dimensions: DimensionGroup = self.raw.dimensions,
        ) -> None:
            """Run a battery of tests on one of a set of very similar queries
            constructed in different ways (see below).
            """

            def check_query_tree(
                tree: qt.QueryTree,
                dimensions: DimensionGroup = dimensions,
            ) -> None:
                """Check the state of the QueryTree object that backs the Query
                or a derived QueryResults object.

                Parameters
                ----------
                tree : `lsst.daf.butler.queries.tree.QueryTree`
                    Object to test.
                dimensions : `DimensionGroup`
                    Dimensions to expect in the `QueryTree`, not necessarily
                    including those in the test 'raw' dataset type.
                """
                self.assertEqual(tree.dimensions, dimensions | self.raw.dimensions)
                self.assertEqual(str(tree.predicate), "raw.run == 'DummyCam/raw/all'")
                self.assertFalse(tree.materializations)
                self.assertFalse(tree.data_coordinate_uploads)
                self.assertEqual(tree.datasets.keys(), {"raw"})
                self.assertEqual(tree.datasets["raw"].dimensions, self.raw.dimensions)
                self.assertEqual(tree.datasets["raw"].collections, ("DummyCam/defaults",))
                self.assertEqual(tree.get_joined_dimension_groups(), frozenset({self.raw.dimensions}))

            def check_data_id_results(*args, query: Query, dimensions: DimensionGroup = dimensions) -> None:
                """Construct a DataCoordinateQueryResults object from the query
                with the given arguments and run a battery of tests on it.

                Parameters
                ----------
                *args
                    Forwarded to `Query.data_ids`.
                query : `Query`
                    Query to start from.
                dimensions : `DimensionGroup`, optional
                    Dimensions the result data IDs should have.
                """
                with self.assertRaises(_TestQueryExecution) as cm:
                    list(query.data_ids(*args))
                self.assertEqual(
                    cm.exception.result_spec,
                    qrs.DataCoordinateResultSpec(dimensions=dimensions),
                )
                check_query_tree(cm.exception.tree, dimensions=dimensions)

            def check_dataset_results(
                *args: Any,
                query: Query,
                find_first: bool = True,
                storage_class_name: str = self.raw.storageClass_name,
            ) -> None:
                """Construct a DatasetRefQueryResults object from the query
                with the given arguments and run a battery of tests on it.

                Parameters
                ----------
                *args
                    Forwarded to `Query.datasets`.
                query : `Query`
                    Query to start from.
                find_first : `bool`, optional
                    Whether to do find-first resolution on the results.
                storage_class_name : `str`, optional
                    Expected name of the storage class for the results.
                """
                with self.assertRaises(_TestQueryExecution) as cm:
                    list(query.datasets(*args, find_first=find_first))
                self.assertEqual(
                    cm.exception.result_spec,
                    qrs.DatasetRefResultSpec(
                        dataset_type_name="raw",
                        dimensions=self.raw.dimensions,
                        storage_class_name=storage_class_name,
                        find_first=find_first,
                    ),
                )
                check_query_tree(cm.exception.tree)

            def check_materialization(
                kwargs: Mapping[str, Any],
                query: Query,
                dimensions: DimensionGroup = dimensions,
                has_dataset: bool = True,
            ) -> None:
                """Materialize the query with the given arguments and run a
                battery of tests on the result.

                Parameters
                ----------
                kwargs
                    Forwarded as keyword arguments to `Query.materialize`.
                query : `Query`
                    Query to start from.
                dimensions : `DimensionGroup`, optional
                    Dimensions to expect in the materialization and its derived
                    query.
                has_dataset : `bool`, optional
                    Whether the query backed by the materialization should
                    still have the test 'raw' dataset joined in.
                """
                # Materialize the query and check the query tree sent to the
                # driver and the one in the materialized query.
                with self.assertRaises(_TestQueryExecution) as cm:
                    list(query.materialize(**kwargs).data_ids())
                derived_tree = cm.exception.tree
                self.assertEqual(derived_tree.dimensions, dimensions)
                # Predicate should be materialized away; it no longer appears
                # in the derived query.
                self.assertEqual(str(derived_tree.predicate), "True")
                self.assertFalse(derived_tree.data_coordinate_uploads)
                if has_dataset:
                    # Dataset search is still there, even though its existence
                    # constraint is included in the materialization, because we
                    # might need to re-join for some result columns in a
                    # derived query.
                    self.assertTrue(derived_tree.datasets.keys(), {"raw"})
                    self.assertEqual(derived_tree.datasets["raw"].dimensions, self.raw.dimensions)
                    self.assertEqual(derived_tree.datasets["raw"].collections, ("DummyCam/defaults",))
                else:
                    self.assertFalse(derived_tree.datasets)
                ((key, derived_tree_materialized_dimensions),) = derived_tree.materializations.items()
                self.assertEqual(derived_tree_materialized_dimensions, dimensions)
                (
                    materialized_tree,
                    materialized_dimensions,
                    materialized_datasets,
                ) = cm.exception.driver.materializations[key]
                self.assertEqual(derived_tree_materialized_dimensions, materialized_dimensions)
                if has_dataset:
                    self.assertEqual(materialized_datasets, {"raw"})
                else:
                    self.assertFalse(materialized_datasets)
                check_query_tree(materialized_tree)

            # Actual logic for the check() function begins here.

            self.assertEqual(query.constraint_dataset_types, {"raw"})
            self.assertEqual(query.constraint_dimensions, self.raw.dimensions)

            # Adding a constraint on a field for this dataset type should work
            # (this constraint will be present in all downstream tests).
            query = query.where(query.expression_factory["raw"].run == "DummyCam/raw/all")
            with self.assertRaises(InvalidQueryError):
                # Adding constraint on a different dataset should not work.
                query.where(query.expression_factory["refcat"].run == "refcats")

            # Data IDs, with dimensions defaulted.
            check_data_id_results(query=query)
            # Dimensions for data IDs the same as defaults.
            check_data_id_results(["exposure", "detector"], query=query)
            # Dimensions are a subset of the query dimensions.
            check_data_id_results(["exposure"], query=query, dimensions=self.universe.conform(["exposure"]))
            # Dimensions are a superset of the query dimensions.
            check_data_id_results(
                ["exposure", "detector", "visit"],
                query=query,
                dimensions=self.universe.conform(["exposure", "detector", "visit"]),
            )
            # Dimensions are neither a superset nor a subset of the query
            # dimensions.
            check_data_id_results(
                ["detector", "visit"], query=query, dimensions=self.universe.conform(["visit", "detector"])
            )
            # Dimensions are empty.
            check_data_id_results([], query=query, dimensions=self.universe.conform([]))

            # Get DatasetRef results, with various arguments and defaulting.
            check_dataset_results("raw", query=query)
            check_dataset_results("raw", query=query, find_first=True)
            check_dataset_results("raw", ["DummyCam/defaults"], query=query)
            check_dataset_results("raw", ["DummyCam/defaults"], query=query, find_first=True)
            check_dataset_results(self.raw, query=query)
            check_dataset_results(self.raw, query=query, find_first=True)
            check_dataset_results(self.raw, ["DummyCam/defaults"], query=query)
            check_dataset_results(self.raw, ["DummyCam/defaults"], query=query, find_first=True)

            # Changing collections at this stage is not allowed.
            with self.assertRaises(InvalidQueryError):
                query.datasets("raw", collections=["DummyCam/calib"])

            # Changing storage classes is allowed, if they're compatible.
            check_dataset_results(
                self.raw.overrideStorageClass("ArrowNumpy"), query=query, storage_class_name="ArrowNumpy"
            )
            with self.assertRaises(DatasetTypeError):
                # Can't use overrideStorageClass, because it'll raise
                # before the code we want to test can.
                query.datasets(DatasetType("raw", self.raw.dimensions, "int"))

            # Check the 'any' and 'explain_no_results' methods on Query itself.
            for execute, exact in itertools.permutations([False, True], 2):
                with self.assertRaises(_TestQueryAny) as cm:
                    query.any(execute=execute, exact=exact)
                self.assertEqual(cm.exception.execute, execute)
                self.assertEqual(cm.exception.exact, exact)
                check_query_tree(cm.exception.tree, dimensions)
            with self.assertRaises(_TestQueryExplainNoResults):
                query.explain_no_results()
            check_query_tree(cm.exception.tree, dimensions)

            # Materialize the query with defaults.
            check_materialization({}, query=query)
            # Materialize the query with args that match defaults.
            check_materialization({"dimensions": ["exposure", "detector"], "datasets": {"raw"}}, query=query)
            # Materialize the query with a superset of the original dimensions.
            check_materialization(
                {"dimensions": ["exposure", "detector", "visit"]},
                query=query,
                dimensions=self.universe.conform(["exposure", "visit", "detector"]),
            )
            # Materialize the query with no datasets.
            check_materialization(
                {"dimensions": ["exposure", "detector"], "datasets": frozenset()},
                query=query,
                has_dataset=False,
            )
            # Materialize the query with no datasets and a subset of the
            # dimensions.
            check_materialization(
                {"dimensions": ["exposure"], "datasets": frozenset()},
                query=query,
                has_dataset=False,
                dimensions=self.universe.conform(["exposure"]),
            )
            # Materializing the query with a dataset that is not in the query
            # is an error.
            with self.assertRaises(InvalidQueryError):
                query.materialize(datasets={"refcat"})
            # Materializing the query with dimensions that are not a superset
            # of any materialized dataset dimensions is an error.
            with self.assertRaises(InvalidQueryError):
                query.materialize(dimensions=["exposure"], datasets={"raw"})

        # Actual logic for test_dataset_joins starts here.

        # Default collections and existing dataset type name.
        check(self.query().join_dataset_search("raw"))
        # Default collections and existing DatasetType instance.
        check(self.query().join_dataset_search(self.raw))
        # Manual collections and existing dataset type.
        check(
            self.query(default_collections=None).join_dataset_search("raw", collections=["DummyCam/defaults"])
        )
        check(
            self.query(default_collections=None).join_dataset_search(
                self.raw, collections=["DummyCam/defaults"]
            )
        )
        with self.assertRaises(MissingDatasetTypeError):
            # Dataset type does not exist.
            self.query(dataset_types={}).join_dataset_search("raw", collections=["DummyCam/raw/all"])
        with self.assertRaises(DatasetTypeError):
            # Dataset type object with bad dimensions passed.
            self.query().join_dataset_search(
                DatasetType(
                    "raw",
                    dimensions={"detector", "visit"},
                    storageClass=self.raw.storageClass_name,
                    universe=self.universe,
                )
            )
        with self.assertRaises(TypeError):
            # Bad type for dataset type argument.
            self.query().join_dataset_search(3)
        with self.assertRaises(InvalidQueryError):
            # Cannot pass storage class override to join_dataset_search,
            # because we cannot use it there.
            self.query().join_dataset_search(self.raw.overrideStorageClass("ArrowAstropy"))

    def test_dimension_record_results(self) -> None:
        """Test queries that return dimension records.

        This includes tests for:

        - joining against uploaded data coordinates;
        - counting result rows;
        - expanding dimensions as needed for 'where' conditions;
        - order_by and limit.

        It does not include the iteration methods of
        DimensionRecordQueryResults, since those require a different mock
        driver setup (see test_dimension_record_iteration).
        """
        # Set up the base query-results object to test.
        query = self.query()
        x = query.expression_factory
        self.assertFalse(query.constraint_dimensions)
        query = query.where(x.skymap == "m")
        self.assertEqual(query.constraint_dimensions, self.universe.conform(["skymap"]))
        upload_rows = [
            DataCoordinate.standardize(instrument="DummyCam", visit=3, universe=self.universe),
            DataCoordinate.standardize(instrument="DummyCam", visit=4, universe=self.universe),
        ]
        raw_rows = frozenset([data_id.required_values for data_id in upload_rows])
        query = query.join_data_coordinates(upload_rows)
        self.assertEqual(query.constraint_dimensions, self.universe.conform(["skymap", "visit"]))
        results = query.dimension_records("patch")
        results = results.where(x.tract == 4)

        # Define a closure to run tests on variants of the base query.
        def check(
            results: DimensionRecordQueryResults,
            order_by: Any = (),
            limit: int | None = None,
        ) -> list[str]:
            results = results.order_by(*order_by).limit(limit)
            self.assertEqual(results.element.name, "patch")
            with self.assertRaises(_TestQueryExecution) as cm:
                list(results)
            tree = cm.exception.tree
            self.assertEqual(str(tree.predicate), "skymap == 'm' AND tract == 4")
            self.assertEqual(tree.dimensions, self.universe.conform(["visit", "patch"]))
            self.assertFalse(tree.materializations)
            self.assertFalse(tree.datasets)
            ((key, upload_dimensions),) = tree.data_coordinate_uploads.items()
            self.assertEqual(upload_dimensions, self.universe.conform(["visit"]))
            self.assertEqual(cm.exception.driver.data_coordinate_uploads[key], (upload_dimensions, raw_rows))
            result_spec = cm.exception.result_spec
            self.assertEqual(result_spec.result_type, "dimension_record")
            self.assertEqual(result_spec.element, self.universe["patch"])
            self.assertEqual(result_spec.limit, limit)
            for exact, discard in itertools.permutations([False, True], r=2):
                with self.assertRaises(_TestQueryCount) as cm:
                    results.count(exact=exact, discard=discard)
                self.assertEqual(cm.exception.result_spec, result_spec)
                self.assertEqual(cm.exception.exact, exact)
                self.assertEqual(cm.exception.discard, discard)
            return [str(term) for term in result_spec.order_by]

        # Run the closure's tests on variants of the base query.
        self.assertEqual(check(results), [])
        self.assertEqual(check(results, limit=2), [])
        self.assertEqual(check(results, order_by=[x.patch.cell_x]), ["patch.cell_x"])
        self.assertEqual(
            check(results, order_by=[x.patch.cell_x, x.patch.cell_y.desc]),
            ["patch.cell_x", "patch.cell_y DESC"],
        )
        with self.assertRaises(InvalidQueryError):
            # Cannot upload empty list of data IDs.
            query.join_data_coordinates([])
        with self.assertRaises(InvalidQueryError):
            # Cannot upload heterogeneous list of data IDs.
            query.join_data_coordinates(
                [
                    DataCoordinate.make_empty(self.universe),
                    DataCoordinate.standardize(instrument="DummyCam", universe=self.universe),
                ]
            )

    def test_dimension_record_iteration(self) -> None:
        """Tests for DimensionRecordQueryResult iteration."""

        def make_record(n: int) -> DimensionRecord:
            return self.universe["patch"].RecordClass(skymap="m", tract=4, patch=n)

        result_rows = (
            [make_record(n) for n in range(3)],
            [make_record(n) for n in range(3, 6)],
            [make_record(10)],
        )
        results = self.query(result_rows=result_rows).dimension_records("patch")
        self.assertEqual(list(results), list(itertools.chain.from_iterable(result_rows)))
        self.assertEqual(
            list(results.iter_set_pages()),
            [DimensionRecordSet(self.universe["patch"], rows) for rows in result_rows],
        )
        self.assertEqual(
            [table.column("id").to_pylist() for table in results.iter_table_pages()],
            [list(range(3)), list(range(3, 6)), [10]],
        )

    def test_data_coordinate_results(self) -> None:
        """Test queries that return data coordinates.

        This includes tests for:

        - counting result rows;
        - expanding dimensions as needed for 'where' conditions;
        - order_by and limit.

        It does not include the iteration methods of
        DataCoordinateQueryResults, since those require a different mock
        driver setup (see test_data_coordinate_iteration).  More tests for
        different inputs to DataCoordinateQueryResults construction are in
        test_dataset_join.
        """
        # Set up the base query-results object to test.
        query = self.query()
        x = query.expression_factory
        self.assertFalse(query.constraint_dimensions)
        query = query.where(x.skymap == "m")
        results = query.data_ids(["patch", "band"])
        results = results.where(x.tract == 4)

        # Define a closure to run tests on variants of the base query.
        def check(
            results: DataCoordinateQueryResults,
            order_by: Any = (),
            limit: int | None = None,
            include_dimension_records: bool = False,
        ) -> list[str]:
            results = results.order_by(*order_by).limit(limit)
            self.assertEqual(results.dimensions, self.universe.conform(["patch", "band"]))
            with self.assertRaises(_TestQueryExecution) as cm:
                list(results)
            tree = cm.exception.tree
            self.assertEqual(str(tree.predicate), "skymap == 'm' AND tract == 4")
            self.assertEqual(tree.dimensions, self.universe.conform(["patch", "band"]))
            self.assertFalse(tree.materializations)
            self.assertFalse(tree.datasets)
            self.assertFalse(tree.data_coordinate_uploads)
            result_spec = cm.exception.result_spec
            self.assertEqual(result_spec.result_type, "data_coordinate")
            self.assertEqual(result_spec.dimensions, self.universe.conform(["patch", "band"]))
            self.assertEqual(result_spec.include_dimension_records, include_dimension_records)
            self.assertEqual(result_spec.limit, limit)
            self.assertIsNone(result_spec.find_first_dataset)
            for exact, discard in itertools.permutations([False, True], r=2):
                with self.assertRaises(_TestQueryCount) as cm:
                    results.count(exact=exact, discard=discard)
                self.assertEqual(cm.exception.result_spec, result_spec)
                self.assertEqual(cm.exception.exact, exact)
                self.assertEqual(cm.exception.discard, discard)
            return [str(term) for term in result_spec.order_by]

        # Run the closure's tests on variants of the base query.
        self.assertEqual(check(results), [])
        self.assertEqual(check(results.with_dimension_records(), include_dimension_records=True), [])
        self.assertEqual(
            check(results.with_dimension_records().with_dimension_records(), include_dimension_records=True),
            [],
        )
        self.assertEqual(check(results, limit=2), [])
        self.assertEqual(check(results, order_by=[x.patch.cell_x]), ["patch.cell_x"])
        self.assertEqual(
            check(results, order_by=[x.patch.cell_x, x.patch.cell_y.desc]),
            ["patch.cell_x", "patch.cell_y DESC"],
        )
        self.assertEqual(
            check(results, order_by=["patch.cell_x", "-cell_y"]),
            ["patch.cell_x", "patch.cell_y DESC"],
        )

    def test_data_coordinate_iteration(self) -> None:
        """Tests for DataCoordinateQueryResult iteration."""

        def make_data_id(n: int) -> DimensionRecord:
            return DataCoordinate.standardize(skymap="m", tract=4, patch=n, universe=self.universe)

        result_rows = (
            [make_data_id(n) for n in range(3)],
            [make_data_id(n) for n in range(3, 6)],
            [make_data_id(10)],
        )
        results = self.query(result_rows=result_rows).data_ids(["patch"])
        self.assertEqual(list(results), list(itertools.chain.from_iterable(result_rows)))

    def test_dataset_results(self) -> None:
        """Test queries that return dataset refs.

        This includes tests for:

        - counting result rows;
        - expanding dimensions as needed for 'where' conditions;
        - different ways of passing a data ID to 'where' methods;
        - order_by and limit.

        It does not include the iteration methods of the DatasetRefQueryResults
        classes, since those require a different mock driver setup (see
        test_dataset_iteration).  More tests for different inputs to
        DatasetRefQueryResults construction are in test_dataset_join.
        """
        # Set up a few equivalent base query-results object to test.
        query = self.query()
        x = query.expression_factory
        self.assertFalse(query.constraint_dimensions)
        results1 = query.datasets("raw").where(x.instrument == "DummyCam", visit=4)
        results2 = query.datasets("raw", collections=["DummyCam/defaults"]).where(
            {"instrument": "DummyCam", "visit": 4}
        )
        results3 = query.datasets("raw").where(
            DataCoordinate.standardize(instrument="DummyCam", visit=4, universe=self.universe)
        )

        # Define a closure to check a DatasetRefQueryResults instance.
        def check(
            results: DatasetRefQueryResults,
            order_by: Any = (),
            limit: int | None = None,
            include_dimension_records: bool = False,
        ) -> list[str]:
            results = results.order_by(*order_by).limit(limit)
            with self.assertRaises(_TestQueryExecution) as cm:
                list(results)
            tree = cm.exception.tree
            self.assertEqual(str(tree.predicate), "instrument == 'DummyCam' AND visit == 4")
            self.assertEqual(
                tree.dimensions,
                self.universe.conform(["visit"]).union(results.dataset_type.dimensions),
            )
            self.assertFalse(tree.materializations)
            self.assertEqual(tree.datasets.keys(), {results.dataset_type.name})
            self.assertEqual(tree.datasets[results.dataset_type.name].collections, ("DummyCam/defaults",))
            self.assertEqual(
                tree.datasets[results.dataset_type.name].dimensions,
                results.dataset_type.dimensions,
            )
            self.assertFalse(tree.data_coordinate_uploads)
            result_spec = cm.exception.result_spec
            self.assertEqual(result_spec.result_type, "dataset_ref")
            self.assertEqual(result_spec.include_dimension_records, include_dimension_records)
            self.assertEqual(result_spec.limit, limit)
            self.assertEqual(result_spec.find_first_dataset, result_spec.dataset_type_name)
            for exact, discard in itertools.permutations([False, True], r=2):
                with self.assertRaises(_TestQueryCount) as cm:
                    results.count(exact=exact, discard=discard)
                self.assertEqual(cm.exception.result_spec, result_spec)
                self.assertEqual(cm.exception.exact, exact)
                self.assertEqual(cm.exception.discard, discard)
            with self.assertRaises(_TestQueryExecution) as cm:
                list(results.data_ids)
            self.assertEqual(
                cm.exception.result_spec,
                qrs.DataCoordinateResultSpec(
                    dimensions=results.dataset_type.dimensions,
                    include_dimension_records=include_dimension_records,
                ),
            )
            self.assertIs(cm.exception.tree, tree)
            return [str(term) for term in result_spec.order_by]

        # Run the closure's tests on variants of the base query.
        self.assertEqual(check(results1), [])
        self.assertEqual(check(results2), [])
        self.assertEqual(check(results3), [])
        self.assertEqual(check(results1.with_dimension_records(), include_dimension_records=True), [])
        self.assertEqual(
            check(results1.with_dimension_records().with_dimension_records(), include_dimension_records=True),
            [],
        )
        self.assertEqual(check(results1, limit=2), [])
        self.assertEqual(check(results1, order_by=["raw.timespan.begin"]), ["raw.timespan.begin"])
        self.assertEqual(check(results1, order_by=["detector"]), ["detector"])
        self.assertEqual(check(results1, order_by=["ingest_date"]), ["raw.ingest_date"])

    def test_dataset_iteration(self) -> None:
        """Tests for SingleTypeDatasetQueryResult iteration."""

        def make_ref(n: int) -> DimensionRecord:
            return DatasetRef(
                self.raw,
                DataCoordinate.standardize(
                    instrument="DummyCam", exposure=4, detector=n, universe=self.universe
                ),
                run="DummyCam/raw/all",
                id=uuid.uuid4(),
            )

        result_rows = (
            [make_ref(n) for n in range(3)],
            [make_ref(n) for n in range(3, 6)],
            [make_ref(10)],
        )
        results = self.query(result_rows=result_rows).datasets("raw")
        self.assertEqual(list(results), list(itertools.chain.from_iterable(result_rows)))

    def test_identifiers(self) -> None:
        """Test edge-cases of identifiers in order_by expressions."""

        def extract_order_by(results: DataCoordinateQueryResults) -> list[str]:
            with self.assertRaises(_TestQueryExecution) as cm:
                list(results)
            return [str(term) for term in cm.exception.result_spec.order_by]

        self.assertEqual(
            extract_order_by(self.query().data_ids(["day_obs"]).order_by("-timespan.begin")),
            ["day_obs.timespan.begin DESC"],
        )
        self.assertEqual(
            extract_order_by(self.query().data_ids(["day_obs"]).order_by("timespan.end")),
            ["day_obs.timespan.end"],
        )
        self.assertEqual(
            extract_order_by(self.query().data_ids(["visit"]).order_by("-visit.timespan.begin")),
            ["visit.timespan.begin DESC"],
        )
        self.assertEqual(
            extract_order_by(self.query().data_ids(["visit"]).order_by("visit.timespan.end")),
            ["visit.timespan.end"],
        )
        self.assertEqual(
            extract_order_by(self.query().data_ids(["visit"]).order_by("visit.science_program")),
            ["visit.science_program"],
        )
        self.assertEqual(
            extract_order_by(self.query().data_ids(["visit"]).order_by("visit.id")),
            ["visit"],
        )
        self.assertEqual(
            extract_order_by(self.query().data_ids(["visit"]).order_by("visit.physical_filter")),
            ["physical_filter"],
        )
        with self.assertRaises(TypeError):
            self.query().data_ids(["visit"]).order_by(3)
        with self.assertRaises(InvalidQueryError):
            self.query().data_ids(["visit"]).order_by("visit.region")
        with self.assertRaisesRegex(InvalidQueryError, "Ambiguous"):
            self.query().data_ids(["visit", "exposure"]).order_by("timespan.begin")
        with self.assertRaisesRegex(InvalidQueryError, "Unrecognized"):
            self.query().data_ids(["visit", "exposure"]).order_by("blarg")
        with self.assertRaisesRegex(InvalidQueryError, "Unrecognized"):
            self.query().data_ids(["visit", "exposure"]).order_by("visit.horse")
        with self.assertRaisesRegex(InvalidQueryError, "Unrecognized"):
            self.query().data_ids(["visit", "exposure"]).order_by("visit.science_program.monkey")
        with self.assertRaisesRegex(InvalidQueryError, "not valid for datasets"):
            self.query().datasets("raw").order_by("raw.seq_num")

    def test_invalid_models(self) -> None:
        """Test invalid models and combinations of models that cannot be
        constructed via the public Query and *QueryResults interfaces.
        """
        x = ExpressionFactory(self.universe)
        with self.assertRaises(InvalidQueryError):
            # QueryTree dimensions do not cover dataset dimensions.
            qt.QueryTree(
                dimensions=self.universe.conform(["visit"]),
                datasets={
                    "raw": qt.DatasetSearch(
                        collections=("DummyCam/raw/all",),
                        dimensions=self.raw.dimensions,
                    )
                },
            )
        with self.assertRaises(InvalidQueryError):
            # QueryTree dimensions do no cover predicate dimensions.
            qt.QueryTree(
                dimensions=self.universe.conform(["visit"]),
                predicate=(x.detector > 5),
            )
        with self.assertRaises(InvalidQueryError):
            # Predicate references a dataset not in the QueryTree.
            qt.QueryTree(
                dimensions=self.universe.conform(["exposure", "detector"]),
                predicate=(x["raw"].collection == "bird"),
            )
        with self.assertRaises(InvalidQueryError):
            # ResultSpec's dimensions are not a subset of the query tree's.
            DimensionRecordQueryResults(
                _TestQueryDriver(),
                qt.QueryTree(dimensions=self.universe.conform(["tract"])),
                qrs.DimensionRecordResultSpec(element=self.universe["detector"]),
            )
        with self.assertRaises(InvalidQueryError):
            # ResultSpec's datasets are not a subset of the query tree's.
            DatasetRefQueryResults(
                _TestQueryDriver(),
                qt.QueryTree(dimensions=self.raw.dimensions),
                qrs.DatasetRefResultSpec(
                    dataset_type_name="raw",
                    dimensions=self.raw.dimensions,
                    storage_class_name=self.raw.storageClass_name,
                    find_first=True,
                ),
            )
        with self.assertRaises(InvalidQueryError):
            # ResultSpec's order_by expression is not related to the dimensions
            # we're returning.
            x = ExpressionFactory(self.universe)
            DimensionRecordQueryResults(
                _TestQueryDriver(),
                qt.QueryTree(dimensions=self.universe.conform(["detector", "visit"])),
                qrs.DimensionRecordResultSpec(
                    element=self.universe["detector"], order_by=(x.unwrap(x.visit),)
                ),
            )
        with self.assertRaises(InvalidQueryError):
            # ResultSpec's order_by expression is not related to the datasets
            # we're returning.
            x = ExpressionFactory(self.universe)
            DimensionRecordQueryResults(
                _TestQueryDriver(),
                qt.QueryTree(dimensions=self.universe.conform(["detector", "visit"])),
                qrs.DimensionRecordResultSpec(
                    element=self.universe["detector"], order_by=(x.unwrap(x["raw"].ingest_date),)
                ),
            )

    def test_general_result_spec(self) -> None:
        """Tests for GeneralResultSpec.

        Unlike the other ResultSpec objects, we don't have a *QueryResults
        class for GeneralResultSpec yet, so we can't use the higher-level
        interfaces to test it like we can the others.
        """
        a = qrs.GeneralResultSpec(
            dimensions=self.universe.conform(["detector"]),
            dimension_fields={"detector": {"purpose"}},
            dataset_fields={},
            find_first=False,
        )
        self.assertEqual(a.find_first_dataset, None)
        a_columns = qt.ColumnSet(self.universe.conform(["detector"]))
        a_columns.dimension_fields["detector"].add("purpose")
        self.assertEqual(a.get_result_columns(), a_columns)
        b = qrs.GeneralResultSpec(
            dimensions=self.universe.conform(["detector"]),
            dimension_fields={},
            dataset_fields={"bias": {"timespan", "dataset_id"}},
            find_first=True,
        )
        self.assertEqual(b.find_first_dataset, "bias")
        b_columns = qt.ColumnSet(self.universe.conform(["detector"]))
        b_columns.dataset_fields["bias"].add("timespan")
        b_columns.dataset_fields["bias"].add("dataset_id")
        self.assertEqual(b.get_result_columns(), b_columns)
        with self.assertRaises(InvalidQueryError):
            # More than one dataset type with find_first
            qrs.GeneralResultSpec(
                dimensions=self.universe.conform(["detector", "exposure"]),
                dimension_fields={},
                dataset_fields={"bias": {"dataset_id"}, "raw": {"dataset_id"}},
                find_first=True,
            )
        with self.assertRaises(InvalidQueryError):
            # Out-of-bounds dimension fields.
            qrs.GeneralResultSpec(
                dimensions=self.universe.conform(["detector"]),
                dimension_fields={"visit": {"name"}},
                dataset_fields={},
                find_first=False,
            )
        with self.assertRaises(InvalidQueryError):
            # No fields for dimension element.
            qrs.GeneralResultSpec(
                dimensions=self.universe.conform(["detector"]),
                dimension_fields={"detector": set()},
                dataset_fields={},
                find_first=True,
            )
        with self.assertRaises(InvalidQueryError):
            # No fields for dataset.
            qrs.GeneralResultSpec(
                dimensions=self.universe.conform(["detector"]),
                dimension_fields={},
                dataset_fields={"bias": set()},
                find_first=True,
            )


if __name__ == "__main__":
    unittest.main()

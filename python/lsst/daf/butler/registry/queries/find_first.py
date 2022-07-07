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

__all__ = ("FindFirst",)

from collections.abc import Mapping, Sequence, Set
from typing import Any

import sqlalchemy
from lsst.daf.relation import ColumnError, Extension, OrderByTerm, Relation, UniqueKey, sql
from lsst.utils.classes import cached_getter

from ...core import ColumnTag, LogicalColumn


class FindFirst(Extension[ColumnTag]):
    def __init__(
        self,
        base: Relation[ColumnTag],
        rank: ColumnTag,
        partition: Set[ColumnTag],
    ):
        self._base = base
        self._rank = rank
        self._partition = partition

    @property  # type: ignore
    @cached_getter
    def columns(self) -> Set[ColumnTag]:
        # Docstring inherited.
        columns: set[ColumnTag] = set(self._base.columns)
        columns.remove(self._rank)
        return columns

    @property
    def unique_keys(self) -> Set[UniqueKey[ColumnTag]]:
        # Docstring inherited.
        return frozenset()

    @property
    def base(self) -> Relation[ColumnTag]:
        # Docstring inherited.
        return self._base

    @property
    def name(self) -> str:
        # Docstring inherited.
        return "find_first"

    def checked_and_simplified(self, *, recursive: bool = True) -> Relation[ColumnTag]:
        if self._rank not in self._base.columns:
            raise ColumnError(
                f"Rank column {self._rank} for find-first search not in base relation {self._base}."
            )
        if not self._partition <= self._base.columns:
            raise ColumnError(
                f"Partition column(s) {set(self._partition - self._base.columns)} for find-first search "
                f"not in base relation {self._base}."
            )
        return super().checked_and_simplified(recursive=recursive)

    def rebased(self, base: Relation[ColumnTag], *, equivalent: bool) -> Relation[ColumnTag]:
        # Docstring inherited.
        if equivalent:
            return FindFirst(base, self._rank, self._partition).assert_checked_and_simplified()
        else:
            return FindFirst(base, self._rank, self._partition).checked_and_simplified()

    def write_extra_to_mapping(self) -> Mapping[str, Any]:
        # Docstring inherited.
        return {"rank": str(self._rank)}

    def to_select_parts(
        self, column_types: sql.ColumnTypeInfo[ColumnTag, LogicalColumn]
    ) -> sql.SelectParts[ColumnTag, LogicalColumn]:
        # In the more general case, we build a subquery of the form below to
        # search the collections in order.
        #
        # WITH {dst}_search AS (
        #     SELECT {data-id-cols}, id, run_id, 1 AS rank
        #         FROM <collection1>
        #     UNION ALL
        #     SELECT {data-id-cols}, id, run_id, 2 AS rank
        #         FROM <collection2>
        #     UNION ALL
        #     ...
        # )
        # SELECT
        #     {dst}_window.{data-id-cols},
        #     {dst}_window.id,
        #     {dst}_window.run_id
        # FROM (
        #     SELECT
        #         {dst}_search.{data-id-cols},
        #         {dst}_search.id,
        #         {dst}_search.run_id,
        #         ROW_NUMBER() OVER (
        #             PARTITION BY {dst_search}.{data-id-cols}
        #             ORDER BY rank
        #         ) AS rownum
        #     ) {dst}_window
        # WHERE
        #     {dst}_window.rownum = 1;
        #
        # We'll start with the Common Table Expression (CTE) at the top.
        # We get that by delegating to join_dataset_query, which also takes
        # care of any relation or constraints the caller may have passed.
        search_cte = self.base.visit(sql.ToExecutable(column_types)).cte()
        # Create a columns object that holds the SQLAlchemy objects for the
        # columns that are SELECT'd in the CTE, and hence available downstream.
        search_columns = column_types.extract_mapping(self._base.columns, search_cte.columns)

        # Now we fill out the inner SELECT subquery from the CTE.  We replace
        # the rank column with the window-function 'rownum' calculated from it;
        # which is like the rank in that it orders datasets by the collection
        # in which they are found (separately for each data ID), but critically
        # it doesn't have gaps where the dataset wasn't found in one the input
        # collections, and hence ``rownum=1`` reliably picks out the find-first
        # result.  We use SQLAlchemy objects directly here instead of Relation,
        # because it involves some advanced SQL constructs we don't want
        # Relation to try to wrap.  We still use our columns classes to let
        # them encapsulate actual column names.
        rownum_column = (
            sqlalchemy.sql.func.row_number()
            .over(
                partition_by=[search_columns[tag] for tag in self._partition],
                order_by=search_columns[self._rank],
            )
            .label("rownum")
        )
        del search_columns[self._rank]
        window_subquery = column_types.select_items(
            search_columns.items(), search_cte, rownum_column
        ).subquery()
        # Create a new columns mapping to hold the columns SELECTed by the
        # subquery.  This does not include the calculated 'rownum' column,
        # which we'll handle separately; this works out well because we only
        # want it in the WHERE clause anyway.
        window_columns = column_types.extract_mapping(search_columns.keys(), window_subquery.columns)
        # We'll want to package up the full query as Relation instance, so we
        # build one from SQL parts instead of using SQLAlchemy to make a SELECT
        # directly.
        return sql.SelectParts[ColumnTag, LogicalColumn](
            window_subquery, where=[window_subquery.columns["rownum"] == 1], columns_available=window_columns
        )

    def to_executable(
        self,
        column_types: sql.ColumnTypeInfo[ColumnTag, LogicalColumn],
        *,
        distinct: bool = False,
        order_by: Sequence[OrderByTerm[ColumnTag]] = (),
        offset: int = 0,
        limit: int | None = None,
    ) -> sqlalchemy.sql.expression.SelectBase:
        return self.to_select_parts(column_types).to_executable(
            self, column_types, distinct=distinct, order_by=order_by, offset=offset, limit=limit
        )

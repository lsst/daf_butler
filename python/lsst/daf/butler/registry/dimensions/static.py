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

import dataclasses
import itertools
import logging
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence, Set
from typing import TYPE_CHECKING, Any

import sqlalchemy
from lsst.daf.relation import Calculation, ColumnExpression, Join, Relation, sql
from lsst.sphgeom import Region

from ... import ddl
from ..._column_tags import DimensionKeyColumnTag, DimensionRecordColumnTag
from ..._column_type_info import LogicalColumn
from ..._exceptions import UnimplementedQueryError
from ..._named import NamedKeyDict
from ...dimensions import (
    DatabaseDimensionElement,
    DatabaseTopologicalFamily,
    DataCoordinate,
    Dimension,
    DimensionElement,
    DimensionGroup,
    DimensionRecord,
    DimensionRecordSet,
    DimensionUniverse,
    SkyPixDimension,
    addDimensionForeignKey,
)
from ...dimensions.record_cache import DimensionRecordCache
from ...direct_query_driver import QueryBuilder, QueryJoiner  # Future query system (direct,server).
from ...queries import tree as qt  # Future query system (direct,client,server)
from ...queries.overlaps import OverlapsVisitor
from ...queries.visitors import PredicateVisitFlags
from .._exceptions import MissingSpatialOverlapError
from ..interfaces import Database, DimensionRecordStorageManager, StaticTablesContext, VersionTuple

if TYPE_CHECKING:
    from .. import queries  # Current Registry.query* system.


# This has to be updated on every schema change
_VERSION = VersionTuple(6, 0, 2)

_LOG = logging.getLogger(__name__)


class StaticDimensionRecordStorageManager(DimensionRecordStorageManager):
    """An implementation of `DimensionRecordStorageManager` for single-layer
    `Registry` and the base layers of multi-layer `Registry`.

    This manager creates `DimensionRecordStorage` instances for all elements
    in the `DimensionUniverse` in its own `initialize` method, as part of
    static table creation, so it never needs to manage any dynamic registry
    tables.

    Parameters
    ----------
    db : `Database`
        Interface to the underlying database engine and namespace.
    tables : `dict` [ `str`, `sqlalchemy.Table` ]
        Mapping from dimension element name to SQL table, for all elements that
        have `DimensionElement.has_own_table` `True`.
    overlap_tables : `dict` [ `str`, `tuple` [ `sqlalchemy.Table`, \
            `sqlalchemy.Table` ] ]
        Mapping from dimension element name to SQL table holding overlaps
        between the common skypix dimension and that element, for all elements
        that have `DimensionElement.has_own_table` `True` and
        `DimensionElement.spatial` not `None`.
    dimension_group_storage : `_DimensionGroupStorage`
        Object that manages saved `DimensionGroup` definitions.
    universe : `DimensionUniverse`
        All known dimensions.
    registry_schema_version : `VersionTuple` or `None`, optional
        Version of registry schema.
    """

    def __init__(
        self,
        db: Database,
        *,
        tables: dict[str, sqlalchemy.Table],
        overlap_tables: dict[str, tuple[sqlalchemy.Table, sqlalchemy.Table]],
        dimension_group_storage: _DimensionGroupStorage,
        universe: DimensionUniverse,
        registry_schema_version: VersionTuple | None = None,
    ):
        super().__init__(universe=universe, registry_schema_version=registry_schema_version)
        self._db = db
        self._tables = tables
        self._overlap_tables = overlap_tables
        self._dimension_group_storage = dimension_group_storage

    def clone(self, db: Database) -> StaticDimensionRecordStorageManager:
        return StaticDimensionRecordStorageManager(
            db,
            tables=self._tables,
            overlap_tables=self._overlap_tables,
            dimension_group_storage=self._dimension_group_storage.clone(db),
            universe=self.universe,
            registry_schema_version=self._registry_schema_version,
        )

    @classmethod
    def initialize(
        cls,
        db: Database,
        context: StaticTablesContext,
        *,
        universe: DimensionUniverse,
        registry_schema_version: VersionTuple | None = None,
    ) -> DimensionRecordStorageManager:
        # Docstring inherited from DimensionRecordStorageManager.
        tables: dict[str, sqlalchemy.Table] = {}
        # Define tables for governor dimensions, which are never spatial or
        # temporal and always have tables.
        for dimension in universe.governor_dimensions:
            spec = dimension.RecordClass.fields.makeTableSpec(
                TimespanReprClass=db.getTimespanRepresentation()
            )
            tables[dimension.name] = context.addTable(dimension.name, spec)
        # Define tables for database dimension elements, which may or may not
        # have their own tables and may be spatial or temporal.
        spatial = NamedKeyDict[DatabaseTopologicalFamily, list[DimensionElement]]()
        overlap_tables: dict[str, tuple[sqlalchemy.Table, sqlalchemy.Table]] = {}
        for element in universe.database_elements:
            if not element.has_own_table:
                continue
            spec = element.RecordClass.fields.makeTableSpec(TimespanReprClass=db.getTimespanRepresentation())
            tables[element.name] = context.addTable(element.name, spec)
            if element.spatial is not None:
                spatial.setdefault(element.spatial, []).append(element)
                overlap_tables[element.name] = cls._make_skypix_overlap_tables(context, element)
            for field_name in spec.fields.names:
                if (
                    len(qt.ColumnSet.get_qualified_name(element.name, field_name))
                    >= db.dialect.max_identifier_length
                ):
                    # Being able to assume that all dimension fields fit inside
                    # the DB's identifier limit is really convenient and very
                    # unlikely to cause trouble in practice.  We'll just make
                    # sure we catch any such trouble as early as possible.
                    raise RuntimeError(
                        f"Dimension filed '{element.name}.{field_name}' is too long for this database. "
                        "Please file a ticket for long-field support if this was not a mistake."
                    )
        # Add some tables for materialized overlaps between database
        # dimensions.  We've never used these and no longer plan to, but we
        # have to keep creating them to keep schema versioning consistent.
        cls._make_legacy_overlap_tables(context, spatial)
        # Create tables that store DimensionGroup definitions.
        dimension_group_storage = _DimensionGroupStorage.initialize(db, context, universe=universe)
        return cls(
            db=db,
            tables=tables,
            overlap_tables=overlap_tables,
            universe=universe,
            dimension_group_storage=dimension_group_storage,
            registry_schema_version=registry_schema_version,
        )

    def fetch_cache_dict(self) -> dict[str, DimensionRecordSet]:
        # Docstring inherited.
        result: dict[str, DimensionRecordSet] = {}
        with self._db.transaction():
            for element in self.universe.elements:
                if not element.is_cached:
                    continue
                assert not element.temporal, (
                    "Cached dimension elements should not be spatial or temporal, as that "
                    "suggests a large number of records."
                )
                if element.implied_union_target is not None:
                    assert isinstance(element, Dimension), "Only dimensions can be implied dependencies."
                    table = self._tables[element.implied_union_target.name]
                    sql = sqlalchemy.select(
                        table.columns[element.name].label(element.primary_key.name)
                    ).distinct()
                else:
                    table = self._tables[element.name]
                    sql = table.select()
                with self._db.query(sql) as results:
                    result[element.name] = DimensionRecordSet(
                        element=element,
                        records=[element.RecordClass(**row) for row in results.mappings()],
                    )
        return result

    def insert(
        self,
        element: DimensionElement,
        *records: DimensionRecord,
        replace: bool = False,
        skip_existing: bool = False,
    ) -> None:
        # Docstring inherited.
        if not element.has_own_table:
            raise TypeError(f"Cannot insert {element.name} records.")
        db_rows = self._make_record_db_rows(element, records, replace=replace)
        table = self._tables[element.name]
        with self._db.transaction():
            if replace:
                self._db.replace(table, *db_rows.main_rows)
            elif skip_existing:
                self._db.ensure(table, *db_rows.main_rows, primary_key_only=True)
            else:
                self._db.insert(table, *db_rows.main_rows)
            self._insert_overlaps(
                element, db_rows.overlap_insert_rows, db_rows.overlap_delete_rows, skip_existing=skip_existing
            )
            for related_element_name, summary_rows in db_rows.overlap_summary_rows.items():
                self._db.ensure(self._overlap_tables[related_element_name][0], *summary_rows)

    def sync(self, record: DimensionRecord, update: bool = False) -> bool | dict[str, Any]:
        # Docstring inherited.
        if not record.definition.has_own_table:
            raise TypeError(f"Cannot sync {record.definition.name} records.")
        # We might not need the overlap rows at all; we won't know until we try
        # to insert the main row.  But we figure it's better to spend the time
        # to compute them in advance always *outside* the database transaction
        # than to compute them only as-needed inside the database transaction,
        # since in-transaction time is especially precious.
        db_rows = self._make_record_db_rows(record.definition, [record], replace=True)
        (compared,) = db_rows.main_rows
        keys = {}
        for name in record.fields.required.names:
            keys[name] = compared.pop(name)
        with self._db.transaction():
            _, inserted_or_updated = self._db.sync(
                self._tables[record.definition.name],
                keys=keys,
                compared=compared,
                update=update,
            )
            if inserted_or_updated:
                if inserted_or_updated is True:
                    # Inserted a new row, so we just need to insert new
                    # overlap rows (if there are any).
                    self._insert_overlaps(
                        record.definition, db_rows.overlap_insert_rows, overlap_delete_rows=[]
                    )
                elif "region" in inserted_or_updated:
                    # Updated the region, so we need to delete old overlap
                    # rows and insert new ones.
                    self._insert_overlaps(
                        record.definition, db_rows.overlap_insert_rows, db_rows.overlap_delete_rows
                    )
                for related_element_name, summary_rows in db_rows.overlap_summary_rows.items():
                    self._db.ensure(self._overlap_tables[related_element_name][0], *summary_rows)
        return inserted_or_updated

    def fetch_one(
        self,
        element_name: str,
        data_id: DataCoordinate,
        cache: DimensionRecordCache,
    ) -> DimensionRecord | None:
        # Docstring inherited.
        element = self.universe[element_name]
        if element_name in cache:
            try:
                return cache[element_name].find(data_id)
            except LookupError:
                return None
        if element.implied_union_target is not None:
            assert isinstance(element, Dimension), "Only dimensions can be implied dependencies."
            table = self._tables[element.implied_union_target.name]
            sql = sqlalchemy.select(table.columns[element.name].label(element.primary_key.name)).where(
                table.columns[element_name] == data_id[element_name]
            )
        elif isinstance(element, SkyPixDimension):
            id = data_id[element_name]
            return element.RecordClass(id=id, region=element.pixelization.pixel(id))
        else:
            table = self._tables[element.name]
            sql = table.select().where(
                *[
                    table.columns[column_name] == data_id[dimension_name]
                    for column_name, dimension_name in zip(
                        element.schema.required.names, element.required.names
                    )
                ]
            )
        with self._db.query(sql) as results:
            row = results.fetchone()
            if row is None:
                return None
            mapping: Mapping
            if element.temporal is not None:
                mapping = dict(**row._mapping)
                timespan = self._db.getTimespanRepresentation().extract(mapping)
                for name in self._db.getTimespanRepresentation().getFieldNames():
                    del mapping[name]
                mapping["timespan"] = timespan
            else:
                mapping = row._mapping
            return element.RecordClass(**mapping)

    def save_dimension_group(self, group: DimensionGroup) -> int:
        # Docstring inherited from DimensionRecordStorageManager.
        return self._dimension_group_storage.save(group)

    def load_dimension_group(self, key: int) -> DimensionGroup:
        # Docstring inherited from DimensionRecordStorageManager.
        return self._dimension_group_storage.load(key)

    def join(
        self,
        element_name: str,
        target: Relation,
        join: Join,
        context: queries.SqlQueryContext,
    ) -> Relation:
        # Docstring inherited.
        element = self.universe[element_name]
        # We use Join.partial(...).apply(...) instead of Join.apply(..., ...)
        # for the "backtracking" insertion capabilities of the former; more
        # specifically, if `target` is a tree that starts with SQL relations
        # and ends with iteration-engine operations (e.g. region-overlap
        # postprocessing), this will try to perform the join upstream in the
        # SQL engine before the transfer to iteration.
        if element.has_own_table:
            return join.partial(self._make_relation(element, context)).apply(target)
        elif element.implied_union_target is not None:
            columns = DimensionKeyColumnTag(element.name)
            return join.partial(
                self._make_relation(element.implied_union_target, context)
                .with_only_columns(
                    {columns},
                    preferred_engine=context.preferred_engine,
                    require_preferred_engine=True,
                )
                .without_duplicates()
            ).apply(target)
        elif isinstance(element, SkyPixDimension):
            assert join.predicate.as_trivial(), "Expected trivial join predicate for skypix relation."
            id_column = DimensionKeyColumnTag(element.name)
            assert id_column in target.columns, "Guaranteed by QueryBuilder.make_dimension_target."
            function_name = f"{element.name}_region"
            context.iteration_engine.functions[function_name] = element.pixelization.pixel
            calculation = Calculation(
                tag=DimensionRecordColumnTag(element.name, "region"),
                expression=ColumnExpression.function(function_name, ColumnExpression.reference(id_column)),
            )
            return calculation.apply(
                target, preferred_engine=context.iteration_engine, transfer=True, backtrack=True
            )
        else:
            raise AssertionError(f"Unexpected definition of {element_name!r}.")

    def make_spatial_join_relation(
        self,
        element1: str,
        element2: str,
        context: queries.SqlQueryContext,
        existing_relationships: Set[frozenset[str]] = frozenset(),
    ) -> tuple[Relation, bool]:
        # Docstring inherited.
        group1 = self.universe[element1].minimal_group
        group2 = self.universe[element2].minimal_group
        overlap_relationships = {
            frozenset(a | b)
            for a, b in itertools.product(
                [group1.names, group1.required],
                [group2.names, group2.required],
            )
        }
        if not overlap_relationships.isdisjoint(existing_relationships):
            return context.preferred_engine.make_join_identity_relation(), False

        overlaps: Relation | None = None
        needs_refinement: bool = False
        if element1 == self.universe.commonSkyPix.name:
            (element1, element2) = (element2, element1)

        if element1 in self._overlap_tables:
            if element2 in self._overlap_tables:
                # Use commonSkyPix as an intermediary with post-query
                # refinement.
                have_overlap1_already = (
                    frozenset(self.universe[element1].dimensions.names | {self.universe.commonSkyPix.name})
                    in existing_relationships
                )
                have_overlap2_already = (
                    frozenset(self.universe[element2].dimensions.names | {self.universe.commonSkyPix.name})
                    in existing_relationships
                )
                overlap1 = context.preferred_engine.make_join_identity_relation()
                overlap2 = context.preferred_engine.make_join_identity_relation()
                if not have_overlap1_already:
                    overlap1 = self._make_common_skypix_join_relation(self.universe[element1], context)
                if not have_overlap2_already:
                    overlap2 = self._make_common_skypix_join_relation(self.universe[element2], context)
                overlaps = overlap1.join(overlap2)
                if not have_overlap1_already and not have_overlap2_already:
                    # Drop the common skypix ID column from the overlap
                    # relation we return, since we don't want that column
                    # to be mistakenly equated with any other appearance of
                    # that column, since this would mangle queries like
                    # "join visit to tract and tract to healpix10", by
                    # incorrectly requiring all visits and healpix10 pixels
                    # share common skypix pixels, not just tracts.
                    columns = set(overlaps.columns)
                    columns.remove(DimensionKeyColumnTag(self.universe.commonSkyPix.name))
                    overlaps = overlaps.with_only_columns(columns)
                needs_refinement = True
            elif element2 == self.universe.commonSkyPix.name:
                overlaps = self._make_common_skypix_join_relation(self.universe[element1], context)
        if overlaps is None:
            # In the future, there's a lot more we could try here:
            #
            # - for skypix dimensions, looking for materialized overlaps at
            #   smaller spatial scales (higher-levels) and using bit-shifting;
            #
            # - for non-skypix dimensions, looking for materialized overlaps
            #   for more finer-grained members of the same family, and then
            #   doing SELECT DISTINCT (or even tolerating duplicates) on the
            #   columns we care about (e.g. use patch overlaps to satisfy a
            #   request for tract overlaps).
            #
            # It's not obvious that's better than just telling the user to
            # materialize more overlaps, though.
            raise MissingSpatialOverlapError(
                f"No materialized overlaps for spatial join between {element1!r} and {element2!r}."
            )
        return overlaps, needs_refinement

    def make_query_joiner(self, element: DimensionElement, fields: Set[str]) -> QueryJoiner:
        if element.implied_union_target is not None:
            assert not fields, "Dimensions with implied-union storage never have fields."
            return QueryBuilder(
                self.make_query_joiner(element.implied_union_target, fields),
                columns=qt.ColumnSet(element.minimal_group).drop_implied_dimension_keys(),
                distinct=True,
            ).to_joiner()
        if not element.has_own_table:
            raise NotImplementedError(f"Cannot join dimension element {element} with no table.")
        table = self._tables[element.name]
        result = QueryJoiner(self._db, table)
        for dimension_name, column_name in zip(element.required.names, element.schema.required.names):
            result.dimension_keys[dimension_name].append(table.columns[column_name])
        result.extract_dimensions(element.implied.names)
        for field in fields:
            if field == "timespan":
                result.timespans[element.name] = self._db.getTimespanRepresentation().from_columns(
                    table.columns
                )
            else:
                result.fields[element.name][field] = table.columns[field]
        return result

    def process_query_overlaps(
        self,
        dimensions: DimensionGroup,
        predicate: qt.Predicate,
        join_operands: Iterable[DimensionGroup],
        calibration_dataset_types: Set[str],
    ) -> tuple[qt.Predicate, QueryBuilder]:
        overlaps_visitor = _CommonSkyPixMediatedOverlapsVisitor(
            self._db, dimensions, calibration_dataset_types, self._overlap_tables
        )
        new_predicate = overlaps_visitor.run(predicate, join_operands)
        return new_predicate, overlaps_visitor.builder

    def _make_relation(
        self,
        element: DimensionElement,
        context: queries.SqlQueryContext,
    ) -> Relation:
        table = self._tables[element.name]
        payload = sql.Payload[LogicalColumn](table)
        for tag, field_name in element.RecordClass.fields.columns.items():
            if field_name == "timespan":
                payload.columns_available[tag] = self._db.getTimespanRepresentation().from_columns(
                    table.columns, name=field_name
                )
            else:
                payload.columns_available[tag] = table.columns[field_name]
        return context.sql_engine.make_leaf(
            payload.columns_available.keys(),
            name=element.name,
            payload=payload,
        )

    def _make_common_skypix_join_relation(
        self,
        element: DimensionElement,
        context: queries.SqlQueryContext,
    ) -> Relation:
        """Construct a subquery expression containing overlaps between the
        common skypix dimension and the given dimension element.

        Parameters
        ----------
        element : `DimensionElement`
            Spatial dimension element whose overlaps with the common skypix
            system are represented by the returned relation.
        context : `.queries.SqlQueryContext`
            Object that manages relation engines and database-side state
            (e.g. temporary tables) for the query.

        Returns
        -------
        relation : `sql.Relation`
            Join relation.
        """
        assert element.spatial is not None, "Only called for spatial dimension elements."
        assert element.has_own_table, "Only called for dimension elements with their own tables."
        _, table = self._overlap_tables[element.name]
        payload = sql.Payload[LogicalColumn](table)
        payload.columns_available[DimensionKeyColumnTag(self.universe.commonSkyPix.name)] = (
            payload.from_clause.columns.skypix_index
        )
        for dimension_name in element.minimal_group.required:
            payload.columns_available[DimensionKeyColumnTag(dimension_name)] = payload.from_clause.columns[
                dimension_name
            ]
        payload.where.append(table.columns.skypix_system == self.universe.commonSkyPix.system.name)
        payload.where.append(table.columns.skypix_level == self.universe.commonSkyPix.level)
        leaf = context.sql_engine.make_leaf(
            payload.columns_available.keys(),
            name=f"{element.name}_{self.universe.commonSkyPix.name}_overlap",
            payload=payload,
        )
        return leaf

    @classmethod
    def currentVersions(cls) -> list[VersionTuple]:
        # Docstring inherited from VersionedExtension.
        return [_VERSION]

    @classmethod
    def _make_skypix_overlap_tables(
        cls, context: StaticTablesContext, element: DimensionElement
    ) -> tuple[sqlalchemy.Table, sqlalchemy.Table]:
        assert element.governor is not None
        summary_spec = ddl.TableSpec(
            fields=[
                ddl.FieldSpec(
                    name="skypix_system",
                    dtype=sqlalchemy.String,
                    length=16,
                    nullable=False,
                    primaryKey=True,
                ),
                ddl.FieldSpec(
                    name="skypix_level",
                    dtype=sqlalchemy.SmallInteger,
                    nullable=False,
                    primaryKey=True,
                ),
            ]
        )
        addDimensionForeignKey(summary_spec, element.governor, primaryKey=True)
        overlap_spec = ddl.TableSpec(
            fields=[
                ddl.FieldSpec(
                    name="skypix_system",
                    dtype=sqlalchemy.String,
                    length=16,
                    nullable=False,
                    primaryKey=True,
                ),
                ddl.FieldSpec(
                    name="skypix_level",
                    dtype=sqlalchemy.SmallInteger,
                    nullable=False,
                    primaryKey=True,
                ),
                # (more columns added below)
            ],
            unique=set(),
            indexes={
                # This index has the same fields as the PK, in a different
                # order, to facilitate queries that know skypix_index and want
                # to find the other element.
                ddl.IndexSpec(
                    "skypix_system",
                    "skypix_level",
                    "skypix_index",
                    *element.minimal_group.required,
                ),
            },
            foreignKeys=[
                # Foreign key to summary table.  This makes sure we don't
                # materialize any overlaps without remembering that we've done
                # so in the summary table, though it can't prevent the converse
                # of adding a summary row without adding overlap row (either of
                # those is a logic bug, of course, but we want to be defensive
                # about those).  Using ON DELETE CASCADE, it'd be very easy to
                # implement "disabling" an overlap materialization, because we
                # can just delete the summary row.
                # Note that the governor dimension column is added below, in
                # the call to addDimensionForeignKey.
                ddl.ForeignKeySpec(
                    f"{element.name}_skypix_overlap_summary",
                    source=("skypix_system", "skypix_level", element.governor.name),
                    target=("skypix_system", "skypix_level", element.governor.name),
                    onDelete="CASCADE",
                ),
            ],
        )
        # Add fields for the standard element this class manages overlaps for.
        # This is guaranteed to add a column for the governor dimension,
        # because that's a required dependency of element.
        for dimension in element.required:
            addDimensionForeignKey(overlap_spec, dimension, primaryKey=True)
        # Add field for the actual skypix index.  We do this later because I
        # think we care (at least a bit) about the order in which the primary
        # key is defined, in that we want a non-summary column like this one
        # to appear after the governor dimension column.
        overlap_spec.fields.add(
            ddl.FieldSpec(
                name="skypix_index",
                dtype=sqlalchemy.BigInteger,
                nullable=False,
                primaryKey=True,
            )
        )
        return (
            context.addTable(f"{element.name}_skypix_overlap_summary", summary_spec),
            context.addTable(f"{element.name}_skypix_overlap", overlap_spec),
        )

    @classmethod
    def _make_legacy_overlap_tables(
        cls,
        context: StaticTablesContext,
        spatial: NamedKeyDict[DatabaseTopologicalFamily, list[DimensionElement]],
    ) -> None:
        for (_, elements1), (_, elements2) in itertools.combinations(spatial.items(), 2):
            for element1, element2 in itertools.product(elements1, elements2):
                if element1 > element2:
                    (element2, element1) = (element1, element2)
                assert element1.spatial is not None and element2.spatial is not None
                assert element1.governor != element2.governor
                assert element1.governor is not None and element2.governor is not None
                summary_spec = ddl.TableSpec(fields=[])
                addDimensionForeignKey(summary_spec, element1.governor, primaryKey=True)
                addDimensionForeignKey(summary_spec, element2.governor, primaryKey=True)
                context.addTable(f"{element1.name}_{element2.name}_overlap_summary", summary_spec)
                overlap_spec = ddl.TableSpec(fields=[])
                addDimensionForeignKey(overlap_spec, element1.governor, primaryKey=True)
                addDimensionForeignKey(overlap_spec, element2.governor, primaryKey=True)
                for dimension in element1.required:
                    if dimension != element1.governor:
                        addDimensionForeignKey(overlap_spec, dimension, primaryKey=True)
                for dimension in element2.required:
                    if dimension != element2.governor:
                        addDimensionForeignKey(overlap_spec, dimension, primaryKey=True)
                context.addTable(f"{element1.name}_{element2.name}_overlap", overlap_spec)

    def _make_record_db_rows(
        self, element: DimensionElement, records: Sequence[DimensionRecord], replace: bool
    ) -> _DimensionRecordDatabaseRows:
        result = _DimensionRecordDatabaseRows()
        result.main_rows = [record.toDict() for record in records]
        if element.temporal is not None:
            TimespanReprClass = self._db.getTimespanRepresentation()
            for row in result.main_rows:
                timespan = row.pop("timespan")
                TimespanReprClass.update(timespan, result=row)
        if element.spatial is not None:
            result.overlap_insert_rows = self._compute_common_skypix_overlap_inserts(element, records)
            if replace:
                result.overlap_delete_rows = self._compute_common_skypix_overlap_deletes(records)
        if element in self.universe.governor_dimensions:
            for related_element_name in self._overlap_tables.keys():
                if self.universe[related_element_name].governor == element:
                    result.overlap_summary_rows[related_element_name] = [
                        {
                            "skypix_system": self.universe.commonSkyPix.system.name,
                            "skypix_level": self.universe.commonSkyPix.level,
                            element.name: record.dataId[element.name],
                        }
                        for record in records
                    ]
        return result

    def _compute_common_skypix_overlap_deletes(
        self, records: Sequence[DimensionRecord]
    ) -> list[dict[str, Any]]:
        return [
            {
                "skypix_system": self.universe.commonSkyPix.system.name,
                "skypix_level": self.universe.commonSkyPix.level,
                **record.dataId.required,
            }
            for record in records
        ]

    def _compute_common_skypix_overlap_inserts(
        self,
        element: DimensionElement,
        records: Sequence[DimensionRecord],
    ) -> list[dict[str, Any]]:
        _LOG.debug("Precomputing common skypix overlaps for %s.", element.name)
        overlap_records: list[dict[str, Any]] = []
        for record in records:
            if record.region is None:
                continue
            base_overlap_record = dict(record.dataId.required)
            base_overlap_record["skypix_system"] = self.universe.commonSkyPix.system.name
            base_overlap_record["skypix_level"] = self.universe.commonSkyPix.level
            for begin, end in self.universe.commonSkyPix.pixelization.envelope(record.region):
                for index in range(begin, end):
                    overlap_records.append({"skypix_index": index, **base_overlap_record})
        return overlap_records

    def _insert_overlaps(
        self,
        element: DimensionElement,
        overlap_insert_rows: list[dict[str, Any]],
        overlap_delete_rows: list[dict[str, Any]],
        skip_existing: bool = False,
    ) -> None:
        if overlap_delete_rows:
            # Since any of the new records might have replaced existing ones
            # that already have overlap records, and we don't know which, we
            # have no choice but to delete all overlaps for these records and
            # recompute them. We include the skypix_system and skypix_level
            # column values explicitly instead of just letting the query search
            # for all of those related to the given records, because they are
            # the first columns in the primary key, and hence searching with
            # them will be way faster (and we don't want to add a new index
            # just for this operation).
            _LOG.debug("Deleting old common skypix overlaps for %s.", element.name)
            self._db.delete(
                self._overlap_tables[element.name][1],
                ["skypix_system", "skypix_level"] + list(element.minimal_group.required),
                *overlap_delete_rows,
            )
        if overlap_insert_rows:
            _LOG.debug("Inserting %d new skypix overlap rows for %s.", len(overlap_insert_rows), element.name)
            if skip_existing:
                self._db.ensure(
                    self._overlap_tables[element.name][1], *overlap_insert_rows, primary_key_only=True
                )
            else:
                self._db.insert(self._overlap_tables[element.name][1], *overlap_insert_rows)
            # We have only ever put overlaps with the commonSkyPix system into
            # this table, and *probably* only ever will. But the schema leaves
            # open the possibility that we should be inserting overlaps for
            # some other skypix system, as we once thought we'd support.  In
            # case that door opens again in the future, we need to check the
            # "overlap summary" table to see if are any skypix systems other
            # than the common skypix system and raise (rolling back the entire
            # transaction) if there are.
            summary_table = self._overlap_tables[element.name][0]
            check_sql = (
                sqlalchemy.sql.select(summary_table.columns.skypix_system, summary_table.columns.skypix_level)
                .select_from(summary_table)
                .where(
                    sqlalchemy.sql.not_(
                        sqlalchemy.sql.and_(
                            summary_table.columns.skypix_system == self.universe.commonSkyPix.system.name,
                            summary_table.columns.skypix_level == self.universe.commonSkyPix.level,
                        )
                    )
                )
            )
            with self._db.query(check_sql) as sql_result:
                bad_summary_rows = sql_result.fetchall()
            if bad_summary_rows:
                bad_skypix_names = [f"{row.skypix_system}{row.skypix.level}" for row in bad_summary_rows]
                raise RuntimeError(
                    f"Data repository has overlaps between {element} and {bad_skypix_names} that "
                    "are not supported by this version of daf_butler.  Please use a newer version."
                )


@dataclasses.dataclass
class _DimensionRecordDatabaseRows:
    """Rows to be inserted into the database whenever a DimensionRecord is
    added.
    """

    main_rows: list[dict[str, Any]] = dataclasses.field(default_factory=list)
    """Rows for the dimension element table itself."""

    overlap_insert_rows: list[dict[str, Any]] = dataclasses.field(default_factory=list)
    """Rows for overlaps with the common skypix dimension."""

    overlap_delete_rows: list[dict[str, Any]] = dataclasses.field(default_factory=list)
    """Rows for overlaps with the common skypix dimension that should be
    deleted before inserting new ones.
    """

    overlap_summary_rows: dict[str, list[dict[str, Any]]] = dataclasses.field(default_factory=dict)
    """Rows that record which overlaps between skypix dimensiosn and other
    dimension elements are stored.

    This is populated when inserting governor dimension rows, with keys being
    the names of spatial dimension elements associated with that governor.
    """


class _DimensionGroupStorage:
    """Helper object that manages saved DimensionGroup definitions.

    Should generally be constructed by calling `initialize` instead of invoking
    the constructor directly.

    Parameters
    ----------
    db : `Database`
        Interface to the underlying database engine and namespace.
    idTable : `sqlalchemy.schema.Table`
        Table that just holds unique IDs for dimension graphs.
    definitionTable : `sqlalchemy.schema.Table`
        Table that maps dimension names to the IDs of the dimension graphs to
        which they belong.
    universe : `DimensionUniverse`
        All known dimensions.
    """

    def __init__(
        self,
        db: Database,
        idTable: sqlalchemy.schema.Table,
        definitionTable: sqlalchemy.schema.Table,
        universe: DimensionUniverse,
    ):
        self._db = db
        self._idTable = idTable
        self._definitionTable = definitionTable
        self._universe = universe
        self._keysByGroup: dict[DimensionGroup, int] = {universe.empty: 0}
        self._groupsByKey: dict[int, DimensionGroup] = {0: universe.empty}

    def clone(self, db: Database) -> _DimensionGroupStorage:
        """Make an independent copy of this manager instance bound to a new
        `Database` instance.

        Parameters
        ----------
        db : `Database`
            New `Database` object to use when instantiating the manager.

        Returns
        -------
        instance : `_DimensionGroupStorage`
            New manager instance with the same configuration as this instance,
            but bound to a new Database object.
        """
        return _DimensionGroupStorage(
            db=db, idTable=self._idTable, definitionTable=self._definitionTable, universe=self._universe
        )

    @classmethod
    def initialize(
        cls,
        db: Database,
        context: StaticTablesContext,
        *,
        universe: DimensionUniverse,
    ) -> _DimensionGroupStorage:
        """Construct a new instance, including creating tables if necessary.

        Parameters
        ----------
        db : `Database`
            Interface to the underlying database engine and namespace.
        context : `StaticTablesContext`
            Context object obtained from `Database.declareStaticTables`; used
            to declare any tables that should always be present.
        universe : `DimensionUniverse`
            All known dimensions.

        Returns
        -------
        storage : `_DimensionGroupStorage`
            New instance of this class.
        """
        # We need two tables just so we have one where the autoincrement key is
        # the only primary key column, as is required by (at least) SQLite.  In
        # other databases, we might be able to use a Sequence directly.
        idTable = context.addTable(
            "dimension_graph_key",
            ddl.TableSpec(
                fields=[
                    ddl.FieldSpec(
                        name="id",
                        dtype=sqlalchemy.BigInteger,
                        autoincrement=True,
                        primaryKey=True,
                    ),
                ],
            ),
        )
        definitionTable = context.addTable(
            "dimension_graph_definition",
            ddl.TableSpec(
                fields=[
                    ddl.FieldSpec(name="dimension_graph_id", dtype=sqlalchemy.BigInteger, primaryKey=True),
                    ddl.FieldSpec(name="dimension_name", dtype=sqlalchemy.Text, primaryKey=True),
                ],
                foreignKeys=[
                    ddl.ForeignKeySpec(
                        "dimension_graph_key",
                        source=("dimension_graph_id",),
                        target=("id",),
                        onDelete="CASCADE",
                    ),
                ],
            ),
        )
        return cls(db, idTable, definitionTable, universe=universe)

    def refresh(self) -> None:
        """Refresh the in-memory cache of saved DimensionGroup definitions.

        This should be done automatically whenever needed, but it can also
        be called explicitly.
        """
        dimensionNamesByKey: dict[int, set[str]] = defaultdict(set)
        with self._db.query(self._definitionTable.select()) as sql_result:
            sql_rows = sql_result.mappings().fetchall()
        for row in sql_rows:
            key = row[self._definitionTable.columns.dimension_graph_id]
            dimensionNamesByKey[key].add(row[self._definitionTable.columns.dimension_name])
        keysByGraph: dict[DimensionGroup, int] = {self._universe.empty: 0}
        graphsByKey: dict[int, DimensionGroup] = {0: self._universe.empty}
        for key, dimensionNames in dimensionNamesByKey.items():
            graph = DimensionGroup(self._universe, names=dimensionNames)
            keysByGraph[graph] = key
            graphsByKey[key] = graph
        self._groupsByKey = graphsByKey
        self._keysByGroup = keysByGraph

    def save(self, group: DimensionGroup) -> int:
        """Save a `DimensionGroup` definition to the database, allowing it to
        be retrieved later via the returned key.

        Parameters
        ----------
        group : `DimensionGroup`
            Set of dimensions to save.

        Returns
        -------
        key : `int`
            Integer used as the unique key for this `DimensionGroup` in the
            database.
        """
        key = self._keysByGroup.get(group)
        if key is not None:
            return key
        # Lock tables and then refresh to guard against races where some other
        # process is trying to register the exact same dimension graph.  This
        # is probably not the most efficient way to do it, but it should be a
        # rare operation, especially since the short-circuit above will usually
        # work in long-lived data repositories.
        with self._db.transaction(lock=[self._idTable, self._definitionTable]):
            self.refresh()
            key = self._keysByGroup.get(group)
            if key is None:
                (key,) = self._db.insert(self._idTable, {}, returnIds=True)  # type: ignore
                self._db.insert(
                    self._definitionTable,
                    *[{"dimension_graph_id": key, "dimension_name": name} for name in group.required],
                )
            self._keysByGroup[group] = key
            self._groupsByKey[key] = group
        return key

    def load(self, key: int) -> DimensionGroup:
        """Retrieve a `DimensionGroup` that was previously saved in the
        database.

        Parameters
        ----------
        key : `int`
            Integer used as the unique key for this `DimensionGroup` in the
            database.

        Returns
        -------
        graph : `DimensionGroup`
            Retrieved graph.
        """
        graph = self._groupsByKey.get(key)
        if graph is None:
            self.refresh()
            graph = self._groupsByKey[key]
        return graph


class _CommonSkyPixMediatedOverlapsVisitor(OverlapsVisitor):
    def __init__(
        self,
        db: Database,
        dimensions: DimensionGroup,
        calibration_dataset_types: Set[str],
        overlap_tables: Mapping[str, tuple[sqlalchemy.Table, sqlalchemy.Table]],
    ):
        super().__init__(dimensions, calibration_dataset_types)
        self.builder: QueryBuilder = QueryJoiner(db).to_builder(qt.ColumnSet(dimensions))
        self.common_skypix = dimensions.universe.commonSkyPix
        self.overlap_tables: Mapping[str, tuple[sqlalchemy.Table, sqlalchemy.Table]] = overlap_tables
        self.common_skypix_overlaps_done: set[DatabaseDimensionElement] = set()

    def visit_spatial_constraint(
        self,
        element: DimensionElement,
        region: Region,
        flags: PredicateVisitFlags,
    ) -> qt.Predicate | None:
        # Reject spatial constraints that are nested inside OR or NOT, because
        # the postprocessing needed for those would be a lot harder.
        if flags & PredicateVisitFlags.INVERTED or flags & PredicateVisitFlags.HAS_OR_SIBLINGS:
            raise NotImplementedError(
                "Spatial overlap constraints nested inside OR or NOT are not supported."
            )
        # Delegate to super just because that's good practice with
        # OverlapVisitor.
        super().visit_spatial_constraint(element, region, flags)
        match element:
            case DatabaseDimensionElement():
                # If this is a database dimension element like tract, patch, or
                # visit, we need to:
                # - join in the common skypix overlap table for this element;
                # - constrain the common skypix index to be inside the
                #   ranges that overlap the region as a SQL where clause;
                # - add postprocessing to reject rows where the database
                #   dimension element's region doesn't actually overlap the
                #   region.
                self.builder.postprocessing.spatial_where_filtering.append((element, region))
                if self.common_skypix.name in self.dimensions:
                    # The common skypix dimension should be part of the query
                    # as a first-class dimension, so we can join in the overlap
                    # table directly, and fall through to the end of this
                    # function to construct a Predicate that will turn into the
                    # SQL WHERE clause we want.
                    self._join_common_skypix_overlap(element)
                    skypix = self.common_skypix
                else:
                    # We need to hide the common skypix dimension from the
                    # larger query, so we make a subquery out of the overlap
                    # table that embeds the SQL WHERE clause we want and then
                    # projects out that dimension (with SELECT DISTINCT, to
                    # avoid introducing duplicate rows into the larger query).
                    joiner = self._make_common_skypix_overlap_joiner(element)
                    sql_where_or: list[sqlalchemy.ColumnElement[bool]] = []
                    sql_skypix_col = joiner.dimension_keys[self.common_skypix.name][0]
                    for begin, end in self.common_skypix.pixelization.envelope(region):
                        sql_where_or.append(sqlalchemy.and_(sql_skypix_col >= begin, sql_skypix_col < end))
                    joiner.where(sqlalchemy.or_(*sql_where_or))
                    self.builder.join(
                        joiner.to_builder(
                            qt.ColumnSet(element.minimal_group).drop_implied_dimension_keys(), distinct=True
                        ).to_joiner()
                    )
                    # Short circuit here since the SQL WHERE clause has already
                    # been embedded in the subquery.
                    return qt.Predicate.from_bool(True)
            case SkyPixDimension():
                # If this is a skypix dimension, we can do a index-in-ranges
                # test directly on that dimension.  Note that this doesn't on
                # its own guarantee the skypix dimension column will be in the
                # query; that'll be the job of the DirectQueryDriver to sort
                # out (generally this will require a dataset using that skypix
                # dimension to be joined in, unless this is the common skypix
                # system).
                assert (
                    element.name in self.dimensions
                ), "QueryTree guarantees dimensions are expanded when constraints are added."
                skypix = element
            case _:
                raise NotImplementedError(
                    f"Spatial overlap constraint for dimension {element} not supported."
                )
        # Convert the region-overlap constraint into a skypix
        # index range-membership constraint in SQL.
        result = qt.Predicate.from_bool(False)
        skypix_col_ref = qt.DimensionKeyReference.model_construct(dimension=skypix)
        for begin, end in skypix.pixelization.envelope(region):
            result = result.logical_or(qt.Predicate.in_range(skypix_col_ref, start=begin, stop=end))
        return result

    def visit_spatial_join(
        self, a: DimensionElement, b: DimensionElement, flags: PredicateVisitFlags
    ) -> qt.Predicate | None:
        # Reject spatial joins that are nested inside OR or NOT, because the
        # postprocessing needed for those would be a lot harder.
        if flags & PredicateVisitFlags.INVERTED or flags & PredicateVisitFlags.HAS_OR_SIBLINGS:
            raise UnimplementedQueryError("Spatial overlap joins nested inside OR or NOT are not supported.")
        # Delegate to super to check for invalid joins and record this
        # "connection" for use when seeing whether to add an automatic join
        # later.
        super().visit_spatial_join(a, b, flags)
        match (a, b):
            case (self.common_skypix, DatabaseDimensionElement() as b):
                self._join_common_skypix_overlap(b)
            case (DatabaseDimensionElement() as a, self.common_skypix):
                self._join_common_skypix_overlap(a)
            case (DatabaseDimensionElement() as a, DatabaseDimensionElement() as b):
                if self.common_skypix.name in self.dimensions:
                    # We want the common skypix dimension to appear in the
                    # query as a first-class dimension, so just join in the
                    # two overlap tables directly.
                    self._join_common_skypix_overlap(a)
                    self._join_common_skypix_overlap(b)
                else:
                    # We do not want the common skypix system to appear in the
                    # query or cause duplicate rows, so we join the two overlap
                    # tables in a subquery that projects out the common skypix
                    # index column with SELECT DISTINCT.

                    self.builder.join(
                        self._make_common_skypix_overlap_joiner(a)
                        .join(self._make_common_skypix_overlap_joiner(b))
                        .to_builder(
                            qt.ColumnSet(a.minimal_group | b.minimal_group).drop_implied_dimension_keys(),
                            distinct=True,
                        )
                        .to_joiner()
                    )
                # In both cases we add postprocessing to check that the regions
                # really do overlap, since overlapping the same common skypix
                # tile is necessary but not sufficient for that.
                self.builder.postprocessing.spatial_join_filtering.append((a, b))
            case _:
                raise UnimplementedQueryError(f"Unsupported combination for spatial join: {a, b}.")
        return qt.Predicate.from_bool(True)

    def _join_common_skypix_overlap(self, element: DatabaseDimensionElement) -> None:
        if element not in self.common_skypix_overlaps_done:
            self.builder.join(self._make_common_skypix_overlap_joiner(element))
            self.common_skypix_overlaps_done.add(element)

    def _make_common_skypix_overlap_joiner(self, element: DatabaseDimensionElement) -> QueryJoiner:
        _, overlap_table = self.overlap_tables[element.name]
        return (
            QueryJoiner(self.builder.joiner.db, overlap_table)
            .extract_dimensions(element.required.names, skypix_index=self.common_skypix.name)
            .where(
                sqlalchemy.and_(
                    overlap_table.c.skypix_system == self.common_skypix.system.name,
                    overlap_table.c.skypix_level == self.common_skypix.level,
                )
            )
        )

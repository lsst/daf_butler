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

__all__ = ["TableDimensionRecordStorage"]

import dataclasses
import logging
from collections.abc import Mapping, Sequence, Set
from typing import Any

import sqlalchemy
from lsst.daf.relation import Join, Relation, sql

from ...core import (
    DatabaseDimensionElement,
    DataCoordinate,
    DimensionElement,
    DimensionKeyColumnTag,
    DimensionRecord,
    GovernorDimension,
    LogicalColumn,
    NamedKeyMapping,
    SkyPixDimension,
    TimespanDatabaseRepresentation,
    addDimensionForeignKey,
    ddl,
)
from .. import queries
from ..interfaces import (
    Database,
    DatabaseDimensionOverlapStorage,
    DatabaseDimensionRecordStorage,
    GovernorDimensionRecordStorage,
    StaticTablesContext,
)

_LOG = logging.getLogger(__name__)


MAX_FETCH_CHUNK = 1000
"""Maximum number of data IDs we fetch records at a time.

Barring something database-engine-specific, this sets the size of the actual
SQL query, not just the number of result rows, because the only way to query
for multiple data IDs in a single SELECT query via SQLAlchemy is to have an OR
term in the WHERE clause for each one.
"""


class TableDimensionRecordStorage(DatabaseDimensionRecordStorage):
    """A record storage implementation uses a regular database table.

    Parameters
    ----------
    db : `Database`
        Interface to the database engine and namespace that will hold these
        dimension records.
    element : `DatabaseDimensionElement`
        The element whose records this storage will manage.
    table : `sqlalchemy.schema.Table`
        The logical table for the element.
    skypix_overlap_tables : `_SkyPixOverlapTables`, optional
        Object that manages the tables that hold materialized spatial overlap
        joins to skypix dimensions.  Should be `None` if (and only if)
        ``element.spatial is None``.
    """

    def __init__(
        self,
        db: Database,
        element: DatabaseDimensionElement,
        *,
        table: sqlalchemy.schema.Table,
        skypix_overlap_tables: _SkyPixOverlapTables | None = None,
    ):
        self._db = db
        self._table = table
        self._element = element
        self._fetchColumns: dict[str, sqlalchemy.sql.ColumnElement] = {
            dimension.name: self._table.columns[name]
            for dimension, name in zip(
                self._element.dimensions, self._element.RecordClass.fields.dimensions.names
            )
        }
        self._skypix_overlap_tables = skypix_overlap_tables
        self._otherOverlaps: dict[str, DatabaseDimensionOverlapStorage] = {}

    @classmethod
    def initialize(
        cls,
        db: Database,
        element: DatabaseDimensionElement,
        *,
        context: StaticTablesContext | None = None,
        config: Mapping[str, Any],
        governors: NamedKeyMapping[GovernorDimension, GovernorDimensionRecordStorage],
        view_target: DatabaseDimensionRecordStorage | None = None,
    ) -> DatabaseDimensionRecordStorage:
        # Docstring inherited from DatabaseDimensionRecordStorage.
        assert view_target is None, f"Storage for {element} is not a view."
        spec = element.RecordClass.fields.makeTableSpec(TimespanReprClass=db.getTimespanRepresentation())
        if context is not None:
            table = context.addTable(element.name, spec)
        else:
            table = db.ensureTableExists(element.name, spec)
        if element.spatial is not None:
            governor = governors[element.spatial.governor]
            skypix_overlap_tables = _SkyPixOverlapTables.initialize(db, element, context=context)
            result = cls(db, element, table=table, skypix_overlap_tables=skypix_overlap_tables)
            governor.registerInsertionListener(result._on_governor_insert)
            return result
        else:
            return cls(db, element, table=table)

    @property
    def element(self) -> DatabaseDimensionElement:
        # Docstring inherited from DimensionRecordStorage.element.
        return self._element

    def clearCaches(self) -> None:
        # Docstring inherited from DimensionRecordStorage.clearCaches.
        pass

    def make_relation(self, context: queries.SqlQueryContext) -> Relation:
        # Docstring inherited from DimensionRecordStorage.
        payload = self._build_sql_payload(self._table, context.column_types)
        return context.sql_engine.make_leaf(
            payload.columns_available.keys(),
            name=self.element.name,
            payload=payload,
        )

    def fetch_one(self, data_id: DataCoordinate, context: queries.SqlQueryContext) -> DimensionRecord | None:
        # Docstring inherited from DimensionRecordStorage.
        from .. import queries

        relation = self.join(context.make_initial_relation(), Join(), context).with_rows_satisfying(
            context.make_data_coordinate_predicate(data_id, full=False)
        )[0:1]
        rows = list(context.fetch_iterable(relation))
        if not rows:
            return None
        reader = queries.DimensionRecordReader(self._element)
        return reader.read(rows[0])

    def insert(self, *records: DimensionRecord, replace: bool = False, skip_existing: bool = False) -> None:
        # Docstring inherited from DimensionRecordStorage.insert.
        elementRows = [record.toDict() for record in records]
        if self.element.temporal is not None:
            TimespanReprClass = self._db.getTimespanRepresentation()
            for row in elementRows:
                timespan = row.pop(TimespanDatabaseRepresentation.NAME)
                TimespanReprClass.update(timespan, result=row)
        with self._db.transaction():
            if replace:
                self._db.replace(self._table, *elementRows)
            elif skip_existing:
                self._db.ensure(self._table, *elementRows, primary_key_only=True)
            else:
                self._db.insert(self._table, *elementRows)
            if self._skypix_overlap_tables is not None:
                self._insert_skypix_overlaps(records, replace=replace, skip_existing=skip_existing)

    def sync(self, record: DimensionRecord, update: bool = False) -> bool | dict[str, Any]:
        # Docstring inherited from DimensionRecordStorage.sync.
        compared = record.toDict()
        keys = {}
        for name in record.fields.required.names:
            keys[name] = compared.pop(name)
        if self.element.temporal is not None:
            TimespanReprClass = self._db.getTimespanRepresentation()
            timespan = compared.pop(TimespanDatabaseRepresentation.NAME)
            TimespanReprClass.update(timespan, result=compared)
        with self._db.transaction():
            _, inserted_or_updated = self._db.sync(
                self._table,
                keys=keys,
                compared=compared,
                update=update,
            )
            if inserted_or_updated and self._skypix_overlap_tables is not None:
                if inserted_or_updated is True:
                    # Inserted a new row, so we just need to insert new overlap
                    # rows.
                    self._insert_skypix_overlaps([record])
                elif "region" in inserted_or_updated:
                    # Updated the region, so we need to delete old overlap rows
                    # and insert new ones.
                    self._insert_skypix_overlaps([record], replace=True)
                # We updated something other than a region.
        return inserted_or_updated

    def digestTables(self) -> list[sqlalchemy.schema.Table]:
        # Docstring inherited from DimensionRecordStorage.digestTables.
        result = [self._table]
        if self._skypix_overlap_tables is not None:
            result.append(self._skypix_overlap_tables.summary)
            result.append(self._skypix_overlap_tables.overlaps)
        return result

    def connect(self, overlaps: DatabaseDimensionOverlapStorage) -> None:
        # Docstring inherited from DatabaseDimensionRecordStorage.
        (other,) = set(overlaps.elements) - {self.element}
        self._otherOverlaps[other.name] = overlaps

    def make_spatial_join_relation(
        self,
        other: DimensionElement,
        context: queries.SqlQueryContext,
        governor_constraints: Mapping[str, Set[str]],
    ) -> Relation | None:
        # Docstring inherited from DatabaseDimensionRecordStorage.
        match other:
            case SkyPixDimension() as skypix:
                return self._make_skypix_join_relation(skypix, context)
            case DatabaseDimensionElement() as other:
                return self._otherOverlaps[other.name].make_relation(context, governor_constraints)
            case _:
                raise TypeError(f"Unexpected dimension element type for spatial join: {other}.")

    def _on_governor_insert(self, record: DimensionRecord) -> None:
        """A `GovernorDimensionRecordStorage.registerInsertionListener`
        callback for this element.

        Parameters
        ----------
        record : `DimensionRecord`
            Record for this element's governor dimension.
        """
        # We need to enable overlaps between this new governor dimension value
        # and the common skypix dimension to record that we materialize
        # overlaps for that combination.  Foreign keys guarantee that there
        # can't be any rows of this storage object's own element with that
        # governor value yet, so we know there's nothing to insert into the
        # overlaps table yet.
        skypix = self.element.universe.commonSkyPix
        assert self._element.spatial is not None, "Only called for spatial dimension elements."
        assert (
            self._skypix_overlap_tables is not None
        ), "Spatial dimension elements always have skypix overlap tables."
        governor = self._element.spatial.governor
        self._db.sync(
            self._skypix_overlap_tables.summary,
            keys={
                "skypix_system": skypix.system.name,
                "skypix_level": skypix.level,
                governor.name: record.dataId[governor.name],
            },
        )

    def _insert_skypix_overlaps(
        self, records: Sequence[DimensionRecord], replace: bool = False, skip_existing: bool = False
    ) -> None:
        """Compute and insert overlap rows between this dimesion element and
        the common skypix system.

        Parameters
        ----------
        records : `Sequence` [ `DimensionRecord` ]
            Records for ``self.element`` that are being inserted.
        replace : `bool`, optional
            If `True`, the given records are being inserted in a mode that may
            replace existing records, and hence overlap rows may need to be
            replaced as well.
        skip_existing : `bool`, optional
            If `True`, the given records are being inserted in a mode that
            ignored existing records with the same data ID, and hence overlap
            rows need to be inserted this way as well.
        """
        assert self._element.spatial is not None, "Only called for spatial dimension elements."
        assert (
            self._skypix_overlap_tables is not None
        ), "Spatial dimension elements always have skypix overlap tables."
        # At present, only overlaps with the "commonSkyPix" system can be
        # materialized, so we just compute and insert overlaps with those.
        #
        # To guard against this code being used with a data repository in which
        # newer code has enabled other overlaps, we check afterwards that the
        # summary table only contains commonSkyPix for all of these governor
        # dimensions.  In the future, we'll have to think about whether we need
        # some table locking to guarantee consistency for those other overlaps
        # if the summary table is updated at the same time as records are
        # being inserted.  This should happen within the same transaction
        # (handled by the caller) so that previous inserts get rolled back.
        skypix = self._element.universe.commonSkyPix
        if replace:
            # Since any of the new records might have replaced existing ones
            # that already have overlap records, and we don't know which, we
            # have no choice but to delete all overlaps for these records and
            # recompute them.
            # We include the skypix_system and skypix_level column values
            # explicitly instead of just letting the query search for all
            # of those related to the given records, because they are the
            # first columns in the primary key, and hence searching with
            # them will be way faster (and we don't want to add a new index
            # just for this operation).
            to_delete: list[dict[str, Any]] = [
                {"skypix_system": skypix.system.name, "skypix_level": skypix.level, **record.dataId.byName()}
                for record in records
            ]
            _LOG.debug("Deleting old common skypix overlaps for %s.", self.element.name)
            self._db.delete(
                self._skypix_overlap_tables.overlaps,
                ["skypix_system", "skypix_level"] + list(self.element.graph.required.names),
                *to_delete,
            )
        _LOG.debug("Precomputing common skypix overlaps for %s.", self.element.name)
        overlap_records: list[dict[str, Any]] = []
        for record in records:
            if record.region is None:
                continue
            base_overlap_record = record.dataId.byName()
            base_overlap_record["skypix_system"] = skypix.system.name
            base_overlap_record["skypix_level"] = skypix.level
            for begin, end in skypix.pixelization.envelope(record.region):
                for index in range(begin, end):
                    overlap_records.append({"skypix_index": index, **base_overlap_record})
        _LOG.debug("Inserting %d new skypix overlap rows for %s.", len(overlap_records), self.element.name)
        if skip_existing:
            self._db.ensure(self._skypix_overlap_tables.overlaps, *overlap_records, primary_key_only=True)
        else:
            self._db.insert(self._skypix_overlap_tables.overlaps, *overlap_records)
        # Finally we check for non-commonSkyPix values in the summary table, as
        # noted above.
        summary = self._skypix_overlap_tables.summary
        check_sql = (
            sqlalchemy.sql.select([summary.columns.skypix_system, summary.columns.skypix_level])
            .select_from(summary)
            .where(
                sqlalchemy.sql.not_(
                    sqlalchemy.sql.and_(
                        summary.columns.skypix_system == skypix.system.name,
                        summary.columns.skypix_level == skypix.level,
                    )
                )
            )
        )
        with self._db.query(check_sql) as sql_result:
            bad_summary_rows = sql_result.fetchall()
        if bad_summary_rows:
            bad_skypix_names = [f"{row.skypix_system}{row.skypix.level}" for row in bad_summary_rows]
            raise RuntimeError(
                f"Data repository has overlaps between {self._element} and {bad_skypix_names} that "
                "are not supported by this version of daf_butler.  Please use a newer version."
            )

    def _make_skypix_join_relation(
        self,
        skypix: SkyPixDimension,
        context: queries.SqlQueryContext,
    ) -> Relation | None:
        """Construct a subquery expression containing overlaps between the
        given skypix dimension and governor values.

        Parameters
        ----------
        skypix : `SkyPixDimension`
            The skypix dimension (system and level) for which overlaps should
            be materialized.
        context : `.queries.SqlQueryContext`
            Object that manages relation engines and database-side state
            (e.g. temporary tables) for the query.

        Returns
        -------
        relation : `sql.Relation` or `None`
            Join relation, or `None` if overlaps are not materialized for this
            combination of dimensions.
        """
        assert self._element.spatial is not None, "Only called for spatial dimension elements."
        assert (
            self._skypix_overlap_tables is not None
        ), "Spatial dimension elements always have skypix overlap tables."
        if skypix != self._element.universe.commonSkyPix:
            return None
        table = self._skypix_overlap_tables.overlaps
        payload = sql.Payload[LogicalColumn](table)
        payload.columns_available[
            DimensionKeyColumnTag(skypix.name)
        ] = payload.from_clause.columns.skypix_index
        for dimension_name in self.element.graph.required.names:
            payload.columns_available[DimensionKeyColumnTag(dimension_name)] = payload.from_clause.columns[
                dimension_name
            ]
        payload.where.append(table.columns.skypix_system == skypix.system.name)
        payload.where.append(table.columns.skypix_level == skypix.level)
        leaf = context.sql_engine.make_leaf(
            payload.columns_available.keys(),
            name=f"{self.element.name}_{skypix.name}_overlap",
            payload=payload,
        )
        return leaf


@dataclasses.dataclass
class _SkyPixOverlapTables:
    """A helper object for `TableDimensionRecordStorage` that manages the
    tables for materialized overlaps with skypix dimensions.

    New instances should be constructed by calling `initialize`, not by calling
    the dataclass-provided constructor directly.

    Notes
    -----
    This class (and the related methods in TableDimensionRecordStorage) can in
    principle manage overlaps between a database dimension element and any
    skypix dimension, but at present it is only being used to manage
    relationships with the special ``commonSkyPix`` dimension, because that's
    all the query system uses.  Eventually, we expect to require users to
    explicitly materialize more relationships.

    Other possible future improvements include:

     - allowing finer-grained skypix dimensions to provide overlap rows for
       coarser ones, by dividing indices by powers of 4 (and possibly doing
       ``SELECT DISTINCT`` in the subquery to remove duplicates);

     - allowing finer-grained database elements (e.g. patch) to provide overlap
       rows for coarser ones (e.g. tract), by ignoring irrelevant columns (e.g.
       the patch IDs) in the subquery (again, possible with ``SELECT
       DISTINCT``).

    But there's no point to doing any of that until the query system can figure
    out how best to ask for overlap rows when an exact match isn't available.
    """

    summary: sqlalchemy.schema.Table
    """Table that records which governor value / skypix combinations have
    materialized overlaps.
    """

    overlaps: sqlalchemy.schema.Table
    """Table that actually holds overlap rows.
    """

    @classmethod
    def initialize(
        cls,
        db: Database,
        element: DatabaseDimensionElement,
        *,
        context: StaticTablesContext | None,
    ) -> _SkyPixOverlapTables:
        """Construct a new instance, creating tables as needed.

        Parameters
        ----------
        db : `Database`
            Interface to the underlying database engine and namespace.
        element : `DatabaseDimensionElement`
            Dimension element whose overlaps are to be managed.
        context : `StaticTablesContext`, optional
            If provided, an object to use to create any new tables.  If not
            provided, ``db.ensureTableExists`` should be used instead.
        """
        if context is not None:
            op = context.addTable
        else:
            op = db.ensureTableExists
        summary = op(
            cls._SUMMARY_TABLE_NAME_SPEC.format(element=element),
            cls._makeSummaryTableSpec(element),
        )
        overlaps = op(
            cls._OVERLAP_TABLE_NAME_SPEC.format(element=element),
            cls._makeOverlapTableSpec(element),
        )
        return cls(summary=summary, overlaps=overlaps)

    _SUMMARY_TABLE_NAME_SPEC = "{element.name}_skypix_overlap_summary"

    @classmethod
    def _makeSummaryTableSpec(cls, element: DatabaseDimensionElement) -> ddl.TableSpec:
        """Create a specification for the table that records which combinations
        of skypix dimension and governor value have materialized overlaps.

        Parameters
        ----------
        element : `DatabaseDimensionElement`
            Dimension element whose overlaps are to be managed.

        Returns
        -------
        tableSpec : `ddl.TableSpec`
            Table specification.
        """
        assert element.spatial is not None
        tableSpec = ddl.TableSpec(
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
        addDimensionForeignKey(tableSpec, element.spatial.governor, primaryKey=True)
        return tableSpec

    _OVERLAP_TABLE_NAME_SPEC = "{element.name}_skypix_overlap"

    @classmethod
    def _makeOverlapTableSpec(cls, element: DatabaseDimensionElement) -> ddl.TableSpec:
        """Create a specification for the table that holds materialized
        overlap rows.

        Parameters
        ----------
        element : `DatabaseDimensionElement`
            Dimension element whose overlaps are to be managed.

        Returns
        -------
        tableSpec : `ddl.TableSpec`
            Table specification.
        """
        assert element.spatial is not None
        tableSpec = ddl.TableSpec(
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
                    *element.graph.required.names,
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
                    cls._SUMMARY_TABLE_NAME_SPEC.format(element=element),
                    source=("skypix_system", "skypix_level", element.spatial.governor.name),
                    target=("skypix_system", "skypix_level", element.spatial.governor.name),
                    onDelete="CASCADE",
                ),
            ],
        )
        # Add fields for the standard element this class manages overlaps for.
        # This is guaranteed to add a column for the governor dimension,
        # because that's a required dependency of element.
        for dimension in element.required:
            addDimensionForeignKey(tableSpec, dimension, primaryKey=True)
        # Add field for the actual skypix index.  We do this later because I
        # think we care (at least a bit) about the order in which the primary
        # key is defined, in that we want a non-summary column like this one
        # to appear after the governor dimension column.
        tableSpec.fields.add(
            ddl.FieldSpec(
                name="skypix_index",
                dtype=sqlalchemy.BigInteger,
                nullable=False,
                primaryKey=True,
            )
        )
        return tableSpec

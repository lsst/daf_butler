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

import itertools
import logging
import warnings
from collections import defaultdict
from typing import AbstractSet, Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Set, Union

import sqlalchemy

from ...core import (
    DatabaseDimensionElement,
    DataCoordinate,
    DataCoordinateIterable,
    DimensionElement,
    DimensionRecord,
    GovernorDimension,
    NamedKeyDict,
    NamedKeyMapping,
    NamedValueSet,
    SimpleQuery,
    SkyPixDimension,
    SkyPixSystem,
    SpatialRegionDatabaseRepresentation,
    TimespanDatabaseRepresentation,
    addDimensionForeignKey,
    ddl,
)
from ..interfaces import (
    Database,
    DatabaseDimensionOverlapStorage,
    DatabaseDimensionRecordStorage,
    GovernorDimensionRecordStorage,
    StaticTablesContext,
)
from ..queries import QueryBuilder
from ..wildcards import Ellipsis, EllipsisType

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
    skyPixOverlap : `_SkyPixOverlapStorage`, optional
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
        skyPixOverlap: Optional[_SkyPixOverlapStorage] = None,
    ):
        self._db = db
        self._table = table
        self._element = element
        self._fetchColumns: Dict[str, sqlalchemy.sql.ColumnElement] = {
            dimension.name: self._table.columns[name]
            for dimension, name in zip(
                self._element.dimensions, self._element.RecordClass.fields.dimensions.names
            )
        }
        self._skyPixOverlap = skyPixOverlap
        self._otherOverlaps: List[DatabaseDimensionOverlapStorage] = []

    @classmethod
    def initialize(
        cls,
        db: Database,
        element: DatabaseDimensionElement,
        *,
        context: Optional[StaticTablesContext] = None,
        config: Mapping[str, Any],
        governors: NamedKeyMapping[GovernorDimension, GovernorDimensionRecordStorage],
    ) -> DatabaseDimensionRecordStorage:
        # Docstring inherited from DatabaseDimensionRecordStorage.
        spec = element.RecordClass.fields.makeTableSpec(
            RegionReprClass=db.getSpatialRegionRepresentation(),
            TimespanReprClass=db.getTimespanRepresentation(),
        )
        if context is not None:
            table = context.addTable(element.name, spec)
        else:
            table = db.ensureTableExists(element.name, spec)
        skyPixOverlap: Optional[_SkyPixOverlapStorage]
        if element.spatial is not None:
            governor = governors[element.spatial.governor]
            skyPixOverlap = _SkyPixOverlapStorage.initialize(
                db,
                element,
                context=context,
                governor=governor,
            )
            result = cls(db, element, table=table, skyPixOverlap=skyPixOverlap)

            # Whenever anyone inserts a new governor dimension value, we want
            # to enable overlaps for that value between this element and
            # commonSkyPix.
            def callback(record: DimensionRecord) -> None:
                skyPixOverlap.enable(  # type: ignore
                    result,
                    element.universe.commonSkyPix,
                    getattr(record, element.spatial.governor.primaryKey.name),  # type: ignore
                )

            governor.registerInsertionListener(callback)
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

    def join(
        self,
        builder: QueryBuilder,
        *,
        regions: Optional[NamedKeyDict[DimensionElement, SpatialRegionDatabaseRepresentation]] = None,
        timespans: Optional[NamedKeyDict[DimensionElement, TimespanDatabaseRepresentation]] = None,
    ) -> None:
        # Docstring inherited from DimensionRecordStorage.
        if regions is not None:
            dimensions = NamedValueSet(self.element.required)
            dimensions.add(self.element.universe.commonSkyPix)
            assert self._skyPixOverlap is not None
            builder.joinTable(
                self._skyPixOverlap.select(self.element.universe.commonSkyPix, Ellipsis),
                dimensions,
            )
            regionsInTable = self._db.getSpatialRegionRepresentation().fromSelectable(self._table)
            regions[self.element] = regionsInTable
        joinOn = builder.startJoin(
            self._table, self.element.dimensions, self.element.RecordClass.fields.dimensions.names
        )
        if timespans is not None:
            timespanInTable = self._db.getTimespanRepresentation().fromSelectable(self._table)
            for timespanInQuery in timespans.values():
                joinOn.append(timespanInQuery.overlaps(timespanInTable))
            timespans[self.element] = timespanInTable
        builder.finishJoin(self._table, joinOn)
        return self._table

    def fetch(self, dataIds: DataCoordinateIterable) -> Iterable[DimensionRecord]:
        # Docstring inherited from DimensionRecordStorage.fetch.
        RecordClass = self.element.RecordClass
        query = SimpleQuery()
        query.columns.extend(self._table.columns[name] for name in RecordClass.fields.standard.names)
        if self.element.spatial is not None:
            query.columns.append(self._table.columns["region"])
        if self.element.temporal is not None:
            TimespanReprClass = self._db.getTimespanRepresentation()
            query.columns.extend(self._table.columns[name] for name in TimespanReprClass.getFieldNames())
        query.join(self._table)
        dataIds.constrain(query, lambda name: self._fetchColumns[name])
        with warnings.catch_warnings():
            # Some of our generated queries may contain cartesian joins, this
            # is not a serious issue as it is properly constrained, so we want
            # to suppress sqlalchemy warnings.
            warnings.filterwarnings(
                "ignore",
                message="SELECT statement has a cartesian product",
                category=sqlalchemy.exc.SAWarning,
            )
            for row in self._db.query(query.combine()):
                values = row._asdict()
                if self.element.temporal is not None:
                    values[TimespanDatabaseRepresentation.NAME] = TimespanReprClass.extract(values)
                yield RecordClass(**values)

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
            if self._skyPixOverlap is not None:
                self._skyPixOverlap.insert(records, replace=replace)

    def sync(self, record: DimensionRecord, update: bool = False) -> Union[bool, Dict[str, Any]]:
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
            if inserted_or_updated and self._skyPixOverlap is not None:
                if inserted_or_updated is True:
                    # Inserted a new row, so we just need to insert new overlap
                    # rows.
                    self._skyPixOverlap.insert([record])
                elif "region" in inserted_or_updated:
                    # Updated the region, so we need to delete old overlap rows
                    # and insert new ones.
                    # (mypy should be able to tell that inserted_or_updated
                    # must be a dict if we get to this clause, but it can't)
                    self._skyPixOverlap.insert([record], replace=True)
                # We updated something other than a region.
        return inserted_or_updated

    def digestTables(self) -> Iterable[sqlalchemy.schema.Table]:
        # Docstring inherited from DimensionRecordStorage.digestTables.
        result = [self._table]
        if self._skyPixOverlap is not None:
            result.extend(self._skyPixOverlap.digestTables())
        return result

    def connect(self, overlaps: DatabaseDimensionOverlapStorage) -> None:
        # Docstring inherited from DatabaseDimensionRecordStorage.
        self._otherOverlaps.append(overlaps)


class _SkyPixOverlapStorage:
    """A helper object for `TableDimensionRecordStorage` that manages its
    materialized overlaps with skypix dimensions.

    New instances should be constructed by calling `initialize`, not by calling
    the constructor directly.

    Parameters
    ----------
    db : `Database`
        Interface to the underlying database engine and namespace.
    element : `DatabaseDimensionElement`
        Dimension element whose overlaps are to be managed.
    summaryTable : `sqlalchemy.schema.Table`
        Table that records which combinations of skypix dimensions and
        governor dimension values have materialized overlap rows.
    overlapTable : `sqlalchemy.schema.Table`
        Table containing the actual materialized overlap rows.
    governor : `GovernorDimensionRecordStorage`
        Record storage backend for this element's governor dimension.

    Notes
    -----
    This class (and most importantly, the tables it relies on) can in principle
    manage overlaps between with any skypix dimension, but at present it is
    only being used to manage relationships with the special ``commonSkyPix``
    dimension, because that's all the query system uses.  Eventually, we expect
    to require users to explicitly materialize all relationships they will
    want to use in queries.

    Other possible future improvements include:

     - allowing finer-grained skypix dimensions to provide overlap rows for
       coarser ones, by dividing indices by powers of 4 (and possibly doing
       ``SELECT DISTINCT`` in the subquery to remove duplicates);

     - allowing finer-grained database elements (e.g. patch) to provide overlap
       rows for coarser ones (e.g. tract), by ignoring irrelevant columns
       (e.g. the patch IDs) in the subquery (again, possible with
       ``SELECT DISTINCT``).

    But there's no point to doing any of that until the query system can
    figure out how best to ask for overlap rows when an exact match isn't
    available.
    """

    def __init__(
        self,
        db: Database,
        element: DatabaseDimensionElement,
        summaryTable: sqlalchemy.schema.Table,
        overlapTable: sqlalchemy.schema.Table,
        governor: GovernorDimensionRecordStorage,
    ):
        self._db = db
        self.element = element
        assert element.spatial is not None
        self._summaryTable = summaryTable
        self._overlapTable = overlapTable
        self._governor = governor

    @classmethod
    def initialize(
        cls,
        db: Database,
        element: DatabaseDimensionElement,
        *,
        context: Optional[StaticTablesContext],
        governor: GovernorDimensionRecordStorage,
    ) -> _SkyPixOverlapStorage:
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
        governor : `GovernorDimensionRecordStorage`
            Record storage backend for this element's governor dimension.
        """
        if context is not None:
            op = context.addTable
        else:
            op = db.ensureTableExists
        summaryTable = op(
            cls._SUMMARY_TABLE_NAME_SPEC.format(element=element),
            cls._makeSummaryTableSpec(element),
        )
        overlapTable = op(
            cls._OVERLAP_TABLE_NAME_SPEC.format(element=element),
            cls._makeOverlapTableSpec(element),
        )
        return _SkyPixOverlapStorage(
            db, element, summaryTable=summaryTable, overlapTable=overlapTable, governor=governor
        )

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
                (
                    "skypix_system",
                    "skypix_level",
                    "skypix_index",
                )
                + tuple(element.graph.required.names),
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

    def enable(
        self,
        storage: TableDimensionRecordStorage,
        skypix: SkyPixDimension,
        governorValue: str,
    ) -> None:
        """Enable materialization of overlaps between a skypix dimension
        and the records of ``self.element`` with a particular governor value.

        Parameters
        ----------
        storage : `TableDimensionRecordStorage`
            Storage object for the records of ``self.element``.
        skypix : `SkyPixDimension`
            The skypix dimension (system and level) for which overlaps should
            be materialized.
        governorValue : `str`
            Value of this element's governor dimension for which overlaps
            should be materialized.  For example, if ``self.element`` is
            ``visit``, this is an instrument name; if ``self.element`` is
            ``patch``, this is a skymap name.

        Notes
        -----
        If there are existing rows for the given ``governorValue``, overlap
        rows for them will be immediately computed and inserted.  At present,
        that never happens, because we only enable overlaps with
        `DimensionUniverse.commonSkyPix`, and that happens immediately after
        each governor row is inserted (and there can't be any patch rows,
        for example, until after the corresponding skymap row is inserted).

        After calling `enable` for a particular combination, any new records
        for ``self.element`` that are inserted will automatically be
        accompanied by overlap records (via calls to `insert` made
        by `TableDimensionRecordStorage` methods).
        """
        # Because we're essentially materializing a view in Python, we
        # aggressively lock all tables we're reading and writing in order to be
        # sure nothing gets out of sync.  This may not be the most efficient
        # approach possible, but we'll focus on correct before we focus on
        # fast, and enabling a new overlap combination should be a very rare
        # operation anyway, and never one we do in parallel.
        with self._db.transaction(
            lock=[self._governor.table, storage._table, self._summaryTable, self._overlapTable]
        ):
            result, inserted = self._db.sync(
                self._summaryTable,
                keys={
                    "skypix_system": skypix.system.name,
                    "skypix_level": skypix.level,
                    self._governor.element.name: governorValue,
                },
            )
            if inserted:
                _LOG.debug(
                    "Precomputing initial overlaps for %s vs %s for %s=%s",
                    skypix.name,
                    self.element.name,
                    self._governor.element.name,
                    governorValue,
                )
                self._fill(storage=storage, skypix=skypix, governorValue=governorValue)
            else:
                _LOG.debug(
                    "Overlaps already precomputed for %s vs %s for %s=%s",
                    skypix.name,
                    self.element.name,
                    self._governor.element.name,
                    governorValue,
                )

    def _fill(
        self,
        *,
        storage: TableDimensionRecordStorage,
        skypix: SkyPixDimension,
        governorValue: str,
    ) -> None:
        """Insert overlap records for a newly-enabled combination of skypix
        dimension and governor value.

        This method should only be called by `enable`.

        Parameters
        ----------
        storage : `TableDimensionRecordStorage`
            Storage object for the records of ``self.element``.
        skypix : `SkyPixDimension`
            The skypix dimension (system and level) for which overlaps should
            be materialized.
        governorValue : `str`
            Value of this element's governor dimension for which overlaps
            should be materialized.  For example, if ``self.element`` is
            ``visit``, this is an instrument name; if ``self.element`` is
            ``patch``, this is a skymap name.
        """
        overlapRecords: List[dict] = []
        # `DimensionRecordStorage.fetch` as defined by the ABC expects to be
        # given iterables of data IDs that correspond to that element's graph
        # (e.g. {instrument, visit, detector}), not just some subset of it
        # (e.g. {instrument}).  But we know the implementation of `fetch` for
        # `TableDimensionRecordStorage will use this iterable to do exactly
        # what we want.
        governorDataId = DataCoordinate.standardize(
            {self._governor.element.name: governorValue}, graph=self._governor.element.graph
        )
        for record in storage.fetch(DataCoordinateIterable.fromScalar(governorDataId)):
            if record.region is None:
                continue
            baseOverlapRecord = record.dataId.byName()
            baseOverlapRecord["skypix_system"] = skypix.system.name
            baseOverlapRecord["skypix_level"] = skypix.level
            for begin, end in skypix.pixelization.envelope(record.region):
                overlapRecords.extend(
                    dict(baseOverlapRecord, skypix_index=index) for index in range(begin, end)
                )
        _LOG.debug(
            "Inserting %d initial overlap rows for %s vs %s for %s=%r",
            len(overlapRecords),
            skypix.name,
            self.element.name,
            self._governor.element.name,
            governorValue,
        )
        self._db.insert(self._overlapTable, *overlapRecords)

    def insert(self, records: Sequence[DimensionRecord], replace: bool = False) -> None:
        """Insert overlaps for a sequence of ``self.element`` records that
        have just been inserted.

        This must be called by any method that inserts records for that
        element (i.e. `TableDimensionRecordStorage.insert` and
        `TableDimensionRecordStorage.sync`), within the same transaction.

        Parameters
        ----------
        records : `Sequence` [ `DimensionRecord` ]
            Records for ``self.element``.  Records with `None` regions are
            ignored.
        replace : `bool`, optional
            If `True` (`False` is default) one or more of the given records may
            already exist and is being updated, so we need to delete any
            existing overlap records first.
        """
        # Group records by family.governor value.
        grouped: Dict[str, List[DimensionRecord]] = defaultdict(list)
        for record in records:
            grouped[getattr(record, self._governor.element.name)].append(record)
        _LOG.debug(
            "Precomputing new skypix overlaps for %s where %s in %s.",
            self.element.name,
            self._governor.element.name,
            grouped.keys(),
        )
        # Make sure the set of combinations to materialize does not change
        # while we are materializing the ones we have, by locking the summary
        # table.  Because we aren't planning to write to the summary table,
        # this could just be a SHARED lock instead of an EXCLUSIVE one, but
        # there's no API for that right now.
        with self._db.transaction(lock=[self._summaryTable]):
            # Query for the skypix dimensions to be associated with each
            # governor value.
            gvCol = self._summaryTable.columns[self._governor.element.name]
            sysCol = self._summaryTable.columns.skypix_system
            lvlCol = self._summaryTable.columns.skypix_level
            query = (
                sqlalchemy.sql.select(
                    gvCol,
                    sysCol,
                    lvlCol,
                )
                .select_from(self._summaryTable)
                .where(gvCol.in_(list(grouped.keys())))
            )
            # Group results by governor value, then skypix system.
            skypix: Dict[str, NamedKeyDict[SkyPixSystem, List[int]]] = {
                gv: NamedKeyDict() for gv in grouped.keys()
            }
            for summaryRow in self._db.query(query).mappings():
                system = self.element.universe.skypix[summaryRow[sysCol]]
                skypix[summaryRow[gvCol]].setdefault(system, []).append(summaryRow[lvlCol])
            if replace:
                # Construct constraints for a DELETE query as a list of dicts.
                # We include the skypix_system and skypix_level column values
                # explicitly instead of just letting the query search for all
                # of those related to the given records, because they are the
                # first columns in the primary key, and hence searching with
                # them will be way faster (and we don't want to add a new index
                # just for this operation).
                to_delete: List[Dict[str, Any]] = []
                for gv, skypix_systems in skypix.items():
                    for system, skypix_levels in skypix_systems.items():
                        to_delete.extend(
                            {"skypix_system": system.name, "skypix_level": level, **record.dataId.byName()}
                            for record, level in itertools.product(grouped[gv], skypix_levels)
                        )
                self._db.delete(
                    self._overlapTable,
                    ["skypix_system", "skypix_level"] + list(self.element.graph.required.names),
                    *to_delete,
                )
            overlapRecords: List[dict] = []
            # Compute overlaps for one governor value at a time, but gather
            # them all up for one insert.
            for gv, group in grouped.items():
                overlapRecords.extend(self._compute(group, skypix[gv], gv))
            _LOG.debug(
                "Inserting %d new skypix overlap rows for %s where %s in %s.",
                len(overlapRecords),
                self.element.name,
                self._governor.element.name,
                grouped.keys(),
            )
            self._db.insert(self._overlapTable, *overlapRecords)

    def _compute(
        self,
        records: Sequence[DimensionRecord],
        skypix: NamedKeyDict[SkyPixSystem, List[int]],
        governorValue: str,
    ) -> Iterator[dict]:
        """Compute all overlap rows for a particular governor dimension value
        and all of the skypix dimensions for which its overlaps are enabled.

        This method should only be called by `insert`.

        Parameters
        ----------
        records : `Sequence` [ `DimensionRecord` ]
            Records for ``self.element``.  Records with `None` regions are
            ignored.  All must have the governor value given.
        skypix : `NamedKeyDict` [ `SkyPixSystem`, `list` [ `int` ] ]
            Mapping containing all skypix systems and levels for which overlaps
            should be computed, grouped by `SkyPixSystem`.
        governorValue : `str`
            Value of this element's governor dimension for which overlaps
            should be computed.  For example, if ``self.element`` is ``visit``,
            this is an instrument name; if ``self.element`` is ``patch``, this
            is a skymap name.

        Yields
        ------
        row : `dict`
            Dictionary representing an overlap row.
        """
        # Process input records one at time, computing all skypix indices for
        # each.
        for record in records:
            if record.region is None:
                continue
            assert getattr(record, self._governor.element.name) == governorValue
            for system, levels in skypix.items():
                if not levels:
                    continue
                baseOverlapRecord = record.dataId.byName()
                baseOverlapRecord["skypix_system"] = system.name
                levels.sort(reverse=True)
                # Start with the first level, which is the finest-grained one.
                # Compute skypix envelope indices directly for that.
                indices: Dict[int, Set[int]] = {levels[0]: set()}
                for begin, end in system[levels[0]].pixelization.envelope(record.region):
                    indices[levels[0]].update(range(begin, end))
                # Divide those indices by powers of 4 (and remove duplicates)
                # work our way up to the last (coarsest) level.
                for lastLevel, nextLevel in zip(levels[:-1], levels[1:]):
                    factor = 4 ** (lastLevel - nextLevel)
                    indices[nextLevel] = {index // factor for index in indices[lastLevel]}
                for level in levels:
                    yield from (
                        {
                            "skypix_level": level,
                            "skypix_index": index,
                            **baseOverlapRecord,  # type: ignore
                        }
                        for index in indices[level]
                    )

    def select(
        self,
        skypix: SkyPixDimension,
        governorValues: Union[AbstractSet[str], EllipsisType],
    ) -> sqlalchemy.sql.FromClause:
        """Construct a subquery expression containing overlaps between the
        given skypix dimension and governor values.

        Parameters
        ----------
        skypix : `SkyPixDimension`
            The skypix dimension (system and level) for which overlaps should
            be materialized.
        governorValues : `str`
            Values of this element's governor dimension for which overlaps
            should be returned.  For example, if ``self.element`` is ``visit``,
            this is a set of instrument names; if ``self.element`` is
            ``patch``, this is a set of skymap names.  If ``...`` all values
            in the database are used (`GovernorDimensionRecordStorage.values`).

        Returns
        -------
        subquery : `sqlalchemy.sql.FromClause`
            A SELECT query with an alias, intended for use as a subquery, with
            columns equal to ``self.element.required.names`` + ``skypix.name``.
        """
        if skypix != self.element.universe.commonSkyPix:
            # We guarantee elsewhere that we always materialize all overlaps
            # vs. commonSkyPix, but for everything else, we need to check that
            # we have materialized this combination of governor values and
            # skypix.
            summaryWhere = [
                self._summaryTable.columns.skypix_system == skypix.system.name,
                self._summaryTable.columns.skypix_level == skypix.level,
            ]
            gvCol = self._summaryTable.columns[self._governor.element.name]
            if governorValues is not Ellipsis:
                summaryWhere.append(gvCol.in_(list(governorValues)))
            summaryQuery = (
                sqlalchemy.sql.select(gvCol)
                .select_from(self._summaryTable)
                .where(sqlalchemy.sql.and_(*summaryWhere))
            )
            materializedGovernorValues = {row._mapping[gvCol] for row in self._db.query(summaryQuery)}
            if governorValues is Ellipsis:
                missingGovernorValues = self._governor.values - materializedGovernorValues
            else:
                missingGovernorValues = governorValues - materializedGovernorValues
            if missingGovernorValues:
                raise RuntimeError(
                    f"Query requires an overlap join between {skypix.name} and {self.element.name} "
                    f"(for {self._governor.element.name} in {missingGovernorValues}), but these "
                    f"have not been materialized."
                )
        columns = [self._overlapTable.columns.skypix_index.label(skypix.name)]
        columns.extend(self._overlapTable.columns[name] for name in self.element.graph.required.names)
        overlapWhere = [
            self._overlapTable.columns.skypix_system == skypix.system.name,
            self._overlapTable.columns.skypix_level == skypix.level,
        ]
        if governorValues is not Ellipsis:
            overlapWhere.append(
                self._overlapTable.columns[self._governor.element.name].in_(list(governorValues))
            )
        overlapQuery = (
            sqlalchemy.sql.select(*columns)
            .select_from(self._overlapTable)
            .where(sqlalchemy.sql.and_(*overlapWhere))
        )
        return overlapQuery.alias(f"{self.element.name}_{skypix.name}_overlap")

    def digestTables(self) -> Iterable[sqlalchemy.schema.Table]:
        """Return tables used for schema digest.

        Returns
        -------
        tables : `Iterable` [ `sqlalchemy.schema.Table` ]
            Possibly empty set of tables for schema digest calculations.
        """
        return [self._summaryTable, self._overlapTable]

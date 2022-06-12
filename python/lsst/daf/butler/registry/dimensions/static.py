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

import itertools
from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple, cast

import sqlalchemy

from ...core import (
    DatabaseDimensionElement,
    DatabaseTopologicalFamily,
    DimensionElement,
    DimensionGraph,
    DimensionUniverse,
    GovernorDimension,
    NamedKeyDict,
    SkyPixDimension,
    ddl,
    sql,
)
from .._exceptions import MissingSpatialOverlapError
from ..interfaces import (
    Database,
    DatabaseDimensionOverlapStorage,
    DatabaseDimensionRecordStorage,
    DimensionRecordStorage,
    DimensionRecordStorageManager,
    GovernorDimensionRecordStorage,
    StaticTablesContext,
    VersionTuple,
)

# This has to be updated on every schema change
_VERSION = VersionTuple(6, 0, 2)


class StaticDimensionRecordStorageManager(DimensionRecordStorageManager):
    """An implementation of `DimensionRecordStorageManager` for single-layer
    `Registry` and the base layers of multi-layer `Registry`.

    This manager creates `DimensionRecordStorage` instances for all elements in
    the `DimensionUniverse` in its own `initialize` method, as part of static
    table creation, so it never needs to manage any dynamic registry tables.

    Parameters
    ----------
    db : `Database`
        Interface to the underlying database engine and namespace.
    records : `NamedKeyDict`
        Mapping from `DimensionElement` to `DimensionRecordStorage` for that
        element.
    overlaps : `list` [ `DatabaseDimensionOverlapStorage` ]
        Objects that manage materialized overlaps between database-backed
        dimensions.
    dimensionGraphStorage : `_DimensionGraphStorage`
        Object that manages saved `DimensionGraph` definitions.
    column_types : `sql.ColumnTypeInfo`
        Information about column types that can differ between data
        repositories and registry instances, including the dimension universe.
    """

    def __init__(
        self,
        db: Database,
        *,
        records: NamedKeyDict[DimensionElement, DimensionRecordStorage],
        overlaps: Dict[
            Tuple[DatabaseDimensionElement, DatabaseDimensionElement], DatabaseDimensionOverlapStorage
        ],
        dimensionGraphStorage: _DimensionGraphStorage,
        column_types: sql.ColumnTypeInfo,
    ):
        super().__init__(universe=column_types.universe)
        self._db = db
        self._records = records
        self._overlaps = overlaps
        self._dimensionGraphStorage = dimensionGraphStorage
        self._column_types = column_types

    @classmethod
    def initialize(
        cls,
        db: Database,
        context: StaticTablesContext,
        *,
        column_types: sql.ColumnTypeInfo,
    ) -> DimensionRecordStorageManager:
        # Docstring inherited from DimensionRecordStorageManager.
        universe = column_types.universe
        # Start by initializing governor dimensions; those go both in the main
        # 'records' mapping we'll pass to init, and a local dictionary that we
        # can pass in when initializing storage for DatabaseDimensionElements.
        governors = NamedKeyDict[GovernorDimension, GovernorDimensionRecordStorage]()
        records = NamedKeyDict[DimensionElement, DimensionRecordStorage]()
        for dimension in universe.getGovernorDimensions():
            governorStorage = dimension.makeStorage(db, context=context, column_types=column_types)
            governors[dimension] = governorStorage
            records[dimension] = governorStorage
        # Next we initialize storage for DatabaseDimensionElements.
        # We remember the spatial ones (grouped by family) so we can go back
        # and initialize overlap storage for them later.
        spatial = NamedKeyDict[DatabaseTopologicalFamily, List[DatabaseDimensionRecordStorage]]()
        for element in universe.getDatabaseElements():
            elementStorage = element.makeStorage(
                db, context=context, governors=governors, column_types=column_types
            )
            records[element] = elementStorage
            if element.spatial is not None:
                spatial.setdefault(element.spatial, []).append(elementStorage)
        # Finally we initialize overlap storage.  The implementation class for
        # this is currently hard-coded (it's not obvious there will ever be
        # others).  Note that overlaps between database-backed dimensions and
        # skypix dimensions is internal to `DatabaseDimensionRecordStorage`,
        # and hence is not included here.
        from ..dimensions.overlaps import CrossFamilyDimensionOverlapStorage

        overlaps: Dict[
            Tuple[DatabaseDimensionElement, DatabaseDimensionElement], DatabaseDimensionOverlapStorage
        ] = {}
        for (family1, storages1), (family2, storages2) in itertools.combinations(spatial.items(), 2):
            for elementStoragePair in itertools.product(storages1, storages2):
                governorStoragePair = (governors[family1.governor], governors[family2.governor])
                if elementStoragePair[0].element > elementStoragePair[1].element:
                    elementStoragePair = (elementStoragePair[1], elementStoragePair[0])
                    governorStoragePair = (governorStoragePair[1], governorStoragePair[1])
                overlapStorage = CrossFamilyDimensionOverlapStorage.initialize(
                    db,
                    elementStoragePair,
                    governorStoragePair,
                    context=context,
                )
                elementStoragePair[0].connect(overlapStorage)
                elementStoragePair[1].connect(overlapStorage)
                overlaps[overlapStorage.elements] = overlapStorage
        # Create table that stores DimensionGraph definitions.
        dimensionGraphStorage = _DimensionGraphStorage.initialize(db, context, universe=universe)
        return cls(
            db=db,
            records=records,
            overlaps=overlaps,
            dimensionGraphStorage=dimensionGraphStorage,
            column_types=column_types,
        )

    def refresh(self) -> None:
        # Docstring inherited from DimensionRecordStorageManager.
        for dimension in self.universe.getGovernorDimensions():
            storage = self._records[dimension]
            assert isinstance(storage, GovernorDimensionRecordStorage)
            storage.refresh()

    def get(self, element: DimensionElement | str) -> Optional[DimensionRecordStorage]:
        # Docstring inherited from DimensionRecordStorageManager.
        r = self._records.get(element)
        if r is None:
            element = self.universe[cast(str, getattr(element, "name", element))]
            if isinstance(element, SkyPixDimension):
                return element.makeStorage()
        return r

    def register(self, element: DimensionElement) -> DimensionRecordStorage:
        # Docstring inherited from DimensionRecordStorageManager.
        result = self.get(element)
        assert result, "All records instances should be created in initialize()."
        return result

    def saveDimensionGraph(self, graph: DimensionGraph) -> int:
        # Docstring inherited from DimensionRecordStorageManager.
        return self._dimensionGraphStorage.save(graph)

    def loadDimensionGraph(self, key: int) -> DimensionGraph:
        # Docstring inherited from DimensionRecordStorageManager.
        return self._dimensionGraphStorage.load(key)

    def clearCaches(self) -> None:
        # Docstring inherited from DimensionRecordStorageManager.
        for storage in self._records.values():
            storage.clearCaches()

    def get_spatial_join_relation(
        self, element1: str, element2: str, constraints: sql.LocalConstraints | None = None
    ) -> tuple[sql.Relation, bool]:
        # Docstring inherited.
        storage1 = self[element1]
        storage2 = self[element2]
        overlaps: sql.Relation | None = None
        needs_refinement: bool = False
        match (storage1, storage2):
            case [
                DatabaseDimensionRecordStorage() as db_storage1,
                DatabaseDimensionRecordStorage() as db_storage2,
            ]:
                # Construction guarantees that we only need to try this in one
                # direction; either both storage objects know about the other
                # or neither do.
                overlaps = db_storage1.get_spatial_join_relation(db_storage2.element, constraints)
                if overlaps is None:
                    # No direct materialized overlaps; use commonSkyPix as an
                    # intermediary.
                    common_skypix_overlap1 = db_storage1.get_spatial_join_relation(
                        self.universe.commonSkyPix, constraints
                    )
                    common_skypix_overlap2 = db_storage2.get_spatial_join_relation(
                        self.universe.commonSkyPix, constraints
                    )
                    assert (
                        common_skypix_overlap1 is not None and common_skypix_overlap2 is not None
                    ), "Overlaps with the common skypix dimension should always be available,"
                    overlaps = common_skypix_overlap1.join(
                        common_skypix_overlap2,
                        extra_connections={
                            frozenset(storage1.element.required.names | storage2.element.required.names)
                        },
                    )
                    needs_refinement = True
            case [DatabaseDimensionRecordStorage() as db_storage, other]:
                overlaps = db_storage.get_spatial_join_relation(other.element, constraints)
            case [other, DatabaseDimensionRecordStorage() as db_storage]:
                overlaps = db_storage.get_spatial_join_relation(other.element, constraints)
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

    @classmethod
    def currentVersion(cls) -> Optional[VersionTuple]:
        # Docstring inherited from VersionedExtension.
        return _VERSION

    def schemaDigest(self) -> Optional[str]:
        # Docstring inherited from VersionedExtension.
        tables: List[sqlalchemy.schema.Table] = []
        for recStorage in self._records.values():
            tables += recStorage.digestTables()
        for overlapStorage in self._overlaps.values():
            tables += overlapStorage.digestTables()
        return self._defaultSchemaDigest(tables, self._db.dialect)


class _DimensionGraphStorage:
    """Helper object that manages saved DimensionGraph definitions.

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
        self._keysByGraph: Dict[DimensionGraph, int] = {universe.empty: 0}
        self._graphsByKey: Dict[int, DimensionGraph] = {0: universe.empty}

    @classmethod
    def initialize(
        cls,
        db: Database,
        context: StaticTablesContext,
        *,
        universe: DimensionUniverse,
    ) -> _DimensionGraphStorage:
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
        storage : `_DimensionGraphStorage`
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
        """Refresh the in-memory cache of saved DimensionGraph definitions.

        This should be done automatically whenever needed, but it can also
        be called explicitly.
        """
        dimensionNamesByKey: Dict[int, Set[str]] = defaultdict(set)
        for row in self._db.query(self._definitionTable.select()).mappings():
            key = row[self._definitionTable.columns.dimension_graph_id]
            dimensionNamesByKey[key].add(row[self._definitionTable.columns.dimension_name])
        keysByGraph: Dict[DimensionGraph, int] = {self._universe.empty: 0}
        graphsByKey: Dict[int, DimensionGraph] = {0: self._universe.empty}
        for key, dimensionNames in dimensionNamesByKey.items():
            graph = DimensionGraph(self._universe, names=dimensionNames)
            keysByGraph[graph] = key
            graphsByKey[key] = graph
        self._graphsByKey = graphsByKey
        self._keysByGraph = keysByGraph

    def save(self, graph: DimensionGraph) -> int:
        """Save a `DimensionGraph` definition to the database, allowing it to
        be retrieved later via the returned key.

        Parameters
        ----------
        graph : `DimensionGraph`
            Set of dimensions to save.

        Returns
        -------
        key : `int`
            Integer used as the unique key for this `DimensionGraph` in the
            database.
        """
        key = self._keysByGraph.get(graph)
        if key is not None:
            return key
        # Lock tables and then refresh to guard against races where some other
        # process is trying to register the exact same dimension graph.  This
        # is probably not the most efficient way to do it, but it should be a
        # rare operation, especially since the short-circuit above will usually
        # work in long-lived data repositories.
        with self._db.transaction(lock=[self._idTable, self._definitionTable]):
            self.refresh()
            key = self._keysByGraph.get(graph)
            if key is None:
                (key,) = self._db.insert(self._idTable, {}, returnIds=True)  # type: ignore
                self._db.insert(
                    self._definitionTable,
                    *[{"dimension_graph_id": key, "dimension_name": name} for name in graph.required.names],
                )
            self._keysByGraph[graph] = key
            self._graphsByKey[key] = graph
        return key

    def load(self, key: int) -> DimensionGraph:
        """Retrieve a `DimensionGraph` that was previously saved in the
        database.

        Parameters
        ----------
        key : `int`
            Integer used as the unique key for this `DimensionGraph` in the
            database.

        Returns
        -------
        graph : `DimensionGraph`
            Retrieved graph.
        """
        graph = self._graphsByKey.get(key)
        if graph is None:
            self.refresh()
            graph = self._graphsByKey[key]
        return graph

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

import itertools
from collections import defaultdict
from collections.abc import Mapping, Set
from typing import TYPE_CHECKING, cast

import sqlalchemy
from lsst.daf.relation import Relation

from ... import ddl
from ..._column_tags import DimensionKeyColumnTag
from ..._named import NamedKeyDict
from ...dimensions import (
    DatabaseDimensionElement,
    DatabaseTopologicalFamily,
    DimensionElement,
    DimensionGroup,
    DimensionUniverse,
    GovernorDimension,
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

if TYPE_CHECKING:
    from .. import queries


# This has to be updated on every schema change
_VERSION = VersionTuple(6, 0, 2)


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
    records : `NamedKeyDict`
        Mapping from `DimensionElement` to `DimensionRecordStorage` for that
        element.
    overlaps : `list` [ `DatabaseDimensionOverlapStorage` ]
        Objects that manage materialized overlaps between database-backed
        dimensions.
    dimension_group_storage : `_DimensionGroupStorage`
        Object that manages saved `DimensionGroup` definitions.
    universe : `DimensionUniverse`
        All known dimensions.
    """

    def __init__(
        self,
        db: Database,
        *,
        records: NamedKeyDict[DimensionElement, DimensionRecordStorage],
        overlaps: dict[
            tuple[DatabaseDimensionElement, DatabaseDimensionElement], DatabaseDimensionOverlapStorage
        ],
        dimension_group_storage: _DimensionGroupStorage,
        universe: DimensionUniverse,
        registry_schema_version: VersionTuple | None = None,
    ):
        super().__init__(universe=universe, registry_schema_version=registry_schema_version)
        self._db = db
        self._records = records
        self._overlaps = overlaps
        self._dimension_group_storage = dimension_group_storage

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
        # Start by initializing governor dimensions; those go both in the main
        # 'records' mapping we'll pass to init, and a local dictionary that we
        # can pass in when initializing storage for DatabaseDimensionElements.
        governors = NamedKeyDict[GovernorDimension, GovernorDimensionRecordStorage]()
        records = NamedKeyDict[DimensionElement, DimensionRecordStorage]()
        for dimension in universe.governor_dimensions:
            governorStorage = dimension.makeStorage(db, context=context)
            governors[dimension] = governorStorage
            records[dimension] = governorStorage
        # Next we initialize storage for DatabaseDimensionElements.  Some
        # elements' storage may be views into anothers; we'll do a first pass
        # to gather a mapping from the names of those targets back to their
        # views.
        view_targets = {
            element.viewOf: element for element in universe.database_elements if element.viewOf is not None
        }
        # We remember the spatial ones (grouped by family) so we can go back
        # and initialize overlap storage for them later.
        spatial = NamedKeyDict[DatabaseTopologicalFamily, list[DatabaseDimensionRecordStorage]]()
        for element in universe.database_elements:
            if element.viewOf is not None:
                # We'll initialize this storage when the view's target is
                # initialized.
                continue
            elementStorage = element.makeStorage(db, context=context, governors=governors)
            records[element] = elementStorage
            if element.spatial is not None:
                spatial.setdefault(element.spatial, []).append(elementStorage)
            if (view_element := view_targets.get(element.name)) is not None:
                view_element_storage = view_element.makeStorage(
                    db,
                    context=context,
                    governors=governors,
                    view_target=elementStorage,
                )
                records[view_element] = view_element_storage
                if view_element.spatial is not None:
                    spatial.setdefault(view_element.spatial, []).append(view_element_storage)

        # Finally we initialize overlap storage.  The implementation class for
        # this is currently hard-coded (it's not obvious there will ever be
        # others).  Note that overlaps between database-backed dimensions and
        # skypix dimensions is internal to `DatabaseDimensionRecordStorage`,
        # and hence is not included here.
        from ..dimensions.overlaps import CrossFamilyDimensionOverlapStorage

        overlaps: dict[
            tuple[DatabaseDimensionElement, DatabaseDimensionElement], DatabaseDimensionOverlapStorage
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
        dimension_group_storage = _DimensionGroupStorage.initialize(db, context, universe=universe)
        return cls(
            db=db,
            records=records,
            universe=universe,
            overlaps=overlaps,
            dimension_group_storage=dimension_group_storage,
            registry_schema_version=registry_schema_version,
        )

    def get(self, element: DimensionElement | str) -> DimensionRecordStorage | None:
        # Docstring inherited from DimensionRecordStorageManager.
        r = self._records.get(element)
        if r is None:
            if (dimension := self.universe.skypix_dimensions.get(element)) is not None:
                return dimension.makeStorage()
        return r

    def register(self, element: DimensionElement) -> DimensionRecordStorage:
        # Docstring inherited from DimensionRecordStorageManager.
        result = self.get(element)
        assert result, "All records instances should be created in initialize()."
        return result

    def save_dimension_group(self, graph: DimensionGroup) -> int:
        # Docstring inherited from DimensionRecordStorageManager.
        return self._dimension_group_storage.save(graph)

    def load_dimension_group(self, key: int) -> DimensionGroup:
        # Docstring inherited from DimensionRecordStorageManager.
        return self._dimension_group_storage.load(key)

    def clearCaches(self) -> None:
        # Docstring inherited from DimensionRecordStorageManager.
        for storage in self._records.values():
            storage.clearCaches()

    def make_spatial_join_relation(
        self,
        element1: str,
        element2: str,
        context: queries.SqlQueryContext,
        governor_constraints: Mapping[str, Set[str]],
        existing_relationships: Set[frozenset[str]] = frozenset(),
    ) -> tuple[Relation, bool]:
        # Docstring inherited.
        overlap_relationship = frozenset(
            self.universe[element1].dimensions.names | self.universe[element2].dimensions.names
        )
        if overlap_relationship in existing_relationships:
            return context.preferred_engine.make_join_identity_relation(), False
        storage1 = self[element1]
        storage2 = self[element2]
        overlaps: Relation | None = None
        needs_refinement: bool = False
        match (storage1, storage2):
            case [
                DatabaseDimensionRecordStorage() as db_storage1,
                DatabaseDimensionRecordStorage() as db_storage2,
            ]:
                # Construction guarantees that we only need to try this in one
                # direction; either both storage objects know about the other
                # or neither do.
                overlaps = db_storage1.make_spatial_join_relation(
                    db_storage2.element, context, governor_constraints
                )
                if overlaps is None:
                    # No direct materialized overlaps; use commonSkyPix as an
                    # intermediary.
                    have_overlap1_already = (
                        frozenset(
                            self.universe[element1].dimensions.names | {self.universe.commonSkyPix.name}
                        )
                        in existing_relationships
                    )
                    have_overlap2_already = (
                        frozenset(
                            self.universe[element2].dimensions.names | {self.universe.commonSkyPix.name}
                        )
                        in existing_relationships
                    )
                    overlap1 = context.preferred_engine.make_join_identity_relation()
                    overlap2 = context.preferred_engine.make_join_identity_relation()
                    if not have_overlap1_already:
                        overlap1 = cast(
                            Relation,
                            db_storage1.make_spatial_join_relation(
                                self.universe.commonSkyPix, context, governor_constraints
                            ),
                        )
                    if not have_overlap2_already:
                        overlap2 = cast(
                            Relation,
                            db_storage2.make_spatial_join_relation(
                                self.universe.commonSkyPix, context, governor_constraints
                            ),
                        )
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
            case [DatabaseDimensionRecordStorage() as db_storage, other]:
                overlaps = db_storage.make_spatial_join_relation(other.element, context, governor_constraints)
            case [other, DatabaseDimensionRecordStorage() as db_storage]:
                overlaps = db_storage.make_spatial_join_relation(other.element, context, governor_constraints)
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
    def currentVersions(cls) -> list[VersionTuple]:
        # Docstring inherited from VersionedExtension.
        return [_VERSION]


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
        self._keysByGroup: dict[DimensionGroup, int] = {universe.empty.as_group(): 0}
        self._groupsByKey: dict[int, DimensionGroup] = {0: universe.empty.as_group()}

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
        """Refresh the in-memory cache of saved DimensionGraph definitions.

        This should be done automatically whenever needed, but it can also
        be called explicitly.
        """
        dimensionNamesByKey: dict[int, set[str]] = defaultdict(set)
        with self._db.query(self._definitionTable.select()) as sql_result:
            sql_rows = sql_result.mappings().fetchall()
        for row in sql_rows:
            key = row[self._definitionTable.columns.dimension_graph_id]
            dimensionNamesByKey[key].add(row[self._definitionTable.columns.dimension_name])
        keysByGraph: dict[DimensionGroup, int] = {self._universe.empty.as_group(): 0}
        graphsByKey: dict[int, DimensionGroup] = {0: self._universe.empty.as_group()}
        for key, dimensionNames in dimensionNamesByKey.items():
            graph = DimensionGroup(self._universe, names=dimensionNames)
            keysByGraph[graph] = key
            graphsByKey[key] = graph
        self._groupsByKey = graphsByKey
        self._keysByGroup = keysByGraph

    def save(self, group: DimensionGroup) -> int:
        """Save a `DimensionGraph` definition to the database, allowing it to
        be retrieved later via the returned key.

        Parameters
        ----------
        group : `DimensionGroup`
            Set of dimensions to save.

        Returns
        -------
        key : `int`
            Integer used as the unique key for this `DimensionGraph` in the
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
        graph = self._groupsByKey.get(key)
        if graph is None:
            self.refresh()
            graph = self._groupsByKey[key]
        return graph

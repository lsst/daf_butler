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

__all__ = (
    "StaticDatasetTablesTuple",
    "addDatasetForeignKey",
    "makeCalibTableName",
    "makeCalibTableSpec",
    "makeStaticTableSpecs",
    "makeTagTableName",
    "makeTagTableSpec",
)

from collections import namedtuple
from typing import Any, TypeAlias

import sqlalchemy

from lsst.utils.classes import immutable

from .... import ddl
from ...._utilities.thread_safe_cache import ThreadSafeCache
from ....dimensions import DimensionGroup, DimensionUniverse, GovernorDimension, addDimensionForeignKey
from ....timespan_database_representation import TimespanDatabaseRepresentation
from ...interfaces import CollectionManager, Database, VersionTuple

DATASET_TYPE_NAME_LENGTH = 128


class MissingDatabaseTableError(RuntimeError):
    """Exception raised when a table is not found in a database."""


StaticDatasetTablesTuple = namedtuple(
    "StaticDatasetTablesTuple",
    [
        "dataset_type",
        "dataset",
    ],
)


def addDatasetForeignKey(
    tableSpec: ddl.TableSpec,
    *,
    name: str = "dataset",
    onDelete: str | None = None,
    constraint: bool = True,
    **kwargs: Any,
) -> ddl.FieldSpec:
    """Add a foreign key column for datasets and (optionally) a constraint to
    a table.

    This is an internal interface for the ``byDimensions`` package; external
    code should use `DatasetRecordStorageManager.addDatasetForeignKey` instead.

    Parameters
    ----------
    tableSpec : `ddl.TableSpec`
        Specification for the table that should reference the dataset
        table.  Will be modified in place.
    name : `str`, optional
        A name to use for the prefix of the new field; the full name is
        ``{name}_id``.
    onDelete : `str`, optional
        One of "CASCADE" or "SET NULL", indicating what should happen to
        the referencing row if the collection row is deleted.  `None`
        indicates that this should be an integrity error.
    constraint : `bool`, optional
        If `False` (`True` is default), add a field that can be joined to
        the dataset primary key, but do not add a foreign key constraint.
    **kwargs
        Additional keyword arguments are forwarded to the `ddl.FieldSpec`
        constructor (only the ``name`` and ``dtype`` arguments are
        otherwise provided).

    Returns
    -------
    idSpec : `ddl.FieldSpec`
        Specification for the ID field.
    """
    idFieldSpec = ddl.FieldSpec(f"{name}_id", dtype=ddl.GUID, **kwargs)
    tableSpec.fields.add(idFieldSpec)
    if constraint:
        tableSpec.foreignKeys.append(
            ddl.ForeignKeySpec("dataset", source=(idFieldSpec.name,), target=("id",), onDelete=onDelete)
        )
    return idFieldSpec


def makeStaticTableSpecs(
    collections: type[CollectionManager],
    universe: DimensionUniverse,
    schema_version: VersionTuple,
) -> StaticDatasetTablesTuple:
    """Construct all static tables used by the classes in this package.

    Static tables are those that are present in all Registries and do not
    depend on what DatasetTypes have been registered.

    Parameters
    ----------
    collections : `CollectionManager`
        Manager object for the collections in this `Registry`.
    universe : `DimensionUniverse`
        Universe graph containing all dimensions known to this `Registry`.
    schema_version : `VersionTuple`
        The version of this schema.

    Returns
    -------
    specs : `StaticDatasetTablesTuple`
        A named tuple containing `ddl.TableSpec` instances.
    """
    ingest_date_type: type
    ingest_date_default: Any = None
    if schema_version.major > 1:
        ingest_date_type = ddl.AstropyTimeNsecTai
    else:
        ingest_date_type = sqlalchemy.TIMESTAMP
        # New code provides explicit values for ingest_data, but we keep
        # default just to be consistent with the existing schema.
        ingest_date_default = sqlalchemy.sql.func.now()

    specs = StaticDatasetTablesTuple(
        dataset_type=ddl.TableSpec(
            fields=[
                ddl.FieldSpec(
                    name="id",
                    dtype=sqlalchemy.BigInteger,
                    autoincrement=True,
                    primaryKey=True,
                    doc=(
                        "Autoincrement ID that uniquely identifies a dataset "
                        "type in other tables.  Python code outside the "
                        "`Registry` class should never interact with this; "
                        "its existence is considered an implementation detail."
                    ),
                ),
                ddl.FieldSpec(
                    name="name",
                    dtype=sqlalchemy.String,
                    length=DATASET_TYPE_NAME_LENGTH,
                    nullable=False,
                    doc="String name that uniquely identifies a dataset type.",
                ),
                ddl.FieldSpec(
                    name="storage_class",
                    dtype=sqlalchemy.String,
                    length=64,
                    nullable=False,
                    doc=(
                        "Name of the storage class associated with all "
                        "datasets of this type.  Storage classes are "
                        "generally associated with a Python class, and are "
                        "enumerated in butler configuration."
                    ),
                ),
                ddl.FieldSpec(
                    name="dimensions_key",
                    dtype=sqlalchemy.BigInteger,
                    nullable=False,
                    doc="Unique key for the set of dimensions that identifies datasets of this type.",
                ),
                ddl.FieldSpec(
                    name="tag_association_table",
                    dtype=sqlalchemy.String,
                    length=128,
                    nullable=False,
                    doc=(
                        "Name of the table that holds associations between "
                        "datasets of this type and most types of collections."
                    ),
                ),
                ddl.FieldSpec(
                    name="calibration_association_table",
                    dtype=sqlalchemy.String,
                    length=128,
                    nullable=True,
                    doc=(
                        "Name of the table that holds associations between "
                        "datasets of this type and CALIBRATION collections.  "
                        "NULL values indicate dataset types with "
                        "isCalibration=False."
                    ),
                ),
            ],
            unique=[("name",)],
        ),
        dataset=ddl.TableSpec(
            fields=[
                ddl.FieldSpec(
                    name="id",
                    dtype=ddl.GUID,
                    primaryKey=True,
                    doc="A unique field used as the primary key for dataset.",
                ),
                ddl.FieldSpec(
                    name="dataset_type_id",
                    dtype=sqlalchemy.BigInteger,
                    nullable=False,
                    doc="Reference to the associated entry in the dataset_type table.",
                ),
                ddl.FieldSpec(
                    name="ingest_date",
                    dtype=ingest_date_type,
                    default=ingest_date_default,
                    nullable=False,
                    doc="Time of dataset ingestion.",
                ),
                # Foreign key field/constraint to run added below.
            ],
            foreignKeys=[
                ddl.ForeignKeySpec("dataset_type", source=("dataset_type_id",), target=("id",)),
            ],
        ),
    )
    # Add foreign key fields programmatically.
    collections.addRunForeignKey(specs.dataset, onDelete="CASCADE", nullable=False)
    return specs


def makeTagTableName(dimensionsKey: int) -> str:
    """Construct the name for a dynamic (DatasetType-dependent) tag table used
    by the classes in this package.

    Parameters
    ----------
    dimensionsKey : `int`
        Integer key used to save ``datasetType.dimensions`` to the database.

    Returns
    -------
    name : `str`
        Name for the table.
    """
    return f"dataset_tags_{dimensionsKey:08d}"


def makeCalibTableName(dimensionsKey: int) -> str:
    """Construct the name for a dynamic (DatasetType-dependent) tag + validity
    range table used by the classes in this package.

    Parameters
    ----------
    dimensionsKey : `int`
        Integer key used to save ``datasetType.dimensions`` to the database.

    Returns
    -------
    name : `str`
        Name for the table.
    """
    return f"dataset_calibs_{dimensionsKey:08d}"


def makeTagTableSpec(
    dimensions: DimensionGroup, collections: type[CollectionManager], *, constraints: bool = True
) -> ddl.TableSpec:
    """Construct the specification for a dynamic (DatasetType-dependent) tag
    table used by the classes in this package.

    Parameters
    ----------
    dimensions : `DimensionGroup`
        Dimensions of the dataset type.
    collections : `type` [ `CollectionManager` ]
        `CollectionManager` subclass that can be used to construct foreign keys
        to the run and/or collection tables.
    constraints : `bool`, optional
        If `False` (`True` is default), do not define foreign key constraints.

    Returns
    -------
    spec : `ddl.TableSpec`
        Specification for the table.
    """
    tableSpec = ddl.TableSpec(
        fields=[
            # Foreign key fields to dataset, collection, and usually dimension
            # tables added below.
            # The dataset_type_id field here would be redundant with the one
            # in the main monolithic dataset table, but we need it here for an
            # important unique constraint.
            ddl.FieldSpec("dataset_type_id", dtype=sqlalchemy.BigInteger, nullable=False),
        ]
    )
    if constraints:
        tableSpec.foreignKeys.append(
            ddl.ForeignKeySpec("dataset_type", source=("dataset_type_id",), target=("id",))
        )
    # We'll also have a unique constraint on dataset type, collection, and data
    # ID.  We only include the required part of the data ID, as that's
    # sufficient and saves us from worrying about nulls in the constraint.
    constraint = ["dataset_type_id"]
    # Add foreign key fields to dataset table (part of the primary key)
    addDatasetForeignKey(tableSpec, primaryKey=True, onDelete="CASCADE", constraint=constraints)
    # Add foreign key fields to collection table (part of the primary key and
    # the data ID unique constraint).
    collectionFieldSpec = collections.addCollectionForeignKey(
        tableSpec, primaryKey=True, onDelete="CASCADE", constraint=constraints
    )
    constraint.append(collectionFieldSpec.name)
    # Add foreign key constraint to the collection_summary_dataset_type table.
    if constraints:
        tableSpec.foreignKeys.append(
            ddl.ForeignKeySpec(
                "collection_summary_dataset_type",
                source=(collectionFieldSpec.name, "dataset_type_id"),
                target=(collectionFieldSpec.name, "dataset_type_id"),
            )
        )
    for dimension_name in dimensions.required:
        dimension = dimensions.universe.dimensions[dimension_name]
        fieldSpec = addDimensionForeignKey(
            tableSpec, dimension=dimension, nullable=False, primaryKey=False, constraint=constraints
        )
        constraint.append(fieldSpec.name)
        # If this is a governor dimension, add a foreign key constraint to the
        # collection_summary_<dimension> table.
        if isinstance(dimension, GovernorDimension) and constraints:
            tableSpec.foreignKeys.append(
                ddl.ForeignKeySpec(
                    f"collection_summary_{dimension.name}",
                    source=(collectionFieldSpec.name, fieldSpec.name),
                    target=(collectionFieldSpec.name, fieldSpec.name),
                )
            )
    # Actually add the unique constraint.
    tableSpec.unique.add(tuple(constraint))
    return tableSpec


def makeCalibTableSpec(
    dimensions: DimensionGroup,
    collections: type[CollectionManager],
    TimespanReprClass: type[TimespanDatabaseRepresentation],
) -> ddl.TableSpec:
    """Construct the specification for a dynamic (DatasetType-dependent) tag +
    validity range table used by the classes in this package.

    Parameters
    ----------
    dimensions : `DimensionGroup`
        Dimensions of the dataset type.
    collections : `type` [ `CollectionManager` ]
        `CollectionManager` subclass that can be used to construct foreign keys
        to the run and/or collection tables.
    TimespanReprClass : `type` of `TimespanDatabaseRepresentation`
        The Python type to use to represent a timespan.

    Returns
    -------
    spec : `ddl.TableSpec`
        Specification for the table.
    """
    tableSpec = ddl.TableSpec(
        fields=[
            # This table has no natural primary key, compound or otherwise, so
            # we add an autoincrement key.  We may use this field a bit
            # internally, but its presence is an implementation detail and it
            # shouldn't appear as a foreign key in any other tables.
            ddl.FieldSpec("id", dtype=sqlalchemy.BigInteger, autoincrement=True, primaryKey=True),
            # Foreign key fields to dataset, collection, and usually dimension
            # tables added below.  The dataset_type_id field here is redundant
            # with the one in the main monolithic dataset table, but this bit
            # of denormalization lets us define what should be a much more
            # useful index.
            ddl.FieldSpec("dataset_type_id", dtype=sqlalchemy.BigInteger, nullable=False),
        ],
        foreignKeys=[
            ddl.ForeignKeySpec("dataset_type", source=("dataset_type_id",), target=("id",)),
        ],
    )
    # Record fields that should go in the temporal lookup index/constraint,
    # starting with the dataset type.
    index: list[str | type[TimespanDatabaseRepresentation]] = ["dataset_type_id"]
    # Add foreign key fields to dataset table (not part of the temporal
    # lookup/constraint).
    addDatasetForeignKey(tableSpec, nullable=False, onDelete="CASCADE")
    # Add foreign key fields to collection table (part of the temporal lookup
    # index/constraint).
    collectionFieldSpec = collections.addCollectionForeignKey(tableSpec, nullable=False, onDelete="CASCADE")
    index.append(collectionFieldSpec.name)
    # Add foreign key constraint to the collection_summary_dataset_type table.
    tableSpec.foreignKeys.append(
        ddl.ForeignKeySpec(
            "collection_summary_dataset_type",
            source=(collectionFieldSpec.name, "dataset_type_id"),
            target=(collectionFieldSpec.name, "dataset_type_id"),
        )
    )
    # Add dimension fields (part of the temporal lookup index.constraint).
    for dimension_name in dimensions.required:
        dimension = dimensions.universe.dimensions[dimension_name]
        fieldSpec = addDimensionForeignKey(tableSpec, dimension=dimension, nullable=False, primaryKey=False)
        index.append(fieldSpec.name)
        # If this is a governor dimension, add a foreign key constraint to the
        # collection_summary_<dimension> table.
        if isinstance(dimension, GovernorDimension):
            tableSpec.foreignKeys.append(
                ddl.ForeignKeySpec(
                    f"collection_summary_{dimension.name}",
                    source=(collectionFieldSpec.name, fieldSpec.name),
                    target=(collectionFieldSpec.name, fieldSpec.name),
                )
            )
    # Add validity-range field(s) (part of the temporal lookup
    # index/constraint).
    tsFieldSpecs = TimespanReprClass.makeFieldSpecs(nullable=False)
    for fieldSpec in tsFieldSpecs:
        tableSpec.fields.add(fieldSpec)
    if TimespanReprClass.hasExclusionConstraint():
        # This database's timespan representation can define a database-level
        # constraint that prevents overlapping validity ranges for entries with
        # the same DatasetType, collection, and data ID.
        # This also creates an index.
        index.append(TimespanReprClass)
        tableSpec.exclusion.add(tuple(index))
    else:
        # No database-level constraint possible.  We'll have to simulate that
        # in our DatasetRecordStorage.certify() implementation, and just create
        # a regular index here in the hope that helps with lookups.
        index.extend(fieldSpec.name for fieldSpec in tsFieldSpecs)
        tableSpec.indexes.add(ddl.IndexSpec(*index))  # type: ignore
    return tableSpec


TableCache: TypeAlias = ThreadSafeCache[str, sqlalchemy.Table]


@immutable
class DynamicTables:
    """A struct that holds the "dynamic" tables common to dataset types that
    share the same dimensions.

    Objects of this class may be shared between multiple threads, so it must be
    immutable to prevent concurrency issues.

    Parameters
    ----------
    dimensions : `DimensionGroup`
        Dimensions of the dataset types that use these tables.
    dimensions_key : `int`
        Integer key used to persist this dimension group in the database and
        name the associated tables.
    tags_name : `str`
        Name of the "tags" table that associates datasets with data IDs in
        RUN and TAGGED collections.
    calibs_name : `str` or `None`
        Name of the "calibs" table that associates datasets with data IDs and
        timespans in CALIBRATION collections.  This is `None` if none of the
        dataset types (or at least none of those seen by this client) are
        calibrations.
    """

    def __init__(
        self, dimensions: DimensionGroup, dimensions_key: int, tags_name: str, calibs_name: str | None
    ):
        self._dimensions = dimensions
        self.dimensions_key = dimensions_key
        self.tags_name = tags_name
        self.calibs_name = calibs_name

    def copy(self, calibs_name: str) -> DynamicTables:
        return DynamicTables(self._dimensions, self.dimensions_key, self.tags_name, calibs_name)

    @classmethod
    def from_dimensions_key(
        cls, dimensions: DimensionGroup, dimensions_key: int, is_calibration: bool
    ) -> DynamicTables:
        """Construct with table names generated from the dimension key.

        Parameters
        ----------
        dimensions : `DimensionGroup`
            Dimensions of the dataset types that use these tables.
        dimensions_key : `int`
            Integer key used to persist this dimension group in the database
            and name the associated tables.
        is_calibration : `bool`
            Whether any of the dataset types that use these tables are
            calibrations.

        Returns
        -------
        dynamic_tables : `DynamicTables`
            Struct that holds tables for a group of dataset types.
        """
        return cls(
            dimensions,
            dimensions_key=dimensions_key,
            tags_name=makeTagTableName(dimensions_key),
            calibs_name=makeCalibTableName(dimensions_key) if is_calibration else None,
        )

    def create(self, db: Database, collections: type[CollectionManager], cache: TableCache) -> None:
        """Create the tables if they don't already exist.

        Parameters
        ----------
        db : `Database`
            Database interface.
        collections : `type` [ `CollectionManager` ]
            Manager class for collections; used to create foreign key columns
            for collections.
        cache : `DynamicTablesCache`
            Cache used to store sqlalchemy Table objects.
        """
        if cache.get(self.tags_name) is None:
            cache.set_or_get(
                self.tags_name,
                db.ensureTableExists(
                    self.tags_name,
                    makeTagTableSpec(self._dimensions, collections),
                ),
            )

        if self.calibs_name is not None and cache.get(self.calibs_name) is None:
            cache.set_or_get(
                self.calibs_name,
                db.ensureTableExists(
                    self.calibs_name,
                    makeCalibTableSpec(self._dimensions, collections, db.getTimespanRepresentation()),
                ),
            )

    def add_calibs(
        self, db: Database, collections: type[CollectionManager], cache: TableCache
    ) -> DynamicTables:
        """Create a calibs table for a dataset type whose dimensions already
        have a tags table.

        Parameters
        ----------
        db : `Database`
            Database interface.
        collections : `type` [ `CollectionManager` ]
            Manager class for collections; used to create foreign key columns
            for collections.
        cache : `DynamicTablesCache`
            Cache used to store sqlalchemy Table objects.
        """
        calibs_name = makeCalibTableName(self.dimensions_key)
        cache.set_or_get(
            calibs_name,
            db.ensureTableExists(
                calibs_name,
                makeCalibTableSpec(self._dimensions, collections, db.getTimespanRepresentation()),
            ),
        )

        return self.copy(calibs_name=calibs_name)

    def tags(self, db: Database, collections: type[CollectionManager], cache: TableCache) -> sqlalchemy.Table:
        """Return the "tags" table that associates datasets with data IDs in
        TAGGED and RUN collections.

        This method caches its result the first time it is called (and assumes
        the arguments it is given never change).

        Parameters
        ----------
        db : `Database`
            Database interface.
        collections : `type` [ `CollectionManager` ]
            Manager class for collections; used to create foreign key columns
            for collections.
        cache : `DynamicTablesCache`
            Cache used to store sqlalchemy Table objects.

        Returns
        -------
        table : `sqlalchemy.Table`
            SQLAlchemy table object.
        """
        table = cache.get(self.tags_name)
        if table is not None:
            return table

        spec = makeTagTableSpec(self._dimensions, collections)
        table = db.getExistingTable(self.tags_name, spec)
        if table is None:
            raise MissingDatabaseTableError(f"Table {self.tags_name!r} is missing from database schema.")
        return cache.set_or_get(self.tags_name, table)

    def calibs(
        self, db: Database, collections: type[CollectionManager], cache: TableCache
    ) -> sqlalchemy.Table:
        """Return the "calibs" table that associates datasets with data IDs and
        timespans in CALIBRATION collections.

        This method caches its result the first time it is called (and assumes
        the arguments it is given never change).  It may only be called if the
        dataset type is calibration.

        Parameters
        ----------
        db : `Database`
            Database interface.
        collections : `type` [ `CollectionManager` ]
            Manager class for collections; used to create foreign key columns
            for collections.
        cache : `DynamicTablesCache`
            Cache used to store sqlalchemy Table objects.

        Returns
        -------
        table : `sqlalchemy.Table`
            SQLAlchemy table object.
        """
        assert self.calibs_name is not None, (
            "Dataset type should be checked to be calibration by calling code."
        )
        table = cache.get(self.calibs_name)
        if table is not None:
            return table

        spec = makeCalibTableSpec(self._dimensions, collections, db.getTimespanRepresentation())
        table = db.getExistingTable(self.calibs_name, spec)
        if table is None:
            raise MissingDatabaseTableError(f"Table {self.calibs_name!r} is missing from database schema.")
        return cache.set_or_get(self.calibs_name, table)

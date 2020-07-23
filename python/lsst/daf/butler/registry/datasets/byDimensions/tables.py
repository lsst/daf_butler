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

__all__ = (
    "addDatasetForeignKey",
    "makeStaticTableSpecs",
    "makeDynamicTableName",
    "makeDynamicTableSpec",
    "StaticDatasetTablesTuple",
)

from typing import (
    Any,
    Optional,
    Type,
)

from collections import namedtuple

import sqlalchemy

from lsst.daf.butler import (
    DatasetType,
    ddl,
    DimensionUniverse,
)
from lsst.daf.butler import addDimensionForeignKey
from lsst.daf.butler.registry.interfaces import CollectionManager


DATASET_TYPE_NAME_LENGTH = 128


StaticDatasetTablesTuple = namedtuple(
    "StaticDatasetTablesTuple",
    [
        "dataset_type",
        "dataset",
    ]
)


def addDatasetForeignKey(tableSpec: ddl.TableSpec, *,
                         name: str = "dataset",
                         onDelete: Optional[str] = None,
                         constraint: bool = True,
                         **kwargs: Any) -> ddl.FieldSpec:
    """Add a foreign key column for datasets and (optionally) a constraint to
    a table.

    This is an internal interface for the ``byDimensions`` package; external
    code should use `DatasetRecordStorageManager.addDatasetForeignKey` instead.

    Parameters
    ----------
    tableSpec : `ddl.TableSpec`
        Specification for the table that should reference the dataset
        table.  Will be modified in place.
    name: `str`, optional
        A name to use for the prefix of the new field; the full name is
        ``{name}_id``.
    onDelete: `str`, optional
        One of "CASCADE" or "SET NULL", indicating what should happen to
        the referencing row if the collection row is deleted.  `None`
        indicates that this should be an integrity error.
    constraint: `bool`, optional
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
    idFieldSpec = ddl.FieldSpec(f"{name}_id", dtype=sqlalchemy.BigInteger, **kwargs)
    tableSpec.fields.add(idFieldSpec)
    if constraint:
        tableSpec.foreignKeys.append(ddl.ForeignKeySpec("dataset", source=(idFieldSpec.name,),
                                                        target=("id",), onDelete=onDelete))
    return idFieldSpec


def makeStaticTableSpecs(collections: Type[CollectionManager],
                         universe: DimensionUniverse,
                         ) -> StaticDatasetTablesTuple:
    """Construct all static tables used by the classes in this package.

    Static tables are those that are present in all Registries and do not
    depend on what DatasetTypes have been registered.

    Parameters
    ----------
    collections: `CollectionManager`
        Manager object for the collections in this `Registry`.
    universe : `DimensionUniverse`
        Universe graph containing all dimensions known to this `Registry`.

    Returns
    -------
    specs : `StaticDatasetTablesTuple`
        A named tuple containing `ddl.TableSpec` instances.
    """
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
                    )
                ),
                ddl.FieldSpec(
                    name="dimensions_encoded",
                    dtype=ddl.Base64Bytes,
                    nbytes=universe.getEncodeLength(),
                    nullable=False,
                    doc=(
                        "An opaque (but reversible) encoding of the set of "
                        "dimensions used to identify dataset of this type."
                    ),
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
            ],
            unique=[("name",)],
        ),
        dataset=ddl.TableSpec(
            fields=[
                ddl.FieldSpec(
                    name="id",
                    dtype=sqlalchemy.BigInteger,
                    autoincrement=True,
                    primaryKey=True,
                    doc="A unique autoincrement field used as the primary key for dataset.",
                ),
                ddl.FieldSpec(
                    name="dataset_type_id",
                    dtype=sqlalchemy.BigInteger,
                    nullable=False,
                    doc=(
                        "Reference to the associated entry in the dataset_type "
                        "table."
                    ),
                ),
                # Foreign key field/constraint to run added below.
            ],
            foreignKeys=[
                ddl.ForeignKeySpec("dataset_type", source=("dataset_type_id",), target=("id",)),
            ]
        ),
    )
    # Add foreign key fields programmatically.
    collections.addRunForeignKey(specs.dataset, onDelete="CASCADE", nullable=False)
    return specs


def makeDynamicTableName(datasetType: DatasetType) -> str:
    """Construct the name for a dynamic (DatasetType-dependent) table used by
    the classes in this package.

    Parameters
    ----------
    datasetType : `DatasetType`
        Dataset type to construct a name for.  Multiple dataset types may
        share the same table.

    Returns
    -------
    name : `str`
        Name for the table.
    """
    return f"dataset_collection_{datasetType.dimensions.encode().hex()}"


def makeDynamicTableSpec(datasetType: DatasetType, collections: Type[CollectionManager]) -> ddl.TableSpec:
    """Construct the specification for a dynamic (DatasetType-dependent) table
    used by the classes in this package.

    Parameters
    ----------
    datasetType : `DatasetType`
        Dataset type to construct a spec for.  Multiple dataset types may
        share the same table.

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
        ],
        foreignKeys=[
            ddl.ForeignKeySpec("dataset_type", source=("dataset_type_id",), target=("id",)),
        ]
    )
    # We'll also have a unique constraint on dataset type, collection, and data
    # ID.  We only include the required part of the data ID, as that's
    # sufficient and saves us from worrying about nulls in the constraint.
    constraint = ["dataset_type_id"]
    # Add foreign key fields to dataset table (part of the primary key)
    addDatasetForeignKey(tableSpec, primaryKey=True, onDelete="CASCADE")
    # Add foreign key fields to collection table (part of the primary key and
    # the data ID unique constraint).
    fieldSpec = collections.addCollectionForeignKey(tableSpec, primaryKey=True, onDelete="CASCADE")
    constraint.append(fieldSpec.name)
    for dimension in datasetType.dimensions.required:
        fieldSpec = addDimensionForeignKey(tableSpec, dimension=dimension, nullable=False, primaryKey=False)
        constraint.append(fieldSpec.name)
    # Actually add the unique constraint.
    tableSpec.unique.add(tuple(constraint))
    return tableSpec

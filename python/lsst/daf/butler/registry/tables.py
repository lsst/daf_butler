# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software=you can redistribute it and/or modify
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

__all__ = ["RegistryTablesTuple", "makeRegistryTableSpecs"]

from collections import namedtuple

import sqlalchemy

from ..core.dimensions import DimensionUniverse
from ..core.dimensions.schema import addDimensionForeignKey

from ..core import ddl

from .interfaces import CollectionManager


RegistryTablesTuple = namedtuple(
    "RegistryTablesTuple",
    [
        "dataset",
        "dataset_composition",
        "dataset_type",
        "dataset_type_dimensions",
        "dataset_collection",
        "quantum",
        "dataset_consumers",
        "dataset_storage",
    ]
)


def makeRegistryTableSpecs(universe: DimensionUniverse, collections: CollectionManager
                           ) -> RegistryTablesTuple:
    """Construct descriptions of all tables in the Registry, aside from those
    that correspond to `DimensionElement` instances.

    Parameters
    ----------
    universe : `DimensionUniverse`
        All dimensions known to the `Registry`.
    collections : `Collection`
        The `CollectionManager` that will be used for this `Registry`; used to
        create foreign keys to the run and collection tables.

    Returns
    -------
    specs : `RegistryTablesTuple`
        A named tuple containing `ddl.TableSpec` instances.
    """
    # The 'dataset' table is special: we need to add foreign key fields for
    # each dimension in the universe, as well as a foreign key field for run.
    dataset = ddl.TableSpec(
        fields=[
            ddl.FieldSpec(
                name="dataset_id",
                dtype=sqlalchemy.BigInteger,
                primaryKey=True,
                autoincrement=True,
                doc="A unique autoincrement field used as the primary key for dataset.",
            ),
            ddl.FieldSpec(
                name="dataset_type_name",
                dtype=sqlalchemy.String,
                length=128,
                nullable=False,
                doc=(
                    "The name of the DatasetType associated with this dataset; a "
                    "reference to the dataset_type table."
                ),
            ),
            ddl.FieldSpec(
                name="quantum_id",
                dtype=sqlalchemy.BigInteger,
                doc=(
                    "The id of the quantum that produced this dataset, providing access "
                    "to fine-grained provenance information.  May be null for datasets "
                    "not produced by running a PipelineTask."
                ),
            ),
            ddl.FieldSpec(
                name="dataset_ref_hash",
                dtype=ddl.Base64Bytes,
                nbytes=32,
                nullable=False,
                doc="Secure hash of the data ID (i.e. dimension link values) and dataset_type_name.",
            ),
        ],
        foreignKeys=[
            ddl.ForeignKeySpec(
                table="dataset_type",
                source=("dataset_type_name",),
                target=("dataset_type_name",),
            ),
            ddl.ForeignKeySpec(
                table="quantum",
                source=("quantum_id",),
                target=("id",),
                onDelete="SET NULL",
            ),
        ],
    )
    field = collections.addRunForeignKey(dataset, onDelete="CASCADE", nullable=False)
    dataset.unique.add(("dataset_ref_hash", field.name))
    for dimension in universe.dimensions:
        addDimensionForeignKey(dataset, dimension, primaryKey=False, nullable=True)

    # The dataset_collection table needs a foreign key to collection.
    dataset_collection = ddl.TableSpec(
        doc=(
            "A table that associates Dataset records with Collections, "
            "which are implemented simply as string tags."
        ),
        fields=[
            ddl.FieldSpec(
                name="dataset_id",
                dtype=sqlalchemy.BigInteger,
                primaryKey=True,
                nullable=False,
                doc="Link to a unique record in the dataset table.",
            ),
            ddl.FieldSpec(
                name="dataset_ref_hash",
                dtype=ddl.Base64Bytes,
                nbytes=32,
                nullable=False,
                doc="Secure hash of the data ID (i.e. dimension link values) and dataset_type_name.",
            ),
        ],
        foreignKeys=[
            ddl.ForeignKeySpec(
                table="dataset",
                source=("dataset_id",),
                target=("dataset_id",),
                onDelete="CASCADE",
            )
        ],
    )
    field = collections.addCollectionForeignKey(dataset_collection, onDelete="CASCADE", nullable=False)
    dataset_collection.unique.add(("dataset_ref_hash", field.name))

    # The quantum table needs a foreign key to run.
    quantum = ddl.TableSpec(
        doc="A table used to capture fine-grained provenance for datasets produced by PipelineTasks.",
        fields=[
            ddl.FieldSpec(
                name="id",
                dtype=sqlalchemy.BigInteger,
                primaryKey=True,
                autoincrement=True,
                doc="A unique autoincrement integer identifier for this quantum.",
            ),
            ddl.FieldSpec(
                name="task",
                dtype=sqlalchemy.String,
                length=256,
                doc="Fully qualified name of the SuperTask that executed this quantum.",
            ),
            ddl.FieldSpec(
                name="start_time",
                dtype=ddl.AstropyTimeNsecTai,
                nullable=True,
                doc="The start time for the quantum.",
            ),
            ddl.FieldSpec(
                name="end_time",
                dtype=ddl.AstropyTimeNsecTai,
                nullable=True,
                doc="The end time for the quantum.",
            ),
            ddl.FieldSpec(
                name="host",
                dtype=sqlalchemy.String,
                length=64,
                nullable=True,
                doc="The system on which the quantum was executed.",
            ),
        ],
    )
    collections.addRunForeignKey(quantum, onDelete="CASCADE", nullable=False)

    # All other table specs are fully static and do not depend on
    # configuration.
    return RegistryTablesTuple(
        dataset=dataset,
        dataset_composition=ddl.TableSpec(
            doc="A self-join table that relates components of a dataset to their parents.",
            fields=[
                ddl.FieldSpec(
                    name="parent_dataset_id",
                    dtype=sqlalchemy.BigInteger,
                    primaryKey=True,
                    doc="Link to the dataset entry for the parent/composite dataset.",
                ),
                ddl.FieldSpec(
                    name="component_dataset_id",
                    dtype=sqlalchemy.BigInteger,
                    primaryKey=True,
                    doc="Link to the dataset entry for a child/component dataset.",
                ),
                ddl.FieldSpec(
                    name="component_name",
                    dtype=sqlalchemy.String,
                    length=32,
                    nullable=False,
                    doc="Name of this component within this composite.",
                ),
            ],
            foreignKeys=[
                ddl.ForeignKeySpec(
                    table="dataset",
                    source=("parent_dataset_id",),
                    target=("dataset_id",),
                    onDelete="CASCADE",
                ),
                ddl.ForeignKeySpec(
                    table="dataset",
                    source=("component_dataset_id",),
                    target=("dataset_id",),
                    onDelete="CASCADE",
                ),
            ],
        ),
        dataset_type=ddl.TableSpec(
            doc="A Table containing the set of registered DatasetTypes and their StorageClasses.",
            fields=[
                ddl.FieldSpec(
                    name="dataset_type_name",
                    dtype=sqlalchemy.String,
                    length=128,
                    primaryKey=True,
                    nullable=False,
                    doc="Globally unique name for this DatasetType.",
                ),
                ddl.FieldSpec(
                    name="storage_class",
                    dtype=sqlalchemy.String,
                    length=64,
                    nullable=False,
                    doc=(
                        "Name of the StorageClass associated with this DatasetType.  All "
                        "registries must support the full set of standard StorageClasses, "
                        "so the set of allowed StorageClasses and their properties is "
                        "maintained in the registry Python code rather than the database."
                    ),
                ),
            ],
        ),
        dataset_type_dimensions=ddl.TableSpec(
            doc=(
                "A definition table indicating which dimension fields in Dataset are "
                "non-NULL for Datasets with this DatasetType."
            ),
            fields=[
                ddl.FieldSpec(
                    name="dataset_type_name",
                    dtype=sqlalchemy.String,
                    length=128,
                    primaryKey=True,
                    doc="The name of the DatasetType.",
                ),
                ddl.FieldSpec(
                    name="dimension_name",
                    dtype=sqlalchemy.String,
                    length=32,
                    primaryKey=True,
                    doc="The name of a Dimension associated with this DatasetType.",
                ),
            ],
            foreignKeys=[
                ddl.ForeignKeySpec(
                    table="dataset_type",
                    source=("dataset_type_name",),
                    target=("dataset_type_name",),
                )
            ],
        ),
        dataset_collection=dataset_collection,
        quantum=quantum,
        dataset_consumers=ddl.TableSpec(
            doc="A table relating Quantum records to the Datasets they used as inputs.",
            fields=[
                ddl.FieldSpec(
                    name="quantum_id",
                    dtype=sqlalchemy.BigInteger,
                    nullable=False,
                    doc="A link to the associated Quantum.",
                ),
                ddl.FieldSpec(
                    name="dataset_id",
                    dtype=sqlalchemy.BigInteger,
                    nullable=False,
                    doc="A link to the associated Dataset.",
                ),
                ddl.FieldSpec(
                    name="actual",
                    dtype=sqlalchemy.Boolean,
                    nullable=False,
                    doc=(
                        "Whether the Dataset was actually used as an input by the Quantum "
                        "(as opposed to just predicted to be used during preflight)."
                    ),
                ),
            ],
            foreignKeys=[
                ddl.ForeignKeySpec(
                    table="quantum",
                    source=("quantum_id",),
                    target=("id",),
                    onDelete="CASCADE",
                ),
                ddl.ForeignKeySpec(
                    table="dataset",
                    source=("dataset_id",),
                    target=("dataset_id",),
                    onDelete="CASCADE",
                ),
            ],
        ),
        dataset_storage=ddl.TableSpec(
            doc=(
                "A table that provides information on whether a Dataset is stored in "
                "one or more Datastores.  The presence or absence of a record in this "
                "table itself indicates whether the Dataset is present in that "
                "Datastore. "
            ),
            fields=[
                ddl.FieldSpec(
                    name="dataset_id",
                    dtype=sqlalchemy.BigInteger,
                    primaryKey=True,
                    nullable=False,
                    doc="Link to the dataset table.",
                ),
                ddl.FieldSpec(
                    name="datastore_name",
                    dtype=sqlalchemy.String,
                    length=256,
                    primaryKey=True,
                    nullable=False,
                    doc="Name of the Datastore this entry corresponds to.",
                ),
            ],
            foreignKeys=[
                ddl.ForeignKeySpec(
                    table="dataset", source=("dataset_id",), target=("dataset_id",)
                )
            ],
        ),
    )

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
from typing import Type

import sqlalchemy

from ..core import (
    ddl,
    DimensionUniverse,
)
from .interfaces import CollectionManager, DatasetRecordStorageManager


RegistryTablesTuple = namedtuple(
    "RegistryTablesTuple",
    [
        "quantum",
        "dataset_consumers",
    ]
)


def makeRegistryTableSpecs(universe: DimensionUniverse,
                           collections: Type[CollectionManager],
                           datasets: Type[DatasetRecordStorageManager],
                           ) -> RegistryTablesTuple:
    """Construct descriptions tables in the Registry that are not (yet)
    managed by helper classes.

    Parameters
    ----------
    universe : `DimensionUniverse`
        All dimensions known to the `Registry`.
    collections : `Collection`
        The `CollectionManager` that will be used for this `Registry`; used to
        create foreign keys to the run and collection tables.
    datasets : subclass of `DatasetRecordStorageManager`
        Manager class for dataset tables; used only to create foreign keys.

    Returns
    -------
    specs : `RegistryTablesTuple`
        A named tuple containing `ddl.TableSpec` instances.
    """
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

    dataset_consumers = ddl.TableSpec(
        doc="A table relating Quantum records to the datasets they used as inputs.",
        fields=[
            ddl.FieldSpec(
                name="quantum_id",
                dtype=sqlalchemy.BigInteger,
                nullable=False,
                doc="A link to the associated Quantum.",
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
        ]
    )
    datasets.addDatasetForeignKey(dataset_consumers, nullable=True, onDelete="SET NULL")
    return RegistryTablesTuple(
        quantum=quantum,
        dataset_consumers=dataset_consumers,
    )

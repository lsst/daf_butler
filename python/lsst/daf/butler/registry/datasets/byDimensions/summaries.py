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
    "CollectionSummaryTables",
)

from typing import (
    Generic,
    TypeVar,
)

import sqlalchemy

from lsst.daf.butler import (
    ddl,
    GovernorDimension,
    NamedKeyDict,
    NamedKeyMapping,
)
from lsst.daf.butler import addDimensionForeignKey
from lsst.daf.butler.registry.interfaces import (
    CollectionManager,
    Database,
    DimensionRecordStorageManager,
    StaticTablesContext,
)


_T = TypeVar("_T")


class CollectionSummaryTables(Generic[_T]):
    """Structure that holds the table or table specification objects that
    summarize the contents of collections.

    Parameters
    ----------
    datasetType
        Table [specification] that summarizes which dataset types are in each
        collection.
    dimensions
        Mapping of table [specifications] that summarize which governor
        dimension values are present in the data IDs of each collection.
    """
    def __init__(
        self,
        datasetType: _T,
        dimensions: NamedKeyMapping[GovernorDimension, _T],
    ):
        self.datasetType = datasetType
        self.dimensions = dimensions

    @classmethod
    def initialize(
        cls,
        db: Database,
        context: StaticTablesContext, *,
        collections: CollectionManager,
        dimensions: DimensionRecordStorageManager,
    ) -> CollectionSummaryTables[sqlalchemy.schema.Table]:
        """Create all summary tables (or check that they have been created).

        Parameters
        ----------
        db : `Database`
            Interface to the underlying database engine and namespace.
        context : `StaticTablesContext`
            Context object obtained from `Database.declareStaticTables`; used
            to declare any tables that should always be present.
        collections: `CollectionManager`
            Manager object for the collections in this `Registry`.
        dimensions : `DimensionRecordStorageManager`
            Manager object for the dimensions in this `Registry`.

        Returns
        -------
        tables : `CollectionSummaryTables` [ `sqlalchemy.schema.Table` ]
            Structure containing table objects.
        """
        specs = cls.makeTableSpecs(collections, dimensions)
        return CollectionSummaryTables(
            datasetType=context.addTable("collection_summary_dataset_type", specs.datasetType),
            dimensions=NamedKeyDict({
                dimension: context.addTable(f"collection_summary_{dimension.name}", spec)
                for dimension, spec in specs.dimensions.items()
            }).freeze(),
        )

    @classmethod
    def makeTableSpecs(
        cls,
        collections: CollectionManager,
        dimensions: DimensionRecordStorageManager,
    ) -> CollectionSummaryTables[ddl.TableSpec]:
        """Create specifications for all summary tables.

        Parameters
        ----------
        collections: `CollectionManager`
            Manager object for the collections in this `Registry`.
        dimensions : `DimensionRecordStorageManager`
            Manager object for the dimensions in this `Registry`.

        Returns
        -------
        tables : `CollectionSummaryTables` [ `ddl.TableSpec` ]
            Structure containing table specifications.
        """
        # Spec for collection_summary_dataset_type.
        datasetTypeTableSpec = ddl.TableSpec(fields=[])
        collections.addCollectionForeignKey(datasetTypeTableSpec, primaryKey=True, onDelete="CASCADE")
        datasetTypeTableSpec.fields.add(
            ddl.FieldSpec("dataset_type_id", dtype=sqlalchemy.BigInteger, primaryKey=True)
        )
        datasetTypeTableSpec.foreignKeys.append(
            ddl.ForeignKeySpec("dataset_type", source=("dataset_type_id",), target=("id",),
                               onDelete="CASCADE")
        )
        # Specs for collection_summary_<dimension>.
        dimensionTableSpecs = NamedKeyDict[GovernorDimension, ddl.TableSpec]()
        for dimension in dimensions.universe.getGovernorDimensions():
            tableSpec = ddl.TableSpec(fields=[])
            collections.addCollectionForeignKey(tableSpec, primaryKey=True, onDelete="CASCADE")
            addDimensionForeignKey(tableSpec, dimension, primaryKey=True)
            dimensionTableSpecs[dimension] = tableSpec
        return CollectionSummaryTables(
            datasetType=datasetTypeTableSpec,
            dimensions=dimensionTableSpecs.freeze(),
        )

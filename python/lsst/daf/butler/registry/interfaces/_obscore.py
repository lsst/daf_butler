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

"""Interfaces for classes that manage obscore table(s) in a `Registry`.
"""

from __future__ import annotations

__all__ = ["ObsCoreTableManager"]

from abc import abstractmethod
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

import sqlalchemy

from ._versioning import VersionedExtension, VersionTuple

if TYPE_CHECKING:
    from lsst.sphgeom import Region

    from ..._column_type_info import ColumnTypeInfo
    from ..._dataset_ref import DatasetRef
    from ...dimensions import DimensionUniverse
    from ._collections import CollectionRecord
    from ._database import Database, StaticTablesContext
    from ._datasets import DatasetRecordStorageManager
    from ._dimensions import DimensionRecordStorageManager


class ObsCoreTableManager(VersionedExtension):
    """An interface for populating ObsCore tables(s).

    Parameters
    ----------
    registry_schema_version : `VersionTuple` or `None`, optional
        Version of registry schema.
    """

    def __init__(self, *, registry_schema_version: VersionTuple | None = None):
        super().__init__(registry_schema_version=registry_schema_version)

    @abstractmethod
    def clone(
        self,
        *,
        db: Database,
        dimensions: DimensionRecordStorageManager,
    ) -> ObsCoreTableManager:
        """Make an independent copy of this manager instance bound to new
        instances of `Database` and other managers.

        Parameters
        ----------
        db : `Database`
            New `Database` object to use when instantiating the manager.
        dimensions : `DimensionRecordStorageManager`
            New `DimensionRecordStorageManager` object to use when
            instantiating the manager.

        Returns
        -------
        instance : `ObsCoreTableManager`
            New manager instance with the same configuration as this instance,
            but bound to a new Database object.
        """
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def initialize(
        cls,
        db: Database,
        context: StaticTablesContext,
        *,
        universe: DimensionUniverse,
        config: Mapping,
        datasets: type[DatasetRecordStorageManager],
        dimensions: DimensionRecordStorageManager,
        registry_schema_version: VersionTuple | None = None,
        column_type_info: ColumnTypeInfo,
    ) -> ObsCoreTableManager:
        """Construct an instance of the manager.

        Parameters
        ----------
        db : `Database`
            Interface to the underlying database engine and namespace.
        context : `StaticTablesContext`
            Context object obtained from `Database.declareStaticTables`; used
            to declare any tables that should always be present in a layer
            implemented with this manager.
        universe : `DimensionUniverse`
            All dimensions known to the registry.
        config : `dict` [ `str`, `Any` ]
            Configuration of the obscore manager.
        datasets : `type`
            Type of dataset manager.
        dimensions : `DimensionRecordStorageManager`
            Manager for Registry dimensions.
        registry_schema_version : `VersionTuple` or `None`
            Schema version of this extension as defined in registry.
        column_type_info : `ColumnTypeInfo`
            Information about column types that can differ between data
            repositories and registry instances.

        Returns
        -------
        manager : `ObsCoreTableManager`
            An instance of a concrete `ObsCoreTableManager` subclass.
        """
        raise NotImplementedError()

    @abstractmethod
    def config_json(self) -> str:
        """Dump configuration in JSON format.

        Returns
        -------
        json : `str`
            Configuration serialized in JSON format.
        """
        raise NotImplementedError()

    @abstractmethod
    def add_datasets(self, refs: Iterable[DatasetRef]) -> int:
        """Possibly add datasets to the obscore table.

        This method should be called when new datasets are added to a RUN
        collection.

        Parameters
        ----------
        refs : `iterable` [ `DatasetRef` ]
            Dataset refs to add. Dataset refs have to be completely expanded.
            If a record with the same dataset ID is already in obscore table,
            the dataset is ignored.

        Returns
        -------
        count : `int`
            Actual number of records inserted into obscore table.

        Notes
        -----
        Dataset data types and collection names are checked against configured
        list of collections and dataset types, non-matching datasets are
        ignored and not added to the obscore table.

        When configuration parameter ``collection_type`` is not "RUN", this
        method should return immediately.

        Note that there is no matching method to remove datasets from obscore
        table, we assume that removal happens via foreign key constraint to
        dataset table with "ON DELETE CASCADE" option.
        """
        raise NotImplementedError()

    @abstractmethod
    def associate(self, refs: Iterable[DatasetRef], collection: CollectionRecord) -> int:
        """Possibly add datasets to the obscore table.

        This method should be called when existing datasets are associated with
        a TAGGED collection.

        Parameters
        ----------
        refs : `iterable` [ `DatasetRef` ]
            Dataset refs to add. Dataset refs have to be completely expanded.
            If a record with the same dataset ID is already in obscore table,
            the dataset is ignored.
        collection : `CollectionRecord`
            Collection record for a TAGGED collection.

        Returns
        -------
        count : `int`
            Actual number of records inserted into obscore table.

        Notes
        -----
        Dataset data types and collection names are checked against configured
        list of collections and dataset types, non-matching datasets are
        ignored and not added to the obscore table.

        When configuration parameter ``collection_type`` is not "TAGGED", this
        method should return immediately.
        """
        raise NotImplementedError()

    @abstractmethod
    def disassociate(self, refs: Iterable[DatasetRef], collection: CollectionRecord) -> int:
        """Possibly remove datasets from the obscore table.

        This method should be called when datasets are disassociated from a
        TAGGED collection.

        Parameters
        ----------
        refs : `iterable` [ `DatasetRef` ]
            Dataset refs to remove. Dataset refs have to be resolved.
        collection : `CollectionRecord`
            Collection record for a TAGGED collection.

        Returns
        -------
        count : `int`
            Actual number of records removed from obscore table.

        Notes
        -----
        Dataset data types and collection names are checked against configured
        list of collections and dataset types, non-matching datasets are
        ignored and not added to the obscore table.

        When configuration parameter ``collection_type`` is not "TAGGED", this
        method should return immediately.
        """
        raise NotImplementedError()

    @abstractmethod
    def update_exposure_regions(self, instrument: str, region_data: Iterable[tuple[int, int, Region]]) -> int:
        """Update existing exposure records with spatial region data.

        Parameters
        ----------
        instrument : `str`
            Instrument name.
        region_data : `~collections.abc.Iterable` [`tuple` [`int`, `int`, \
                `~lsst.sphgeom.Region` ]]
            Sequence of tuples, each tuple contains three values - exposure ID,
            detector ID, and corresponding region.

        Returns
        -------
        count : `int`
            Actual number of records updated.

        Notes
        -----
        This method is needed to update obscore records for raw exposures which
        are ingested before their corresponding visits are defined. Exposure
        records added when visit is already defined will get their regions
        from their matching visits automatically.
        """
        raise NotImplementedError()

    @abstractmethod
    @contextmanager
    def query(
        self, columns: Iterable[str | sqlalchemy.sql.expression.ColumnElement] | None = None, /, **kwargs: Any
    ) -> Iterator[sqlalchemy.engine.CursorResult]:
        """Run a SELECT query against obscore table and return result rows.

        Parameters
        ----------
        columns : `~collections.abc.Iterable` [`str`]
            Columns to return from query. It is a sequence which can include
            column names or any other column elements (e.g.
            `sqlalchemy.sql.functions.count` function).
        **kwargs
            Restriction on values of individual obscore columns. Key is the
            column name, value is the required value of the column. Multiple
            restrictions are ANDed together.

        Returns
        -------
        result_context : `sqlalchemy.engine.CursorResult`
            Context manager that returns the query result object when entered.
            These results are invalidated when the context is exited.
        """
        raise NotImplementedError()

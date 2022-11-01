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

"""Interfaces for classes that manage obscore table(s) in a `Registry`.
"""

__all__ = ["ObsCoreTableManager"]

from abc import abstractmethod
from collections.abc import Mapping
from typing import TYPE_CHECKING, Iterable, Type

from ._versioning import VersionedExtension

if TYPE_CHECKING:
    from ...core import DatasetRef, DimensionUniverse
    from ._collections import CollectionRecord
    from ._database import Database, StaticTablesContext
    from ._datasets import DatasetRecordStorageManager
    from ._dimensions import DimensionRecordStorageManager


class ObsCoreTableManager(VersionedExtension):
    """An interface for populating ObsCore tables(s)."""

    @classmethod
    @abstractmethod
    def initialize(
        cls,
        db: Database,
        context: StaticTablesContext,
        *,
        universe: DimensionUniverse,
        config: Mapping,
        datasets: Type[DatasetRecordStorageManager],
        dimensions: DimensionRecordStorageManager,
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
        dimensions: `DimensionRecordStorageManager`
            Manager for Registry dimensions.

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

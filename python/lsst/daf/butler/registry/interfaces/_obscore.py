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
from typing import TYPE_CHECKING, Iterable, Optional, Type

from ._versioning import VersionedExtension

if TYPE_CHECKING:
    from ...core import DatasetRef, DimensionUniverse
    from ._collections import CollectionManager
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
        collections: CollectionManager,
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
        collections : `CollectionManager`
            Manager of Registry collections.
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

    def add_datasets(self, refs: Iterable[DatasetRef], collection: Optional[str] = None) -> None:
        """Possibly add datasets to the obscore table.

        Parameters
        ----------
        refs : `iterable` [ `DatasetRef` ]
            Dataset refs to add. Dataset refs have to be completely expanded.
            If a record with the same dataset ID is already in obscore table,
            the dataset is ignored.
        collection : `str`, optional
            Name of a collection. This should be set to `None` when datasets
            are added to their run collection, in which case datasets' own run
            collection name is used. When existing dataset are being associated
            with TAGGED or CALIBRATION collections, this parameter should be
            set to the name of the associated collection.

        Notes
        -----
        Dataset data types and collection names are checked against configured
        list of collections and dataset types, non-matching datasets are
        ignored and not added to the obscore table. When ``collection`` is
        specified it is used instead of dataset run name for filtering
        purposes, but obscore record will still store original dataset run
        name (when configuration defines the column for it).
        """
        raise NotImplementedError()

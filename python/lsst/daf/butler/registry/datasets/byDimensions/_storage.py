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

__all__ = ("ByDimensionsDatasetRecordStorageUUID",)

from collections.abc import Callable
from typing import TYPE_CHECKING

import sqlalchemy

from ...._dataset_type import DatasetType
from ...interfaces import DatasetRecordStorage

if TYPE_CHECKING:
    from ...interfaces import CollectionManager, Database
    from .summaries import CollectionSummaryManager
    from .tables import StaticDatasetTablesTuple


class ByDimensionsDatasetRecordStorageUUID(DatasetRecordStorage):
    """Dataset record storage implementation paired with
    `ByDimensionsDatasetRecordStorageManagerUUID`; see that class for more
    information.

    Instances of this class should never be constructed directly; use
    `DatasetRecordStorageManager.register` instead.

    Parameters
    ----------
    datasetType : `DatasetType`
        The dataset type to use.
    db : `Database`
        Database connection.
    dataset_type_id : `int`
        Dataset type identifier.
    collections : `CollectionManager`
        The collection manager.
    static : `StaticDatasetTablesTuple`
        Unknown.
    summaries : `CollectionSummaryManager`
        Collection summary manager.
    tags_table_factory : `~collections.abc.Callable`
        Factory for creating tags tables.
    use_astropy_ingest_date : `bool`
        Whether to use Astropy for ingest date.
    calibs_table_factory : `~collections.abc.Callable`
        Factory for creating calibration tables.
    """

    def __init__(
        self,
        *,
        datasetType: DatasetType,
        db: Database,
        dataset_type_id: int,
        collections: CollectionManager,
        static: StaticDatasetTablesTuple,
        summaries: CollectionSummaryManager,
        tags_table_factory: Callable[[], sqlalchemy.schema.Table],
        use_astropy_ingest_date: bool,
        calibs_table_factory: Callable[[], sqlalchemy.schema.Table] | None,
    ):
        super().__init__(datasetType=datasetType)
        self.dataset_type_id = dataset_type_id
        self._db = db
        self._collections = collections
        self._static = static
        self._summaries = summaries
        self._tags_table_factory = tags_table_factory
        self._calibs_table_factory = calibs_table_factory
        self._runKeyColumn = collections.getRunForeignKeyName()
        self._use_astropy = use_astropy_ingest_date
        self._tags_table: sqlalchemy.schema.Table | None = None
        self._calibs_table: sqlalchemy.schema.Table | None = None

    @property
    def tags(self) -> sqlalchemy.schema.Table:
        if self._tags_table is None:
            self._tags_table = self._tags_table_factory()
        return self._tags_table

    @property
    def calibs(self) -> sqlalchemy.schema.Table | None:
        if self._calibs_table is None:
            if self._calibs_table_factory is None:
                return None
            self._calibs_table = self._calibs_table_factory()
        return self._calibs_table

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

__all__ = ["DimensionRecordStorage"]

from abc import ABC, abstractmethod
from typing import Optional

from sqlalchemy.sql import FromClause

from ...core import DataId, DimensionElement, DimensionRecord


class DimensionRecordStorage(ABC):
    """An abstract base class that represents a way of storing the records
    associated with a single `DimensionElement`.

    Concrete `DimensionRecordStorage` instances should generally be constructed
    via a call to `setupDimensionStorage`, which selects the appropriate
    subclass for each element according to its configuration.

    All `DimensionRecordStorage` methods are pure abstract, even though in some
    cases a reasonable default implementation might be possible, in order to
    better guarantee all methods are correctly overridden.  All of these
    potentially-defaultable implementations are extremely trivial, so asking
    subclasses to provide them is not a significant burden.
    """

    @property
    @abstractmethod
    def element(self) -> DimensionElement:
        """The element whose records this instance holds (`DimensionElement`).
        """
        raise NotImplementedError()

    @abstractmethod
    def clearCaches(self):
        """Clear any in-memory caches held by the storage instance.

        This is called by `Registry` when transactions are rolled back, to
        avoid in-memory caches from ever containing records that are not
        present in persistent storage.
        """
        raise NotImplementedError()

    @abstractmethod
    def matches(self, dataId: Optional[DataId] = None) -> bool:
        """Test whether this storage could hold any records consistent with the
        given data ID.

        Parameters
        ----------
        dataId : `DataId`, optional
            The data ID to test.  May be an informal data ID dictionary or
            a validated `DataCoordinate`.

        Returns
        -------
        matches : `bool`
            `True` if this storage might hold a record whose data ID matches
            the given on; this is not a guarantee that any such record exists.
            `False` only if a matching record definitely does not exist.
        """
        raise NotImplementedError()

    @abstractmethod
    def getElementTable(self, dataId: Optional[DataId] = None) -> FromClause:
        """Return the logical table for the element as a SQLAlchemy object.

        The returned object may be a select statement or view instead of a
        true table.

        The caller is responsible for checking that the element actually has
        a table (via `DimensionElement.hasTable`).  The exception raised when
        this is not true is unspecified.

        Parameters
        ----------
        dataId : `DataId`, optional
            A data ID that restricts any query that includes the returned
            logical table, which may be used by implementations to return
            a simpler object in some contexts (for example, if the records
            are split across multiple tables that in general must be combined
            via a UNION query).  Implementations are *not* required to
            apply a filter based on this ID, and should only do so when it
            allows them to simplify what is returned.

        Returns
        -------
        table : `sqlalchemy.sql.FromClause`
            A table or table-like SQLAlchemy expression object that can be
            included in a select query.
        """
        raise NotImplementedError()

    @abstractmethod
    def getCommonSkyPixOverlapTable(self, dataId: Optional[DataId] = None) -> FromClause:
        """Return the logical table that relates the given element to the
        common skypix dimension.

        The returned object may be a select statement or view instead of a
        true table.

        The caller is responsible for checking that the element actually has
        a skypix overlap table, which is the case when
        `DimensionElement.hasTable` and `DimensionElement.spatial` are both
        `True`.

        Parameters
        ----------
        dataId : `DataId`, optional
            A data ID that restricts any query that includes the returned
            logical table, which may be used by implementations to return
            a simpler object in some contexts (for example, if the records
            are split across multiple tables that in general must be combined
            via a UNION query).  Implementations are *not* required to
            apply a filter based on this ID, and should only do so when it
            allows them to simplify what is returned.

        Returns
        -------
        table : `sqlalchemy.sql.FromClause`
            A table or table-like SQLAlchemy expression object that can be
            included in a select query.
        """
        raise NotImplementedError()

    @abstractmethod
    def insert(self, *records: DimensionRecord):
        """Insert one or more records into storage.

        Parameters
        ----------
        records
            One or more instances of the `DimensionRecord` subclass for the
            element this storage is associated with.

        Raises
        ------
        TypeError
            Raised if the element does not support record insertion.
        sqlalchemy.exc.IntegrityError
            Raised if one or more records violate database integrity
            constraints.

        Notes
        -----
        As `insert` is expected to be called only by a `Registry`, we rely
        on `Registry` to provide transactionality, both by using a SQLALchemy
        connection shared with the `Registry` and by relying on it to call
        `clearCaches` when rolling back transactions.
        """
        raise NotImplementedError()

    @abstractmethod
    def fetch(self, dataId: DataId) -> Optional[DimensionRecord]:
        """Retrieve a record from storage.

        Parameters
        ----------
        dataId : `DataId`
            A data ID that identifies the record to be retrieved.  This may
            be an informal data ID dict or a validated `DataCoordinate`.

        Returns
        -------
        record : `DimensionRecord` or `None`
            A record retrieved from storage, or `None` if there is no such
            record.
        """
        raise NotImplementedError()

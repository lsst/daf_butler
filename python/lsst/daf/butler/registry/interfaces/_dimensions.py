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
from typing import Optional, TYPE_CHECKING

import sqlalchemy

if TYPE_CHECKING:
    from ..queries import QueryBuilder
    from ..core import DataId, DimensionElement, DimensionRecord, Timespan
    from ..core.utils import NamedKeyDict


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
    def join(
        self,
        builder: QueryBuilder, *,
        regions: Optional[NamedKeyDict[DimensionElement, sqlalchemy.sql.ColumnElement]] = None,
        timespans: Optional[NamedKeyDict[DimensionElement, Timespan[sqlalchemy.sql.ColumnElement]]] = None,
    ):
        """Add the dimension element's logical table to a query under
        construction.

        This is a visitor pattern interface that is expected to be called only
        by `QueryBuilder.joinDimensionElement`.

        Parameters
        ----------
        builder : `QueryBuilder`
            Builder for the query that should contain this element.
        regions : `NamedKeyDict`, optional
            A mapping from `DimensionElement` to a SQLAlchemy column containing
            the region for that element, which should be updated to include a
            region column for this element if one exists.  If `None`,
            ``self.element`` is not being included in the query via a spatial
            join.
        timespan : `NamedKeyDict`, optional
            A mapping from `DimensionElement` to a `Timespan` of SQLALchemy
            columns containing the timespan for that element, which should be
            updated to include timespan columns for this element if they exist.
            If `None`, ``self.element`` is not being included in the query via
            a temporal join.

        Notes
        -----
        Elements are only included in queries via spatial and/or temporal joins
        when necessary to connect them to other elements in the query, so
        ``regions`` and ``timespans`` cannot be assumed to be not `None` just
        because an element has a region or timespan.
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

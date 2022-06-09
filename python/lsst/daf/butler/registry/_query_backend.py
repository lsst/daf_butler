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

__all__ = ("QueryBackend",)

from abc import ABC, abstractmethod
from typing import AbstractSet, TYPE_CHECKING

if TYPE_CHECKING:
    from ..core import DimensionUniverse, sql


class QueryBackend(ABC):
    """An interface for `sql.Relation` and `sql.QueryContext` creation to be
    specialized for different `Registry` implementations.
    """

    @abstractmethod
    def context(self) -> sql.QueryContext:
        """Return a context manager for relation query execution and other
        operations that involve an active database connection.

        Return
        ------
        context : `QueryContext`
            Context manager.
        """
        raise NotImplementedError()

    @property
    def universe(self) -> DimensionUniverse:
        """Definition of all dimensions and dimension elements for this
        registry.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def unit_relation(self) -> sql.Relation:
        """A special `Relation` that has no columns but one (conceptual)
        row.

        Joining this relation to any other relation returns the other relation.
        """
        raise NotImplementedError()

    @abstractmethod
    def make_doomed_relation(self, *messages: str, columns: AbstractSet[sql.ColumnTag]) -> sql.Relation:
        """Return a `Relation` object that has no rows, with diagnostic
        messages explaining why.

        Parameters
        ----------
        *messages : `str`
            Diagnostics messages explaining the absence of rows.
        columns : `AbstractSet` [ `ColumnTag` ]
            Columns the relation has.  In some contexts this may not matter,
            since doomed queries should never be executed, but it is necessary
            to allow this special relation to behave like any other.
        """
        raise NotImplementedError()

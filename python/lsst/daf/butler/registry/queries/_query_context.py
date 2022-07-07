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

__all__ = ("QueryContext",)

from abc import abstractmethod
from typing import ContextManager

from lsst.daf.relation import Relation, iteration

from ...core import ColumnTag


class QueryContext(ContextManager["QueryContext"]):
    """A context manager interface for query operations that require some
    connection-like state.
    """

    @abstractmethod
    def fetch_iterable(self, relation: Relation[ColumnTag]) -> iteration.RowIterable[ColumnTag]:
        """Execute the SQL query represented by a relation and return the
        results as a possibly-lazy iterable.

        Parameters
        ----------
        relation : `Relation`
            Relation representing the query to execute.

        Returns
        -------
        rows : `~lsst.daf.relation.iteration.RowIterable`
            An iterable over rows, with each row a mapping from `ColumnTag`
            to column value.
        """
        raise NotImplementedError()

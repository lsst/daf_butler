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
from typing import ContextManager, Iterable, Iterator, Optional

from ._column_tags import ResultRow
from ._order_by import OrderByTerm
from ._relation import Relation


class QueryContext(ContextManager["QueryContext"]):
    """A context manager interface for query operations that typically involve
    an open database connection.
    """

    @abstractmethod
    def fetch(
        self,
        relation: Relation,
        force_unique: bool = False,
        order_by: Iterable[OrderByTerm] = (),
        offset: int = 0,
        limit: Optional[int] = None,
    ) -> Iterator[ResultRow]:
        """Execute the SQL query represented by a relation and return the
        results, applying any postprocessors held by the relation.

        Parameters
        ----------
        relation : `Relation`
            Relation representing the query to execute.
        force_unique : `bool`, optional
            If `True`, force returned rows to be unique (e.g. via
            ``SELECT DISTINCT``).
        order_by : `Iterable` [ `OrderByTerm` ], operation.
            Columns or column expressions used to order rows.
        offset : `int`, optional
            Integer index of the first row in the set to return, starting from
            zero.  This is applied before any postprocessors are run.
        limit : `int`, optional
            Maximum number of rows prior to postprocessing.

        Returns
        -------
        rows : `Iterator` [ `Mapping` [ `ColumnTag`, `object` ] ]
            An iterator over rows, with each row a mapping from `ColumnTag`
            to column value.
        """
        raise NotImplementedError()

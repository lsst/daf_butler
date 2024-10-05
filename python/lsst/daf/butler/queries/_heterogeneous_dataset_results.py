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

__all__ = ("HeterogeneousDatasetRefQueryResults",)

from collections.abc import Iterable, Iterator

from .._dataset_ref import DatasetId, DatasetRef
from ._base import QueryBase


class HeterogeneousDatasetRefQueryResults(QueryBase):
    """A query result object for datasets with multiple dataset types."""

    def __iter__(self) -> Iterator[DatasetRef]:
        raise NotImplementedError()

    def ids(self) -> Iterable[DatasetId]:
        """Iterate over just the dataset IDs.

        This may return a lazy-single pass iterator or a regular in-memory
        iterable, in order to allow for the possibility that it may be
        upgraded into a query results object in the future.
        """
        # In some cases - depending on the WHERE clause and other things joined
        # in - this could result in a single query, rather than a Python-side
        # aggregation of per-dimension-group queries.
        raise NotImplementedError()

    def any(self, *, execute: bool = True, exact: bool = True) -> bool:
        # Docstring inherited.
        raise NotImplementedError("Base class implementation is not correct for this derived class.")

    def explain_no_results(self, execute: bool = True) -> Iterable[str]:
        # Docstring inherited.
        raise NotImplementedError("Base class implementation is not correct for this derived class.")

    def count(self, *, exact: bool = True, discard: bool = False) -> int:
        """Count the number of rows this query would return.

        Parameters
        ----------
        exact : `bool`, optional
            If `True`, run the full query and perform post-query filtering if
            needed to account for that filtering in the count.  If `False`, the
            result may be an upper bound.
        discard : `bool`, optional
            If `True`, compute the exact count even if it would require running
            the full query and then throwing away the result rows after
            counting them.  If `False`, this is an error, as the user would
            usually be better off executing the query first to fetch its rows
            into a new query (or passing ``exact=False``).  Ignored if
            ``exact=False``.

        Returns
        -------
        count : `int`
            The number of rows the query would return, or an upper bound if
            ``exact=False``.
        """
        raise NotImplementedError()

    # This class intentionally lacks some attributes that are defined on other
    # QueryResults objects:
    #
    # - 'dimensions' isn't well-defined in general.
    #
    # - 'order_by' and 'limit' are hard to implement in the common case where
    #   we have to run one query for each dimension group.
    #
    # - 'where' exists on other result objects because the way they are
    #   constructed adds context (a dataset search join, some dimensions) that
    #   can help interpret arguments to 'where'.  That's not generally true
    #   here, so calling `Query.where(...).all_datasets()` can do anything that
    #   `Query.all_datasets().where(...)` might be able to do.

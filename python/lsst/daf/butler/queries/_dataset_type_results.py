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

__all__ = ("DatasetTypeQueryResults",)

from collections.abc import Iterable, Iterator

from .._dataset_type import DatasetType


class DatasetTypeQueryResults:
    """A query result object that summarizes a query for datasets by doing the
    equivalent of a SQL GROUP BY on the dataset type.
    """

    def __iter__(self) -> Iterator[DatasetType]:
        raise NotImplementedError()

    def names(self) -> Iterable[str]:
        """Iterate over the names of the matched dataset types."""
        raise NotImplementedError()

    def by_collection(
        self,
        *,
        flatten_chains: bool = False,
        include_chains: bool | None = None,
    ) -> Iterable[tuple[str, Iterable[DatasetType]]]:
        """Iterate over results while grouping by collection as well as dataset
        type.

        Parameters
        ----------
        flatten_chains : `bool`, optional
            If `True` (`False` is default), expand the child collections of
            matching `~CollectionType.CHAINED` collections in the results.
        include_chains : `bool` or `None`, optional
            If `True`, yield records for matching `~CollectionType.CHAINED`
            collections. Default is the opposite of ``flatten_chains``:
            include either CHAINED collections or their children, but not both.

        Returns
        -------
        rows : `~collections.abc.Iterable` [ `tuple` ]
            An iterable of ``(collection, dataset_types)`` pairs.  The
            ``dataset_types`` values are guaranteed to be regular in-memory
            iterables, not lazy single-pass iterators, but the exact type
            of iterable is left unspecified to leave room for future
            improvements.
        """
        raise NotImplementedError()

    def with_counts(self, find_first: bool = True) -> Iterable[tuple[DatasetType, int]]:
        """Iterate over results with counts for the number of datasets of each
        type.

        Parameters
        ----------
        find_first : `bool`, optional
            If `True` (default), only count unique dataset type + data ID
            combinations, not shadowed datasets.

        Returns
        -------
        rows : `tuple` [ `DatasetRef`, `int` ]
            An iterable of ``(dataset_type, count)`` pairs.
        """
        raise NotImplementedError()

    def by_collection_with_counts(
        self,
        *,
        flatten_chains: bool = False,
        include_chains: bool | None = None,
    ) -> Iterable[tuple[str, Iterable[tuple[DatasetType, int]]]]:
        """Iterate over results while grouping by collection as well as dataset
        type, and counting the number of datasets in each combination.

        Parameters
        ----------
        flatten_chains : `bool`, optional
            If `True` (`False` is default), expand the child collections of
            matching `~CollectionType.CHAINED` collections in the results.
        include_chains : `bool` or `None`, optional
            If `True`, yield records for matching `~CollectionType.CHAINED`
            collections. Default is the opposite of ``flatten_chains``:
            include either CHAINED collections or their children, but not both.

        Returns
        -------
        rows : `~collections.abc.Iterable` [ `tuple` ]
            An iterable of ``(collection, dataset_types_with_counts)`` pairs,
            with the latter an iterable of ``(DatasetType, int`)``.
            These inner iterables are guaranteed to be regular in-memory
            iterables, not lazy single-pass iterators, but the exact type of
            iterable is left unspecified to leave room for future improvements.
        """
        raise NotImplementedError()

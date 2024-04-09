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

__all__ = ("ButlerCollections",)

from abc import ABC, abstractmethod
from collections.abc import Iterable


class ButlerCollections(ABC):
    """Methods for working with collections stored in the Butler."""

    @abstractmethod
    def extend_chain(self, parent_collection_name: str, child_collection_names: str | Iterable[str]) -> None:
        """Add children to the end of a CHAINED collection.

        If any of the children already existed in the chain, they will be moved
        to the new position at the end of the chain.

        Parameters
        ----------
        parent_collection_name : `str`
            The name of a CHAINED collection to which we will add new children.
        child_collection_names : `~collections.abc.Iterable` [ `str ` ] | `str`
            A child collection name or list of child collection names to be
            added to the parent.

        Raises
        ------
        MissingCollectionError
            If any of the specified collections do not exist.
        CollectionTypeError
            If the parent collection is not a CHAINED collection.
        CollectionCycleError
            If this operation would create a collection cycle.

        Notes
        -----
        If this function is called within a call to ``Butler.transaction``, it
        will hold a lock that prevents other processes from modifying the
        parent collection until the end of the transaction.  Keep these
        transactions short.
        """
        raise NotImplementedError()

    @abstractmethod
    def prepend_chain(self, parent_collection_name: str, child_collection_names: str | Iterable[str]) -> None:
        """Add children to the beginning of a CHAINED collection.

        If any of the children already existed in the chain, they will be moved
        to the new position at the beginning of the chain.

        Parameters
        ----------
        parent_collection_name : `str`
            The name of a CHAINED collection to which we will add new children.
        child_collection_names : `~collections.abc.Iterable` [ `str ` ] | `str`
            A child collection name or list of child collection names to be
            added to the parent.

        Raises
        ------
        MissingCollectionError
            If any of the specified collections do not exist.
        CollectionTypeError
            If the parent collection is not a CHAINED collection.
        CollectionCycleError
            If this operation would create a collection cycle.

        Notes
        -----
        If this function is called within a call to ``Butler.transaction``, it
        will hold a lock that prevents other processes from modifying the
        parent collection until the end of the transaction.  Keep these
        transactions short.
        """
        raise NotImplementedError()

    @abstractmethod
    def redefine_chain(
        self, parent_collection_name: str, child_collection_names: str | Iterable[str]
    ) -> None:
        """Replace the contents of a CHAINED collection with new children.

        Parameters
        ----------
        parent_collection_name : `str`
            The name of a CHAINED collection to which we will assign new
            children.
        child_collection_names : `~collections.abc.Iterable` [ `str ` ] | `str`
            A child collection name or list of child collection names to be
            added to the parent.

        Raises
        ------
        MissingCollectionError
            If any of the specified collections do not exist.
        CollectionTypeError
            If the parent collection is not a CHAINED collection.
        CollectionCycleError
            If this operation would create a collection cycle.

        Notes
        -----
        If this function is called within a call to ``Butler.transaction``, it
        will hold a lock that prevents other processes from modifying the
        parent collection until the end of the transaction.  Keep these
        transactions short.
        """
        raise NotImplementedError()

    @abstractmethod
    def remove_from_chain(
        self, parent_collection_name: str, child_collection_names: str | Iterable[str]
    ) -> None:
        """Remove children from a CHAINED collection.

        Parameters
        ----------
        parent_collection_name : `str`
            The name of a CHAINED collection from which we will remove
            children.
        child_collection_names : `~collections.abc.Iterable` [ `str ` ] | `str`
            A child collection name or list of child collection names to be
            removed from the parent.

        Raises
        ------
        MissingCollectionError
            If any of the specified collections do not exist.
        CollectionTypeError
            If the parent collection is not a CHAINED collection.

        Notes
        -----
        If this function is called within a call to ``Butler.transaction``, it
        will hold a lock that prevents other processes from modifying the
        parent collection until the end of the transaction.  Keep these
        transactions short.
        """
        raise NotImplementedError()

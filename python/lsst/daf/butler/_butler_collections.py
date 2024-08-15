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

__all__ = ("ButlerCollections", "CollectionInfo")

from abc import ABC, abstractmethod
from collections.abc import Iterable, Sequence, Set
from typing import Any, overload

from pydantic import BaseModel

from ._collection_type import CollectionType


class CollectionInfo(BaseModel):
    """Information about a single Butler collection."""

    name: str
    """Name of the collection."""
    type: CollectionType
    """Type of the collection."""
    doc: str = ""
    """Documentation string associated with this collection."""
    children: tuple[str, ...] = tuple()
    """Children of this collection (only if CHAINED)."""
    parents: frozenset[str] = frozenset()
    """Any parents of this collection."""


class ButlerCollections(ABC, Sequence):
    """Methods for working with collections stored in the Butler."""

    @overload
    def __getitem__(self, index: int) -> str: ...

    @overload
    def __getitem__(self, index: slice) -> Sequence[str]: ...

    def __getitem__(self, index: int | slice) -> str | Sequence[str]:
        return self.defaults[index]

    def __len__(self) -> int:
        return len(self.defaults)

    def __eq__(self, other: Any) -> bool:
        # Do not try to compare registry instances.
        if not isinstance(other, type(self)):
            return False
        return self.defaults == other.defaults

    @property
    @abstractmethod
    def defaults(self) -> Sequence[str]:
        """Collection defaults associated with this butler."""
        raise NotImplementedError("Defaults must be implemented by a subclass")

    @abstractmethod
    def extend_chain(self, parent_collection_name: str, child_collection_names: str | Iterable[str]) -> None:
        """Add children to the end of a CHAINED collection.

        If any of the children already existed in the chain, they will be moved
        to the new position at the end of the chain.

        Parameters
        ----------
        parent_collection_name : `str`
            The name of a CHAINED collection to which we will add new children.
        child_collection_names : `~collections.abc.Iterable` [ `str` ] | `str`
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
        child_collection_names : `~collections.abc.Iterable` [ `str` ] | `str`
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
        child_collection_names : `~collections.abc.Iterable` [ `str` ] | `str`
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
        child_collection_names : `~collections.abc.Iterable` [ `str` ] | `str`
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

    @abstractmethod
    def x_query(
        self,
        expression: str | Iterable[str],
        collection_types: Set[CollectionType] | CollectionType | None = None,
        flatten_chains: bool = False,
        include_chains: bool | None = None,
    ) -> Sequence[str]:
        """Query the butler for collections matching an expression.

        Parameters
        ----------
        expression : `str` or `~collections.abc.Iterable` [ `str` ]
            One or more collection names or globs to include in the search.
        collection_types : `set` [`CollectionType`], `CollectionType` or `None`
            Restrict the types of collections to be searched. If `None` all
            collection types are searched.
        flatten_chains : `bool`, optional
            If `True` (`False` is default), recursively yield the child
            collections of matching `~CollectionType.CHAINED` collections.
        include_chains : `bool` or `None`, optional
            If `True`, yield records for matching `~CollectionType.CHAINED`
            collections. Default is the opposite of ``flatten_chains``:
            include either CHAINED collections or their children, but not both.

        Returns
        -------
        collections : `~collections.abc.Sequence` [ `str` ]
            The names of collections that match ``expression``.

        Notes
        -----
        The order in which collections are returned is unspecified, except that
        the children of a `~CollectionType.CHAINED` collection are guaranteed
        to be in the order in which they are searched.  When multiple parent
        `~CollectionType.CHAINED` collections match the same criteria, the
        order in which the two lists appear is unspecified, and the lists of
        children may be incomplete if a child has multiple parents.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_info(self, name: str, include_doc: bool = False, include_parents: bool = False) -> CollectionInfo:
        """Obtain information for a specific collection.

        Parameters
        ----------
        name : `str`
            The name of the collection of interest.
        include_doc : `bool`, optional
            If `True` any documentation about this collection will be included.
        include_parents : `bool`, optional
           If `True` any parents of this collection will be included.

        Returns
        -------
        info : `CollectionInfo`
            Information on the requested collection.
        """
        raise NotImplementedError()

    @abstractmethod
    def register(self, name: str, type: CollectionType = CollectionType.RUN, doc: str | None = None) -> bool:
        """Add a new collection if one with the given name does not exist.

        Parameters
        ----------
        name : `str`
            The name of the collection to create.
        type : `CollectionType`, optional
            Enum value indicating the type of collection to create. Default
            is to create a RUN collection.
        doc : `str`, optional
            Documentation string for the collection.

        Returns
        -------
        registered : `bool`
            Boolean indicating whether the collection was already registered
            or was created by this call.

        Notes
        -----
        This method cannot be called within transactions, as it needs to be
        able to perform its own transaction to be concurrent
        """
        raise NotImplementedError()

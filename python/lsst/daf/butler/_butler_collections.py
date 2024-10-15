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
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence, Set
from typing import TYPE_CHECKING, Any, overload

from deprecated.sphinx import deprecated
from pydantic import BaseModel

from ._collection_type import CollectionType

if TYPE_CHECKING:
    from ._dataset_type import DatasetType


class CollectionInfo(BaseModel):
    """Information about a single Butler collection."""

    # This class is serialized for the server API -- any new properties you add
    # must have default values provided to preserve backwards compatibility.

    name: str
    """Name of the collection."""
    type: CollectionType
    """Type of the collection."""
    doc: str = ""
    """Documentation string associated with this collection."""
    children: tuple[str, ...] = tuple()
    """Children of this collection (only if CHAINED)."""
    parents: frozenset[str] | None = None
    """Any parents of this collection.

    `None` if the parents were not requested.
    """
    dataset_types: frozenset[str] | None = None
    """Names of any dataset types associated with datasets in this collection.

    `None` if no dataset type information was requested
    """

    def __lt__(self, other: Any) -> bool:
        """Compare objects by collection name."""
        if not isinstance(other, type(self)):
            return NotImplemented
        return self.name < other.name


class ButlerCollections(ABC, Sequence):
    """Methods for working with collections stored in the Butler."""

    @overload
    def __getitem__(self, index: int) -> str: ...

    @overload
    def __getitem__(self, index: slice) -> Sequence[str]: ...

    @deprecated(
        "‘Butler.collections’ should no longer be used to get the list of default collections."
        " Use ‘Butler.collections.default’ instead. Will be removed after v28.",
        version="v28",
        category=FutureWarning,
    )
    def __getitem__(self, index: int | slice) -> str | Sequence[str]:
        return self.defaults[index]

    @deprecated(
        "‘Butler.collections’ should no longer be used to get the list of default collections."
        " Use ‘Butler.collections.default’ instead. Will be removed after v28.",
        version="v28",
        category=FutureWarning,
    )
    def __len__(self) -> int:
        return len(self.defaults)

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

    def query(
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

        The default implementation is a wrapper around `x_query_info`.
        """
        collections_info = self.query_info(
            expression,
            collection_types=collection_types,
            flatten_chains=flatten_chains,
            include_chains=include_chains,
        )
        return [info.name for info in collections_info]

    @abstractmethod
    def query_info(
        self,
        expression: str | Iterable[str],
        collection_types: Set[CollectionType] | CollectionType | None = None,
        flatten_chains: bool = False,
        include_chains: bool | None = None,
        include_parents: bool = False,
        include_summary: bool = False,
        include_doc: bool = False,
        summary_datasets: Iterable[DatasetType] | Iterable[str] | None = None,
    ) -> Sequence[CollectionInfo]:
        """Query the butler for collections matching an expression and
        return detailed information about those collections.

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
        include_parents : `bool`, optional
            Whether the returned information includes parents.
        include_summary : `bool`, optional
            Whether the returned information includes dataset type and
            governor information for the collections.
        include_doc : `bool`, optional
            Whether the returned information includes collection documentation
            string.
        summary_datasets : `~collections.abc.Iterable` [ `DatasetType` ] or \
            `~collections.abc.Iterable` [ `str` ], optional
            Dataset types to include in returned summaries. Only used if
            ``include_summary`` is `True`. If not specified then all dataset
            types will be included.

        Returns
        -------
        collections : `~collections.abc.Sequence` [ `CollectionInfo` ]
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
    def get_info(
        self, name: str, include_parents: bool = False, include_summary: bool = False
    ) -> CollectionInfo:
        """Obtain information for a specific collection.

        Parameters
        ----------
        name : `str`
            The name of the collection of interest.
        include_parents : `bool`, optional
           If `True` any parents of this collection will be included.
        include_summary : `bool`, optional
           If `True` dataset type names and governor dimensions of datasets
           stored in this collection will be included in the result.

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

    @abstractmethod
    def x_remove(self, name: str) -> None:
        """Remove the given collection from the registry.

        **This is an experimental interface that can change at any time.**

        Parameters
        ----------
        name : `str`
            The name of the collection to remove.

        Raises
        ------
        lsst.daf.butler.registry.MissingCollectionError
            Raised if no collection with the given name exists.
        lsst.daf.butler.registry.OrphanedRecordError
            Raised if the database rows associated with the collection are
            still referenced by some other table, such as a dataset in a
            datastore (for `~CollectionType.RUN` collections only) or a
            `~CollectionType.CHAINED` collection of which this collection is
            a child.

        Notes
        -----
        If this is a `~CollectionType.RUN` collection, all datasets and quanta
        in it will removed from the `Registry` database.  This requires that
        those datasets be removed (or at least trashed) from any datastores
        that hold them first.

        A collection may not be deleted as long as it is referenced by a
        `~CollectionType.CHAINED` collection; the ``CHAINED`` collection must
        be deleted or redefined first.
        """
        raise NotImplementedError()

    def _filter_dataset_types(
        self, dataset_types: Iterable[str], collections: Iterable[CollectionInfo]
    ) -> Iterable[str]:
        dataset_types_set = set(dataset_types)
        collection_dataset_types: set[str] = set()
        for info in collections:
            if info.dataset_types is None:
                raise RuntimeError("Can only filter by collections if include_summary was True")
            collection_dataset_types.update(info.dataset_types)
        dataset_types_set = dataset_types_set.intersection(collection_dataset_types)
        return dataset_types_set

    def _group_by_dataset_type(
        self, dataset_types: Set[str], collection_infos: Iterable[CollectionInfo]
    ) -> Mapping[str, list[str]]:
        """Filter dataset types and collections names based on summary in
        collecion infos.

        Parameters
        ----------
        dataset_types : `~collections.abc.Set` [`str`]
            Set of dataset type names to extract.
        collection_infos : `~collections.abc.Iterable` [`CollectionInfo`]
            Collection infos, must contain dataset type summary.

        Returns
        -------
        filtered : `~collections.abc.Mapping` [`str`, `list`[`str`]]
            Mapping of the dataset type name to its corresponding list of
            collection names.
        """
        dataset_type_collections: dict[str, list[str]] = defaultdict(list)
        for info in collection_infos:
            if info.dataset_types is None:
                raise RuntimeError("Can only filter by collections if include_summary was True")
            for dataset_type in info.dataset_types & dataset_types:
                dataset_type_collections[dataset_type].append(info.name)
        return dataset_type_collections

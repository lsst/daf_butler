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

__all__ = ("Registry", "CollectionArgType")

import contextlib
import logging
import re
from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator, Mapping, Sequence
from types import EllipsisType
from typing import TYPE_CHECKING, Any, TypeAlias

from .._collection_type import CollectionType
from .._dataset_association import DatasetAssociation
from .._dataset_ref import DatasetId, DatasetIdGenEnum, DatasetRef
from .._dataset_type import DatasetType
from .._storage_class import StorageClassFactory
from .._timespan import Timespan
from ..dimensions import (
    DataCoordinate,
    DataId,
    DimensionElement,
    DimensionGroup,
    DimensionRecord,
    DimensionUniverse,
)
from ._collection_summary import CollectionSummary
from ._defaults import RegistryDefaults
from .queries import DataCoordinateQueryResults, DatasetQueryResults, DimensionRecordQueryResults
from .wildcards import CollectionWildcard

if TYPE_CHECKING:
    from .interfaces import ObsCoreTableManager

_LOG = logging.getLogger(__name__)

# TYpe alias for `collections` arguments.
CollectionArgType: TypeAlias = (
    str | re.Pattern | Iterable[str | re.Pattern] | EllipsisType | CollectionWildcard
)


class Registry(ABC):
    """Abstract Registry interface.

    All subclasses should store `~lsst.daf.butler.registry.RegistryDefaults` in
    a ``_defaults`` property. No other properties are assumed shared between
    implementations.

    See Also
    --------
    lsst.daf.butler.Butler
    """

    @abstractmethod
    def isWriteable(self) -> bool:
        """Return `True` if this registry allows write operations, and `False`
        otherwise.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def dimensions(self) -> DimensionUniverse:
        """Definitions of all dimensions recognized by this `Registry`
        (`DimensionUniverse`).
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def defaults(self) -> RegistryDefaults:
        """Default collection search path and/or output `~CollectionType.RUN`
        collection (`~lsst.daf.butler.registry.RegistryDefaults`).

        This is an immutable struct whose components may not be set
        individually, but the entire struct can be set by assigning to this
        property.
        """
        raise NotImplementedError()

    @defaults.setter
    @abstractmethod
    def defaults(self, value: RegistryDefaults) -> None:
        raise NotImplementedError()

    @abstractmethod
    def refresh(self) -> None:
        """Refresh all in-memory state by querying the database.

        This may be necessary to enable querying for entities added by other
        registry instances after this one was constructed.
        """
        raise NotImplementedError()

    @abstractmethod
    def refresh_collection_summaries(self) -> None:
        """Refresh content of the collection summary tables in the database.

        This method can be run to cleanup the contents of the summary tables
        after removing datasets from collections.
        """
        raise NotImplementedError()

    @abstractmethod
    def caching_context(self) -> contextlib.AbstractContextManager[None]:
        """Context manager that enables caching."""
        raise NotImplementedError()

    @contextlib.contextmanager
    @abstractmethod
    def transaction(self, *, savepoint: bool = False) -> Iterator[None]:
        """Return a context manager that represents a transaction.

        Parameters
        ----------
        savepoint : `bool`
            Whether to create a SAVEPOINT or not.
        """
        raise NotImplementedError()

    def resetConnectionPool(self) -> None:
        """Reset connection pool for registry if relevant.

        This operation can be used reset connections to servers when
        using registry with fork-based multiprocessing. This method should
        usually be called by the child process immediately
        after the fork.

        The base class implementation is a no-op.
        """
        pass

    @abstractmethod
    def registerCollection(
        self, name: str, type: CollectionType = CollectionType.TAGGED, doc: str | None = None
    ) -> bool:
        """Add a new collection if one with the given name does not exist.

        Parameters
        ----------
        name : `str`
            The name of the collection to create.
        type : `CollectionType`
            Enum value indicating the type of collection to create.
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
        able to perform its own transaction to be concurrent.
        """
        raise NotImplementedError()

    @abstractmethod
    def getCollectionType(self, name: str) -> CollectionType:
        """Return an enumeration value indicating the type of the given
        collection.

        Parameters
        ----------
        name : `str`
            The name of the collection.

        Returns
        -------
        type : `CollectionType`
            Enum value indicating the type of this collection.

        Raises
        ------
        lsst.daf.butler.registry.MissingCollectionError
            Raised if no collection with the given name exists.
        """
        raise NotImplementedError()

    @abstractmethod
    def registerRun(self, name: str, doc: str | None = None) -> bool:
        """Add a new run if one with the given name does not exist.

        Parameters
        ----------
        name : `str`
            The name of the run to create.
        doc : `str`, optional
            Documentation string for the collection.

        Returns
        -------
        registered : `bool`
            Boolean indicating whether a new run was registered. `False`
            if it already existed.

        Notes
        -----
        This method cannot be called within transactions, as it needs to be
        able to perform its own transaction to be concurrent.
        """
        raise NotImplementedError()

    @abstractmethod
    def removeCollection(self, name: str) -> None:
        """Remove the given collection from the registry.

        Parameters
        ----------
        name : `str`
            The name of the collection to remove.

        Raises
        ------
        lsst.daf.butler.registry.MissingCollectionError
            Raised if no collection with the given name exists.
        sqlalchemy.exc.IntegrityError
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

    @abstractmethod
    def getCollectionChain(self, parent: str) -> Sequence[str]:
        """Return the child collections in a `~CollectionType.CHAINED`
        collection.

        Parameters
        ----------
        parent : `str`
            Name of the chained collection.  Must have already been added via
            a call to `Registry.registerCollection`.

        Returns
        -------
        children : `~collections.abc.Sequence` [ `str` ]
            An ordered sequence of collection names that are searched when the
            given chained collection is searched.

        Raises
        ------
        lsst.daf.butler.registry.MissingCollectionError
            Raised if ``parent`` does not exist in the `Registry`.
        lsst.daf.butler.registry.CollectionTypeError
            Raised if ``parent`` does not correspond to a
            `~CollectionType.CHAINED` collection.
        """
        raise NotImplementedError()

    @abstractmethod
    def setCollectionChain(self, parent: str, children: Any, *, flatten: bool = False) -> None:
        """Define or redefine a `~CollectionType.CHAINED` collection.

        Parameters
        ----------
        parent : `str`
            Name of the chained collection.  Must have already been added via
            a call to `Registry.registerCollection`.
        children : collection expression
            An expression defining an ordered search of child collections,
            generally an iterable of `str`; see
            :ref:`daf_butler_collection_expressions` for more information.
        flatten : `bool`, optional
            If `True` (`False` is default), recursively flatten out any nested
            `~CollectionType.CHAINED` collections in ``children`` first.

        Raises
        ------
        lsst.daf.butler.registry.MissingCollectionError
            Raised when any of the given collections do not exist in the
            `Registry`.
        lsst.daf.butler.registry.CollectionTypeError
            Raised if ``parent`` does not correspond to a
            `~CollectionType.CHAINED` collection.
        ValueError
            Raised if the given collections contains a cycle.
        """
        raise NotImplementedError()

    @abstractmethod
    def getCollectionParentChains(self, collection: str) -> set[str]:
        """Return the CHAINED collections that directly contain the given one.

        Parameters
        ----------
        collection : `str`
            Name of the collection.

        Returns
        -------
        chains : `set` of `str`
            Set of `~CollectionType.CHAINED` collection names.
        """
        raise NotImplementedError()

    @abstractmethod
    def getCollectionDocumentation(self, collection: str) -> str | None:
        """Retrieve the documentation string for a collection.

        Parameters
        ----------
        collection : `str`
            Name of the collection.

        Returns
        -------
        docs : `str` or `None`
            Docstring for the collection with the given name.
        """
        raise NotImplementedError()

    @abstractmethod
    def setCollectionDocumentation(self, collection: str, doc: str | None) -> None:
        """Set the documentation string for a collection.

        Parameters
        ----------
        collection : `str`
            Name of the collection.
        doc : `str` or `None`
            Docstring for the collection with the given name; will replace any
            existing docstring.  Passing `None` will remove any existing
            docstring.
        """
        raise NotImplementedError()

    @abstractmethod
    def getCollectionSummary(self, collection: str) -> CollectionSummary:
        """Return a summary for the given collection.

        Parameters
        ----------
        collection : `str`
            Name of the collection for which a summary is to be retrieved.

        Returns
        -------
        summary : `~lsst.daf.butler.registry.CollectionSummary`
            Summary of the dataset types and governor dimension values in
            this collection.
        """
        raise NotImplementedError()

    @abstractmethod
    def registerDatasetType(self, datasetType: DatasetType) -> bool:
        """Add a new `DatasetType` to the Registry.

        It is not an error to register the same `DatasetType` twice.

        Parameters
        ----------
        datasetType : `DatasetType`
            The `DatasetType` to be added.

        Returns
        -------
        inserted : `bool`
            `True` if ``datasetType`` was inserted, `False` if an identical
            existing `DatasetType` was found.  Note that in either case the
            DatasetType is guaranteed to be defined in the Registry
            consistently with the given definition.

        Raises
        ------
        ValueError
            Raised if the dimensions or storage class are invalid.
        lsst.daf.butler.registry.ConflictingDefinitionError
            Raised if this `DatasetType` is already registered with a different
            definition.

        Notes
        -----
        This method cannot be called within transactions, as it needs to be
        able to perform its own transaction to be concurrent.
        """
        raise NotImplementedError()

    @abstractmethod
    def removeDatasetType(self, name: str | tuple[str, ...]) -> None:
        """Remove the named `DatasetType` from the registry.

        .. warning::

            Registry implementations can cache the dataset type definitions.
            This means that deleting the dataset type definition may result in
            unexpected behavior from other butler processes that are active
            that have not seen the deletion.

        Parameters
        ----------
        name : `str` or `tuple` [`str`]
            Name of the type to be removed or tuple containing a list of type
            names to be removed. Wildcards are allowed.

        Raises
        ------
        lsst.daf.butler.registry.OrphanedRecordError
            Raised if an attempt is made to remove the dataset type definition
            when there are already datasets associated with it.

        Notes
        -----
        If the dataset type is not registered the method will return without
        action.
        """
        raise NotImplementedError()

    @abstractmethod
    def getDatasetType(self, name: str) -> DatasetType:
        """Get the `DatasetType`.

        Parameters
        ----------
        name : `str`
            Name of the type.

        Returns
        -------
        type : `DatasetType`
            The `DatasetType` associated with the given name.

        Raises
        ------
        lsst.daf.butler.registry.MissingDatasetTypeError
            Raised if the requested dataset type has not been registered.

        Notes
        -----
        This method handles component dataset types automatically, though most
        other registry operations do not.
        """
        raise NotImplementedError()

    @abstractmethod
    def supportsIdGenerationMode(self, mode: DatasetIdGenEnum) -> bool:
        """Test whether the given dataset ID generation mode is supported by
        `insertDatasets`.

        Parameters
        ----------
        mode : `DatasetIdGenEnum`
            Enum value for the mode to test.

        Returns
        -------
        supported : `bool`
            Whether the given mode is supported.
        """
        raise NotImplementedError()

    @abstractmethod
    def findDataset(
        self,
        datasetType: DatasetType | str,
        dataId: DataId | None = None,
        *,
        collections: CollectionArgType | None = None,
        timespan: Timespan | None = None,
        datastore_records: bool = False,
        **kwargs: Any,
    ) -> DatasetRef | None:
        """Find a dataset given its `DatasetType` and data ID.

        This can be used to obtain a `DatasetRef` that permits the dataset to
        be read from a `Datastore`. If the dataset is a component and can not
        be found using the provided dataset type, a dataset ref for the parent
        will be returned instead but with the correct dataset type.

        Parameters
        ----------
        datasetType : `DatasetType` or `str`
            A `DatasetType` or the name of one.  If this is a `DatasetType`
            instance, its storage class will be respected and propagated to
            the output, even if it differs from the dataset type definition
            in the registry, as long as the storage classes are convertible.
        dataId : `dict` or `DataCoordinate`, optional
            A `dict`-like object containing the `Dimension` links that identify
            the dataset within a collection.
        collections : collection expression, optional
            An expression that fully or partially identifies the collections to
            search for the dataset; see
            :ref:`daf_butler_collection_expressions` for more information.
            Defaults to ``self.defaults.collections``.
        timespan : `Timespan`, optional
            A timespan that the validity range of the dataset must overlap.
            If not provided, any `~CollectionType.CALIBRATION` collections
            matched by the ``collections`` argument will not be searched.
        datastore_records : `bool`, optional
            Whether to attach datastore records to the `DatasetRef`.
        **kwargs
            Additional keyword arguments passed to
            `DataCoordinate.standardize` to convert ``dataId`` to a true
            `DataCoordinate` or augment an existing one.

        Returns
        -------
        ref : `DatasetRef`
            A reference to the dataset, or `None` if no matching Dataset
            was found.

        Raises
        ------
        lsst.daf.butler.registry.NoDefaultCollectionError
            Raised if ``collections`` is `None` and
            ``self.defaults.collections`` is `None`.
        LookupError
            Raised if one or more data ID keys are missing.
        lsst.daf.butler.registry.MissingDatasetTypeError
            Raised if the dataset type does not exist.
        lsst.daf.butler.registry.MissingCollectionError
            Raised if any of ``collections`` does not exist in the registry.

        Notes
        -----
        This method simply returns `None` and does not raise an exception even
        when the set of collections searched is intrinsically incompatible with
        the dataset type, e.g. if ``datasetType.isCalibration() is False``, but
        only `~CollectionType.CALIBRATION` collections are being searched.
        This may make it harder to debug some lookup failures, but the behavior
        is intentional; we consider it more important that failed searches are
        reported consistently, regardless of the reason, and that adding
        additional collections that do not contain a match to the search path
        never changes the behavior.

        This method handles component dataset types automatically, though most
        other registry operations do not.
        """
        raise NotImplementedError()

    @abstractmethod
    def insertDatasets(
        self,
        datasetType: DatasetType | str,
        dataIds: Iterable[DataId],
        run: str | None = None,
        expand: bool = True,
        idGenerationMode: DatasetIdGenEnum = DatasetIdGenEnum.UNIQUE,
    ) -> list[DatasetRef]:
        """Insert one or more datasets into the `Registry`.

        This always adds new datasets; to associate existing datasets with
        a new collection, use ``associate``.

        Parameters
        ----------
        datasetType : `DatasetType` or `str`
            A `DatasetType` or the name of one.
        dataIds : `~collections.abc.Iterable` of `dict` or `DataCoordinate`
            Dimension-based identifiers for the new datasets.
        run : `str`, optional
            The name of the run that produced the datasets.  Defaults to
            ``self.defaults.run``.
        expand : `bool`, optional
            If `True` (default), expand data IDs as they are inserted.  This is
            necessary in general to allow datastore to generate file templates,
            but it may be disabled if the caller can guarantee this is
            unnecessary.
        idGenerationMode : `DatasetIdGenEnum`, optional
            Specifies option for generating dataset IDs. By default unique IDs
            are generated for each inserted dataset.

        Returns
        -------
        refs : `list` of `DatasetRef`
            Resolved `DatasetRef` instances for all given data IDs (in the same
            order).

        Raises
        ------
        lsst.daf.butler.registry.DatasetTypeError
            Raised if ``datasetType`` is not known to registry.
        lsst.daf.butler.registry.CollectionTypeError
            Raised if ``run`` collection type is not `~CollectionType.RUN`.
        lsst.daf.butler.registry.NoDefaultCollectionError
            Raised if ``run`` is `None` and ``self.defaults.run`` is `None`.
        lsst.daf.butler.registry.ConflictingDefinitionError
            If a dataset with the same dataset type and data ID as one of those
            given already exists in ``run``.
        lsst.daf.butler.registry.MissingCollectionError
            Raised if ``run`` does not exist in the registry.
        """
        raise NotImplementedError()

    @abstractmethod
    def _importDatasets(
        self,
        datasets: Iterable[DatasetRef],
        expand: bool = True,
    ) -> list[DatasetRef]:
        """Import one or more datasets into the `Registry`.

        Difference from `insertDatasets` method is that this method accepts
        `DatasetRef` instances which should already be resolved and have a
        dataset ID. If registry supports globally-unique dataset IDs (e.g.
        `uuid.UUID`) then datasets which already exist in the registry will be
        ignored if imported again.

        Parameters
        ----------
        datasets : `~collections.abc.Iterable` of `DatasetRef`
            Datasets to be inserted. All `DatasetRef` instances must have
            identical ``datasetType`` and ``run`` attributes. ``run``
            attribute can be `None` and defaults to ``self.defaults.run``.
            Datasets can specify ``id`` attribute which will be used for
            inserted datasets. All dataset IDs must have the same type
            (`int` or `uuid.UUID`), if type of dataset IDs does not match
            configured backend then IDs will be ignored and new IDs will be
            generated by backend.
        expand : `bool`, optional
            If `True` (default), expand data IDs as they are inserted.  This is
            necessary in general, but it may be disabled if the caller can
            guarantee this is unnecessary.

        Returns
        -------
        refs : `list` of `DatasetRef`
            Resolved `DatasetRef` instances for all given data IDs (in the same
            order). If any of ``datasets`` has an ID which already exists in
            the database then it will not be inserted or updated, but a
            resolved `DatasetRef` will be returned for it in any case.

        Raises
        ------
        lsst.daf.butler.registry.NoDefaultCollectionError
            Raised if ``run`` is `None` and ``self.defaults.run`` is `None`.
        lsst.daf.butler.registry.DatasetTypeError
            Raised if datasets correspond to more than one dataset type or
            dataset type is not known to registry.
        lsst.daf.butler.registry.ConflictingDefinitionError
            If a dataset with the same dataset type and data ID as one of those
            given already exists in ``run``.
        lsst.daf.butler.registry.MissingCollectionError
            Raised if ``run`` does not exist in the registry.

        Notes
        -----
        This method is considered package-private and internal to Butler
        implementation. Clients outside daf_butler package should not use this
        method.
        """
        raise NotImplementedError()

    @abstractmethod
    def getDataset(self, id: DatasetId) -> DatasetRef | None:
        """Retrieve a Dataset entry.

        Parameters
        ----------
        id : `DatasetId`
            The unique identifier for the dataset.

        Returns
        -------
        ref : `DatasetRef` or `None`
            A ref to the Dataset, or `None` if no matching Dataset
            was found.
        """
        raise NotImplementedError()

    @abstractmethod
    def removeDatasets(self, refs: Iterable[DatasetRef]) -> None:
        """Remove datasets from the Registry.

        The datasets will be removed unconditionally from all collections, and
        any `Quantum` that consumed this dataset will instead be marked with
        having a NULL input.  `Datastore` records will *not* be deleted; the
        caller is responsible for ensuring that the dataset has already been
        removed from all Datastores.

        Parameters
        ----------
        refs : `~collections.abc.Iterable` [`DatasetRef`]
            References to the datasets to be removed.  Must include a valid
            ``id`` attribute, and should be considered invalidated upon return.

        Raises
        ------
        lsst.daf.butler.AmbiguousDatasetError
            Raised if any ``ref.id`` is `None`.
        lsst.daf.butler.registry.OrphanedRecordError
            Raised if any dataset is still present in any `Datastore`.
        """
        raise NotImplementedError()

    @abstractmethod
    def associate(self, collection: str, refs: Iterable[DatasetRef]) -> None:
        """Add existing datasets to a `~CollectionType.TAGGED` collection.

        If a DatasetRef with the same exact ID is already in a collection
        nothing is changed. If a `DatasetRef` with the same `DatasetType` and
        data ID but with different ID exists in the collection,
        `~lsst.daf.butler.registry.ConflictingDefinitionError` is raised.

        Parameters
        ----------
        collection : `str`
            Indicates the collection the datasets should be associated with.
        refs : `~collections.abc.Iterable` [ `DatasetRef` ]
            An iterable of resolved `DatasetRef` instances that already exist
            in this `Registry`.

        Raises
        ------
        lsst.daf.butler.registry.ConflictingDefinitionError
            If a Dataset with the given `DatasetRef` already exists in the
            given collection.
        lsst.daf.butler.registry.MissingCollectionError
            Raised if ``collection`` does not exist in the registry.
        lsst.daf.butler.registry.CollectionTypeError
            Raise adding new datasets to the given ``collection`` is not
            allowed.
        """
        raise NotImplementedError()

    @abstractmethod
    def disassociate(self, collection: str, refs: Iterable[DatasetRef]) -> None:
        """Remove existing datasets from a `~CollectionType.TAGGED` collection.

        ``collection`` and ``ref`` combinations that are not currently
        associated are silently ignored.

        Parameters
        ----------
        collection : `str`
            The collection the datasets should no longer be associated with.
        refs : `~collections.abc.Iterable` [ `DatasetRef` ]
            An iterable of resolved `DatasetRef` instances that already exist
            in this `Registry`.

        Raises
        ------
        lsst.daf.butler.AmbiguousDatasetError
            Raised if any of the given dataset references is unresolved.
        lsst.daf.butler.registry.MissingCollectionError
            Raised if ``collection`` does not exist in the registry.
        lsst.daf.butler.registry.CollectionTypeError
            Raise adding new datasets to the given ``collection`` is not
            allowed.
        """
        raise NotImplementedError()

    @abstractmethod
    def certify(self, collection: str, refs: Iterable[DatasetRef], timespan: Timespan) -> None:
        """Associate one or more datasets with a calibration collection and a
        validity range within it.

        Parameters
        ----------
        collection : `str`
            The name of an already-registered `~CollectionType.CALIBRATION`
            collection.
        refs : `~collections.abc.Iterable` [ `DatasetRef` ]
            Datasets to be associated.
        timespan : `Timespan`
            The validity range for these datasets within the collection.

        Raises
        ------
        lsst.daf.butler.AmbiguousDatasetError
            Raised if any of the given `DatasetRef` instances is unresolved.
        lsst.daf.butler.registry.ConflictingDefinitionError
            Raised if the collection already contains a different dataset with
            the same `DatasetType` and data ID and an overlapping validity
            range.
        lsst.daf.butler.registry.CollectionTypeError
            Raised if ``collection`` is not a `~CollectionType.CALIBRATION`
            collection or if one or more datasets are of a dataset type for
            which `DatasetType.isCalibration` returns `False`.
        """
        raise NotImplementedError()

    @abstractmethod
    def decertify(
        self,
        collection: str,
        datasetType: str | DatasetType,
        timespan: Timespan,
        *,
        dataIds: Iterable[DataId] | None = None,
    ) -> None:
        """Remove or adjust datasets to clear a validity range within a
        calibration collection.

        Parameters
        ----------
        collection : `str`
            The name of an already-registered `~CollectionType.CALIBRATION`
            collection.
        datasetType : `str` or `DatasetType`
            Name or `DatasetType` instance for the datasets to be decertified.
        timespan : `Timespan`, optional
            The validity range to remove datasets from within the collection.
            Datasets that overlap this range but are not contained by it will
            have their validity ranges adjusted to not overlap it, which may
            split a single dataset validity range into two.
        dataIds : iterable [`dict` or `DataCoordinate`], optional
            Data IDs that should be decertified within the given validity range
            If `None`, all data IDs for ``self.datasetType`` will be
            decertified.

        Raises
        ------
        lsst.daf.butler.registry.CollectionTypeError
            Raised if ``collection`` is not a `~CollectionType.CALIBRATION`
            collection or if ``datasetType.isCalibration() is False``.
        """
        raise NotImplementedError()

    @abstractmethod
    def getDatasetLocations(self, ref: DatasetRef) -> Iterable[str]:
        """Retrieve datastore locations for a given dataset.

        Parameters
        ----------
        ref : `DatasetRef`
            A reference to the dataset for which to retrieve storage
            information.

        Returns
        -------
        datastores : `~collections.abc.Iterable` [ `str` ]
            All the matching datastores holding this dataset.

        Raises
        ------
        lsst.daf.butler.AmbiguousDatasetError
            Raised if ``ref.id`` is `None`.
        """
        raise NotImplementedError()

    @abstractmethod
    def expandDataId(
        self,
        dataId: DataId | None = None,
        *,
        dimensions: Iterable[str] | DimensionGroup | None = None,
        records: Mapping[str, DimensionRecord | None] | None = None,
        withDefaults: bool = True,
        **kwargs: Any,
    ) -> DataCoordinate:
        """Expand a dimension-based data ID to include additional information.

        Parameters
        ----------
        dataId : `DataCoordinate` or `dict`, optional
            Data ID to be expanded; augmented and overridden by ``kwargs``.
        dimensions : `~collections.abc.Iterable` [ `str` ] or \
                `DimensionGroup` optional
            The dimensions to be identified by the new `DataCoordinate`.
            If not provided, will be inferred from the keys of ``dataId`` and
            ``**kwargs``, and ``universe`` must be provided unless ``dataId``
            is already a `DataCoordinate`.
        records : `~collections.abc.Mapping` [`str`, `DimensionRecord`], \
                optional
            Dimension record data to use before querying the database for that
            data, keyed by element name.
        withDefaults : `bool`, optional
            Utilize ``self.defaults.dataId`` to fill in missing governor
            dimension key-value pairs.  Defaults to `True` (i.e. defaults are
            used).
        **kwargs
            Additional keywords are treated like additional key-value pairs for
            ``dataId``, extending and overriding.

        Returns
        -------
        expanded : `DataCoordinate`
            A data ID that includes full metadata for all of the dimensions it
            identifies, i.e. guarantees that ``expanded.hasRecords()`` and
            ``expanded.hasFull()`` both return `True`.

        Raises
        ------
        lsst.daf.butler.registry.DataIdError
            Raised when ``dataId`` or keyword arguments specify unknown
            dimensions or values, or when a resulting data ID contains
            contradictory key-value pairs, according to dimension
            relationships.

        Notes
        -----
        This method cannot be relied upon to reject invalid data ID values
        for dimensions that do actually not have any record columns.  For
        efficiency reasons the records for these dimensions (which have only
        dimension key values that are given by the caller) may be constructed
        directly rather than obtained from the registry database.
        """
        raise NotImplementedError()

    @abstractmethod
    def insertDimensionData(
        self,
        element: DimensionElement | str,
        *data: Mapping[str, Any] | DimensionRecord,
        conform: bool = True,
        replace: bool = False,
        skip_existing: bool = False,
    ) -> None:
        """Insert one or more dimension records into the database.

        Parameters
        ----------
        element : `DimensionElement` or `str`
            The `DimensionElement` or name thereof that identifies the table
            records will be inserted into.
        *data : `dict` or `DimensionRecord`
            One or more records to insert.
        conform : `bool`, optional
            If `False` (`True` is default) perform no checking or conversions,
            and assume that ``element`` is a `DimensionElement` instance and
            ``data`` is a one or more `DimensionRecord` instances of the
            appropriate subclass.
        replace : `bool`, optional
            If `True` (`False` is default), replace existing records in the
            database if there is a conflict.
        skip_existing : `bool`, optional
            If `True` (`False` is default), skip insertion if a record with
            the same primary key values already exists.  Unlike
            `syncDimensionData`, this will not detect when the given record
            differs from what is in the database, and should not be used when
            this is a concern.
        """
        raise NotImplementedError()

    @abstractmethod
    def syncDimensionData(
        self,
        element: DimensionElement | str,
        row: Mapping[str, Any] | DimensionRecord,
        conform: bool = True,
        update: bool = False,
    ) -> bool | dict[str, Any]:
        """Synchronize the given dimension record with the database, inserting
        if it does not already exist and comparing values if it does.

        Parameters
        ----------
        element : `DimensionElement` or `str`
            The `DimensionElement` or name thereof that identifies the table
            records will be inserted into.
        row : `dict` or `DimensionRecord`
            The record to insert.
        conform : `bool`, optional
            If `False` (`True` is default) perform no checking or conversions,
            and assume that ``element`` is a `DimensionElement` instance and
            ``data`` is a one or more `DimensionRecord` instances of the
            appropriate subclass.
        update : `bool`, optional
            If `True` (`False` is default), update the existing record in the
            database if there is a conflict.

        Returns
        -------
        inserted_or_updated : `bool` or `dict`
            `True` if a new row was inserted, `False` if no changes were
            needed, or a `dict` mapping updated column names to their old
            values if an update was performed (only possible if
            ``update=True``).

        Raises
        ------
        lsst.daf.butler.registry.ConflictingDefinitionError
            Raised if the record exists in the database (according to primary
            key lookup) but is inconsistent with the given one.
        """
        raise NotImplementedError()

    @abstractmethod
    def queryDatasetTypes(
        self,
        expression: Any = ...,
        *,
        missing: list[str] | None = None,
    ) -> Iterable[DatasetType]:
        """Iterate over the dataset types whose names match an expression.

        Parameters
        ----------
        expression : dataset type expression, optional
            An expression that fully or partially identifies the dataset types
            to return, such as a `str`, `re.Pattern`, or iterable thereof.
            ``...`` can be used to return all dataset types, and is the
            default. See :ref:`daf_butler_dataset_type_expressions` for more
            information.
        missing : `list` of `str`, optional
            String dataset type names that were explicitly given (i.e. not
            regular expression patterns) but not found will be appended to this
            list, if it is provided.

        Returns
        -------
        dataset_types : `~collections.abc.Iterable` [ `DatasetType`]
            An `~collections.abc.Iterable` of `DatasetType` instances whose
            names match ``expression``.

        Raises
        ------
        lsst.daf.butler.registry.DatasetTypeExpressionError
            Raised when ``expression`` is invalid.
        """
        raise NotImplementedError()

    @abstractmethod
    def queryCollections(
        self,
        expression: Any = ...,
        datasetType: DatasetType | None = None,
        collectionTypes: Iterable[CollectionType] | CollectionType = CollectionType.all(),
        flattenChains: bool = False,
        includeChains: bool | None = None,
    ) -> Sequence[str]:
        """Iterate over the collections whose names match an expression.

        Parameters
        ----------
        expression : collection expression, optional
            An expression that identifies the collections to return, such as
            a `str` (for full matches or partial matches via globs),
            `re.Pattern` (for partial matches), or iterable thereof.  ``...``
            can be used to return all collections, and is the default.
            See :ref:`daf_butler_collection_expressions` for more information.
        datasetType : `DatasetType`, optional
            If provided, only yield collections that may contain datasets of
            this type.  This is a conservative approximation in general; it may
            yield collections that do not have any such datasets.
        collectionTypes : `~collections.abc.Set` [`CollectionType`] or \
            `CollectionType`, optional
            If provided, only yield collections of these types.
        flattenChains : `bool`, optional
            If `True` (`False` is default), recursively yield the child
            collections of matching `~CollectionType.CHAINED` collections.
        includeChains : `bool`, optional
            If `True`, yield records for matching `~CollectionType.CHAINED`
            collections.  Default is the opposite of ``flattenChains``: include
            either CHAINED collections or their children, but not both.

        Returns
        -------
        collections : `~collections.abc.Sequence` [ `str` ]
            The names of collections that match ``expression``.

        Raises
        ------
        lsst.daf.butler.registry.CollectionExpressionError
            Raised when ``expression`` is invalid.

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
    def queryDatasets(
        self,
        datasetType: Any,
        *,
        collections: CollectionArgType | None = None,
        dimensions: Iterable[str] | None = None,
        dataId: DataId | None = None,
        where: str = "",
        findFirst: bool = False,
        bind: Mapping[str, Any] | None = None,
        check: bool = True,
        **kwargs: Any,
    ) -> DatasetQueryResults:
        """Query for and iterate over dataset references matching user-provided
        criteria.

        Parameters
        ----------
        datasetType : dataset type expression
            An expression that fully or partially identifies the dataset types
            to be queried.  Allowed types include `DatasetType`, `str`,
            `re.Pattern`, and iterables thereof.  The special value ``...`` can
            be used to query all dataset types.  See
            :ref:`daf_butler_dataset_type_expressions` for more information.
        collections : collection expression, optional
            An expression that identifies the collections to search, such as a
            `str` (for full matches or partial matches via globs), `re.Pattern`
            (for partial matches), or iterable thereof.  ``...`` can be used to
            search all collections (actually just all `~CollectionType.RUN`
            collections, because this will still find all datasets).
            If not provided, ``self.default.collections`` is used.  See
            :ref:`daf_butler_collection_expressions` for more information.
        dimensions : `~collections.abc.Iterable` [ `str` ]
            Dimensions to include in the query (in addition to those used
            to identify the queried dataset type(s)), either to constrain
            the resulting datasets to those for which a matching dimension
            exists, or to relate the dataset type's dimensions to dimensions
            referenced by the ``dataId`` or ``where`` arguments.
        dataId : `dict` or `DataCoordinate`, optional
            A data ID whose key-value pairs are used as equality constraints
            in the query.
        where : `str`, optional
            A string expression similar to a SQL WHERE clause.  May involve
            any column of a dimension table or (as a shortcut for the primary
            key column of a dimension table) dimension name.  See
            :ref:`daf_butler_dimension_expressions` for more information.
        findFirst : `bool`, optional
            If `True` (`False` is default), for each result data ID, only
            yield one `DatasetRef` of each `DatasetType`, from the first
            collection in which a dataset of that dataset type appears
            (according to the order of ``collections`` passed in).  If `True`,
            ``collections`` must not contain regular expressions and may not
            be ``...``.
        bind : `~collections.abc.Mapping`, optional
            Mapping containing literal values that should be injected into the
            ``where`` expression, keyed by the identifiers they replace.
            Values of collection type can be expanded in some cases; see
            :ref:`daf_butler_dimension_expressions_identifiers` for more
            information.
        check : `bool`, optional
            If `True` (default) check the query for consistency before
            executing it.  This may reject some valid queries that resemble
            common mistakes (e.g. queries for visits without specifying an
            instrument).
        **kwargs
            Additional keyword arguments are forwarded to
            `DataCoordinate.standardize` when processing the ``dataId``
            argument (and may be used to provide a constraining data ID even
            when the ``dataId`` argument is `None`).

        Returns
        -------
        refs : `.queries.DatasetQueryResults`
            Dataset references matching the given query criteria.  Nested data
            IDs are guaranteed to include values for all implied dimensions
            (i.e. `DataCoordinate.hasFull` will return `True`), but will not
            include dimension records (`DataCoordinate.hasRecords` will be
            `False`) unless `~.queries.DatasetQueryResults.expanded` is
            called on the result object (which returns a new one).

        Raises
        ------
        lsst.daf.butler.registry.DatasetTypeExpressionError
            Raised when ``datasetType`` expression is invalid.
        TypeError
            Raised when the arguments are incompatible, such as when a
            collection wildcard is passed when ``findFirst`` is `True`, or
            when ``collections`` is `None` and ``self.defaults.collections`` is
            also `None`.
        lsst.daf.butler.registry.DataIdError
            Raised when ``dataId`` or keyword arguments specify unknown
            dimensions or values, or when they contain inconsistent values.
        lsst.daf.butler.registry.UserExpressionError
            Raised when ``where`` expression is invalid.

        Notes
        -----
        When multiple dataset types are queried in a single call, the
        results of this operation are equivalent to querying for each dataset
        type separately in turn, and no information about the relationships
        between datasets of different types is included.  In contexts where
        that kind of information is important, the recommended pattern is to
        use `queryDataIds` to first obtain data IDs (possibly with the
        desired dataset types and collections passed as constraints to the
        query), and then use multiple (generally much simpler) calls to
        `queryDatasets` with the returned data IDs passed as constraints.
        """
        raise NotImplementedError()

    @abstractmethod
    def queryDataIds(
        self,
        dimensions: DimensionGroup | Iterable[str] | str,
        *,
        dataId: DataId | None = None,
        datasets: Any = None,
        collections: CollectionArgType | None = None,
        where: str = "",
        bind: Mapping[str, Any] | None = None,
        check: bool = True,
        **kwargs: Any,
    ) -> DataCoordinateQueryResults:
        """Query for data IDs matching user-provided criteria.

        Parameters
        ----------
        dimensions : `DimensionGroup`, `str`, or \
                `~collections.abc.Iterable` [ `str` ]
            The dimensions of the data IDs to yield.  Will be automatically
            expanded to a complete `DimensionGroup`.
        dataId : `dict` or `DataCoordinate`, optional
            A data ID whose key-value pairs are used as equality constraints
            in the query.
        datasets : dataset type expression, optional
            An expression that fully or partially identifies dataset types
            that should constrain the yielded data IDs.  For example, including
            "raw" here would constrain the yielded ``instrument``,
            ``exposure``, ``detector``, and ``physical_filter`` values to only
            those for which at least one "raw" dataset exists in
            ``collections``.  Allowed types include `DatasetType`, `str`,
            and iterables thereof. See
            :ref:`daf_butler_dataset_type_expressions` for more information.
        collections : collection expression, optional
            An expression that identifies the collections to search for
            datasets, such as a `str` (for full matches or partial matches
            via globs), `re.Pattern` (for partial matches), or iterable
            thereof.  ``...`` can be used to search all collections (actually
            just all `~CollectionType.RUN` collections, because this will
            still find all datasets).  If not provided,
            ``self.default.collections`` is used.  Ignored unless ``datasets``
            is also passed.  See :ref:`daf_butler_collection_expressions` for
            more information.
        where : `str`, optional
            A string expression similar to a SQL WHERE clause.  May involve
            any column of a dimension table or (as a shortcut for the primary
            key column of a dimension table) dimension name.  See
            :ref:`daf_butler_dimension_expressions` for more information.
        bind : `~collections.abc.Mapping`, optional
            Mapping containing literal values that should be injected into the
            ``where`` expression, keyed by the identifiers they replace.
            Values of collection type can be expanded in some cases; see
            :ref:`daf_butler_dimension_expressions_identifiers` for more
            information.
        check : `bool`, optional
            If `True` (default) check the query for consistency before
            executing it.  This may reject some valid queries that resemble
            common mistakes (e.g. queries for visits without specifying an
            instrument).
        **kwargs
            Additional keyword arguments are forwarded to
            `DataCoordinate.standardize` when processing the ``dataId``
            argument (and may be used to provide a constraining data ID even
            when the ``dataId`` argument is `None`).

        Returns
        -------
        dataIds : `.queries.DataCoordinateQueryResults`
            Data IDs matching the given query parameters.  These are guaranteed
            to identify all dimensions (`DataCoordinate.hasFull` returns
            `True`), but will not contain `DimensionRecord` objects
            (`DataCoordinate.hasRecords` returns `False`).  Call
            `~.queries.DataCoordinateQueryResults.expanded` on the
            returned object to fetch those (and consider using
            `~.queries.DataCoordinateQueryResults.materialize` on the
            returned object first if the expected number of rows is very
            large). See documentation for those methods for additional
            information.

        Raises
        ------
        lsst.daf.butler.registry.NoDefaultCollectionError
            Raised if ``collections`` is `None` and
            ``self.defaults.collections`` is `None`.
        lsst.daf.butler.registry.CollectionExpressionError
            Raised when ``collections`` expression is invalid.
        lsst.daf.butler.registry.DataIdError
            Raised when ``dataId`` or keyword arguments specify unknown
            dimensions or values, or when they contain inconsistent values.
        lsst.daf.butler.registry.DatasetTypeExpressionError
            Raised when ``datasetType`` expression is invalid.
        lsst.daf.butler.registry.UserExpressionError
            Raised when ``where`` expression is invalid.
        """
        raise NotImplementedError()

    @abstractmethod
    def queryDimensionRecords(
        self,
        element: DimensionElement | str,
        *,
        dataId: DataId | None = None,
        datasets: Any = None,
        collections: CollectionArgType | None = None,
        where: str = "",
        bind: Mapping[str, Any] | None = None,
        check: bool = True,
        **kwargs: Any,
    ) -> DimensionRecordQueryResults:
        """Query for dimension information matching user-provided criteria.

        Parameters
        ----------
        element : `DimensionElement` or `str`
            The dimension element to obtain records for.
        dataId : `dict` or `DataCoordinate`, optional
            A data ID whose key-value pairs are used as equality constraints
            in the query.
        datasets : dataset type expression, optional
            An expression that fully or partially identifies dataset types
            that should constrain the yielded records.  See `queryDataIds` and
            :ref:`daf_butler_dataset_type_expressions` for more information.
        collections : collection expression, optional
            An expression that identifies the collections to search for
            datasets, such as a `str` (for full matches  or partial matches
            via globs), `re.Pattern` (for partial matches), or iterable
            thereof.  ``...`` can be used to search all collections (actually
            just all `~CollectionType.RUN` collections, because this will
            still find all datasets).  If not provided,
            ``self.default.collections`` is used.  Ignored unless ``datasets``
            is also passed.  See :ref:`daf_butler_collection_expressions` for
            more information.
        where : `str`, optional
            A string expression similar to a SQL WHERE clause.  See
            `queryDataIds` and :ref:`daf_butler_dimension_expressions` for more
            information.
        bind : `~collections.abc.Mapping`, optional
            Mapping containing literal values that should be injected into the
            ``where`` expression, keyed by the identifiers they replace.
            Values of collection type can be expanded in some cases; see
            :ref:`daf_butler_dimension_expressions_identifiers` for more
            information.
        check : `bool`, optional
            If `True` (default) check the query for consistency before
            executing it.  This may reject some valid queries that resemble
            common mistakes (e.g. queries for visits without specifying an
            instrument).
        **kwargs
            Additional keyword arguments are forwarded to
            `DataCoordinate.standardize` when processing the ``dataId``
            argument (and may be used to provide a constraining data ID even
            when the ``dataId`` argument is `None`).

        Returns
        -------
        dataIds : `.queries.DimensionRecordQueryResults`
            Data IDs matching the given query parameters.

        Raises
        ------
        lsst.daf.butler.registry.NoDefaultCollectionError
            Raised if ``collections`` is `None` and
            ``self.defaults.collections`` is `None`.
        lsst.daf.butler.registry.CollectionExpressionError
            Raised when ``collections`` expression is invalid.
        lsst.daf.butler.registry.DataIdError
            Raised when ``dataId`` or keyword arguments specify unknown
            dimensions or values, or when they contain inconsistent values.
        lsst.daf.butler.registry.DatasetTypeExpressionError
            Raised when ``datasetType`` expression is invalid.
        lsst.daf.butler.registry.UserExpressionError
            Raised when ``where`` expression is invalid.
        """
        raise NotImplementedError()

    @abstractmethod
    def queryDatasetAssociations(
        self,
        datasetType: str | DatasetType,
        collections: CollectionArgType | None = ...,
        *,
        collectionTypes: Iterable[CollectionType] = CollectionType.all(),
        flattenChains: bool = False,
    ) -> Iterator[DatasetAssociation]:
        """Iterate over dataset-collection combinations where the dataset is in
        the collection.

        This method is a temporary placeholder for better support for
        association results in `queryDatasets`.  It will probably be
        removed in the future, and should be avoided in production code
        whenever possible.

        Parameters
        ----------
        datasetType : `DatasetType` or `str`
            A dataset type object or the name of one.
        collections : collection expression, optional
            An expression that identifies the collections to search for
            datasets, such as a `str` (for full matches  or partial matches
            via globs), `re.Pattern` (for partial matches), or iterable
            thereof.  ``...`` can be used to search all collections (actually
            just all `~CollectionType.RUN` collections, because this will still
            find all datasets).  If not provided, ``self.default.collections``
            is used.  See :ref:`daf_butler_collection_expressions` for more
            information.
        collectionTypes : `~collections.abc.Set` [ `CollectionType` ], optional
            If provided, only yield associations from collections of these
            types.
        flattenChains : `bool`, optional
            If `True`, search in the children of `~CollectionType.CHAINED`
            collections.  If `False`, ``CHAINED`` collections are ignored.

        Yields
        ------
        association : `.DatasetAssociation`
            Object representing the relationship between a single dataset and
            a single collection.

        Raises
        ------
        lsst.daf.butler.registry.NoDefaultCollectionError
            Raised if ``collections`` is `None` and
            ``self.defaults.collections`` is `None`.
        lsst.daf.butler.registry.CollectionExpressionError
            Raised when ``collections`` expression is invalid.
        """
        raise NotImplementedError()

    @property
    def obsCoreTableManager(self) -> ObsCoreTableManager | None:
        """The ObsCore manager instance for this registry
        (`~.interfaces.ObsCoreTableManager`
        or `None`).

        ObsCore manager may not be implemented for all registry backend, or
        may not be enabled for many repositories.
        """
        return None

    storageClasses: StorageClassFactory
    """All storage classes known to the registry (`StorageClassFactory`).
    """

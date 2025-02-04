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

__all__ = ("LimitedButler",)

import logging
from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import Any, ClassVar

from lsst.resources import ResourcePath

from ._dataset_provenance import DatasetProvenance
from ._dataset_ref import DatasetRef
from ._deferredDatasetHandle import DeferredDatasetHandle
from ._storage_class import StorageClass, StorageClassFactory
from .datastore import DatasetRefURIs, Datastore
from .dimensions import DimensionUniverse

log = logging.getLogger(__name__)


class LimitedButler(ABC):
    """A minimal butler interface that is sufficient to back
    `~lsst.pipe.base.PipelineTask` execution.
    """

    GENERATION: ClassVar[int] = 3
    """This is a Generation 3 Butler.

    This attribute may be removed in the future, once the Generation 2 Butler
    interface has been fully retired; it should only be used in transitional
    code.
    """

    @abstractmethod
    def isWriteable(self) -> bool:
        """Return `True` if this `Butler` supports write operations."""
        raise NotImplementedError()

    @abstractmethod
    def put(self, obj: Any, ref: DatasetRef, /, *, provenance: DatasetProvenance | None = None) -> DatasetRef:
        """Store a dataset that already has a UUID and ``RUN`` collection.

        Parameters
        ----------
        obj : `object`
            The dataset.
        ref : `DatasetRef`
            Resolved reference for a not-yet-stored dataset.
        provenance : `DatasetProvenance` or `None`, optional
            Any provenance that should be attached to the serialized dataset.
            Not supported by all serialization mechanisms.

        Returns
        -------
        ref : `DatasetRef`
            The same as the given, for convenience and symmetry with
            `Butler.put`.

        Raises
        ------
        TypeError
            Raised if the butler is read-only.

        Notes
        -----
        Whether this method inserts the given dataset into a ``Registry`` is
        implementation defined (some `LimitedButler` subclasses do not have a
        `Registry`), but it always adds the dataset to a `Datastore`, and the
        given ``ref.id`` and ``ref.run`` are always preserved.
        """
        raise NotImplementedError()

    def get(
        self,
        ref: DatasetRef,
        /,
        *,
        parameters: dict[str, Any] | None = None,
        storageClass: StorageClass | str | None = None,
    ) -> Any:
        """Retrieve a stored dataset.

        Parameters
        ----------
        ref : `DatasetRef`
            A resolved `DatasetRef` directly associated with a dataset.
        parameters : `dict`
            Additional StorageClass-defined options to control reading,
            typically used to efficiently read only a subset of the dataset.
        storageClass : `StorageClass` or `str`, optional
            The storage class to be used to override the Python type
            returned by this method. By default the returned type matches
            the dataset type definition for this dataset. Specifying a
            read `StorageClass` can force a different type to be returned.
            This type must be compatible with the original type.

        Returns
        -------
        obj : `object`
            The dataset.

        Raises
        ------
        AmbiguousDatasetError
            Raised if the supplied `DatasetRef` is unresolved.

        Notes
        -----
        In a `LimitedButler` the only allowable way to specify a dataset is
        to use a resolved `DatasetRef`. Subclasses can support more options.
        """
        log.debug("Butler get: %s, parameters=%s, storageClass: %s", ref, parameters, storageClass)
        return self._datastore.get(ref, parameters=parameters, storageClass=storageClass)

    def getDeferred(
        self,
        ref: DatasetRef,
        /,
        *,
        parameters: dict[str, Any] | None = None,
        storageClass: str | StorageClass | None = None,
    ) -> DeferredDatasetHandle:
        """Create a `DeferredDatasetHandle` which can later retrieve a dataset,
        after an immediate registry lookup.

        Parameters
        ----------
        ref : `DatasetRef`
            For the default implementation of a `LimitedButler`, the only
            acceptable parameter is a resolved `DatasetRef`.
        parameters : `dict`
            Additional StorageClass-defined options to control reading,
            typically used to efficiently read only a subset of the dataset.
        storageClass : `StorageClass` or `str`, optional
            The storage class to be used to override the Python type
            returned by this method. By default the returned type matches
            the dataset type definition for this dataset. Specifying a
            read `StorageClass` can force a different type to be returned.
            This type must be compatible with the original type.

        Returns
        -------
        obj : `DeferredDatasetHandle`
            A handle which can be used to retrieve a dataset at a later time.

        Notes
        -----
        In a `LimitedButler` the only allowable way to specify a dataset is
        to use a resolved `DatasetRef`. Subclasses can support more options.
        """
        return DeferredDatasetHandle(butler=self, ref=ref, parameters=parameters, storageClass=storageClass)

    def get_datastore_names(self) -> tuple[str, ...]:
        """Return the names of the datastores associated with this butler.

        Returns
        -------
        names : `tuple` [`str`, ...]
            The names of the datastores.
        """
        return self._datastore.names

    def get_datastore_roots(self) -> dict[str, ResourcePath | None]:
        """Return the defined root URIs for all registered datastores.

        Returns
        -------
        roots : `dict` [`str`, `~lsst.resources.ResourcePath` | `None`]
            A mapping from datastore name to datastore root URI. The root
            can be `None` if the datastore does not have any concept of a root
            URI.
        """
        return self._datastore.roots

    def getURIs(
        self,
        ref: DatasetRef,
        /,
        *,
        predict: bool = False,
    ) -> DatasetRefURIs:
        """Return the URIs associated with the dataset.

        Parameters
        ----------
        ref : `DatasetRef`
            A `DatasetRef` for which URIs are requested.
        predict : `bool`
            If `True`, allow URIs to be returned of datasets that have not
            been written.

        Returns
        -------
        uris : `DatasetRefURIs`
            The URI to the primary artifact associated with this dataset (if
            the dataset was disassembled within the datastore this may be
            `None`), and the URIs to any components associated with the dataset
            artifact (can be empty if there are no components).
        """
        return self._datastore.getURIs(ref, predict)

    def getURI(
        self,
        ref: DatasetRef,
        /,
        *,
        predict: bool = False,
    ) -> ResourcePath:
        """Return the URI to the Dataset.

        Parameters
        ----------
        ref : `DatasetRef`
            A `DatasetRef` for which a single URI is requested.
        predict : `bool`
            If `True`, allow URIs to be returned of datasets that have not
            been written.

        Returns
        -------
        uri : `lsst.resources.ResourcePath`
            URI pointing to the Dataset within the datastore. If the
            Dataset does not exist in the datastore, and if ``predict`` is
            `True`, the URI will be a prediction and will include a URI
            fragment "#predicted".
            If the datastore does not have entities that relate well
            to the concept of a URI the returned URI string will be
            descriptive. The returned URI is not guaranteed to be obtainable.

        Raises
        ------
        RuntimeError
            Raised if a URI is requested for a dataset that consists of
            multiple artifacts.
        """
        primary, components = self.getURIs(ref, predict=predict)

        if primary is None or components:
            raise RuntimeError(
                f"Dataset ({ref}) includes distinct URIs for components. Use LimitedButler.getURIs() instead."
            )
        return primary

    def get_many_uris(
        self,
        refs: Iterable[DatasetRef],
        predict: bool = False,
        allow_missing: bool = False,
    ) -> dict[DatasetRef, DatasetRefURIs]:
        """Return URIs associated with many datasets.

        Parameters
        ----------
        refs : iterable of `DatasetIdRef`
            References to the required datasets.
        predict : `bool`, optional
            If `True`, allow URIs to be returned of datasets that have not
            been written.
        allow_missing : `bool`
            If `False`, and ``predict`` is `False`, will raise if a
            `DatasetRef` does not exist.

        Returns
        -------
        URIs : `dict` of [`DatasetRef`, `DatasetRefURIs`]
            A dict of primary and component URIs, indexed by the passed-in
            refs.

        Raises
        ------
        FileNotFoundError
            A URI has been requested for a dataset that does not exist and
            guessing is not allowed.

        Notes
        -----
        In file-based datastores, get_many_uris does not check that the file is
        present. It assumes that if datastore is aware of the file then it
        actually exists.
        """
        return self._datastore.getManyURIs(refs, predict=predict, allow_missing=allow_missing)

    def stored(self, ref: DatasetRef) -> bool:
        """Indicate whether the dataset's artifacts are present in the
        Datastore.

        Parameters
        ----------
        ref : `DatasetRef`
            Resolved reference to a dataset.

        Returns
        -------
        stored : `bool`
            Whether the dataset artifact exists in the datastore and can be
            retrieved.
        """
        return self._datastore.exists(ref)

    def stored_many(
        self,
        refs: Iterable[DatasetRef],
    ) -> dict[DatasetRef, bool]:
        """Check the datastore for artifact existence of multiple datasets
        at once.

        Parameters
        ----------
        refs : iterable of `DatasetRef`
            The datasets to be checked.

        Returns
        -------
        existence : `dict` of [`DatasetRef`, `bool`]
            Mapping from given dataset refs to boolean indicating artifact
            existence.
        """
        return self._datastore.mexists(refs)

    def markInputUnused(self, ref: DatasetRef) -> None:
        """Indicate that a predicted input was not actually used when
        processing a `Quantum`.

        Parameters
        ----------
        ref : `DatasetRef`
            Reference to the unused dataset.

        Notes
        -----
        By default, a dataset is considered "actually used" if it is accessed
        via `get` or a handle to it is obtained via `getDeferred`
        (even if the handle is not used).  This method must be called after one
        of those in order to remove the dataset from the actual input list.

        This method does nothing for butlers that do not store provenance
        information (which is the default implementation provided by the base
        class).
        """
        pass

    @abstractmethod
    def pruneDatasets(
        self,
        refs: Iterable[DatasetRef],
        *,
        disassociate: bool = True,
        unstore: bool = False,
        tags: Iterable[str] = (),
        purge: bool = False,
    ) -> None:
        """Remove one or more datasets from a collection and/or storage.

        Parameters
        ----------
        refs : `~collections.abc.Iterable` of `DatasetRef`
            Datasets to prune.  These must be "resolved" references (not just
            a `DatasetType` and data ID).
        disassociate : `bool`, optional
            Disassociate pruned datasets from ``tags``, or from all collections
            if ``purge=True``.
        unstore : `bool`, optional
            If `True` (`False` is default) remove these datasets from all
            datastores known to this butler.  Note that this will make it
            impossible to retrieve these datasets even via other collections.
            Datasets that are already not stored are ignored by this option.
        tags : `~collections.abc.Iterable` [ `str` ], optional
            `~CollectionType.TAGGED` collections to disassociate the datasets
            from.  Ignored if ``disassociate`` is `False` or ``purge`` is
            `True`.
        purge : `bool`, optional
            If `True` (`False` is default), completely remove the dataset from
            the `Registry`.  To prevent accidental deletions, ``purge`` may
            only be `True` if all of the following conditions are met:

             - ``disassociate`` is `True`;
             - ``unstore`` is `True`.

            This mode may remove provenance information from datasets other
            than those provided, and should be used with extreme care.

        Raises
        ------
        TypeError
            Raised if the butler is read-only, if no collection was provided,
            or the conditions for ``purge=True`` were not met.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def dimensions(self) -> DimensionUniverse:
        """Structure managing all dimensions recognized by this data
        repository (`DimensionUniverse`).
        """
        raise NotImplementedError()

    _datastore: Datastore
    """The object that manages actual dataset storage (`Datastore`)."""

    storageClasses: StorageClassFactory
    """An object that maps known storage class names to objects that fully
    describe them (`StorageClassFactory`).
    """

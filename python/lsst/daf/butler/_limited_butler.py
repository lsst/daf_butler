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

__all__ = ("LimitedButler",)

import logging
from abc import ABC, abstractmethod
from typing import Any, ClassVar, Dict, Iterable, Optional, Union

from ._deferredDatasetHandle import DeferredDatasetHandle
from .core import (
    AmbiguousDatasetError,
    DatasetRef,
    Datastore,
    DimensionUniverse,
    StorageClass,
    StorageClassFactory,
)

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
    def putDirect(self, obj: Any, ref: DatasetRef) -> DatasetRef:
        """Store a dataset that already has a UUID and ``RUN`` collection.

        Parameters
        ----------
        obj : `object`
            The dataset.
        ref : `DatasetRef`
            Resolved reference for a not-yet-stored dataset.

        Returns
        -------
        ref : `DatasetRef`
            The same as the given, for convenience and symmetry with
            `Butler.put`.

        Raises
        ------
        TypeError
            Raised if the butler is read-only.
        AmbiguousDatasetError
            Raised if ``ref.id is None``, i.e. the reference is unresolved.

        Notes
        -----
        Whether this method inserts the given dataset into a ``Registry`` is
        implementation defined (some `LimitedButler` subclasses do not have a
        `Registry`), but it always adds the dataset to a `Datastore`, and the
        given ``ref.id`` and ``ref.run`` are always preserved.
        """
        raise NotImplementedError()

    def getDirect(
        self,
        ref: DatasetRef,
        *,
        parameters: Optional[Dict[str, Any]] = None,
        storageClass: str | StorageClass | None = None,
    ) -> Any:
        """Retrieve a stored dataset.

        Unlike `Butler.get`, this method allows datasets outside the Butler's
        collection to be read as long as the `DatasetRef` that identifies them
        can be obtained separately.

        Parameters
        ----------
        ref : `DatasetRef`
            Resolved reference to an already stored dataset.
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
            Raised if ``ref.id is None``, i.e. the reference is unresolved.
        """
        return self.datastore.get(ref, parameters=parameters, storageClass=storageClass)

    def getDirectDeferred(
        self,
        ref: DatasetRef,
        *,
        parameters: Union[dict, None] = None,
        storageClass: str | StorageClass | None = None,
    ) -> DeferredDatasetHandle:
        """Create a `DeferredDatasetHandle` which can later retrieve a dataset,
        from a resolved `DatasetRef`.

        Parameters
        ----------
        ref : `DatasetRef`
            Resolved reference to an already stored dataset.
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

        Raises
        ------
        AmbiguousDatasetError
            Raised if ``ref.id is None``, i.e. the reference is unresolved.
        """
        if ref.id is None:
            raise AmbiguousDatasetError(
                f"Dataset of type {ref.datasetType.name} with data ID {ref.dataId} is not resolved."
            )
        return DeferredDatasetHandle(butler=self, ref=ref, parameters=parameters, storageClass=storageClass)

    def datasetExistsDirect(self, ref: DatasetRef) -> bool:
        """Return `True` if a dataset is actually present in the Datastore.

        Parameters
        ----------
        ref : `DatasetRef`
            Resolved reference to a dataset.

        Returns
        -------
        exists : `bool`
            Whether the dataset exists in the Datastore.
        """
        return self.datastore.exists(ref)

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
        via `getDirect` or a handle to it is obtained via `getDirectDeferred`
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
        tags : `Iterable` [ `str` ], optional
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

    datastore: Datastore
    """The object that manages actual dataset storage (`Datastore`).

    Direct user access to the datastore should rarely be necessary; the primary
    exception is the case where a `Datastore` implementation provides extra
    functionality beyond what the base class defines.
    """

    storageClasses: StorageClassFactory
    """An object that maps known storage class names to objects that fully
    describe them (`StorageClassFactory`).
    """

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

"""Module containing classes used with deferring dataset loading."""

from __future__ import annotations

__all__ = ("DeferredDatasetHandle",)

import dataclasses
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ._dataset_ref import DatasetRef
    from ._limited_butler import LimitedButler
    from ._storage_class import StorageClass
    from .dimensions import DataCoordinate


@dataclasses.dataclass(frozen=True)
class DeferredDatasetHandle:
    """Proxy class that provides deferred loading of datasets from a butler."""

    def get(
        self,
        *,
        component: str | None = None,
        parameters: dict | None = None,
        storageClass: str | StorageClass | None = None,
    ) -> Any:
        """Retrieve the dataset pointed to by this handle.

        This handle may be used multiple times, possibly with different
        parameters.

        Parameters
        ----------
        component : `str` or None
            If the deferred object is a component dataset type, this parameter
            may specify the name of the component to use in the get operation.
            The component is looked up in the storage class given when this
            object was created, if there was one, and not in the storage class
            the dataset was written with.
        parameters : `dict` or None
            The parameters argument will be passed to the butler get method.
            It defaults to None. If the value is not None,  this dict will
            be merged with the parameters dict used to construct the
            `DeferredDatasetHandle` class.
        storageClass : `StorageClass` or `str`, optional
            The storage class to be used to override the Python type
            returned by this method. By default the returned type matches
            the dataset type definition for this dataset or the storage
            class specified when this object was created. Specifying a
            read `StorageClass` can force a different type to be returned.
            This type must be compatible with the original type. If
            ``component`` is given this overrides the type of the component
            rather than that of the composite.

        Returns
        -------
        return : `Object`
            The dataset pointed to by this handle.
        """
        if self.parameters is not None:
            mergedParameters = self.parameters.copy()
            if parameters is not None:
                mergedParameters.update(parameters)
        elif parameters is not None:
            mergedParameters = parameters
        else:
            mergedParameters = {}

        if component is None:
            # No component so apply the storage class override that was either
            # given explicitly or provided to the constructor.
            if storageClass is None:
                storageClass = self.storageClass
            return self.butler.get(self.ref, parameters=mergedParameters, storageClass=storageClass)

        # We have a component request.
        # The instance storageClass refers to the composite and so should
        # not be applied to the component. If set we apply it to the
        # requested ref before looking for components.
        ref = self.ref
        if self.storageClass is not None:
            ref = ref.overrideStorageClass(self.storageClass)

        # Now the component request goes through the overridden composite.
        ref = ref.makeComponentRef(component)

        # And run get with the override storage class if given.
        return self.butler.get(ref, parameters=mergedParameters, storageClass=storageClass)

    @property
    def dataId(self) -> DataCoordinate:
        """The full data ID associated with the dataset
        (`DataCoordinate`).

        Guaranteed to contain records.
        """
        return self.ref.dataId

    butler: LimitedButler
    """The butler that will be used to fetch the dataset (`LimitedButler`).
    """

    ref: DatasetRef
    """Reference to the dataset (`DatasetRef`).
    """

    parameters: dict | None
    """Optional parameters that may be used to specify a subset of the dataset
    to be loaded (`dict` or `None`).
    """

    storageClass: str | StorageClass | None = None
    """Optional storage class override that can be applied on ``get()``."""

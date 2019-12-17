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

"""
Module containing classes used with deferring dataset loading
"""

__all__ = ("DeferredDatasetHandle",)

import dataclasses
import typing
import types

from .core import DataId, DatasetRef, DatasetType

if typing.TYPE_CHECKING:
    from .butler import Butler


@dataclasses.dataclass(frozen=True)
class DeferredDatasetHandle:
    """This is a class to support deferred loading of a dataset from a butler.

    Parameters
    ----------
    butler : `Butler`
        The butler that will be used to fetch the deferred dataset
    datasetRefOrType : `DatasetRef`, `DatasetType`, or `str`
        When `DatasetRef` the `dataId` should be `None`.
        Otherwise the `DatasetType` or name thereof.
    dataId : `dict` or `DataCoordinate`, optional
        A dictionary of `Dimension` link name, value pairs that label the
        `DatasetRef` within a Collection. When `None`, a `DatasetRef`
        should be provided as the first argument.
    parameters : `dict`
        Additional StorageClass-defined options to control reading,
        typically used to efficiently read only a subset of the dataset.
    kwds : `dict`
        Additional keyword arguments used to augment or construct a
        `DataId`.  See `DataId` construction parameters.

    """

    datasetRefOrType: typing.Union[DatasetRef, DatasetType, str]
    dataId: DataId
    parameters: typing.Union[dict, None]
    kwds: dict

    def __init__(self, butler: 'Butler', datasetRefOrType: typing.Union[DatasetRef, DatasetType, str],
                 dataId: typing.Union[dict, DataId], parameters: typing.Union[dict, None], kwds: dict):
        object.__setattr__(self, 'datasetRefOrType', datasetRefOrType)
        object.__setattr__(self, 'dataId', dataId)
        object.__setattr__(self, 'parameters', parameters)
        object.__setattr__(self, 'kwds', kwds)

        # Closure over butler to discourage accessing a raw butler through a
        # deferred handle
        def _get(self, parameters: typing.Union[None, dict]) -> typing.Any:
            return butler.get(self.datasetRefOrType, self.dataId, parameters, **self.kwds)

        object.__setattr__(self, '_get', types.MethodType(_get, self))

    def get(self, parameters: typing.Union[None, dict] = None, **kwargs: dict) -> typing.Any:
        """ Retrieves the dataset pointed to by this handle

        This handle may be used multiple times, possibly with different
        parameters.

        Parameters
        ----------
        parameters : `dict` or None
            The parameters argument will be passed to the butler get method.
            It defaults to None. If the value is not None,  this dict will
            be merged with the parameters dict used to construct the
            `DeferredDatasetHandle` class.
        kwargs : `dict`
            This argument is deprecated and only exists to support legacy
            gen2 butler code during migration. It is completely ignored
            and will be removed in the future.

        Returns
        -------
        return : `Object`
            The dataset pointed to by this handle
        """
        if self.parameters is not None:
            mergedParameters = self.parameters.copy()
            if parameters is not None:
                mergedParameters.update(parameters)
        elif parameters is not None:
            mergedParameters = parameters
        else:
            mergedParameters = {}

        return self._get(mergedParameters)

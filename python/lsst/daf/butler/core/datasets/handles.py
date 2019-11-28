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

__all__ = ["PartialDatasetHandle", "CheckedDatasetHandle", "ResolvedDatasetHandle"]

import hashlib
from typing import Dict, Optional

from .type import DatasetType
from ..dimensions import DataId, DataCoordinate
from ..utils import immutable


class _LookupNamesForDatasetHandleBase:

    def _lookupNames(self):
        """Name keys to use when looking up this dataset in a configuration.

        The names are returned in order of priority.

        Returns
        -------
        names : `tuple` of `LookupKey`
            Tuple of the `DatasetType` name and the `StorageClass` name.
            If ``instrument`` is defined in the dataId, each of those names
            is added to the start of the tuple with a key derived from the
            value of ``instrument``.
        """
        # Special case the instrument Dimension since we allow configs
        # to include the instrument name in the hierarchy.
        names = tuple(self.datasetType._lookupNames())
        if "instrument" in self.dataId:
            names += tuple(n.clone(dataId={"instrument": self.dataId["instrument"]}) for n in names)
        return names


class PartialDatasetHandle(_LookupNamesForDatasetHandleBase):

    __slots__ = ("datasetType", "dataId")

    def __init__(self, datasetType: DatasetType, dataId: DataCoordinate):
        self.datasetType = datasetType
        self.dataId = dataId

    def __repr__(self):
        return f"PartialDatasetHandle({self.datasetType!r}, {self.dataId!r})"

    def __str__(self):
        return f"({self.datasetType}, {self.dataId})"

    def __eq__(self, other):
        return self.datasetType == other.datasetType and self.dataId == other.dataId

    def checked(self):
        return CheckedDatasetHandle(self.datasetType, self.dataId)

    def isComponent(self):
        """Boolean indicating whether this dataset refers to a component of a
        composite.

        Returns
        -------
        isComponent : `bool`
            `True` if this dataset is a component, `False` otherwise.
        """
        return self.datasetType.isComponent()

    def isComposite(self):
        """Boolean indicating whether this dataset is a composite type.

        Returns
        -------
        isComposite : `bool`
            `True` if this dataset is a composite type, `False` otherwise.
        """
        return self.datasetType.isComposite()

    datasetType: DatasetType

    dataId: DataId


@immutable
class CheckedDatasetHandle(_LookupNamesForDatasetHandleBase):

    __slots__ = ("datasetType", "dataId", "_hash",)

    def __new__(cls, datasetType: DatasetType, dataId: DataId):
        self = super().__new__(cls)
        self.datasetType = datasetType
        self.dataId = DataCoordinate.standardize(dataId, graph=datasetType.dimensions)
        return self

    @property
    def hash(self):
        """Secure hash of the `DatasetType` name and Data ID (`bytes`).
        """
        if not hasattr(self, "_hash"):
            message = hashlib.blake2b(digest_size=32)
            message.update(self.datasetType.name.encode("utf8"))
            self.dataId.fingerprint(message.update)
            self._hash = message.digest()
        return self._hash

    @property
    def dimensions(self):
        """The dimensions associated with the underlying `DatasetType` and
        Data ID (`DimensionGraph`).
        """
        return self.datasetType.dimensions

    def isComponent(self):
        """Boolean indicating whether this dataset is a
        component of a composite.

        Returns
        -------
        isComponent : `bool`
            `True` if this dataset is a component, `False` otherwise.
        """
        return self.datasetType.isComponent()

    def isComposite(self):
        """Boolean indicating whether this dataset is a composite type.

        Returns
        -------
        isComposite : `bool`
            `True` if this dataset is a composite type, `False`
            otherwise.
        """
        return self.datasetType.isComposite()

    def __repr__(self):
        return f"CheckedDatasetHandle({self.datasetType!r}, {self.dataId!r})"

    def __str__(self):
        return f"({self.datasetType}, {self.dataId})"

    def __eq__(self, other):
        return self.datasetType == other.datasetType and self.dataId == other.dataId

    def __get_newargs__(self):
        return (self.datasetType, self.dataId)

    datasetType: DatasetType

    dataId: DataCoordinate


class ResolvedDatasetHandle(CheckedDatasetHandle):

    __slots__ = ("id", "run", "components")

    def __new__(cls, datasetType: DatasetType, dataId: DataCoordinate, id: int, origin: int,
                run: str, components: Optional[Dict[str, ResolvedDatasetHandle]] = None):
        self = super().__new__(cls, datasetType, dataId)
        self.id = id
        self.origin = origin
        self.run = run
        self.components = components
        return self

    def unresolved(self):
        return CheckedDatasetHandle(self.datasetType, self.dataId)

    def __repr__(self):
        return (f"ResolvedDatasetHandle({self.datasetType.name!r}, {self.dataId!r}, "
                f"id={self.id}, run={self.run!r})")

    def __str__(self):
        return f"({self.datasetType.name}, {self.dataId}, id={self.id}, run={self.run})"

    def __get_newargs__(self):
        return (self.datasetType, self.dataId, self.id, self.run)

    id: int

    origin: int

    run: str

    components: Dict[str, ResolvedDatasetHandle]

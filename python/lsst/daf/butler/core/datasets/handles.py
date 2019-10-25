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

__all__ = ["DatasetHandle", "CheckedDatasetHandle", "ResolvedDatasetHandle"]

import hashlib
from typing import Dict
from types import MappingProxyType

from .type import DatasetType
from ..dimensions import DataId, DataCoordinate, ExpandedDataCoordinate
from ..run import Run
from ..utils import immutable


@immutable
class DatasetHandle:

    __slots__ = ("datasetType", "dataId")

    def __new__(cls, datasetType: DatasetType, dataId: DataCoordinate):
        self = super().__new__(cls)
        self.datasetType = datasetType
        if not isinstance(dataId, DataCoordinate):
            dataId = MappingProxyType(dict(dataId))
        self.dataId = dataId
        return self

    def __repr__(self):
        return f"DatasetHandle({self.datasetType!r}, {self.dataId!r})"

    def __str__(self):
        return f"({self.datasetType}, {self.dataId})"

    def __eq__(self, other):
        return self.datasetType == other.datasetType and self.dataId == other.dataId

    def __get_newargs__(self):
        return (self.datasetType, self.dataId)

    def checked(self):
        return CheckedDatasetHandle(self.datasetType, self.dataId)

    def isComponent(self):
        """Boolean indicating whether this `DatasetHandle` refers to a
        component of a composite.

        Returns
        -------
        isComponent : `bool`
            `True` if this `DatasetHandle` is a component, `False` otherwise.
        """
        return self.datasetType.isComponent()

    def isComposite(self):
        """Boolean indicating whether this `DatasetHandle` is a composite type.

        Returns
        -------
        isComposite : `bool`
            `True` if this `DatasetHandle` is a composite type, `False`
            otherwise.
        """
        return self.datasetType.isComposite()

    def _lookupNames(self):
        """Name keys to use when looking up this DatasetHandle in a
        configuration.

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
        names = self.datasetType._lookupNames()

        if "instrument" in self.dataId:
            names = tuple(n.clone(dataId={"instrument": self.dataId["instrument"]})
                          for n in names) + names

        return names

    datasetType: DatasetType

    dataId: DataId


class CheckedDatasetHandle(DatasetHandle):

    __slots__ = ("_hash",)

    def __new__(cls, datasetType: DatasetType, dataId: DataId):
        dataId = DataCoordinate.standardize(dataId, graph=datasetType.dimensions)
        return super().__new__(cls, datasetType, dataId)

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


class ResolvedDatasetHandle(CheckedDatasetHandle):

    __slots__ = ("id", "run", "components")

    def __new__(cls, datasetType: DatasetType, dataId: ExpandedDataCoordinate, run: Run,
                components: Dict[str, ResolvedDatasetHandle]):
        self = super().__new__(cls, datasetType, dataId)
        assert isinstance(self.dataId, ExpandedDataCoordinate)
        self.run = run
        self.components = dict(components)
        return self

    def unresolved(self):
        return CheckedDatasetHandle(self.datasetType, self.dataId)

    def __repr__(self):
        # We keep repr concise by not trying to include components.
        return (f"ResolvedDatasetHandle({self.datasetType!r}, {self.dataId!r}, "
                f"id={self.id}, run={self.run!r}, ...)")

    def __str__(self):
        front = f"({self.datasetType}, {self.dataId}, id={self.id}, run={self.run}"
        if self.components is not None:
            return front + f", components={set(self.components.keys())})"
        else:
            return front + ")"

    def __get_newargs__(self):
        return (self.datasetType, self.dataId, self.run, self.components)

    id: int

    run: Run

    components: Dict[str, ResolvedDatasetHandle]

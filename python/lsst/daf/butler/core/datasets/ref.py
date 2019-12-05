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

__all__ = ["DatasetRef"]

import hashlib
from typing import Mapping, Optional, Tuple

from types import MappingProxyType
from ..dimensions import DataCoordinate, DimensionGraph
from ..run import Run
from ..configSupport import LookupKey
from .type import DatasetType


def _safeMakeMappingProxyType(data):
    if data is None:
        data = {}
    return MappingProxyType(data)


class DatasetRef:
    """Reference to a Dataset in a `Registry`.

    A `DatasetRef` may point to a Dataset that currently does not yet exist
    (e.g., because it is a predicted input for provenance).

    Parameters
    ----------
    datasetType : `DatasetType`
        The `DatasetType` for this Dataset.
    dataId : `DataCoordinate`
        A mapping of dimensions that labels the Dataset within a Collection.
    id : `int`, optional
        The unique integer identifier assigned when the dataset is created.
    run : `Run`, optional
        The run this dataset was associated with when it was created.
    hash : `bytes`, optional
        A hash of the dataset type and data ID.  Should only be provided if
        copying from another `DatasetRef` with the same dataset type and data
        ID.
    components : `dict`, optional
        A dictionary mapping component name to a `DatasetRef` for that
        component.
    conform : `bool`, optional
        If `True` (default), call `DataCoordinate.standardize` to ensure that
        the data ID's dimensions are consistent with the dataset type's.
        `DatasetRef` instances for which those dimensions are not equal should
        not be created in new code, but are still supported for backwards
        compatibility.  New code should only pass `False` if it can guarantee
        that the dimensions are already consistent.
    """

    __slots__ = ("_id", "_datasetType", "_dataId", "_run", "_hash", "_components")

    def __init__(self, datasetType: DatasetType, dataId: DataCoordinate, *,
                 id: Optional[int] = None,
                 run: Optional[Run] = None, hash: Optional[bytes] = None,
                 components: Optional[Mapping[str, DatasetRef]] = None, conform: bool = True):
        assert isinstance(datasetType, DatasetType)
        # TODO: it would be nice to guarantee that id and run should be either
        # both None or not None together.  We can't easily do that yet because
        # the Query infrastructure has a hard time obtaining Run objects, but
        # that will change.
        self._id = id
        self._datasetType = datasetType
        if conform:
            self._dataId = DataCoordinate.standardize(dataId, graph=datasetType.dimensions)
        else:
            self._dataId = dataId
        self._components = dict()
        if components is not None:
            self._components.update(components)
        self._run = run
        self._hash = hash

    def __eq__(self, other: DatasetRef):
        return (self._datasetType, self.dataId, self._id) == (other._datasetType, other._dataId, other._id)

    def __repr__(self):
        return f"DatasetRef({self.datasetType}, {self.dataId}, id={self.id}, run={self.run})"

    @property
    def id(self) -> Optional[int]:
        """Primary key of the dataset (`int` or `None`)

        Typically assigned by `Registry`.
        """
        return self._id

    @property
    def hash(self) -> bytes:
        """Secure hash of the `DatasetType` name and data ID (`bytes`).
        """
        if self._hash is None:
            message = hashlib.blake2b(digest_size=32)
            message.update(self.datasetType.name.encode("utf8"))
            self.dataId.fingerprint(message.update)
            self._hash = message.digest()
        return self._hash

    @property
    def datasetType(self) -> DatasetType:
        """The `DatasetType` associated with the Dataset the `DatasetRef`
        points to.
        """
        return self._datasetType

    @property
    def dataId(self) -> DataCoordinate:
        """A mapping of `Dimension` primary key values that labels the Dataset
        within a Collection (`DataCoordinate`).
        """
        return self._dataId

    @property
    def run(self) -> Optional[Run]:
        """The `~lsst.daf.butler.Run` instance that produced (or will produce)
        the Dataset.

        Read-only; update via `~lsst.daf.butler.Registry.addDataset()` or
        `~lsst.daf.butler.Butler.put()`.
        """
        return self._run

    @property
    def components(self) -> Mapping[str, DatasetRef]:
        """Named `DatasetRef` components.

        Read-only; update via `Registry.attachComponent()`.
        """
        return _safeMakeMappingProxyType(self._components)

    @property
    def dimensions(self) -> DimensionGraph:
        """The dimensions associated with the underlying `DatasetType`
        """
        return self.datasetType.dimensions

    def __str__(self) -> str:
        components = ""
        if self.components:
            components = ", components=[" + ", ".join(self.components) + "]"
        return "DatasetRef({}, id={}, dataId={} {})".format(self.datasetType.name,
                                                            self.id, self.dataId, components)

    def resolved(self, id: int, run: Run, components: Optional[Mapping[str, DatasetRef]] = None
                 ) -> DatasetRef:
        """Return a new `DatasetRef` with the same data ID and dataset type
        and the given ID and run.

        Parameters
        ----------
        id : `int`
            The unique integer identifier assigned when the dataset is created.
        run : `Run`
            The run this dataset was associated with when it was created.
        components : `dict`, optional
            A dictionary mapping component name to a `DatasetRef` for that
            component.

        Returns
        -------
        ref : `DatasetRef`
            A new `DatasetRef`.
        """
        newComponents = self._components.copy()
        if components:
            newComponents.update(components)
        return DatasetRef(datasetType=self.datasetType, dataId=self.dataId,
                          id=id, run=run, hash=self._hash, components=newComponents, conform=False)

    def unresolved(self) -> DatasetRef:
        """Return a new `DatasetRef` with the same data ID and dataset type,
        but no ID, run, or components.

        Returns
        -------
        ref : `DatasetRef`
            A new `DatasetRef`.

        Notes
        -----
        This can be used to compare only the data ID and dataset type of a
        pair of `DatasetRef` instances, regardless of whether either is
        resolved::

            if ref1.unresolved() == ref2.unresolved():
                ...
        """
        return DatasetRef(datasetType=self.datasetType, dataId=self.dataId, hash=self._hash, conform=False)

    def isComponent(self) -> bool:
        """Boolean indicating whether this `DatasetRef` refers to a
        component of a composite.

        Returns
        -------
        isComponent : `bool`
            `True` if this `DatasetRef` is a component, `False` otherwise.
        """
        return self.datasetType.isComponent()

    def isComposite(self) -> bool:
        """Boolean indicating whether this `DatasetRef` is a composite type.

        Returns
        -------
        isComposite : `bool`
            `True` if this `DatasetRef` is a composite type, `False`
            otherwise.
        """
        return self.datasetType.isComposite()

    def _lookupNames(self) -> Tuple[LookupKey]:
        """Name keys to use when looking up this DatasetRef in a configuration.

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

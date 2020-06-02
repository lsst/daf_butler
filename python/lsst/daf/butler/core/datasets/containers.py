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

__all__ = ("DatasetContainer",)

from typing import (
    AbstractSet,
    Any,
    Collection,
    Dict,
    Iterable,
    Iterator,
    KeysView,
    Union,
)

from ..dimensions import DataCoordinate
from ..named import NamedKeyDict, NamedKeyMapping
from .type import DatasetType
from .ref import DatasetRef


class GroupedView:

    def __init__(self, parent: DatasetContainer):
        self._dict = parent._dict

    @property
    def names(self) -> AbstractSet[str]:
        return self._dict.names

    def __iter__(self) -> Iterator[DatasetType]:
        return iter(self._dict)

    def __len__(self) -> int:
        return len(self._dict)

    def keys(self) -> KeysView[DatasetType]:
        return self._dict.keys()


class GroupedDatasetRefView(GroupedView, NamedKeyMapping[DatasetType, Collection[DatasetRef]]):

    def byName(self) -> Dict[str, Collection[DatasetRef]]:
        return {datasetType.name: nested.values() for datasetType, nested in self._dict.items()}

    def __getitem__(self, key: Union[str, DatasetType]) -> Collection[DatasetRef]:
        return self._dict[key].values()


class GroupedDataIdView(GroupedView, NamedKeyMapping[DatasetType, AbstractSet[DataCoordinate]]):

    def byName(self) -> Dict[str, AbstractSet[DataCoordinate]]:
        return {datasetType.name: nested.keys() for datasetType, nested in self._dict.items()}

    def __getitem__(self, key: Union[str, DatasetType]) -> AbstractSet[DataCoordinate]:
        return self._dict[key].keys()


class DatasetContainer(Collection[DatasetRef]):

    def __init__(self, refs: Iterable[DatasetRef]):
        self._dict: NamedKeyDict[DatasetType, Dict[DataCoordinate, DatasetRef]]
        if isinstance(refs, DatasetContainer):
            self._dict = NamedKeyDict(
                (datasetType, nested.copy()) for datasetType, nested in refs._dict.items()
            )
        else:
            self._dict = NamedKeyDict()
            for ref in refs:
                nested = self._dict.get(ref.datasetType)
                if nested is None:
                    nested = {ref.dataId: ref}
                    self._dict[ref.datasetType] = nested
                else:
                    nested[ref.dataId] = ref

    def __contains__(self, key: Any) -> bool:
        if isinstance(key, DatasetRef):
            nested = self._dict.get(key.datasetType)
            if nested is None:
                return False
            else:
                return key.dataId in nested
        else:
            return False

    def __iter__(self) -> Iterator[DatasetRef]:
        for nested in self._dict.values():
            yield from nested.values()

    def __len__(self) -> int:
        return sum(len(nested) for nested in self._dict.values())

    @property
    def datasetTypes(self) -> AbstractSet[DatasetType]:
        return self._dict.keys()

    @property
    def groupedRefs(self) -> GroupedDatasetRefView:
        return GroupedDatasetRefView(self)

    @property
    def groupedDataIds(self) -> GroupedDataIdView:
        return GroupedDataIdView(self)

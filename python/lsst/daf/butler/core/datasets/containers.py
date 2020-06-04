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

__all__ = ("DatasetContainer", "MutableDatasetContainer")

from typing import (
    AbstractSet,
    Any,
    Dict,
    Iterable,
    Iterator,
    KeysView,
    Mapping,
    Optional,
    Set,
    Union,
)

from ..dimensions import DataCoordinate
from ..named import NamedKeyDict, NamedKeyMapping
from .type import DatasetType
from .ref import DatasetRef


_InternalDict = NamedKeyDict[DatasetType, Dict[DataCoordinate, Set[DatasetRef]]]


class _DataIdsView(NamedKeyMapping[DatasetType, AbstractSet[DataCoordinate]]):

    def __init__(self, internal: _InternalDict):
        self._dict = internal

    __slots__ = ("_dict",)

    def names(self) -> AbstractSet[str]:
        return self._dict.names

    def byName(self) -> Dict[str, AbstractSet[DataCoordinate]]:
        return {datasetType.name: nested.keys() for datasetType, nested in self._dict.items()}

    def __getitem__(self, key: Union[str, DatasetType]) -> AbstractSet[DataCoordinate]:
        return self._dict[key].keys()

    def keys(self) -> KeysView[DatasetType]:
        return self._dict.keys()

    def __iter__(self) -> Iterator[DatasetType]:
        return iter(self._dict)

    def __len__(self) -> int:
        return len(self._dict)


class _UnresolvedView(AbstractSet[DatasetRef]):

    def __init__(self, internal: _InternalDict):
        self._dict = internal

    __slots__ = ("_dict",)

    @classmethod
    def _from_iterable(cls, refs: Iterable[DatasetRef]) -> Set[DatasetRef]:
        return set(refs)

    def __contains__(self, key: Any) -> bool:
        if not isinstance(key, DatasetRef):
            return False
        elif key.id is not None:
            raise TypeError(
                f"Cannot test resolved DatasetRef {key} against an unresolved DatasetContainer view."
            )
        else:
            nested = self._dict.get(key.datasetType)
            return nested is not None and key.dataId in nested

    def __iter__(self) -> Iterator[DatasetRef]:
        for datasetType, nested in self._dict.items():
            for dataId in nested:
                yield DatasetRef(datasetType, dataId)

    def __len__(self) -> int:
        return sum(len(nested) for nested in self._dict.values())

    def __nonzero__(self) -> bool:
        return bool(self._dict)


class _ResolvedView(AbstractSet[DatasetRef]):

    def __init__(self, internal: _InternalDict):
        self._dict = internal

    __slots__ = ("_dict",)

    @classmethod
    def _from_iterable(cls, refs: Iterable[DatasetRef]) -> Set[DatasetRef]:
        return set(refs)

    def __contains__(self, key: Any) -> bool:
        if not isinstance(key, DatasetRef):
            return False
        elif key.id is None:
            raise TypeError(
                f"Cannot test unresolved DatasetRef {key} against a resolved DatasetContainer view."
            )
        else:
            nested = self._dict.get(key.datasetType)
            if nested is None:
                return False
            else:
                info = nested.get(key)
                if info is None:
                    return False
                else:
                    return key in info.resolved

    def __iter__(self) -> Iterator[DatasetRef]:
        for datasetType, nested in self._dict.items():
            for refs in nested.values():
                yield from refs

    def __len__(self) -> int:
        return sum(
            sum(len(refs) for refs in nested.values())
            for nested in self._dict.values()
        )

    def __nonzero__(self) -> bool:
        return any(any(nested.values()) for nested in self._dict.values())


class _GroupedView(Mapping[DatasetRef, AbstractSet[DatasetRef]]):

    def __init__(self, internal: _InternalDict):
        self._dict = internal

    __slots__ = ("_dict",)

    def __getitem__(self, key: DatasetRef) -> AbstractSet[DatasetRef]:
        return self._dict[key.datasetType][key.dataId]

    def __iter__(self) -> Iterator[DatasetRef]:
        for datasetType, nested in self._dict.items():
            for refs in nested.values():
                yield from refs

    def __len__(self) -> int:
        return sum(len(nested) for nested in self._dict.values())

    def __nonzero__(self) -> bool:
        return bool(self._dict)


class DatasetContainer:

    def __init__(self, refs: Iterable[DatasetRef] = (), *, _internal: Optional[_InternalDict] = None):
        self._dict: _InternalDict
        if _internal:
            self._dict = _internal
        else:
            self._dict = NamedKeyDict()
            self._update(refs)

    __slots__ = ("_dict",)

    @staticmethod
    def _addRefToNested(nested: Dict[DataCoordinate, Set[DatasetRef]], ref: DatasetRef) -> None:
        refs = nested.setdefault(ref.dataId, set())
        if ref.id is not None:
            refs.add(ref)

    def _add(self, ref: DatasetRef) -> None:
        nested = self._dict.get(ref.datasetType)
        if nested is None:
            if ref.id is not None:
                refs = {ref}
            else:
                refs = set()
            nested = {ref.dataId: refs}
            self._dict[ref.datasetType] = nested
        else:
            self._addRefToNested(nested, ref)

    def _update(self, refs: Iterable[DatasetRef], *, datasetType: Optional[DatasetType] = None) -> None:
        if datasetType is not None:
            nested = self._dict.get(datasetType)
            for ref in refs:
                self._addRefToNested(nested, ref)
        else:
            for ref in refs:
                self._add(ref)

    def __nonzero__(self) -> bool:
        return bool(self._dict)

    @property
    def datasetTypes(self) -> AbstractSet[DatasetType]:
        return self._dict.keys()

    @property
    def dataIds(self) -> NamedKeyMapping[DatasetType, AbstractSet[DataCoordinate]]:
        return _DataIdsView(self._dict)

    @property
    def unresolved(self) -> AbstractSet[DatasetRef]:
        return _UnresolvedView(self._dict)

    @property
    def resolved(self) -> AbstractSet[DatasetRef]:
        return _ResolvedView(self._dict)

    @property
    def grouped(self) -> Mapping[DatasetRef, AbstractSet[DatasetRef]]:
        return _GroupedView(self._dict)

    def require(self, resolved: Optional[bool] = None, unique: bool = False) -> None:
        for unresolvedRef, resolvedRefs in self.grouped.items():
            if resolved is True and not resolvedRefs:
                raise ValueError(f"No resolved references for {unresolvedRef} (expected at least 1).")
            if resolved is False and resolvedRefs:
                raise ValueError(
                    f"{len(resolvedRefs)} resolved references for {unresolvedRef} (expected none)."
                )
            if unique and len(resolvedRefs) > 1:
                raise ValueError(
                    f"{len(resolvedRefs)} resolved references for {unresolvedRef} (expected at most 1)."
                )


class MutableDatasetContainer(DatasetContainer):

    def add(self, ref: DatasetRef) -> None:
        self._add(ref)

    def update(self, refs: Iterable[DatasetRef], *, datasetType: Optional[DatasetType] = None) -> None:
        self._update(refs)

    def intoFrozen(self) -> DatasetContainer:
        result = DatasetContainer(_internal=self._dict)
        self._dict = NamedKeyDict()
        return result

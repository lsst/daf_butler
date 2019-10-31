from __future__ import annotations
from typing import (
    Iterable,
    Iterator,
    Optional,
    Sequence,
    Set,
    TYPE_CHECKING,
)
from collections import defaultdict
import dataclasses

import sqlalchemy.sql

if TYPE_CHECKING:
    from .core.dimensions import (
        DataCoordinate,
        DimensionGraph,
    )
    from .core.datasets import DatasetType, ResolvedDatasetHandle
    from .core.utils import NamedValueSet
    from .prototype import RegistryBackend


@dataclasses.dataclass(frozen=True)
class DatasetExpansionFlags:
    composites: bool = False
    collections: bool = False
    dataIds: bool = False


@dataclasses.dataclass(frozen=True)
class DatasetIterableFlags:
    reentrant: bool = False
    expanded: DatasetExpansionFlags


@dataclasses.dataclass(frozen=True)
class DataIdIterableFlags:
    reentrant: bool = False
    expanded: bool = False


class DatasetIterable(Iterable[ResolvedDatasetHandle]):

    def __init__(self, iterable: Iterable[ResolvedDatasetHandle], *,
                 selectable: Optional[sqlalchemy.sql.FromClause] = None,
                 flags: Optional[DatasetIterableFlags] = None, **kwds: bool):
        self._iterable = iterable
        self._selectable = selectable
        if flags is not None:
            flags = DatasetIterableFlags()
        if kwds:
            flags = dataclasses.replace(flags, **kwds)
        self.flags = flags

    def __iter__(self) -> Iterator[ResolvedDatasetHandle]:
        yield from self._iterable

    def one(self) -> ResolvedDatasetHandle:
        handle, = iter(self)
        return handle

    def select(self) -> Optional[sqlalchemy.sql.FromClause]:
        return self._selectable

    def reentrant(self) -> DatasetIterable:
        if self.flags.reentrant:
            return self
        else:
            return DatasetIterable(list(self), flags=self.flags, selectable=self.select(), reentrant=True)

    def expanded(self, registry: RegistryBackend, *, composites: Optional[bool] = None,
                 collections: Optional[bool] = None, dataIds: Optional[bool] = None):
        if dataIds:
            registry.fetchDimensionData()
        # TODO:
        raise NotImplementedError("TODO: add base class implementation.")

    def deduplicated(self, collections: Sequence[str]) -> DatasetIterable:
        raise NotImplementedError("TODO: add base class implementation.")

    def groupByDatasetType(self) -> Iterable[SingleDatasetTypeIterable]:
        groups = defaultdict(list)
        for handle in self:
            groups[handle.datasetType].append(handle)
        for datasetType, handles in groups.items():
            yield SingleDatasetTypeIterable(datasetType, handles, flags=self.flags, reentrant=True)

    def collections(self) -> Set[str]:
        collections = set()
        for handle in self.expanded(collections=True):
            collections.update(handle.collections)
        return collections

    def datasetTypes(self) -> NamedValueSet[DatasetType]:
        datasetTypes = NamedValueSet()
        for handle in self:
            datasetTypes.add(handle.datasetType)
        return datasetTypes

    flags: DatasetIterableFlags


class SingleDatasetTypeIterable(DatasetIterable):

    def __init__(self, datasetType: DatasetType, iterable: Iterable[ResolvedDatasetHandle], *,
                 selectable: Optional[sqlalchemy.sql.FromClause] = None,
                 flags: Optional[DatasetIterableFlags] = None, **kwds: bool):
        super().__init__(iterable, selectable=selectable, flags=flags, **kwds)
        self.datasetType = datasetType

    def reentrant(self) -> SingleDatasetTypeIterable:
        if self.flags.reentrant:
            return self
        else:
            return SingleDatasetTypeIterable(self.datasetType, list(self), flags=self.flags,
                                             selectable=self.select(), reentrant=True)

    def deduplicated(self, collections: Sequence[str]) -> SingleDatasetTypeIterable:
        raise NotImplementedError("TODO: add base class implementation.")

    def groupByDatasetType(self) -> Iterable[SingleDatasetTypeIterable]:
        yield self

    def extractDataIds(self) -> DataIdIterable:
        return DataIdIterable(self.datasetType.dimensions, (handle.dataId for handle in self),
                              selectable=self.selectable, flags=self.flags)

    def datasetTypes(self) -> NamedValueSet[DatasetType]:
        return NamedValueSet([self.datasetType])

    datasetType: DatasetType


class DataIdIterable(Iterable):

    def __init__(self, dimensions: DimensionGraph, iterable: Iterable[DataCoordinate], *,
                 selectable: Optional[sqlalchemy.sql.FromClause] = None,
                 flags: Optional[DatasetIterableFlags] = None, **kwds: bool):
        self.dimensions = dimensions
        self._iterable = iterable
        self._selectable = selectable
        if flags is not None:
            flags = DatasetIterableFlags()
        if kwds:
            flags = dataclasses.replace(flags, **kwds)
        self.flags = flags

    def __iter__(self) -> Iterator[DataCoordinate]:
        yield from self._iterable

    def one(self) -> DataCoordinate:
        handle, = iter(self)
        return handle

    def select(self) -> Optional[sqlalchemy.sql.Selectable]:
        return self._selectable

    def reentrant(self) -> DataIdIterable:
        if self.flags.reentrant:
            return self
        else:
            return DataIdIterable(self.dimensions, list(self), flags=self.flags,
                                  selectable=self.selectable, reentrant=True)

    def expanded(self, registry: RegistryBackend) -> DataIdIterable:
        raise NotImplementedError("TODO: add base class implementation.")

    def subset(self, dimensions: DimensionGraph) -> DataIdIterable:
        return DataIdIterable(dimensions, (dataId.subset(dimensions) for dataId in self),
                              selectable=self.selectable, flags=self.flags)

    flags: DatasetIterableFlags
    dimensions: DimensionGraph

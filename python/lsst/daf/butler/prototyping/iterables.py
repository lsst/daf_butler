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
import itertools

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
    materialized: bool = False
    expanded: DatasetExpansionFlags


@dataclasses.dataclass(frozen=True)
class DataIdIterableFlags:
    materialized: bool = False
    expanded: bool = False


class DatasetIterable(Iterable[ResolvedDatasetHandle]):

    def __init__(self, *, flags: DatasetIterableFlags):
        self.flags = flags

    def one(self) -> ResolvedDatasetHandle:
        handle, = iter(self)
        return handle

    def selectable(self) -> Optional[sqlalchemy.sql.Selectable]:
        return None

    def materialized(self) -> DatasetIterable:
        if self.flags.materialized:
            return self
        else:
            return _DatasetIterableAdapter(list(self), dataclasses.replace(self.flags, materialized=True))

    def expanded(self, registry: RegistryBackend, *, composites: Optional[bool] = None,
                 collections: Optional[bool] = None, dataIds: Optional[bool] = None):
        raise NotImplementedError("TODO: add base class implementation.")

    def deduplicated(self, collections: Sequence[str]) -> DatasetIterable:
        raise NotImplementedError("TODO: add base class implementation.")

    def groupByDatasetType(self) -> Iterable[SingleDatasetTypeIterable]:
        groups = defaultdict(list)
        for handle in self:
            groups[handle.datasetType].append(handle)
        for datasetType, handles in groups.items():
            yield _SingleDatasetTypeIterableAdapter(datasetType, handles)

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

    def __init__(self, datasetType: DatasetType, *, flags: DatasetIterableFlags):
        super().__init__(flags=flags)
        self.datasetType = datasetType

    def materialized(self) -> SingleDatasetTypeIterable:
        if self.flags.materialized:
            return self
        else:
            return _SingleDatasetTypeIterableAdapter(self.datasetType, list(self),
                                                     dataclasses.replace(self.flags, materialized=True))

    def deduplicated(self, collections: Sequence[str]) -> DatasetIterable:
        raise NotImplementedError("TODO: add base class implementation.")

    def groupByDatasetType(self) -> Iterable[SingleDatasetTypeIterable]:
        yield self

    def extractDataIds(self) -> DataIdIterable:
        return _DataIdFromDatasetIterator(self)

    def datasetTypes(self) -> NamedValueSet[DatasetType]:
        return NamedValueSet([self.datasetType])

    datasetType: DatasetType


class DataIdIterable(Iterable):

    def __init__(self, dimensions: DimensionGraph, *, flags: DataIdIterableFlags):
        self.flags = flags
        self.dimensions = dimensions

    def one(self) -> DataCoordinate:
        handle, = iter(self)
        return handle

    def selectable(self) -> Optional[sqlalchemy.sql.Selectable]:
        return None

    def materialized(self) -> DataIdIterable:
        if self.flags.materialized:
            return self
        else:
            return _DataIdIterableAdapter(self.dimensions, list(self),
                                          dataclasses.replace(self.flags, materialized=True))

    def expanded(self, registry: RegistryBackend) -> DataIdIterable:
        raise NotImplementedError("TODO: add base class implementation.")

    def subset(self, dimensions: DimensionGraph) -> DataIdIterable:
        raise NotImplementedError("TODO: provide base class implementation")

    flags: DatasetIterableFlags
    dimensions: DimensionGraph


class _DatasetIterableAdapter(DatasetIterable):

    def __init__(self, handles: Iterable[ResolvedDatasetHandle], *, flags: DatasetIterableFlags):
        super().__init__(flags=flags)
        self._handles = handles

    def __iter__(self) -> Iterator[ResolvedDatasetHandle]:
        yield from self._handles


class _GroupedDatasetIterable(DatasetIterable):

    def __init__(self, groups: Sequence[SingleDatasetTypeIterable], *, flags: DatasetIterableFlags):
        super().__init__(flags=flags)
        self._groups = groups

    def __iter__(self) -> Iterator[ResolvedDatasetHandle]:
        return itertools.chain.from_iterable(self._groups)

    def groupByDatasetType(self) -> Iterable[SingleDatasetTypeIterable]:
        yield from self._groups


class _SingleDatasetTypeIterableAdapter(SingleDatasetTypeIterable):

    def __init__(self, datasetType: DatasetType, handles: Iterable[ResolvedDatasetHandle], *,
                 flags: DatasetIterableFlags):
        super().__init__(datasetType, flags=flags)
        self._handles = handles

    def __iter__(self) -> Iterator[ResolvedDatasetHandle]:
        yield from self._handles


class _DataIdIterableAdapter(DataIdIterable):

    def __init__(self, dimensions: DimensionGraph, dataIds: Iterable[DataCoordinate], *,
                 flags: DataIdIterableFlags):
        super().__init__(dimensions, flags=flags)
        self._dataIds = dataIds

    def __iter__(self) -> Iterator[DataCoordinate]:
        yield from self._dataIds


class _DataIdFromDatasetIterator(DataIdIterable):

    def __init__(self, datasets: SingleDatasetTypeIterable):
        super().__init__(datasets.datasetType.dimensions,
                         flags=DataIdIterableFlags(materialized=datasets.flags.materialized,
                                                   expandedDataIds=datasets.flags.expandedDataIds))
        self._datasets = datasets

    def __iter__(self) -> Iterator[DataCoordinate]:
        for handle in self._datasets:
            yield handle.dataId


class _DataIdSubsetIterator(DataIdIterable):

    def __init__(self, parent: DataIdIterable, dimensions: DimensionGraph, *, flags: DataIdIterableFlags):
        super().__init__(dimensions, flags=flags)
        self._parent = parent

    def __iter__(self) -> Iterator[DataCoordinate]:
        for coordinate in self._parent:
            yield coordinate.subset(self.dimensions)

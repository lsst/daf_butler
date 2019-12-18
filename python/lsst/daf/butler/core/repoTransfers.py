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

__all__ = ["FileDataset", "RepoExport",
           "RepoExportBackend", "RepoImportBackend", "RepoTransferFormatConfig",
           "YamlRepoExportBackend", "YamlRepoImportBackend"]

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterable, Optional, IO, List, Mapping, Tuple, Callable, Union, Type
from collections import defaultdict

import yaml

from lsst.utils import doImport
from .config import ConfigSubset
from .datasets import DatasetType

if TYPE_CHECKING:
    from .dimensions import DimensionElement, DimensionRecord, ExpandedDataCoordinate
    from .datasets import DatasetRef
    from .registry import Registry
    from .datastore import Datastore
    from .formatters import Formatter


class RepoTransferFormatConfig(ConfigSubset):
    """The section of butler configuration that associates repo import/export
    backends with file formats.
    """
    component = "repo_transfer_formats"
    defaultConfigFile = "repo_transfer_formats.yaml"


@dataclass
class FileDataset:
    """A struct that represents a dataset exported to a file.
    """
    __slots__ = ("ref", "path", "formatter")

    ref: DatasetRef
    """Registry information about the dataset (`DatasetRef`).
    """

    path: str
    """Path to the dataset (`str`).

    If the dataset was exported with ``transfer=None`` (i.e. in-place),
    this is relative to the datastore root (only datastores that have a
    well-defined root in the local filesystem can be expected to support
    in-place exports).  Otherwise this is relative to the directory passed
    to `Datastore.export`.
    """

    formatter: Union[None, str, Type[Formatter]]
    """A `Formatter` class or fully-qualified name.
    """

    def __init__(self, path: str, ref: DatasetRef, *, formatter: Union[None, str, Type[Formatter]] = None):
        self.path = path
        self.ref = ref
        self.formatter = formatter


class RepoExport:
    """Public interface for exporting a subset of a data repository.

    Instances of this class are obtained by calling `Butler.export` as the
    value returned by that context manager::

        with butler.export(filename="export.yaml") as export:
            export.saveDataIds(...)
            export.saveDatasts(...)

    Parameters
    ----------
    registry : `Registry`
        Registry to export from.
    datastore : `Datastore`
        Datastore to export from.
    backend : `RepoExportBackend`
        Implementation class for a particular export file format.
    directory : `str`, optional
        Directory to pass to `Datastore.export`.
    transfer : `str`, optional
        Transfer mdoe to pass to `Datastore.export`.
    """

    def __init__(self, registry: Registry, datastore: Datastore, backend: RepoExportBackend, *,
                 directory: Optional[str] = None, transfer: Optional[str] = None):
        self._registry = registry
        self._datastore = datastore
        self._backend = backend
        self._directory = directory
        self._transfer = transfer
        self._dataset_ids = set()

    def saveDataIds(self, dataIds: Iterable[ExpandedDataCoordinate], *,
                    elements: Optional[Iterable[DimensionElement]] = None):
        """Export the dimension records associated with one or more data IDs.

        Parameters
        ----------
        dataIds : iterable of `ExpandedDataCoordinate`.
            Fully-expanded data IDs to export.
        elements : iterable of `DimensionElement`, optional
            Dimension elements whose records should be exported.  If `None`,
            records for all dimensions will be exported.
        """
        if elements is None:
            elements = frozenset(element for element in self._registry.dimensions.elements
                                 if element.hasTable() and element.viewOf is None)
        else:
            elements = frozenset(elements)
        records = defaultdict(dict)
        for dataId in dataIds:
            for record in dataId.records.values():
                if record.definition in elements:
                    records[record.definition].setdefault(record.dataId, record)
        for element in self._registry.dimensions.sorted(records.keys()):
            self._backend.saveDimensionData(element, *records[element].values())

    def saveDatasets(self, refs: Iterable[DatasetRef], *,
                     elements: Optional[Iterable[DimensionElement]] = None,
                     rewrite: Optional[Callable[[FileDataset], FileDataset]] = None):
        """Export one or more datasets.

        This automatically exports any `DatasetType`, `Run`, and dimension
        records associated with the datasets.

        Parameters
        ----------
        refs : iterable of `DatasetRef`
            References to the datasets to export.  Their `DatasetRef.id`
            attributes must not be `None`.  Duplicates are automatically
            ignored.
        elements : iterable of `DimensionElement`, optional
            Dimension elements whose records should be exported; this is
            forwarded to `saveDataIds` when exporting the data IDs of the
            given datasets.
        rewrite : callable, optional
            A callable that takes a single `FileDataset` argument and returns
            a modified `FileDataset`.  This is typically used to rewrite the
            path generated by the datastore.  If `None`, the `FileDataset`
            returned by `Datastore.export` will be used directly.

        Notes
        -----
        At present, this only associates datasets with the collection that
        matches their run name.  Other collections will be included in the
        export in the future (once `Registry` provides a way to look up that
        information).
        """
        dataIds = set()
        datasets: Mapping[Tuple[DatasetType, str], List[FileDataset]] = defaultdict(list)
        for ref in refs:
            # The query interfaces that are often used to generate the refs
            # passed here often don't remove duplicates, so do that here for
            # convenience.
            if ref.id in self._dataset_ids:
                continue
            dataIds.add(ref.dataId)
            # TODO: we need to call getDataset here because most ways of
            # obtaining a DatasetRef (including queryDataset) don't populate
            # the run attribute.  We should address that upstream in the
            # future.
            ref = self._registry.getDataset(ref.id, dataId=ref.dataId, datasetType=ref.datasetType)
            # `exports` is a single-element list here, because we anticipate
            # a future where more than just Datastore.export has a vectorized
            # API and we can pull this out of the loop.
            exports = self._datastore.export([ref], directory=self._directory, transfer=self._transfer)
            if rewrite is not None:
                exports = [rewrite(export) for export in exports]
            datasets[ref.datasetType, ref.run].extend(exports)
            self._dataset_ids.add(ref.id)
        self.saveDataIds(dataIds, elements=elements)
        for (datasetType, run), records in datasets.items():
            self._backend.saveDatasets(datasetType, run, *records)

    def _finish(self):
        """Delegate to the backend to finish the export process.

        For use by `Butler.export` only.
        """
        self._backend.finish()


class RepoExportBackend(ABC):
    """An abstract interface for data repository export implementations.
    """

    @abstractmethod
    def saveDimensionData(self, element: DimensionElement, *data: DimensionRecord):
        """Export one or more dimension element records.

        Parameters
        ----------
        element : `DimensionElement`
            The `DimensionElement` whose elements are being exported.
        data : `DimensionRecord` (variadic)
            One or more records to export.
        """
        raise NotImplementedError()

    @abstractmethod
    def saveDatasets(self, datasetType: DatasetType, run: str, *datasets: FileDataset,
                     collections: Iterable[str] = ()):
        """Export one or more datasets, including their associated DatasetType
        and run information (but not including associated dimension
        information).

        Parameters
        ----------
        datasetType : `DatasetType`
            Type of all datasets being exported with this call.
        run : `str`
            Run associated with all datasets being exported with this call.
        datasets : `FileDataset`, variadic
            Per-dataset information to be exported.  `FileDataset.formatter`
            attributes should be strings, not `Formatter` instances or classes.
        collections : iterable of `str`
            Extra collections (in addition to ``run``) the dataset
            should be associated with.
        """
        raise NotImplementedError()

    @abstractmethod
    def finish(self):
        """Complete the export process.
        """
        raise NotImplementedError()


class RepoImportBackend(ABC):
    """An abstract interface for data repository import implementations.
    """

    @abstractmethod
    def load(self, registry: Registry, datastore: Datastore, *,
             directory: Optional[str] = None, transfer: Optional[str] = None):
        """Import all information associated with the backend into the given
        registry and datastore.

        Import backends are expected to be constructed with a description of
        the objects that need to be imported (from, e.g., a file written by the
        corresponding export backend).

        Parameters
        ----------
        registry : `Registry`
            Registry to import into.
        datastore : `Datastore`
            Datastore to import into.
        directory : `str`, optional
            File all dataset paths are relative to.
        transfer : `str`, optional
            Transfer mode forwarded to `Datastore.ingest`.
        """
        raise NotImplementedError()


class YamlRepoExportBackend(RepoExportBackend):
    """A repository export implementation that saves to a YAML file.

    Parameters
    ----------
    stream
        A writeable file-like object.
    """

    def __init__(self, stream: IO):
        self.stream = stream
        self.data = []

    def saveDimensionData(self, element: DimensionElement, *data: DimensionRecord):
        # Docstring inherited from RepoExportBackend.saveDimensionData.
        self.data.append({
            "type": "dimension",
            "element": element.name,
            "records": [d.toDict() for d in data],  # TODO: encode regions
        })

    def saveDatasets(self, datasetType: DatasetType, run: str, *datasets: FileDataset):
        # Docstring inherited from RepoExportBackend.saveDatasets.
        self.data.append({
            "type": "dataset_type",
            "name": datasetType.name,
            "dimensions": [d.name for d in datasetType.dimensions],
            "storage_class": datasetType.storageClass.name,
        })
        self.data.append({
            "type": "run",
            "name": run,
        })
        self.data.append({
            "type": "dataset",
            "dataset_type": datasetType.name,
            "run": run,
            "records": [
                {
                    "dataset_id": dataset.ref.id,
                    "data_id": dataset.ref.dataId.byName(),
                    "path": dataset.path,
                    "formatter": dataset.formatter,
                    # TODO: look up and save other collections
                }
                for dataset in datasets
            ]
        })

    def finish(self):
        # Docstring inherited from RepoExportBackend.
        yaml.dump(
            {
                "description": "Butler Data Repository Export",
                "version": 0,
                "data": self.data,
            },
            stream=self.stream,
            sort_keys=False,
        )


class YamlRepoImportBackend(RepoImportBackend):
    """A repository import implementation that reads from a YAML file.

    Parameters
    ----------
    stream
        A readable file-like object.
    """

    def __init__(self, stream: IO):
        self.stream = stream

    def load(self, registry: Registry, datastore: Datastore, *,
             directory: Optional[str] = None, transfer: Optional[str] = None):
        # Docstring inherited from RepoImportBackend.load.
        wrapper = yaml.safe_load(self.stream)
        # TODO: When version numbers become meaningful, check here that we can
        # read the version in the file.
        # Mapping from collection name to list of DatasetRefs to associate.
        collections = {}
        # FileDatasets to ingest into the datastore (in bulk):
        datasets = []
        for data in wrapper["data"]:
            if data["type"] == "dimension":
                registry.insertDimensionData(data["element"], *data["records"])
            elif data["type"] == "run":
                registry.registerRun(data["name"])
            elif data["type"] == "dataset_type":
                registry.registerDatasetType(
                    DatasetType(data["name"], dimensions=data["dimensions"],
                                storageClass=data["storage_class"], universe=registry.dimensions)
                )
            elif data["type"] == "dataset":
                datasetType = registry.getDatasetType(data["dataset_type"])
                for dataset in data["records"]:
                    ref = registry.addDataset(datasetType, dataset["data_id"], run=data["run"],
                                              recursive=True)
                    formatter = doImport(dataset["formatter"])
                    if directory is not None:
                        path = os.path.join(directory, dataset["path"])
                    else:
                        path = dataset["path"]
                    datasets.append(FileDataset(path, ref, formatter=formatter))
                    for collection in dataset.get("collections", []):
                        collections[collection].append(ref)
            else:
                raise ValueError(f"Unexpected dictionary type: {data['type']}.")
        datastore.ingest(*datasets, transfer=transfer)
        for collection, refs in collections.items():
            registry.associate(collection, refs)

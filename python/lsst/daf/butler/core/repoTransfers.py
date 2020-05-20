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
from datetime import datetime
from typing import (
    Any,
    Callable,
    Dict,
    IO,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Set,
    Tuple,
    Type,
    TYPE_CHECKING,
    Union,
)
from collections import defaultdict

import yaml
import astropy.time

from lsst.utils import doImport
from .config import ConfigSubset
from .datasets import DatasetType, DatasetRef
from .utils import iterable
from .named import NamedValueSet

if TYPE_CHECKING:
    from .dimensions import DataCoordinate, DimensionElement, DimensionRecord, ExpandedDataCoordinate
    from ..registry import Registry
    from .datastore import Datastore
    from .formatter import FormatterParameter


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
    __slots__ = ("refs", "path", "formatter")

    refs: List[DatasetRef]
    """Registry information about the dataset. (`list` of `DatasetRef`).
    """

    path: str
    """Path to the dataset (`str`).

    If the dataset was exported with ``transfer=None`` (i.e. in-place),
    this is relative to the datastore root (only datastores that have a
    well-defined root in the local filesystem can be expected to support
    in-place exports).  Otherwise this is relative to the directory passed
    to `Datastore.export`.
    """

    formatter: Optional[FormatterParameter]
    """A `Formatter` class or fully-qualified name.
    """

    def __init__(self, path: str, refs: Union[DatasetRef, List[DatasetRef]], *,
                 formatter: Optional[FormatterParameter] = None):
        self.path = path
        if isinstance(refs, DatasetRef):
            refs = [refs]
        self.refs = refs
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
        self._dataset_ids: Set[int] = set()

    def saveDataIds(self, dataIds: Iterable[ExpandedDataCoordinate], *,
                    elements: Optional[Iterable[DimensionElement]] = None) -> None:
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
        records: MutableMapping[DimensionElement, Dict[DataCoordinate, DimensionRecord]] = defaultdict(dict)
        for dataId in dataIds:
            for record in dataId.records.values():
                if record is not None and record.definition in elements:
                    records[record.definition].setdefault(record.dataId, record)
        for element in self._registry.dimensions.sorted(records.keys()):
            self._backend.saveDimensionData(element, *records[element].values())

    def saveDatasets(self, refs: Iterable[DatasetRef], *,
                     elements: Optional[Iterable[DimensionElement]] = None,
                     rewrite: Optional[Callable[[FileDataset], FileDataset]] = None) -> None:
        """Export one or more datasets.

        This automatically exports any `DatasetType`, `Run`, and dimension
        records associated with the datasets.

        Parameters
        ----------
        refs : iterable of `DatasetRef`
            References to the datasets to export.  Their `DatasetRef.id`
            attributes must not be `None`.  Duplicates are automatically
            ignored.  Nested data IDs must be `ExpandedDataCoordinate`
            instances.
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
            dataIds.add(self._registry.expandDataId(ref.dataId))
            # `exports` is a single-element list here, because we anticipate
            # a future where more than just Datastore.export has a vectorized
            # API and we can pull this out of the loop.
            exports = self._datastore.export([ref], directory=self._directory, transfer=self._transfer)
            if rewrite is not None:
                exports = [rewrite(export) for export in exports]
            self._dataset_ids.add(ref.getCheckedId())
            assert ref.run is not None
            datasets[ref.datasetType, ref.run].extend(exports)
        self.saveDataIds(dataIds, elements=elements)
        for (datasetType, run), records in datasets.items():
            self._backend.saveDatasets(datasetType, run, *records)

    def _finish(self) -> None:
        """Delegate to the backend to finish the export process.

        For use by `Butler.export` only.
        """
        self._backend.finish()


class RepoExportBackend(ABC):
    """An abstract interface for data repository export implementations.
    """

    @abstractmethod
    def saveDimensionData(self, element: DimensionElement, *data: DimensionRecord) -> None:
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
    def saveDatasets(self, datasetType: DatasetType, run: str, *datasets: FileDataset) -> None:
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
        """
        raise NotImplementedError()

    @abstractmethod
    def finish(self) -> None:
        """Complete the export process.
        """
        raise NotImplementedError()


class RepoImportBackend(ABC):
    """An abstract interface for data repository import implementations.

    Import backends are expected to be constructed with a description of
    the objects that need to be imported (from, e.g., a file written by the
    corresponding export backend), along with a `Registry`.
    """

    @abstractmethod
    def register(self) -> None:
        """Register all runs and dataset types associated with the backend with
        the `Registry` the backend was constructed with.

        These operations cannot be performed inside transactions, unlike those
        performed by `load`, and must in general be performed before `load`.
        """

    @abstractmethod
    def load(self, datastore: Optional[Datastore], *,
             directory: Optional[str] = None, transfer: Optional[str] = None) -> None:
        """Import information associated with the backend into the given
        registry and datastore.

        This must be run after `register`, and may be performed inside a
        transaction.

        Parameters
        ----------
        datastore : `Datastore`
            Datastore to import into.  If `None`, datasets will only be
            inserted into the `Registry` (primarily intended for tests).
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
        self.data: List[Dict[str, Any]] = []

    def saveDimensionData(self, element: DimensionElement, *data: DimensionRecord) -> None:
        # Docstring inherited from RepoExportBackend.saveDimensionData.
        # Convert astropy time in TAI to datetime in UTC for YAML
        data_dicts = []
        for record in data:
            rec_dict = record.toDict()
            for key in rec_dict:
                if isinstance(rec_dict[key], astropy.time.Time):
                    rec_dict[key] = rec_dict[key].utc.to_datetime()
            data_dicts += [rec_dict]
        self.data.append({
            "type": "dimension",
            "element": element.name,
            "records": data_dicts,  # TODO: encode regions
        })

    def saveDatasets(self, datasetType: DatasetType, run: str, *datasets: FileDataset) -> None:
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
                    "dataset_id": [ref.id for ref in dataset.refs],
                    "data_id": [ref.dataId.byName() for ref in dataset.refs],
                    "path": dataset.path,
                    "formatter": dataset.formatter,
                    # TODO: look up and save other collections
                }
                for dataset in datasets
            ]
        })

    def finish(self) -> None:
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
    registry : `Registry`
        The registry datasets will be imported into.  Only used to retreive
        dataset types during construction; all write happen in `register`
        and `load`.
    """

    def __init__(self, stream: IO, registry: Registry):
        # We read the file fully and convert its contents to Python objects
        # instead of loading incrementally so we can spot some problems early;
        # because `register` can't be put inside a transaction, we'd rather not
        # run that at all if there's going to be problem later in `load`.
        wrapper = yaml.safe_load(stream)
        # TODO: When version numbers become meaningful, check here that we can
        # read the version in the file.
        self.runs: List[str] = []
        self.datasetTypes: NamedValueSet[DatasetType] = NamedValueSet()
        self.dimensions: Mapping[DimensionElement, List[DimensionRecord]] = defaultdict(list)
        self.registry: Registry = registry
        datasetData = []
        for data in wrapper["data"]:
            if data["type"] == "dimension":
                # convert all datetiem values to astropy
                for record in data["records"]:
                    for key in record:
                        if isinstance(record[key], datetime):
                            record[key] = astropy.time.Time(record[key], scale="utc")
                element = self.registry.dimensions[data["element"]]
                RecordClass: Type[DimensionRecord] = element.RecordClass
                self.dimensions[element].extend(
                    RecordClass.fromDict(r) for r in data["records"]
                )
            elif data["type"] == "run":
                self.runs.append(data["name"])
            elif data["type"] == "dataset_type":
                self.datasetTypes.add(
                    DatasetType(data["name"], dimensions=data["dimensions"],
                                storageClass=data["storage_class"], universe=self.registry.dimensions)
                )
            elif data["type"] == "dataset":
                # Save raw dataset data for a second loop, so we can ensure we
                # know about all dataset types first.
                datasetData.append(data)
            else:
                raise ValueError(f"Unexpected dictionary type: {data['type']}.")
        # key is (dataset type name, run); inner most list is collections
        self.datasets: Mapping[Tuple[str, str], List[Tuple[FileDataset, List[str]]]] = defaultdict(list)
        for data in datasetData:
            datasetType = self.datasetTypes.get(data["dataset_type"])
            if datasetType is None:
                datasetType = self.registry.getDatasetType(data["dataset_type"])
            self.datasets[data["dataset_type"], data["run"]].extend(
                (
                    FileDataset(
                        d.get("path"),
                        [DatasetRef(datasetType, dataId, run=data["run"], id=refid)
                         for dataId, refid in zip(iterable(d["data_id"]), iterable(d["dataset_id"]))],
                        formatter=doImport(d.get("formatter")) if "formatter" in d else None
                    ),
                    d.get("collections", [])
                )
                for d in data["records"]
            )

    def register(self) -> None:
        # Docstring inherited from RepoImportBackend.register.
        for run in self.runs:
            self.registry.registerRun(run)
        for datasetType in self.datasetTypes:
            self.registry.registerDatasetType(datasetType)

    def load(self, datastore: Optional[Datastore], *,
             directory: Optional[str] = None, transfer: Optional[str] = None) -> None:
        # Docstring inherited from RepoImportBackend.load.
        for element, dimensionRecords in self.dimensions.items():
            self.registry.insertDimensionData(element, *dimensionRecords)
        # Mapping from collection name to list of DatasetRefs to associate.
        collections = defaultdict(list)
        # FileDatasets to ingest into the datastore (in bulk):
        fileDatasets = []
        for (datasetTypeName, run), records in self.datasets.items():
            datasetType = self.registry.getDatasetType(datasetTypeName)
            # Make a big flattened list of all data IDs, while remembering
            # slices that associate them with the FileDataset instances they
            # came from.
            dataIds: List[DataCoordinate] = []
            slices = []
            for fileDataset, _ in records:
                start = len(dataIds)
                dataIds.extend(ref.dataId for ref in fileDataset.refs)
                stop = len(dataIds)
                slices.append(slice(start, stop))
            # Insert all of those DatasetRefs at once.
            # For now, we ignore the dataset_id we pulled from the file
            # and just insert without one to get a new autoincrement value.
            # Eventually (once we have origin in IDs) we'll preserve them.
            resolvedRefs = self.registry.insertDatasets(
                datasetType,
                dataIds=dataIds,
                run=run,
                recursive=True
            )
            # Now iterate over the original records, and install the new
            # resolved DatasetRefs to replace the unresolved ones as we
            # reorganize the collection information.
            for sliceForFileDataset, (fileDataset, collectionsForDataset) in zip(slices, records):
                fileDataset.refs = resolvedRefs[sliceForFileDataset]
                if directory is not None:
                    fileDataset.path = os.path.join(directory, fileDataset.path)
                fileDatasets.append(fileDataset)
                for collection in collectionsForDataset:
                    collections[collection].extend(fileDataset.refs)
        # Ingest everything into the datastore at once.
        if datastore is not None and fileDatasets:
            datastore.ingest(*fileDatasets, transfer=transfer)
        # Associate with collections, one collection at a time.
        for collection, refs in collections.items():
            self.registry.associate(collection, refs)

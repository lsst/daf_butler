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

__all__ = ["YamlRepoExportBackend", "YamlRepoImportBackend"]

import uuid
import warnings
from collections import defaultdict
from datetime import datetime
from typing import IO, Any, Dict, Iterable, List, Mapping, Optional, Set, Tuple, Type

import astropy.time
import yaml
from lsst.resources import ResourcePath
from lsst.utils import doImportType
from lsst.utils.iteration import ensure_iterable

from ..core import (
    DatasetAssociation,
    DatasetId,
    DatasetRef,
    DatasetType,
    Datastore,
    DimensionElement,
    DimensionRecord,
    DimensionUniverse,
    FileDataset,
    Timespan,
)
from ..core.named import NamedValueSet
from ..registry import CollectionType, Registry
from ..registry.interfaces import (
    ChainedCollectionRecord,
    CollectionRecord,
    DatasetIdGenEnum,
    RunRecord,
    VersionTuple,
)
from ..registry.versions import IncompatibleVersionError
from ._interfaces import RepoExportBackend, RepoImportBackend

EXPORT_FORMAT_VERSION = VersionTuple(1, 0, 2)
"""Export format version.

Files with a different major version or a newer minor version cannot be read by
this version of the code.
"""


def _uuid_representer(dumper: yaml.Dumper, data: uuid.UUID) -> yaml.Node:
    """Generate YAML representation for UUID.

    This produces a scalar node with a tag "!uuid" and value being a regular
    string representation of UUID.
    """
    return dumper.represent_scalar("!uuid", str(data))


def _uuid_constructor(loader: yaml.Loader, node: yaml.Node) -> Optional[uuid.UUID]:
    if node.value is not None:
        return uuid.UUID(hex=node.value)
    return None


yaml.Dumper.add_representer(uuid.UUID, _uuid_representer)
yaml.SafeLoader.add_constructor("!uuid", _uuid_constructor)


class YamlRepoExportBackend(RepoExportBackend):
    """A repository export implementation that saves to a YAML file.

    Parameters
    ----------
    stream
        A writeable file-like object.
    """

    def __init__(self, stream: IO, universe: DimensionUniverse):
        self.stream = stream
        self.universe = universe
        self.data: List[Dict[str, Any]] = []

    def saveDimensionData(self, element: DimensionElement, *data: DimensionRecord) -> None:
        # Docstring inherited from RepoExportBackend.saveDimensionData.
        data_dicts = [record.toDict(splitTimespan=True) for record in data]
        self.data.append(
            {
                "type": "dimension",
                "element": element.name,
                "records": data_dicts,
            }
        )

    def saveCollection(self, record: CollectionRecord, doc: Optional[str]) -> None:
        # Docstring inherited from RepoExportBackend.saveCollections.
        data: Dict[str, Any] = {
            "type": "collection",
            "collection_type": record.type.name,
            "name": record.name,
        }
        if doc is not None:
            data["doc"] = doc
        if isinstance(record, RunRecord):
            data["host"] = record.host
            data["timespan_begin"] = record.timespan.begin
            data["timespan_end"] = record.timespan.end
        elif isinstance(record, ChainedCollectionRecord):
            data["children"] = list(record.children)
        self.data.append(data)

    def saveDatasets(self, datasetType: DatasetType, run: str, *datasets: FileDataset) -> None:
        # Docstring inherited from RepoExportBackend.saveDatasets.
        self.data.append(
            {
                "type": "dataset_type",
                "name": datasetType.name,
                "dimensions": [d.name for d in datasetType.dimensions],
                "storage_class": datasetType.storageClass_name,
                "is_calibration": datasetType.isCalibration(),
            }
        )
        self.data.append(
            {
                "type": "dataset",
                "dataset_type": datasetType.name,
                "run": run,
                "records": [
                    {
                        "dataset_id": [ref.id for ref in sorted(dataset.refs)],
                        "data_id": [ref.dataId.byName() for ref in sorted(dataset.refs)],
                        "path": dataset.path,
                        "formatter": dataset.formatter,
                        # TODO: look up and save other collections
                    }
                    for dataset in datasets
                ],
            }
        )

    def saveDatasetAssociations(
        self, collection: str, collectionType: CollectionType, associations: Iterable[DatasetAssociation]
    ) -> None:
        # Docstring inherited from RepoExportBackend.saveDatasetAssociations.
        if collectionType is CollectionType.TAGGED:
            self.data.append(
                {
                    "type": "associations",
                    "collection": collection,
                    "collection_type": collectionType.name,
                    "dataset_ids": [assoc.ref.id for assoc in associations],
                }
            )
        elif collectionType is CollectionType.CALIBRATION:
            idsByTimespan: Dict[Timespan, List[DatasetId]] = defaultdict(list)
            for association in associations:
                assert association.timespan is not None
                assert association.ref.id is not None
                idsByTimespan[association.timespan].append(association.ref.id)
            self.data.append(
                {
                    "type": "associations",
                    "collection": collection,
                    "collection_type": collectionType.name,
                    "validity_ranges": [
                        {
                            "timespan": timespan,
                            "dataset_ids": dataset_ids,
                        }
                        for timespan, dataset_ids in idsByTimespan.items()
                    ],
                }
            )

    def finish(self) -> None:
        # Docstring inherited from RepoExportBackend.
        yaml.dump(
            {
                "description": "Butler Data Repository Export",
                "version": str(EXPORT_FORMAT_VERSION),
                "universe_version": self.universe.version,
                "universe_namespace": self.universe.namespace,
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
        if wrapper["version"] == 0:
            # Grandfather-in 'version: 0' -> 1.0.0, which is what we wrote
            # before we really tried to do versioning here.
            fileVersion = VersionTuple(1, 0, 0)
        else:
            fileVersion = VersionTuple.fromString(wrapper["version"])
            if fileVersion.major != EXPORT_FORMAT_VERSION.major:
                raise IncompatibleVersionError(
                    f"Cannot read repository export file with version={fileVersion} "
                    f"({EXPORT_FORMAT_VERSION.major}.x.x required)."
                )
            if fileVersion.minor > EXPORT_FORMAT_VERSION.minor:
                raise IncompatibleVersionError(
                    f"Cannot read repository export file with version={fileVersion} "
                    f"< {EXPORT_FORMAT_VERSION.major}.{EXPORT_FORMAT_VERSION.minor}.x required."
                )
        self.runs: Dict[str, Tuple[Optional[str], Timespan]] = {}
        self.chains: Dict[str, List[str]] = {}
        self.collections: Dict[str, CollectionType] = {}
        self.collectionDocs: Dict[str, str] = {}
        self.datasetTypes: NamedValueSet[DatasetType] = NamedValueSet()
        self.dimensions: Mapping[DimensionElement, List[DimensionRecord]] = defaultdict(list)
        self.tagAssociations: Dict[str, List[DatasetId]] = defaultdict(list)
        self.calibAssociations: Dict[str, Dict[Timespan, List[DatasetId]]] = defaultdict(dict)
        self.refsByFileId: Dict[DatasetId, DatasetRef] = {}
        self.registry: Registry = registry

        universe_version = wrapper.get("universe_version", 0)
        universe_namespace = wrapper.get("universe_namespace", "daf_butler")

        # If this is data exported before the reorganization of visits
        # and visit systems and that new schema is in use, some filtering
        # will be needed. The entry in the visit dimension record will be
        # silently dropped when visit is created but the
        # visit_system_membership must be constructed.
        migrate_visit_system = False
        if (
            universe_version < 2
            and universe_namespace == "daf_butler"
            and "visit_system_membership" in self.registry.dimensions
        ):
            migrate_visit_system = True

        datasetData = []
        for data in wrapper["data"]:
            if data["type"] == "dimension":
                # convert all datetime values to astropy
                for record in data["records"]:
                    for key in record:
                        # Some older YAML files were produced with native
                        # YAML support for datetime, we support reading that
                        # data back. Newer conversion uses _AstropyTimeToYAML
                        # class with special YAML tag.
                        if isinstance(record[key], datetime):
                            record[key] = astropy.time.Time(record[key], scale="utc")
                element = self.registry.dimensions[data["element"]]
                RecordClass: Type[DimensionRecord] = element.RecordClass
                self.dimensions[element].extend(RecordClass(**r) for r in data["records"])

                if data["element"] == "visit" and migrate_visit_system:
                    # Must create the visit_system_membership records.
                    element = self.registry.dimensions["visit_system_membership"]
                    RecordClass = element.RecordClass
                    self.dimensions[element].extend(
                        RecordClass(instrument=r["instrument"], visit_system=r["visit_system"], visit=r["id"])
                        for r in data["records"]
                    )

            elif data["type"] == "collection":
                collectionType = CollectionType.from_name(data["collection_type"])
                if collectionType is CollectionType.RUN:
                    self.runs[data["name"]] = (
                        data["host"],
                        Timespan(begin=data["timespan_begin"], end=data["timespan_end"]),
                    )
                elif collectionType is CollectionType.CHAINED:
                    children = []
                    for child in data["children"]:
                        if not isinstance(child, str):
                            warnings.warn(
                                f"CHAINED collection {data['name']} includes restrictions on child "
                                "collection searches, which are no longer suppored and will be ignored."
                            )
                            # Old form with dataset type restrictions only,
                            # supported for backwards compatibility.
                            child, _ = child
                        children.append(child)
                    self.chains[data["name"]] = children
                else:
                    self.collections[data["name"]] = collectionType
                doc = data.get("doc")
                if doc is not None:
                    self.collectionDocs[data["name"]] = doc
            elif data["type"] == "run":
                # Also support old form of saving a run with no extra info.
                self.runs[data["name"]] = (None, Timespan(None, None))
            elif data["type"] == "dataset_type":
                dimensions = data["dimensions"]
                if migrate_visit_system and "visit" in dimensions and "visit_system" in dimensions:
                    dimensions.remove("visit_system")
                self.datasetTypes.add(
                    DatasetType(
                        data["name"],
                        dimensions=dimensions,
                        storageClass=data["storage_class"],
                        universe=self.registry.dimensions,
                        isCalibration=data.get("is_calibration", False),
                    )
                )
            elif data["type"] == "dataset":
                # Save raw dataset data for a second loop, so we can ensure we
                # know about all dataset types first.
                datasetData.append(data)
            elif data["type"] == "associations":
                collectionType = CollectionType.from_name(data["collection_type"])
                if collectionType is CollectionType.TAGGED:
                    self.tagAssociations[data["collection"]].extend(data["dataset_ids"])
                elif collectionType is CollectionType.CALIBRATION:
                    assocsByTimespan = self.calibAssociations[data["collection"]]
                    for d in data["validity_ranges"]:
                        if "timespan" in d:
                            assocsByTimespan[d["timespan"]] = d["dataset_ids"]
                        else:
                            # TODO: this is for backward compatibility, should
                            # be removed at some point.
                            assocsByTimespan[Timespan(begin=d["begin"], end=d["end"])] = d["dataset_ids"]
                else:
                    raise ValueError(f"Unexpected calibration type for association: {collectionType.name}.")
            else:
                raise ValueError(f"Unexpected dictionary type: {data['type']}.")
        # key is (dataset type name, run)
        self.datasets: Mapping[Tuple[str, str], List[FileDataset]] = defaultdict(list)
        for data in datasetData:
            datasetType = self.datasetTypes.get(data["dataset_type"])
            if datasetType is None:
                datasetType = self.registry.getDatasetType(data["dataset_type"])
            self.datasets[data["dataset_type"], data["run"]].extend(
                FileDataset(
                    d.get("path"),
                    [
                        DatasetRef(datasetType, dataId, run=data["run"], id=refid)
                        for dataId, refid in zip(
                            ensure_iterable(d["data_id"]), ensure_iterable(d["dataset_id"])
                        )
                    ],
                    formatter=doImportType(d.get("formatter")) if "formatter" in d else None,
                )
                for d in data["records"]
            )

    def register(self) -> None:
        # Docstring inherited from RepoImportBackend.register.
        for datasetType in self.datasetTypes:
            self.registry.registerDatasetType(datasetType)
        for run in self.runs:
            self.registry.registerRun(run, doc=self.collectionDocs.get(run))
            # No way to add extra run info to registry yet.
        for collection, collection_type in self.collections.items():
            self.registry.registerCollection(
                collection, collection_type, doc=self.collectionDocs.get(collection)
            )
        for chain, children in self.chains.items():
            self.registry.registerCollection(
                chain, CollectionType.CHAINED, doc=self.collectionDocs.get(chain)
            )
            self.registry.setCollectionChain(chain, children)

    def load(
        self,
        datastore: Optional[Datastore],
        *,
        directory: Optional[str] = None,
        transfer: Optional[str] = None,
        skip_dimensions: Optional[Set] = None,
        idGenerationMode: DatasetIdGenEnum = DatasetIdGenEnum.UNIQUE,
        reuseIds: bool = False,
    ) -> None:
        # Docstring inherited from RepoImportBackend.load.
        for element, dimensionRecords in self.dimensions.items():
            if skip_dimensions and element in skip_dimensions:
                continue
            # Using skip_existing=True here assumes that the records in the
            # database are either equivalent or at least preferable to the ones
            # being imported.  It'd be ideal to check that, but that would mean
            # using syncDimensionData, which is not vectorized and is hence
            # unacceptably slo.
            self.registry.insertDimensionData(element, *dimensionRecords, skip_existing=True)
        # FileDatasets to ingest into the datastore (in bulk):
        fileDatasets = []
        for (datasetTypeName, run), records in self.datasets.items():
            # Make a big flattened list of all data IDs and dataset_ids, while
            # remembering slices that associate them with the FileDataset
            # instances they came from.
            datasets: List[DatasetRef] = []
            dataset_ids: List[DatasetId] = []
            slices = []
            for fileDataset in records:
                start = len(datasets)
                datasets.extend(fileDataset.refs)
                dataset_ids.extend(ref.id for ref in fileDataset.refs)  # type: ignore
                stop = len(datasets)
                slices.append(slice(start, stop))
            # Insert all of those DatasetRefs at once.
            # For now, we ignore the dataset_id we pulled from the file
            # and just insert without one to get a new autoincrement value.
            # Eventually (once we have origin in IDs) we'll preserve them.
            resolvedRefs = self.registry._importDatasets(
                datasets, idGenerationMode=idGenerationMode, reuseIds=reuseIds
            )
            # Populate our dictionary that maps int dataset_id values from the
            # export file to the new DatasetRefs
            for fileId, ref in zip(dataset_ids, resolvedRefs):
                self.refsByFileId[fileId] = ref
            # Now iterate over the original records, and install the new
            # resolved DatasetRefs to replace the unresolved ones as we
            # reorganize the collection information.
            for sliceForFileDataset, fileDataset in zip(slices, records):
                fileDataset.refs = resolvedRefs[sliceForFileDataset]
                if directory is not None:
                    fileDataset.path = ResourcePath(directory, forceDirectory=True).join(fileDataset.path)
                fileDatasets.append(fileDataset)
        # Ingest everything into the datastore at once.
        if datastore is not None and fileDatasets:
            datastore.ingest(*fileDatasets, transfer=transfer)
        # Associate datasets with tagged collections.
        for collection, dataset_ids in self.tagAssociations.items():
            self.registry.associate(collection, [self.refsByFileId[i] for i in dataset_ids])
        # Associate datasets with calibration collections.
        for collection, idsByTimespan in self.calibAssociations.items():
            for timespan, dataset_ids in idsByTimespan.items():
                self.registry.certify(collection, [self.refsByFileId[i] for i in dataset_ids], timespan)

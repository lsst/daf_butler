# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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

import logging
import uuid
import warnings
from collections import UserDict, defaultdict
from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import IO, TYPE_CHECKING, Any

import astropy.time
import yaml
from lsst.resources import ResourcePath
from lsst.utils import doImportType
from lsst.utils.introspection import find_outside_stacklevel
from lsst.utils.iteration import ensure_iterable

from .._collection_type import CollectionType
from .._dataset_association import DatasetAssociation
from .._dataset_ref import DatasetId, DatasetRef
from .._dataset_type import DatasetType
from .._file_dataset import FileDataset
from .._named import NamedValueSet
from .._timespan import Timespan
from ..datastore import Datastore
from ..dimensions import DimensionElement, DimensionRecord, DimensionUniverse
from ..registry.interfaces import ChainedCollectionRecord, CollectionRecord, RunRecord, VersionTuple
from ..registry.versions import IncompatibleVersionError
from ._interfaces import RepoExportBackend, RepoImportBackend

if TYPE_CHECKING:
    from lsst.daf.butler import Butler
    from lsst.resources import ResourcePathExpression

_LOG = logging.getLogger(__name__)

EXPORT_FORMAT_VERSION = VersionTuple(1, 0, 2)
"""Export format version.

Files with a different major version or a newer minor version cannot be read by
this version of the code.
"""


class _RefMapper(UserDict[int, uuid.UUID]):
    """Create a local dict subclass which creates new deterministic UUID for
    missing keys.
    """

    _namespace = uuid.UUID("4d4851f4-2890-4d41-8779-5f38a3f5062b")

    def __missing__(self, key: int) -> uuid.UUID:
        newUUID = uuid.uuid3(namespace=self._namespace, name=str(key))
        self[key] = newUUID
        return newUUID


_refIntId2UUID = _RefMapper()


def _uuid_representer(dumper: yaml.Dumper, data: uuid.UUID) -> yaml.Node:
    """Generate YAML representation for UUID.

    This produces a scalar node with a tag "!uuid" and value being a regular
    string representation of UUID.
    """
    return dumper.represent_scalar("!uuid", str(data))


def _uuid_constructor(loader: yaml.Loader, node: yaml.Node) -> uuid.UUID | None:
    if node.value is not None:
        return uuid.UUID(hex=node.value)
    return None


yaml.Dumper.add_representer(uuid.UUID, _uuid_representer)
yaml.SafeLoader.add_constructor("!uuid", _uuid_constructor)


class YamlRepoExportBackend(RepoExportBackend):
    """A repository export implementation that saves to a YAML file.

    Parameters
    ----------
    stream : `io.IO`
        A writeable file-like object.
    universe : `DimensionUniverse`
        The dimension universe to use for the export.
    """

    def __init__(self, stream: IO, universe: DimensionUniverse):
        self.stream = stream
        self.universe = universe
        self.data: list[dict[str, Any]] = []

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

    def saveCollection(self, record: CollectionRecord, doc: str | None) -> None:
        # Docstring inherited from RepoExportBackend.saveCollections.
        data: dict[str, Any] = {
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
                "dimensions": list(datasetType.dimensions.names),
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
                        "data_id": [dict(ref.dataId.required) for ref in sorted(dataset.refs)],
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
            idsByTimespan: dict[Timespan, list[DatasetId]] = defaultdict(list)
            for association in associations:
                assert association.timespan is not None
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


class _DayObsOffsetCalculator:
    """Interface to allow the day_obs offset to be calculated from an
    instrument class name and cached.
    """

    name_to_class_name: dict[str, str]
    name_to_offset: dict[str, int | None]

    def __init__(self) -> None:
        self.name_to_class_name = {}
        self.name_to_offset = {}

    def __setitem__(self, name: str, class_name: str) -> None:
        """Store the instrument class name.

        Parameters
        ----------
        name : `str`
            Name of the instrument.
        class_name : `str`
            Full name of the instrument class.
        """
        self.name_to_class_name[name] = class_name

    def get_offset(self, name: str, date: astropy.time.Time) -> int | None:
        """Return the offset to use when calculating day_obs.

        Parameters
        ----------
        name : `str`
            The instrument name.
        date : `astropy.time.Time`
            Time for which the offset is required.

        Returns
        -------
        offset : `int`
            The offset in seconds.
        """
        if name in self.name_to_offset:
            return self.name_to_offset[name]

        try:
            instrument_class = doImportType(self.name_to_class_name[name])
        except Exception:
            # Any error at all, store None and do not try again.
            self.name_to_offset[name] = None
            return None

        # Assume this is a `lsst.pipe.base.Instrument` and that it has
        # a translatorClass property pointing to an
        # astro_metadata_translator.MetadataTranslator class. If this is not
        # true give up and store None.
        try:
            offset_delta = instrument_class.translatorClass.observing_date_to_offset(date)  # type: ignore
        except Exception:
            offset_delta = None

        if offset_delta is None:
            self.name_to_offset[name] = None
            return None

        self.name_to_offset[name] = round(offset_delta.to_value("s"))
        return self.name_to_offset[name]


class YamlRepoImportBackend(RepoImportBackend):
    """A repository import implementation that reads from a YAML file.

    Parameters
    ----------
    stream : `io.IO`
        A readable file-like object.
    butler : `Butler`
        The butler datasets will be imported into.  Only used to retrieve
        dataset types during construction; all writes happen in `register`
        and `load`.
    """

    def __init__(self, stream: IO, butler: Butler):
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
        self.runs: dict[str, tuple[str | None, Timespan]] = {}
        self.chains: dict[str, list[str]] = {}
        self.collections: dict[str, CollectionType] = {}
        self.collectionDocs: dict[str, str] = {}
        self.datasetTypes: NamedValueSet[DatasetType] = NamedValueSet()
        self.dimensions: Mapping[DimensionElement, list[DimensionRecord]] = defaultdict(list)
        self.tagAssociations: dict[str, list[DatasetId]] = defaultdict(list)
        self.calibAssociations: dict[str, dict[Timespan, list[DatasetId]]] = defaultdict(dict)
        self.refsByFileId: dict[DatasetId, DatasetRef] = {}
        self.butler: Butler = butler

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
            and "visit_system_membership" in self.butler.dimensions
        ):
            migrate_visit_system = True

        # Drop "seeing" from visits in files older than version 1.
        migrate_visit_seeing = False
        if (
            universe_version < 1
            and universe_namespace == "daf_butler"
            and "visit" in self.butler.dimensions
            and "seeing" not in self.butler.dimensions["visit"].metadata
        ):
            migrate_visit_seeing = True

        # If this data exported before group was a first-class dimension,
        # we'll need to modify some exposure columns and add group records.
        migrate_group = False
        if (
            universe_version < 6
            and universe_namespace == "daf_butler"
            and "exposure" in self.butler.dimensions
            and "group" in self.butler.dimensions["exposure"].implied
        ):
            migrate_group = True

        # If this data exported before day_obs was a first-class dimension,
        # we'll need to modify some exposure and visit columns and add day_obs
        # records.  This is especially tricky because some files even predate
        # the existence of data ID values.
        migrate_exposure_day_obs = False
        migrate_visit_day_obs = False
        day_obs_ids: set[tuple[str, int]] = set()
        if universe_version < 6 and universe_namespace == "daf_butler":
            if (
                "exposure" in self.butler.dimensions
                and "day_obs" in self.butler.dimensions["exposure"].implied
            ):
                migrate_exposure_day_obs = True
            if "visit" in self.butler.dimensions and "day_obs" in self.butler.dimensions["visit"].implied:
                migrate_visit_day_obs = True

        # If this is pre-v1 universe we may need to fill in a missing
        # visit.day_obs field.
        migrate_add_visit_day_obs = False
        if (
            universe_version < 1
            and universe_namespace == "daf_butler"
            and (
                "day_obs" in self.butler.dimensions["visit"].implied
                or "day_obs" in self.butler.dimensions["visit"].metadata
            )
        ):
            migrate_add_visit_day_obs = True

        # Some conversions may need to work out a day_obs timespan.
        # The only way this offset can be found is by querying the instrument
        # class. Read all the existing instrument classes indexed by name.
        instrument_classes: dict[str, int] = {}
        if migrate_exposure_day_obs or migrate_visit_day_obs or migrate_add_visit_day_obs:
            day_obs_offset_calculator = _DayObsOffsetCalculator()
            for rec in self.butler.registry.queryDimensionRecords("instrument"):
                day_obs_offset_calculator[rec.name] = rec.class_name

        datasetData = []
        RecordClass: type[DimensionRecord]
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

                if data["element"] == "instrument":
                    if migrate_exposure_day_obs or migrate_visit_day_obs:
                        # Might want the instrument class name for later.
                        for record in data["records"]:
                            if record["name"] not in instrument_classes:
                                instrument_classes[record["name"]] = record["class_name"]

                if data["element"] == "visit":
                    if migrate_visit_system:
                        # Must create the visit_system_membership records.
                        # But first create empty list for visits since other
                        # logic in this file depends on self.dimensions being
                        # populated in an order consistent with primary keys.
                        self.dimensions[self.butler.dimensions["visit"]] = []
                        element = self.butler.dimensions["visit_system_membership"]
                        RecordClass = element.RecordClass
                        self.dimensions[element].extend(
                            RecordClass(
                                instrument=r["instrument"], visit_system=r.pop("visit_system"), visit=r["id"]
                            )
                            for r in data["records"]
                        )
                    if migrate_visit_seeing:
                        for record in data["records"]:
                            record.pop("seeing", None)
                    if migrate_add_visit_day_obs:
                        # The day_obs field is missing. It can be derived from
                        # the datetime_begin field.
                        for record in data["records"]:
                            date = record["datetime_begin"].tai
                            offset = day_obs_offset_calculator.get_offset(record["instrument"], date)
                            # This field is required so we have to calculate
                            # it even if the offset is not defined.
                            if offset:
                                date = date - astropy.time.TimeDelta(offset, format="sec", scale="tai")
                            record["day_obs"] = int(date.strftime("%Y%m%d"))
                    if migrate_visit_day_obs:
                        # Poke the entry for this dimension to make sure it
                        # appears in the right order, even though we'll
                        # populate it later.
                        self.dimensions[self.butler.dimensions["day_obs"]]
                        for record in data["records"]:
                            day_obs_ids.add((record["instrument"], record["day_obs"]))

                if data["element"] == "exposure":
                    if migrate_group:
                        element = self.butler.dimensions["group"]
                        RecordClass = element.RecordClass
                        group_records = self.dimensions[element]
                        for exposure_record in data["records"]:
                            exposure_record["group"] = exposure_record.pop("group_name")
                            del exposure_record["group_id"]
                            group_records.append(
                                RecordClass(
                                    instrument=exposure_record["instrument"], name=exposure_record["group"]
                                )
                            )
                    if migrate_exposure_day_obs:
                        # Poke the entry for this dimension to make sure it
                        # appears in the right order, even though we'll
                        # populate it later.
                        for record in data["records"]:
                            day_obs_ids.add((record["instrument"], record["day_obs"]))

                element = self.butler.dimensions[data["element"]]
                RecordClass = element.RecordClass
                self.dimensions[element].extend(RecordClass(**r) for r in data["records"])

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
                                "collection searches, which are no longer supported and will be ignored.",
                                stacklevel=find_outside_stacklevel("lsst.daf.butler"),
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
                        universe=self.butler.dimensions,
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
                    self.tagAssociations[data["collection"]].extend(
                        [x if not isinstance(x, int) else _refIntId2UUID[x] for x in data["dataset_ids"]]
                    )
                elif collectionType is CollectionType.CALIBRATION:
                    assocsByTimespan = self.calibAssociations[data["collection"]]
                    for d in data["validity_ranges"]:
                        if "timespan" in d:
                            assocsByTimespan[d["timespan"]] = [
                                x if not isinstance(x, int) else _refIntId2UUID[x] for x in d["dataset_ids"]
                            ]
                        else:
                            # TODO: this is for backward compatibility, should
                            # be removed at some point.
                            assocsByTimespan[Timespan(begin=d["begin"], end=d["end"])] = [
                                x if not isinstance(x, int) else _refIntId2UUID[x] for x in d["dataset_ids"]
                            ]
                else:
                    raise ValueError(f"Unexpected calibration type for association: {collectionType.name}.")
            else:
                raise ValueError(f"Unexpected dictionary type: {data['type']}.")

        if day_obs_ids:
            element = self.butler.dimensions["day_obs"]
            RecordClass = element.RecordClass
            missing_offsets = set()
            for instrument, day_obs in day_obs_ids:
                # To get the offset we need the astropy time. Since we are
                # going from a day_obs to a time, it's possible that in some
                # scenario the offset will be wrong.
                ymd = str(day_obs)
                t = astropy.time.Time(
                    f"{ymd[0:4]}-{ymd[4:6]}-{ymd[6:8]}T00:00:00", format="isot", scale="tai"
                )
                offset = day_obs_offset_calculator.get_offset(instrument, t)

                # This should always return an offset but as a fallback
                # allow None here in case something has gone wrong above.
                # In particular, not being able to load an instrument class.
                if offset is not None:
                    timespan = Timespan.from_day_obs(day_obs, offset=offset)
                else:
                    timespan = None
                    missing_offsets.add(instrument)
                self.dimensions[element].append(
                    RecordClass(instrument=instrument, id=day_obs, timespan=timespan)
                )

            if missing_offsets:
                plural = "" if len(missing_offsets) == 1 else "s"
                warnings.warn(
                    "Constructing day_obs records with no timespans for "
                    "visit/exposure records that were exported before day_obs was a dimension. "
                    f"(instrument{plural}: {missing_offsets})",
                    stacklevel=find_outside_stacklevel("lsst.daf.butler"),
                )

        # key is (dataset type name, run)
        self.datasets: Mapping[tuple[str, str], list[FileDataset]] = defaultdict(list)
        for data in datasetData:
            datasetType = self.datasetTypes.get(data["dataset_type"])
            if datasetType is None:
                datasetType = self.butler.get_dataset_type(data["dataset_type"])
            self.datasets[data["dataset_type"], data["run"]].extend(
                FileDataset(
                    d.get("path"),
                    [
                        DatasetRef(
                            datasetType,
                            dataId,
                            run=data["run"],
                            id=refid if not isinstance(refid, int) else _refIntId2UUID[refid],
                        )
                        for dataId, refid in zip(
                            ensure_iterable(d["data_id"]), ensure_iterable(d["dataset_id"]), strict=True
                        )
                    ],
                    formatter=doImportType(d.get("formatter")) if "formatter" in d else None,
                )
                for d in data["records"]
            )

    def register(self) -> None:
        # Docstring inherited from RepoImportBackend.register.
        for datasetType in self.datasetTypes:
            self.butler.registry.registerDatasetType(datasetType)
        for run in self.runs:
            self.butler.collections.register(run, doc=self.collectionDocs.get(run))
            # No way to add extra run info to registry yet.
        for collection, collection_type in self.collections.items():
            self.butler.collections.register(
                collection, collection_type, doc=self.collectionDocs.get(collection)
            )
        for chain, children in self.chains.items():
            self.butler.collections.register(
                chain, CollectionType.CHAINED, doc=self.collectionDocs.get(chain)
            )
            self.butler.registry.setCollectionChain(chain, children)

    def load(
        self,
        datastore: Datastore | None,
        *,
        directory: ResourcePathExpression | None = None,
        transfer: str | None = None,
        skip_dimensions: set | None = None,
        record_validation_info: bool = True,
    ) -> None:
        # Docstring inherited from RepoImportBackend.load.
        # Must ensure we insert in order supported by the universe.
        for element in self.butler.dimensions.sorted(self.dimensions.keys()):
            dimensionRecords = self.dimensions[element]
            if skip_dimensions and element in skip_dimensions:
                continue
            # Using skip_existing=True here assumes that the records in the
            # database are either equivalent or at least preferable to the ones
            # being imported.  It'd be ideal to check that, but that would mean
            # using syncDimensionData, which is not vectorized and is hence
            # unacceptably slo.
            self.butler.registry.insertDimensionData(element, *dimensionRecords, skip_existing=True)
        # FileDatasets to ingest into the datastore (in bulk):
        fileDatasets = []
        for records in self.datasets.values():
            # Make a big flattened list of all data IDs and dataset_ids, while
            # remembering slices that associate them with the FileDataset
            # instances they came from.
            datasets: list[DatasetRef] = []
            dataset_ids: list[DatasetId] = []
            slices = []
            for fileDataset in records:
                start = len(datasets)
                datasets.extend(fileDataset.refs)
                dataset_ids.extend(ref.id for ref in fileDataset.refs)
                stop = len(datasets)
                slices.append(slice(start, stop))
            # Insert all of those DatasetRefs at once.
            # For now, we ignore the dataset_id we pulled from the file
            # and just insert without one to get a new autoincrement value.
            # Eventually (once we have origin in IDs) we'll preserve them.
            resolvedRefs = self.butler.registry._importDatasets(datasets)
            # Populate our dictionary that maps int dataset_id values from the
            # export file to the new DatasetRefs
            for fileId, ref in zip(dataset_ids, resolvedRefs, strict=True):
                self.refsByFileId[fileId] = ref
            # Now iterate over the original records, and install the new
            # resolved DatasetRefs to replace the unresolved ones as we
            # reorganize the collection information.
            for sliceForFileDataset, fileDataset in zip(slices, records, strict=True):
                fileDataset.refs = resolvedRefs[sliceForFileDataset]
                if directory is not None:
                    fileDataset.path = ResourcePath(directory, forceDirectory=True).join(fileDataset.path)
                fileDatasets.append(fileDataset)
        # Ingest everything into the datastore at once.
        if datastore is not None and fileDatasets:
            datastore.ingest(*fileDatasets, transfer=transfer, record_validation_info=record_validation_info)
        # Associate datasets with tagged collections.
        for collection, dataset_ids in self.tagAssociations.items():
            self.butler.registry.associate(collection, [self.refsByFileId[i] for i in dataset_ids])
        # Associate datasets with calibration collections.
        for collection, idsByTimespan in self.calibAssociations.items():
            for timespan, dataset_ids in idsByTimespan.items():
                self.butler.registry.certify(
                    collection, [self.refsByFileId[i] for i in dataset_ids], timespan
                )

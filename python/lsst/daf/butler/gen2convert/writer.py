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

__all__ = ("ConversionWriter,")

import os
import re
from datetime import timedelta
from collections import OrderedDict

from lsst.afw.image import bboxFromMetadata
from lsst.geom import Box2D
from lsst.sphgeom import ConvexPolygon
from lsst.log import Log

from ..core import Config, Run, DatasetType, StorageClassFactory, DataId, DimensionUniverse
from ..instrument import (Instrument, makeExposureRecordFromObsInfo, makeVisitRecordFromObsInfo,
                          addUnboundedCalibrationLabel)
from .structures import ConvertedRepo
from .translators import Translator, makeCalibrationLabel


class ConversionWriter:
    """A class that creates a Gen3 snapshot view into one or more Gen2
    repositories already scanned by a ConversionWalker.

    ConversionWriter is designed to avoid actually having to inspect the
    filesystem where a Gen2 Data Repository exists or instantiate its mapper
    (all of that is done by ConversionWalker).  It does handle any state that
    involves user choices about how to perform the conversion.

    Parameters
    ----------
    config : Config
        Configuration used by both ConversionWalkers and ConversionWriters.
        Defaults are maintained in daf_base/config/gen2convert.yaml.
    gen2repos : dict
        A dictionary with absolute paths as keys and `Gen2Repo` objects as
        values, usually obtained from the `scanned` attribute of a
        `ConversionWalker`.
    skyMaps : dict
        A dictionary with hashes as keys and `BaseSkyMap` instances as
        values, usually obtained from the `skyMaps` attribute of a
        `ConversionWalker`.
    skyMapRoots : dict
        A dictionary with hashes as keys and lists of repository roots as
        values, usually obtained from the `skyMapRoots` attribute of a
        `ConversionWalker`.
    obsInfo: dict
        A nested dictionary of `astro_metadata_translator.ObservationInfo`
        objects, with MapperClass names as outer keys and tuples of
        instrument-dependent Gen2 visit/exposure dentifiers as inner keys.
        Usually obtained from `ConversionWalker.obsInfo`.
    """

    @classmethod
    def fromWalker(cls, walker):
        """Construct a ConversionWriter from a ConversionWalker."""
        return cls(config=walker.config, gen2repos=walker.scanned,
                   skyMaps=walker.skyMaps, skyMapRoots=walker.skyMapRoots,
                   obsInfo=walker.obsInfo)

    def __init__(self, config, gen2repos, skyMaps, skyMapRoots, obsInfo):
        log = Log.getLogger("lsst.daf.butler.gen2convert")
        self.config = Config(config)
        self.skyMaps = skyMaps
        self.obsInfo = obsInfo
        self.repos = OrderedDict()
        self.datasetTypes = dict()
        self.runs = {k: Run(id=v, collection=k) for k, v in self.config["runs"].items()}
        self.skyMapNames = {}  # mapping from hash to Gen3 skymap name
        skyMapConfig = self.config.get("skymaps", {})
        # Swap keys and values in skyMapConfig; the original can't be in
        # the order we want, because roots can have '.', and that gets
        # interpreted specially by Config when used as a key.
        rootToSkyMapName = {v: k for k, v in skyMapConfig.items()}
        for hash, skyMap in self.skyMaps.items():
            log.debug("Processing input skyMap with hash=%s", hash.hex())
            for root in skyMapRoots[hash]:
                log.debug("Processing input skyMapRoot %s", root)
                skyMapName = rootToSkyMapName.get(root, None)
                if skyMapName is not None:
                    log.debug("Using '%s' for skymap with hash=%s", skyMapName, hash.hex())
                    self.skyMapNames[hash] = skyMapName
                    break
        # Ideally we'd get the dimension universe from a Registry, but that
        # would require restructuring things in breaking ways, and I'm hoping
        # to just remove all of this code in favor of
        # obs.base.gen3.RepoConverter anyway.
        universe = DimensionUniverse()
        for gen2repo in gen2repos.values():
            self._addConvertedRepoSorted(gen2repo, universe)

    def _addConvertedRepoSorted(self, gen2repo, universe):
        """Recursively create ConvertedRepo objects from Gen2Repo objects,
        adding them to self.repos in a way that sorts them such that parents
        always precede their children.

        Also constructs all Translators and populates self.skyMapNames and
        self.datasetTypes.
        """
        log = Log.getLogger("lsst.daf.butler.gen2convert")
        log.info("Preparing writing for repo at '%s'", gen2repo.root)
        converted = self.repos.get(gen2repo.root, None)
        if converted is not None:
            return
        # Determine the SkyMaps, Collections, and Runs for this repo.
        collection = gen2repo.root
        for sub in self.config["collections", "substitutions"]:
            collection = re.sub(sub["pattern"], sub["repl"], collection)
        log.debug("Using collection '%s' for root '%s'", collection, gen2repo.root)
        run = self.runs.setdefault(collection, Run(collection=collection))
        instrument = self.config["mappers", gen2repo.MapperClass.__name__, "instrument"]
        skyMapNamesByCoaddName = {}
        for coaddName, skyMap in gen2repo.skyMaps.items():
            log.debug("Using SkyMap with hash=%s for '%s' in '%s'",
                      skyMap.getSha1().hex(), coaddName, gen2repo.root)
            skyMapNamesByCoaddName[coaddName] = self.skyMapNames[skyMap.getSha1()]
        # Create translators and Gen3 DatasetType objects from Gen2DatasetType
        # objects, but only if we actually use them for Datasets in this repo.
        translators = {}
        scFactory = StorageClassFactory()
        scConfig = self.config["storageClasses"]
        for datasetTypeName in gen2repo.datasets.keys():
            if datasetTypeName in self.datasetTypes:
                continue
            gen2dst = gen2repo.datasetTypes[datasetTypeName]
            baseDataId = {"instrument": instrument}
            # If this dataset uses skymap dimensions, look for a Gen2 dataset
            # that defines it and the Gen3 name associated with it.
            skyMap = None
            i = datasetTypeName.find("Coadd")
            if ("tract" in gen2dst.keys or i > 0):
                if len(gen2repo.skyMaps) == 1:
                    skyMap, = gen2repo.skyMaps.values()
                    baseDataId["skymap"], = skyMapNamesByCoaddName.values()
                elif i > 0:
                    coaddName = datasetTypeName[:i]
                    try:
                        skyMap = gen2repo.skyMaps[coaddName]
                        baseDataId["skymap"] = skyMapNamesByCoaddName[coaddName]
                    except KeyError:
                        pass
                if skyMap is None:
                    log.warn("No SkyMap associated with DatasetType %s in %s; skipping.",
                             datasetTypeName, gen2repo.root)
                    continue
            else:
                skyMap = None
            translators[datasetTypeName] = Translator.makeMatching(baseDataId=baseDataId,
                                                                   datasetType=gen2dst,
                                                                   skyMap=skyMap)
            log.debug("Looking for StorageClass configured for %s by datasetType", gen2dst.name)
            storageClassName = scConfig.get(datasetTypeName, None)
            if storageClassName is None:
                log.debug("Looking for StorageClass configured for %s; trying python '%s'",
                          gen2dst.name, gen2dst.python)
                storageClassName = scConfig.get(gen2dst.python, None)
            if storageClassName is None:
                log.debug("Looking for StorageClass configured for %s; trying persistable '%s'",
                          gen2dst.name, gen2dst.persistable)
                storageClassName = scConfig.get(gen2dst.persistable, None)
            if storageClassName is None:
                unqualified = gen2dst.python.split(".")[-1]
                log.debug("Looking for StorageClass configured for %s; trying unqualified python '%s'",
                          gen2dst.name, unqualified)
                storageClassName = scConfig.get(unqualified, None)
            if storageClassName is not None:
                log.debug("Found StorageClass configured for %s: '%s'",
                          gen2dst.name, storageClassName)
                storageClass = scFactory.getStorageClass(storageClassName)
            else:
                try:
                    log.debug("No StorageClass configured for %s; trying persistable '%s'",
                              gen2dst.name, gen2dst.python)
                    storageClass = scFactory.getStorageClass(gen2dst.persistable)
                except KeyError:
                    storageClass = None
                if storageClass is None:
                    log.debug("No StorageClass configured for %s; trying unqualified python type '%s'",
                              gen2dst.name, unqualified)
                    try:
                        storageClass = scFactory.getStorageClass(unqualified)
                    except KeyError:
                        log.warn("No StorageClass found for %s; skipping.", gen2dst.name)
                        continue
            log.debug("Using StorageClass %s for %s", storageClass.name, gen2dst.name)
            self.datasetTypes[datasetTypeName] = DatasetType(
                name=datasetTypeName,
                storageClass=storageClass,
                dimensions=translators[datasetTypeName].dimensionNames,
                universe=universe
            )
        converted = ConvertedRepo(gen2repo, instrument=instrument, run=run, translators=translators)
        # Add parent repositories first, so self.repos is sorted topologically.
        for parent in gen2repo.parents:
            self._addConvertedRepoSorted(parent, universe)
        # Now we can finally add the current repo to self.repos.
        self.repos[gen2repo.root] = converted
        return converted

    def run(self, registry, datastore):
        """Main driver for ConversionWriter.

        Runs all steps to create a Gen3 Repo.
        """
        # Transaction here should help with performance as well as making the
        # conversion atomic, as it prevents each Registry.addDataset from
        # having to grab a new lock on the database.
        with registry.transaction():
            self.insertInstruments(registry)
            self.insertSkyMaps(registry)
            self.insertObservations(registry)
            self.insertCalibrationLabels(registry)
            self.insertDatasetTypes(registry)
            self.insertDatasets(registry, datastore)
            self.insertObservationRegions(registry, datastore)

    def insertInstruments(self, registry):
        """Check that all necessary instruments are already present in the
        Registry, and insert them if they are not.
        """
        log = Log.getLogger("lsst.daf.butler.gen2convert")
        instruments = set()
        for repo in self.repos.values():
            instruments.add(self.config["mappers", repo.gen2.MapperClass.__name__, "instrument"])
        for instrument in instruments:
            log.debug("Looking for preexisting instrument '%s'.", instrument)
            if registry.findDimensionEntry("instrument", {"instrument": instrument}) is None:
                factory = Instrument.factories.get(instrument)
                if factory is None:
                    raise LookupError(
                        f"Instrument '{instrument}' has not been registered with the given Registry and "
                        f"no factory found; please make sure it has been imported."
                    )
                instance = factory()
                instance.register(registry)

    def insertSkyMaps(self, registry):
        """Add all necessary SkyMap Dimensions (and associated tracts and
        patches) to the Registry.
        """
        log = Log.getLogger("lsst.daf.butler.gen2convert")
        for hash, skyMap in self.skyMaps.items():
            skyMapName = self.skyMapNames.get(hash, None)
            try:
                existing, = registry.query("SELECT skymap FROM skymap WHERE hash=:hash",
                                           hash=hash)
                if skyMapName is None:
                    skyMapName = existing["skymap"]
                    self.skyMapNames[hash] = skyMapName
                    log.debug("Using preexisting skymap '%s' with hash=%s", skyMapName, hash.hex())
                if skyMapName != existing["skymap"]:
                    raise ValueError(
                        ("skymap with new name={} and hash={} already exists in the Registry "
                         "with name={}".format(skyMapName, hash.hex(), existing["skymap"]))
                    )
                continue
            except ValueError:
                # No skymap with this hash exists, so we need to insert it.
                pass
            if skyMapName is None:
                raise LookupError(
                    ("skymap with hash={} has no name "
                     "and does not already exist in the Registry.").format(hash.hex())
                )
            log.info("Inserting skymap '%s' with hash=%s", skyMapName, hash.hex())
            skyMap.register(skyMapName, registry)

    def insertObservations(self, registry):
        """Add all necessary visit and exposure Dimensions to the Registry.
        """
        log = Log.getLogger("lsst.daf.butler.gen2convert")
        for mapperName, nested in self.obsInfo.items():
            instrument = self.config["mappers", mapperName, "instrument"]
            log.info("Inserting exposure and visit Dimensions for instrument '%s'", instrument)
            for obsInfo, _ in nested.values():
                visitRecord = makeVisitRecordFromObsInfo(obsInfo, registry.dimensions)
                exposureRecord = makeExposureRecordFromObsInfo(obsInfo, registry.dimensions)
                log.debug("Inserting exposure %d and visit %d.", exposureRecord.id, visitRecord.id)
                registry.insertDimensionData("visit", visitRecord)
                registry.insertDimensionData("exposure", exposureRecord)

    def insertCalibrationLabels(self, registry):
        """Add all necessary calibration_label Dimension entries to the
        Registry.
        """
        log = Log.getLogger("lsst.daf.butler.gen2convert")
        for repo in self.repos.values():
            if repo.gen2.calibDict is None:
                continue
            # TODO: we currently implicitly assume that there is only one
            # calib repo being converted, or at least that different calib
            # repos don't have any of the same calibDates.  To fix that we
            # probably need to add a column to the calibration_label table
            # to represent a "CalibrationSet", and provide a way to configure
            # which one a Registry uses.  We'll probably also want to use that
            # pattern for other dimensions in the future, such as systems of
            # observation relationships that define a particular mapping from
            # exposure to visit.
            mapperName = repo.gen2.MapperClass.__name__
            instrument = self.config["mappers", mapperName, "instrument"]
            log.debug("Inserting unbounded calibration_label.")
            addUnboundedCalibrationLabel(registry, instrument)
            for (datasetTypeName, calibDate, ccd, filter), (first, last) in repo.gen2.calibDict.items():
                dataId = DataId(calibration_label=makeCalibrationLabel(datasetTypeName, calibDate,
                                                                       ccd=ccd, filter=filter),
                                instrument=instrument,
                                universe=registry.dimensions)
                dataId.entries["calibration_label"]["valid_first"] = first
                dataId.entries["calibration_label"]["valid_last"] = last + timedelta(days=1)
                log.debug("Inserting calibration_label %s with validity range %s - %s.",
                          dataId["calibration_label"], first, last)
                registry.addDimensionEntry("calibration_label", dataId)

    def insertDatasetTypes(self, registry):
        """Add all necessary DatasetType registrations to the Registry.
        """
        log = Log.getLogger("lsst.daf.butler.gen2convert")
        for datasetType in self.datasetTypes.values():
            log.debug("Registering DatasetType '%s'." % datasetType.name)
            registry.registerDatasetType(datasetType)

    def insertDatasets(self, registry, datastore):
        """Add all Dataset entries to the given Registry and Datastore.
        """
        log = Log.getLogger("lsst.daf.butler.gen2convert")
        instrumentCache = {}
        for repo in self.repos.values():
            refs = []
            for datasetTypeName, datasets in repo.gen2.datasets.items():
                datasetType = self.datasetTypes.get(datasetTypeName, None)
                if datasetType is None or datasetTypeName in self.config.get("skip", []):
                    log.debug("Skipping insertion of '%s' from %s", datasetTypeName, repo.gen2.root)
                    continue
                log.info("Inserting '%s' from %s", datasetTypeName, repo.gen2.root)
                collectionTemplate = self.config["collections", "overrides"].get(datasetTypeName, None)
                if collectionTemplate is None:
                    collection = repo.run.collection
                    registry.ensureRun(repo.run)
                    run = repo.run
                translator = repo.translators[datasetTypeName]
                for dataset in datasets.values():
                    try:
                        gen3id = translator(dataset.dataId)
                    except TypeError as err:
                        log.warn(
                            "Skipping insertion of '%s': %s",
                            dataset.filePath,
                            err
                        )
                        continue
                    if collectionTemplate is not None:
                        allIds = dataset.dataId.copy()
                        allIds.update(gen3id)
                        collection = collectionTemplate.format(**allIds)
                        run = self.runs.setdefault(collection, Run(collection=collection))
                        registry.ensureRun(run)
                    formatter = None
                    if datasetTypeName == "raw":
                        instrument = instrumentCache.get(gen3id["instrument"])
                        if instrument is None:
                            factory = Instrument.factories.get(gen3id["instrument"])
                            if factory is None:
                                log.warn(
                                    "Instrument not imported; raw formatter for %s not specialized.",
                                    dataset.filePath
                                )
                            instrument = factory()
                            instrumentCache[gen3id["instrument"]] = instrument
                        formatter = instrument.getRawFormatter(gen3id)
                    log.debug("Adding Dataset %s as %s in %s", dataset.filePath, gen3id, repo.run)
                    ref = registry.addDataset(datasetType, gen3id, run)
                    refs.append(ref)
                    for component in datasetType.storageClass.components:
                        compTypeName = datasetType.componentTypeName(component)
                        log.debug("  ...adding component dataset %s", compTypeName)
                        compDatasetType = registry.getDatasetType(compTypeName)
                        compRef = registry.addDataset(compDatasetType, gen3id, run=run)
                        registry.attachComponent(component, ref, compRef)
                        refs.append(compRef)
                    datastore.ingest(path=os.path.relpath(dataset.fullPath, start=datastore.root), ref=ref,
                                     formatter=formatter)

            # Add Datasets to collections associated with any child repos to
            # simulate Gen2 parent lookups.

            # TODO: The Gen2 behavior is to associate *everything* from the
            #       parent repo, because it's a repo-level link.  In Gen3, we
            #       want to limit to that to just the "relevant" datasets -
            #       which we probably define to be those in the full
            #       provenance tree of anything in the child repo.  Right now,
            #       the conversion behavior is the Gen2 behavior, which could
            #       get very expensive in the common case where we have a very
            #       large parent repo with many small child repos.
            for potentialChildRepo in self.repos.values():
                if repo.gen2.isRecursiveParentOf(potentialChildRepo.gen2):
                    log.info("Adding Datasets from %s to child collection %s.", repo.gen2.root,
                             potentialChildRepo.run.collection)
                    registry.associate(potentialChildRepo.run.collection, refs)

    def insertObservationRegions(self, registry, datastore):
        """Add spatial regions for visit-detector combinations.
        """
        sql = ("SELECT wcs.instrument AS instrument, wcs.visit AS visit, wcs.detector AS detector, "
               "        wcs.dataset_id AS wcs, metadata.dataset_id AS metadata "
               "    FROM dataset wcs "
               "        INNER JOIN dataset_collection wcs_collection "
               "            ON (wcs.dataset_id = wcs_collection.dataset_id) "
               "        INNER JOIN dataset metadata "
               "            ON (wcs.instrument = metadata.instrument "
               "                AND wcs.visit = metadata.visit "
               "                AND wcs.detector = metadata.detector) "
               "        INNER JOIN dataset_collection metadata_collection "
               "            ON (metadata.dataset_id = metadata_collection.dataset_id) "
               "    WHERE wcs_collection.collection = :collection "
               "          AND metadata_collection.collection = :collection "
               "          AND wcs.dataset_type_name = :wcs_name"
               "          AND metadata.dataset_type_name = :metadata_name")
        log = Log.getLogger("lsst.daf.butler.gen2convert")
        for config in self.config["regions"]:
            log.info("Adding observation regions using %s from %s.",
                     config["DatasetType"], config["collection"])
            visits = {}
            for row in registry.query(sql, collection=config["collection"],
                                      wcs_name="{}.wcs".format(config["DatasetType"]),
                                      metadata_name="{}.metadata".format(config["DatasetType"])):
                wcsRef = registry.getDataset(row["wcs"])
                metadataRef = registry.getDataset(row["metadata"])
                wcs = datastore.get(wcsRef)
                metadata = datastore.get(metadataRef)
                bbox = Box2D(bboxFromMetadata(metadata))
                bbox.grow(config["padding"])
                region = ConvexPolygon([sp.getVector() for sp in wcs.pixelToSky(bbox.getCorners())])
                registry.setDimensionRegion({k: row[k] for k in ("instrument", "visit", "detector")},
                                            region=region, update=False)
                visits.setdefault((row["instrument"], row["visit"]), []).extend(region.getVertices())
            for (instrument, visit), vertices in visits.items():
                region = ConvexPolygon(vertices)
                registry.setDimensionRegion(instrument=instrument, visit=visit, region=region)

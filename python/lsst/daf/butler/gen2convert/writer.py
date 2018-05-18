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

import os
import re
import datetime
from collections import OrderedDict

from lsst.log import Log

from ..core import Config, Run, DatasetType, StorageClassFactory
from .structures import ConvertedRepo
from .translators import Translator, NoSkyMapError


__all__ = ("ConversionWriter,")


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
        A dictionary with sha1 hashes as keys and `BaseSkyMap` instances as
        values, usually obtained from the `skyMaps` attribute of a
        `ConversionWalker`.
    skyMapRoots : dict
        A dictionary with sha1 hashes as keys and lists of repository roots as
        values, usually obtained from the `skyMapRoots` attribute of a
        `ConversionWalker`.
    visitInfo: dict
        A nested dictionary of afw.image.VisitInfo objects, with MapperClass
        names as outer keys and tuples of camera-dependent Gen2 visit/exposure
        identifiers as inner keys.  Usually obtained from
        `ConversionWalker.visitInfo`.
    """

    @classmethod
    def fromWalker(cls, walker):
        """Construct a ConversionWriter from a ConversionWalker."""
        return cls(config=walker.config, gen2repos=walker.scanned,
                   skyMaps=walker.skyMaps, skyMapRoots=walker.skyMapRoots,
                   visitInfo=walker.visitInfo)

    def __init__(self, config, gen2repos, skyMaps, skyMapRoots, visitInfo):
        log = Log.getLogger("lsst.daf.butler.gen2convert")
        self.config = Config(config)
        self.skyMaps = skyMaps
        self.visitInfo = visitInfo
        self.repos = OrderedDict()
        self.datasetTypes = dict()
        self.runs = {k: Run(id=v, collection=k) for k, v in self.config["runs"].items()}
        self.skyMapNames = {}  # mapping from sha1 to Gen3 SkyMap name
        skyMapConfig = self.config.get("skymaps", {})
        for sha1, skyMap in self.skyMaps.items():
            for root in skyMapRoots[sha1]:
                skyMapName = skyMapConfig.get(root, None)
                if skyMapName is not None:
                    log.debug("Using '%s' for SkyMap with sha1=%s", skyMapName, skyMap.getSha1().hex())
                    self.skyMapNames[skyMap.getSha1()] = skyMapName
                    break
        for gen2repo in gen2repos.values():
            self._addConvertedRepoSorted(gen2repo)

    def _addConvertedRepoSorted(self, gen2repo):
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
        for sub in self.config["collections.substitutions"]:
            collection = re.sub(sub["pattern"], sub["repl"], collection)
        log.debug("Using collection '%s' for root '%s'", collection, gen2repo.root)
        run = self.runs.setdefault(collection, Run(collection=collection))
        camera = self.config["mappers"][gen2repo.MapperClass.__name__]["camera"]
        skyMapNamesByCoaddName = {}
        for coaddName, skyMap in gen2repo.skyMaps.items():
            log.debug("Using SkyMap with sha1=%s for '%s' in '%s'",
                      skyMap.getSha1().hex(), coaddName, gen2repo.root)
            skyMapNamesByCoaddName[coaddName] = self.skyMapNames[skyMap.getSha1()]
        # Create translators and Gen3 DatasetType objects from Gen2DatasetType objects, but
        # only if we actually use them for Datasets in this repo.
        translators = {}
        scFactory = StorageClassFactory()
        scConfig = self.config["storageClasses"]
        for datasetTypeName in gen2repo.datasets.keys():
            gen2dst = gen2repo.datasetTypes[datasetTypeName]
            try:
                translators[datasetTypeName] = Translator.makeMatching(camera=camera, datasetType=gen2dst,
                                                                       skyMaps=gen2repo.skyMaps,
                                                                       skyMapNames=skyMapNamesByCoaddName)
            except NoSkyMapError:
                log.warn("No SkyMap associated with DatasetType %s in %s; skipping.",
                         datasetTypeName, gen2repo.root)
                continue
            if datasetTypeName in self.datasetTypes:
                continue
            log.debug("Looking for StorageClass configured for %s; trying full python '%s'",
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
                dataUnits=translators[datasetTypeName].gen3units
            )
        converted = ConvertedRepo(gen2repo, camera=camera, run=run, translators=translators)
        # Add parent repositories first, so self.repos is sorted topologically.
        for parent in gen2repo.parents:
            self._addConvertedRepoSorted(parent)
        # Now we can finally add the current repo to self.repos.
        self.repos[gen2repo.root] = converted
        return converted

    def run(self, registry, datastore):
        """Main driver for ConversionWriter.

        Runs all steps to create a Gen3 Repo, aside from Camera registration
        (we merely check that the needed Cameras, Sensors, and PhysicalFilters
        have already been registered).
        """
        self.checkCameras(registry)
        self.insertSkyMaps(registry)
        self.insertObservations(registry)
        self.insertDatasetTypes(registry)
        self.insertDatasets(registry, datastore)

    def checkCameras(self, registry):
        """Check that all necessary Cameras are already present in the
        Registry.
        """
        log = Log.getLogger("lsst.daf.butler.gen2convert")
        cameras = set()
        for repo in self.repos.values():
            cameras.add(self.config["mappers"][repo.gen2.MapperClass.__name__]["camera"])
        for camera in cameras:
            log.debug("Looking for preexisting Camera '%s'.", camera)
            if registry.findDataUnitEntry("Camera", {"camera": camera}) is None:
                raise LookupError(
                    "Camera '{}' has not been registered with the given Registry.".format(camera)
                )

    def insertSkyMaps(self, registry):
        """Add all necessary SkyMap DataUnits (and associated Tracts and
        Patches) to the Registry.
        """
        log = Log.getLogger("lsst.daf.butler.gen2convert")
        for sha1, skyMap in self.skyMaps.items():
            skyMapName = self.skyMapNames.get(sha1, None)
            try:
                existing, = registry.query("SELECT skymap FROM SkyMap WHERE sha1=:sha1",
                                           sha1=sha1)
                if skyMapName is None:
                    skyMapName = existing["skymap"]
                    self.skyMapNames[sha1] = skyMapName
                    log.debug("Using preexisting SkyMap '%s' with sha1=%s", skyMapName, sha1.hex())
                if skyMapName != existing["skymap"]:
                    raise ValueError(
                        ("SkyMap with new name={} and sha1={} already exists in the Registry "
                         "with name={}".format(skyMapName, sha1.hex(), existing["skymap"]))
                    )
                continue
            except ValueError:
                # No SkyMap with this sha1 exists, so we need to insert it.
                pass
            if skyMapName is None:
                raise LookupError(
                    ("SkyMap with sha1={} has no name "
                     "and does not already exist in the Registry.").format(sha1.hex())
                )
            log.info("Inserting SkyMap '%s' with sha1=%s", skyMapName, sha1.hex())
            skyMap.register(skyMapName, registry)

    def insertObservations(self, registry):
        """Add all necessary Visit and Exposure DataUnits to the Registry.
        """
        log = Log.getLogger("lsst.daf.butler.gen2convert")
        for mapperName, nested in self.visitInfo.items():
            camera = self.config["mappers"][mapperName]["camera"]
            log.info("Inserting Exposure and Visit DataUnits for Camera '%s'", camera)
            for visitInfoId, (visitInfo, filt) in nested.items():
                # TODO: generalize this to cameras with snaps and/or compound gen2 visit/exposure IDs
                visitId, = visitInfoId
                exposureId, = visitInfoId
                # TODO: skip insertion if DataUnits already exist.
                mid = visitInfo.getDate().toPython()
                offset = datetime.timedelta(seconds=0.5*visitInfo.getExposureTime())
                commonValues = {
                    "camera": camera,
                    "visit": visitId,
                    "physical_filter": filt,
                    "datetime_begin": mid - offset,
                    "exposure_time": visitInfo.getExposureTime(),
                    "boresight_az": visitInfo.getBoresightAzAlt().getLongitude().asDegrees(),
                    "boresight_alt": visitInfo.getBoresightAzAlt().getLatitude().asDegrees(),
                    "rot_angle": visitInfo.getBoresightRotAngle().asDegrees(),

                }
                exposureValues = commonValues.copy()
                exposureValues.update({
                    "exposure": exposureId,
                    "snap": 0,
                    "dark_time": visitInfo.getDarkTime()
                })
                visitValues = commonValues.copy()
                visitValues.update({
                    "datetime_end": mid + offset,
                    "earth_rotation_angle": visitInfo.getEra().asDegrees(),
                    "boresight_ra": visitInfo.getBoresightRaDec().getLongitude().asDegrees(),
                    "boresight_dec": visitInfo.getBoresightRaDec().getLatitude().asDegrees(),
                    "boresight_parallactic_angle": visitInfo.getBoresightParAngle().asDegrees(),
                    "local_era": visitInfo.getLocalEra().asDegrees(),
                })
                log.debug("Inserting Exposure %d and Visit %d.", exposureId, visitId)
                registry.addDataUnitEntry("Visit", visitValues)
                registry.addDataUnitEntry("Exposure", exposureValues)

    def insertDatasetTypes(self, registry):
        """Add all necessary DatasetType registrations to the Registry.
        """
        log = Log.getLogger("lsst.daf.butler.gen2convert")
        for datasetType in self.datasetTypes.values():
            # TODO: should put this "just make sure it exists" logic
            # into registerDatasetType itself, and make it a bit more careful.
            try:
                registry.registerDatasetType(datasetType)
                log.debug("Registered DatasetType '%s'." % datasetType.name)
            except KeyError:
                log.debug("DatasetType '%s' already registered." % datasetType.name)

    def insertDatasets(self, registry, datastore):
        """Add all Dataset entries to the given Registry and Datastore.
        """
        log = Log.getLogger("lsst.daf.butler.gen2convert")
        for repo in self.repos.values():
            refs = []
            for datasetTypeName, datasets in repo.gen2.datasets.items():
                datasetType = self.datasetTypes.get(datasetTypeName, None)
                if datasetType is None:
                    log.debug("Skipping insertion of '%s' from %s", datasetTypeName, repo.gen2.root)
                    continue
                log.info("Inserting '%s' from %s", datasetTypeName, repo.gen2.root)
                collectionTemplate = self.config["collections.overrides"].get(datasetTypeName, None)
                if collectionTemplate is None:
                    collection = repo.run.collection
                    registry.ensureRun(repo.run)
                    run = repo.run
                translator = repo.translators[datasetTypeName]
                for dataset in datasets.values():
                    gen3id = translator(dataset.dataId)
                    if collectionTemplate is not None:
                        allIds = dataset.dataId.copy()
                        allIds.update(gen3id)
                        collection = collectionTemplate.format(**allIds)
                        run = self.runs.setdefault(collection, Run(collection=collection))
                        registry.ensureRun(run)
                    log.debug("Adding Dataset %s as %s in %s", dataset.filePath, gen3id, repo.run)
                    ref = registry.addDataset(datasetType, gen3id, run)
                    datastore.ingest(path=os.path.relpath(dataset.fullPath, start=datastore.root), ref=ref)
                    refs.append(ref)

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

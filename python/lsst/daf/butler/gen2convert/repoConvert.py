import datetime
import os
import sqlite3
import re
import time

from collections import OrderedDict

from lsst.afw.image import readMetadata
from lsst.log import Log
from lsst.utils import getPackageDir, doImport

from astro_metadata_translator import ObservationInfo

from lsst.daf.butler import Butler, ButlerConfig, Registry, Datastore
from ..core import Config, Run, StorageClassFactory, DataId
from ..instrument import (Instrument, updateExposureEntryFromObsInfo, updateVisitEntryFromObsInfo)

from lsst.daf.butler.gen2convert.extractor import Extractor, FilePathParser
from lsst.daf.butler.gen2convert.walker import ConversionWalker
from lsst.daf.butler.gen2convert.writer import ConversionWriter
from lsst.daf.butler.gen2convert.structures import Gen2Repo, Gen2Dataset, Gen2DatasetType

from lsst.obs.subaru.gen3.hsc import HyperSuprimeCam

__all__ = ['Gen3Generic', 'OneWalker', 'OneExtractor', 'OneWriter']


class Gen3Generic():
    only = []

    def __init__(self, kwargs):
        self.converterConfig = Config(os.path.join(getPackageDir("daf_butler"), "config/gen2convert.yaml"))

        self.REPO_ROOT = None
        self.OUT_ROOT = None
        self.SCAN_ROOT = None
        self.MAPPER_ROOT = None

        if 'source' in kwargs.keys():
            self.REPO_ROOT = kwargs['source']
        if 'root' in kwargs.keys():
            self.OUT_ROOT = kwargs['root']
        if 'out' in kwargs.keys():
            self.OUT_ROOT = kwargs['out']
        if 'scan' in kwargs.keys():
            self.SCAN_ROOT = kwargs['scan']
        if 'mapper' in kwargs.keys():
            self.MAPPER_ROOT = kwargs['mapper']

        if self.REPO_ROOT is None:
            raise RuntimeError("No source repo specified")
        if self.OUT_ROOT is None:
            self.OUT_ROOT = self.REPO_ROOT
        if self.SCAN_ROOT is None:
            self.SCAN_ROOT = self.REPO_ROOT
        if self.MAPPER_ROOT is None:
            self.MAPPER_ROOT = self.REPO_ROOT

        if 'runName' in kwargs.keys():
            self.converterConfig["skymaps"] = {kwargs['runName']: os.path.join(self.REPO_ROOT, "rerun",
                                                                               kwargs['runName'])}
            self.converterConfig["regions"][0]["collection"] = "/".join("shared", kwargs['runName'])
        if 'only' in kwargs.keys():
            if kwargs['only'] is not None:
                self.only = kwargs['only']

        if 'parent' in kwargs.keys():
            self.getParent = kwargs['parent']

        self.butlerConfig = ButlerConfig(self.OUT_ROOT)
        StorageClassFactory().addFromConfig(self.butlerConfig)

        # This assignment is here to allow HSC's instrument class to exist,
        # which is needed for the writer.  There must be a more portable
        # way to do this.
        self.instrument = HyperSuprimeCam()

    def getRegistry(self):
        return Registry.fromConfig(self.butlerConfig)

    def getDatastore(self, registry):
        return Datastore.fromConfig(config=self.butlerConfig, registry=registry)

    def getButler(self, collection):
        return Butler(config=self.butlerConfig, run=collection)

    def walk(self):
        walker = OneWalker(self.converterConfig)
        walker.followLinks = True
        walker.getParent = self.getParent
        if len(self.only) > 0:
            walker.allowedDataset.append(self.only)
            walker.followLinks = True

        walker.tryRoot(self.SCAN_ROOT, self.MAPPER_ROOT)
        walker.scanAll()
        walker.readObsInfo()
        return walker

    def write(self, walker, registry, datastore):
        writer = OneWriter.fromWalker(walker)
        writer.run(registry, datastore)


class OneWalker(ConversionWalker):

    allowedDataset = []
    disallowedDataset = []
    collections = []
    firstMapper = None
    followLinks = False
    followParents = False
    _calibDict = dict()

    def tryRoot(self, root, mapper_root):
        log = Log.getLogger("czw.1convert")
        root = os.path.abspath(root)
        repo = self.found.get(root, None)
        if repo is not None:
            return repo
        if root in self.ignored:
            return True

        self.mapperClass = None
        mapperFilePath = os.path.join(root, "_mapper")
        if os.path.exists(mapperFilePath):
            with open(mapperFilePath, "r") as f:
                mapperClassPath = f.read().strip()
            self.mapperClass = doImport(mapperClassPath)

        if self.mapperClass is None:
            mapperFilePath = os.path.join(mapper_root, "_mapper")
            if os.path.exists(mapperFilePath):
                with open(mapperFilePath, "r") as f:
                    mapperClassPath = f.read().strip()
                    self.mapperClass = doImport(mapperClassPath)
            if self.mapperClass is None:
                raise RuntimeError("No mapper known for root directory %s", root)

        repo = Gen2Repo(root, self.mapperClass, calibDict=self._calibDict)
        self.found[root] = repo

        log.info("%s: identified as a data repository with mapper=%s.", root, repo.MapperClass.__name__)
        return repo

    def scanRepo(self, repo):
        log = Log.getLogger("czw.1convert")
        assert(repo.root in self.found)
        assert(repo.root not in self.ignored)
        # Short-circuit if we"ve already scanned this path.
        existing = self.scanned.setdefault(repo.root, repo)
        if existing is not repo:
            return existing

        log.info("%s: making extractor.", repo.root)
        extractor = OneExtractor(repo)
        repo.datasetTypes.update(extractor.getDatasetTypes())

        log.info("%s: walking datasets.", repo.root)
        dirs = []

        dirs.append(repo.root)

        while len(dirs) > 0:
            toDir = dirs.pop()
            print(len(dirs))
            for dirPath, dirNames, fileNames in os.walk(toDir, followlinks=self.followLinks):
                # There should need to be no recursing, as we're only
                # adding one repo tree.
                relative = dirPath[len(repo.root) + 1:]

                print(repo.root, relative, dirPath)
                if len(self.allowedDataset) > 0 and 'raw' in self.allowedDataset:
                    if 'rerun' in dirPath:
                        continue
                    elif 'ref_cats' in dirPath:
                        continue
                    elif 'BrightObject' in dirPath:
                        continue

                for fileName in fileNames:
                    if fileName in ("registry.sqlite3"):
                        # handle obsInfo extraction?
                        pass
                    elif fileName in ("calibRegistry.sqlite3"):
                        # handle calibInfo extraction
                        log.info("Reading calibration data from %s %s", dirPath, fileName)
                        calibDict = self.readCalibInfo(calibPath=os.path.join(dirPath, fileName))
                        for key, val in calibDict.items():
                            repo.calibDict[key] = val
                    elif fileName in ("_mapper"):
                        # handle mapper clashes or die?
                        pass
                    else:
                        if self.getParent is True:
                            top, bottom = os.path.split(dirPath)
                            relative = bottom

                        filePath = os.path.join(relative, fileName)
                        dataset = extractor(filePath)

                        if 'ref_cats' not in filePath:
                            print(filePath, dataset)

                        if dataset is None:
                            log.debug("%s: %s unrecognized.", repo.root, filePath)
                            repo.unrecognized.append(filePath)
                        elif dataset.datasetType.name in self.disallowedDataset:
                            log.debug("%s: %s disallowed.", repo.root, filePath)
                            repo.unrecognized.append(filePath)
                        elif (len(self.allowedDataset) > 0 and
                              dataset.datasetType.name not in self.allowedDataset):
                            log.debug("%s: %s not allowed (%d %s).", repo.root, filePath,
                                      len(self.allowedDataset), self.allowedDataset)
                            repo.unrecognized.append(filePath)
                        else:
                            if self.getParent is True:
                                top, bottom = os.path.split(dirPath)
                                relative = bottom
                                dataset.filePath = re.sub(r"^" + str(relative) + "/", "", dataset.filePath)

                            # Found a usable dataset.
                            log.debug("%s: found %s in %s with %s", repo.root, dataset.datasetType.name,
                                      dataset.root, dataset.dataId)
                            log.debug("  Filepath: %s", filePath)
                            repo.datasets[dataset.datasetType.name][filePath] = dataset

                            if dataset.root not in self.collections:
                                self.collections.append(dataset.root)

    def readObsInfo(self):
        for repo in self.scanned.values():
            config = self.config["mappers", repo.MapperClass.__name__, "VisitInfo"]
            instrumentObsInfo = self.obsInfo.setdefault(repo.MapperClass.__name__, {})
            datasets = repo.datasets.get(config["DatasetType"], {})
            for key, dataset in datasets.items():
                obsInfoId = tuple(dataset.dataId[k] for k in config["uniqueKeys"])
                if obsInfoId in instrumentObsInfo:
                    continue
                try:
                    md = readMetadata(dataset.fullPath)
                except Exception:
                    # try a more full-er path.
                    newPath = os.path.join(repo.root, dataset.fullPath)
                    try:
                        md = readMetadata(newPath)
                    except Exception:
                        # raise RuntimeError("Tried to be tricky and failed.")
                        print("Tried to be tricky and failed.  Removing offending entry: %s %s %s" %
                              (key, dataset, newPath))
                        datasets.pop(key, None)

                filt = repo.mapper.queryMetadata(config["DatasetType"], ("filter",), dataset.dataId)[0][0]
                instrumentObsInfo[obsInfoId] = (ObservationInfo(md), filt)

    def readCalibInfo(self, calibPath=None):
        """Load calibration validity ranges from a Gen2 sqlite database.

        Parameters
        ----------
        calibPath : `str`
            Path to a Gen2 ``calibRegistry.sqlite3`` file.

        Returns
        -------
        calibDict : `dict`
            Dictionary mapping tuples of (datasetTypeName, calibDate,
            filter) to
            tuples of (valid_first, valid_last);
        """
        result = {}
        with sqlite3.connect(calibPath) as calibConn:
            calibConn.row_factory = sqlite3.Row
            c = calibConn.cursor()

            queryList = []
            # This query only includes calibration types that are known at
            # this time
            for tableRow in c.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name IN "
                    "('bias', 'dark', 'defect', 'flat', 'fringe', 'sky')"):
                queryList.append("SELECT '%s' AS type,filter,ccd,calibDate,validStart,validEnd FROM %s" %
                                 (tableRow["name"], tableRow["name"]))

            query = " UNION ".join(queryList)

            for row in c.execute(query):
                key = (row["type"], row["calibDate"], row["filter"])
                value = (datetime.datetime.strptime(row["validStart"], "%Y-%m-%d"),
                         datetime.datetime.strptime(row["validEnd"], "%Y-%m-%d"))
                oldValue = result.get(key)
                if oldValue is not None and oldValue != value:
                    raise NotImplementedError("gen2convert made a bad assumption in readCalibInfo.")
                result[key] = value
        return result


TEMPLATE_RE = re.compile(r"\%\((?P<name>\w+)\)[^\%]*?(?P<type>[idrs])")


class OnePathParser(FilePathParser):
    @classmethod
    def fromMapping(cls, mapping):
        """Construct a FilePathParser instance from a Gen2
        `lsst.obs.base.Mapping` instance.
        """
        try:
            template = mapping.template
        except RuntimeError:
            return None

        # template = '/' + template
        template = re.sub(r"\[.*\]$", "", template)
        if template == '/%(path)s':
            return None

        datasetType = Gen2DatasetType(name=mapping.datasetType,
                                      keys={},
                                      persistable=mapping.persistable,
                                      python=mapping.python)
        # The template string is something like
        # "deepCoadd/%(tract)04d-%(patch)s/%(filter)s"; each step of this
        # iterator corresponds to a %-tagged substitution string.
        # Our goal in all of this parsing is to turn the template into a regex
        # we can use to extract the associated values when matching strings
        # generated with the template.
        last = 0
        terms = []
        allKeys = mapping.keys()
        for match in TEMPLATE_RE.finditer(template):
            # Copy the (escaped) regular string between the last substitution
            # and this one to the terms that will form the regex.
            terms.append(re.escape(template[last:match.start()]))
            # Pull out the data ID key from the name used in the
            # subistution string.  Use that and the substition
            # type to come up with the pattern to use in the regex.
            name = match.group("name")
            if name == "patch":
                pattern = r"\d+,\d+"
            elif match.group("type") in "id":  # integers
                pattern = r"0*\d+"
            else:
                pattern = "[^/]+"
            # only use named groups for the first occurence of a key
            if name not in datasetType.keys:
                terms.append(r"(?P<%s>%s)" % (name, pattern))
                datasetType.keys[name] = allKeys[name]
            else:
                terms.append(r"(%s)" % pattern)
            # Remember the end of this match
            last = match.end()

        # Append anything remaining after the last substitution string
        # to the regex.
        terms.append(re.escape(template[last:]))
        terms.append(r"$")
        #        if 'raw' in datasetType.name:
        print(datasetType, template)
        return cls(datasetType=datasetType, regex=re.compile("".join(terms)))

    def __call__(self, filePath, root):
        """Extract a Gen2Dataset instance from the given path.

        Parameters
        ----------
        filePath : `str`
            Path and filename relative to `root`.
        root : `str`
            Absolute path to the root of the Gen2 data repository containing
            this file.
        """
        m = self.regex.search(filePath)
        if m is None:
            return None
        dataId = {k: v(m.group(k)) for k, v in self.datasetType.keys.items()}
        newRoot = m.string[:m.start()]
        newRoot = re.sub(f"^{root}/", "", newRoot)

        return Gen2Dataset(datasetType=self.datasetType, dataId=dataId,
                           filePath=filePath, root=newRoot)


class OneExtractor(Extractor):
    def __init__(self, repo):
        self.repo = repo
        self.parsers = OrderedDict()
        print(self.repo.mapper.mappings)
        for mapping in self.repo.mapper.mappings.values():
            parser = OnePathParser.fromMapping(mapping)
            if parser is not None:
                if parser.datasetType.name != 'defects':
                    self.parsers[parser.datasetType.name] = parser


class OneWriter(ConversionWriter):

    def run(self, registry, datastore):
        """Main driver for ConversionWriter.

        Wrap components with transactions, with the exception of
        insertDatasets, which handles its own transactions.

        Runs (almost) all steps to create a Gen3 Repo.
        """
        # Transaction here should help with performance as well as making the
        # conversion atomic, as it prevents each Registry.addDataset from
        # having to grab a new lock on the database.
        #        import pdb
        #        pdb.set_trace()
        with registry.transaction():
            self.insertInstruments(registry)
        with registry.transaction():
            self.insertSkyMaps(registry)
        with registry.transaction():
            self.insertObservations(registry)
        #    pdb.set_trace()
        with registry.transaction():
            self.insertCalibrationLabels(registry)
        with registry.transaction():
            self.insertDatasetTypes(registry)
        # Do transaction internally.
        self.insertDatasets(registry, datastore)
        with registry.transaction():
            self.insertObservationRegions(registry, datastore)

    def insertSkyMaps(self, registry):
        """Add all necessary SkyMap Dimensions (and associated Tracts and
        Patches) to the Registry.
        """
        log = Log.getLogger("lsst.daf.butler.gen2convert")
        for hash, skyMap in self.skyMaps.items():
            skyMapName = self.skyMapNames.get(hash, None)
            try:
                existing, = registry.query("SELECT skymap FROM SkyMap WHERE hash=:hash",
                                           hash=hash)
                if skyMapName is None:
                    skyMapName = existing["skymap"]
                    self.skyMapNames[hash] = skyMapName
                    log.debug("Using preexisting SkyMap '%s' with hash=%s", skyMapName, hash.hex())
                if skyMapName != existing["skymap"]:
                    raise ValueError(
                        ("SkyMap with new name={} and hash={} already exists in the Registry "
                         "with name={}".format(skyMapName, hash.hex(), existing["skymap"]))
                    )
                continue
            except ValueError:
                # No SkyMap with this hash exists, so we need to insert it.
                pass
            if skyMapName is None:
                print("insertSkyMaps: unusable skymap found (%s %s).", skyMapName, hash.hex())
                continue

            log.info("Inserting SkyMap '%s' with hash=%s", skyMapName, hash.hex())
            skyMap.register(skyMapName, registry)

    def insertObservations(self, registry):
        """Add all necessary Visit and Exposure Dimensions to the Registry.
        """
        log = Log.getLogger("lsst.daf.butler.gen2convert")
        for mapperName, nested in self.obsInfo.items():
            instrument = self.config["mappers", mapperName, "instrument"]
            log.info("Inserting Exposure and Visit Dimensions for Instrument '%s'", instrument)
            for obsInfoId, (obsInfo, filt) in nested.items():
                # TODO: generalize this to instruments with snaps and/or
                # compound gen2 visit/exposure IDs
                visitId, = obsInfoId
                exposureId, = obsInfoId
                # TODO: skip insertion if Dimensions already exist.
                dataId = DataId(instrument=instrument, visit=visitId, physical_filter=filt,
                                exposure=exposureId, universe=registry.dimensions)
                updateVisitEntryFromObsInfo(dataId, obsInfo)
                updateExposureEntryFromObsInfo(dataId, obsInfo)
                log.debug("Inserting Exposure %d and Visit %d.", exposureId, visitId)
                try:
                    registry.addDimensionEntry("Visit", dataId)
                    registry.addDimensionEntry("Exposure", dataId)
                except Exception as e:
                    print("Whoops, recieved error: %s %s", e, dataId)

    def insertDatasets(self, registry, datastore):
        """Add all Dataset entries to the given Registry and Datastore.
        """
        log = Log.getLogger("lsst.daf.butler.gen2convert")
        instrumentCache = {}
        for repo in self.repos.values():
            refs = []
            for datasetTypeName, datasets in repo.gen2.datasets.items():
                datasetType = self.datasetTypes.get(datasetTypeName, None)
                if datasetType is None:
                    log.debug("Skipping insertion of '%s' from %s", datasetTypeName, repo.gen2.root)
                    continue
                log.info("Inserting '%s' from %s", datasetTypeName, repo.gen2.root)
                collectionTemplate = self.config["collections", "overrides"].get(datasetTypeName, None)
                if collectionTemplate is None:
                    collection = repo.run.collection
                    print(repo.run, repo.run.collection)
                    run = registry.ensureRun(repo.run)
                    if run is None:
                        run = repo.run
                translator = repo.translators[datasetTypeName]
                T = time.time()
                for dataset in datasets.values():
                    #                    import pdb
                    #                    pdb.set_trace()
                    print("Starting dataset: %f" % (time.time() - T))
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
                        run = registry.ensureRun(run)
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
                    print("Begin transaction dataset: %f" % (time.time() - T))
                    with registry.transaction():
                        try:
                            ref = registry.addDataset(datasetType, gen3id, run)
                        except Exception as e:
                            print("Donk (%s) for %s.  Skipping." % (e, dataset.fullPath))
                            continue
                        refs.append(ref)
                        for component in datasetType.storageClass.components:
                            compTypeName = datasetType.componentTypeName(component)
                            log.debug("  ...adding component dataset %s", compTypeName)
                            compDatasetType = registry.getDatasetType(compTypeName)
                            print("Add dataset start: %f" % (time.time() - T))
                            compRef = registry.addDataset(compDatasetType, gen3id, run=run)
                            print("Add dataset end: %f" % (time.time() - T))
                            registry.attachComponent(component, ref, compRef)
                            refs.append(compRef)
                        print("End registry component : %f" % (time.time() - T))
                        try:
                            datastore.ingest(path=os.path.join(repo.gen2.root, dataset.fullPath),
                                             ref=ref,
                                             formatter=formatter, transfer='symlink')
                        except Exception as e:
                            print("Couldn't ingest %s %s %s %s" %
                                  (dataset.fullPath, datastore.root, formatter, e))
                    print("End transaction & ingest : %f" % (time.time() - T))

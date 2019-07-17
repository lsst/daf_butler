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

__all__ = ("ConversionWalker",)

import os
import pickle
import yaml
import sqlite3
import datetime

# register YAML loader for repositoryCfg.yaml files.
import lsst.daf.persistence.repositoryCfg   # noqa: F401

from astro_metadata_translator import ObservationInfo
from lsst.afw.image import readMetadata
from lsst.log import Log
from lsst.utils import doImport

from ..core import Config

from .structures import Gen2Repo
from .extractor import Extractor


class ConversionWalker:
    """A class that walks Gen2 Data Repositories to extract the information
    necessary to create a Gen3 "snapshot" view to them.

    ConversionWalkers attempt to only include state that does not involve any
    user choices about how to perform the conversion; anything that does is
    handled by ConversionWriter instead.

    Parameters
    ----------
    config : Config
        Configuration used by both ConversionWalkers and ConversionWriters.
        Defaults are maintained in daf_base/config/gen2convert.yaml.
    """

    def __init__(self, config):
        self.config = Config(config)
        self._found = {}
        self._scanned = {}
        self._ignored = set()
        self._skyMaps = dict()
        self._skyMapRoots = dict()
        self._obsInfo = dict()

    def scanAll(self):
        """Recursively inspect and scan Gen2 data repositories.

        This calls `scanRepo` repeatedly until all repositories in `self.found`
        are in either `self.ignored` or `self.scanned`.
        """
        todo = self.found.keys() - self.scanned.keys() - self.ignored
        while todo:
            for root in todo:
                self.scanRepo(self.found[root])
            todo = self.found.keys() - self.scanned.keys() - self.ignored

    def tryRoot(self, root):
        """Attempt to identify a Gen2 Data Repository at the given root path.

        If the path appears to be a Gen2 Data Repository, a Gen2Data instance
        representing it will be added to `self.found` (unless it is already
        there) and returned.  `tryRoot()` will also be called recursively on
        all parent repositories, with all processed repositories added
        `self.found` and their `.parents` and `.MapperClass` attributes
        populated.

        Returns `None` if the path does not appear to be the root of a Gen2
        Data Repository.

        Returns `True` if the path does appear to be a Gen2 Data Repository but
        it is in `self.ignored`.
        """
        log = Log.getLogger("lsst.daf.butler.gen2convert")
        root = os.path.abspath(root)
        repo = self.found.get(root, None)
        if repo is not None:
            return repo
        if root in self.ignored:
            return True

        MapperClass = None
        parentPaths = []
        repoCfgPath = os.path.join(root, "repositoryCfg.yaml")
        calibPath = os.path.join(root, "calibRegistry.sqlite3")
        calibDict = None
        if os.path.exists(repoCfgPath):
            with open(repoCfgPath, "r") as f:
                repoCfg = yaml.load(f, Loader=yaml.UnsafeLoader)
            parentPaths = [parent.root for parent in repoCfg.parents]
            MapperClass = repoCfg.mapper
        elif os.path.exists(calibPath):
            # Slurp all the entries into _calibDict
            calibDict = self.readCalibInfo(calibPath=calibPath)
            calibRoot = os.path.dirname(root)
            mapperFilePath = os.path.join(calibRoot, "_mapper")
            MapperClass = None
            if os.path.exists(mapperFilePath):
                with open(mapperFilePath, "r") as f:
                    mapperClassPath = f.read().strip()
                    MapperClass = doImport(mapperClassPath)
        else:
            parentLinkPath = os.path.join(root, "_parent")
            if os.path.exists(parentLinkPath):
                parentPaths.append(os.readlink(parentLinkPath))
            mapperFilePath = os.path.join(root, "_mapper")
            if os.path.exists(mapperFilePath):
                with open(mapperFilePath, "r") as f:
                    mapperClassPath = f.read().strip()
                MapperClass = doImport(mapperClassPath)

        # This directory has no _mapper, no _parent, and no repositoryCfg.yaml.
        # It probably just isn't a repository root.
        if not (parentPaths or MapperClass or calibDict):
            log.debug("%s: not a data repository.", root)
            return None

        # We now have either a MapperClass or a non-zero parents list, so
        # it's likely this is a valid repository.  We"ll add it to self.found
        # before we recurse into parents to make sure we short-cut any cycles
        # (which shouldn't exist, but better to detect that later rather than
        # recurse infinitely).
        repo = Gen2Repo(root, MapperClass, calibDict=calibDict)
        self.found[root] = repo

        try:
            # Recursively construct (or look up) Gen2Repo objects for parents.
            for path in parentPaths:
                parent = self.tryRoot(path)
                if parent is None:
                    raise ValueError("%s is a parent of %s, but is not a repository." % (parent, root))
                if parent is True:  # parent has already been marked as ignored
                    continue
                repo.parents.append(parent)
            # Make sure there are no cycles in the graph of parents.
            repo._findCycles()
            # Make sure we can find a MapperClass in parents if we don't have
            # one already.
            self._ensureMapperClass(repo)
            # Make sure we have all the SkyMaps we need.
            self._ensureSkyMaps(repo)
        except Exception:
            # Unwind changes to self if something goes wrong.
            del self.found[root]
            raise

        log.info("%s: identified as a data repository with mapper=%s.", root, repo.MapperClass.__name__)
        return repo

    def scanRepo(self, repo):
        """Scan an already-found Gen2 data repository for datasets.

        Parameters
        ----------
        repo : `Gen2Repo`
            A Gen2Repo instance that must already present in `self.found`.

        Returns
        -------
        repo : `Gen2Repo`
            The same repository object passed as an argument.

        On return, the scanned repo will be added to `self.scanned`, and
        its `datasetTypes`, `datasets`, and `unrecognized` attributes will be
        populated.
        """
        log = Log.getLogger("lsst.daf.butler.gen2convert")
        assert(repo.root in self.found)
        assert(repo.root not in self.ignored)
        # Short-circuit if we"ve already scanned this path.
        existing = self.scanned.setdefault(repo.root, repo)
        if existing is not repo:
            return existing

        log.info("%s: making extractor.", repo.root)
        extractor = Extractor(repo)
        repo.datasetTypes.update(extractor.getDatasetTypes())

        log.info("%s: walking datasets.", repo.root)
        for dirPath, dirNames, fileNames in os.walk(repo.root, followlinks=True):
            dirNames[:] = [dirName for dirName in dirNames
                           if not self.tryRoot(os.path.join(dirPath, dirName))]
            relative = dirPath[len(repo.root) + 1:]
            for fileName in fileNames:
                if fileName in ("registry.sqlite3", "_mapper", "repositoryCfg.yaml", "calibRegistry.sqlite3"):
                    continue
                filePath = os.path.join(relative, fileName)
                dataset = extractor(filePath)
                if dataset is None:
                    log.debug("%s: %s unrecognized.", repo.root, filePath)
                    repo.unrecognized.append(filePath)
                else:
                    log.debug("%s: found %s with %s", repo.root, dataset.datasetType.name, dataset.dataId)
                    repo.datasets[dataset.datasetType.name][filePath] = dataset

    def readObsInfo(self):
        """Load unique ObservationInfo objects and filter associations from
        all scanned repositories.
        """
        for repo in self.scanned.values():
            config = self.config["mappers", repo.MapperClass.__name__, "VisitInfo"]
            instrumentObsInfo = self.obsInfo.setdefault(repo.MapperClass.__name__, {})
            datasets = repo.datasets.get(config["DatasetType"], {})
            for dataset in datasets.values():
                obsInfoId = tuple(dataset.dataId[k] for k in config["uniqueKeys"])
                if obsInfoId in instrumentObsInfo:
                    continue
                # Metadata will be corrected by the call to ObservationInfo
                md = readMetadata(dataset.fullPath)
                filt = repo.mapper.queryMetadata(config["DatasetType"], ("filter",), dataset.dataId)[0][0]
                instrumentObsInfo[obsInfoId] = (ObservationInfo(md), filt)

    def readCalibInfo(self, calibPath):
        """Load calibration validity ranges from a Gen2 sqlite database.

        Parameters
        ----------
        calibPath : `str`
            Path to a Gen2 ``calibRegistry.sqlite3`` file.

        Returns
        -------
        calibDict : `dict`
            Dictionary mapping tuples of (datasetTypeName, calibDate) to
            tuples of (valid_first, valid_last).
        """
        log = Log.getLogger("lsst.daf.butler.gen2convert")
        result = {}
        with sqlite3.connect(calibPath) as calibConn:
            calibConn.row_factory = sqlite3.Row
            c = calibConn.cursor()
            typesWithFilters = ("sky", "flat", "fringe")
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
                ccd = row["ccd"]
                filter = row["filter"] if row["type"] in typesWithFilters else None
                key = (row["type"], row["calibDate"], ccd, filter)
                value = (datetime.datetime.strptime(row["validStart"], "%Y-%m-%d"),
                         datetime.datetime.strptime(row["validEnd"], "%Y-%m-%d"))
                result[key] = value
                log.debug("Read Gen2 validity range %s -> %s", key, value)
        return result

    @property
    def found(self):
        """All known Gen2 data repositories (`dict` of `{abspath: Gen2Repo}`).

        Found repos will always have a root, MapperClass, and a set of SkyMaps.
        """
        return self._found

    @property
    def scanned(self):
        """Gen2 data repositories that have been scanned for Datasets (`dict`
        of `{abspath: Gen2Repo}`).

        Scanned repos will have populated `datasets` and `unrecognized`
        attributes in addition to the attributes guaranteed by `found`.
        """
        return self._scanned

    @property
    def ignored(self):
        """Gen2 data repository roots that should be ignored (`set` of `str`).

        Entries are absolute paths.  Any path in `ignored` will never be
        included in `found` or `processed`.

        TODO: Provide a way to ignore repositories via the config file.
        """
        return self._ignored

    @property
    def skyMaps(self):
        """All SkyMaps found in any repository
        (`dict` of `{hash: BaseSkyMap}`).

        The SkyMaps here are a superset of those actually used by scanned
        Datasets; some may be used by Datasets in data repositories that were
        ignored after the SkyMap was found.
        """
        return self._skyMaps

    @property
    def skyMapRoots(self):
        """Repository roots in which each SkyMap was found
        (`dict` of `{hash: list}`).
        """
        return self._skyMapRoots

    @property
    def obsInfo(self):
        """All unique `astro_metadata_translator.ObservationInfo` objects and
        visit-filter associations found in any repository (`dict`, nested).

        This is a nested dictionary, with mapper class names as outer keys and
        the inner keys a tuple of mapper-specific data ID values determined
        from configuration.  Values are a tuple of
        (`astro_metadata_translator.ObservationInfo`, `str)`, with the latter
        a Gen2 filter name.
        """
        return self._obsInfo

    def _ensureMapperClass(self, repo):
        """Make sure self.MapperClass is defined.

        If self.MapperClass is None, set it from the MapperClass used by
        parent repositories.

        Assumes all parent repositories objects have been fully initialized.

        Raises
        ------
        ValueError
            Raisedif this is there is no single MapperClass used by all
            parents.
        """
        if repo.MapperClass is None:
            parentMapperClasses = set(type(p.MapperClass) for p in repo.parents)
            if len(parentMapperClasses) != 1:
                raise ValueError("Could not determine mapper for %s." % repo.root)
            repo.MapperClass = parentMapperClasses.pop()

    def _ensureSkyMaps(self, repo):
        """Make sure any SkyMaps we can use are present.

        Loops over all SkyMap datasets defined by the mapper and tries to load
        them.  When we can't find one, we look to see if there's a unique
        skymap to inherit from the parent repos (for each coaddName).

        Assumes all parent repository objects have been fully initialized.
        """
        log = Log.getLogger("lsst.daf.butler.gen2convert")
        for datasetTypeName, mapping in repo.mapper.mappings.items():
            if not datasetTypeName.endswith("_skyMap"):
                continue
            coaddName = datasetTypeName[:-len("Coadd_skyMap")]
            filePath = mapping.template  # SkyMaps should never need a dataId for lookup
            fullPath = os.path.join(repo.root, filePath)
            if os.path.exists(fullPath):
                with open(fullPath, "rb") as f:
                    skyMap = pickle.load(f, encoding="latin1")
                # If we have already loaded an equivalent SkyMap, use that.
                skyMap = self.skyMaps.setdefault(skyMap.getSha1(), skyMap)
                self.skyMapRoots.setdefault(skyMap.getSha1(), []).append(repo.root)
                log.debug("%s: using %s directly from repo.", repo.root, datasetTypeName)
            else:
                # No SkyMap for this coaddName found in this repo; see if
                # we can inherit one from parent repos (requires that any
                # that define a SkyMap for this coaddName agree).
                parentSkyMaps = set(pd.skyMaps.get(coaddName, None) for pd in repo.parents)
                parentSkyMaps.discard(None)
                if len(parentSkyMaps) == 1:
                    skyMap = parentSkyMaps.pop()
                    log.debug("%s: using %s from parents.", repo.root, datasetTypeName)
                else:
                    # No SkyMap for this coaddName.  This is not necessarily an
                    # error, because the repo may not have any datasets with
                    # that coaddName.
                    skyMap = None
                    log.debug("%s: no %s found.", repo.root, datasetTypeName)
            # Remember that this is the SkyMap associated with this
            # coaddName in this repo.
            if skyMap is not None:
                repo.skyMaps[coaddName] = skyMap

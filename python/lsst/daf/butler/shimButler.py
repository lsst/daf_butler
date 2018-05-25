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

"""
ShimButler
"""

import pickle
from collections import defaultdict

from lsst.log import Log

from lsst.daf.butler.gen2convert.structures import Gen2DatasetType
from lsst.daf.butler.gen2convert.translators import Translator
from lsst.daf.persistence import Butler as FallbackButler


__all__ = ("ShimButler", )


def _fallbackOnFailure(func):
    """Decorator that wraps a `ShimButler` method and falls back to the
    corresponding Gen2 `Butler` method when a `NotImplementedError` is raised.
    """
    def inner(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except NotImplementedError as e:
            log = Log.getLogger("lsst.daf.butler.shimButler")
            log.info("Fallback called for: %s, original call failed with: %s on args=%s, kwargs=%s",
                     func.__name__, e, args, kwargs)
            fallbackFunc = getattr(self._fallbackButler, func.__name__)
            return fallbackFunc(*args, **kwargs)
    return inner


class ShimButlerMeta(type):
    """Metaclass for ShimButler to also forward static and classmethods to
    the FallbackButler.
    """
    def __getattr__(cls, name):   # noqa N805
        return getattr(FallbackButler, name)


class ShimButler(metaclass=ShimButlerMeta):
    """Shim around Butler that acts as a Gen2 Butler.

    TODO until implementation is complete we fall back to a Gen2 Butler
    instance upon receiving an unimplemented call.

    Parameters
    ----------
    butler : `daf.butler.Butler`
        A gen3 `Butler` instance.
    fallbackButler : `daf.persistence.Butler`
        A gen2 `Butler` instance to use as fallback when calls to the primary
        fail with a `NotImplementedError`.
    """
    def __init__(self, butler, fallbackButler=None):
        self._butler = butler
        self._fallbackButler = fallbackButler
        self._translators = {}
        # Hardcode camera to HSC and import the corresponding camera module.
        # This should be replaced by a dynamic import from config,
        # but hardcode it for now.  Cannot be module level import due to
        # circular dependency.
        self._camera = "HSC"
        from lsst.obs.subaru import gen3  # noqa F401

    def _extractFullDataId(self, dataId, rest):
        """Extract full Gen2 DataId from the provided DataId and additional
        kwargs.

        Parameters
        ----------
        dataId : `dict`
            A Gen2 `DataId` dict.
        rest : `dict`
            Additional kwargs provided to Gen2 `Butler` methods.

        Returns
        -------
        fullId : `dict`
            The full DataId.
        """
        if dataId is None:
            fullDataId = {}
        else:
            fullDataId = dataId.copy()
        fullDataId.update(**rest)
        return fullDataId

    def _mapDatasetType(self, datasetType):
        """Map a gen2 dataset type name to the corresponding gen3 dataset type
        name.

        Parameters
        ----------
        datasetType : `str`
            Gen2 dataset type name.

        Returns
        -------
        datasetType : `str`
            Gen3 dataset type name.
        """
        log = Log.getLogger("lsst.daf.butler.shimButler")
        log.info("mapping datasetType: %s to: %s", datasetType, datasetType)
        return datasetType

    def _mapDataId(self, datasetType, dataId):
        """Map a gen2 data id to the corresponding gen3 data id.

        Parameters
        ----------
        datasetType : `str`
            Gen2 dataset type name.
        dataId : `dict`
            Gen2 data id.

        Returns
        -------
        dataId : `dict`
            Gen3 data id.
        """
        if "raw" == datasetType:
            raise NotImplementedError(
                "Skipping {}, do not know what to do with raw yet".format(datasetType))
        if datasetType not in self._translators:
            gen2dst = Gen2DatasetType(name=datasetType,
                                      keys={k: type(v) for k, v in dataId.items()},
                                      persistable=None,
                                      python=None)
            # TODO remove hardcoding of SkyMap to `ci_hsc` and make it configurable.
            skyMapNames = defaultdict(lambda: 'ci_hsc')
            skyMapNames['calexp'] = 'ci_hsc'
            with open('./DATA/skyMap.pickle', 'rb') as f:
                skyMaps = {'ci_hsc': pickle.load(f, encoding='latin1')}
            self._translators[datasetType] = Translator.makeMatching(camera=self._camera,
                                                                     datasetType=gen2dst,
                                                                     skyMapNames=skyMapNames,
                                                                     skyMaps=skyMaps)
        newId = self._translators[datasetType](dataId)
        newId["camera"] = self._camera  # TODO this should be added automatically?
        log = Log.getLogger("lsst.daf.butler.shimButler")
        log.info("mapping datasetType: %s with dataId %s to %s", datasetType, dataId, newId)
        return newId

    @_fallbackOnFailure
    def getKeys(self, datasetType=None, level=None, tag=None):
        """Get the valid data id keys at or above the given level of hierarchy for the dataset type or the
        entire collection if None. The dict values are the basic Python types corresponding to the keys (int,
        float, string).

        Parameters
        ----------
        datasetType - string
            The type of dataset to get keys for, entire collection if None.
        level - string
            The hierarchy level to descend to. None if it should not be restricted. Use an empty string if the
            mapper should lookup the default level.
        tags - any, or list of any
            Any object that can be tested to be the same as the tag in a dataId passed into butler input
            functions. Applies only to input repositories: If tag is specified by the dataId then the repo
            will only be read from used if the tag in the dataId matches a tag used for that repository.

        Returns
        -------
        Returns a dict. The dict keys are the valid data id keys at or above the given level of hierarchy for
        the dataset type or the entire collection if None. The dict values are the basic Python types
        corresponding to the keys (int, float, string).
        """
        raise NotImplementedError("not yet supported by ShimButler")

    @_fallbackOnFailure
    def queryMetadata(self, datasetType, format, dataId={}, **rest):
        """Returns the valid values for one or more keys when given a partial
        input collection data id.

        Parameters
        ----------
        datasetType - string
            The type of dataset to inquire about.
        format - str, tuple
            Key or tuple of keys to be returned.
        dataId - DataId, dict
            The partial data id.
        **rest -
            Keyword arguments for the partial data id.

        Returns
        -------
        A list of valid values or tuples of valid values as specified by the
        format.
        """
        raise NotImplementedError("not yet supported by ShimButler")

    @_fallbackOnFailure
    def datasetExists(self, datasetType, dataId={}, write=False, **rest):
        """Determines if a dataset file exists.
        Parameters
        ----------
        datasetType - string
            The type of dataset to inquire about.
        dataId - DataId, dict
            The data id of the dataset.
        write - bool
            If True, look only in locations where the dataset could be written,
            and return True only if it is present in all of them.
        **rest keyword arguments for the data id.
        Returns
        -------
        exists - bool
            True if the dataset exists or is non-file-based.
        """
        raise NotImplementedError("not yet supported by ShimButler")

    @_fallbackOnFailure
    def get(self, datasetType, dataId=None, immediate=True, **rest):
        """Retrieves a dataset given an input collection data id.
        Parameters
        ----------
        datasetType - string
            The type of dataset to retrieve.
        dataId - dict
            The data id.
        immediate - bool
            If False use a proxy for delayed loading.
        **rest
            keyword arguments for the data id.
        Returns
        -------
            An object retrieved from the dataset (or a proxy for one).
        """
        # Map Gen2 -> Gen3 DatasetType and DataId
        fullDataId = self._extractFullDataId(dataId, rest)
        mappedDatasetType = self._mapDatasetType(datasetType)
        mappedDataId = self._mapDataId(datasetType, fullDataId)
        # TODO currently the Gen3 `DatasetType` (and corresponding DataUnits)
        # are assumed to be present, otherwise the call is skipped.  Fix this.
        try:
            self._butler.registry.getDatasetType(mappedDatasetType)
        except KeyError:
            raise NotImplementedError("Skipped, do not have Gen3 DatasetType: {}".format(mappedDatasetType))
        log = Log.getLogger("lsst.daf.butler.shimButler")
        log.info("get datasetType: {} with dataId: {}".format(
            mappedDatasetType,
            mappedDataId))
        value = self._butler.get(datasetType=mappedDatasetType,
                                 dataId=mappedDataId)
        # Exposure needs to have detector set which is not persistable (yet).
        # The only way to get it currently is from the fallback butler.
        if hasattr(value, 'setDetector'):
            camera = self._fallbackButler.get('camera')
            value.setDetector(camera[int(fullDataId['ccd'])])
        return value

    @_fallbackOnFailure
    def put(self, obj, datasetType, dataId={}, doBackup=False, **rest):
        """Persists a dataset given an output collection data id.
        Parameters
        ----------
        obj -
            The object to persist.
        datasetType - string
            The type of dataset to persist.
        dataId - dict
            The data id.
        doBackup - bool
            If True, rename existing instead of overwriting.
            WARNING: Setting doBackup=True is not safe for parallel processing, as it may be subject to race
            conditions.
        **rest
            Keyword arguments for the data id.
        """
        # Map Gen2 -> Gen3 DatasetType and DataId
        fullDataId = self._extractFullDataId(dataId, rest)
        mappedDatasetType = self._mapDatasetType(datasetType)
        mappedDataId = self._mapDataId(datasetType, fullDataId)
        # TODO currently the Gen3 `DatasetType` (and corresponding DataUnits)
        # are assumed to be present, otherwise the call is skipped.  Fix this.
        try:
            self._butler.registry.getDatasetType(mappedDatasetType)
        except KeyError:
            raise NotImplementedError("Skipped, do not have Gen3 DatasetType: {}".format(mappedDatasetType))
        log = Log.getLogger("lsst.daf.butler.shimButler")
        log.info("put datasetType: {} with dataId: {}".format(
            mappedDatasetType,
            mappedDataId))
        self._butler.put(obj,
                         datasetType=mappedDatasetType,
                         dataId=mappedDataId)
        # TODO for now, duppicate call to put, such that dataset is found in
        # both Gen2 and Gen3 repositories.  Change this.
        raise NotImplementedError()

    @_fallbackOnFailure
    def dataRef(self, datasetType, level=None, dataId={}, **rest):
        """Returns a single ButlerDataRef.

        Given a complete dataId specified in dataId and **rest, find the unique dataset at the given level
        specified by a dataId key (e.g. visit or sensor or amp for a camera) and return a ButlerDataRef.

        Parameters
        ----------
        datasetType - string
            The type of dataset collection to reference
        level - string
            The level of dataId at which to reference
        dataId - dict
            The data id.
        **rest
            Keyword arguments for the data id.

        Returns
        -------
        dataRef - ButlerDataRef
            ButlerDataRef for dataset matching the data id
        """
        raise NotImplementedError("not yet supported by ShimButler")

    def subset(self, datasetType, level=None, dataId={}, **rest):
        """Return complete dataIds for a dataset type that match a partial (or empty) dataId.

        Given a partial (or empty) dataId specified in dataId and **rest, find all datasets that match the
        dataId.  Optionally restrict the results to a given level specified by a dataId key (e.g. visit or
        sensor or amp for a camera).  Return an iterable collection of complete dataIds as ButlerDataRefs.
        Datasets with the resulting dataIds may not exist; that needs to be tested with datasetExists().

        Parameters
        ----------
        datasetType - string
            The type of dataset collection to subset
        level - string
            The level of dataId at which to subset. Use an empty string if the mapper should look up the
            default level.
        dataId - dict
            The data id.
        **rest
            Keyword arguments for the data id.

        Returns
        -------
        subset - ButlerSubset
            Collection of ButlerDataRefs for datasets matching the data id.

        Examples
        -----------
        To print the full dataIds for all r-band measurements in a source catalog
        (note that the subset call is equivalent to: `butler.subset('src', dataId={'filter':'r'})`):

        >>> subset = butler.subset('src', filter='r')
        >>> for data_ref in subset: print(data_ref.dataId)
        """
        butlerSubset = self._fallbackButler.subset(datasetType, level=level, dataId=dataId, **rest)
        butlerSubset.butler = self  # Needs shim too
        return butlerSubset

    def __getattr__(self, name):
        """Forwards all unwrapped attributes directly to Gen2 `Butler`.
        """
        # Do not forward special members (prevents recursion and other
        # surprising behavior)
        if name.startswith("__"):
            raise AttributeError()
        return getattr(self._fallbackButler, name)

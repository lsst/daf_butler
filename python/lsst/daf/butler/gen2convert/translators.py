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

__all__ = ("Translator", "NoSkyMapError", "KeyHandler", "CopyKeyHandler", "ConstantKeyHandler",
           "makeCalibrationLabel")

import itertools
from abc import ABCMeta, abstractmethod


def makeCalibrationLabel(datasetTypeName, calibDate, filter):
    """Make a Gen3 CalibrationLabel string from a Gen2 dataset type name and
    calibDate value.
    """
    if filter is None or filter == 'None' or filter == 'NONE':
        return f"gen2/{datasetTypeName}-NONE_{calibDate}"
    else:
        return f"gen2/{datasetTypeName}-{filter}_{calibDate}"


class NoSkyMapError(LookupError):
    """Exception thrown when a Translator cannot be created because it needs
    a nonexistent SkyMap.
    """
    pass


class KeyHandler(metaclass=ABCMeta):
    """Base class for Translator helpers that each handle just one Gen3 Data
    ID key.

    Parameters
    ----------
    gen3key : `str`
        Name of the Gen3 Data ID key (Dimension link field name) populated by
        this handler (e.g. "visit" or "abstract_filter")
    gen3unit : `str`
        Name of the Gen3 Dimension associated with `gen3key` (e.g. "Visit" or
        "AbstractFilter").
    """

    __slots__ = ("gen3key", "gen3unit")

    def __init__(self, gen3key, gen3unit):
        self.gen3key = gen3key
        self.gen3unit = gen3unit

    def translate(self, gen2id, gen3id, skyMap, skyMapName, datasetTypeName):
        gen3id[self.gen3key] = self.extract(gen2id, skyMap=skyMap, skyMapName=skyMapName,
                                            datasetTypeName=datasetTypeName)

    @abstractmethod
    def extract(self, gen2id, skyMap, skyMapName, datasetTypeName):
        raise NotImplementedError()


class ConstantKeyHandler(KeyHandler):
    """A KeyHandler that adds a constant key-value pair to the Gen3 data ID."""

    __slots__ = ("value",)

    def __init__(self, gen3key, gen3unit, value):
        super().__init__(gen3key, gen3unit)
        self.value = value

    def extract(self, gen2id, skyMap, skyMapName, datasetTypeName):
        return self.value


class CopyKeyHandler(KeyHandler):
    """A KeyHandler that simply copies a value from a Gen3 data ID.

    Parameters
    ----------
    gen3key : `str`
        Name of the Gen3 data ID key produced by this handler.
    gen3unit : `str`
        Name of the Gen3 dimension produced by this handler.
    dtype : `type`, optional
        If not `None`, the type that values for this key must be an
        instance of.
    """

    __slots__ = ("gen2key", "dtype")

    def __init__(self, gen3key, gen3unit, gen2key=None, dtype=None):
        super().__init__(gen3key, gen3unit)
        self.gen2key = gen2key if gen2key is not None else gen3key
        self.dtype = dtype

    def extract(self, gen2id, skyMap, skyMapName, datasetTypeName):
        r = gen2id[self.gen2key]
        if self.dtype is not None:
            try:
                r = self.dtype(r)
            except ValueError as err:
                raise TypeError(
                    f"'{r}' is not a valid value for {self.gen3key}; "
                    f"expected {self.dtype.__name__}, got {type(r).__name__}."
                ) from err
        return r


class PatchKeyHandler(KeyHandler):
    """A KeyHandler for Patches."""

    __slots__ = ()

    def __init__(self):
        super().__init__("patch", "Patch")

    def extract(self, gen2id, skyMap, skyMapName, datasetTypeName):
        tract = gen2id["tract"]
        tractInfo = skyMap[tract]
        x, y = gen2id["patch"].split(",")
        patchInfo = tractInfo[int(x), int(y)]
        return tractInfo.getSequentialPatchIndex(patchInfo)


class SkyMapKeyHandler(KeyHandler):
    """A KeyHandler for SkyMaps."""

    __slots__ = ()

    def __init__(self):
        super().__init__("skymap", "SkyMap")

    def extract(self, gen2id, skyMap, skyMapName, datasetTypeName):
        return skyMapName


class CalibKeyHandler(KeyHandler):
    """A KeyHandler for master calibration datasets.

    This translator currently assumes that dataset type name and
    the "calibDate" value from the Gen2 calibration registry together
    have a unique mapping to a validity range (i.e. there are no overrides
    for e.g. detector or filter).
    """

    __slots__ = ()

    def __init__(self):
        super().__init__("calibration_label", "CalibrationLabel")

    def extract(self, gen2id, skyMap, skyMapName, datasetTypeName):
        if 'filter' in gen2id.keys():
            return makeCalibrationLabel(datasetTypeName, gen2id["calibDate"], gen2id['filter'])
        else:
            return makeCalibrationLabel(datasetTypeName, gen2id["calibDate"], None)


class Translator:
    """Callable object that translates Gen2 Data IDs to Gen3 Data IDs for a
    particular DatasetType.

    Translators should usually be constructed via the `makeMatching` method.

    Parameters
    ----------
    handlers : `list`
        A list of KeyHandlers this Translator should use.
    skyMap : `BaseSkyMap`
        SkyMap instance used to define any Tract or Patch Dimensions.
    skyMapName : `str`
        Gen3 SkyMap Dimension name to be associated with any Tract or Patch
        Dimensions.
    """

    __slots__ = ("handlers", "skyMap", "skyMapName", "datasetTypeName")

    # Rules used to match Handlers when constring a Translator.
    # outer key is instrument name, or None for any
    # inner key is DatasetType name, or None for any
    # values are 3-tuples of (frozenset(gen2keys), handler, consume)
    _rules = {
        None: {
            None: []
        }
    }

    @classmethod
    def addRule(cls, handler, instrument=None, datasetTypeName=None, gen2keys=(), consume=True):
        """Add a KeyHandler and an associated matching rule.

        Parameters
        ----------
        handler : `KeyHandler`
            A KeyHandler instance to add to a Translator when this rule
            matches.
        instrument : `str`
            Gen3 instrument name the Gen2 repository must be associated with
            for this rule to match, or None to match any instrument.
        datasetTypeName : `str`
            Name of the DatasetType this rule matches, or None to match any
            DatasetType.
        gen2Keys : sequence
            Sequence of Gen2 data ID keys that must all be present for this
            rule to match.
        consume : `bool` or `tuple`
            If True (default), remove all entries in gen2keys from the set of
            keys being matched to in order to prevent less-specific handlers
            from matching them.
            May also be a `tuple` listing only the keys to consume.
        """
        # Ensure consume is always a frozenset, so we can process it uniformly
        # from here on.
        if consume is True:
            consume = frozenset(gen2keys)
        elif consume:
            consume = frozenset(consume)
        else:
            consume = frozenset()
        # find the rules for this instrument, or if we haven't seen it before,
        # add a nested dictionary that matches any DatasetType name and then
        # append this rule.
        rulesForInstrument = cls._rules.setdefault(instrument, {None: []})
        rulesForInstrumentAndDatasetType = rulesForInstrument.setdefault(datasetTypeName, [])
        rulesForInstrumentAndDatasetType.append((frozenset(gen2keys), handler, consume))

    @classmethod
    def makeMatching(cls, instrument, datasetType, skyMapNames, skyMaps):
        """Construct a Translator appropriate for instances of the given
        dataset.

        Parameters
        ----------
        instrument : `str`
            String name of the Gen3 instrument associated with the Gen2
            repository this dataset is being translated from.
        datasetType : `Gen2DatasetType`
            A structure containing information about the Gen2 DatasetType,
            including its name and data ID keys.
        skyMaps: `dict`
            A dictionary mapping "coaddName" strings to Gen2SkyMap instances.
        skyMapNames : `dict`
            A dictionary mapping "coaddName" strings to Gen3 SkyMap names.

        Returns
        -------
        translator : `Translator`
            A translator whose translate() method can be used to transform Gen2
            data IDs to Gen3 dataIds.

        Raises
        ------
        NoSkyMapError
            Raised when the given skyMaps and/or skyMapNames dicts do not
            contain a entry with the "coaddName" associated with this Dataset.
        """
        rulesForInstrument = cls._rules.get(instrument, {None: []})
        rulesForAnyInstrument = cls._rules[None]
        candidateRules = itertools.chain(
            rulesForInstrument.get(datasetType.name, []),     # this instrument, this DatasetType
            rulesForInstrument[None],                         # this instrument, any DatasetType
            rulesForAnyInstrument.get(datasetType.name, []),  # any instrument, this DatasetType
            rulesForAnyInstrument[None],                      # any instrument, any DatasetType
        )
        matchedHandlers = []
        targetKeys = set(datasetType.keys)
        for ruleKeys, ruleHandlers, consume in candidateRules:
            if ruleKeys.issubset(targetKeys):
                matchedHandlers.append(ruleHandlers)
                targetKeys -= consume
        i = datasetType.name.find("Coadd")
        if ("tract" in datasetType.keys or i > 0):
            if len(skyMaps) == 1:
                skyMap, = skyMaps.values()
                skyMapName, = skyMapNames.values()
            elif i > 0:
                coaddName = datasetType.name[:i]
                try:
                    skyMap = skyMaps[coaddName]
                    skyMapName = skyMapNames[coaddName]
                except KeyError:
                    raise NoSkyMapError("No skymap found for {}.".format(datasetType.name))
            else:
                raise NoSkyMapError("No skymap found for {}.".format(datasetType.name))
        else:
            skyMap = None
            skyMapName = None
        return Translator(matchedHandlers, skyMap=skyMap, skyMapName=skyMapName,
                          datasetTypeName=datasetType.name)

    def __init__(self, handlers, skyMap, skyMapName, datasetTypeName):
        self.handlers = handlers
        self.skyMap = skyMap
        self.skyMapName = skyMapName
        self.datasetTypeName = datasetTypeName

    def __call__(self, gen2id):
        """Return a Gen3 data ID that corresponds to the given Gen2 data ID.
        """
        gen3id = {}
        for handler in self.handlers:
            handler.translate(gen2id, gen3id, skyMap=self.skyMap, skyMapName=self.skyMapName,
                              datasetTypeName=self.datasetTypeName)
        return gen3id

    @property
    def gen3keys(self):
        """The Gen3 data ID keys populated by this Translator (`frozenset`)."""
        return frozenset(h.gen3key for h in self.handlers)

    @property
    def gen3units(self):
        """The Gen3 Dimension (names) populated by this Translator
        (`frozenset`)."""
        return frozenset(h.gen3unit for h in self.handlers)


# Add "skymap" to Gen3 ID if Gen2 ID has a "tract" key.
Translator.addRule(SkyMapKeyHandler(), gen2keys=("tract",), consume=False)

# Add "skymap" to Gen3 Id if DatasetType is one of a few specific ones
for coaddName in ("deep", "goodSeeing", "psfMatched", "dcr"):
    Translator.addRule(SkyMapKeyHandler(), datasetTypeName=f"{coaddName}Coadd_skyMap")

# Translate Gen2 str patch IDs to Gen3 sequential integers.
Translator.addRule(PatchKeyHandler(), gen2keys=("patch",))

# Copy Gen2 "tract" to Gen3 "tract".
Translator.addRule(CopyKeyHandler("tract", "Tract", dtype=int), gen2keys=("tract",))

# Add valid_first, valid_last to Instrument-level transmission/ datasets;
# these are considered calibration products in Gen3.
for datasetTypeName in ("transmission_sensor", "transmission_optics", "transmission_filter"):
    Translator.addRule(ConstantKeyHandler("calibration_label", "CalibrationLabel", "unbounded"),
                       datasetTypeName=datasetTypeName)

# Translate Gen2 pixel_id to Gen3 skypix.
# For now, we just assume that the Gen3 Registry's pixelization happens to be
# the same as what the ref_cat indexer uses.
Translator.addRule(CopyKeyHandler("skypix", "SkyPix", gen2key="pixel_id", dtype=int), gen2keys=("pixel_id",))

# Translate Gen2 calibDate and datasetType to Gen3 calibration_label.
Translator.addRule(CalibKeyHandler(), gen2keys=("calibDate",))

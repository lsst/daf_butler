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

import itertools
from abc import ABCMeta, abstractmethod

__all__ = ("Translator", "NoSkyMapError", "KeyHandler", "CopyKeyHandler", "ConstantKeyHandler")


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
        Name of the Gen3 Data ID key (DataUnit link field name) populated by
        this handler (e.g. "visit" or "abstract_filter")
    gen3unit : `str`
        Name of the Gen3 DataUnit associated with `gen3key` (e.g. "Visit" or
        "AbstractFilter").
    """

    __slots__ = ("gen3key", "gen3unit")

    def __init__(self, gen3key, gen3unit):
        self.gen3key = gen3key
        self.gen3unit = gen3unit

    def translate(self, gen2id, gen3id, skyMap, skyMapName):
        gen3id[self.gen3key] = self.extract(gen2id, skyMap=skyMap, skyMapName=skyMapName)

    @abstractmethod
    def extract(self, gen2id, skyMap, skyMapName):
        raise NotImplementedError()


class ConstantKeyHandler(KeyHandler):
    """A KeyHandler that adds a constant key-value pair to the Gen3 data ID."""

    __slots__ = ("value",)

    def __init__(self, gen3key, gen3unit, value):
        super().__init__(gen3key, gen3unit)
        self.value = value

    def extract(self, gen2id, skyMap, skyMapName):
        return self.value


class CopyKeyHandler(KeyHandler):
    """A KeyHandler that simply copies a value from a Gen3 data ID."""

    __slots__ = ("gen2key",)

    def __init__(self, gen3key, gen3unit, gen2key=None):
        super().__init__(gen3key, gen3unit)
        self.gen2key = gen2key if gen2key is not None else gen3key

    def extract(self, gen2id, skyMap, skyMapName):
        return gen2id[self.gen2key]


class PatchKeyHandler(KeyHandler):
    """A KeyHandler for Patches."""

    __slots__ = ()

    def __init__(self):
        super().__init__("patch", "Patch")

    def extract(self, gen2id, skyMap, skyMapName):
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

    def extract(self, gen2id, skyMap, skyMapName):
        return skyMapName


class Translator:
    """Callable object that translates Gen2 Data IDs to Gen3 Data IDs for a
    particular DatasetType.

    Translators should usually be constructed via the `makeMatching` method.

    Parameters
    ----------
    handlers : `list`
        A list of KeyHandlers this Translator should use.
    skyMap : `BaseSkyMap`
        SkyMap instance used to define any Tract or Patch DataUnits.
    skyMapName : `str`
        Gen3 SkyMap DataUnit name to be associated with any Tract or Patch
        DataUnits.
    """

    __slots__ = ("handlers", "skyMap", "skyMapName")

    # Rules used to match Handlers when constring a Translator.
    # outer key is camera name, or None for any
    # inner key is DatasetType name, or None for any
    # values are 3-tuples of (frozenset(gen2keys), handler, consume)
    _rules = {
        None: {
            None: []
        }
    }

    @classmethod
    def addRule(cls, handler, camera=None, datasetTypeName=None, gen2keys=(), consume=True):
        """Add a KeyHandler and an associated matching rule.

        Parameters
        ----------
        handler : `KeyHandler`
            A KeyHandler instance to add to a Translator when this rule
            matches.
        camera : `str`
            Gen3 camera name the Gen2 repository must be associated with for
            this rule to match, or None to match any camera.
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
        # find the rules for this camera, or if we haven't seen it before,
        # add a nested dictionary that matches any DatasetType name and then
        # append this rule.
        rulesForCamera = cls._rules.setdefault(camera, {None: []})
        rulesForCameraAndDatasetType = rulesForCamera.setdefault(datasetTypeName, [])
        rulesForCameraAndDatasetType.append((frozenset(gen2keys), handler, consume))

    @classmethod
    def makeMatching(cls, camera, datasetType, skyMapNames, skyMaps):
        """Construct a Translator appropriate for instances of the given
        dataset.

        Parameters
        ----------
        camera : `str`
            String name of the Gen3 camera associated with the Gen2 repository
            this dataset is being translated from.
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
        rulesForCamera = cls._rules.get(camera, {None: []})
        rulesForAnyCamera = cls._rules[None]
        candidateRules = itertools.chain(
            rulesForCamera.get(datasetType.name, []),     # this camera, this DatasetType
            rulesForCamera[None],                         # this camera, any DatasetType
            rulesForAnyCamera.get(datasetType.name, []),  # any camera, this DatasetType
            rulesForAnyCamera[None],                      # any camera, any DatasetType
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
        return Translator(matchedHandlers, skyMap=skyMap, skyMapName=skyMapName)

    def __init__(self, handlers, skyMap, skyMapName):
        self.handlers = handlers
        self.skyMap = skyMap
        self.skyMapName = skyMapName

    def __call__(self, gen2id):
        """Return a Gen3 data ID that corresponds to the given Gen2 data ID.
        """
        gen3id = {}
        for handler in self.handlers:
            handler.translate(gen2id, gen3id, skyMap=self.skyMap, skyMapName=self.skyMapName)
        return gen3id

    @property
    def gen3keys(self):
        """The Gen3 data ID keys populated by this Translator (`frozenset`)."""
        return frozenset(h.gen3key for h in self.handlers)

    @property
    def gen3units(self):
        """The Gen3 DataUnit (names) populated by this Translator (`frozenset`)."""
        return frozenset(h.gen3unit for h in self.handlers)


# Add "skymap" to Gen3 ID if Gen2 ID has a "tract" key.
Translator.addRule(SkyMapKeyHandler(), gen2keys=("tract",), consume=False)

# Translate Gen2 str patch IDs to Gen3 sequential integers.
Translator.addRule(PatchKeyHandler(), gen2keys=("patch",))

# Copy Gen2 "tract" to Gen3 "tract".
Translator.addRule(CopyKeyHandler("tract", "Tract"), gen2keys=("tract",))

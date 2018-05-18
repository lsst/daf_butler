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
from collections import defaultdict

from lsst.log import Log

__all__ = ("Gen2DatasetType", "Gen2Dataset", "Gen2Repo", "ConvertedRepo")


class Gen2DatasetType:
    """Data structure that represents a DatasetType defined by a Gen2 Mapper.

    Parameters
    ----------
    name : `str`
        String name of this DatasetType.
    keys : `dict`
        A mapping from data ID key names to the type objects corresponding to
        their values.
    persistable : `str`
        The string configured as the "persistable" type for the corresponding
        Gen2 Mapping object.
    python : `str`
        String name of the fully-qualified Python type associated with this
        DatasetType.
    """

    __slots__ = ("name", "keys", "persistable", "python")

    def __init__(self, name, keys, persistable, python):
        self.name = name
        self.keys = keys
        self.persistable = persistable
        self.python = python

    def __str__(self):
        return self.name

    def __repr__(self):
        return "Gen2DatasetType(%r, %s, %r, %r)" % (self.name, self.keys, self.persistable, self.python)


class Gen2Dataset:
    """Data structure that represents a Dataset in a Gen2 Data Repository.

    Parameters
    ----------
    datasetType : `Gen2DatasetType`
        Structure describing the DatasetType of this Dataset.
    dataId : `dict`
        Gen2 Data ID dictionary.  Contains at least the key-value pairs that
        are necessary to uniquely identify the Dataset, and may contain others.
    filePath : `str`
        Path and filename of the Dataset relative to its repository.
    root : `str`
        Root of the repository in which this Dataset exists.
    """

    __slots__ = ("datasetType", "dataId", "filePath", "root")

    def __init__(self, datasetType, dataId, filePath, root):
        self.datasetType = datasetType
        self.dataId = dataId
        self.filePath = filePath
        self.root = root

    def __str__(self):
        return "%s at %s" % (self.datasetType, self.dataId)

    def __repr__(self):
        return "Gen2Dataset(%s, %s, %s, %s)" % (self.datasetType, self.dataId, self.filePath, self.root)

    @property
    def fullPath(self):
        """Full path to this dataset, combining `root` and `filePath` (`str`)."""
        return os.path.join(self.root, self.filePath)


class Gen2Repo:
    """Data structure that represents the contents of a Gen2 Data Repository.

    Gen2Repo instances should *only* be constructed by calling
    `ConversionWalker.tryRoot`.
    """

    __slots__ = ("_root", "_MapperClass", "_mapper", "_parents", "_skyMaps", "_datasetTypes",
                 "_datasets", "_unrecognized")

    def __init__(self, root, MapperClass):
        self._root = root
        self._MapperClass = MapperClass
        self._mapper = None
        self._parents = []
        self._skyMaps = {}
        self._datasetTypes = {}
        self._datasets = defaultdict(dict)
        self._unrecognized = []

    def __eq__(self, rhs):
        if not isinstance(rhs, Gen2Repo):
            return NotImplemented
        return self.root == rhs.root

    def __ne__(self, rhs):
        return not (self == rhs)

    def isRecursiveParentOf(self, other):
        """Return true if self is a recursive parent repository of other."""
        for parent in other.parents:
            if parent == self:
                return True
            if self.isRecursiveParentOf(parent):
                return True
        return False

    @property
    def mapper(self):
        """Gen2 Mapper that organizes this repository (`obs.base.CameraMapper)."""
        if self._mapper is None:
            mapperLog = Log.getLogger("CameraMapper")
            mapperLogLevel = mapperLog.getLevel()
            mapperLog.setLevel(mapperLog.ERROR)
            self._mapper = self.MapperClass(root=self.root)
            mapperLog.setLevel(mapperLogLevel)
        return self._mapper

    @property
    def root(self):
        """Absolute path to the root of the repository (`str`)."""
        return self._root

    @property
    def MapperClass(self):
        """Gen2 Mapper type responsible for organizing this repository (`type`)."""
        return self._MapperClass

    @property
    def parents(self):
        """A list of parent repositories (`list` of `Gen2Repo`)."""
        return self._parents

    @property
    def skyMaps(self):
        """SkyMaps used by this repository (`dict`).

        Keys are coaddNames (`str`) and values are `Gen2SkyMap` instances.
        """
        return self._skyMaps

    @property
    def datasetTypes(self):
        """Information associated with the DatasetTypes in the repo (`dict`).

        A mapping from DatasetType name to Gen2DatasetType instance.
        """
        return self._datasetTypes

    @property
    def datasets(self):
        """Datasets found directly in this repository, not a parent (`defaultdict`).

        This is a two-level nested dictionary; outer keys are
        datasetType (`str`), inner keys are filePath (`str`), values are
        `Gen2Dataset`.
        """
        return self._datasets

    @property
    def unrecognized(self):
        """A list of filePaths that were not recognized as datasets (`str`)."""
        return self._unrecognized

    def _findCycles(self, seen=None):
        """Look for cycles in the parents graph.

        Raises ValueError if a cycle is found.

        Assumes all parent repositories objects have been fully initialized.
        """
        if seen is None:
            seen = set()
        if self.root in seen:
            raise ValueError("Cycle detected for repository at %s" % self.root)
        seen.add(self.root)
        for parent in self.parents:
            parent._findCycles(seen)


class ConvertedRepo:
    """Representation of a Gen2 repository that is being converted to Gen3.

    Parameters
    ----------
    gen2 : `Gen2Repo`
        Structure describing the Gen2 data repository.
    camera : `str`
        Gen3 Camera DataUnit name.
    run : `Run`
        Gen3 Run instance Datasets will be added to (unless overridden) at
        the DatasetType or Dataset level.
    translators : `dict`
        Dictionary mapping DatasetType name to `Translator` instance.
    """

    __slots__ = ("gen2", "camera", "run", "translators")

    def __eq__(self, rhs):
        if not isinstance(rhs, ConvertedRepo):
            return NotImplemented
        return self.gen2 == rhs.gen2

    def __ne__(self, rhs):
        return not (self == rhs)

    def __init__(self, gen2, camera, run, translators):
        self.gen2 = gen2
        self.camera = camera
        self.run = run
        self.translators = translators

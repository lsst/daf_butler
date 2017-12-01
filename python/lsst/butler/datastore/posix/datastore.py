#
# LSST Data Management System
#
# Copyright 2008-2017  AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
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
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <https://www.lsstcorp.org/LegalNotices/>.
#

import os
import urllib
from abc import ABCMeta, abstractmethod

from lsst.daf.persistence.safeFileIo import safeMakeDir

from ..datastore import DatastoreConfig, Datastore
from ...storageClass import StorageClass
from ...datasets import DatasetType
from .location import Location, LocationFactory
from .fileDescriptor import FileDescriptor
from .formatter import FormatterFactory
from .fitsCatalogFormatter import FitsCatalogFormatter

import yaml


class PosixDatastoreConfig(DatastoreConfig):
    """Holds configuration for a PosixDatastore.
    """

    class Data(yaml.YAMLObject):
        """Internal datastructure to hold configuration data.
        """

        yaml_tag = u'!PosixDatastoreConfig'

        def __init__(self):
            self.formatters = {}

    def __init__(self):
        self._data = PosixDatastoreConfig.Data()
        self._loadDefaultConfiguration()

    def load(self, stream):
        """Load configuration from an input stream.

        Parameters
        ----------
        stream : `file`
            The file stream to load from (e.g. from `open()`).
        """
        self._data = yaml.load(stream)

    def dump(self, stream):
        """Dump configuration to an output stream.

        Parameters
        stream : `file`
            The file stream to dump to (e.g. from `open()`).
        """
        return yaml.dump(self._data, stream)

    def loadFromFile(self, filename):
        """Load configuration from a file.

        Parameters
        ----------
        filename : `str`
            The file to load from.
        """
        super().loadFromFile(filename)

    def dumpToFile(self, filename):
        """Dump configuration to a file.

        Parameters
        ----------
        filename : `str`
            The file to dump to.
        """
        super().dumpToFile(filename)

    @property
    def formatters(self):
        return self._data.formatters

    def _loadDefaultConfiguration(self):
        self._data.formatters = {
            "SourceCatalog": "lsst.butler.datastore.posix.fitsCatalogFormatter.FitsCatalogFormatter"}


class PosixDatastore(Datastore):
    """Basic POSIX filesystem backed Datastore.
    """

    def __init__(self, root="./", create=False, configFile=None):
        """Construct a Datastore backed by a POSIX filesystem.

        Parameters
        ----------
        root : `str`
            Root directory.
        create : `bool`
            Create root directory if it does not exist.

        Raises
        ------
        `ValueError` : If root directory does not exist and `create` is `False`.
        """
        if not os.path.isdir(root):
            if not create:
                raise ValueError("No valid root at: {0}".format(root))
            safeMakeDir(root)
        self.root = root

        self.locationFactory = LocationFactory(self.root)

        self.config = PosixDatastoreConfig()
        if configFile:
            self.config.loadFromFile(configFile)

        self.formatterFactory = FormatterFactory()
        for storageClass, formatter in self.config.formatters.items():
            self.formatterFactory.registerFormatter(storageClass, formatter)

    def get(self, uri, storageClass, parameters=None):
        """Load an `InMemoryDataset` from the store.

        Parameters
        ----------
        uri : `str`
            a Universal Resource Identifier that specifies the location of the stored `Dataset`.
        storageClass : `StorageClass`
            the `StorageClass` associated with the `DatasetType`.
        parameters : `dict`
            `StorageClass`-specific parameters that specify a slice of the `Dataset` to be loaded.

        Returns
        -------
        inMemoryDataset : `InMemoryDataset`
            Requested `Dataset` or slice thereof as an `InMemoryDataset`.
        """
        formatter = self.formatterFactory.getFormatter(storageClass)
        location = self.locationFactory.fromUri(uri)
        if not os.path.exists(location.path):
            raise ValueError("No such file: {0}".format(location.uri))
        return formatter.read(FileDescriptor(location, storageClass.type, parameters))

    def put(self, inMemoryDataset, storageClass, path, typeName=None):
        """Write a `InMemoryDataset` with a given `StorageClass` to the store.

        Parameters
        ----------
        inMemoryDataset : `InMemoryDataset`
            The `Dataset` to store.
        storageClass : `StorageClass`
            The `StorageClass` associated with the `DatasetType`.
        path : `str`
            A `Path` that provides a hint that the `Datastore` may use as (part of) the URI.
        typeName : `str`
            The `DatasetType` name, which may be used by this `Datastore` to override the
            default serialization format for the `StorageClass`.

        Returns
        -------
        uri : `str` 
            The `URI` where the primary `Dataset` is stored.
        components : `dict`, optional
            A dictionary of URIs for the `Dataset`' components.
            The latter will be empty if the `Dataset` is not a composite.
        """
        formatter = self.formatterFactory.getFormatter(storageClass, typeName)
        location = self.locationFactory.fromPath(path)
        return formatter.write(inMemoryDataset, FileDescriptor(location, storageClass.type))

    def remove(self, uri):
        """Indicate to the Datastore that a `Dataset` can be removed.

        Parameters
        ----------
        uri : `str`
            a Universal Resource Identifier that specifies the location of the stored `Dataset`.

        .. note::
            Some Datastores may implement this method as a silent no-op to disable `Dataset` deletion through standard interfaces.
        """
        location = self.locationFactory.fromUri(uri)
        if not os.path.exists(location.path):
            raise ValueError("No such file: {0}".format(location.uri))
        os.remove(location.path)

    def transfer(self, inputDatastore, inputUri, storageClass, path, typeName=None):
        """Retrieve a `Dataset` with a given `URI` from an input `Datastore`,
        and store the result in this `Datastore`.

        Parameters
        ----------
        inputDatastore : `Datastore`
            The external `Datastore` from which to retreive the `Dataset`.
        inputUri : `str`
            The `URI` of the `Dataset` in the input `Datastore`.
        storageClass : `StorageClass`
            The `StorageClass` associated with the `DatasetType`.
        path : `str`
            A `Path` that provides a hint that this `Datastore` may use as [part of] the `URI`.
        typeName : `str`
            The `DatasetType` name, which may be used by this `Datastore` to override the default serialization format for the `StorageClass`.

        Returns
        -------
        uri : `str` 
            The `URI` where the primary `Dataset` is stored.
        components : `dict`, optional
            A dictionary of URIs for the `Dataset`' components.
            The latter will be empty if the `Dataset` is not a composite.
        """
        assert inputDatastore is not self  # unless we want it for renames?
        inMemoryDataset = inputDatastore.get(inputUri, storageClass)
        return self.put(inMemoryDataset, storageClass, path, typeName)

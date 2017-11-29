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
from .storageClass import StorageClass
from .datasets import DatasetType

from lsst.daf.persistence.safeFileIo import safeMakeDir
import lsst.afw.table


class Location:
    __slots__ = ("_datastoreRoot", "_uri")

    def __init__(self, datastoreRoot, uri):
        self._datastoreRoot = datastoreRoot
        self._uri = urllib.parse.urlparse(uri)

    @property
    def uri(self):
        return self._uri.geturl()

    @property
    def path(self):
        return os.path.join(self._datastoreRoot, self._uri.path.lstrip("/"))


class LocationFactory:
    def __init__(self, datastoreRoot):
        self._datastoreRoot = datastoreRoot

    def fromUri(self, uri):
        return Location(self._datastoreRoot, uri)

    def fromPath(self, path):
        uri = urllib.parse.urljoin("file://", path)
        return self.fromUri(uri)


class FileDescriptor:
    def __init__(self, location, type=None, parameters=None):
        assert isinstance(location, Location)
        self.location = location
        self.type = type
        self.parameters = parameters


class Formatter(object, metaclass=ABCMeta):
    @abstractmethod
    def read(self, fileDescriptor):
        raise NotImplementedError("Type does not support reading")

    @abstractmethod
    def write(self, inMemoryDataset, fileDescriptor):
        raise NotImplementedError("Type does not support writing")


class FitsCatalogFormatter(Formatter):
    def read(self, fileDescriptor):
        assert isinstance(fileDescriptor, FileDescriptor)
        return fileDescriptor.type.readFits(fileDescriptor.location.path)

    def write(self, inMemoryDataset, fileDescriptor):
        assert isinstance(fileDescriptor, FileDescriptor)
        inMemoryDataset.writeFits(fileDescriptor.location.path)
        return fileDescriptor.location.uri


class FormatterFactory:
    def __init__(self):
        self._registry = {}

    def getFormatter(self, storageClass, datasetType=None):
        if datasetType:
            try:
                return self._registry[self._getName(datasetTypeName)]()
            except KeyError:
                pass
        return self._registry[self._getName(storageClass)]()

    def registerFormatter(self, type, formatter):
        """Register a Formatter.

        Parameters
        ----------
        type : string or StorageClass or DatasetType instance

        formatter : Formatter subclass (not an instance)
        """
        assert issubclass(formatter, Formatter)
        self._registry[self._getName(type)] = formatter

    @staticmethod
    def _getName(typeOrName):
        if isinstance(typeOrName, str):
            return typeOrName
        elif isinstance(typeOrName, DatasetType):
            return typeOrName.name
        elif issubclass(typeOrName, StorageClass):
            return typeOrName.name
        else:
            raise ValueError("Cannot extract name from type")


class Datastore:
    """Basic POSIX filesystem backed Datastore.
    """

    def __init__(self, root="./", create=False):
        """Construct a POSIX Datastore.
        """
        if not os.path.isdir(root):
            if not create:
                raise ValueError("No valid root at: {0}".format(root))
            safeMakeDir(root)
        self.root = root
        self.locationFactory = LocationFactory(self.root)
        self.formatterFactory = FormatterFactory()
        self.formatterFactory.registerFormatter("SourceCatalog", FitsCatalogFormatter)

    def get(self, uri, storageClass, parameters=None):
        """Load an :ref:`InMemoryDataset` from the store.

        :param str uri: a :ref:`URI` that specifies the location of the stored :ref:`Dataset`.

        :param StorageClass storageClass: the :ref:`StorageClass` associated with the :ref:`DatasetType`.

        :param dict parameters: :ref:`StorageClass`-specific parameters that specify a slice of the :ref:`Dataset` to be loaded.

        :returns: an :ref:`InMemoryDataset` or slice thereof.
        """
        formatter = self.formatterFactory.getFormatter(storageClass)
        location = self.locationFactory.fromUri(uri)
        if not os.path.exists(location.path):
            raise ValueError("No such file: {0}".format(location.uri))
        return formatter.read(FileDescriptor(location, storageClass.type))

    def put(self, inMemoryDataset, storageClass, path, typeName=None):
        """Write a :ref:`InMemoryDataset` with a given :ref:`StorageClass` to the store.

        :param inMemoryDataset: the :ref:`InMemoryDataset` to store.

        :param StorageClass storageClass: the :ref:`StorageClass` associated with the :ref:`DatasetType`.

        :param str path: A :ref:`Path` that provides a hint that the :ref:`Datastore` may use as (part of) the :ref:`URI`.

        :param str typeName: The :ref:`DatasetType` name, which may be used by this :ref:`Datastore` to override the default serialization format for the :ref:`StorageClass`.

        :returns: the :py:class:`str` :ref:`URI` and a dictionary of :ref:`URIs <URI>` for the :ref:`Dataset's <Dataset>` components.  The latter will be empty (or None?) if the :ref:`Dataset` is not a composite.
        """
        formatter = self.formatterFactory.getFormatter(storageClass)
        location = self.locationFactory.fromPath(path)
        return formatter.write(inMemoryDataset, FileDescriptor(location, storageClass.type))

    def remove(self, uri):
        """Indicate to the Datastore that a :ref:`Dataset` can be removed.

        Some Datastores may implement this method as a silent no-op to disable :ref:`Dataset` deletion through standard interfaces.
        """
        location = self.locationFactory.fromUri(uri)
        if not os.path.exists(location.path):
            raise ValueError("No such file: {0}".format(location.uri))
        os.remove(location.path)

    def transfer(self, inputDatastore, inputUri, storageClass, path, typeName=None):
        """Retrieve a :ref:`Dataset` with a given :ref:`URI` from an input :ref:`Datastore`,
        and store the result in this :ref:`Datastore`.

        :param Datastore inputDatastore: the external :ref:`Datastore` from which to retreive the :ref:`Dataset`.

        :param str inputUri: the :ref:`URI` of the :ref:`Dataset` in the input :ref:`Datastore`.

        :param StorageClass storageClass: the :ref:`StorageClass` associated with the :ref:`DatasetType`.

        :param str path: A :ref:`Path` that provides a hint that this :ref:`Datastore` may use as [part of] the :ref:`URI`.

        :param str typeName: The :ref:`DatasetType` name, which may be used by this :ref:`Datastore` to override the default serialization format for the :ref:`StorageClass`.

        :returns: the :py:class:`str` :ref:`URI` and a dictionary of :ref:`URIs <URI>` for the :ref:`Dataset's <Dataset>` components.  The latter will be empty (or None?) if the :ref:`Dataset` is not a composite.
        """
        assert inputDatastore is not self  # unless we want it for renames?
        inMemoryDataset = inputDatastore.get(inputUri, storageClass)
        return self.put(inMemoryDataset, storageClass, path, typeName)

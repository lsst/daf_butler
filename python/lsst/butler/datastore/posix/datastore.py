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

from ..datastore import Datastore
from ...storageClass import StorageClass
from ...datasets import DatasetType

from lsst.daf.persistence.safeFileIo import safeMakeDir
import lsst.afw.table


class Location:
    """Identifies a location in the `Datastore`.

    Attributes
    ----------
    uri
    path
    """

    __slots__ = ("_datastoreRoot", "_uri")

    def __init__(self, datastoreRoot, uri):
        self._datastoreRoot = datastoreRoot
        self._uri = urllib.parse.urlparse(uri)

    @property
    def uri(self):
        """URI corresponding to location.
        """
        return self._uri.geturl()

    @property
    def path(self):
        """Path corresponding to location.

        This path includes the root of the `Datastore`.
        """
        return os.path.join(self._datastoreRoot, self._uri.path.lstrip("/"))


class LocationFactory:
    """Factory for `Location` instances.
    """

    def __init__(self, datastoreRoot):
        """Constructor

        Parameters
        ----------
        datastoreRoot : `str`
            Root directory of the `Datastore` in the filesystem.
        """
        self._datastoreRoot = datastoreRoot

    def fromUri(self, uri):
        """Factory function to create a `Location` from a URI.

        Parameters
        ----------
        uri : `str`
            A valid Universal Resource Identifier.
        
        Returns
        location : `Location`
            The equivalent `Location`.
        """
        return Location(self._datastoreRoot, uri)

    def fromPath(self, path):
        """Factory function to create a `Location` from a POSIX path.

        Parameters
        ----------
        path : `str`
            A standard POSIX path, relative to the `Datastore` root.
        
        Returns
        location : `Location`
            The equivalent `Location`.
        """
        uri = urllib.parse.urljoin("file://", path)
        return self.fromUri(uri)


class FileDescriptor:
    """Describes a particular file.

    Attributes
    ----------
    location : `Location`
        Storage location.
    type : `cls`
        Type the object will have after reading in Python (typically `StorageClass.type`).
    parameters : `dict`
        Additional parameters that can be used for reading and writing.
    """

    __slots__ = ('location', 'type', 'parameters')

    def __init__(self, location, type=None, parameters=None):
        """Constructor

        Parameters
        ----------
        location : `Location`
            Storage location.
        type : `cls`
            Type the object will have after reading in Python (typically `StorageClass.type`).
        parameters : `dict`
            Additional parameters that can be used for reading and writing.
        """
        assert isinstance(location, Location)
        self.location = location
        self.type = type
        self.parameters = parameters


class Formatter(object, metaclass=ABCMeta):
    """Interface for reading and writing `Dataset`s with a particular `StorageClass`.
    """
    @abstractmethod
    def read(self, fileDescriptor):
        """Read a `Dataset`.

        Parameters
        ----------
        fileDescriptor : `FileDescriptor`
            Identifies the file to read, type to read it into and parameters
            to be used for reading.

        Returns
        -------
        inMemoryDataset : `InMemoryDataset`
            The requested `Dataset`.
        """
        raise NotImplementedError("Type does not support reading")

    @abstractmethod
    def write(self, inMemoryDataset, fileDescriptor):
        """Write a `Dataset`.

        Parameters
        ----------
        inMemoryDataset : `InMemoryDataset`
            The `Dataset` to store.
        fileDescriptor : `FileDescriptor`
            Identifies the file to read, type to read it into and parameters
            to be used for reading.

        Returns
        -------
        uri : `str` 
            The `URI` where the primary `Dataset` is stored.
        components : `dict`, optional
            A dictionary of URIs for the `Dataset`' components.
            The latter will be empty if the `Dataset` is not a composite.
        """
        raise NotImplementedError("Type does not support writing")


class FitsCatalogFormatter(Formatter):
    """Interface for reading and writing catalogs to and from FITS files.
    """

    def read(self, fileDescriptor):
        """Read a `Catalog` from a FITS file.

        Parameters
        ----------
        fileDescriptor : `FileDescriptor`
            Identifies the file to read, type to read it into and parameters
            to be used for reading.

        Returns
        -------
        inMemoryDataset : `Catalog`
            The requested `Catalog`.
            The actual returned type will be a derived class (e.g. `SourceCatalog` or `ExposureCatalog`).
        """
        assert isinstance(fileDescriptor, FileDescriptor)
        return fileDescriptor.type.readFits(fileDescriptor.location.path)

    def write(self, inMemoryDataset, fileDescriptor):
        """Write a `Catalog` to a FITS file.

        Parameters
        ----------
        inMemoryDataset : `Catalog`
            The `Catalog` to store.
        fileDescriptor : `FileDescriptor`
            Identifies the file to read, type to read it into and parameters
            to be used for reading.

        Returns
        -------
        uri : `str` 
            The `URI` where the primary `Catalog` is stored.
        components : `dict`, optional
            A dictionary of URIs for the `Catalog`' components.
            The latter will be empty if the `Catalog` is not a composite.
        """
        assert isinstance(fileDescriptor, FileDescriptor)
        inMemoryDataset.writeFits(fileDescriptor.location.path)
        return fileDescriptor.location.uri, {}


class FormatterFactory:
    """Factory for `Formatter` instances.
    """

    def __init__(self):
        """Constructor.
        """
        self._registry = {}

    def getFormatter(self, storageClass, datasetType=None):
        """Get a new formatter instance.

        Parameters
        ----------
        storageClass : `StorageClass`
            Get `Formatter` associated with this `StorageClass`, unless.
        datasetType : `DatasetType` or `str, optional
            If given, look if an override has been specified for this `DatasetType` and,
            if so return that instead.
        """
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
        type : `str` or `StorageClass` or `DatasetType`
            Type for which this formatter is to be used.
        formatter : `Formatter` subclass (not an instance)
            The `Formatter` to use for reading and writing `Dataset`s of this type.
        """
        assert issubclass(formatter, Formatter)
        self._registry[self._getName(type)] = formatter

    @staticmethod
    def _getName(typeOrName):
        """Extract name of `DatasetType` or `StorageClass` as string.
        """
        if isinstance(typeOrName, str):
            return typeOrName
        elif isinstance(typeOrName, DatasetType):
            return typeOrName.name
        elif issubclass(typeOrName, StorageClass):
            return typeOrName.name
        else:
            raise ValueError("Cannot extract name from type")


class PosixDatastore(Datastore):
    """Basic POSIX filesystem backed Datastore.
    """

    def __init__(self, root="./", create=False):
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
        self.formatterFactory = FormatterFactory()
        self.formatterFactory.registerFormatter("SourceCatalog", FitsCatalogFormatter)

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
        return formatter.read(FileDescriptor(location, storageClass.type))

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
        formatter = self.formatterFactory.getFormatter(storageClass)
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

#
# LSST Data Management System
#
# Copyright 2008-2018  AURA/LSST.
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

from lsst.daf.persistence.safeFileIo import safeMakeDir

from lsst.daf.butler.core.datastore import Datastore
from lsst.daf.butler.core.datastore import DatastoreConfig  # noqa F401
from lsst.daf.butler.core.location import LocationFactory
from lsst.daf.butler.core.fileDescriptor import FileDescriptor
from lsst.daf.butler.core.formatter import FormatterFactory
from lsst.daf.butler.core.storageClass import StorageClassFactory


class PosixDatastore(Datastore):
    """Basic POSIX filesystem backed Datastore.
    """

    def __init__(self, config):
        """Construct a Datastore backed by a POSIX filesystem.

        Parameters
        ----------
        config : `DatastoreConfig` or `str`
            Configuration.

        Raises
        ------
        `ValueError` : If root location does not exist and `create` is `False`.
        """
        super().__init__(config)
        self.root = self.config['root']
        if not os.path.isdir(self.root):
            if 'create' not in self.config or not self.config['create']:
                raise ValueError("No valid root at: {0}".format(self.root))
            safeMakeDir(self.root)

        self.locationFactory = LocationFactory(self.root)
        self.storageClassFactory = StorageClassFactory()
        self.formatterFactory = FormatterFactory()

        for name, info in self.config["storageClasses"].items():
            # Create the storage class
            components = None
            if "components" in info:
                components = {}
                for cname, ctype in info["components"].items():
                    components[cname] = self.storageClassFactory.getStorageClass(ctype)
            self.storageClassFactory.registerStorageClass(name, info["type"], components)

            # Create the formatter, indexed by the storage class
            # Currently, we allow this to be optional because some storage classes
            # are not yet defined fully.
            if "formatter" in info:
                self.formatterFactory.registerFormatter(name, info["formatter"])

    def get(self, uri, storageClass, parameters=None):
        """Load an `InMemoryDataset` from the store.

        Parameters
        ----------
        uri : `str`
            a Universal Resource Identifier that specifies the location of the
            stored `Dataset`.
        storageClass : `StorageClass`
            the `StorageClass` associated with the `DatasetType`.
        parameters : `dict`
            `StorageClass`-specific parameters that specify a slice of the
            `Dataset` to be loaded.

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

    def put(self, inMemoryDataset, storageClass, storageHint, typeName=None):
        """Write a `InMemoryDataset` with a given `StorageClass` to the store.

        Parameters
        ----------
        inMemoryDataset : `InMemoryDataset`
            The `Dataset` to store.
        storageClass : `StorageClass`
            The `StorageClass` associated with the `DatasetType`.
        storageHint : `str`
            Provides a hint that the `Datastore` may use as (part of) the URI.
        typeName : `str`
            The `DatasetType` name, which may be used by this `Datastore` to
            override the default serialization format for the `StorageClass`.

        Returns
        -------
        uri : `str`
            The `URI` where the primary `Dataset` is stored.
        components : `dict`, optional
            A dictionary of URIs for the `Dataset`' components.
            The latter will be empty if the `Dataset` is not a composite.
        """
        formatter = self.formatterFactory.getFormatter(storageClass, typeName)
        location = self.locationFactory.fromPath(storageHint)
        storageDir = os.path.dirname(location.path)
        if not os.path.isdir(storageDir):
            safeMakeDir(storageDir)
        return formatter.write(inMemoryDataset, FileDescriptor(location, storageClass.type))

    def remove(self, uri):
        """Indicate to the Datastore that a `Dataset` can be removed.

        Parameters
        ----------
        uri : `str`
            A Universal Resource Identifier that specifies the location of the
            stored `Dataset`.

        .. note::
            Some Datastores may implement this method as a silent no-op to
            disable `Dataset` deletion through standard interfaces.
        """
        location = self.locationFactory.fromUri(uri)
        if not os.path.exists(location.path):
            raise FileNotFoundError("No such file: {0}".format(location.uri))
        os.remove(location.path)

    def transfer(self, inputDatastore, inputUri, storageClass, storageHint, typeName=None):
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
        storageHint : `str`
            Provides a hint that this `Datastore` may use as [part of] the
            `URI`.
        typeName : `str`
            The `DatasetType` name, which may be used by this `Datastore`
            to override the default serialization format for the `StorageClass`.

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
        return self.put(inMemoryDataset, storageClass, storageHint, typeName)

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

from lsst.daf.butler.core.safeFileIo import safeMakeDir
from lsst.daf.butler.core.datastore import Datastore
from lsst.daf.butler.core.datastore import DatastoreConfig  # noqa F401
from lsst.daf.butler.core.location import LocationFactory
from lsst.daf.butler.core.fileDescriptor import FileDescriptor
from lsst.daf.butler.core.formatter import FormatterFactory
from lsst.daf.butler.core.storageClass import StorageClassFactory, makeNewStorageClass
from lsst.daf.butler.core.fileTemplates import FileTemplates

__all__ = ("PosixDatastore", )


class PosixDatastore(Datastore):
    """Basic POSIX filesystem backed Datastore.

    Parameters
    ----------
    config : `DatastoreConfig` or `str`
        Configuration.

    Raises
    ------
    ValueError
        If root location does not exist and `create` is `False`.
    """

    def __init__(self, config):
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

            # Extract scalar items from dict that are needed for StorageClass Constructor
            storageClassKwargs = {k: info[k] for k in ("pytype", "assembler") if k in info}

            # Fill in other items
            storageClassKwargs["components"] = components

            # Create the new storage class and register it
            newStorageClass = makeNewStorageClass(name, **storageClassKwargs)
            self.storageClassFactory.registerStorageClass(newStorageClass)

            # Create the formatter, indexed by the storage class
            # Currently, we allow this to be optional because some storage classes
            # are not yet defined fully.
            if "formatter" in info:
                self.formatterFactory.registerFormatter(name, info["formatter"])

        # Read the file naming templates
        self.templates = FileTemplates(self.config["templates"])

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

        Raises
        ------
        ValueError
            Requested URI can not be retrieved.
        TypeError
            Return value from formatter has unexpected type.
        """
        formatter = self.formatterFactory.getFormatter(storageClass)
        location = self.locationFactory.fromUri(uri)

        # if we are asking for a component, the type we pass in to the formatter
        # should be the type of the component and not the composite type
        pytype = storageClass.pytype
        comp = location.fragment
        scComps = storageClass.components
        if comp and scComps is not None:
            pytype = None  # Clear it since this *is* a component
            if comp in scComps:
                pytype = scComps[comp].pytype

        try:
            result = formatter.read(FileDescriptor(location, pytype=pytype,
                                                   storageClass=storageClass, parameters=parameters))
        except Exception as e:
            raise ValueError("Failure from formatter for URI {}: {}".format(uri, e))

        # Validate the returned data type matches the expected data type
        if pytype and not isinstance(result, pytype):
            raise TypeError("Got type {} from formatter but expected {}".format(type(result), pytype))

        return result

    def put(self, inMemoryDataset, storageClass, dataUnits, typeName=None):
        """Write a `InMemoryDataset` with a given `StorageClass` to the store.

        Parameters
        ----------
        inMemoryDataset : `InMemoryDataset`
            The `Dataset` to store.
        storageClass : `StorageClass`
            The `StorageClass` associated with the `DatasetType`.
        dataUnits : `DataUnits`
            DataUnits to use when constructing the filename.
        typeName : `str`
            The `DatasetType` name, which may be used by this `Datastore` to
            override the default serialization format for the `StorageClass`.

        Returns
        -------
        uri : `str`
            The `URI` where the primary `Dataset` is stored.
        components : `dict`, optional
            A dictionary of URIs for the `Dataset` components.
            The latter will be empty if the `Dataset` is not a composite.
        """

        # Check to see if this storage class has a disassembler
        # and also has components
        if storageClass.assemblerClass.disassemble is not None and storageClass.components:
            compUris = {}
            components = storageClass.assembler().disassemble(inMemoryDataset)
            if components:
                for comp, info in components.items():
                    compTypeName = typeName
                    if compTypeName is not None:
                        compTypeName = "{}.{}".format(compTypeName, comp)
                    compUris[comp], _ = self.put(info.component, info.storageClass,
                                                 dataUnits, compTypeName)
                return None, compUris

        template = self.templates.getTemplate(typeName)
        location = self.locationFactory.fromPath(template.format(dataUnits,
                                                                 datasetType=typeName))

        # Write a single component
        formatter = self.formatterFactory.getFormatter(storageClass, typeName)

        storageDir = os.path.dirname(location.path)
        if not os.path.isdir(storageDir):
            safeMakeDir(storageDir)
        return formatter.write(inMemoryDataset, FileDescriptor(location, pytype=storageClass.pytype,
                                                               storageClass=storageClass))

    def remove(self, uri):
        """Indicate to the Datastore that a `Dataset` can be removed.

        Parameters
        ----------
        uri : `str`
            A Universal Resource Identifier that specifies the location of the
            stored `Dataset`.

        Notes
        -----
        Some Datastores may implement this method as a silent no-op to
        disable `Dataset` deletion through standard interfaces.
        """
        location = self.locationFactory.fromUri(uri)
        if not os.path.exists(location.preferredPath()):
            raise FileNotFoundError("No such file: {0}".format(location.uri))
        os.remove(location.preferredPath())

    def transfer(self, inputDatastore, inputUri, storageClass, dataUnits, typeName=None):
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
        dataUnits : `DataUnits`
            DataUnits to use when constructing the filename.
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
        return self.put(inMemoryDataset, storageClass, dataUnits, typeName)

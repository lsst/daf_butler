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

from abc import ABCMeta, abstractmethod

from .mappingFactory import MappingFactory


class Formatter(object, metaclass=ABCMeta):
    """Interface for reading and writing `Dataset`s with a particular
    `StorageClass`.
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


class FormatterFactory(MappingFactory):
    """Factory for `Formatter` instances.
    """
    refType = Formatter

    def getFormatter(self, storageClass, datasetType=None):
        """Get a new formatter instance.

        Parameters
        ----------
        storageClass : `StorageClass`
            Get `Formatter` associated with this `StorageClass`, unless.
        datasetType : `DatasetType` or `str`, optional
            If given, look if an override has been specified for this `DatasetType` and,
            if so return that instead.
        """
        return self.getFromRegistry(storageClass, override=datasetType)

    def registerFormatter(self, type_, formatter):
        """Register a Formatter.

        Parameters
        ----------
        type_ : `str` or `StorageClass` or `DatasetType`
            Type for which this formatter is to be used.
        formatter : `str`
            Identifies a `Formatter` subclass to use for reading and writing `Dataset`s of this type.

        Raises
        ------
        e : `ValueError`
            If formatter does not name a valid formatter type.
        """
        self.placeInRegistry(type_, formatter)

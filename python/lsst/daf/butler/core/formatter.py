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

from abc import ABCMeta, abstractmethod

from .mappingFactory import MappingFactory
from .utils import getFullTypeName

__all__ = ("Formatter", "FormatterFactory")


class Formatter(metaclass=ABCMeta):
    """Interface for reading and writing Datasets with a particular
    `StorageClass`.
    """

    @classmethod
    def name(cls):
        """Returns the fully qualified name of the formatter.
        """
        return getFullTypeName(cls)

    @abstractmethod
    def read(self, fileDescriptor, component=None):
        """Read a Dataset.

        Parameters
        ----------
        fileDescriptor : `FileDescriptor`
            Identifies the file to read, type to read it into and parameters
            to be used for reading.
        component : `str`, optional
            Component to read from the file. Only used if the `StorageClass`
            for reading differed from the `StorageClass` used to write the
            file.

        Returns
        -------
        inMemoryDataset : `InMemoryDataset`
            The requested Dataset.
        """
        raise NotImplementedError("Type does not support reading")

    @abstractmethod
    def write(self, inMemoryDataset, fileDescriptor):
        """Write a Dataset.

        Parameters
        ----------
        inMemoryDataset : `InMemoryDataset`
            The Dataset to store.
        fileDescriptor : `FileDescriptor`
            Identifies the file to write.

        Returns
        -------
        path : `str`
            The path to where the Dataset was stored.
        """
        raise NotImplementedError("Type does not support writing")

    @abstractmethod
    def predictPath(self, location):
        """Return the path that would be returned by write, without actually
        writing.

        location : `Location`
            The location to simulate writing to.
        """
        raise NotImplementedError("Type does not support writing")


class FormatterFactory:
    """Factory for `Formatter` instances.
    """

    def __init__(self):
        self._mappingFactory = MappingFactory(Formatter)

    def getFormatter(self, entity):
        """Get a new formatter instance.

        Parameters
        ----------
        entity : `DatasetRef`, `DatasetType` or `StorageClass`, or `str`
            Entity to use to determine the formatter to return.
            `StorageClass` will be used as a last resort if `DatasetRef`
            or `DatasetType` instance is provided.
        """
        if isinstance(entity, str):
            names = (entity,)
        else:
            names = entity._lookupNames()
        return self._mappingFactory.getFromRegistry(*names)

    def registerFormatter(self, type_, formatter):
        """Register a `Formatter`.

        Parameters
        ----------
        type_ : `str` or `StorageClass` or `DatasetType`
            Type for which this formatter is to be used.
        formatter : `str`
            Identifies a `Formatter` subclass to use for reading and writing
            Datasets of this type.

        Raises
        ------
        ValueError
            If formatter does not name a valid formatter type.
        """
        self._mappingFactory.placeInRegistry(type_, formatter)

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

"""Support for reading and writing files to a POSIX file system."""

__all__ = ("FileFormatter",)

import copy
from abc import abstractmethod

from lsst.daf.butler import Formatter


class FileFormatter(Formatter):
    """Interface for reading and writing files on a POSIX file system.
    """

    extension = None
    """Default file extension to use for writing files. None means that no
    modifications will be made to the supplied file extension. (`str`)"""

    @abstractmethod
    def _readFile(self, path, pytype=None):
        """Read a file from the path in the correct format.

        Parameters
        ----------
        path : `str`
            Path to use to open the file.
        pytype : `class`, optional
            Class to use to read the file.

        Returns
        -------
        data : `object`
            Data read from file. Returns `None` if the file can not be
            found at the given path.

        Raises
        ------
        Exception
            Some problem reading the file.
        """
        pass

    @abstractmethod
    def _writeFile(self, inMemoryDataset):
        """Write the in memory dataset to file on disk.

        Parameters
        ----------
        inMemoryDataset : `object`
            Object to serialize.

        Raises
        ------
        Exception
            The file could not be written.
        """
        pass

    def _coerceType(self, inMemoryDataset, storageClass, pytype=None):
        """Coerce the supplied inMemoryDataset to type `pytype`.

        Usually a no-op.

        Parameters
        ----------
        inMemoryDataset : `object`
            Object to coerce to expected type.
        storageClass : `StorageClass`
            StorageClass associated with ``inMemoryDataset``.
        pytype : `class`, optional
            Override type to use for conversion.

        Returns
        -------
        inMemoryDataset : `object`
            Object of expected type `pytype`.
        """
        return inMemoryDataset

    def _assembleDataset(self, data, fileDescriptor, component):
        """Assembles and coerces the dataset, or one of its components,
        into an appropriate python type and returns it.

        Parameters
        ----------
        fileDescriptor : `FileDescriptor`
            Identifies the file to read, type to read it into and parameters
            to be used for reading.
        data : `dict` or `object`
            a composite or a dict that, or which component, needs to be
            coerced to a ptype
        fileDescriptor : `FileDescriptor`
            Identifies the file to read, type to read it into and parameters
            to be used for reading.
        component : `str`, optional
            Component to read from the file. Only used if the `StorageClass`
            for reading differed from the `StorageClass` used to write the
            file.
        Returns
        -------
        inMemoryDataset : `object`
            Object of expected type `pytype`.

        """
        fileDescriptor = self.fileDescriptor

        # Read the file naively
        path = fileDescriptor.location.path
        data = self._readFile(path, fileDescriptor.storageClass.pytype)

        # if read and write storage classes differ, more work is required
        readStorageClass = fileDescriptor.readStorageClass
        if readStorageClass != fileDescriptor.storageClass:
            if component is None:
                raise ValueError("Storage class inconsistency ({} vs {}) but no"
                                 " component requested".format(readStorageClass.name,
                                                               fileDescriptor.storageClass.name))

            # Concrete composite written as a single file (we hope)
            try:
                data = fileDescriptor.storageClass.assembler().getComponent(data, component)
            except AttributeError:
                # Defer the complaint
                data = None

        # Coerce to the requested type (not necessarily the type that was
        # written)
        data = self._coerceType(data, fileDescriptor.readStorageClass,
                                pytype=fileDescriptor.readStorageClass.pytype)

        return data

    def read(self, component=None):
        """Read data from a file.

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
        inMemoryDataset : `object`
            The requested data as a Python object. The type of object
            is controlled by the specific formatter.

        Raises
        ------
        ValueError
            Component requested but this file does not seem to be a concrete
            composite.
        """

        # Read the file naively
        path = fileDescriptor.location.path
        data = self._readFile(path, fileDescriptor.storageClass.pytype)

        if data is None:
            raise ValueError("Unable to read data with URI {}".format(fileDescriptor.location.uri))

        # Assemble the requested dataset and potentially return only its component
        # coercing it to its appropriate ptype
        data = self._assembleDataset(data, fileDescriptor, component)

        return data

    def fromBytes(self, pickledDataset, fileDescriptor, component=None):
        """Read data from a bytestring.

        Parameters
        ----------
        pickledDataset : `str`
            Bytes object to unserialize
        component : `str`, optional
            Component to read from the file. Only used if the `StorageClass`
            for reading differed from the `StorageClass` used to write the
            file.
        pytype : 'class'
            Class to use to read the dataset into a python object.

        Returns
        -------
        inMemoryDataset : `object`
            The requested data as a Python object. The type of object
            is controlled by the specific formatter.

        Raises
        ------
        ValueError
            Component requested but this file does not seem to be a concrete
            composite.
        """
        data = self._fromBytes(pickledDataset, fileDescriptor.storageClass.pytype)
        if data is None:
            raise ValueError("Unable to read data with URI {}".format(fileDescriptor.location.uri))

        # Assemble the requested dataset and potentially return only its component
        # coercing it to its appropriate ptype
        data = self._assembleDataset(data, fileDescriptor, component)
        return data

    def write(self, inMemoryDataset):
        """Write a Python object to a file.

        Parameters
        ----------
        inMemoryDataset : `object`
            The Python object to store.

        Returns
        -------
        path : `str`
            The path where the primary file is stored within the datastore.
        """
        fileDescriptor = self.fileDescriptor
        # Update the location with the formatter-preferred file extension
        fileDescriptor.location.updateExtension(self.extension)

        self._writeFile(inMemoryDataset)

        return fileDescriptor.location.pathInStore

    def toBytes(self, inMemoryDataset, fileDescriptor):
        """Write a Python object to a bytestring.

        Parameters
        ----------
        inMemoryDataset : `object`
            The Python object to store.

        Returns
        -------
        pickedDataset : `str`
            The bytestring representing the pickled dataset.
        """
        fileDescriptor.location.updateExtension(self.extension)
        return self._toBytes(inMemoryDataset)


    @classmethod
    def predictPathFromLocation(cls, location):
        """Return the path that would be returned by write, without actually
        writing.

        Parameters
        ----------
        location : `Location`
            Location of file for which path prediction is required.

        Returns
        -------
        path : `str`
            Path within datastore that would be associated with this location.
        """
        location = copy.deepcopy(location)
        location.updateExtension(cls.extension)
        return location.pathInStore

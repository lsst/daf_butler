#
# LSST Data Management System
#
# Copyright 2018  AURA/LSST.
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

"""Support for reading and writing files to a POSIX file system."""

from abc import abstractmethod

from lsst.daf.butler.core.formatter import Formatter


class FileFormatter(Formatter):
    """Interface for reading and writing files on a POSIX file system.
    """

    extension = None
    """Default file extension to use for writing files. None means that no
    modifications will be made to the supplied file extension."""

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
    def _writeFile(self, inMemoryDataset, fileDescriptor):
        """Write the in memory dataset to file on disk.

        Parameters
        ----------
        inMemoryDataset : `object`
            Object to serialize.
        fileDescriptor : `FileDescriptor`
            Details of the file to be written.

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
            StorageClass associated with `inMemoryDataset`.
        pytype : `class`, optional
            Override type to use for conversion.

        Returns
        -------
        inMemoryDataset : `object`
            Object of expected type `pytype`.
        """
        return inMemoryDataset

    def read(self, fileDescriptor):
        """Read data from a file.

        Parameters
        ----------
        fileDescriptor : `FileDescriptor`
            Identifies the file to read, type to read it into and parameters
            to be used for reading.

        Returns
        -------
        inMemoryDataset : `object`
            The requested data as a Python object. The type of object
            is controlled by the specific formatter.
        """
        # Try the file or the component version
        path = fileDescriptor.location.preferredPath()
        data = self._readFile(path, fileDescriptor.storageClass.pytype)
        name = fileDescriptor.location.fragment

        if name:
            if data is None:
                # Must be composite written as single file
                data = self._readFile(fileDescriptor.location.path, fileDescriptor.storageClass.pytype)

                # Now need to "get" the component somehow
                try:
                    data = fileDescriptor.storageClass.assembler().getComponent(data, name)
                except AttributeError:
                    # Defer the complaint
                    data = None

            else:
                # The component was written standalone
                pass
        else:
            # Not requesting a component, so already read
            pass

        data = self._coerceType(data, fileDescriptor.storageClass, pytype=fileDescriptor.pytype)

        if data is None:
            raise ValueError("Unable to read data with URI {}".format(fileDescriptor.location.uri))

        return data

    def write(self, inMemoryDataset, fileDescriptor):
        """Write a Python object to a file.

        Parameters
        ----------
        inMemoryDataset : `object`
            The Python object to store.
        fileDescriptor : `FileDescriptor`
            Identifies the file to read, type to read it into and parameters
            to be used for reading.

        Returns
        -------
        uri : `str`
            The `URI` where the primary file is stored.
        components : `dict`, optional
            A dictionary of URIs for the components.
            The latter will be empty if the object is not a composite.
        """
        # Update the location with the formatter-preferred file extension
        fileDescriptor.location.updateExtension(self.extension)

        self._writeFile(inMemoryDataset, fileDescriptor)

        # Get the list of valid components so we can build URIs
        storageClass = fileDescriptor.storageClass
        components = storageClass.assembler().getValidComponents(inMemoryDataset, storageClass)

        return (fileDescriptor.location.uri,
                {c: fileDescriptor.location.componentUri(c) for c in components})

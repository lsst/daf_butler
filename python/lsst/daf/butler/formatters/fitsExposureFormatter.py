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

import copy

from lsst.daf.butler import Formatter

__all__ = ("FitsExposureFormatter", )


class FitsExposureFormatter(Formatter):
    """Interface for reading and writing Exposures to and from FITS files.
    """
    extension = ".fits"

    parameters = frozenset(("bbox", "origin"))

    def read(self, fileDescriptor, component=None):
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
        KeyError
            Raised when parameters passed with fileDescriptor are not
            supported.
        """
        from lsst.afw.image import LOCAL, readMetadata
        from lsst.geom import Box2I, Point2I

        if component == "metadata":
            data = readMetadata(fileDescriptor.location.path)
        else:
            # If we"re reading a non-image component, just read in a
            # single-pixel image for efficiency.
            kwds = {}
            if component in ("image", "variance", "mask"):
                kwds["bbox"] = Box2I(minimum=Point2I(0, 0), maximum=Point2I(0, 0))
                kwds["origin"] = LOCAL
            elif fileDescriptor.parameters is not None:
                # Just pass parameters into kwargs for constructor, but check that we recognize them.
                kwds.update(fileDescriptor.parameters)
                if not self.parameters.issuperset(kwds.keys()):
                    raise KeyError("Unrecognized parameter key(s): {}".format(kwds.keys() - self.parameters))
            # Read the file naively
            data = fileDescriptor.storageClass.pytype(fileDescriptor.location.path, **kwds)

            # TODO: most of the rest of this method has a lot in common with
            # FileFormatter; some refactoring could probably restore that
            # inheritance relationship and remove this duplication.

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
        path : `str`
            The `URI` where the primary file is stored.
        """
        # Update the location with the formatter-preferred file extension
        fileDescriptor.location.updateExtension(self.extension)
        inMemoryDataset.writeFits(fileDescriptor.location.path)
        return fileDescriptor.location.pathInStore

    def predictPath(self, location):
        """Return the path that would be returned by write, without actually
        writing.

        location : `Location`
            The location to simulate writing to.
        """
        location = copy.deepcopy(location)
        location.updateExtension(self.extension)
        return location.pathInStore

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

    def readImageComponent(self, fileDescriptor, component):
        """Read the image, mask, or variance component of an Exposure.

        Parameters
        ----------
        fileDescriptor : `FileDescriptor`
            Identifies the file to read and parameters to be used for reading.
        component : `str`, optional
            Component to read from the file.  Always one of "image",
            "variance", or "mask".

        Returns
        -------
        image : `~lsst.afw.image.Image` or `~lsst.afw.image.Mask`
            In-memory image, variance, or mask component.
        """
        # TODO: could be made more efficient *if* Exposure type objects
        # held the class objects of their components.
        full = self.readFull(fileDescriptor, fileDescriptor.parameters)
        return fileDescriptor.storageClass.assembler().getComponent(full, component)

    def readMetadata(self, fileDescriptor):
        """Read all header metadata directly into a PropertyList.

        Parameters
        ----------
        fileDescriptor : `FileDescriptor`
            Identifies the file to read and parameters to be used for reading.

        Returns
        -------
        metadata : `~lsst.daf.base.PropertyList`
            Header metadata.
        """
        from lsst.afw.image import readMetadata
        return readMetadata(fileDescriptor.location.path)

    def stripMetadata(self, metadata):
        """Remove metadata entries that are parsed into components.

        This is only called when just the metadata is requested; stripping
        entries there forces code that wants other components to ask for those
        components directly rather than trying to extract them from the
        metadata manually, which is fragile.  This behavior is an intentional
        change from Gen2.

        Parameters
        ----------
        metadata : `~lsst.daf.base.PropertyList`
            Header metadata, to be modified in-place.
        """
        # TODO: make sure this covers everything, by delegating to something
        # that doesn't yet exist in afw.image.ExposureInfo.
        from lsst.afw.image import bboxFromMetadata
        from lsst.afw.geom import makeSkyWcs
        bboxFromMetadata(metadata)  # always strips
        makeSkyWcs(metadata, strip=True)

    def readInfoComponent(self, fileDescriptor, component):
        """Read a component held by ExposureInfo.

        Parameters
        ----------
        fileDescriptor : `FileDescriptor`
            Identifies the file to read and parameters to be used for reading.
        component : `str`, optional
            Component to read from the file.

        Returns
        -------
        obj : component-dependent
            In-memory component object.
        """
        from lsst.afw.image import LOCAL
        from lsst.geom import Box2I, Point2I
        parameters = dict(bbox=Box2I(minimum=Point2I(0, 0), maximum=Point2I(0, 0)), origin=LOCAL)
        tiny = self.readFull(fileDescriptor, parameters)
        return fileDescriptor.storageClass.assembler().getComponent(tiny, component)

    def readFull(self, fileDescriptor, parameters=None):
        """Read the full Exposure object.

        Parameters
        ----------
        fileDescriptor : `FileDescriptor`
            Identifies the file to read and parameters to be used for reading.
        parameters : `dict`, optional
            If specified a dictionary of slicing parameters that overrides
            those in ``fileDescriptor`.

        Returns
        -------
        exposure : `~lsst.afw.image.Exposure`
            Complete in-memory exposure.
        """
        if parameters is None:
            parameters = fileDescriptor.parameters
        if parameters is None:
            parameters = {}
        fileDescriptor.storageClass.validateParameters(parameters)
        return fileDescriptor.storageClass.pytype(fileDescriptor.location.path, **parameters)

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
        if fileDescriptor.readStorageClass != fileDescriptor.storageClass:
            if component == "metadata":
                md = self.readMetadata(fileDescriptor)
                self.stripMetadata(md)
                return md
            elif component in ("image", "variance", "mask"):
                return self.readImageComponent(fileDescriptor, component)
            elif component is not None:
                return self.readInfoComponent(fileDescriptor, component)
            else:
                raise ValueError("Storage class inconsistency ({} vs {}) but no"
                                 " component requested".format(fileDescriptor.readStorageClass.name,
                                                               fileDescriptor.storageClass.name))
        return self.readFull(fileDescriptor)

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

        Parameters
        ----------
        location : `Location`
            The location to simulate writing to.
        """
        location = copy.deepcopy(location)
        location.updateExtension(self.extension)
        return location.pathInStore

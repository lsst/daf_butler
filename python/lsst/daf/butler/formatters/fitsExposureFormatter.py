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

__all__ = ("FitsExposureFormatter", )

from astro_metadata_translator import fix_header
from lsst.daf.butler import Formatter
from lsst.afw.image import ExposureFitsReader


class FitsExposureFormatter(Formatter):
    """Interface for reading and writing Exposures to and from FITS files.
    """
    extension = ".fits"
    _metadata = None

    @property
    def metadata(self):
        """The metadata read from this file. It will be stripped as
        components are extracted from it
        (`lsst.daf.base.PropertyList`).
        """
        if self._metadata is None:
            self._metadata = self.readMetadata()
        return self._metadata

    def readMetadata(self):
        """Read all header metadata directly into a PropertyList.

        Returns
        -------
        metadata : `~lsst.daf.base.PropertyList`
            Header metadata.
        """
        from lsst.afw.image import readMetadata
        md = readMetadata(self.fileDescriptor.location.path)
        fix_header(md)
        return md

    def stripMetadata(self):
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
        bboxFromMetadata(self.metadata)  # always strips
        makeSkyWcs(self.metadata, strip=True)

    def readComponent(self, component, parameters=None):
        """Read a component held by the Exposure.

        Parameters
        ----------
        component : `str`, optional
            Component to read from the file.
        parameters : `dict`, optional
            If specified, a dictionary of slicing parameters that
            overrides those in ``fileDescriptor``.

        Returns
        -------
        obj : component-dependent
            In-memory component object.

        Raises
        ------
        KeyError
            Raised if the requested component cannot be handled.
        """
        componentMap = {'wcs': ('readWcs', False),
                        'coaddInputs': ('readCoaddInputs', False),
                        'psf': ('readPsf', False),
                        'image': ('readImage', True),
                        'mask': ('readMask', True),
                        'variance': ('readVariance', True),
                        }
        method, hasParams = componentMap.get(component, None)

        if method:
            reader = ExposureFitsReader(self.fileDescriptor.location.path)
            caller = getattr(reader, method, None)

            if caller:
                if parameters is None:
                    parameters = self.fileDescriptor.parameters
                if parameters is None:
                    parameters = {}
                self.fileDescriptor.storageClass.validateParameters(parameters)

                if hasParams and parameters:
                    return caller(**parameters)
                else:
                    return caller()
        else:
            raise KeyError(f"Unknown component requested: {component}")

    def readFull(self, parameters=None):
        """Read the full Exposure object.

        Parameters
        ----------
        parameters : `dict`, optional
            If specified a dictionary of slicing parameters that overrides
            those in ``fileDescriptor`.

        Returns
        -------
        exposure : `~lsst.afw.image.Exposure`
            Complete in-memory exposure.
        """
        fileDescriptor = self.fileDescriptor
        if parameters is None:
            parameters = fileDescriptor.parameters
        if parameters is None:
            parameters = {}
        fileDescriptor.storageClass.validateParameters(parameters)
        try:
            output = fileDescriptor.storageClass.pytype(fileDescriptor.location.path, **parameters)
        except TypeError:
            reader = ExposureFitsReader(fileDescriptor.location.path)
            output = reader.read(**parameters)
        return output

    def read(self, component=None, parameters=None):
        """Read data from a file.

        Parameters
        ----------
        component : `str`, optional
            Component to read from the file. Only used if the `StorageClass`
            for reading differed from the `StorageClass` used to write the
            file.
        parameters : `dict`, optional
            If specified, a dictionary of slicing parameters that
            overrides those in ``fileDescriptor``.

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
        fileDescriptor = self.fileDescriptor
        if fileDescriptor.readStorageClass != fileDescriptor.storageClass:
            if component == "metadata":
                self.stripMetadata()
                return self.metadata
            elif component is not None:
                return self.readComponent(component, parameters)
            else:
                raise ValueError("Storage class inconsistency ({} vs {}) but no"
                                 " component requested".format(fileDescriptor.readStorageClass.name,
                                                               fileDescriptor.storageClass.name))
        return self.readFull()

    def write(self, inMemoryDataset):
        """Write a Python object to a file.

        Parameters
        ----------
        inMemoryDataset : `object`
            The Python object to store.

        Returns
        -------
        path : `str`
            The `URI` where the primary file is stored.
        """
        # Update the location with the formatter-preferred file extension
        self.fileDescriptor.location.updateExtension(self.extension)
        inMemoryDataset.writeFits(self.fileDescriptor.location.path)
        return self.fileDescriptor.location.pathInStore

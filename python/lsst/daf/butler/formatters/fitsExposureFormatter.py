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

import os.path

from lsst.daf.butler.assemblers.exposureAssembler import validExposureComponents, getComponentFromExposure
from lsst.daf.butler.formatters.fileFormatter import FileFormatter


class FitsExposureFormatter(FileFormatter):
    """Interface for reading and writing Exposures to and from FITS files.
    """
    extension = ".fits"

    def _readFile(self, path, pytype):
        """Read a file from the path in FITS format.

        Parameters
        ----------
        path : `str`
            Path to use to open the file.
        pytype : `class`
            Class to use to read the FITS file.

        Returns
        -------
        data : `object`
            Instance of class `pytype` read from FITS file. None
            if the file could not be opened.
        """
        if not os.path.exists(path):
            return None

        return pytype.readFits(path)

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
        inMemoryDataset.writeFits(fileDescriptor.location.preferredPath())

    def _getComponentFromComposite(self, composite, componentName):
        """Get the component from the composite.

        This implementation uses a generic getter.

        Parameters
        ----------
        composite : `object`
            Object from which to extract component.
        componentName : `str`
            Name of component to extract.

        Returns
        -------
        component : `object`
            Extracted component.

        Raises
        ------
        AttributeError
            The specified component can not be found in `composite`.
        """
        return getComponentFromExposure(composite, componentName)

    def _getValidComponents(self, composite, storageClass):
        """Retrieve list of all components valid for this composite.

        The list of supported components is defined by the `StorageClass`.

        Parameters
        ----------
        composite : `object`
            Object from which to determine if the component exists and is
            not None.
        storageClass : `StorageClass`
            StorageClass used to define the supported components.

        Returns
        -------
        components : `dict`
            Dict of not-None components where the dict key is the name
            of the component and the value is the item extracted from the
            composite.
        """
        return validExposureComponents(composite, storageClass)

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

from lsst.daf.butler.core.formatter import Formatter


class FitsCatalogFormatter(Formatter):
    """Interface for reading and writing catalogs to and from FITS files.
    """

    def read(self, fileDescriptor):
        """Read a `Catalog` from a FITS file.

        Parameters
        ----------
        fileDescriptor : `FileDescriptor`
            Identifies the file to read, type to read it into and parameters
            to be used for reading.

        Returns
        -------
        inMemoryDataset : `Catalog`
            The requested `Catalog`.
            The actual returned type will be a derived class
            (e.g. `SourceCatalog` or `ExposureCatalog`).
        """
        return fileDescriptor.pytype.readFits(fileDescriptor.location.path)

    def write(self, inMemoryDataset, fileDescriptor):
        """Write a `Catalog` to a FITS file.

        Parameters
        ----------
        inMemoryDataset : `Catalog`
            The `Catalog` to store.
        fileDescriptor : `FileDescriptor`
            Identifies the file to read, type to read it into and parameters
            to be used for reading.

        Returns
        -------
        uri : `str`
            The `URI` where the primary `Catalog` is stored.
        components : `dict`, optional
            A dictionary of URIs for the `Catalog`' components.
            The latter will be empty if the `Catalog` is not a composite.
        """
        inMemoryDataset.writeFits(fileDescriptor.location.path)
        return fileDescriptor.location.uri, {}

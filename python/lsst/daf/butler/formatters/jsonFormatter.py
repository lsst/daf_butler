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

import builtins
import json

from lsst.daf.butler.core.composites import genericAssembler
from lsst.daf.butler.formatters.fileFormatter import FileFormatter


class JsonFormatter(FileFormatter):
    """Interface for reading and writing Python objects to and from JSON files.
    """
    extension = ".json"

    def _readFile(self, path, pytype=None):
        """Read a file from the path in JSON format.

        Parameters
        ----------
        path : `str`
            Path to use to open JSON format file.

        Returns
        -------
        data : `object`
            Either data as Python object read from JSON file, or None
            if the file could not be opened.
        """
        try:
            with open(path, "r") as fd:
                data = json.load(fd)
        except FileNotFoundError:
            data = None

        return data

    def _writeFile(self, inMemoryDataset, fileDescriptor):
        """Write the in memory dataset to file on disk.

        Will look for `_asdict()` method to aid JSON serialization, following
        the approach of the simpljson module.

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
        with open(fileDescriptor.location.preferredPath(), "w") as fd:
            if hasattr(inMemoryDataset, "_asdict"):
                inMemoryDataset = inMemoryDataset._asdict()
            json.dump(inMemoryDataset, fd)

    def _coerceType(self, inMemoryDataset, storageClass, pytype=None):
        """Coerce the supplied inMemoryDataset to type `pytype`.

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
        if not hasattr(builtins, pytype.__name__):
            inMemoryDataset = genericAssembler(storageClass, inMemoryDataset, pytype=pytype)
        return inMemoryDataset

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
        return composite[componentName]

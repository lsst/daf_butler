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

__all__ = ("PackagesFormatter",)

import os.path
from typing import Any, Optional, Type

from lsst.daf.butler.formatters.file import FileFormatter
from lsst.utils.packages import Packages


class PackagesFormatter(FileFormatter):
    """Interface for reading and writing `~lsst.utils.packages.Packages`.

    This formatter supports write parameters:

    * ``format``: The file format to use to write the package data. Allowed
      options are ``yaml``, ``json``, and ``pickle``.
    """

    supportedWriteParameters = frozenset({"format"})
    supportedExtensions = frozenset({".yaml", ".pickle", ".pkl", ".json"})

    # MyPy does't like the fact that the base declares this an instance
    # attribute while this derived class uses a property.
    @property
    def extension(self) -> str:  # type: ignore
        # Default to YAML but allow configuration via write parameter
        format = self.writeParameters.get("format", "yaml")
        ext = "." + format
        if ext not in self.supportedExtensions:
            raise RuntimeError(f"Requested file format '{format}' is not supported for Packages")
        return ext

    def _readFile(self, path: str, pytype: Optional[Type] = None) -> Any:
        """Read a file from the path.

        Parameters
        ----------
        path : `str`
            Path to use to open the file.
        pytype : `type`
            Class to use to read the serialized file.

        Returns
        -------
        data : `object`
            Instance of class ``pytype`` read from serialized file. None
            if the file could not be opened.
        """
        if not os.path.exists(path):
            return None

        assert pytype is not None
        assert issubclass(pytype, Packages)
        return pytype.read(path)

    def _fromBytes(self, serializedDataset: Any, pytype: Optional[Type] = None) -> Any:
        """Read the bytes object as a python object.

        Parameters
        ----------
        serializedDataset : `bytes`
            Bytes object to unserialize.
        pytype : `type`
            The Python type to be instantiated. Required.

        Returns
        -------
        inMemoryDataset : `object`
            The requested data as an object, or None if the string could
            not be read.
        """
        # The format can not come from the formatter configuration
        # because the current configuration has no connection to how
        # the data were stored.
        if serializedDataset.startswith(b"!<lsst."):
            format = "yaml"
        elif serializedDataset.startswith(b'{"'):
            format = "json"
        else:
            format = "pickle"

        assert pytype is not None
        assert issubclass(pytype, Packages)
        return pytype.fromBytes(serializedDataset, format)

    def _writeFile(self, inMemoryDataset: Any) -> None:
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
        inMemoryDataset.write(self.fileDescriptor.location.path)

    def _toBytes(self, inMemoryDataset: Any) -> bytes:
        """Write the in memory dataset to a bytestring.

        Parameters
        ----------
        inMemoryDataset : `object`
            Object to serialize

        Returns
        -------
        serializedDataset : `bytes`
            YAML string encoded to bytes.

        Raises
        ------
        Exception
            The object could not be serialized.
        """
        format = self.extension.lstrip(".")
        return inMemoryDataset.toBytes(format)

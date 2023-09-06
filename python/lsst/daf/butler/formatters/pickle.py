# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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

"""Formatter associated with Python pickled objects."""

from __future__ import annotations

__all__ = ("PickleFormatter",)

import pickle
from typing import Any

from .file import FileFormatter


class PickleFormatter(FileFormatter):
    """Interface for reading and writing Python objects to and from pickle
    files.
    """

    extension = ".pickle"

    unsupportedParameters = None
    """This formatter does not support any parameters"""

    def _readFile(self, path: str, pytype: type[Any] | None = None) -> Any:
        """Read a file from the path in pickle format.

        Parameters
        ----------
        path : `str`
            Path to use to open the file.
        pytype : `class`, optional
            Not used by this implementation.

        Returns
        -------
        data : `object`
            Either data as Python object read from the pickle file, or None
            if the file could not be opened.
        """
        try:
            with open(path, "rb") as fd:
                data = self._fromBytes(fd.read(), pytype)
        except FileNotFoundError:
            data = None

        return data

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
        with open(self.fileDescriptor.location.path, "wb") as fd:
            pickle.dump(inMemoryDataset, fd, protocol=-1)

    def _fromBytes(self, serializedDataset: bytes, pytype: type[Any] | None = None) -> Any:
        """Read the bytes object as a python object.

        Parameters
        ----------
        serializedDataset : `bytes`
            Bytes object to unserialize.
        pytype : `class`, optional
            Not used by this implementation.

        Returns
        -------
        inMemoryDataset : `object`
            The requested data as a object, or None if the string could
            not be read.
        """
        try:
            data = pickle.loads(serializedDataset)
        except pickle.PicklingError:
            data = None

        return data

    def _toBytes(self, inMemoryDataset: Any) -> bytes:
        """Write the in memory dataset to a bytestring.

        Parameters
        ----------
        inMemoryDataset : `object`
            Object to serialize

        Returns
        -------
        serializedDataset : `bytes`
            Bytes object representing the pickled object.

        Raises
        ------
        Exception
            The object could not be pickled.
        """
        return pickle.dumps(inMemoryDataset, protocol=-1)

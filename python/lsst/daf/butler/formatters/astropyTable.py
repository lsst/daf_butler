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

__all__ = ("AstropyTableFormatter",)

import os.path
from typing import Any

from .file import FileFormatter


class AstropyTableFormatter(FileFormatter):
    """Interface for reading and writing astropy.Table objects
    in either ECSV or FITS format.
    """

    supportedWriteParameters = frozenset({"format"})
    # Ideally we'd also support fits, but that doesn't
    # round trip string columns correctly, so things
    # need to be fixed up on read.
    supportedExtensions = frozenset(
        {
            ".ecsv",
        }
    )

    @property
    def extension(self) -> str:  # type: ignore
        # Typing is ignored above since this is a property and the
        # parent class has a class attribute

        # Default to ECSV but allow configuration via write parameter
        format = self.writeParameters.get("format", "ecsv")
        if format == "ecsv":
            return ".ecsv"
        # Other supported formats can be added here
        raise RuntimeError(f"Requested file format '{format}' is not supported for Table")

    def _readFile(self, path: str, pytype: type[Any] | None = None) -> Any:
        """Read a file from the path in a supported format format.

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
        if not os.path.exists(path) or pytype is None:
            return None

        return pytype.read(path)

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

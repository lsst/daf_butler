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

__all__ = ("ButlerLogRecordsFormatter", )

from typing import (
    Any,
    Optional,
    Type,
)

from lsst.daf.butler import ButlerLogRecords
from .json import JsonFormatter


class ButlerLogRecordsFormatter(JsonFormatter):
    """Read and write log records in JSON format.

    This is a naive implementation that treats everything as a pydantic.
    model.  In the future this may be changed to be able to read
    ``ButlerLogRecord`` one at time from the file and return a subset
    of records given some filtering parameters.
    """

    def _fromBytes(self, serializedDataset: bytes, pytype: Optional[Type[Any]] = None) -> Any:
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
            The requested data as a Python object or None if the string could
            not be read.
        """
        if pytype is None:
            pytype = ButlerLogRecords
        elif not issubclass(pytype, ButlerLogRecords):
            raise RuntimeError(f"Python type {pytype} does not seem to be a ButlerLogRecords type")
        return pytype.parse_raw(serializedDataset)

    def _toBytes(self, inMemoryDataset: Any) -> bytes:
        """Write the in memory dataset to a bytestring.

        Parameters
        ----------
        inMemoryDataset : `object`
            Object to serialize

        Returns
        -------
        serializedDataset : `bytes`
            bytes representing the serialized dataset.

        Raises
        ------
        Exception
            The object could not be serialized.
        """
        return inMemoryDataset.json().encode()

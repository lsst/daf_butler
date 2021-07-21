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

from lsst.daf.butler import ButlerLogRecords, ButlerLogRecord
from .json import JsonFormatter


class ButlerLogRecordsFormatter(JsonFormatter):
    """Read and write log records in JSON format.

    This is a naive implementation that treats everything as a pydantic.
    model.  In the future this may be changed to be able to read
    ``ButlerLogRecord`` one at time from the file and return a subset
    of records given some filtering parameters.
    """

    def _readFile(self, path: str, pytype: Optional[Type[Any]] = None) -> Any:
        """Read a file from the path in JSON format.

        Parameters
        ----------
        path : `str`
            Path to use to open JSON format file.
        pytype : `class`, optional
            Python type being read. Should be a `ButlerLogRecords` or
            subclass.

        Returns
        -------
        data : `object`
            Data as Python object read from JSON file.

        Notes
        -----
        Can read two forms of JSON log file. It can read a full JSON
        document created from `ButlerLogRecords`, or a stream of standalone
        JSON documents with a log record per line.
        """
        if pytype is None:
            pytype = ButlerLogRecords
        elif not issubclass(pytype, ButlerLogRecords):
            raise RuntimeError(f"Python type {pytype} does not seem to be a ButlerLogRecords type")

        with open(path, "r") as fd:
            first = fd.readline()
            if first.startswith("["):
                # This is a ButlerLogRecords serialization.
                all = first + fd.read()
                return pytype.parse_raw(all)

            # A stream of records with one record per line.
            if not first.startswith("{"):
                raise RuntimeError(f"Unrecognized JSON log format. First lines is '{first}'")
            records = [ButlerLogRecord.parse_raw(first)]
            for line in fd:
                if line:  # Filter out blank lines.
                    records.append(ButlerLogRecord.parse_raw(line))

            return pytype.from_records(records)

    def _fromBytes(self, serializedDataset: bytes, pytype: Optional[Type[Any]] = None) -> Any:
        """Read the bytes object as a python object.

        Parameters
        ----------
        serializedDataset : `bytes`
            Bytes object to unserialize.
        pytype : `class`, optional
            Python type being read. Should be a `ButlerLogRecords` or
            subclass.

        Returns
        -------
        inMemoryDataset : `object`
            The requested data as a Python object or None if the string could
            not be read.
        """
        # Duplicates some of the logic from readFile above.
        if pytype is None:
            pytype = ButlerLogRecords
        elif not issubclass(pytype, ButlerLogRecords):
            raise RuntimeError(f"Python type {pytype} does not seem to be a ButlerLogRecords type")

        if not serializedDataset:
            # No records to return
            return pytype.from_records([])

        if serializedDataset.startswith(b"["):
            return pytype.parse_raw(serializedDataset)

        if not serializedDataset.startswith(b"{"):
            raise RuntimeError(f"These bytes do not look like JSON -- starts with '{serializedDataset[0]}'")

        # Filter out blank lines.
        records = [ButlerLogRecord.parse_raw(line) for line in serializedDataset.split(b"\n") if line]
        return pytype.from_records(records)

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
        return inMemoryDataset.json(exclude_unset=True, exclude_defaults=True).encode()

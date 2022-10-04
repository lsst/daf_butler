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

from __future__ import annotations

__all__ = ("JsonFormatter",)

import builtins
import dataclasses
import json
from typing import TYPE_CHECKING, Any, Optional, Type

from .file import FileFormatter

if TYPE_CHECKING:
    from lsst.daf.butler import StorageClass


class JsonFormatter(FileFormatter):
    """Formatter implementation for JSON files."""

    extension = ".json"

    unsupportedParameters = None
    """This formatter does not support any parameters (`frozenset`)"""

    def _readFile(self, path: str, pytype: Optional[Type[Any]] = None) -> Any:
        """Read a file from the path in JSON format.

        Parameters
        ----------
        path : `str`
            Path to use to open JSON format file.
        pytype : `class`, optional
            Not used by this implementation.

        Returns
        -------
        data : `object`
            Data as Python object read from JSON file.
        """
        with open(path, "rb") as fd:
            data = self._fromBytes(fd.read(), pytype)

        return data

    def _writeFile(self, inMemoryDataset: Any) -> None:
        """Write the in memory dataset to file on disk.

        Will look for `_asdict()` method to aid JSON serialization, following
        the approach of the simplejson module.

        Parameters
        ----------
        inMemoryDataset : `object`
            Object to serialize.

        Raises
        ------
        Exception
            The file could not be written.
        """
        self.fileDescriptor.location.uri.write(self._toBytes(inMemoryDataset))

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
        try:
            data = json.loads(serializedDataset)
        except json.JSONDecodeError:
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
            bytes representing the serialized dataset.

        Raises
        ------
        Exception
            The object could not be serialized.
        """
        # For example, Pydantic models have a .json method so use it.
        try:
            return inMemoryDataset.json().encode()
        except AttributeError:
            pass

        if hasattr(inMemoryDataset, "_asdict"):
            inMemoryDataset = inMemoryDataset._asdict()
        return json.dumps(inMemoryDataset, ensure_ascii=False).encode()

    def _coerceType(
        self, inMemoryDataset: Any, writeStorageClass: StorageClass, readStorageClass: StorageClass
    ) -> Any:
        """Coerce the supplied inMemoryDataset to the correct python type.

        Parameters
        ----------
        inMemoryDataset : `object`
            Object to coerce to expected type.
        writeStorageClass : `StorageClass`
            Storage class used to serialize this data.
        readStorageClass : `StorageClass`
            Storage class requested as the outcome.

        Returns
        -------
        inMemoryDataset : `object`
            Object of expected type ``readStorageClass.pytype``.
        """
        if inMemoryDataset is not None and not hasattr(builtins, readStorageClass.pytype.__name__):
            if writeStorageClass.isComposite():
                # We know we must be able to assemble the written
                # storage class. Coerce later to the read type.
                inMemoryDataset = writeStorageClass.delegate().assemble(
                    inMemoryDataset, pytype=writeStorageClass.pytype
                )
            elif not isinstance(inMemoryDataset, readStorageClass.pytype):
                # JSON data are returned as simple python types.
                # The content will match the written storage class.
                # Pydantic models have their own scheme.
                try:
                    inMemoryDataset = writeStorageClass.pytype.parse_obj(inMemoryDataset)
                except AttributeError:
                    if dataclasses.is_dataclass(writeStorageClass.pytype):
                        # dataclasses accept key/value parameters.
                        inMemoryDataset = writeStorageClass.pytype(**inMemoryDataset)
                    else:
                        # Hope that we can pass the arguments in directly
                        inMemoryDataset = writeStorageClass.pytype(inMemoryDataset)
        # Coerce to the read storage class if necessary.
        return readStorageClass.coerce_type(inMemoryDataset)

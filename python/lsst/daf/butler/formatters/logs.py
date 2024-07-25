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

__all__ = ("ButlerLogRecordsFormatter",)

from typing import Any

from lsst.daf.butler import FormatterV2
from lsst.daf.butler.logging import ButlerLogRecords


class ButlerLogRecordsFormatter(FormatterV2):
    """Read and write log records in JSON format.

    This is a naive implementation that treats everything as a pydantic.
    model.  In the future this may be changed to be able to read
    `ButlerLogRecord` one at time from the file and return a subset
    of records given some filtering parameters.

    Notes
    -----
    Log files can be large and ResourcePath.open() does not support
    ``readline()`` or ``__iter__`` in all cases and
    ``ButlerLogRecords.from_stream`` does not use `.read()` for chunking.
    Therefore must use local file.
    """

    default_extension = ".json"
    supported_extensions = frozenset({".log"})
    can_read_from_local_file = True

    def _get_read_pytype(self) -> type[ButlerLogRecords]:
        """Get the Python type to allow for subclasses."""
        pytype = self.file_descriptor.storageClass.pytype
        if not issubclass(pytype, ButlerLogRecords):
            raise RuntimeError(f"Python type {pytype} does not seem to be a ButlerLogRecords type")
        return pytype

    def read_from_local_file(self, path: str, component: str | None = None, expected_size: int = -1) -> Any:
        # ResourcePath open() cannot do a per-line read so can not use
        # `read_from_stream` and `read_from_uri` does not give any advantage
        # over pre-downloading the whole file (which can be very large).
        return self._get_read_pytype().from_file(path)

    def to_bytes(self, in_memory_dataset: Any) -> bytes:
        return in_memory_dataset.model_dump_json(exclude_unset=True, exclude_defaults=True).encode()

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

from __future__ import annotations

__all__ = ("JsonFormatter",)

import contextlib
import dataclasses
import json
from typing import Any

from lsst.resources import ResourcePath

from .typeless import TypelessFormatter


class JsonFormatter(TypelessFormatter):
    """Read and write JSON files."""

    default_extension = ".json"
    unsupported_parameters = None
    can_read_from_uri = True

    def read_from_uri(self, uri: ResourcePath, component: str | None = None) -> Any:
        # json.load() reads the entire file content into memory
        # and is no different from json.loads(uri.read()). It does not attempt
        # to support incremental reading to minimize memory usage.
        # This means the JSON string always has to be read entirely into
        # memory regardless of being remote or local.
        json_bytes = uri.read()

        try:
            data = json.loads(json_bytes)
        except json.JSONDecodeError:
            data = None

        return data

    def to_bytes(self, in_memory_dataset: Any) -> bytes:
        """Write the in memory dataset to a bytestring.

        Parameters
        ----------
        in_memory_dataset : `object`
            Object to serialize.

        Returns
        -------
        serialized_dataset : `bytes`
            Bytes representing the serialized dataset.

        Raises
        ------
        Exception
            The object could not be serialized.
        """
        # Try different standardized methods for native json.
        # For example, Pydantic models have a .model_dump_json method.
        # v1 models without compatibility layer will need .json()
        with contextlib.suppress(AttributeError):
            return in_memory_dataset.model_dump_json().encode()
        with contextlib.suppress(AttributeError):
            return in_memory_dataset.json().encode()

        if dataclasses.is_dataclass(in_memory_dataset) and not isinstance(in_memory_dataset, type):
            in_memory_dataset = dataclasses.asdict(in_memory_dataset)  # type: ignore
        elif hasattr(in_memory_dataset, "_asdict"):
            in_memory_dataset = in_memory_dataset._asdict()
        return json.dumps(in_memory_dataset, ensure_ascii=False).encode()

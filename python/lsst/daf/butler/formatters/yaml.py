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

__all__ = ("YamlFormatter",)

import contextlib
import dataclasses
from typing import Any

import yaml
from lsst.resources import ResourcePath

from .typeless import TypelessFormatter


class YamlFormatter(TypelessFormatter):
    """Read and write YAML files."""

    default_extension = ".yaml"
    unsupported_parameters = None
    supported_write_parameters = frozenset({"unsafe_dump"})
    can_read_from_uri = True

    def read_from_uri(self, uri: ResourcePath, component: str | None = None, expected_size: int = -1) -> Any:
        # Can not use ResourcePath.open()
        data = yaml.safe_load(uri.read())
        return data

    def to_bytes(self, in_memory_dataset: Any) -> bytes:
        """Write the in memory dataset to a bytestring.

        Will look for `_asdict()` method to aid YAML serialization, following
        the approach of the ``simplejson`` module.  Additionally, can attempt
        to detect `pydantic.BaseModel`.

        The `dict` will be passed to the relevant constructor on read if
        not explicitly handled by Pyyaml.

        Parameters
        ----------
        in_memory_dataset : `object`
            Object to serialize.

        Returns
        -------
        serialized_dataset : `bytes`
            YAML string encoded to bytes.

        Raises
        ------
        Exception
            The object could not be serialized.

        Notes
        -----
        `~yaml.SafeDumper` is used when generating the YAML serialization.
        This will fail for data structures that have complex python classes
        without a registered YAML representer.
        """
        converted = False
        if hasattr(in_memory_dataset, "model_dump") and hasattr(in_memory_dataset, "model_dump_json"):
            # Pydantic v2-like model if both model_dump() and model_dump_json()
            # exist.
            with contextlib.suppress(Exception):
                in_memory_dataset = in_memory_dataset.model_dump()
                converted = True

        if not converted and hasattr(in_memory_dataset, "dict") and hasattr(in_memory_dataset, "json"):
            # Pydantic v1-like model if both dict() and json() exist.
            with contextlib.suppress(Exception):
                in_memory_dataset = in_memory_dataset.dict()
                converted = True

        if not converted:
            # The initial check this is not a type is to help mypy.
            if not isinstance(in_memory_dataset, type) and dataclasses.is_dataclass(in_memory_dataset):
                in_memory_dataset = dataclasses.asdict(in_memory_dataset)
            elif hasattr(in_memory_dataset, "_asdict"):
                in_memory_dataset = in_memory_dataset._asdict()

        unsafe_dump = self.write_parameters.get("unsafe_dump", False)
        # Now that Python always uses an order dict, do not sort keys
        # on write so that order can be preserved on read.
        if unsafe_dump:
            serialized = yaml.dump(in_memory_dataset, sort_keys=False)
        else:
            serialized = yaml.safe_dump(in_memory_dataset, sort_keys=False)
        return serialized.encode()

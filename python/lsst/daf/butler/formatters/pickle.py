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

from lsst.resources import ResourcePath

from .typeless import TypelessFormatter


class PickleFormatter(TypelessFormatter):
    """Interface for reading and writing Python objects to and from pickle
    files.
    """

    default_extension = ".pickle"
    unsupported_parameters = None
    can_read_from_uri = True

    def read_from_uri(self, uri: ResourcePath, component: str | None = None, expected_size: int = -1) -> Any:
        # Read the pickle file directly from the resource into memory.
        try:
            data = pickle.loads(uri.read())
        except pickle.PicklingError:
            data = None

        return data

    def to_bytes(self, in_memory_dataset: Any) -> bytes:
        return pickle.dumps(in_memory_dataset, protocol=-1)

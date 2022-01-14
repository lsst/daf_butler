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

__all__ = ["FileDataset"]

from dataclasses import dataclass
from typing import Any, List, Optional, Union

from lsst.resources import ResourcePath, ResourcePathExpression

from .datasets import DatasetRef
from .formatter import FormatterParameter


@dataclass
class FileDataset:
    """A struct that represents a dataset exported to a file."""

    __slots__ = ("refs", "path", "formatter")

    refs: List[DatasetRef]
    """Registry information about the dataset. (`list` of `DatasetRef`).
    """

    path: Union[str, ResourcePath]
    """Path to the dataset (`lsst.resources.ResourcePath` or `str`).

    If the dataset was exported with ``transfer=None`` (i.e. in-place),
    this is relative to the datastore root (only datastores that have a
    well-defined root in the local filesystem can be expected to support
    in-place exports).  Otherwise this is relative to the directory passed
    to `Datastore.export`.
    """

    formatter: Optional[FormatterParameter]
    """A `Formatter` class or fully-qualified name.
    """

    def __init__(
        self,
        path: ResourcePathExpression,
        refs: Union[DatasetRef, List[DatasetRef]],
        *,
        formatter: Optional[FormatterParameter] = None,
    ):
        # Do not want to store all possible options supported by ResourcePath
        # so force a conversion for the non-str parameters.
        self.path = path if isinstance(path, str) else ResourcePath(path, forceAbsolute=False)
        if isinstance(refs, DatasetRef):
            refs = [refs]
        self.refs = refs
        self.formatter = formatter

    def __lt__(self, other: Any) -> bool:
        # Sort on path alone
        if not isinstance(other, type(self)):
            return NotImplemented
        return str(self.path) < str(other.path)

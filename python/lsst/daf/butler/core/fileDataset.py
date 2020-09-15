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
from typing import (
    List,
    Optional,
    Union,
)


from .datasets import DatasetRef
from .formatter import FormatterParameter


@dataclass
class FileDataset:
    """A struct that represents a dataset exported to a file.
    """
    __slots__ = ("refs", "path", "formatter")

    refs: List[DatasetRef]
    """Registry information about the dataset. (`list` of `DatasetRef`).
    """

    path: str
    """Path to the dataset (`str`).

    If the dataset was exported with ``transfer=None`` (i.e. in-place),
    this is relative to the datastore root (only datastores that have a
    well-defined root in the local filesystem can be expected to support
    in-place exports).  Otherwise this is relative to the directory passed
    to `Datastore.export`.
    """

    formatter: Optional[FormatterParameter]
    """A `Formatter` class or fully-qualified name.
    """

    def __init__(self, path: str, refs: Union[DatasetRef, List[DatasetRef]], *,
                 formatter: Optional[FormatterParameter] = None):
        self.path = path
        if isinstance(refs, DatasetRef):
            refs = [refs]
        self.refs = refs
        self.formatter = formatter

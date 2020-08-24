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
#

from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import (
    List,
    Optional,
)


class FieldType(enum.IntEnum):
    INTEGER = enum.auto()
    STRING = enum.auto()
    FLOAT = enum.auto()
    HASH = enum.auto()
    BLOB = enum.auto()
    DATETIME = enum.auto()


class DependencyType(enum.IntEnum):
    REQUIRES = enum.auto()
    IMPLIES = enum.auto()
    ALIASES = enum.auto()


@dataclass
class FieldMetaRow:
    element_name: str
    field_name: str
    field_type: FieldType
    unique_set: Optional[int]
    unique_index: Optional[int]


@dataclass
class DependencyMetaRow:
    dependent_element: str
    dependency_element: str
    dependency_type: DependencyType


@dataclass
class StandardElementMetaRow:
    name: str
    key: Optional[str]
    spatial: Optional[str]
    temporal: Optional[str]


@dataclass
class SkyPixMetaRow:
    family_name: str
    pixelization_class_name: str
    max_level: int


@dataclass
class UniverseRows:
    standard: List[StandardElementMetaRow]
    skypix: List[SkyPixMetaRow]
    dependencies: List[DependencyMetaRow]
    fields: List[FieldMetaRow]

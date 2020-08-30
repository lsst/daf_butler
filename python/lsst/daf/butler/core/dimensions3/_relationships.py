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

from abc import ABC, abstractmethod
import enum
from typing import (
    Any,
    Iterable,
    Mapping,
    Tuple,
)


@enum.unique
class RelationshipCategory(enum.Enum):
    SPATIAL = enum.auto()
    TEMPORAL = enum.auto()


class RelationshipFamily(ABC):

    def __init__(
        self,
        name: str,
        category: RelationshipCategory,
    ):
        self.name = name
        self.category = category

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, RelationshipFamily):
            return self.category == other.category and self.name == other.name
        return False

    @abstractmethod
    def choose(self, endpoints: Iterable[RelationshipEndpoint]) -> RelationshipEndpoint:
        raise NotImplementedError()

    name: str
    category: RelationshipCategory


class RelationshipEndpoint(ABC):

    def __init__(self, name: str):
        self.name = name

    @property
    def families(self) -> Mapping[RelationshipCategory, RelationshipFamily]:
        return {}

    name: str


class RelationshipLink(Tuple[RelationshipEndpoint, RelationshipEndpoint]):

    def __new__(cls, first: RelationshipEndpoint, second: RelationshipEndpoint) -> RelationshipLink:
        # mypy claims __new__ needs an iterable below, but complains about
        # passing it a tuple.  Seems like a bug, so these ignores can
        # hopefully go away someday.
        if first.name < second.name:
            return super().__new__(cls, (first, second))  # type: ignore
        elif first.name > second.name:
            return super().__new__(cls, (second, first))  # type: ignore
        else:
            raise ValueError(
                f"Identical or baf vertices for edge: {first.name}, {second.name}."
            )

    @property
    def names(self) -> Tuple[str, str]:
        return (self[0].name, self[1].name)

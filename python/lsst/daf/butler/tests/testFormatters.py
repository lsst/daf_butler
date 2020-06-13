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

__all__ = ("FormatterTest", "DoNothingFormatter", "LenientYamlFormatter")

from typing import (
    TYPE_CHECKING,
    Any,
    Mapping,
    Optional,
)

from ..core import Formatter
from ..formatters.yaml import YamlFormatter

if TYPE_CHECKING:
    from ..core import Location


class DoNothingFormatter(Formatter):
    """A test formatter that does not need to format anything and has
    parameters."""

    def read(self, component: Optional[str] = None) -> Any:
        raise NotImplementedError("Type does not support reading")

    def write(self, inMemoryDataset: Any) -> str:
        raise NotImplementedError("Type does not support writing")


class FormatterTest(Formatter):
    """A test formatter that does not need to format anything."""

    supportedWriteParameters = frozenset({"min", "max", "median", "comment", "extra", "recipe"})

    def read(self, component: Optional[str] = None) -> Any:
        raise NotImplementedError("Type does not support reading")

    def write(self, inMemoryDataset: Any) -> str:
        raise NotImplementedError("Type does not support writing")

    @staticmethod
    def validateWriteRecipes(recipes: Optional[Mapping[str, Any]]) -> Optional[Mapping[str, Any]]:
        if not recipes:
            return recipes
        for recipeName in recipes:
            if "mode" not in recipes[recipeName]:
                raise RuntimeError("'mode' is a required write recipe parameter")
        return recipes


class LenientYamlFormatter(YamlFormatter):
    """A test formatter that allows any file extension but always reads and
    writes YAML."""
    extension = ".yaml"

    @classmethod
    def validateExtension(cls, location: Location) -> None:
        return

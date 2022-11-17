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

__all__ = "DeferredFormatter"

from typing import Any

from ..core import Formatter


class DeferredFormatter(Formatter):
    """A test formatter that does not need to format anything and exists
    solely to be guaranteed to be loaded once by one test.

    We want to ensure that the formatter system loads this on demand.
    """

    def read(self, component: str | None = None) -> Any:
        raise NotImplementedError("Type does not support reading")

    def write(self, inMemoryDataset: Any) -> None:
        raise NotImplementedError("Type does not support writing")

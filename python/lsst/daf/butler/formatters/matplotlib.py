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

"""Support for writing matplotlib figures."""

from __future__ import annotations

__all__ = ("MatplotlibFormatter",)

from typing import Any

from .file import FileFormatter


class MatplotlibFormatter(FileFormatter):
    """Interface for writing matplotlib figures."""

    extension = ".png"
    """Matplotlib figures are always written in PNG format."""

    def _readFile(self, path: str, pytype: type[Any] | None = None) -> Any:
        # docstring inherited from FileFormatter._readFile
        raise NotImplementedError(f"matplotlib figures cannot be read by the butler; path is {path}")

    def _writeFile(self, inMemoryDataset: Any) -> None:
        # docstring inherited from FileFormatter._writeFile
        inMemoryDataset.savefig(self.fileDescriptor.location.path)

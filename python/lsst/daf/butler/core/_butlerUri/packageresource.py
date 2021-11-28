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

import contextlib
import pkg_resources
import logging
from typing import IO, Iterator, Optional

__all__ = ('ButlerPackageResourceURI',)

from ._butlerUri import ButlerURI

log = logging.getLogger(__name__)


class ButlerPackageResourceURI(ButlerURI):
    """URI referring to a Python package resource.

    These URIs look like: ``resource://lsst.daf.butler/configs/file.yaml``
    where the network location is the Python package and the path is the
    resource name.
    """

    def exists(self) -> bool:
        """Check that the python resource exists."""
        return pkg_resources.resource_exists(self.netloc, self.relativeToPathRoot)

    def read(self, size: int = -1) -> bytes:
        """Read the contents of the resource."""
        with pkg_resources.resource_stream(self.netloc, self.relativeToPathRoot) as fh:
            return fh.read(size)

    @contextlib.contextmanager
    def open(
        self,
        mode: str = "r",
        *,
        encoding: Optional[str] = None,
        prefer_file_temporary: bool = False,
    ) -> Iterator[IO]:
        # Docstring inherited.
        if "r" not in mode or "+" in mode:
            raise RuntimeError(f"Package resource URI {self} is read-only.")
        if "b" in mode:
            with pkg_resources.resource_stream(self.netloc, self.relativeToPathRoot) as buffer:
                yield buffer
        else:
            with super().open(mode, encoding=encoding, prefer_file_temporary=prefer_file_temporary) as buffer:
                yield buffer

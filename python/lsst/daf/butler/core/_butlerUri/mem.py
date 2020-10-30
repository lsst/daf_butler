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

from typing import Tuple

__all__ = ('ButlerInMemoryURI',)

from ._butlerUri import ButlerURI


class ButlerInMemoryURI(ButlerURI):
    """Internal in-memory datastore URI (`mem://`).

    Not used for any real purpose other than indicating that the dataset
    is in memory.
    """

    def exists(self) -> bool:
        """Test for existence and always return False."""
        return True

    def as_local(self) -> Tuple[str, bool]:
        raise RuntimeError(f"Do not know how to retrieve data for URI '{self}'")

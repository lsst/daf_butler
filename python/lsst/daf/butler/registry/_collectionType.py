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

__all__ = [
    "CollectionType",
]

import enum


class CollectionType(enum.IntEnum):
    """Enumeration used to label different types of collections.
    """

    RUN = 1
    """A ``RUN`` collection (also just called a 'run') is the initial
    collection a dataset is inserted into and the only one it can never be
    removed from.

    Within a particular run, there may only be one dataset with a particular
    dataset type and data ID.
    """

    TAGGED = 2
    """Datasets can be associated with and removed from ``TAGGED`` collections
    arbitrarily.

    Within a particular tagged collection, there may only be one dataset with
    a particular dataset type and data ID.
    """

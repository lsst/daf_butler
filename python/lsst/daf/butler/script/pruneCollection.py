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

from .. import Butler


def pruneCollection(repo, collection, purge, unstore):
    """Remove a collection and possibly prune datasets within it.

    Parameters
    ----------
    repo : `str`
        Same as the ``config`` argument to ``Butler.__init__``
    collection : `str`
        Same as the ``name`` argument to ``Butler.pruneCollection``.
    purge : `bool`, optional
        Same as the ``purge`` argument to ``Butler.pruneCollection``.
    unstore: `bool`, optional
        Same as the ``unstore`` argument to ``Butler.pruneCollection``.
    """
    butler = Butler(repo, writeable=True)
    butler.pruneCollection(collection, purge, unstore)

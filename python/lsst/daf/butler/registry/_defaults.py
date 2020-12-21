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

__all__ = ("RegistryDefaults",)

from typing import Any, Optional

from ..core.utils import immutable
from .wildcards import CollectionSearch


@immutable
class RegistryDefaults:
    """A struct used to provide the default collections searched or written to
    by a `Registry` or `Butler` instance.

    Parameters
    ----------
    collections : `str` or `Iterable` [ `str` ], optional
        An expression specifying the collections to be searched (in order) when
        reading datasets.
        This may be a `str` collection name or an iterable thereof.
        See :ref:`daf_butler_collection_expressions` for more information.
        These collections are not registered automatically and must be
        manually registered before they are used by any `Registry` or `Butler`
        method, but they may be manually registered after a `Registry` or
        `Butler` is initialized with this struct.
    run : `str`, optional
        Name of the `~CollectionType.RUN` collection new datasets should be
        inserted into.  If ``collections`` is `None` and ``run`` is not `None`,
        ``collections`` will be set to ``[run]``.  If not `None`, this
        collection will automatically be registered when the default struct is
        attached to a `Registry` instance.
    """
    def __init__(self, collections: Any = None, run: Optional[str] = None):
        if collections is None:
            if run is not None:
                collections = (run,)
            else:
                collections = ()
        self.collections = CollectionSearch.fromExpression(collections)
        self.run = run

    collections: CollectionSearch
    """The collections to search by default, in order (`CollectionSearch`).
    """

    run: Optional[str]
    """Name of the run this butler writes outputs to by default (`str` or
    `None`).
    """

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

from __future__ import annotations

from ...registry import CollectionArgType, Registry
from ...registry.wildcards import CollectionWildcard


def resolve_collections(
    registry: Registry, collections: CollectionArgType | None, doomed_by: list[str] | None = None
) -> list[str] | None:
    """Resolve the ``collections`` argument used by `Registry` query
    methods into a concrete list of collection names.

    Parameters
    ----------
    registry : `Registry`
        Registry instance used to search for collections.
    collections : collection expression, optional
        See :ref:`daf_butler_collection_expressions` for more information.
    doomed_by : `list` [ `str` ], optional
        If we are unable to find collections matching the search expression,
        messages will be appended to this list explaining why.

    Returns
    -------
    collection_names : `list` [ `str` ]
        A list of collection names matching the search, or the registry's
        default collections if ``collections`` was `None`.
    """
    if collections is None:
        return list(registry.defaults.collections)

    wildcard = CollectionWildcard.from_expression(collections)
    if wildcard.patterns:
        result = list(registry.queryCollections(collections))
        if not result and doomed_by is not None:
            doomed_by.append(f"No collections found matching expression {wildcard}")
        return result
    else:
        return list(wildcard.strings)

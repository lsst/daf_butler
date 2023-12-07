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

__all__ = ("DatasetSearch",)

from functools import cached_property
from typing import Literal

from ...dimensions import DimensionGroup
from ._base import RelationBase, StringOrWildcard


class DatasetSearch(RelationBase):
    """An abstract relation that represents a query for datasets."""

    relation_type: Literal["dataset_search"] = "dataset_search"

    dataset_type: StringOrWildcard
    """The name of the type of datasets returned by the query.

    ``...`` may be used to select all dataset types with the given
    ``dimensions``.
    """

    collections: tuple[str, ...]
    """The collections to search.

    Order matters if this dataset type is later referenced by a `FindFirst`
    operation.  Collection wildcards are always resolved before being included
    in a dataset search.
    """

    dimensions: DimensionGroup
    """The dimensions of the dataset type.

    This must match the dimensions of the dataset type as already defined in
    the butler database, but this cannot generally be verified when a relation
    tree (since it requires a database query) and hence must be checked later.
    """

    @cached_property
    def available_dataset_types(self) -> frozenset[StringOrWildcard]:
        """The dataset types whose ID columns (at least) are available from
        this relation.
        """
        return frozenset({self.dataset_type})

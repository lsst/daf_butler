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

__all__ = ("DataCoordinateUpload",)

from typing import Literal

from ...dimensions import DataIdValue, DimensionGroup
from ._base import RelationBase, StringOrWildcard


class DataCoordinateUpload(RelationBase):
    """An abstract relation that represents (and holds) user-provided data
    ID values.
    """

    relation_type: Literal["data_coordinate_upload"] = "data_coordinate_upload"

    dimensions: DimensionGroup
    """The dimensions of the data IDs."""

    rows: frozenset[tuple[DataIdValue, ...]]
    """The required values of the data IDs."""

    @property
    def available_dataset_types(self) -> frozenset[StringOrWildcard]:
        """The dataset types whose ID columns (at least) are available from
        this relation.
        """
        return frozenset()

    # We probably should validate that the tuples in 'rows' have the right
    # length (len(dimensions.required)) and maybe the right types, but we might
    # switch to Arrow here before that actually matters.

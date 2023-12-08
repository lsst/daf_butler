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

__all__ = ("Relation", "DeferredValidationRelation")

from typing import Annotated, TypeAlias, Union

import pydantic

from ...pydantic_utils import DeferredValidation
from ._data_coordinate_upload import DataCoordinateUpload
from ._dataset_search import DatasetSearch
from ._dimension_join import DimensionJoin
from ._dimension_projection import DimensionProjection
from ._find_first import FindFirst
from ._materialization import Materialization
from ._ordered_slice import OrderedSlice
from ._selection import Selection

Relation: TypeAlias = Annotated[
    Union[
        DataCoordinateUpload,
        DatasetSearch,
        DimensionJoin,
        DimensionProjection,
        FindFirst,
        Materialization,
        OrderedSlice,
        Selection,
    ],
    pydantic.Field(discriminator="relation_type"),
]


DimensionJoin.model_rebuild()
DimensionProjection.model_rebuild()
FindFirst.model_rebuild()
Materialization.model_rebuild()
OrderedSlice.model_rebuild()
Selection.model_rebuild()


class DeferredValidationRelation(DeferredValidation[Relation]):
    pass

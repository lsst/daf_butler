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

__all__ = ["BasicSkyPixDimensionRecordStorage"]

from typing import TYPE_CHECKING

import sqlalchemy
from lsst.daf.relation import Calculation, ColumnExpression, Join, Relation

from ...core import (
    DataCoordinate,
    DimensionKeyColumnTag,
    DimensionRecord,
    DimensionRecordColumnTag,
    SkyPixDimension,
)
from ..interfaces import SkyPixDimensionRecordStorage

if TYPE_CHECKING:
    from .. import queries


class BasicSkyPixDimensionRecordStorage(SkyPixDimensionRecordStorage):
    """A storage implementation specialized for `SkyPixDimension` records.

    `SkyPixDimension` records are never stored in a database, but are instead
    generated-on-the-fly from a `sphgeom.Pixelization` instance.

    Parameters
    ----------
    dimension : `SkyPixDimension`
        The dimension for which this instance will simulate storage.
    """

    def __init__(self, dimension: SkyPixDimension):
        self._dimension = dimension

    @property
    def element(self) -> SkyPixDimension:
        # Docstring inherited from DimensionRecordStorage.element.
        return self._dimension

    def clearCaches(self) -> None:
        # Docstring inherited from DimensionRecordStorage.clearCaches.
        pass

    def join_results_postprocessed(self) -> bool:
        # Docstring inherited.
        return True

    def join(
        self,
        relation: Relation,
        join: Join,
        context: queries.SqlQueryContext,
    ) -> Relation:
        # Docstring inherited.
        assert join.predicate.as_trivial(), "Expected trivial join predicate for skypix relation."
        id_column = DimensionKeyColumnTag(self._dimension.name)
        assert id_column in relation.columns, "Guaranteed by QueryBuilder.make_dimension_relation."
        function_name = f"{self._dimension.name}_region"
        context.iteration_engine.functions[function_name] = self._dimension.pixelization.pixel
        calculation = Calculation(
            tag=DimensionRecordColumnTag(self._dimension.name, "region"),
            expression=ColumnExpression.function(function_name, ColumnExpression.reference(id_column)),
        )
        return calculation.apply(
            relation, preferred_engine=context.iteration_engine, transfer=True, backtrack=True
        )

    def insert(self, *records: DimensionRecord, replace: bool = False, skip_existing: bool = False) -> None:
        # Docstring inherited from DimensionRecordStorage.insert.
        raise TypeError(f"Cannot insert into SkyPix dimension {self._dimension.name}.")

    def sync(self, record: DimensionRecord, update: bool = False) -> bool:
        # Docstring inherited from DimensionRecordStorage.sync.
        raise TypeError(f"Cannot sync SkyPixdimension {self._dimension.name}.")

    def fetch_one(self, data_id: DataCoordinate, context: queries.SqlQueryContext) -> DimensionRecord:
        # Docstring inherited from DimensionRecordStorage.
        index = data_id[self._dimension.name]
        return self._dimension.RecordClass(id=index, region=self._dimension.pixelization.pixel(index))

    def digestTables(self) -> list[sqlalchemy.schema.Table]:
        # Docstring inherited from DimensionRecordStorage.digestTables.
        return []

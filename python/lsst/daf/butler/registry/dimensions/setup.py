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

__all__ = ["setupDimensionStorage"]

from typing import Dict

from sqlalchemy.sql import FromClause

from ...core.utils import NamedKeyDict
from ...core.dimensions.schema import OVERLAP_TABLE_NAME_PATTERN
from ...core import DimensionElement, DimensionUniverse, SkyPixDimension
from ..interfaces import DimensionRecordStorage, Database
from .database import DatabaseDimensionRecordStorage
from .skypix import SkyPixDimensionRecordStorage
from .caching import CachingDimensionRecordStorage


def setupDimensionStorage(db: Database,
                          universe: DimensionUniverse,
                          tables: Dict[str, FromClause]
                          ) -> NamedKeyDict[DimensionElement, DimensionRecordStorage]:
    """Construct a suite of `DimensionRecordStorage` instances for all elements
    in a `DimensionUniverse`.

    Parameters
    ----------
    db : `Database`
        Interface to the database engine namespace that will hold these
        dimension records.
    universe : `DimensionUniverse`
        The set of all dimensions for which storage instances should be
        constructed.
    tables : `dict`
        A dictionary whose keys are a superset of the keys of the dictionary
        returned by `DimensionUniverse.makeSchemaSpec`, and whose values are
        SQLAlchemy objects that represent tables or select queries.

    Returns
    -------
    storages : `NamedKeyDict`
        A dictionary mapping `DimensionElement` instances to the storage
        instances that manage their records.
    """
    result = NamedKeyDict()
    for element in universe.elements:
        if element.hasTable():
            if element.viewOf is not None:
                elementTable = tables[element.viewOf]
            else:
                elementTable = tables[element.name]
            if element.spatial:
                commonSkyPixOverlapTable = \
                    tables[OVERLAP_TABLE_NAME_PATTERN.format(element.name, universe.commonSkyPix.name)]
            else:
                commonSkyPixOverlapTable = None
            storage = DatabaseDimensionRecordStorage(db, element, elementTable=elementTable,
                                                     commonSkyPixOverlapTable=commonSkyPixOverlapTable)
        elif isinstance(element, SkyPixDimension):
            storage = SkyPixDimensionRecordStorage(element)
        else:
            storage = None
        if element.cached:
            if storage is None:
                raise RuntimeError(f"Element {element.name} is marked as cached but has no table.")
            storage = CachingDimensionRecordStorage(storage)
        result[element] = storage
    return result

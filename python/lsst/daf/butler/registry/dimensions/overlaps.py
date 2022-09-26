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

__all__ = ("CrossFamilyDimensionOverlapStorage",)

import logging
from collections.abc import Iterable

import sqlalchemy

from ...core import DatabaseDimensionElement, addDimensionForeignKey, ddl
from ..interfaces import (
    Database,
    DatabaseDimensionOverlapStorage,
    DatabaseDimensionRecordStorage,
    GovernorDimensionRecordStorage,
    StaticTablesContext,
)

_LOG = logging.getLogger(__name__)


class CrossFamilyDimensionOverlapStorage(DatabaseDimensionOverlapStorage):
    """Basic implementation of materialized overlaps between
    otherwise-unrelated dimension elements.

    New instances should be constructed by calling `initialize`, not by calling
    the constructor directly.

    Parameters
    ----------
    db : `Database`
        Interface to the underlying database engine and namespace.
    elementStorage : `tuple` [ `DatabaseDimensionRecordStorage` ]
        Storage objects for the elements this object will relate.
    governorStorage : `tuple` [ `GovernorDimensionRecordStorage` ]
        Storage objects for the governor dimensions of the elements this
        object will relate.
    summaryTable : `sqlalchemy.schema.Table`
        Table that records which combinations of governor dimension values
        have materialized overlap rows.
    overlapTable : `sqlalchemy.schema.Table`
        Table containing the actual materialized overlap rows.

    Notes
    -----
    At present, this class (like its ABC) is just a stub that creates the
    tables it will use, but does nothing else.
    """

    def __init__(
        self,
        db: Database,
        elementStorage: tuple[DatabaseDimensionRecordStorage, DatabaseDimensionRecordStorage],
        governorStorage: tuple[GovernorDimensionRecordStorage, GovernorDimensionRecordStorage],
        summaryTable: sqlalchemy.schema.Table,
        overlapTable: sqlalchemy.schema.Table,
    ):
        self._db = db
        self._elementStorage = elementStorage
        self._governorStorage = governorStorage
        self._summaryTable = summaryTable
        self._overlapTable = overlapTable

    @classmethod
    def initialize(
        cls,
        db: Database,
        elementStorage: tuple[DatabaseDimensionRecordStorage, DatabaseDimensionRecordStorage],
        governorStorage: tuple[GovernorDimensionRecordStorage, GovernorDimensionRecordStorage],
        context: StaticTablesContext | None = None,
    ) -> DatabaseDimensionOverlapStorage:
        # Docstring inherited from DatabaseDimensionOverlapStorage.
        if context is not None:
            op = context.addTable
        else:
            op = db.ensureTableExists
        elements = (elementStorage[0].element, elementStorage[1].element)
        summaryTable = op(
            cls._SUMMARY_TABLE_NAME_SPEC.format(*elements),
            cls._makeSummaryTableSpec(elements),
        )
        overlapTable = op(
            cls._OVERLAP_TABLE_NAME_SPEC.format(*elements),
            cls._makeOverlapTableSpec(elements),
        )
        return CrossFamilyDimensionOverlapStorage(
            db,
            elementStorage,
            governorStorage,
            summaryTable=summaryTable,
            overlapTable=overlapTable,
        )

    @property
    def elements(self) -> tuple[DatabaseDimensionElement, DatabaseDimensionElement]:
        # Docstring inherited from DatabaseDimensionOverlapStorage.
        return (self._elementStorage[0].element, self._elementStorage[1].element)

    def digestTables(self) -> Iterable[sqlalchemy.schema.Table]:
        # Docstring inherited from DatabaseDimensionOverlapStorage.
        return [self._summaryTable, self._overlapTable]

    _SUMMARY_TABLE_NAME_SPEC = "{0.name}_{1.name}_overlap_summary"

    @classmethod
    def _makeSummaryTableSpec(
        cls, elements: tuple[DatabaseDimensionElement, DatabaseDimensionElement]
    ) -> ddl.TableSpec:
        """Create a specification for the table that records which combinations
        of skypix dimension and governor value have materialized overlaps.

        Parameters
        ----------
        elements : `tuple` [ `DatabaseDimensionElement` ]
            Dimension elements whose overlaps are to be managed.

        Returns
        -------
        tableSpec : `ddl.TableSpec`
            Table specification.
        """
        assert elements[0].spatial is not None and elements[1].spatial is not None
        assert elements[0].spatial.governor != elements[1].spatial.governor
        tableSpec = ddl.TableSpec(fields=[])
        addDimensionForeignKey(tableSpec, elements[0].spatial.governor, primaryKey=True)
        addDimensionForeignKey(tableSpec, elements[1].spatial.governor, primaryKey=True)
        return tableSpec

    _OVERLAP_TABLE_NAME_SPEC = "{0.name}_{1.name}_overlap"

    @classmethod
    def _makeOverlapTableSpec(
        cls, elements: tuple[DatabaseDimensionElement, DatabaseDimensionElement]
    ) -> ddl.TableSpec:
        """Create a specification for the table that holds materialized
        overlap rows.

        Parameters
        ----------
        elements : `tuple` [ `DatabaseDimensionElement` ]
            Dimension elements whose overlaps are to be managed.

        Returns
        -------
        tableSpec : `ddl.TableSpec`
            Table specification.
        """
        assert elements[0].graph.required.isdisjoint(elements[1].graph.required)
        tableSpec = ddl.TableSpec(fields=[])
        # Add governor dimensions first, so they appear first in the primary
        # key; we may often (in the future, perhaps always) know these at
        # query-generation time.
        for element in elements:
            assert element.spatial is not None
            addDimensionForeignKey(tableSpec, element.spatial.governor, primaryKey=True)
        # Add remaining dimension keys.
        for element in elements:
            assert element.spatial is not None
            for dimension in element.required:
                if dimension != element.spatial.governor:
                    addDimensionForeignKey(tableSpec, dimension, primaryKey=True)
        return tableSpec

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

"""Support for reading numpy tables (structured arrays) with the Arrow
formatter.
"""
from __future__ import annotations

from typing import Any

import numpy as np
from lsst.daf.butler.formatters.parquet import ArrowNumpySchema
from lsst.utils.introspection import get_full_type_name

from .arrowtable import ArrowTableDelegate

__all__ = ["ArrowNumpyDelegate"]


class ArrowNumpyDelegate(ArrowTableDelegate):
    _datasetType = np.ndarray

    def getComponent(self, composite: np.ndarray, componentName: str) -> Any:
        """Get a component from a numpy table stored via ArrowNumpy.

        Parameters
        ----------
        composite : `~numpy.ndarray`
            Numpy table to access component.
        componentName : `str`
            Name of component to retrieve.

        Returns
        -------
        component : `object`
            The component.

        Raises
        ------
        AttributeError
            The component can not be found.
        """
        match componentName:
            case "columns":
                return list(composite.dtype.names)
            case "schema":
                return ArrowNumpySchema(composite.dtype)
            case "rowcount":
                return len(composite)

        raise AttributeError(
            f"Do not know how to retrieve component {componentName} from {get_full_type_name(composite)}"
        )

    def _getColumns(self, inMemoryDataset: np.ndarray) -> list[str]:
        return inMemoryDataset.dtype.names

    def _selectColumns(self, inMemoryDataset: np.ndarray, columns: list[str]) -> np.ndarray:
        return inMemoryDataset[columns]

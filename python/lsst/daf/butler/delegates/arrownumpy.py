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

from typing import Any, Mapping, Optional

import numpy as np
from lsst.daf.butler import StorageClassDelegate
from lsst.daf.butler.formatters.parquet import ArrowNumpySchema
from lsst.utils.introspection import get_full_type_name
from lsst.utils.iteration import ensure_iterable

__all__ = ["ArrowNumpyDelegate"]


class ArrowNumpyDelegate(StorageClassDelegate):
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
        if componentName == "columns":
            return list(composite.dtype.names)
        elif componentName == "schema":
            return ArrowNumpySchema(composite.dtype)
        elif componentName == "rowcount":
            return len(composite)
        else:
            raise AttributeError(
                f"Do not know how to retrieve component {componentName} from {get_full_type_name(composite)}"
            )

    def handleParameters(
        self, inMemoryDataset: np.ndarray, parameters: Optional[Mapping[str, Any]] = None
    ) -> Any:
        if not isinstance(inMemoryDataset, np.ndarray):
            raise ValueError(
                "handleParameters for an Arrow Numpy Table must get an Numpy table, "
                f"not {get_full_type_name(inMemoryDataset)}."
            )

        if parameters is None:
            return inMemoryDataset

        if "columns" in parameters:
            read_columns = list(ensure_iterable(parameters["columns"]))
            for column in read_columns:
                if not isinstance(column, str):
                    raise NotImplementedError(
                        "InMemoryDataset of an Arrow Astropy Table only supports string column names."
                    )
                if column not in inMemoryDataset.dtype.names:
                    raise ValueError(f"Unrecognized column name {column!r}.")

            return inMemoryDataset[read_columns]
        else:
            return inMemoryDataset

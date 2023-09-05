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

"""Support for reading dictionaries of numpy arrays with the Arrow
formatter.
"""
from __future__ import annotations

from typing import Any

import numpy as np
from lsst.daf.butler.formatters.parquet import ArrowNumpySchema, _numpy_dict_to_dtype
from lsst.utils.introspection import get_full_type_name

from .arrowtable import ArrowTableDelegate

__all__ = ["ArrowNumpyDictDelegate"]


class ArrowNumpyDictDelegate(ArrowTableDelegate):
    """Delegate that understands the ``ArrowNumpyDict`` storage class."""

    _datasetType = dict

    def getComponent(self, composite: dict[str, np.ndarray], componentName: str) -> Any:
        """Get a component from a dict of numpy arrays stored via
        ArrowNumpyDict.

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
                return list(composite.keys())
            case "schema":
                dtype, _ = _numpy_dict_to_dtype(composite)
                return ArrowNumpySchema(dtype)
            case "rowcount":
                return len(composite[list(composite.keys())[0]])

        raise AttributeError(
            f"Do not know how to retrieve component {componentName} from {get_full_type_name(composite)}"
        )

    def _getColumns(self, inMemoryDataset: dict[str, np.ndarray]) -> list[str]:
        return list(inMemoryDataset.keys())

    def _selectColumns(
        self, inMemoryDataset: dict[str, np.ndarray], columns: list[str]
    ) -> dict[str, np.ndarray]:
        return {column: inMemoryDataset[column] for column in columns}

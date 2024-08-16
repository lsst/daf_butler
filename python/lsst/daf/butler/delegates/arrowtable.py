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

"""Support for reading Arrow tables."""
from __future__ import annotations

__all__ = ["ArrowTableDelegate"]

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

import pyarrow as pa
from lsst.daf.butler import StorageClassDelegate
from lsst.utils.introspection import get_full_type_name
from lsst.utils.iteration import ensure_iterable

if TYPE_CHECKING:
    import pandas


class ArrowTableDelegate(StorageClassDelegate):
    """Delegate that understands ArrowTable and related storage classes."""

    def can_accept(self, inMemoryDataset: Any) -> bool:
        # Docstring inherited.
        return _checkArrowCompatibleType(inMemoryDataset) is not None

    def getComponent(self, composite: Any, componentName: str) -> Any:
        """Get a component from an Arrow table or equivalent.

        Parameters
        ----------
        composite : `~pyarrow.Table`
            Arrow table to access component.
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
        typeString = _checkArrowCompatibleType(composite)

        if typeString is None:
            raise ValueError(f"Unsupported composite type {get_full_type_name(composite)}")

        if componentName == "columns":
            if typeString == "arrow":
                return composite.schema
            elif typeString == "astropy":
                return list(composite.columns.keys())
            elif typeString == "numpy":
                return list(composite.dtype.names)
            elif typeString == "numpydict":
                return list(composite.keys())
            elif typeString == "pandas":
                import pandas

                if isinstance(composite.columns, pandas.MultiIndex):
                    return composite.columns
                else:
                    return pandas.Index(self._getAllDataframeColumns(composite))

        elif componentName == "schema":
            if typeString == "arrow":
                return composite.schema
            elif typeString == "astropy":
                from lsst.daf.butler.formatters.parquet import ArrowAstropySchema

                return ArrowAstropySchema(composite)
            elif typeString == "numpy":
                from lsst.daf.butler.formatters.parquet import ArrowNumpySchema

                return ArrowNumpySchema(composite.dtype)
            elif typeString == "numpydict":
                from lsst.daf.butler.formatters.parquet import ArrowNumpySchema, _numpy_dict_to_dtype

                dtype, _ = _numpy_dict_to_dtype(composite)
                return ArrowNumpySchema(dtype)
            elif typeString == "pandas":
                from lsst.daf.butler.formatters.parquet import DataFrameSchema

                return DataFrameSchema(composite.iloc[:0])
        elif componentName == "rowcount":
            if typeString == "arrow":
                return len(composite[composite.schema.names[0]])
            elif typeString in ["astropy", "numpy", "pandas"]:
                return len(composite)
            elif typeString == "numpydict":
                return len(composite[list(composite.keys())[0]])

        raise AttributeError(
            f"Do not know how to retrieve component {componentName} from {get_full_type_name(composite)}"
        )

    def handleParameters(self, inMemoryDataset: Any, parameters: Mapping[str, Any] | None = None) -> Any:
        typeString = _checkArrowCompatibleType(inMemoryDataset)

        if typeString is None:
            raise ValueError(f"Unsupported inMemoryDataset type {get_full_type_name(inMemoryDataset)}")

        if parameters is None:
            return inMemoryDataset

        if "columns" in parameters:
            readColumns = list(ensure_iterable(parameters["columns"]))

            if typeString == "arrow":
                allColumns = inMemoryDataset.schema.names
            elif typeString == "astropy":
                allColumns = inMemoryDataset.columns.keys()
            elif typeString == "numpy":
                allColumns = inMemoryDataset.dtype.names
            elif typeString == "numpydict":
                allColumns = list(inMemoryDataset.keys())
            elif typeString == "pandas":
                import pandas

                allColumns = self._getAllDataframeColumns(inMemoryDataset)

            if typeString == "pandas" and isinstance(inMemoryDataset.columns, pandas.MultiIndex):
                from ..formatters.parquet import _standardize_multi_index_columns

                # We have a multi-index dataframe which needs special
                # handling.
                readColumns = _standardize_multi_index_columns(
                    inMemoryDataset.columns,
                    parameters["columns"],
                    stringify=False,
                )
            else:
                readColumns = list(ensure_iterable(parameters["columns"]))

                for column in readColumns:
                    if not isinstance(column, str):
                        raise NotImplementedError(
                            f"InMemoryDataset of a {get_full_type_name(inMemoryDataset)} only "
                            "supports string column names."
                        )
                    if column not in allColumns:
                        raise ValueError(f"Unrecognized column name {column!r}.")

                if typeString == "pandas":
                    # Exclude index columns from the subset.
                    readColumns = [
                        name
                        for name in ensure_iterable(parameters["columns"])
                        if name not in inMemoryDataset.index.names
                    ]

            # Ensure uniqueness, keeping order.
            readColumns = list(dict.fromkeys(readColumns))

            if typeString == "arrow":
                return inMemoryDataset.select(readColumns)
            elif typeString in ("astropy", "numpy", "pandas"):
                return inMemoryDataset[readColumns]
            elif typeString == "numpydict":
                return {column: inMemoryDataset[column] for column in readColumns}
        else:
            return inMemoryDataset

    def _getAllDataframeColumns(self, dataset: pandas.DataFrame) -> list[str]:
        """Get all columns, including index columns.

        Returns
        -------
        columns : `list` [`str`]
            List of all columns.
        """
        allColumns = list(dataset.columns)
        if dataset.index.names[0] is not None:
            allColumns.extend(dataset.index.names)

        return allColumns


def _checkArrowCompatibleType(dataset: Any) -> str | None:
    """Check a dataset for arrow compatiblity and return type string.

    Parameters
    ----------
    dataset : `object`
        Dataset object.

    Returns
    -------
    typeString : `str`
        Type string will be ``arrow`` or ``astropy`` or ``numpy`` or ``pandas``
        or "numpydict".
    """
    import numpy as np
    from astropy.table import Table as astropyTable

    if isinstance(dataset, pa.Table):
        return "arrow"
    elif isinstance(dataset, astropyTable):
        return "astropy"
    elif isinstance(dataset, np.ndarray):
        return "numpy"
    elif isinstance(dataset, dict):
        for key, item in dataset.items():
            if not isinstance(item, np.ndarray):
                # This is some other sort of dictionary.
                return None
        return "numpydict"
    elif hasattr(dataset, "to_parquet"):
        # This may be a pandas DataFrame
        try:
            import pandas
        except ImportError:
            pandas = None

        if pandas is not None and isinstance(dataset, pandas.DataFrame):
            return "pandas"

    return None

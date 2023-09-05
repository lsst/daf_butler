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

"""Support for reading DataFrames."""
from __future__ import annotations

import collections.abc
from collections.abc import Mapping
from typing import Any

import pandas
from lsst.daf.butler import StorageClassDelegate
from lsst.daf.butler.formatters.parquet import DataFrameSchema
from lsst.utils.introspection import get_full_type_name
from lsst.utils.iteration import ensure_iterable

from ..formatters.parquet import _standardize_multi_index_columns

__all__ = ["DataFrameDelegate"]


class DataFrameDelegate(StorageClassDelegate):
    """Delegate that understands the ``DataFrame`` storage class."""

    def getComponent(self, composite: pandas.DataFrame, componentName: str) -> Any:
        """Get a component from a DataFrame.

        Parameters
        ----------
        composite : `~pandas.DataFrame`
            ``DataFrame`` to access component.
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
            if isinstance(composite.columns, pandas.MultiIndex):
                return composite.columns
            else:
                return pandas.Index(self._getAllColumns(composite))
        elif componentName == "rowcount":
            return len(composite)
        elif componentName == "schema":
            return DataFrameSchema(composite.iloc[:0])
        else:
            raise AttributeError(
                f"Do not know how to retrieve component {componentName} from {get_full_type_name(composite)}"
            )

    def handleParameters(
        self, inMemoryDataset: pandas.DataFrame, parameters: Mapping[str, Any] | None = None
    ) -> Any:
        """Return possibly new in-memory dataset using the supplied parameters.

        Parameters
        ----------
        inMemoryDataset : `object`
            Object to modify based on the parameters.
        parameters : `dict`, optional
            Parameters to apply. Values are specific to the parameter.
            Supported parameters are defined in the associated
            `StorageClass`.  If no relevant parameters are specified the
            ``inMemoryDataset`` will be return unchanged.

        Returns
        -------
        inMemoryDataset : `object`
            Original in-memory dataset, or updated form after parameters
            have been used.
        """
        if not isinstance(inMemoryDataset, pandas.DataFrame):
            raise ValueError(
                "handleParameters for a DataFrame must get a DataFrame, "
                f"not {get_full_type_name(inMemoryDataset)}."
            )

        if parameters is None:
            return inMemoryDataset

        if "columns" in parameters:
            allColumns = self._getAllColumns(inMemoryDataset)

            if not isinstance(parameters["columns"], collections.abc.Iterable):
                raise NotImplementedError(
                    "InMemoryDataset of a DataFrame only supports list/tuple of string column names"
                )

            if isinstance(inMemoryDataset.columns, pandas.MultiIndex):
                # We have a multi-index dataframe which needs special handling.
                readColumns = _standardize_multi_index_columns(
                    inMemoryDataset.columns,
                    parameters["columns"],
                    stringify=False,
                )
            else:
                for column in ensure_iterable(parameters["columns"]):
                    if not isinstance(column, str):
                        raise NotImplementedError(
                            "InMemoryDataset of a DataFrame only supports string column names."
                        )
                    if column not in allColumns:
                        raise ValueError(f"Unrecognized column name {column!r}.")

                # Exclude index columns from the subset.
                readColumns = [
                    name
                    for name in ensure_iterable(parameters["columns"])
                    if name not in inMemoryDataset.index.names
                ]

            # Ensure uniqueness, keeping order.
            readColumns = list(dict.fromkeys(readColumns))

            return inMemoryDataset[readColumns]
        else:
            return inMemoryDataset

    def _getAllColumns(self, inMemoryDataset: pandas.DataFrame) -> list[str]:
        """Get all columns, including index columns.

        Returns
        -------
        columns : `list` [`str`]
            List of all columns.
        """
        allColumns = list(inMemoryDataset.columns)
        if inMemoryDataset.index.names[0] is not None:
            allColumns.extend(inMemoryDataset.index.names)

        return allColumns

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

__all__ = ("ParquetFormatter",)

import collections.abc
import itertools
import json
import re
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Union

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from lsst.daf.butler import Formatter
from lsst.utils.iteration import ensure_iterable


class _ParquetLoader:
    """Helper class for loading Parquet files into `pandas.DataFrame`
    instances.

    Parameters
    ----------
    path : `str`
        Full path to the file to be loaded.
    """

    def __init__(self, path: str):
        self.file = pq.ParquetFile(path)
        self.md = json.loads(self.file.metadata.metadata[b"pandas"])
        indexes = self.md["column_indexes"]
        if len(indexes) <= 1:
            self.columns = pd.Index(
                name for name in self.file.metadata.schema.names if not name.startswith("__")
            )
        else:
            raw_columns = list(self._splitColumnnNames(len(indexes), self.file.metadata.schema.names))
            self.columns = pd.MultiIndex.from_tuples(raw_columns, names=[f["name"] for f in indexes])
        self.indexLevelNames = tuple(self.columns.names)

    @staticmethod
    def _splitColumnnNames(n: int, names: Iterable[str]) -> Iterator[Sequence[str]]:
        """Split a string that represents a multi-index column.

        PyArrow maps Pandas' multi-index column names (which are tuples in
        Pythons) to flat strings on disk.  This routine exists to
        reconstruct the original tuple.

        Parameters
        ----------
        n : `int`
            Number of levels in the `pd.MultiIndex` that is being
            reconstructed.
        names : `~collections.abc.Iterable` of `str`
            Strings to be split.

        Yields
        ------
        tuple : `tuple` of `str`
            A multi-index column name tuple.
        """
        pattern = re.compile(r"\({}\)".format(", ".join(["'(.*)'"] * n)))
        for name in names:
            m = re.search(pattern, name)
            if m is not None:
                yield m.groups()

    def _standardizeColumnParameter(
        self, columns: Union[List[str], List[tuple], Dict[str, Union[str, List[str]]]]
    ) -> Iterator[str]:
        """Transform a dictionary index into a multi-index column into a
        string directly understandable by PyArrow.

        Parameters
        ----------
        columns : `dict`
            Dictionary whose elements are string multi-index level names
            and whose values are the value or values (as a list) for that
            level.

        Yields
        ------
        name : `str`
            Stringified tuple representing a multi-index column name.
        """
        if isinstance(columns, list):
            for requested in columns:
                if not isinstance(requested, tuple):
                    raise ValueError(
                        "columns parameter for multi-index data frame"
                        "must be either a dictionary or list of tuples."
                    )
                yield str(requested)
        else:
            if not isinstance(columns, collections.abc.Mapping):
                raise ValueError(
                    "columns parameter for multi-index data frame"
                    "must be either a dictionary or list of tuples."
                )
            if not set(self.indexLevelNames).issuperset(columns.keys()):
                raise ValueError(
                    f"Cannot use dict with keys {set(columns.keys())} "
                    f"to select columns from {self.indexLevelNames}."
                )
            factors = [
                ensure_iterable(columns.get(level, self.columns.levels[i]))
                for i, level in enumerate(self.indexLevelNames)
            ]
            for requested in itertools.product(*factors):
                for i, value in enumerate(requested):
                    if value not in self.columns.levels[i]:
                        raise ValueError(
                            f"Unrecognized value {value!r} for index {self.indexLevelNames[i]!r}."
                        )
                yield str(requested)

    def read(
        self, columns: Union[str, List[str], List[tuple], Dict[str, Union[str, List[str]]]] = None
    ) -> pd.DataFrame:
        """Read some or all of the Parquet file into a `pandas.DataFrame`
        instance.

        Parameters
        ----------
        columns:  : `dict`, `list`, or `str`, optional
            A description of the columns to be loaded.  See
            :ref:`lsst.daf.butler-concrete_storage_classes_dataframe`.

        Returns
        -------
        df : `pandas.DataFrame`
            A Pandas DataFrame.
        """
        if columns is None:
            return self.file.read(use_pandas_metadata=True).to_pandas()
        elif isinstance(self.columns, pd.MultiIndex):
            assert isinstance(columns, dict) or isinstance(columns, list)
            columns = list(self._standardizeColumnParameter(columns))
        else:
            for column in columns:
                if column not in self.columns:
                    raise ValueError(f"Unrecognized column name {column!r}.")
        return self.file.read(columns=columns, use_pandas_metadata=True).to_pandas()


def _writeParquet(path: str, inMemoryDataset: pd.DataFrame) -> None:
    """Write a `pandas.DataFrame` instance as a Parquet file."""
    table = pa.Table.from_pandas(inMemoryDataset)
    pq.write_table(table, path)


class ParquetFormatter(Formatter):
    """Interface for reading and writing Pandas DataFrames to and from Parquet
    files.

    This formatter is for the
    :ref:`lsst.daf.butler-concrete_storage_classes_dataframe` StorageClass.
    """

    extension = ".parq"

    def read(self, component: Optional[str] = None) -> Any:
        # Docstring inherited from Formatter.read.
        loader = _ParquetLoader(self.fileDescriptor.location.path)
        if component == "columns":
            return loader.columns

        if not self.fileDescriptor.parameters:
            return loader.read()

        return loader.read(**self.fileDescriptor.parameters)

    def write(self, inMemoryDataset: Any) -> None:
        # Docstring inherited from Formatter.write.
        location = self.makeUpdatedLocation(self.fileDescriptor.location)
        _writeParquet(location.path, inMemoryDataset)

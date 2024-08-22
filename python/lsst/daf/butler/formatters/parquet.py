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

__all__ = (
    "ParquetFormatter",
    "arrow_to_pandas",
    "arrow_to_astropy",
    "arrow_to_numpy",
    "arrow_to_numpy_dict",
    "pandas_to_arrow",
    "pandas_to_astropy",
    "astropy_to_arrow",
    "astropy_to_pandas",
    "numpy_to_arrow",
    "numpy_to_astropy",
    "numpy_dict_to_arrow",
    "arrow_schema_to_pandas_index",
    "DataFrameSchema",
    "ArrowAstropySchema",
    "ArrowNumpySchema",
    "compute_row_group_size",
)

import collections.abc
import itertools
import json
import logging
import re
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any, cast

import pyarrow as pa
import pyarrow.parquet as pq
from lsst.daf.butler import FormatterV2
from lsst.daf.butler.delegates.arrowtable import _checkArrowCompatibleType
from lsst.resources import ResourcePath
from lsst.utils.introspection import get_full_type_name
from lsst.utils.iteration import ensure_iterable

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    import astropy.table as atable
    import numpy as np
    import pandas as pd

    try:
        import fsspec
        from fsspec.spec import AbstractFileSystem
    except ImportError:
        fsspec = None
        AbstractFileSystem = type

TARGET_ROW_GROUP_BYTES = 1_000_000_000


class ParquetFormatter(FormatterV2):
    """Interface for reading and writing Arrow Table objects to and from
    Parquet files.
    """

    default_extension = ".parq"
    can_read_from_uri = True
    can_read_from_local_file = True

    def can_accept(self, in_memory_dataset: Any) -> bool:
        # Docstring inherited.
        return _checkArrowCompatibleType(in_memory_dataset) is not None

    def read_from_uri(self, uri: ResourcePath, component: str | None = None, expected_size: int = -1) -> Any:
        # Docstring inherited from Formatter.read.
        try:
            fs, path = uri.to_fsspec()
        except ImportError:
            log.debug("fsspec not available; falling back to local file access.")
            # This signals to the formatter to use the read_from_local_file
            # code path.
            return NotImplemented

        return self._read_parquet(path=path, fs=fs, component=component, expected_size=expected_size)

    def read_from_local_file(self, path: str, component: str | None = None, expected_size: int = -1) -> Any:
        # Docstring inherited from Formatter.read.
        return self._read_parquet(path=path, component=component, expected_size=expected_size)

    def _read_parquet(
        self,
        path: str,
        fs: AbstractFileSystem | None = None,
        component: str | None = None,
        expected_size: int = -1,
    ) -> Any:
        schema = pq.read_schema(path, filesystem=fs)

        schema_names = ["ArrowSchema", "DataFrameSchema", "ArrowAstropySchema", "ArrowNumpySchema"]

        if component in ("columns", "schema") or self.file_descriptor.readStorageClass.name in schema_names:
            # The schema will be translated to column format
            # depending on the input type.
            return schema
        elif component == "rowcount":
            # Get the rowcount from the metadata if possible, otherwise count.
            if b"lsst::arrow::rowcount" in schema.metadata:
                return int(schema.metadata[b"lsst::arrow::rowcount"])

            temp_table = pq.read_table(
                path,
                filesystem=fs,
                columns=[schema.names[0]],
                use_threads=False,
                use_pandas_metadata=False,
            )

            return len(temp_table[schema.names[0]])

        par_columns = None
        if self.file_descriptor.parameters:
            par_columns = self.file_descriptor.parameters.pop("columns", None)
            if par_columns:
                has_pandas_multi_index = False
                if b"pandas" in schema.metadata:
                    md = json.loads(schema.metadata[b"pandas"])
                    if len(md["column_indexes"]) > 1:
                        has_pandas_multi_index = True

                if not has_pandas_multi_index:
                    # Ensure uniqueness, keeping order.
                    par_columns = list(dict.fromkeys(ensure_iterable(par_columns)))
                    file_columns = [name for name in schema.names if not name.startswith("__")]

                    for par_column in par_columns:
                        if par_column not in file_columns:
                            raise ValueError(
                                f"Column {par_column} specified in parameters not available in parquet file."
                            )
                else:
                    par_columns = _standardize_multi_index_columns(
                        arrow_schema_to_pandas_index(schema),
                        par_columns,
                    )

            if len(self.file_descriptor.parameters):
                raise ValueError(
                    f"Unsupported parameters {self.file_descriptor.parameters} in ArrowTable read."
                )

        metadata = schema.metadata if schema.metadata is not None else {}
        arrow_table = pq.read_table(
            path,
            filesystem=fs,
            columns=par_columns,
            use_threads=False,
            use_pandas_metadata=(b"pandas" in metadata),
        )

        return arrow_table

    def write_local_file(self, in_memory_dataset: Any, uri: ResourcePath) -> None:

        if isinstance(in_memory_dataset, pa.Schema):
            pq.write_metadata(in_memory_dataset, uri.ospath)
            return

        type_string = _checkArrowCompatibleType(in_memory_dataset)

        if type_string is None:
            raise ValueError(
                f"Unsupported type {get_full_type_name(in_memory_dataset)} of "
                "inMemoryDataset for ParquetFormatter."
            )

        if type_string == "arrow":
            arrow_table = in_memory_dataset
        elif type_string == "astropy":
            arrow_table = astropy_to_arrow(in_memory_dataset)
        elif type_string == "numpy":
            arrow_table = numpy_to_arrow(in_memory_dataset)
        elif type_string == "numpydict":
            arrow_table = numpy_dict_to_arrow(in_memory_dataset)
        else:
            arrow_table = pandas_to_arrow(in_memory_dataset)

        row_group_size = compute_row_group_size(arrow_table.schema)

        pq.write_table(arrow_table, uri.ospath, row_group_size=row_group_size)


def arrow_to_pandas(arrow_table: pa.Table) -> pd.DataFrame:
    """Convert a pyarrow table to a pandas DataFrame.

    Parameters
    ----------
    arrow_table : `pyarrow.Table`
        Input arrow table to convert. If the table has ``pandas`` metadata
        in the schema it will be used in the construction of the
        ``DataFrame``.

    Returns
    -------
    dataframe : `pandas.DataFrame`
        Converted pandas dataframe.
    """
    return arrow_table.to_pandas(use_threads=False, integer_object_nulls=True)


def arrow_to_astropy(arrow_table: pa.Table) -> atable.Table:
    """Convert a pyarrow table to an `astropy.Table`.

    Parameters
    ----------
    arrow_table : `pyarrow.Table`
        Input arrow table to convert. If the table has astropy unit
        metadata in the schema it will be used in the construction
        of the ``astropy.Table``.

    Returns
    -------
    table : `astropy.Table`
        Converted astropy table.
    """
    from astropy.table import Table

    astropy_table = Table(arrow_to_numpy_dict(arrow_table))

    _apply_astropy_metadata(astropy_table, arrow_table.schema)

    return astropy_table


def arrow_to_numpy(arrow_table: pa.Table) -> np.ndarray | np.ma.MaskedArray:
    """Convert a pyarrow table to a structured numpy array.

    Parameters
    ----------
    arrow_table : `pyarrow.Table`
        Input arrow table.

    Returns
    -------
    array : `numpy.ndarray` or `numpy.ma.MaskedArray` (N,)
        Numpy array table with N rows and the same column names
        as the input arrow table. Will be masked records if any values
        in the table are null.
    """
    import numpy as np

    numpy_dict = arrow_to_numpy_dict(arrow_table)

    has_mask = False
    dtype = []
    for name, col in numpy_dict.items():
        if len(shape := numpy_dict[name].shape) <= 1:
            dtype.append((name, col.dtype))
        else:
            dtype.append((name, (col.dtype, shape[1:])))

        if not has_mask and isinstance(col, np.ma.MaskedArray):
            has_mask = True

    if has_mask:
        array = np.ma.mrecords.fromarrays(numpy_dict.values(), dtype=dtype)
    else:
        array = np.rec.fromarrays(numpy_dict.values(), dtype=dtype)
    return array


def arrow_to_numpy_dict(arrow_table: pa.Table) -> dict[str, np.ndarray]:
    """Convert a pyarrow table to a dict of numpy arrays.

    Parameters
    ----------
    arrow_table : `pyarrow.Table`
        Input arrow table.

    Returns
    -------
    numpy_dict : `dict` [`str`, `numpy.ndarray`]
        Dict with keys as the column names, values as the arrays.
    """
    import numpy as np

    schema = arrow_table.schema
    metadata = schema.metadata if schema.metadata is not None else {}

    numpy_dict = {}

    for name in schema.names:
        t = schema.field(name).type

        if arrow_table[name].null_count == 0:
            # Regular non-masked column
            col = arrow_table[name].to_numpy()
        else:
            # For a masked column, we need to ask arrow to fill the null
            # values with an appropriately typed value before conversion.
            # Then we apply the mask to get a masked array of the correct type.
            null_value: Any
            match t:
                case t if t in (pa.float64(), pa.float32(), pa.float16()):
                    null_value = np.nan
                case t if t in (pa.int64(), pa.int32(), pa.int16(), pa.int8()):
                    null_value = -1
                case t if t in (pa.bool_(),):
                    null_value = True
                case t if t in (pa.string(), pa.binary()):
                    null_value = ""
                case _:
                    # This is the fallback for unsigned ints in particular.
                    null_value = 0

            col = np.ma.masked_array(
                data=arrow_table[name].fill_null(null_value).to_numpy(),
                mask=arrow_table[name].is_null().to_numpy(),
                fill_value=null_value,
            )

        if t in (pa.string(), pa.binary()):
            col = col.astype(_arrow_string_to_numpy_dtype(schema, name, col))
        elif isinstance(t, pa.FixedSizeListType):
            if len(col) > 0:
                col = np.stack(col)
            else:
                # this is an empty column, and needs to be coerced to type.
                col = col.astype(t.value_type.to_pandas_dtype())

            shape = _multidim_shape_from_metadata(metadata, t.list_size, name)
            col = col.reshape((len(arrow_table), *shape))

        numpy_dict[name] = col

    return numpy_dict


def _numpy_dict_to_numpy(numpy_dict: dict[str, np.ndarray]) -> np.ndarray:
    """Convert a dict of numpy arrays to a structured numpy array.

    Parameters
    ----------
    numpy_dict : `dict` [`str`, `numpy.ndarray`]
        Dict with keys as the column names, values as the arrays.

    Returns
    -------
    array : `numpy.ndarray` (N,)
        Numpy array table with N rows and columns names from the dict keys.
    """
    return arrow_to_numpy(numpy_dict_to_arrow(numpy_dict))


def _numpy_to_numpy_dict(np_array: np.ndarray) -> dict[str, np.ndarray]:
    """Convert a structured numpy array to a dict of numpy arrays.

    Parameters
    ----------
    np_array : `numpy.ndarray`
        Input numpy array with multiple fields.

    Returns
    -------
    numpy_dict : `dict` [`str`, `numpy.ndarray`]
        Dict with keys as the column names, values as the arrays.
    """
    return arrow_to_numpy_dict(numpy_to_arrow(np_array))


def numpy_to_arrow(np_array: np.ndarray) -> pa.Table:
    """Convert a numpy array table to an arrow table.

    Parameters
    ----------
    np_array : `numpy.ndarray`
        Input numpy array with multiple fields.

    Returns
    -------
    arrow_table : `pyarrow.Table`
        Converted arrow table.
    """
    type_list = _numpy_dtype_to_arrow_types(np_array.dtype)

    md = {}
    md[b"lsst::arrow::rowcount"] = str(len(np_array))

    for name in np_array.dtype.names:
        _append_numpy_string_metadata(md, name, np_array.dtype[name])
        _append_numpy_multidim_metadata(md, name, np_array.dtype[name])

    schema = pa.schema(type_list, metadata=md)

    arrays = _numpy_style_arrays_to_arrow_arrays(
        np_array.dtype,
        len(np_array),
        np_array,
        schema,
    )

    arrow_table = pa.Table.from_arrays(arrays, schema=schema)

    return arrow_table


def numpy_dict_to_arrow(numpy_dict: dict[str, np.ndarray]) -> pa.Table:
    """Convert a dict of numpy arrays to an arrow table.

    Parameters
    ----------
    numpy_dict : `dict` [`str`, `numpy.ndarray`]
        Dict with keys as the column names, values as the arrays.

    Returns
    -------
    arrow_table : `pyarrow.Table`
        Converted arrow table.

    Raises
    ------
    ValueError if columns in numpy_dict have unequal numbers of rows.
    """
    dtype, rowcount = _numpy_dict_to_dtype(numpy_dict)
    type_list = _numpy_dtype_to_arrow_types(dtype)

    md = {}
    md[b"lsst::arrow::rowcount"] = str(rowcount)

    if dtype.names is not None:
        for name in dtype.names:
            _append_numpy_string_metadata(md, name, dtype[name])
            _append_numpy_multidim_metadata(md, name, dtype[name])

    schema = pa.schema(type_list, metadata=md)

    arrays = _numpy_style_arrays_to_arrow_arrays(
        dtype,
        rowcount,
        numpy_dict,
        schema,
    )

    arrow_table = pa.Table.from_arrays(arrays, schema=schema)

    return arrow_table


def astropy_to_arrow(astropy_table: atable.Table) -> pa.Table:
    """Convert an astropy table to an arrow table.

    Parameters
    ----------
    astropy_table : `astropy.Table`
        Input astropy table.

    Returns
    -------
    arrow_table : `pyarrow.Table`
        Converted arrow table.
    """
    from astropy.table import meta

    type_list = _numpy_dtype_to_arrow_types(astropy_table.dtype)

    md = {}
    md[b"lsst::arrow::rowcount"] = str(len(astropy_table))

    for name in astropy_table.dtype.names:
        _append_numpy_string_metadata(md, name, astropy_table.dtype[name])
        _append_numpy_multidim_metadata(md, name, astropy_table.dtype[name])

    meta_yaml = meta.get_yaml_from_table(astropy_table)
    meta_yaml_str = "\n".join(meta_yaml)
    md[b"table_meta_yaml"] = meta_yaml_str

    # Convert type list to fields with metadata.
    fields = []
    for name, pa_type in type_list:
        field_metadata = {}
        if description := astropy_table[name].description:
            field_metadata["description"] = description
        if unit := astropy_table[name].unit:
            field_metadata["unit"] = str(unit)
        fields.append(
            pa.field(
                name,
                pa_type,
                metadata=field_metadata,
            )
        )

    schema = pa.schema(fields, metadata=md)

    arrays = _numpy_style_arrays_to_arrow_arrays(
        astropy_table.dtype,
        len(astropy_table),
        astropy_table,
        schema,
    )

    arrow_table = pa.Table.from_arrays(arrays, schema=schema)

    return arrow_table


def astropy_to_pandas(astropy_table: atable.Table, index: str | None = None) -> pd.DataFrame:
    """Convert an astropy table to a pandas dataframe via arrow.

    By going via arrow we avoid pandas masked column bugs (e.g.
    https://github.com/pandas-dev/pandas/issues/58173)

    Parameters
    ----------
    astropy_table : `astropy.Table`
        Input astropy table.
    index : `str`, optional
        Name of column to set as index.

    Returns
    -------
    dataframe : `pandas.DataFrame`
        Output pandas dataframe.
    """
    dataframe = arrow_to_pandas(astropy_to_arrow(astropy_table))

    if isinstance(index, str):
        dataframe = dataframe.set_index(index)
    elif index:
        raise RuntimeError("index must be a string or None.")

    return dataframe


def _astropy_to_numpy_dict(astropy_table: atable.Table) -> dict[str, np.ndarray]:
    """Convert an astropy table to an arrow table.

    Parameters
    ----------
    astropy_table : `astropy.Table`
        Input astropy table.

    Returns
    -------
    numpy_dict : `dict` [`str`, `numpy.ndarray`]
        Dict with keys as the column names, values as the arrays.
    """
    return arrow_to_numpy_dict(astropy_to_arrow(astropy_table))


def pandas_to_arrow(dataframe: pd.DataFrame, default_length: int = 10) -> pa.Table:
    """Convert a pandas dataframe to an arrow table.

    Parameters
    ----------
    dataframe : `pandas.DataFrame`
        Input pandas dataframe.
    default_length : `int`, optional
        Default string length when not in metadata or can be inferred
        from column.

    Returns
    -------
    arrow_table : `pyarrow.Table`
        Converted arrow table.
    """
    try:
        arrow_table = pa.Table.from_pandas(dataframe)
    except pa.ArrowInvalid as e:
        msg = "; ".join(e.args)
        msg += "; This is usually because the column is mixed type or has uneven length rows."
        e.add_note(msg)
        raise

    # Update the metadata
    md = arrow_table.schema.metadata

    md[b"lsst::arrow::rowcount"] = str(arrow_table.num_rows)

    # We loop through the arrow table columns because the datatypes have
    # been checked and converted from pandas objects.
    for name in arrow_table.column_names:
        if not name.startswith("__") and arrow_table[name].type == pa.string():
            if len(arrow_table[name]) > 0:
                strlen = max(len(row.as_py()) for row in arrow_table[name] if row.is_valid)
            else:
                strlen = default_length
            md[f"lsst::arrow::len::{name}".encode()] = str(strlen)

    arrow_table = arrow_table.replace_schema_metadata(md)

    return arrow_table


def pandas_to_astropy(dataframe: pd.DataFrame) -> atable.Table:
    """Convert a pandas dataframe to an astropy table, preserving indexes.

    Parameters
    ----------
    dataframe : `pandas.DataFrame`
        Input pandas dataframe.

    Returns
    -------
    astropy_table : `astropy.table.Table`
        Converted astropy table.
    """
    import pandas as pd

    if isinstance(dataframe.columns, pd.MultiIndex):
        raise ValueError("Cannot convert a multi-index dataframe to an astropy table.")

    return arrow_to_astropy(pandas_to_arrow(dataframe))


def _pandas_to_numpy_dict(dataframe: pd.DataFrame) -> dict[str, np.ndarray]:
    """Convert a pandas dataframe to an dict of numpy arrays.

    Parameters
    ----------
    dataframe : `pandas.DataFrame`
        Input pandas dataframe.

    Returns
    -------
    numpy_dict : `dict` [`str`, `numpy.ndarray`]
        Dict with keys as the column names, values as the arrays.
    """
    return arrow_to_numpy_dict(pandas_to_arrow(dataframe))


def numpy_to_astropy(np_array: np.ndarray) -> atable.Table:
    """Convert a numpy table to an astropy table.

    Parameters
    ----------
    np_array : `numpy.ndarray`
        Input numpy array with multiple fields.

    Returns
    -------
    astropy_table : `astropy.table.Table`
        Converted astropy table.
    """
    from astropy.table import Table

    return Table(data=np_array, copy=False)


def arrow_schema_to_pandas_index(schema: pa.Schema) -> pd.Index | pd.MultiIndex:
    """Convert an arrow schema to a pandas index/multiindex.

    Parameters
    ----------
    schema : `pyarrow.Schema`
        Input pyarrow schema.

    Returns
    -------
    index : `pandas.Index` or `pandas.MultiIndex`
        Converted pandas index.
    """
    import pandas as pd

    if b"pandas" in schema.metadata:
        md = json.loads(schema.metadata[b"pandas"])
        indexes = md["column_indexes"]
        len_indexes = len(indexes)
    else:
        len_indexes = 0

    if len_indexes <= 1:
        return pd.Index(name for name in schema.names if not name.startswith("__"))
    else:
        raw_columns = _split_multi_index_column_names(len(indexes), schema.names)
        return pd.MultiIndex.from_tuples(raw_columns, names=[f["name"] for f in indexes])


def arrow_schema_to_column_list(schema: pa.Schema) -> list[str]:
    """Convert an arrow schema to a list of string column names.

    Parameters
    ----------
    schema : `pyarrow.Schema`
        Input pyarrow schema.

    Returns
    -------
    column_list : `list` [`str`]
        Converted list of column names.
    """
    return list(schema.names)


class DataFrameSchema:
    """Wrapper class for a schema for a pandas DataFrame.

    Parameters
    ----------
    dataframe : `pandas.DataFrame`
        Dataframe to turn into a schema.
    """

    def __init__(self, dataframe: pd.DataFrame) -> None:
        self._schema = dataframe.loc[[False] * len(dataframe)]

    @classmethod
    def from_arrow(cls, schema: pa.Schema) -> DataFrameSchema:
        """Convert an arrow schema into a `DataFrameSchema`.

        Parameters
        ----------
        schema : `pyarrow.Schema`
            The pyarrow schema to convert.

        Returns
        -------
        dataframe_schema : `DataFrameSchema`
            Converted dataframe schema.
        """
        empty_table = pa.Table.from_pylist([] * len(schema.names), schema=schema)

        return cls(empty_table.to_pandas())

    def to_arrow_schema(self) -> pa.Schema:
        """Convert to an arrow schema.

        Returns
        -------
        arrow_schema : `pyarrow.Schema`
            Converted pyarrow schema.
        """
        arrow_table = pa.Table.from_pandas(self._schema)

        return arrow_table.schema

    def to_arrow_numpy_schema(self) -> ArrowNumpySchema:
        """Convert to an `ArrowNumpySchema`.

        Returns
        -------
        arrow_numpy_schema : `ArrowNumpySchema`
            Converted arrow numpy schema.
        """
        return ArrowNumpySchema.from_arrow(self.to_arrow_schema())

    def to_arrow_astropy_schema(self) -> ArrowAstropySchema:
        """Convert to an ArrowAstropySchema.

        Returns
        -------
        arrow_astropy_schema : `ArrowAstropySchema`
            Converted arrow astropy schema.
        """
        return ArrowAstropySchema.from_arrow(self.to_arrow_schema())

    @property
    def schema(self) -> np.dtype:
        return self._schema

    def __repr__(self) -> str:
        return repr(self._schema)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DataFrameSchema):
            return NotImplemented

        return self._schema.equals(other._schema)


class ArrowAstropySchema:
    """Wrapper class for a schema for an astropy table.

    Parameters
    ----------
    astropy_table : `astropy.table.Table`
        Input astropy table.
    """

    def __init__(self, astropy_table: atable.Table) -> None:
        self._schema = astropy_table[:0]

    @classmethod
    def from_arrow(cls, schema: pa.Schema) -> ArrowAstropySchema:
        """Convert an arrow schema into a ArrowAstropySchema.

        Parameters
        ----------
        schema : `pyarrow.Schema`
            Input pyarrow schema.

        Returns
        -------
        astropy_schema : `ArrowAstropySchema`
            Converted arrow astropy schema.
        """
        import numpy as np
        from astropy.table import Table

        dtype = _schema_to_dtype_list(schema)

        data = np.zeros(0, dtype=dtype)
        astropy_table = Table(data=data)

        _apply_astropy_metadata(astropy_table, schema)

        return cls(astropy_table)

    def to_arrow_schema(self) -> pa.Schema:
        """Convert to an arrow schema.

        Returns
        -------
        arrow_schema : `pyarrow.Schema`
            Converted pyarrow schema.
        """
        return astropy_to_arrow(self._schema).schema

    def to_dataframe_schema(self) -> DataFrameSchema:
        """Convert to a DataFrameSchema.

        Returns
        -------
        dataframe_schema : `DataFrameSchema`
            Converted dataframe schema.
        """
        return DataFrameSchema.from_arrow(astropy_to_arrow(self._schema).schema)

    def to_arrow_numpy_schema(self) -> ArrowNumpySchema:
        """Convert to an `ArrowNumpySchema`.

        Returns
        -------
        arrow_numpy_schema : `ArrowNumpySchema`
            Converted arrow numpy schema.
        """
        return ArrowNumpySchema.from_arrow(astropy_to_arrow(self._schema).schema)

    @property
    def schema(self) -> atable.Table:
        return self._schema

    def __repr__(self) -> str:
        return repr(self._schema)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ArrowAstropySchema):
            return NotImplemented

        # If this comparison passes then the two tables have the
        # same column names.
        if self._schema.dtype != other._schema.dtype:
            return False

        for name in self._schema.columns:
            if not self._schema[name].unit == other._schema[name].unit:
                return False
            if not self._schema[name].description == other._schema[name].description:
                return False
            if not self._schema[name].format == other._schema[name].format:
                return False

        return True


class ArrowNumpySchema:
    """Wrapper class for a schema for a numpy ndarray.

    Parameters
    ----------
    numpy_dtype : `numpy.dtype`
         Numpy dtype to convert.
    """

    def __init__(self, numpy_dtype: np.dtype) -> None:
        self._dtype = numpy_dtype

    @classmethod
    def from_arrow(cls, schema: pa.Schema) -> ArrowNumpySchema:
        """Convert an arrow schema into an `ArrowNumpySchema`.

        Parameters
        ----------
        schema : `pyarrow.Schema`
            Pyarrow schema to convert.

        Returns
        -------
        numpy_schema : `ArrowNumpySchema`
            Converted arrow numpy schema.
        """
        import numpy as np

        dtype = _schema_to_dtype_list(schema)

        return cls(np.dtype(dtype))

    def to_arrow_astropy_schema(self) -> ArrowAstropySchema:
        """Convert to an `ArrowAstropySchema`.

        Returns
        -------
        astropy_schema : `ArrowAstropySchema`
            Converted arrow astropy schema.
        """
        import numpy as np

        return ArrowAstropySchema.from_arrow(numpy_to_arrow(np.zeros(0, dtype=self._dtype)).schema)

    def to_dataframe_schema(self) -> DataFrameSchema:
        """Convert to a `DataFrameSchema`.

        Returns
        -------
        dataframe_schema : `DataFrameSchema`
            Converted dataframe schema.
        """
        import numpy as np

        return DataFrameSchema.from_arrow(numpy_to_arrow(np.zeros(0, dtype=self._dtype)).schema)

    def to_arrow_schema(self) -> pa.Schema:
        """Convert to a `pyarrow.Schema`.

        Returns
        -------
        arrow_schema : `pyarrow.Schema`
            Converted pyarrow schema.
        """
        import numpy as np

        return numpy_to_arrow(np.zeros(0, dtype=self._dtype)).schema

    @property
    def schema(self) -> np.dtype:
        return self._dtype

    def __repr__(self) -> str:
        return repr(self._dtype)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ArrowNumpySchema):
            return NotImplemented

        if not self._dtype == other._dtype:
            return False

        return True


def _split_multi_index_column_names(n: int, names: Iterable[str]) -> list[Sequence[str]]:
    """Split a string that represents a multi-index column.

    PyArrow maps Pandas' multi-index column names (which are tuples in Python)
    to flat strings on disk. This routine exists to reconstruct the original
    tuple.

    Parameters
    ----------
    n : `int`
        Number of levels in the `pandas.MultiIndex` that is being
        reconstructed.
    names : `~collections.abc.Iterable` [`str`]
        Strings to be split.

    Returns
    -------
    column_names : `list` [`tuple` [`str`]]
        A list of multi-index column name tuples.
    """
    column_names: list[Sequence[str]] = []

    pattern = re.compile(r"\({}\)".format(", ".join(["'(.*)'"] * n)))
    for name in names:
        m = re.search(pattern, name)
        if m is not None:
            column_names.append(m.groups())

    return column_names


def _standardize_multi_index_columns(
    pd_index: pd.MultiIndex,
    columns: Any,
    stringify: bool = True,
) -> list[str | Sequence[Any]]:
    """Transform a dictionary/iterable index from a multi-index column list
    into a string directly understandable by PyArrow.

    Parameters
    ----------
    pd_index : `pandas.MultiIndex`
        Pandas multi-index.
    columns : `list` [`tuple`] or `dict` [`str`, `str` or `list` [`str`]]
        Columns to standardize.
    stringify : `bool`, optional
        Should the column names be stringified?

    Returns
    -------
    names : `list` [`str`]
        Stringified representation of a multi-index column name.
    """
    index_level_names = tuple(pd_index.names)

    names: list[str | Sequence[Any]] = []

    if isinstance(columns, list):
        for requested in columns:
            if not isinstance(requested, tuple):
                raise ValueError(
                    "Columns parameter for multi-index data frame must be a dictionary or list of tuples. "
                    f"Instead got a {get_full_type_name(requested)}."
                )
            if stringify:
                names.append(str(requested))
            else:
                names.append(requested)
    else:
        if not isinstance(columns, collections.abc.Mapping):
            raise ValueError(
                "Columns parameter for multi-index data frame must be a dictionary or list of tuples. "
                f"Instead got a {get_full_type_name(columns)}."
            )
        if not set(index_level_names).issuperset(columns.keys()):
            raise ValueError(
                f"Cannot use dict with keys {set(columns.keys())} to select columns from {index_level_names}."
            )
        factors = [
            ensure_iterable(columns.get(level, pd_index.levels[i]))
            for i, level in enumerate(index_level_names)
        ]
        for requested in itertools.product(*factors):
            for i, value in enumerate(requested):
                if value not in pd_index.levels[i]:
                    raise ValueError(f"Unrecognized value {value!r} for index {index_level_names[i]!r}.")
            if stringify:
                names.append(str(requested))
            else:
                names.append(requested)

    return names


def _apply_astropy_metadata(astropy_table: atable.Table, arrow_schema: pa.Schema) -> None:
    """Apply any astropy metadata from the schema metadata.

    Parameters
    ----------
    astropy_table : `astropy.table.Table`
        Table to apply metadata.
    arrow_schema : `pyarrow.Schema`
        Arrow schema with metadata.
    """
    from astropy.table import meta

    metadata = arrow_schema.metadata if arrow_schema.metadata is not None else {}

    # Check if we have a special astropy metadata header yaml.
    meta_yaml = metadata.get(b"table_meta_yaml", None)
    if meta_yaml:
        meta_yaml = meta_yaml.decode("UTF8").split("\n")
        meta_hdr = meta.get_header_from_yaml(meta_yaml)

        # Set description, format, unit, meta from the column
        # metadata that was serialized with the table.
        header_cols = {x["name"]: x for x in meta_hdr["datatype"]}
        for col in astropy_table.columns.values():
            for attr in ("description", "format", "unit", "meta"):
                if attr in header_cols[col.name]:
                    setattr(col, attr, header_cols[col.name][attr])

        if "meta" in meta_hdr:
            astropy_table.meta.update(meta_hdr["meta"])
    else:
        # If we don't have astropy header data, we may have arrow field
        # metadata.
        for name in arrow_schema.names:
            field_metadata = arrow_schema.field(name).metadata
            if field_metadata is None:
                continue
            if (
                b"description" in field_metadata
                and (description := field_metadata[b"description"].decode("UTF-8")) != ""
            ):
                astropy_table[name].description = description
            if b"unit" in field_metadata and (unit := field_metadata[b"unit"].decode("UTF-8")) != "":
                astropy_table[name].unit = unit


def _arrow_string_to_numpy_dtype(
    schema: pa.Schema, name: str, numpy_column: np.ndarray | None = None, default_length: int = 10
) -> str:
    """Get the numpy dtype string associated with an arrow column.

    Parameters
    ----------
    schema : `pyarrow.Schema`
        Arrow table schema.
    name : `str`
        Column name.
    numpy_column : `numpy.ndarray`, optional
        Column to determine numpy string dtype.
    default_length : `int`, optional
        Default string length when not in metadata or can be inferred
        from column.

    Returns
    -------
    dtype_str : `str`
        Numpy dtype string.
    """
    # Special-case for string and binary columns
    md_name = f"lsst::arrow::len::{name}"
    strlen = default_length
    metadata = schema.metadata if schema.metadata is not None else {}
    if (encoded := md_name.encode("UTF-8")) in metadata:
        # String/bytes length from header.
        strlen = int(schema.metadata[encoded])
    elif numpy_column is not None and len(numpy_column) > 0:
        lengths = [len(row) for row in numpy_column if row]
        strlen = max(lengths) if lengths else 0

    dtype = f"U{strlen}" if schema.field(name).type == pa.string() else f"|S{strlen}"

    return dtype


def _append_numpy_string_metadata(metadata: dict[bytes, str], name: str, dtype: np.dtype) -> None:
    """Append numpy string length keys to arrow metadata.

    All column types are handled, but the metadata is only modified for
    string and byte columns.

    Parameters
    ----------
    metadata : `dict` [`bytes`, `str`]
        Metadata dictionary; modified in place.
    name : `str`
        Column name.
    dtype : `np.dtype`
        Numpy dtype.
    """
    import numpy as np

    if dtype.type is np.str_:
        metadata[f"lsst::arrow::len::{name}".encode()] = str(dtype.itemsize // 4)
    elif dtype.type is np.bytes_:
        metadata[f"lsst::arrow::len::{name}".encode()] = str(dtype.itemsize)


def _append_numpy_multidim_metadata(metadata: dict[bytes, str], name: str, dtype: np.dtype) -> None:
    """Append numpy multi-dimensional shapes to arrow metadata.

    All column types are handled, but the metadata is only modified for
    multi-dimensional columns.

    Parameters
    ----------
    metadata : `dict` [`bytes`, `str`]
        Metadata dictionary; modified in place.
    name : `str`
        Column name.
    dtype : `np.dtype`
        Numpy dtype.
    """
    if len(dtype.shape) > 1:
        metadata[f"lsst::arrow::shape::{name}".encode()] = str(dtype.shape)


def _multidim_shape_from_metadata(metadata: dict[bytes, bytes], list_size: int, name: str) -> tuple[int, ...]:
    """Retrieve the shape from the metadata, if available.

    Parameters
    ----------
    metadata : `dict` [`bytes`, `bytes`]
        Metadata dictionary.
    list_size : `int`
        Size of the list datatype.
    name : `str`
        Column name.

    Returns
    -------
    shape : `tuple` [`int`]
        Shape associated with the column.

    Raises
    ------
    RuntimeError
        Raised if metadata is found but has incorrect format.
    """
    md_name = f"lsst::arrow::shape::{name}"
    if (encoded := md_name.encode("UTF-8")) in metadata:
        groups = re.search(r"\((.*)\)", metadata[encoded].decode("UTF-8"))
        if groups is None:
            raise RuntimeError("Illegal value found in metadata.")
        shape = tuple(int(x) for x in groups[1].split(",") if x != "")
    else:
        shape = (list_size,)

    return shape


def _schema_to_dtype_list(schema: pa.Schema) -> list[tuple[str, tuple[Any] | str]]:
    """Convert a pyarrow schema to a numpy dtype.

    Parameters
    ----------
    schema : `pyarrow.Schema`
        Input pyarrow schema.

    Returns
    -------
    dtype_list: `list` [`tuple`]
        A list with name, type pairs.
    """
    metadata = schema.metadata if schema.metadata is not None else {}

    dtype: list[Any] = []
    for name in schema.names:
        t = schema.field(name).type
        if isinstance(t, pa.FixedSizeListType):
            shape = _multidim_shape_from_metadata(metadata, t.list_size, name)
            dtype.append((name, (t.value_type.to_pandas_dtype(), shape)))
        elif t not in (pa.string(), pa.binary()):
            dtype.append((name, t.to_pandas_dtype()))
        else:
            dtype.append((name, _arrow_string_to_numpy_dtype(schema, name)))

    return dtype


def _numpy_dtype_to_arrow_types(dtype: np.dtype) -> list[Any]:
    """Convert a numpy dtype to a list of arrow types.

    Parameters
    ----------
    dtype : `numpy.dtype`
        Numpy dtype to convert.

    Returns
    -------
    type_list : `list` [`object`]
        Converted list of arrow types.
    """
    from math import prod

    import numpy as np

    type_list: list[Any] = []
    if dtype.names is None:
        return type_list

    for name in dtype.names:
        dt = dtype[name]
        arrow_type: Any
        if len(dt.shape) > 0:
            arrow_type = pa.list_(
                pa.from_numpy_dtype(cast(tuple[np.dtype, tuple[int, ...]], dt.subdtype)[0].type),
                prod(dt.shape),
            )
        elif dt.type == np.datetime64:
            time_unit = "ns" if "ns" in dt.str else "us"
            # The pa.timestamp() is the correct datatype to round-trip
            # a numpy datetime64[ns] or datetime[us] array.
            arrow_type = pa.timestamp(time_unit)
        else:
            try:
                arrow_type = pa.from_numpy_dtype(dt.type)
            except pa.ArrowNotImplementedError as e:
                msg = f"Could not serialize column {name} (type {str(dt)}) to Parquet."
                if dt == np.dtype("O"):
                    msg += " This is usually because the column is mixed type or has uneven length rows."
                e.add_note(msg)
                raise
        type_list.append((name, arrow_type))

    return type_list


def _numpy_dict_to_dtype(numpy_dict: dict[str, np.ndarray]) -> tuple[np.dtype, int]:
    """Extract equivalent table dtype from dict of numpy arrays.

    Parameters
    ----------
    numpy_dict : `dict` [`str`, `numpy.ndarray`]
        Dict with keys as the column names, values as the arrays.

    Returns
    -------
    dtype : `numpy.dtype`
        dtype of equivalent table.
    rowcount : `int`
        Number of rows in the table.

    Raises
    ------
    ValueError if columns in numpy_dict have unequal numbers of rows.
    """
    import numpy as np

    dtype_list = []
    rowcount = 0
    for name, col in numpy_dict.items():
        if rowcount == 0:
            rowcount = len(col)
        if len(col) != rowcount:
            raise ValueError(f"Column {name} has a different number of rows.")
        if len(col.shape) == 1:
            dtype_list.append((name, col.dtype))
        else:
            dtype_list.append((name, (col.dtype, col.shape[1:])))
    dtype = np.dtype(dtype_list)

    return (dtype, rowcount)


def _numpy_style_arrays_to_arrow_arrays(
    dtype: np.dtype,
    rowcount: int,
    np_style_arrays: dict[str, np.ndarray] | np.ndarray | atable.Table,
    schema: pa.Schema,
) -> list[pa.Array]:
    """Convert numpy-style arrays to arrow arrays.

    Parameters
    ----------
    dtype : `numpy.dtype`
        Numpy dtype of input table/arrays.
    rowcount : `int`
        Number of rows in input table/arrays.
    np_style_arrays : `dict` [`str`, `np.ndarray`] or `np.ndarray`
                      or `astropy.table.Table`
        Arrays to convert to arrow.
    schema : `pyarrow.Schema`
        Schema of arrow table.

    Returns
    -------
    arrow_arrays : `list` [`pyarrow.Array`]
        List of converted pyarrow arrays.
    """
    import numpy as np

    arrow_arrays: list[pa.Array] = []
    if dtype.names is None:
        return arrow_arrays

    for name in dtype.names:
        dt = dtype[name]
        val: Any
        if len(dt.shape) > 0:
            if rowcount > 0:
                val = np.split(np_style_arrays[name].ravel(), rowcount)
            else:
                val = []
        else:
            val = np_style_arrays[name]

        try:
            arrow_arrays.append(pa.array(val, type=schema.field(name).type))
        except pa.ArrowNotImplementedError as err:
            # Check if val is big-endian.
            if (np.little_endian and val.dtype.byteorder == ">") or (
                not np.little_endian and val.dtype.byteorder == "="
            ):
                # We need to convert the array to little-endian.
                val2 = val.byteswap()
                val2.dtype = val2.dtype.newbyteorder("<")
                arrow_arrays.append(pa.array(val2, type=schema.field(name).type))
            else:
                # This failed for some other reason so raise the exception.
                raise err

    return arrow_arrays


def compute_row_group_size(schema: pa.Schema, target_size: int = TARGET_ROW_GROUP_BYTES) -> int:
    """Compute approximate row group size for a given arrow schema.

    Given a schema, this routine will compute the number of rows in a row group
    that targets the persisted size on disk (or smaller).  The exact size on
    disk depends on the compression settings and ratios; typical binary data
    tables will have around 15-20% compression with the pyarrow default
    ``snappy`` compression algorithm.

    Parameters
    ----------
    schema : `pyarrow.Schema`
        Arrow table schema.
    target_size : `int`, optional
        The target size (in bytes).

    Returns
    -------
    row_group_size : `int`
        Number of rows per row group to hit the target size.
    """
    bit_width = 0

    metadata = schema.metadata if schema.metadata is not None else {}

    for name in schema.names:
        t = schema.field(name).type

        if t in (pa.string(), pa.binary()):
            md_name = f"lsst::arrow::len::{name}"

            if (encoded := md_name.encode("UTF-8")) in metadata:
                # String/bytes length from header.
                strlen = int(schema.metadata[encoded])
            else:
                # We don't know the string width, so guess something.
                strlen = 10

            # Assuming UTF-8 encoding, and very few wide characters.
            t_width = 8 * strlen
        elif isinstance(t, pa.FixedSizeListType):
            if t.value_type == pa.null():
                t_width = 0
            else:
                t_width = t.list_size * t.value_type.bit_width
        elif t == pa.null():
            t_width = 0
        elif isinstance(t, pa.ListType):
            if t.value_type == pa.null():
                t_width = 0
            else:
                # This is a variable length list, just choose
                # something arbitrary.
                t_width = 10 * t.value_type.bit_width
        else:
            t_width = t.bit_width

        bit_width += t_width

    # Insist it is at least 1 byte wide to avoid any divide-by-zero errors.
    if bit_width < 8:
        bit_width = 8

    byte_width = bit_width // 8

    return target_size // byte_width

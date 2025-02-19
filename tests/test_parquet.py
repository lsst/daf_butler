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

"""Tests for ParquetFormatter.

Tests in this module are disabled unless pandas and pyarrow are importable.
"""

import datetime
import os
import posixpath
import shutil
import unittest
import uuid

try:
    import pyarrow as pa
except ImportError:
    pa = None
try:
    import astropy.table as atable
    from astropy import units
except ImportError:
    atable = None
try:
    import numpy as np
except ImportError:
    np = None
try:
    import pandas as pd
except ImportError:
    pd = None

try:
    import boto3
    import botocore

    from lsst.resources.s3utils import clean_test_environment_for_s3

    try:
        from moto import mock_aws  # v5
    except ImportError:
        from moto import mock_s3 as mock_aws
except ImportError:
    boto3 = None

try:
    import fsspec
except ImportError:
    fsspec = None

try:
    import s3fs
except ImportError:
    s3fs = None


from lsst.daf.butler import (
    Butler,
    Config,
    DatasetProvenance,
    DatasetRef,
    DatasetType,
    FileDataset,
    StorageClassConfig,
    StorageClassFactory,
)
from lsst.resources import ResourcePath

try:
    from lsst.daf.butler.delegates.arrowtable import ArrowTableDelegate
except ImportError:
    pa = None

try:
    from lsst.daf.butler.formatters.parquet import (
        ASTROPY_PANDAS_INDEX_KEY,
        ArrowAstropySchema,
        ArrowNumpySchema,
        DataFrameSchema,
        ParquetFormatter,
        _append_numpy_multidim_metadata,
        _astropy_to_numpy_dict,
        _numpy_dict_to_numpy,
        _numpy_dtype_to_arrow_types,
        _numpy_style_arrays_to_arrow_arrays,
        _numpy_to_numpy_dict,
        add_pandas_index_to_astropy,
        arrow_to_astropy,
        arrow_to_numpy,
        arrow_to_numpy_dict,
        arrow_to_pandas,
        astropy_to_arrow,
        astropy_to_pandas,
        compute_row_group_size,
        numpy_dict_to_arrow,
        numpy_to_arrow,
        pandas_to_arrow,
        pandas_to_astropy,
    )
except ImportError:
    pa = None
    pd = None
    atable = None
    np = None
from lsst.daf.butler.tests.utils import makeTestTempDir, removeTestTempDir

TESTDIR = os.path.abspath(os.path.dirname(__file__))


def _makeSimpleNumpyTable(include_multidim=False, include_bigendian=False):
    """Make a simple numpy table with random data.

    Parameters
    ----------
    include_multidim : `bool`
        Include multi-dimensional columns.
    include_bigendian : `bool`
        Include big-endian columns.

    Returns
    -------
    numpyTable : `numpy.ndarray`
    """
    nrow = 5

    dtype = [
        ("index", "i4"),
        ("a", "f8"),
        ("b", "f8"),
        ("c", "f8"),
        ("ddd", "f8"),
        ("f", "i8"),
        ("strcol", "U10"),
        ("bytecol", "S10"),
        ("dtn", "datetime64[ns]"),
        ("dtu", "datetime64[us]"),
    ]

    if include_multidim:
        dtype.extend(
            [
                ("d1", "f4", (5,)),
                ("d2", "i8", (5, 10)),
                ("d3", "f8", (5, 10)),
            ]
        )

    if include_bigendian:
        dtype.extend([("a_bigendian", ">f8"), ("f_bigendian", ">i8")])

    data = np.zeros(nrow, dtype=dtype)
    data["index"][:] = np.arange(nrow)
    data["a"] = np.random.randn(nrow)
    data["b"] = np.random.randn(nrow)
    data["c"] = np.random.randn(nrow)
    data["ddd"] = np.random.randn(nrow)
    data["f"] = np.arange(nrow) * 10
    data["strcol"][:] = "teststring"
    data["bytecol"][:] = "teststring"
    data["dtn"] = datetime.datetime.fromisoformat("2024-07-23")
    data["dtu"] = datetime.datetime.fromisoformat("2024-07-23")

    if include_multidim:
        data["d1"] = np.random.randn(data["d1"].size).reshape(data["d1"].shape)
        data["d2"] = np.arange(data["d2"].size).reshape(data["d2"].shape)
        data["d3"] = np.asfortranarray(np.random.randn(data["d3"].size).reshape(data["d3"].shape))

    if include_bigendian:
        data["a_bigendian"][:] = data["a"]
        data["f_bigendian"][:] = data["f"]

    return data


def _makeSingleIndexDataFrame(include_masked=False, include_lists=False):
    """Make a single index data frame for testing.

    Parameters
    ----------
    include_masked : `bool`
        Include masked columns.
    include_lists : `bool`
        Include list columns.

    Returns
    -------
    dataFrame : `~pandas.DataFrame`
        The test dataframe.
    allColumns : `list` [`str`]
        List of all the columns (including index columns).
    """
    data = _makeSimpleNumpyTable()
    df = pd.DataFrame(data)
    df = df.set_index("index")

    if include_masked:
        nrow = len(df)

        df["m1"] = pd.array(np.arange(nrow), dtype=pd.Int64Dtype())
        df["m2"] = pd.array(np.arange(nrow), dtype=np.float32)
        df["mstrcol"] = pd.array(np.array(["text"] * nrow))
        df.loc[1, ["m1", "m2", "mstrcol"]] = None
        df.loc[0, "m1"] = 1649900760361600113

    if include_lists:
        nrow = len(df)

        df["l1"] = [[0, 0]] * nrow
        df["l2"] = [[0.0, 0.0]] * nrow
        df["l3"] = [[]] * nrow

    allColumns = df.columns.append(pd.Index(df.index.names))

    return df, allColumns


def _makeMultiIndexDataFrame():
    """Make a multi-index data frame for testing.

    Returns
    -------
    dataFrame : `~pandas.DataFrame`
        The test dataframe.
    """
    columns = pd.MultiIndex.from_tuples(
        [
            ("g", "a"),
            ("g", "b"),
            ("g", "c"),
            ("r", "a"),
            ("r", "b"),
            ("r", "c"),
        ],
        names=["filter", "column"],
    )
    df = pd.DataFrame(np.random.randn(5, 6), index=np.arange(5, dtype=int), columns=columns)

    return df


def _makeSimpleAstropyTable(include_multidim=False, include_masked=False, include_bigendian=False):
    """Make an astropy table for testing.

    Parameters
    ----------
    include_multidim : `bool`
        Include multi-dimensional columns.
    include_masked : `bool`
        Include masked columns.
    include_bigendian : `bool`
        Include big-endian columns.

    Returns
    -------
    astropyTable : `astropy.table.Table`
        The test table.
    """
    data = _makeSimpleNumpyTable(include_multidim=include_multidim, include_bigendian=include_bigendian)
    # Add a couple of units.
    table = atable.Table(data)
    table["a"].unit = units.degree
    table["a"].description = "Description of column a"
    table["b"].unit = units.meter
    table["b"].description = "Description of column b"

    # Add some masked columns.
    if include_masked:
        nrow = len(table)
        mask = np.zeros(nrow, dtype=bool)
        mask[1] = True
        # We set the masked columns with the underlying sentinel value
        # to be able test after serialization.

        # Masked 64-bit integer.
        arr = np.arange(nrow, dtype="i8")
        arr[mask] = -1
        arr[0] = 1649900760361600113
        table["m_i8"] = np.ma.masked_array(data=arr, mask=mask, fill_value=-1)
        # Masked 32-bit float.
        arr = np.arange(nrow, dtype="f4")
        arr[mask] = np.nan
        table["m_f4"] = np.ma.masked_array(data=arr, mask=mask, fill_value=np.nan)
        # Unmasked 32-bit float with NaNs.
        table["um_f4"] = arr
        # Masked 64-bit float.
        arr = np.arange(nrow, dtype="f8")
        arr[mask] = np.nan
        table["m_f8"] = np.ma.masked_array(data=arr, mask=mask, fill_value=np.nan)
        # Unmasked 64-bit float with NaNs.
        table["um_f8"] = arr
        # Masked boolean.
        arr = np.zeros(nrow, dtype=np.bool_)
        arr[mask] = True
        table["m_bool"] = np.ma.masked_array(data=arr, mask=mask, fill_value=True)
        # Masked unsigned 32-bit unsigned int.
        arr = np.arange(nrow, dtype="u4")
        arr[mask] = 0
        table["m_u4"] = np.ma.masked_array(data=arr, mask=mask, fill_value=0)
        # Masked string.
        table["m_str"] = np.ma.masked_array(data=np.array(["text"] * nrow), mask=mask, fill_value="")
        # Masked bytes.
        table["m_byte"] = np.ma.masked_array(data=np.array([b"bytes"] * nrow), mask=mask, fill_value=b"")

    return table


def _makeSimpleArrowTable(include_multidim=False, include_masked=False):
    """Make an arrow table for testing.

    Parameters
    ----------
    include_multidim : `bool`
        Include multi-dimensional columns.
    include_masked : `bool`
        Include masked columns.

    Returns
    -------
    arrowTable : `pyarrow.Table`
        The test table.
    """
    data = _makeSimpleAstropyTable(include_multidim=include_multidim, include_masked=include_masked)
    return astropy_to_arrow(data)


@unittest.skipUnless(pd is not None, "Cannot test ParquetFormatterDataFrame without pandas.")
@unittest.skipUnless(pa is not None, "Cannot test ParquetFormatterDataFrame without pyarrow.")
class ParquetFormatterDataFrameTestCase(unittest.TestCase):
    """Tests for ParquetFormatter, DataFrame, using local file datastore."""

    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")

    def setUp(self):
        """Create a new butler root for each test."""
        self.root = makeTestTempDir(TESTDIR)
        config = Config(self.configFile)
        self.run = "test_run"
        self.butler = Butler.from_config(
            Butler.makeRepo(self.root, config=config), writeable=True, run=self.run
        )
        # No dimensions in dataset type so we don't have to worry about
        # inserting dimension data or defining data IDs.
        self.datasetType = DatasetType(
            "data", dimensions=(), storageClass="DataFrame", universe=self.butler.dimensions
        )
        self.butler.registry.registerDatasetType(self.datasetType)

    def tearDown(self):
        removeTestTempDir(self.root)

    def testSingleIndexDataFrame(self):
        df1, allColumns = _makeSingleIndexDataFrame(include_masked=True)

        self.butler.put(df1, self.datasetType, dataId={})
        # Read the whole DataFrame.
        df2 = self.butler.get(self.datasetType, dataId={})
        self.assertTrue(df1.equals(df2))
        # Read just the column descriptions.
        columns2 = self.butler.get(self.datasetType.componentTypeName("columns"), dataId={})
        self.assertTrue(allColumns.equals(columns2))
        # Read the rowcount.
        rowcount = self.butler.get(self.datasetType.componentTypeName("rowcount"), dataId={})
        self.assertEqual(rowcount, len(df1))
        # Read the schema.
        schema = self.butler.get(self.datasetType.componentTypeName("schema"), dataId={})
        self.assertEqual(schema, DataFrameSchema(df1))
        # Read just some columns a few different ways.
        df3 = self.butler.get(self.datasetType, dataId={}, parameters={"columns": ["a", "c"]})
        self.assertTrue(df1.loc[:, ["a", "c"]].equals(df3))
        df4 = self.butler.get(self.datasetType, dataId={}, parameters={"columns": "a"})
        self.assertTrue(df1.loc[:, ["a"]].equals(df4))
        df5 = self.butler.get(self.datasetType, dataId={}, parameters={"columns": ["index", "a"]})
        self.assertTrue(df1.loc[:, ["a"]].equals(df5))
        df6 = self.butler.get(self.datasetType, dataId={}, parameters={"columns": "ddd"})
        self.assertTrue(df1.loc[:, ["ddd"]].equals(df6))
        df7 = self.butler.get(self.datasetType, dataId={}, parameters={"columns": ["a", "a"]})
        self.assertTrue(df1.loc[:, ["a"]].equals(df7))
        # Passing an unrecognized column should be a ValueError.
        with self.assertRaises(ValueError):
            self.butler.get(self.datasetType, dataId={}, parameters={"columns": ["e"]})

    def testSingleIndexDataFrameWithLists(self):
        df1, allColumns = _makeSingleIndexDataFrame(include_lists=True)

        self.butler.put(df1, self.datasetType, dataId={})
        # Read the whole DataFrame.
        df2 = self.butler.get(self.datasetType, dataId={})

        # We need to check the list columns specially because they go
        # from lists to arrays.
        for col in ["l1", "l2", "l3"]:
            for i in range(len(df1)):
                self.assertTrue(np.all(df2[col].values[i] == df1[col].values[i]))

    def testMultiIndexDataFrame(self):
        df1 = _makeMultiIndexDataFrame()

        self.butler.put(df1, self.datasetType, dataId={})
        # Read the whole DataFrame.
        df2 = self.butler.get(self.datasetType, dataId={})
        self.assertTrue(df1.equals(df2))
        # Read just the column descriptions.
        columns2 = self.butler.get(self.datasetType.componentTypeName("columns"), dataId={})
        self.assertTrue(df1.columns.equals(columns2))
        self.assertEqual(columns2.names, df1.columns.names)
        # Read the rowcount.
        rowcount = self.butler.get(self.datasetType.componentTypeName("rowcount"), dataId={})
        self.assertEqual(rowcount, len(df1))
        # Read the schema.
        schema = self.butler.get(self.datasetType.componentTypeName("schema"), dataId={})
        self.assertEqual(schema, DataFrameSchema(df1))
        # Read just some columns a few different ways.
        df3 = self.butler.get(self.datasetType, dataId={}, parameters={"columns": {"filter": "g"}})
        self.assertTrue(df1.loc[:, ["g"]].equals(df3))
        df4 = self.butler.get(
            self.datasetType, dataId={}, parameters={"columns": {"filter": ["r"], "column": "a"}}
        )
        self.assertTrue(df1.loc[:, [("r", "a")]].equals(df4))
        column_list = [("g", "a"), ("r", "c")]
        df5 = self.butler.get(self.datasetType, dataId={}, parameters={"columns": column_list})
        self.assertTrue(df1.loc[:, column_list].equals(df5))
        column_dict = {"filter": "r", "column": ["a", "b"]}
        df6 = self.butler.get(self.datasetType, dataId={}, parameters={"columns": column_dict})
        self.assertTrue(df1.loc[:, [("r", "a"), ("r", "b")]].equals(df6))
        # Passing an unrecognized column should be a ValueError.
        with self.assertRaises(ValueError):
            self.butler.get(self.datasetType, dataId={}, parameters={"columns": ["d"]})

    def testSingleIndexDataFrameEmptyString(self):
        """Test persisting a single index dataframe with empty strings."""
        df1, _ = _makeSingleIndexDataFrame()

        # Set one of the strings to None
        df1.at[1, "strcol"] = None

        self.butler.put(df1, self.datasetType, dataId={})
        # Read the whole DataFrame.
        df2 = self.butler.get(self.datasetType, dataId={})
        self.assertTrue(df1.equals(df2))

    def testSingleIndexDataFrameAllEmptyStrings(self):
        """Test persisting a single index dataframe with an empty string
        column.
        """
        df1, _ = _makeSingleIndexDataFrame()

        # Set all of the strings to None
        df1.loc[0:, "strcol"] = None

        self.butler.put(df1, self.datasetType, dataId={})
        # Read the whole DataFrame.
        df2 = self.butler.get(self.datasetType, dataId={})
        self.assertTrue(df1.equals(df2))

    def testLegacyDataFrame(self):
        """Test writing a dataframe to parquet via pandas (without additional
        metadata) and ensure that we can read it back with all the new
        functionality.
        """
        df1, allColumns = _makeSingleIndexDataFrame()

        fname = os.path.join(self.root, "test_dataframe.parq")
        df1.to_parquet(fname)

        legacy_type = DatasetType(
            "legacy_dataframe",
            dimensions=(),
            storageClass="DataFrame",
            universe=self.butler.dimensions,
        )
        self.butler.registry.registerDatasetType(legacy_type)

        data_id = {}
        ref = DatasetRef(legacy_type, data_id, run=self.run)
        dataset = FileDataset(path=fname, refs=[ref], formatter=ParquetFormatter)

        self.butler.ingest(dataset, transfer="copy")

        self.butler.put(df1, self.datasetType, dataId={})

        df2a = self.butler.get(self.datasetType, dataId={})
        df2b = self.butler.get("legacy_dataframe", dataId={})
        self.assertTrue(df2a.equals(df2b))

        df3a = self.butler.get(self.datasetType, dataId={}, parameters={"columns": ["a"]})
        df3b = self.butler.get("legacy_dataframe", dataId={}, parameters={"columns": ["a"]})
        self.assertTrue(df3a.equals(df3b))

        columns2a = self.butler.get(self.datasetType.componentTypeName("columns"), dataId={})
        columns2b = self.butler.get("legacy_dataframe.columns", dataId={})
        self.assertTrue(columns2a.equals(columns2b))

        rowcount2a = self.butler.get(self.datasetType.componentTypeName("rowcount"), dataId={})
        rowcount2b = self.butler.get("legacy_dataframe.rowcount", dataId={})
        self.assertEqual(rowcount2a, rowcount2b)

        schema2a = self.butler.get(self.datasetType.componentTypeName("schema"), dataId={})
        schema2b = self.butler.get("legacy_dataframe.schema", dataId={})
        self.assertEqual(schema2a, schema2b)

    def testDataFrameSchema(self):
        tab1 = _makeSimpleArrowTable()

        schema = DataFrameSchema.from_arrow(tab1.schema)

        self.assertIsInstance(schema.schema, pd.DataFrame)
        self.assertEqual(repr(schema), repr(schema._schema))
        self.assertNotEqual(schema, "not_a_schema")
        self.assertEqual(schema, schema)

        tab2 = _makeMultiIndexDataFrame()
        schema2 = DataFrameSchema(tab2)

        self.assertNotEqual(schema, schema2)

    @unittest.skipUnless(atable is not None, "Cannot test reading as astropy without astropy.")
    def testWriteSingleIndexDataFrameReadAsAstropyTable(self):
        df1, allColumns = _makeSingleIndexDataFrame()

        self.butler.put(df1, self.datasetType, dataId={})

        tab2 = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowAstropy")

        tab2_df = tab2.to_pandas(index="index")
        self.assertTrue(df1.equals(tab2_df))

        # Check reading the columns.
        columns = list(tab2.columns.keys())
        columns2 = self.butler.get(
            self.datasetType.componentTypeName("columns"), dataId={}, storageClass="ArrowColumnList"
        )
        # We check the set because pandas reorders the columns.
        self.assertEqual(set(columns2), set(columns))

        # Check reading the schema.
        schema = ArrowAstropySchema(tab2)
        schema2 = self.butler.get(
            self.datasetType.componentTypeName("schema"), dataId={}, storageClass="ArrowAstropySchema"
        )

        # The string types are objectified by pandas, and the order
        # will be changed because of pandas indexing.
        self.assertEqual(len(schema2.schema.columns), len(schema.schema.columns))
        for name in schema.schema.columns:
            self.assertIn(name, schema2.schema.columns)
            if schema2.schema[name].dtype != np.dtype("O"):
                self.assertEqual(schema2.schema[name].dtype, schema.schema[name].dtype)

    @unittest.skipUnless(atable is not None, "Cannot test reading as astropy without astropy.")
    def testWriteSingleIndexDataFrameWithMaskedColsReadAsAstropyTable(self):
        # We need to special-case the write-as-pandas read-as-astropy code
        # with masks because pandas has multiple ways to use masked columns.
        # (The string column mask handling in particular is frustratingly
        # inconsistent.)
        df1, allColumns = _makeSingleIndexDataFrame(include_masked=True)

        self.butler.put(df1, self.datasetType, dataId={})

        tab2 = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowAstropy")
        tab2_df = astropy_to_pandas(tab2, index="index")

        self.assertTrue(df1.columns.equals(tab2_df.columns))
        for name in tab2_df.columns:
            col1 = df1[name]
            col2 = tab2_df[name]

            if col1.hasnans:
                notNull = col1.notnull()
                self.assertTrue(notNull.equals(col2.notnull()))
                # Need to check value-by-value because column may
                # be made of objects, depending on what pandas decides.
                for index in notNull.values.nonzero()[0]:
                    self.assertEqual(col1[index], col2[index])
            else:
                self.assertTrue(col1.equals(col2))

    @unittest.skipUnless(atable is not None, "Cannot test reading as astropy without astropy.")
    def testWriteMultiIndexDataFrameReadAsAstropyTable(self):
        df1 = _makeMultiIndexDataFrame()

        self.butler.put(df1, self.datasetType, dataId={})

        _ = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowAstropy")

        # This is an odd duck, it doesn't really round-trip.
        # This test simply checks that it's readable, but definitely not
        # recommended.

    @unittest.skipUnless(atable is not None, "Cannot test writing as astropy without astropy.")
    def testWriteAstropyTableWithMaskedColsReadAsSingleIndexDataFrame(self):
        tab1 = _makeSimpleAstropyTable(include_masked=True)

        self.butler.put(tab1, self.datasetType, dataId={})

        tab2 = self.butler.get(self.datasetType, dataId={})

        tab1_df = astropy_to_pandas(tab1)
        self.assertTrue(tab1_df.equals(tab2))

        tab2_astropy = pandas_to_astropy(tab2)
        for col in tab1.dtype.names:
            np.testing.assert_array_equal(tab2_astropy[col], tab1[col])
            if isinstance(tab1[col], atable.column.MaskedColumn):
                np.testing.assert_array_equal(tab2_astropy[col].mask, tab1[col].mask)

    @unittest.skipUnless(pa is not None, "Cannot test reading as arrow without pyarrow.")
    def testWriteSingleIndexDataFrameReadAsArrowTable(self):
        df1, allColumns = _makeSingleIndexDataFrame()

        self.butler.put(df1, self.datasetType, dataId={})

        tab2 = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowTable")

        tab2_df = arrow_to_pandas(tab2)
        self.assertTrue(df1.equals(tab2_df))

        # Check reading the columns.
        columns = list(tab2.schema.names)
        columns2 = self.butler.get(
            self.datasetType.componentTypeName("columns"), dataId={}, storageClass="ArrowColumnList"
        )
        # We check the set because pandas reorders the columns.
        self.assertEqual(set(columns), set(columns2))

        # Override the component using a dataset type.
        columnsType = self.datasetType.makeComponentDatasetType("columns").overrideStorageClass(
            "ArrowColumnList"
        )
        self.assertEqual(columns2, self.butler.get(columnsType))

        # Check getting a component while overriding the storage class via
        # the dataset type. This overrides the parent storage class and then
        # selects the component.
        columnsType = self.datasetType.overrideStorageClass("ArrowAstropy").makeComponentDatasetType(
            "columns"
        )
        self.assertEqual(columns2, self.butler.get(columnsType))

        # Check reading the schema.
        schema = tab2.schema
        schema2 = self.butler.get(
            self.datasetType.componentTypeName("schema"), dataId={}, storageClass="ArrowSchema"
        )

        # These will not have the same metadata, nor will the string column
        # information be maintained.
        self.assertEqual(len(schema.names), len(schema2.names))
        for name in schema.names:
            if schema.field(name).type not in (pa.string(), pa.binary()):
                self.assertEqual(schema.field(name).type, schema2.field(name).type)

    @unittest.skipUnless(pa is not None, "Cannot test reading as arrow without pyarrow.")
    def testWriteMultiIndexDataFrameReadAsArrowTable(self):
        df1 = _makeMultiIndexDataFrame()

        self.butler.put(df1, self.datasetType, dataId={})

        tab2 = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowTable")

        tab2_df = arrow_to_pandas(tab2)
        self.assertTrue(df1.equals(tab2_df))

    @unittest.skipUnless(np is not None, "Cannot test reading as numpy without numpy.")
    def testWriteSingleIndexDataFrameReadAsNumpyTable(self):
        df1, allColumns = _makeSingleIndexDataFrame()

        self.butler.put(df1, self.datasetType, dataId={})

        tab2 = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowNumpy")

        tab2_df = pd.DataFrame.from_records(tab2, index=["index"])
        self.assertTrue(df1.equals(tab2_df))

        # Check reading the columns.
        columns = list(tab2.dtype.names)
        columns2 = self.butler.get(
            self.datasetType.componentTypeName("columns"), dataId={}, storageClass="ArrowColumnList"
        )
        # We check the set because pandas reorders the columns.
        self.assertEqual(set(columns2), set(columns))

        # Check reading the schema.
        schema = ArrowNumpySchema(tab2.dtype)
        schema2 = self.butler.get(
            self.datasetType.componentTypeName("schema"), dataId={}, storageClass="ArrowNumpySchema"
        )

        # The string types will be objectified by pandas, and the order
        # will be changed because of pandas indexing.
        self.assertEqual(len(schema.schema.names), len(schema2.schema.names))
        for name in schema.schema.names:
            self.assertIn(name, schema2.schema.names)
            self.assertEqual(schema2.schema[name].type, schema.schema[name].type)

    @unittest.skipUnless(np is not None, "Cannot test reading as numpy without numpy.")
    def testWriteMultiIndexDataFrameReadAsNumpyTable(self):
        df1 = _makeMultiIndexDataFrame()

        self.butler.put(df1, self.datasetType, dataId={})

        _ = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowNumpy")

        # This is an odd duck, it doesn't really round-trip.
        # This test simply checks that it's readable, but definitely not
        # recommended.

    @unittest.skipUnless(np is not None, "Cannot test reading as numpy dict without numpy.")
    def testWriteSingleIndexDataFrameReadAsNumpyDict(self):
        df1, allColumns = _makeSingleIndexDataFrame()

        self.butler.put(df1, self.datasetType, dataId={})

        tab2 = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowNumpyDict")

        tab2_df = pd.DataFrame.from_records(tab2, index=["index"])
        # The column order is not maintained.
        self.assertEqual(set(df1.columns), set(tab2_df.columns))
        for col in df1.columns:
            self.assertTrue(np.all(df1[col].values == tab2_df[col].values))

    @unittest.skipUnless(np is not None, "Cannot test reading as numpy dict without numpy.")
    def testWriteMultiIndexDataFrameReadAsNumpyDict(self):
        df1 = _makeMultiIndexDataFrame()

        self.butler.put(df1, self.datasetType, dataId={})

        _ = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowNumpyDict")

        # This is an odd duck, it doesn't really round-trip.
        # This test simply checks that it's readable, but definitely not
        # recommended.

    def testBadDataFrameColumnParquet(self):
        df1, allColumns = _makeSingleIndexDataFrame()

        # Make a column with mixed type.
        bad_col1 = [0.0] * len(df1)
        bad_col1[1] = 0.0 * units.nJy
        bad_df = df1.copy()
        bad_df["bad_col1"] = bad_col1

        # At the moment we cannot check that the correct note is added
        # to the exception, but that will be possible in the future.
        with self.assertRaises(RuntimeError):
            self.butler.put(bad_df, self.datasetType, dataId={})

    @unittest.skipUnless(atable is not None, "Cannot test reading as astropy without astropy.")
    def testWriteReadAstropyTableLossless(self):
        tab1 = _makeSimpleAstropyTable(include_multidim=True, include_masked=True)

        put_ref = self.butler.put(tab1, self.datasetType, dataId={})

        tab2 = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowAstropy")

        # Check that minimal provenance was written by default.
        expected = {
            "LSST.BUTLER.ID": str(put_ref.id),
            "LSST.BUTLER.RUN": "test_run",
            "LSST.BUTLER.DATASETTYPE": "data",
        }

        self.assertEqual(tab2.meta, expected)

        _checkAstropyTableEquality(tab1, tab2)

    @unittest.skipUnless(atable is not None, "Cannot test reading as astropy without astropy.")
    def testWriteReadAstropyTableProvenance(self):
        tab1 = _makeSimpleAstropyTable()

        # Create a ref for provenance.
        astropy_type = DatasetType(
            "astropy_parquet",
            dimensions=(),
            storageClass="ArrowAstropy",
            universe=self.butler.dimensions,
        )
        self.butler.registry.registerDatasetType(astropy_type)
        input_ref = DatasetRef(astropy_type, {}, run="other_run")
        quantum_id = uuid.uuid4()
        provenance = DatasetProvenance(quantum_id=quantum_id)
        provenance.add_input(input_ref)

        put_ref = self.butler.put(tab1, self.datasetType, dataId={}, provenance=provenance)

        tab2 = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowAstropy")

        expected = {
            "LSST.BUTLER.ID": str(put_ref.id),
            "LSST.BUTLER.RUN": "test_run",
            "LSST.BUTLER.DATASETTYPE": "data",
            "LSST.BUTLER.QUANTUM": str(quantum_id),
            "LSST.BUTLER.INPUT.0.ID": str(input_ref.id),
            "LSST.BUTLER.INPUT.0.RUN": "other_run",
            "LSST.BUTLER.INPUT.0.DATASETTYPE": "astropy_parquet",
        }
        self.assertEqual(tab2.meta, expected)

        # Put the dataset again, with different provenance and ensure
        # that the previous provenance was stripped.
        self.butler.collections.register("new_run")
        put_ref3 = self.butler.put(tab2, self.datasetType, dataId={}, run="new_run")

        # tab2 will have been updated in place.
        expected = {
            "LSST.BUTLER.ID": str(put_ref3.id),
            "LSST.BUTLER.RUN": "new_run",
            "LSST.BUTLER.DATASETTYPE": "data",
        }
        self.assertEqual(tab2.meta, expected)
        null_prov, prov_ref = DatasetProvenance.from_flat_dict(tab2.meta, self.butler)
        self.assertEqual(prov_ref, put_ref3)
        self.assertEqual(null_prov, DatasetProvenance())

    @unittest.skipUnless(np is not None, "Cannot test reading as numpy without numpy.")
    def testWriteReadNumpyTableLossless(self):
        tab1 = _makeSimpleNumpyTable(include_multidim=True)

        self.butler.put(tab1, self.datasetType, dataId={})

        tab2 = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowNumpy")

        _checkNumpyTableEquality(tab1, tab2)

    @unittest.skipUnless(pa is not None, "Cannot test reading as arrow without pyarrow.")
    def testMaskedNumpy(self):
        tab1 = _makeSimpleArrowTable(include_multidim=False, include_masked=True)
        tab1_np = arrow_to_numpy(tab1)
        self.assertIsInstance(tab1_np, np.ma.MaskedArray)
        # Stats on a masked column should ignore the nan in row 1.
        col = tab1_np["m_f8"]
        self.assertEqual(np.mean(col), 2.25, f"Column: {col}")

        # Now without a mask.
        tab1 = _makeSimpleArrowTable(include_multidim=False, include_masked=False)
        tab1_np = arrow_to_numpy(tab1)
        self.assertNotIsInstance(tab1_np, np.ma.MaskedArray)

    @unittest.skipUnless(pa is not None, "Cannot test reading as arrow without pyarrow.")
    def testWriteReadArrowTableLossless(self):
        tab1 = _makeSimpleArrowTable(include_multidim=False, include_masked=True)

        self.butler.put(tab1, self.datasetType, dataId={})

        tab2 = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowTable")

        self.assertEqual(tab1.schema, tab2.schema)
        tab1_np = arrow_to_numpy(tab1)
        tab2_np = arrow_to_numpy(tab2)
        for col in tab1.column_names:
            np.testing.assert_array_equal(tab2_np[col], tab1_np[col])

    @unittest.skipUnless(np is not None, "Cannot test reading as numpy dict without numpy.")
    def testWriteReadNumpyDictLossless(self):
        tab1 = _makeSimpleNumpyTable(include_multidim=True)
        dict1 = _numpy_to_numpy_dict(tab1)

        self.butler.put(tab1, self.datasetType, dataId={})

        dict2 = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowNumpyDict")

        _checkNumpyDictEquality(dict1, dict2)


@unittest.skipUnless(pd is not None, "Cannot test InMemoryDatastore with DataFrames without pandas.")
class InMemoryDataFrameDelegateTestCase(ParquetFormatterDataFrameTestCase):
    """Tests for InMemoryDatastore, using ArrowTableDelegate with Dataframe."""

    configFile = os.path.join(TESTDIR, "config/basic/butler-inmemory.yaml")

    def testBadDataFrameColumnParquet(self):
        # This test does not raise for an in-memory datastore.
        pass

    def testWriteMultiIndexDataFrameReadAsAstropyTable(self):
        df1 = _makeMultiIndexDataFrame()

        self.butler.put(df1, self.datasetType, dataId={})

        with self.assertRaises(ValueError):
            _ = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowAstropy")

    def testLegacyDataFrame(self):
        # This test does not work with an inMemoryDatastore.
        pass

    def testBadInput(self):
        df1, _ = _makeSingleIndexDataFrame()
        delegate = ArrowTableDelegate("DataFrame")

        with self.assertRaises(ValueError):
            delegate.handleParameters(inMemoryDataset="not_a_dataframe")

        with self.assertRaises(AttributeError):
            delegate.getComponent(composite=df1, componentName="nothing")

    def testStorageClass(self):
        df1, allColumns = _makeSingleIndexDataFrame()

        factory = StorageClassFactory()
        factory.addFromConfig(StorageClassConfig())

        storageClass = factory.findStorageClass(type(df1), compare_types=False)
        # Force the name lookup to do name matching.
        storageClass._pytype = None
        self.assertEqual(storageClass.name, "DataFrame")

        storageClass = factory.findStorageClass(type(df1), compare_types=True)
        # Force the name lookup to do name matching.
        storageClass._pytype = None
        self.assertEqual(storageClass.name, "DataFrame")


@unittest.skipUnless(atable is not None, "Cannot test ParquetFormatterArrowAstropy without astropy.")
@unittest.skipUnless(pa is not None, "Cannot test ParquetFormatterArrowAstropy without pyarrow.")
class ParquetFormatterArrowAstropyTestCase(unittest.TestCase):
    """Tests for ParquetFormatter, ArrowAstropy, using local file datastore."""

    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")

    def setUp(self):
        """Create a new butler root for each test."""
        self.root = makeTestTempDir(TESTDIR)
        config = Config(self.configFile)
        self.run = "test_run"
        self.butler = Butler.from_config(
            Butler.makeRepo(self.root, config=config), writeable=True, run=self.run
        )
        # No dimensions in dataset type so we don't have to worry about
        # inserting dimension data or defining data IDs.
        self.datasetType = DatasetType(
            "data", dimensions=(), storageClass="ArrowAstropy", universe=self.butler.dimensions
        )
        self.butler.registry.registerDatasetType(self.datasetType)

    def tearDown(self):
        removeTestTempDir(self.root)

    def testAstropyTable(self):
        tab1 = _makeSimpleAstropyTable(include_multidim=True, include_masked=True)

        self.butler.put(tab1, self.datasetType, dataId={})
        # Read the whole Table.
        tab2 = self.butler.get(self.datasetType, dataId={})
        _checkAstropyTableEquality(tab1, tab2)
        # Read the columns.
        columns2 = self.butler.get(self.datasetType.componentTypeName("columns"), dataId={})
        self.assertEqual(len(columns2), len(tab1.dtype.names))
        for i, name in enumerate(tab1.dtype.names):
            self.assertEqual(columns2[i], name)
        # Read the rowcount.
        rowcount = self.butler.get(self.datasetType.componentTypeName("rowcount"), dataId={})
        self.assertEqual(rowcount, len(tab1))
        # Read the schema.
        schema = self.butler.get(self.datasetType.componentTypeName("schema"), dataId={})
        self.assertEqual(schema, ArrowAstropySchema(tab1))
        # Read just some columns a few different ways.
        tab3 = self.butler.get(self.datasetType, dataId={}, parameters={"columns": ["a", "c"]})
        _checkAstropyTableEquality(tab1[("a", "c")], tab3)
        tab4 = self.butler.get(self.datasetType, dataId={}, parameters={"columns": "a"})
        _checkAstropyTableEquality(tab1[("a",)], tab4)
        tab5 = self.butler.get(self.datasetType, dataId={}, parameters={"columns": ["index", "a"]})
        _checkAstropyTableEquality(tab1[("index", "a")], tab5)
        tab6 = self.butler.get(self.datasetType, dataId={}, parameters={"columns": "ddd"})
        _checkAstropyTableEquality(tab1[("ddd",)], tab6)
        tab7 = self.butler.get(self.datasetType, dataId={}, parameters={"columns": ["a", "a"]})
        _checkAstropyTableEquality(tab1[("a",)], tab7)
        # Passing an unrecognized column should be a ValueError.
        with self.assertRaises(ValueError):
            self.butler.get(self.datasetType, dataId={}, parameters={"columns": ["e"]})

    def testAstropyTableBigEndian(self):
        tab1 = _makeSimpleAstropyTable(include_bigendian=True)

        self.butler.put(tab1, self.datasetType, dataId={})
        # Read the whole Table.
        tab2 = self.butler.get(self.datasetType, dataId={})
        _checkAstropyTableEquality(tab1, tab2, has_bigendian=True)

    def testAstropyTableWithMetadata(self):
        tab1 = _makeSimpleAstropyTable(include_multidim=True)

        meta = {
            "meta_a": 5,
            "meta_b": 10.0,
            "meta_c": [1, 2, 3],
            "meta_d": True,
            "meta_e": "string",
        }

        tab1.meta.update(meta)

        self.butler.put(tab1, self.datasetType, dataId={})
        # Read the whole Table.
        tab2 = self.butler.get(self.datasetType, dataId={})
        # This will check that the metadata is equivalent as well.
        _checkAstropyTableEquality(tab1, tab2)

    def testArrowAstropySchema(self):
        tab1 = _makeSimpleAstropyTable()
        tab1_arrow = astropy_to_arrow(tab1)
        schema = ArrowAstropySchema.from_arrow(tab1_arrow.schema)

        self.assertIsInstance(schema.schema, atable.Table)
        self.assertEqual(repr(schema), repr(schema._schema))
        self.assertNotEqual(schema, "not_a_schema")
        self.assertEqual(schema, schema)

        # Test various inequalities
        tab2 = tab1.copy()
        tab2.rename_column("index", "index2")
        schema2 = ArrowAstropySchema(tab2)
        self.assertNotEqual(schema2, schema)

        tab2 = tab1.copy()
        tab2["index"].unit = units.micron
        schema2 = ArrowAstropySchema(tab2)
        self.assertNotEqual(schema2, schema)

        tab2 = tab1.copy()
        tab2["index"].description = "Index column"
        schema2 = ArrowAstropySchema(tab2)
        self.assertNotEqual(schema2, schema)

        tab2 = tab1.copy()
        tab2["index"].format = "%05d"
        schema2 = ArrowAstropySchema(tab2)
        self.assertNotEqual(schema2, schema)

    def testAstropyParquet(self):
        tab1 = _makeSimpleAstropyTable()

        # Remove datetime column which doesn't work with astropy currently.
        del tab1["dtn"]
        del tab1["dtu"]

        fname = os.path.join(self.root, "test_astropy.parq")
        tab1.write(fname)

        astropy_type = DatasetType(
            "astropy_parquet",
            dimensions=(),
            storageClass="ArrowAstropy",
            universe=self.butler.dimensions,
        )
        self.butler.registry.registerDatasetType(astropy_type)

        data_id = {}
        ref = DatasetRef(astropy_type, data_id, run=self.run)
        dataset = FileDataset(path=fname, refs=[ref], formatter=ParquetFormatter)

        self.butler.ingest(dataset, transfer="copy")

        self.butler.put(tab1, self.datasetType, dataId={})

        tab2a = self.butler.get(self.datasetType, dataId={})
        tab2b = self.butler.get("astropy_parquet", dataId={})
        _checkAstropyTableEquality(tab2a, tab2b)

        columns2a = self.butler.get(self.datasetType.componentTypeName("columns"), dataId={})
        columns2b = self.butler.get("astropy_parquet.columns", dataId={})
        self.assertEqual(len(columns2b), len(columns2a))
        for i, name in enumerate(columns2a):
            self.assertEqual(columns2b[i], name)

        rowcount2a = self.butler.get(self.datasetType.componentTypeName("rowcount"), dataId={})
        rowcount2b = self.butler.get("astropy_parquet.rowcount", dataId={})
        self.assertEqual(rowcount2a, rowcount2b)

        schema2a = self.butler.get(self.datasetType.componentTypeName("schema"), dataId={})
        schema2b = self.butler.get("astropy_parquet.schema", dataId={})
        self.assertEqual(schema2a, schema2b)

    @unittest.skipUnless(pa is not None, "Cannot test reading as arrow without pyarrow.")
    def testWriteAstropyReadAsArrowTable(self):
        # This astropy <-> arrow works fine with masked columns.
        tab1 = _makeSimpleAstropyTable(include_masked=True)

        self.butler.put(tab1, self.datasetType, dataId={})

        tab2 = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowTable")

        tab2_astropy = arrow_to_astropy(tab2)
        _checkAstropyTableEquality(tab1, tab2_astropy)

        # Check reading the columns.
        columns = tab2.schema.names
        columns2 = self.butler.get(
            self.datasetType.componentTypeName("columns"), dataId={}, storageClass="ArrowColumnList"
        )
        self.assertEqual(columns2, columns)

        # Check reading the schema.
        schema = tab2.schema
        schema2 = self.butler.get(
            self.datasetType.componentTypeName("schema"), dataId={}, storageClass="ArrowSchema"
        )

        self.assertEqual(schema, schema2)

    @unittest.skipUnless(pd is not None, "Cannot test reading as a dataframe without pandas.")
    def testWriteAstropyReadAsDataFrame(self):
        tab1 = _makeSimpleAstropyTable()

        self.butler.put(tab1, self.datasetType, dataId={})

        tab2 = self.butler.get(self.datasetType, dataId={}, storageClass="DataFrame")

        # This is tricky because it loses the units and gains a bonus pandas
        # _index_ column, so we just test the dataframe form.

        tab1_df = tab1.to_pandas()
        self.assertTrue(tab1_df.equals(tab2))

        # Check reading the columns.
        columns = tab2.columns
        columns2 = self.butler.get(
            self.datasetType.componentTypeName("columns"), dataId={}, storageClass="DataFrameIndex"
        )
        self.assertTrue(columns.equals(columns2))

        # Check reading the schema.
        schema = DataFrameSchema(tab2)
        schema2 = self.butler.get(
            self.datasetType.componentTypeName("schema"), dataId={}, storageClass="DataFrameSchema"
        )

        self.assertEqual(schema2, schema)

    @unittest.skipUnless(pd is not None, "Cannot test reading as a dataframe without pandas.")
    def testWriteAstropyWithMaskedColsReadAsDataFrame(self):
        # We need to special-case the write-as-astropy read-as-pandas code
        # with masks because pandas has multiple ways to use masked columns.
        # (When writing an astropy table with masked columns we get an object
        # column back, but each unmasked element has the correct type.)
        tab1 = _makeSimpleAstropyTable(include_masked=True)

        self.butler.put(tab1, self.datasetType, dataId={})

        tab2 = self.butler.get(self.datasetType, dataId={}, storageClass="DataFrame")

        tab1_df = astropy_to_pandas(tab1)

        self.assertTrue(tab1_df.columns.equals(tab2.columns))
        for name in tab2.columns:
            col1 = tab1_df[name]
            col2 = tab2[name]

            if col1.hasnans:
                notNull = col1.notnull()
                self.assertTrue(notNull.equals(col2.notnull()))
                # Need to check value-by-value because column may
                # be made of objects, depending on what pandas decides.
                for index in notNull.values.nonzero()[0]:
                    self.assertEqual(col1[index], col2[index])
            else:
                self.assertTrue(col1.equals(col2))

    @unittest.skipUnless(pd is not None, "Cannot test writing as a dataframe without pandas.")
    def testWriteSingleIndexDataFrameWithMaskedColsReadAsAstropyTable(self):
        df1, allColumns = _makeSingleIndexDataFrame(include_masked=True)

        self.butler.put(df1, self.datasetType, dataId={})

        tab2 = self.butler.get(self.datasetType, dataId={})

        df1_tab = pandas_to_astropy(df1)

        _checkAstropyTableEquality(df1_tab, tab2)

    @unittest.skipUnless(np is not None, "Cannot test reading as numpy without numpy.")
    def testWriteAstropyReadAsNumpyTable(self):
        tab1 = _makeSimpleAstropyTable()
        self.butler.put(tab1, self.datasetType, dataId={})

        tab2 = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowNumpy")

        # This is tricky because it loses the units.
        tab2_astropy = atable.Table(tab2)

        _checkAstropyTableEquality(tab1, tab2_astropy, skip_units=True)

        # Check reading the columns.
        columns = list(tab2.dtype.names)
        columns2 = self.butler.get(
            self.datasetType.componentTypeName("columns"), dataId={}, storageClass="ArrowColumnList"
        )
        self.assertEqual(columns2, columns)

        # Check reading the schema.
        schema = ArrowNumpySchema(tab2.dtype)
        schema2 = self.butler.get(
            self.datasetType.componentTypeName("schema"), dataId={}, storageClass="ArrowNumpySchema"
        )

        self.assertEqual(schema2, schema)

    @unittest.skipUnless(np is not None, "Cannot test reading as numpy without numpy.")
    def testWriteAstropyReadAsNumpyDict(self):
        tab1 = _makeSimpleAstropyTable()
        self.butler.put(tab1, self.datasetType, dataId={})

        tab2 = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowNumpyDict")

        # This is tricky because it loses the units.
        tab2_astropy = atable.Table(tab2)

        _checkAstropyTableEquality(tab1, tab2_astropy, skip_units=True)

    def testBadAstropyColumnParquet(self):
        tab1 = _makeSimpleAstropyTable()

        # Make a column with mixed type.
        bad_col1 = [0.0] * len(tab1)
        bad_col1[1] = 0.0 * units.nJy
        bad_tab = tab1.copy()
        bad_tab["bad_col1"] = bad_col1

        # At the moment we cannot check that the correct note is added
        # to the exception, but that will be possible in the future.
        with self.assertRaises(RuntimeError):
            self.butler.put(bad_tab, self.datasetType, dataId={})

        # Make a column with ragged size.
        bad_col2 = [[0]] * len(tab1)
        bad_col2[1] = [0, 0]
        bad_tab = tab1.copy()
        bad_tab["bad_col2"] = bad_col2

        with self.assertRaises(RuntimeError):
            self.butler.put(bad_tab, self.datasetType, dataId={})

    @unittest.skipUnless(pd is not None, "Cannot test ParquetFormatterDataFrame without pandas.")
    def testWriteAstropyTableWithPandasIndexHint(self, testStrip=True):
        tab1 = _makeSimpleAstropyTable()

        add_pandas_index_to_astropy(tab1, "index")

        self.butler.put(tab1, self.datasetType, dataId={})

        # Read in as an astropy table and ensure index hint is still there.
        tab2 = self.butler.get(self.datasetType, dataId={})

        self.assertIn(ASTROPY_PANDAS_INDEX_KEY, tab2.meta)
        self.assertEqual(tab2.meta[ASTROPY_PANDAS_INDEX_KEY], "index")

        # Read as a dataframe and ensure index is set.
        df3 = self.butler.get(self.datasetType, dataId={}, storageClass="DataFrame")

        self.assertEqual(df3.index.name, "index")

        # Read as a dataframe without naming the index column.
        with self.assertLogs(level="WARNING") as cm:
            _ = self.butler.get(
                self.datasetType,
                dataId={},
                storageClass="DataFrame",
                parameters={"columns": ["a", "b"]},
            )
        self.assertIn("Index column ``index``", cm.output[0])

        if testStrip:
            # Read as an astropy table without naming the index column.
            tab5 = self.butler.get(self.datasetType, dataId={}, parameters={"columns": ["a", "b"]})

            self.assertNotIn(ASTROPY_PANDAS_INDEX_KEY, tab5.meta)

        with self.assertRaises(ValueError):
            add_pandas_index_to_astropy(tab1, "not_a_column")


@unittest.skipUnless(atable is not None, "Cannot test InMemoryDatastore with AstropyTable without astropy.")
class InMemoryArrowAstropyDelegateTestCase(ParquetFormatterArrowAstropyTestCase):
    """Tests for InMemoryDatastore, using ArrowTableDelegate with
    AstropyTable.
    """

    configFile = os.path.join(TESTDIR, "config/basic/butler-inmemory.yaml")

    def testAstropyParquet(self):
        # This test does not work with an inMemoryDatastore.
        pass

    def testBadAstropyColumnParquet(self):
        # This test does not raise for an in-memory datastore.
        pass

    def testBadInput(self):
        tab1 = _makeSimpleAstropyTable()
        delegate = ArrowTableDelegate("ArrowAstropy")

        with self.assertRaises(ValueError):
            delegate.handleParameters(inMemoryDataset="not_an_astropy_table")

        with self.assertRaises(NotImplementedError):
            delegate.handleParameters(inMemoryDataset=tab1, parameters={"columns": [("a", "b")]})

        with self.assertRaises(AttributeError):
            delegate.getComponent(composite=tab1, componentName="nothing")

    @unittest.skipUnless(pd is not None, "Cannot test ParquetFormatterDataFrame without pandas.")
    def testWriteAstropyTableWithPandasIndexHint(self):
        super().testWriteAstropyTableWithPandasIndexHint(testStrip=False)


@unittest.skipUnless(np is not None, "Cannot test ParquetFormatterArrowNumpy without numpy.")
@unittest.skipUnless(pa is not None, "Cannot test ParquetFormatterArrowNumpy without pyarrow.")
class ParquetFormatterArrowNumpyTestCase(unittest.TestCase):
    """Tests for ParquetFormatter, ArrowNumpy, using local file datastore."""

    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")

    def setUp(self):
        """Create a new butler root for each test."""
        self.root = makeTestTempDir(TESTDIR)
        config = Config(self.configFile)
        self.butler = Butler.from_config(
            Butler.makeRepo(self.root, config=config), writeable=True, run="test_run"
        )
        # No dimensions in dataset type so we don't have to worry about
        # inserting dimension data or defining data IDs.
        self.datasetType = DatasetType(
            "data", dimensions=(), storageClass="ArrowNumpy", universe=self.butler.dimensions
        )
        self.butler.registry.registerDatasetType(self.datasetType)

    def tearDown(self):
        removeTestTempDir(self.root)

    def testNumpyTable(self):
        tab1 = _makeSimpleNumpyTable(include_multidim=True)

        self.butler.put(tab1, self.datasetType, dataId={})
        # Read the whole Table.
        tab2 = self.butler.get(self.datasetType, dataId={})
        _checkNumpyTableEquality(tab1, tab2)
        # Read the columns.
        columns2 = self.butler.get(self.datasetType.componentTypeName("columns"), dataId={})
        self.assertEqual(len(columns2), len(tab1.dtype.names))
        for i, name in enumerate(tab1.dtype.names):
            self.assertEqual(columns2[i], name)
        # Read the rowcount.
        rowcount = self.butler.get(self.datasetType.componentTypeName("rowcount"), dataId={})
        self.assertEqual(rowcount, len(tab1))
        # Read the schema.
        schema = self.butler.get(self.datasetType.componentTypeName("schema"), dataId={})
        self.assertEqual(schema, ArrowNumpySchema(tab1.dtype))
        # Read just some columns a few different ways.
        tab3 = self.butler.get(self.datasetType, dataId={}, parameters={"columns": ["a", "c"]})
        _checkNumpyTableEquality(tab1[["a", "c"]], tab3)
        tab4 = self.butler.get(self.datasetType, dataId={}, parameters={"columns": "a"})
        _checkNumpyTableEquality(
            tab1[
                [
                    "a",
                ]
            ],
            tab4,
        )
        tab5 = self.butler.get(self.datasetType, dataId={}, parameters={"columns": ["index", "a"]})
        _checkNumpyTableEquality(tab1[["index", "a"]], tab5)
        tab6 = self.butler.get(self.datasetType, dataId={}, parameters={"columns": "ddd"})
        _checkNumpyTableEquality(
            tab1[
                [
                    "ddd",
                ]
            ],
            tab6,
        )
        tab7 = self.butler.get(self.datasetType, dataId={}, parameters={"columns": ["a", "a"]})
        _checkNumpyTableEquality(
            tab1[
                [
                    "a",
                ]
            ],
            tab7,
        )
        # Passing an unrecognized column should be a ValueError.
        with self.assertRaises(ValueError):
            self.butler.get(self.datasetType, dataId={}, parameters={"columns": ["e"]})

    def testNumpyTableBigEndian(self):
        tab1 = _makeSimpleNumpyTable(include_bigendian=True)

        self.butler.put(tab1, self.datasetType, dataId={})
        # Read the whole Table.
        tab2 = self.butler.get(self.datasetType, dataId={})
        _checkNumpyTableEquality(tab1, tab2, has_bigendian=True)

    def testArrowNumpySchema(self):
        tab1 = _makeSimpleNumpyTable(include_multidim=True)
        tab1_arrow = numpy_to_arrow(tab1)
        schema = ArrowNumpySchema.from_arrow(tab1_arrow.schema)

        self.assertIsInstance(schema.schema, np.dtype)
        self.assertEqual(repr(schema), repr(schema._dtype))
        self.assertNotEqual(schema, "not_a_schema")
        self.assertEqual(schema, schema)

        # Test inequality
        tab2 = tab1.copy()
        names = list(tab2.dtype.names)
        names[0] = "index2"
        tab2.dtype.names = names
        schema2 = ArrowNumpySchema(tab2.dtype)
        self.assertNotEqual(schema2, schema)

    @unittest.skipUnless(pa is not None, "Cannot test arrow conversions without pyarrow.")
    def testNumpyDictConversions(self):
        tab1 = _makeSimpleNumpyTable(include_multidim=True)

        # Verify that everything round-trips, including the schema.
        tab1_arrow = numpy_to_arrow(tab1)
        tab1_dict = arrow_to_numpy_dict(tab1_arrow)
        tab1_dict_arrow = numpy_dict_to_arrow(tab1_dict)

        self.assertEqual(tab1_arrow.schema, tab1_dict_arrow.schema)
        self.assertEqual(tab1_arrow, tab1_dict_arrow)

    @unittest.skipUnless(pa is not None, "Cannot test reading as arrow without pyarrow.")
    def testWriteNumpyTableReadAsArrowTable(self):
        tab1 = _makeSimpleNumpyTable(include_multidim=True)

        self.butler.put(tab1, self.datasetType, dataId={})

        tab2 = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowTable")

        tab2_numpy = arrow_to_numpy(tab2)

        _checkNumpyTableEquality(tab1, tab2_numpy)

        # Check reading the columns.
        columns = tab2.schema.names
        columns2 = self.butler.get(
            self.datasetType.componentTypeName("columns"), dataId={}, storageClass="ArrowColumnList"
        )
        self.assertEqual(columns2, columns)

        # Check reading the schema.
        schema = tab2.schema
        schema2 = self.butler.get(
            self.datasetType.componentTypeName("schema"), dataId={}, storageClass="ArrowSchema"
        )
        self.assertEqual(schema2, schema)

    @unittest.skipUnless(pd is not None, "Cannot test reading as a dataframe without pandas.")
    def testWriteNumpyTableReadAsDataFrame(self):
        tab1 = _makeSimpleNumpyTable()

        self.butler.put(tab1, self.datasetType, dataId={})

        tab2 = self.butler.get(self.datasetType, dataId={}, storageClass="DataFrame")

        # Converting this back to numpy gets confused with the index column
        # and changes the datatype of the string column.

        tab1_df = pd.DataFrame(tab1)

        self.assertTrue(tab1_df.equals(tab2))

        # Check reading the columns.
        columns = tab2.columns
        columns2 = self.butler.get(
            self.datasetType.componentTypeName("columns"), dataId={}, storageClass="DataFrameIndex"
        )
        self.assertTrue(columns.equals(columns2))

        # Check reading the schema.
        schema = DataFrameSchema(tab2)
        schema2 = self.butler.get(
            self.datasetType.componentTypeName("schema"), dataId={}, storageClass="DataFrameSchema"
        )

        self.assertEqual(schema2, schema)

    @unittest.skipUnless(atable is not None, "Cannot test reading as astropy without astropy.")
    def testWriteNumpyTableReadAsAstropyTable(self):
        tab1 = _makeSimpleNumpyTable(include_multidim=True)

        self.butler.put(tab1, self.datasetType, dataId={})

        tab2 = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowAstropy")
        tab2_numpy = tab2.as_array()

        _checkNumpyTableEquality(tab1, tab2_numpy)

        # Check reading the columns.
        columns = list(tab2.columns.keys())
        columns2 = self.butler.get(
            self.datasetType.componentTypeName("columns"), dataId={}, storageClass="ArrowColumnList"
        )
        self.assertEqual(columns2, columns)

        # Check reading the schema.
        schema = ArrowAstropySchema(tab2)
        schema2 = self.butler.get(
            self.datasetType.componentTypeName("schema"), dataId={}, storageClass="ArrowAstropySchema"
        )

        self.assertEqual(schema2, schema)

    def testWriteNumpyTableReadAsNumpyDict(self):
        tab1 = _makeSimpleNumpyTable(include_multidim=True)

        self.butler.put(tab1, self.datasetType, dataId={})

        tab2 = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowNumpyDict")
        tab2_numpy = _numpy_dict_to_numpy(tab2)

        _checkNumpyTableEquality(tab1, tab2_numpy)

    def testBadNumpyColumnParquet(self):
        tab1 = _makeSimpleAstropyTable()

        # Make a column with mixed type.
        bad_col1 = [0.0] * len(tab1)
        bad_col1[1] = 0.0 * units.nJy
        bad_tab = tab1.copy()
        bad_tab["bad_col1"] = bad_col1

        bad_tab_np = bad_tab.as_array()

        # At the moment we cannot check that the correct note is added
        # to the exception, but that will be possible in the future.
        with self.assertRaises(RuntimeError):
            self.butler.put(bad_tab_np, self.datasetType, dataId={})

        # Make a column with ragged size.
        bad_col2 = [[0]] * len(tab1)
        bad_col2[1] = [0, 0]
        bad_tab = tab1.copy()
        bad_tab["bad_col2"] = bad_col2

        bad_tab_np = bad_tab.as_array()

        with self.assertRaises(RuntimeError):
            self.butler.put(bad_tab_np, self.datasetType, dataId={})

    @unittest.skipUnless(atable is not None, "Cannot test reading as astropy without astropy.")
    def testWriteReadAstropyTableLossless(self):
        tab1 = _makeSimpleAstropyTable(include_multidim=True, include_masked=True)

        self.butler.put(tab1, self.datasetType, dataId={})

        tab2 = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowAstropy")

        _checkAstropyTableEquality(tab1, tab2)


@unittest.skipUnless(np is not None, "Cannot test ImMemoryDatastore with Numpy table without numpy.")
class InMemoryArrowNumpyDelegateTestCase(ParquetFormatterArrowNumpyTestCase):
    """Tests for InMemoryDatastore, using ArrowTableDelegate with
    Numpy table.
    """

    configFile = os.path.join(TESTDIR, "config/basic/butler-inmemory.yaml")

    def testBadNumpyColumnParquet(self):
        # This test does not raise for an in-memory datastore.
        pass

    def testBadInput(self):
        tab1 = _makeSimpleNumpyTable()
        delegate = ArrowTableDelegate("ArrowNumpy")

        with self.assertRaises(ValueError):
            delegate.handleParameters(inMemoryDataset="not_a_numpy_table")

        with self.assertRaises(NotImplementedError):
            delegate.handleParameters(inMemoryDataset=tab1, parameters={"columns": [("a", "b")]})

        with self.assertRaises(AttributeError):
            delegate.getComponent(composite=tab1, componentName="nothing")

    def testStorageClass(self):
        tab1 = _makeSimpleNumpyTable()

        factory = StorageClassFactory()
        factory.addFromConfig(StorageClassConfig())

        storageClass = factory.findStorageClass(type(tab1), compare_types=False)
        # Force the name lookup to do name matching.
        storageClass._pytype = None
        self.assertEqual(storageClass.name, "ArrowNumpy")

        storageClass = factory.findStorageClass(type(tab1), compare_types=True)
        # Force the name lookup to do name matching.
        storageClass._pytype = None
        self.assertEqual(storageClass.name, "ArrowNumpy")


@unittest.skipUnless(pa is not None, "Cannot test ParquetFormatterArrowTable without pyarrow.")
class ParquetFormatterArrowTableTestCase(unittest.TestCase):
    """Tests for ParquetFormatter, ArrowTable, using local file datastore."""

    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")

    def setUp(self):
        """Create a new butler root for each test."""
        self.root = makeTestTempDir(TESTDIR)
        config = Config(self.configFile)
        self.butler = Butler.from_config(
            Butler.makeRepo(self.root, config=config), writeable=True, run="test_run"
        )
        # No dimensions in dataset type so we don't have to worry about
        # inserting dimension data or defining data IDs.
        self.datasetType = DatasetType(
            "data", dimensions=(), storageClass="ArrowTable", universe=self.butler.dimensions
        )
        self.butler.registry.registerDatasetType(self.datasetType)

    def tearDown(self):
        removeTestTempDir(self.root)

    def testArrowTable(self):
        tab1 = _makeSimpleArrowTable(include_multidim=True, include_masked=True)

        self.butler.put(tab1, self.datasetType, dataId={})
        # Read the whole Table.
        tab2 = self.butler.get(self.datasetType, dataId={})
        # We convert to use the numpy testing framework to handle nan
        # comparisons.
        self.assertEqual(tab1.schema, tab2.schema)
        tab1_np = arrow_to_numpy(tab1)
        tab2_np = arrow_to_numpy(tab2)
        for col in tab1.column_names:
            np.testing.assert_array_equal(tab2_np[col], tab1_np[col])
        # Read the columns.
        columns2 = self.butler.get(self.datasetType.componentTypeName("columns"), dataId={})
        self.assertEqual(len(columns2), len(tab1.schema.names))
        for i, name in enumerate(tab1.schema.names):
            self.assertEqual(columns2[i], name)
        # Read the rowcount.
        rowcount = self.butler.get(self.datasetType.componentTypeName("rowcount"), dataId={})
        self.assertEqual(rowcount, len(tab1))
        # Read the schema.
        schema = self.butler.get(self.datasetType.componentTypeName("schema"), dataId={})
        self.assertEqual(schema, tab1.schema)
        # Read just some columns a few different ways.
        tab3 = self.butler.get(self.datasetType, dataId={}, parameters={"columns": ["a", "c"]})
        self.assertEqual(tab3, tab1.select(("a", "c")))
        tab4 = self.butler.get(self.datasetType, dataId={}, parameters={"columns": "a"})
        self.assertEqual(tab4, tab1.select(("a",)))
        tab5 = self.butler.get(self.datasetType, dataId={}, parameters={"columns": ["index", "a"]})
        self.assertEqual(tab5, tab1.select(("index", "a")))
        tab6 = self.butler.get(self.datasetType, dataId={}, parameters={"columns": "ddd"})
        self.assertEqual(tab6, tab1.select(("ddd",)))
        tab7 = self.butler.get(self.datasetType, dataId={}, parameters={"columns": ["a", "a"]})
        self.assertEqual(tab7, tab1.select(("a",)))
        # Passing an unrecognized column should be a ValueError.
        with self.assertRaises(ValueError):
            self.butler.get(self.datasetType, dataId={}, parameters={"columns": ["e"]})

    def testEmptyArrowTable(self):
        data = _makeSimpleNumpyTable()
        type_list = _numpy_dtype_to_arrow_types(data.dtype)

        schema = pa.schema(type_list)
        arrays = [[]] * len(schema.names)

        tab1 = pa.Table.from_arrays(arrays, schema=schema)

        self.butler.put(tab1, self.datasetType, dataId={})
        tab2 = self.butler.get(self.datasetType, dataId={})
        self.assertEqual(tab2, tab1)

        tab1_numpy = arrow_to_numpy(tab1)
        self.assertEqual(len(tab1_numpy), 0)
        tab1_numpy_arrow = numpy_to_arrow(tab1_numpy)
        self.assertEqual(tab1_numpy_arrow, tab1)

        tab1_pandas = arrow_to_pandas(tab1)
        self.assertEqual(len(tab1_pandas), 0)
        tab1_pandas_arrow = pandas_to_arrow(tab1_pandas)
        # Unfortunately, string/byte columns get mangled when translated
        # through empty pandas dataframes.
        self.assertEqual(
            tab1_pandas_arrow.select(("index", "a", "b", "c", "ddd")),
            tab1.select(("index", "a", "b", "c", "ddd")),
        )

        tab1_astropy = arrow_to_astropy(tab1)
        self.assertEqual(len(tab1_astropy), 0)
        tab1_astropy_arrow = astropy_to_arrow(tab1_astropy)
        self.assertEqual(tab1_astropy_arrow, tab1)

    def testEmptyArrowTableMultidim(self):
        data = _makeSimpleNumpyTable(include_multidim=True)
        type_list = _numpy_dtype_to_arrow_types(data.dtype)

        md = {}
        for name in data.dtype.names:
            _append_numpy_multidim_metadata(md, name, data.dtype[name])

        schema = pa.schema(type_list, metadata=md)
        arrays = [[]] * len(schema.names)

        tab1 = pa.Table.from_arrays(arrays, schema=schema)

        self.butler.put(tab1, self.datasetType, dataId={})
        tab2 = self.butler.get(self.datasetType, dataId={})
        self.assertEqual(tab2, tab1)

        tab1_numpy = arrow_to_numpy(tab1)
        self.assertEqual(len(tab1_numpy), 0)
        tab1_numpy_arrow = numpy_to_arrow(tab1_numpy)
        self.assertEqual(tab1_numpy_arrow, tab1)

        tab1_astropy = arrow_to_astropy(tab1)
        self.assertEqual(len(tab1_astropy), 0)
        tab1_astropy_arrow = astropy_to_arrow(tab1_astropy)
        self.assertEqual(tab1_astropy_arrow, tab1)

    @unittest.skipUnless(pd is not None, "Cannot test reading as a dataframe without pandas.")
    def testWriteArrowTableReadAsSingleIndexDataFrame(self):
        df1, allColumns = _makeSingleIndexDataFrame()

        self.butler.put(df1, self.datasetType, dataId={})

        # Read back out as a dataframe.
        df2 = self.butler.get(self.datasetType, dataId={}, storageClass="DataFrame")
        self.assertTrue(df1.equals(df2))

        # Read back out as an arrow table, convert to dataframe.
        tab3 = self.butler.get(self.datasetType, dataId={})
        df3 = arrow_to_pandas(tab3)
        self.assertTrue(df1.equals(df3))

        # Check reading the columns.
        columns = df2.reset_index().columns
        columns2 = self.butler.get(
            self.datasetType.componentTypeName("columns"), dataId={}, storageClass="DataFrameIndex"
        )
        # We check the set because pandas reorders the columns.
        self.assertEqual(set(columns2.to_list()), set(columns.to_list()))

        # Check reading the schema.
        schema = DataFrameSchema(df1)
        schema2 = self.butler.get(
            self.datasetType.componentTypeName("schema"), dataId={}, storageClass="DataFrameSchema"
        )
        self.assertEqual(schema2, schema)

    @unittest.skipUnless(pd is not None, "Cannot test reading as a dataframe without pandas.")
    def testWriteArrowTableReadAsMultiIndexDataFrame(self):
        df1 = _makeMultiIndexDataFrame()

        self.butler.put(df1, self.datasetType, dataId={})

        # Read back out as a dataframe.
        df2 = self.butler.get(self.datasetType, dataId={}, storageClass="DataFrame")
        self.assertTrue(df1.equals(df2))

        # Read back out as an arrow table, convert to dataframe.
        atab3 = self.butler.get(self.datasetType, dataId={})
        df3 = arrow_to_pandas(atab3)
        self.assertTrue(df1.equals(df3))

        # Check reading the columns.
        columns = df2.columns
        columns2 = self.butler.get(
            self.datasetType.componentTypeName("columns"), dataId={}, storageClass="DataFrameIndex"
        )
        self.assertTrue(columns2.equals(columns))

        # Check reading the schema.
        schema = DataFrameSchema(df1)
        schema2 = self.butler.get(
            self.datasetType.componentTypeName("schema"), dataId={}, storageClass="DataFrameSchema"
        )
        self.assertEqual(schema2, schema)

    @unittest.skipUnless(atable is not None, "Cannot test reading as astropy without astropy.")
    def testWriteArrowTableReadAsAstropyTable(self):
        tab1 = _makeSimpleAstropyTable(include_multidim=True, include_masked=True)

        self.butler.put(tab1, self.datasetType, dataId={})

        # Read back out as an astropy table.
        tab2 = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowAstropy")
        _checkAstropyTableEquality(tab1, tab2)

        # Read back out as an arrow table, convert to astropy table.
        atab3 = self.butler.get(self.datasetType, dataId={})
        tab3 = arrow_to_astropy(atab3)
        _checkAstropyTableEquality(tab1, tab3)

        # Check reading the columns.
        columns = list(tab2.columns.keys())
        columns2 = self.butler.get(
            self.datasetType.componentTypeName("columns"), dataId={}, storageClass="ArrowColumnList"
        )
        self.assertEqual(columns2, columns)

        # Check reading the schema.
        schema = ArrowAstropySchema(tab1)
        schema2 = self.butler.get(
            self.datasetType.componentTypeName("schema"), dataId={}, storageClass="ArrowAstropySchema"
        )
        self.assertEqual(schema2, schema)

        # Check the schema conversions and units.
        arrow_schema = schema.to_arrow_schema()
        for name in arrow_schema.names:
            field_metadata = arrow_schema.field(name).metadata
            if (
                b"description" in field_metadata
                and (description := field_metadata[b"description"].decode("UTF-8")) != ""
            ):
                self.assertEqual(schema2.schema[name].description, description)
            else:
                self.assertIsNone(schema2.schema[name].description)
            if b"unit" in field_metadata and (unit := field_metadata[b"unit"].decode("UTF-8")) != "":
                self.assertEqual(schema2.schema[name].unit, units.Unit(unit))

    @unittest.skipUnless(np is not None, "Cannot test reading as numpy without numpy.")
    def testWriteArrowTableReadAsNumpyTable(self):
        tab1 = _makeSimpleNumpyTable(include_multidim=True)

        self.butler.put(tab1, self.datasetType, dataId={})

        # Read back out as a numpy table.
        tab2 = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowNumpy")
        _checkNumpyTableEquality(tab1, tab2)

        # Read back out as an arrow table, convert to numpy table.
        atab3 = self.butler.get(self.datasetType, dataId={})
        tab3 = arrow_to_numpy(atab3)
        _checkNumpyTableEquality(tab1, tab3)

        # Check reading the columns.
        columns = list(tab2.dtype.names)
        columns2 = self.butler.get(
            self.datasetType.componentTypeName("columns"), dataId={}, storageClass="ArrowColumnList"
        )
        self.assertEqual(columns2, columns)

        # Check reading the schema.
        schema = ArrowNumpySchema(tab1.dtype)
        schema2 = self.butler.get(
            self.datasetType.componentTypeName("schema"), dataId={}, storageClass="ArrowNumpySchema"
        )
        self.assertEqual(schema2, schema)

    @unittest.skipUnless(np is not None, "Cannot test reading as numpy without numpy.")
    def testWriteArrowTableReadAsNumpyDict(self):
        tab1 = _makeSimpleNumpyTable(include_multidim=True)

        self.butler.put(tab1, self.datasetType, dataId={})

        tab2 = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowNumpyDict")
        tab2_numpy = _numpy_dict_to_numpy(tab2)
        _checkNumpyTableEquality(tab1, tab2_numpy)

    @unittest.skipUnless(atable is not None, "Cannot test reading as astropy without astropy.")
    def testWriteReadAstropyTableLossless(self):
        tab1 = _makeSimpleAstropyTable(include_multidim=True, include_masked=True)

        self.butler.put(tab1, self.datasetType, dataId={})

        tab2 = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowAstropy")

        _checkAstropyTableEquality(tab1, tab2)


@unittest.skipUnless(pa is not None, "Cannot test InMemoryDatastore with ArroWTable without pyarrow.")
class InMemoryArrowTableDelegateTestCase(ParquetFormatterArrowTableTestCase):
    """Tests for InMemoryDatastore, using ArrowTableDelegate."""

    configFile = os.path.join(TESTDIR, "config/basic/butler-inmemory.yaml")

    def testBadInput(self):
        tab1 = _makeSimpleArrowTable()
        delegate = ArrowTableDelegate("ArrowTable")

        with self.assertRaises(ValueError):
            delegate.handleParameters(inMemoryDataset="not_an_arrow_table")

        with self.assertRaises(NotImplementedError):
            delegate.handleParameters(inMemoryDataset=tab1, parameters={"columns": [("a", "b")]})

        with self.assertRaises(AttributeError):
            delegate.getComponent(composite=tab1, componentName="nothing")

    def testStorageClass(self):
        tab1 = _makeSimpleArrowTable()

        factory = StorageClassFactory()
        factory.addFromConfig(StorageClassConfig())

        storageClass = factory.findStorageClass(type(tab1), compare_types=False)
        # Force the name lookup to do name matching.
        storageClass._pytype = None
        self.assertEqual(storageClass.name, "ArrowTable")

        storageClass = factory.findStorageClass(type(tab1), compare_types=True)
        # Force the name lookup to do name matching.
        storageClass._pytype = None
        self.assertEqual(storageClass.name, "ArrowTable")


@unittest.skipUnless(np is not None, "Cannot test ParquetFormatterArrowNumpy without numpy.")
@unittest.skipUnless(pa is not None, "Cannot test ParquetFormatterArrowNumpy without pyarrow.")
class ParquetFormatterArrowNumpyDictTestCase(unittest.TestCase):
    """Tests for ParquetFormatter, ArrowNumpyDict, using local file store."""

    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")

    def setUp(self):
        """Create a new butler root for each test."""
        self.root = makeTestTempDir(TESTDIR)
        config = Config(self.configFile)
        self.butler = Butler.from_config(
            Butler.makeRepo(self.root, config=config), writeable=True, run="test_run"
        )
        # No dimensions in dataset type so we don't have to worry about
        # inserting dimension data or defining data IDs.
        self.datasetType = DatasetType(
            "data", dimensions=(), storageClass="ArrowNumpyDict", universe=self.butler.dimensions
        )
        self.butler.registry.registerDatasetType(self.datasetType)

    def tearDown(self):
        removeTestTempDir(self.root)

    def testNumpyDict(self):
        tab1 = _makeSimpleNumpyTable(include_multidim=True)
        dict1 = _numpy_to_numpy_dict(tab1)

        self.butler.put(dict1, self.datasetType, dataId={})
        # Read the whole table.
        dict2 = self.butler.get(self.datasetType, dataId={})
        _checkNumpyDictEquality(dict1, dict2)
        # Read the columns.
        columns2 = self.butler.get(self.datasetType.componentTypeName("columns"), dataId={})
        self.assertEqual(len(columns2), len(dict1.keys()))
        for name in dict1:
            self.assertIn(name, columns2)
        # Read the rowcount.
        rowcount = self.butler.get(self.datasetType.componentTypeName("rowcount"), dataId={})
        self.assertEqual(rowcount, len(dict1["a"]))
        # Read the schema.
        schema = self.butler.get(self.datasetType.componentTypeName("schema"), dataId={})
        self.assertEqual(schema, ArrowNumpySchema(tab1.dtype))
        # Read just some columns a few different ways.
        tab3 = self.butler.get(self.datasetType, dataId={}, parameters={"columns": ["a", "c"]})
        subdict = {key: dict1[key] for key in ["a", "c"]}
        _checkNumpyDictEquality(subdict, tab3)
        tab4 = self.butler.get(self.datasetType, dataId={}, parameters={"columns": "a"})
        subdict = {key: dict1[key] for key in ["a"]}
        _checkNumpyDictEquality(subdict, tab4)
        tab5 = self.butler.get(self.datasetType, dataId={}, parameters={"columns": ["index", "a"]})
        subdict = {key: dict1[key] for key in ["index", "a"]}
        _checkNumpyDictEquality(subdict, tab5)
        tab6 = self.butler.get(self.datasetType, dataId={}, parameters={"columns": "ddd"})
        subdict = {key: dict1[key] for key in ["ddd"]}
        _checkNumpyDictEquality(subdict, tab6)
        tab7 = self.butler.get(self.datasetType, dataId={}, parameters={"columns": ["a", "a"]})
        subdict = {key: dict1[key] for key in ["a"]}
        _checkNumpyDictEquality(subdict, tab7)
        # Passing an unrecognized column should be a ValueError.
        with self.assertRaises(ValueError):
            self.butler.get(self.datasetType, dataId={}, parameters={"columns": ["e"]})

    @unittest.skipUnless(pa is not None, "Cannot test reading as arrow without pyarrow.")
    def testWriteNumpyDictReadAsArrowTable(self):
        tab1 = _makeSimpleNumpyTable(include_multidim=True)
        dict1 = _numpy_to_numpy_dict(tab1)

        self.butler.put(dict1, self.datasetType, dataId={})

        tab2 = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowTable")

        tab2_dict = arrow_to_numpy_dict(tab2)

        _checkNumpyDictEquality(dict1, tab2_dict)

    @unittest.skipUnless(pd is not None, "Cannot test reading as a dataframe without pandas.")
    def testWriteNumpyDictReadAsDataFrame(self):
        tab1 = _makeSimpleNumpyTable()
        dict1 = _numpy_to_numpy_dict(tab1)

        self.butler.put(dict1, self.datasetType, dataId={})

        tab2 = self.butler.get(self.datasetType, dataId={}, storageClass="DataFrame")

        # The order of the dict may get mixed up, so we need to check column
        # by column. We also need to do this in dataframe form because pandas
        # changes the datatype of the string column.
        tab1_df = pd.DataFrame(tab1)

        self.assertEqual(set(tab1_df.columns), set(tab2.columns))
        for col in tab1_df.columns:
            self.assertTrue(np.all(tab1_df[col].values == tab2[col].values))

    @unittest.skipUnless(atable is not None, "Cannot test reading as astropy without astropy.")
    def testWriteNumpyDictReadAsAstropyTable(self):
        tab1 = _makeSimpleNumpyTable(include_multidim=True)
        dict1 = _numpy_to_numpy_dict(tab1)

        self.butler.put(dict1, self.datasetType, dataId={})

        tab2 = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowAstropy")
        tab2_dict = _astropy_to_numpy_dict(tab2)

        _checkNumpyDictEquality(dict1, tab2_dict)

    def testWriteNumpyDictReadAsNumpyTable(self):
        tab1 = _makeSimpleNumpyTable(include_multidim=True)
        dict1 = _numpy_to_numpy_dict(tab1)

        self.butler.put(dict1, self.datasetType, dataId={})

        tab2 = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowNumpy")
        tab2_dict = _numpy_to_numpy_dict(tab2)

        _checkNumpyDictEquality(dict1, tab2_dict)

    def testWriteNumpyDictBad(self):
        dict1 = {"a": 4, "b": np.ndarray([1])}
        with self.assertRaises(RuntimeError):
            self.butler.put(dict1, self.datasetType, dataId={})

        dict2 = {"a": np.zeros(4), "b": np.zeros(5)}
        with self.assertRaises(RuntimeError):
            self.butler.put(dict2, self.datasetType, dataId={})

        dict3 = {"a": [0] * 5, "b": np.zeros(5)}
        with self.assertRaises(RuntimeError):
            self.butler.put(dict3, self.datasetType, dataId={})

        dict4 = {"a": np.zeros(4), "b": np.zeros(4, dtype="O")}
        with self.assertRaises(RuntimeError):
            self.butler.put(dict4, self.datasetType, dataId={})

    @unittest.skipUnless(atable is not None, "Cannot test reading as astropy without astropy.")
    def testWriteReadAstropyTableLossless(self):
        tab1 = _makeSimpleAstropyTable(include_multidim=True, include_masked=True)

        self.butler.put(tab1, self.datasetType, dataId={})

        tab2 = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowAstropy")

        _checkAstropyTableEquality(tab1, tab2)


@unittest.skipUnless(np is not None, "Cannot test InMemoryDatastore with NumpyDict without numpy.")
@unittest.skipUnless(pa is not None, "Cannot test InMemoryDatastore with NumpyDict without pyarrow.")
class InMemoryNumpyDictDelegateTestCase(ParquetFormatterArrowNumpyDictTestCase):
    """Tests for InMemoryDatastore, using ArrowTableDelegate with
    Numpy dict.
    """

    configFile = os.path.join(TESTDIR, "config/basic/butler-inmemory.yaml")

    def testWriteNumpyDictBad(self):
        # The sub-type checking is not done on in-memory datastore.
        pass


@unittest.skipUnless(pa is not None, "Cannot test ArrowSchema without pyarrow.")
class ParquetFormatterArrowSchemaTestCase(unittest.TestCase):
    """Tests for ParquetFormatter, ArrowSchema, using local file datastore."""

    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")

    def setUp(self):
        """Create a new butler root for each test."""
        self.root = makeTestTempDir(TESTDIR)
        config = Config(self.configFile)
        self.butler = Butler.from_config(
            Butler.makeRepo(self.root, config=config), writeable=True, run="test_run"
        )
        # No dimensions in dataset type so we don't have to worry about
        # inserting dimension data or defining data IDs.
        self.datasetType = DatasetType(
            "data", dimensions=(), storageClass="ArrowSchema", universe=self.butler.dimensions
        )
        self.butler.registry.registerDatasetType(self.datasetType)

    def tearDown(self):
        removeTestTempDir(self.root)

    def _makeTestSchema(self):
        schema = pa.schema(
            [
                pa.field(
                    "int32",
                    pa.int32(),
                    nullable=False,
                    metadata={
                        "description": "32-bit integer",
                        "unit": "",
                    },
                ),
                pa.field(
                    "int64",
                    pa.int64(),
                    nullable=False,
                    metadata={
                        "description": "64-bit integer",
                        "unit": "",
                    },
                ),
                pa.field(
                    "uint64",
                    pa.uint64(),
                    nullable=False,
                    metadata={
                        "description": "64-bit unsigned integer",
                        "unit": "",
                    },
                ),
                pa.field(
                    "float32",
                    pa.float32(),
                    nullable=False,
                    metadata={
                        "description": "32-bit float",
                        "unit": "count",
                    },
                ),
                pa.field(
                    "float64",
                    pa.float64(),
                    nullable=False,
                    metadata={
                        "description": "64-bit float",
                        "unit": "nJy",
                    },
                ),
                pa.field(
                    "fixed_size_list",
                    pa.list_(pa.float64(), list_size=10),
                    nullable=False,
                    metadata={
                        "description": "Fixed size list of 64-bit floats.",
                        "unit": "nJy",
                    },
                ),
                pa.field(
                    "variable_size_list",
                    pa.list_(pa.float64()),
                    nullable=False,
                    metadata={
                        "description": "Variable size list of 64-bit floats.",
                        "unit": "nJy",
                    },
                ),
                # One of these fields will have no description.
                pa.field(
                    "string",
                    pa.string(),
                    nullable=False,
                    metadata={
                        "unit": "",
                    },
                ),
                # One of these fields will have no metadata.
                pa.field(
                    "binary",
                    pa.binary(),
                    nullable=False,
                ),
            ]
        )

        return schema

    def testArrowSchema(self):
        schema1 = self._makeTestSchema()
        self.butler.put(schema1, self.datasetType, dataId={})

        schema2 = self.butler.get(self.datasetType, dataId={})
        self.assertEqual(schema2, schema1)

    @unittest.skipUnless(pd is not None, "Cannot test reading as a dataframe schema without pandas.")
    def testWriteArrowSchemaReadAsDataFrameSchema(self):
        schema1 = self._makeTestSchema()
        self.butler.put(schema1, self.datasetType, dataId={})

        df_schema1 = DataFrameSchema.from_arrow(schema1)

        df_schema2 = self.butler.get(self.datasetType, dataId={}, storageClass="DataFrameSchema")
        self.assertEqual(df_schema2, df_schema1)

    @unittest.skipUnless(atable is not None, "Cannot test reading as an astropy schema without astropy.")
    def testWriteArrowSchemaReadAsArrowAstropySchema(self):
        schema1 = self._makeTestSchema()
        self.butler.put(schema1, self.datasetType, dataId={})

        ap_schema1 = ArrowAstropySchema.from_arrow(schema1)

        ap_schema2 = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowAstropySchema")
        self.assertEqual(ap_schema2, ap_schema1)

        # Confirm that the ap_schema2 has the unit/description we expect.
        for name in schema1.names:
            field_metadata = schema1.field(name).metadata
            if field_metadata is None:
                continue
            if (
                b"description" in field_metadata
                and (description := field_metadata[b"description"].decode("UTF-8")) != ""
            ):
                self.assertEqual(ap_schema2.schema[name].description, description)
            else:
                self.assertIsNone(ap_schema2.schema[name].description)
            if b"unit" in field_metadata and (unit := field_metadata[b"unit"].decode("UTF-8")) != "":
                self.assertEqual(ap_schema2.schema[name].unit, units.Unit(unit))

    @unittest.skipUnless(atable is not None, "Cannot test reading as an numpy schema without numpy.")
    def testWriteArrowSchemaReadAsArrowNumpySchema(self):
        schema1 = self._makeTestSchema()
        self.butler.put(schema1, self.datasetType, dataId={})

        np_schema1 = ArrowNumpySchema.from_arrow(schema1)

        np_schema2 = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowNumpySchema")
        self.assertEqual(np_schema2, np_schema1)


@unittest.skipUnless(pa is not None, "Cannot test InMemoryDatastore with ArrowSchema without pyarrow.")
class InMemoryArrowSchemaDelegateTestCase(ParquetFormatterArrowSchemaTestCase):
    """Tests for InMemoryDatastore and ArrowSchema."""

    configFile = os.path.join(TESTDIR, "config/basic/butler-inmemory.yaml")


@unittest.skipUnless(pa is not None, "Cannot test S3 without pyarrow.")
@unittest.skipUnless(boto3 is not None, "Cannot test S3 without boto3.")
@unittest.skipUnless(fsspec is not None, "Cannot test S3 without fsspec.")
@unittest.skipUnless(s3fs is not None, "Cannot test S3 without s3fs.")
class ParquetFormatterArrowTableS3TestCase(unittest.TestCase):
    """Tests for arrow table/parquet with S3."""

    # Code is adapted from test_butler.py
    configFile = os.path.join(TESTDIR, "config/basic/butler-s3store.yaml")
    fullConfigKey = None
    validationCanFail = True

    bucketName = "anybucketname"

    root = "butlerRoot/"

    datastoreStr = [f"datastore={root}"]

    datastoreName = ["FileDatastore@s3://{bucketName}/{root}"]

    registryStr = "/gen3.sqlite3"

    mock_aws = mock_aws()

    def setUp(self):
        self.root = makeTestTempDir(TESTDIR)

        config = Config(self.configFile)
        uri = ResourcePath(config[".datastore.datastore.root"])
        self.bucketName = uri.netloc

        # Enable S3 mocking of tests.
        self.enterContext(clean_test_environment_for_s3())
        self.mock_aws.start()

        rooturi = f"s3://{self.bucketName}/{self.root}"
        config.update({"datastore": {"datastore": {"root": rooturi}}})

        # need local folder to store registry database
        self.reg_dir = makeTestTempDir(TESTDIR)
        config["registry", "db"] = f"sqlite:///{self.reg_dir}/gen3.sqlite3"

        # MOTO needs to know that we expect Bucket bucketname to exist
        # (this used to be the class attribute bucketName)
        s3 = boto3.resource("s3")
        s3.create_bucket(Bucket=self.bucketName)

        self.datastoreStr = [f"datastore='{rooturi}'"]
        self.datastoreName = [f"FileDatastore@{rooturi}"]
        Butler.makeRepo(rooturi, config=config, forceConfigRoot=False)
        self.tmpConfigFile = posixpath.join(rooturi, "butler.yaml")

        self.butler = Butler(self.tmpConfigFile, writeable=True, run="test_run")

        # No dimensions in dataset type so we don't have to worry about
        # inserting dimension data or defining data IDs.
        self.datasetType = DatasetType(
            "data", dimensions=(), storageClass="ArrowTable", universe=self.butler.dimensions
        )
        self.butler.registry.registerDatasetType(self.datasetType)

    def tearDown(self):
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(self.bucketName)
        try:
            bucket.objects.all().delete()
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                # the key was not reachable - pass
                pass
            else:
                raise

        bucket = s3.Bucket(self.bucketName)
        bucket.delete()

        # Stop the S3 mock.
        self.mock_aws.stop()

        if self.reg_dir is not None and os.path.exists(self.reg_dir):
            shutil.rmtree(self.reg_dir, ignore_errors=True)

        if os.path.exists(self.root):
            shutil.rmtree(self.root, ignore_errors=True)

    def testArrowTableS3(self):
        tab1 = _makeSimpleArrowTable(include_multidim=True, include_masked=True)

        self.butler.put(tab1, self.datasetType, dataId={})

        # Read the whole Table.
        tab2 = self.butler.get(self.datasetType, dataId={})
        # We convert to use the numpy testing framework to handle nan
        # comparisons.
        self.assertEqual(tab1.schema, tab2.schema)
        tab1_np = arrow_to_numpy(tab1)
        tab2_np = arrow_to_numpy(tab2)
        for col in tab1.column_names:
            np.testing.assert_array_equal(tab2_np[col], tab1_np[col])
        # Read the columns.
        columns2 = self.butler.get(self.datasetType.componentTypeName("columns"), dataId={})
        self.assertEqual(len(columns2), len(tab1.schema.names))
        for i, name in enumerate(tab1.schema.names):
            self.assertEqual(columns2[i], name)
        # Read the rowcount.
        rowcount = self.butler.get(self.datasetType.componentTypeName("rowcount"), dataId={})
        self.assertEqual(rowcount, len(tab1))
        # Read the schema.
        schema = self.butler.get(self.datasetType.componentTypeName("schema"), dataId={})
        self.assertEqual(schema, tab1.schema)
        # Read just some columns a few different ways.
        tab3 = self.butler.get(self.datasetType, dataId={}, parameters={"columns": ["a", "c"]})
        self.assertEqual(tab3, tab1.select(("a", "c")))
        tab4 = self.butler.get(self.datasetType, dataId={}, parameters={"columns": "a"})
        self.assertEqual(tab4, tab1.select(("a",)))
        tab5 = self.butler.get(self.datasetType, dataId={}, parameters={"columns": ["index", "a"]})
        self.assertEqual(tab5, tab1.select(("index", "a")))
        tab6 = self.butler.get(self.datasetType, dataId={}, parameters={"columns": "ddd"})
        self.assertEqual(tab6, tab1.select(("ddd",)))
        tab7 = self.butler.get(self.datasetType, dataId={}, parameters={"columns": ["a", "a"]})
        self.assertEqual(tab7, tab1.select(("a",)))
        # Passing an unrecognized column should be a ValueError.
        with self.assertRaises(ValueError):
            self.butler.get(self.datasetType, dataId={}, parameters={"columns": ["e"]})


@unittest.skipUnless(np is not None, "Cannot test compute_row_group_size without numpy.")
@unittest.skipUnless(pa is not None, "Cannot test compute_row_group_size without pyarrow.")
class ComputeRowGroupSizeTestCase(unittest.TestCase):
    """Tests for compute_row_group_size."""

    def testRowGroupSizeNoMetadata(self):
        numpyTable = _makeSimpleNumpyTable(include_multidim=True)

        # We can't use the numpy_to_arrow convenience function because
        # that adds metadata.
        type_list = _numpy_dtype_to_arrow_types(numpyTable.dtype)
        schema = pa.schema(type_list)
        arrays = _numpy_style_arrays_to_arrow_arrays(
            numpyTable.dtype,
            len(numpyTable),
            numpyTable,
            schema,
        )
        arrowTable = pa.Table.from_arrays(arrays, schema=schema)

        row_group_size = compute_row_group_size(arrowTable.schema)

        self.assertGreater(row_group_size, 1_000_000)
        self.assertLess(row_group_size, 2_000_000)

    def testRowGroupSizeWithMetadata(self):
        numpyTable = _makeSimpleNumpyTable(include_multidim=True)

        arrowTable = numpy_to_arrow(numpyTable)

        row_group_size = compute_row_group_size(arrowTable.schema)

        self.assertGreater(row_group_size, 1_000_000)
        self.assertLess(row_group_size, 2_000_000)

    def testRowGroupSizeTinyTable(self):
        numpyTable = np.zeros(1, dtype=[("a", np.bool_)])

        arrowTable = numpy_to_arrow(numpyTable)

        row_group_size = compute_row_group_size(arrowTable.schema)

        self.assertGreater(row_group_size, 1_000_000)

    @unittest.skipUnless(pd is not None, "Cannot run testRowGroupSizeDataFrameWithLists without pandas.")
    def testRowGroupSizeDataFrameWithLists(self):
        df = pd.DataFrame({"a": np.zeros(10), "b": [[0, 0]] * 10, "c": [[0.0, 0.0]] * 10, "d": [[]] * 10})
        arrowTable = pandas_to_arrow(df)
        row_group_size = compute_row_group_size(arrowTable.schema)

        self.assertGreater(row_group_size, 1_000_000)


def _checkAstropyTableEquality(table1, table2, skip_units=False, has_bigendian=False):
    """Check if two astropy tables have the same columns/values.

    Parameters
    ----------
    table1 : `astropy.table.Table`
    table2 : `astropy.table.Table`
    skip_units : `bool`
    has_bigendian : `bool`
    """
    if not has_bigendian:
        assert table1.dtype == table2.dtype
    else:
        for name in table1.dtype.names:
            # Only check type matches, force to little-endian.
            assert table1.dtype[name].newbyteorder(">") == table2.dtype[name].newbyteorder(">")

    # Strip provenance before comparison.
    DatasetProvenance.strip_provenance_from_flat_dict(table1.meta)
    DatasetProvenance.strip_provenance_from_flat_dict(table2.meta)
    assert table1.meta == table2.meta
    if not skip_units:
        for name in table1.columns:
            assert table1[name].unit == table2[name].unit
            assert table1[name].description == table2[name].description
            assert table1[name].format == table2[name].format

    for name in table1.columns:
        # We need to check masked/regular columns after filling.
        has_masked = False
        if isinstance(table1[name], atable.column.MaskedColumn):
            c1 = table1[name].filled()
            has_masked = True
        else:
            c1 = np.array(table1[name])
        if has_masked:
            assert isinstance(table2[name], atable.column.MaskedColumn)
            c2 = table2[name].filled()
        else:
            assert not isinstance(table2[name], atable.column.MaskedColumn)
            c2 = np.array(table2[name])
        np.testing.assert_array_equal(c1, c2)
        # If we have a masked column then we test the underlying data.
        if has_masked:
            np.testing.assert_array_equal(np.array(c1), np.array(c2))
            np.testing.assert_array_equal(table1[name].mask, table2[name].mask)


def _checkNumpyTableEquality(table1, table2, has_bigendian=False):
    """Check if two numpy tables have the same columns/values

    Parameters
    ----------
    table1 : `numpy.ndarray`
    table2 : `numpy.ndarray`
    has_bigendian : `bool`
    """
    assert table1.dtype.names == table2.dtype.names
    for name in table1.dtype.names:
        if not has_bigendian:
            assert table1.dtype[name] == table2.dtype[name]
        else:
            # Only check type matches, force to little-endian.
            assert table1.dtype[name].newbyteorder(">") == table2.dtype[name].newbyteorder(">")
        assert np.all(table1 == table2)


def _checkNumpyDictEquality(dict1, dict2):
    """Check if two numpy dicts have the same columns/values.

    Parameters
    ----------
    dict1 : `dict` [`str`, `np.ndarray`]
    dict2 : `dict` [`str`, `np.ndarray`]
    """
    assert set(dict1.keys()) == set(dict2.keys())
    for name in dict1:
        assert dict1[name].dtype == dict2[name].dtype
        assert np.all(dict1[name] == dict2[name])


if __name__ == "__main__":
    unittest.main()

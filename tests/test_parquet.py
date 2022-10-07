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

"""Tests for ParquetFormatter.

Tests in this module are disabled unless pandas and pyarrow are importable.
"""

import os
import unittest

import numpy as np
import pandas as pd
from astropy import units
from astropy.table import Table
from lsst.daf.butler import Butler, Config, DatasetType, StorageClassConfig, StorageClassFactory
from lsst.daf.butler.delegates.arrowtable import ArrowTableDelegate
from lsst.daf.butler.delegates.dataframe import DataFrameDelegate
from lsst.daf.butler.formatters.parquet import (
    ArrowAstropySchema,
    ArrowNumpySchema,
    DataFrameSchema,
    arrow_to_astropy,
    arrow_to_numpy,
    arrow_to_pandas,
    numpy_to_arrow,
)
from lsst.daf.butler.tests.utils import makeTestTempDir, removeTestTempDir

TESTDIR = os.path.abspath(os.path.dirname(__file__))


def _makeSimpleNumpyTable():
    """Make a simple numpy table with random data.

    Returns
    -------
    numpyTable : `numpy.ndarray`
    """
    nrow = 5
    data = np.zeros(
        nrow, dtype=[("index", "i4"), ("a", "f8"), ("b", "f8"), ("c", "f8"), ("ddd", "f8"), ("strcol", "U10")]
    )
    data["index"][:] = np.arange(nrow)
    data["a"] = np.random.randn(nrow)
    data["b"] = np.random.randn(nrow)
    data["c"] = np.random.randn(nrow)
    data["ddd"] = np.random.randn(nrow)
    data["strcol"][:] = "test"

    return data


def _makeSingleIndexDataFrame():
    """Make a single index data frame for testing.

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


def _makeSimpleAstropyTable():
    """Make an astropy table for testing.

    Returns
    -------
    astropyTable : `astropy.table.Table`
        The test table.
    """
    data = _makeSimpleNumpyTable()
    # Add a couple of units.
    table = Table(data)
    table["a"].unit = units.degree
    table["b"].unit = units.meter
    return table


def _makeSimpleArrowTable():
    """Make an arrow table for testing.

    Returns
    -------
    arrowTable : `pyarrow.Table`
        The test table.
    """
    data = _makeSimpleNumpyTable()
    return numpy_to_arrow(data)


class ParquetFormatterDataFrameTestCase(unittest.TestCase):
    """Tests for ParquetFormatter, DataFrame, using local file datastore."""

    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")

    def setUp(self):
        """Create a new butler root for each test."""
        self.root = makeTestTempDir(TESTDIR)
        config = Config(self.configFile)
        self.butler = Butler(Butler.makeRepo(self.root, config=config), writeable=True, run="test_run")
        # No dimensions in dataset type so we don't have to worry about
        # inserting dimension data or defining data IDs.
        self.datasetType = DatasetType(
            "data", dimensions=(), storageClass="DataFrame", universe=self.butler.registry.dimensions
        )
        self.butler.registry.registerDatasetType(self.datasetType)

    def tearDown(self):
        removeTestTempDir(self.root)

    def testSingleIndexDataFrame(self):
        df1, allColumns = _makeSingleIndexDataFrame()

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
        # Passing an unrecognized column should be a ValueError.
        with self.assertRaises(ValueError):
            self.butler.get(self.datasetType, dataId={}, parameters={"columns": ["e"]})

    def testMultiIndexDataFrame(self):
        df1 = _makeMultiIndexDataFrame()

        self.butler.put(df1, self.datasetType, dataId={})
        # Read the whole DataFrame.
        df2 = self.butler.get(self.datasetType, dataId={})
        self.assertTrue(df1.equals(df2))
        # Read just the column descriptions.
        columns2 = self.butler.get(self.datasetType.componentTypeName("columns"), dataId={})
        self.assertTrue(df1.columns.equals(columns2))
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
        # Passing an unrecognized column should be a ValueError.
        with self.assertRaises(ValueError):
            self.butler.get(self.datasetType, dataId={}, parameters={"columns": ["d"]})

    def testWriteSingleIndexDataFrameReadAsAstropyTable(self):
        df1, allColumns = _makeSingleIndexDataFrame()

        self.butler.put(df1, self.datasetType, dataId={})

        tab2 = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowAstropy")

        tab2_df = tab2.to_pandas(index="index")
        self.assertTrue(df1.equals(tab2_df))

    def testWriteMultiIndexDataFrameReadAsAstropyTable(self):
        df1 = _makeMultiIndexDataFrame()

        self.butler.put(df1, self.datasetType, dataId={})

        _ = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowAstropy")

        # This is an odd duck, it doesn't really round-trip.
        # This test simply checks that it's readable, but definitely not
        # recommended.

    def testWriteSingleIndexDataFrameReadAsArrowTable(self):
        df1, allColumns = _makeSingleIndexDataFrame()

        self.butler.put(df1, self.datasetType, dataId={})

        tab2 = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowTable")

        tab2_df = arrow_to_pandas(tab2)
        self.assertTrue(df1.equals(tab2_df))

    def testWriteMultiIndexDataFrameReadAsArrowTable(self):
        df1 = _makeMultiIndexDataFrame()

        self.butler.put(df1, self.datasetType, dataId={})

        tab2 = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowTable")

        tab2_df = arrow_to_pandas(tab2)
        self.assertTrue(df1.equals(tab2_df))

    def testWriteSingleIndexDataFrameReadAsNumpyTable(self):
        df1, allColumns = _makeSingleIndexDataFrame()

        self.butler.put(df1, self.datasetType, dataId={})

        tab2 = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowNumpy")

        tab2_df = pd.DataFrame.from_records(tab2, index=["index"])
        self.assertTrue(df1.equals(tab2_df))

    def testWriteMultiIndexDataFrameReadAsNumpyTable(self):
        df1 = _makeMultiIndexDataFrame()

        self.butler.put(df1, self.datasetType, dataId={})

        _ = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowNumpy")

        # This is an odd duck, it doesn't really round-trip.
        # This test simply checks that it's readable, but definitely not
        # recommended.


class InMemoryDataFrameDelegateTestCase(ParquetFormatterDataFrameTestCase):
    """Tests for InMemoryDatastore, using DataFrameDelegate."""

    configFile = os.path.join(TESTDIR, "config/basic/butler-inmemory.yaml")

    def testMultiIndexDataFrame(self):
        df1 = _makeMultiIndexDataFrame()

        delegate = DataFrameDelegate("DataFrame")

        # Read the whole DataFrame.
        df2 = delegate.handleParameters(inMemoryDataset=df1)
        self.assertTrue(df1.equals(df2))
        # Read just the column descriptions.
        columns2 = delegate.getComponent(composite=df1, componentName="columns")
        self.assertTrue(df1.columns.equals(columns2))

        # Read just some columns a few different ways.
        with self.assertRaises(NotImplementedError) as cm:
            delegate.handleParameters(inMemoryDataset=df1, parameters={"columns": {"filter": "g"}})
        self.assertIn("only supports string column names", str(cm.exception))
        with self.assertRaises(NotImplementedError) as cm:
            delegate.handleParameters(
                inMemoryDataset=df1, parameters={"columns": {"filter": ["r"], "column": "a"}}
            )
        self.assertIn("only supports string column names", str(cm.exception))

    def testWriteMultiIndexDataFrameReadAsAstropyTable(self):
        df1 = _makeMultiIndexDataFrame()

        self.butler.put(df1, self.datasetType, dataId={})

        with self.assertRaises(ValueError):
            _ = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowAstropy")

    def testBadInput(self):
        delegate = DataFrameDelegate("DataFrame")

        with self.assertRaises(ValueError):
            delegate.handleParameters(inMemoryDataset="not_a_dataframe")

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


class ParquetFormatterArrowAstropyTestCase(unittest.TestCase):
    """Tests for ParquetFormatter, ArrowAstropy, using local file datastore."""

    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")

    def setUp(self):
        """Create a new butler root for each test."""
        self.root = makeTestTempDir(TESTDIR)
        config = Config(self.configFile)
        self.butler = Butler(Butler.makeRepo(self.root, config=config), writeable=True, run="test_run")
        # No dimensions in dataset type so we don't have to worry about
        # inserting dimension data or defining data IDs.
        self.datasetType = DatasetType(
            "data", dimensions=(), storageClass="ArrowAstropy", universe=self.butler.registry.dimensions
        )
        self.butler.registry.registerDatasetType(self.datasetType)

    def tearDown(self):
        removeTestTempDir(self.root)

    def testAstropyTable(self):
        tab1 = _makeSimpleAstropyTable()

        self.butler.put(tab1, self.datasetType, dataId={})
        # Read the whole Table.
        tab2 = self.butler.get(self.datasetType, dataId={})
        self._checkAstropyTableEquality(tab1, tab2)
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
        self._checkAstropyTableEquality(tab1[("a", "c")], tab3)
        tab4 = self.butler.get(self.datasetType, dataId={}, parameters={"columns": "a"})
        self._checkAstropyTableEquality(tab1[("a",)], tab4)
        tab5 = self.butler.get(self.datasetType, dataId={}, parameters={"columns": ["index", "a"]})
        self._checkAstropyTableEquality(tab1[("index", "a")], tab5)
        tab6 = self.butler.get(self.datasetType, dataId={}, parameters={"columns": "ddd"})
        self._checkAstropyTableEquality(tab1[("ddd",)], tab6)
        # Passing an unrecognized column should be a ValueError.
        with self.assertRaises(ValueError):
            self.butler.get(self.datasetType, dataId={}, parameters={"columns": ["e"]})

    def testWriteAstropyReadAsArrowTable(self):
        tab1 = _makeSimpleAstropyTable()

        self.butler.put(tab1, self.datasetType, dataId={})

        tab2 = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowTable")

        tab2_astropy = arrow_to_astropy(tab2)
        self._checkAstropyTableEquality(tab1, tab2_astropy)

    def testWriteAstropyReadAsDataFrame(self):
        tab1 = _makeSimpleAstropyTable()

        self.butler.put(tab1, self.datasetType, dataId={})

        tab2 = self.butler.get(self.datasetType, dataId={}, storageClass="DataFrame")

        # This is tricky because it loses the units and gains a bonus pandas
        # _index_ column, so we just test the dataframe form.

        tab1_df = tab1.to_pandas()
        self.assertTrue(tab1_df.equals(tab2))

    def testWriteAstropyReadAsNumpyTable(self):
        tab1 = _makeSimpleAstropyTable()

        self.butler.put(tab1, self.datasetType, dataId={})

        tab2 = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowNumpy")

        # This is tricky because it loses the units.
        tab2_astropy = Table(tab2)

        self._checkAstropyTableEquality(tab1, tab2_astropy, skip_units=True)

    def _checkAstropyTableEquality(self, table1, table2, skip_units=False):
        """Check if two astropy tables have the same columns/values.

        Parameters
        ----------
        table1 : `astropy.table.Table`
        table2 : `astropy.table.Table`
        skip_units : `bool`
        """
        self.assertEqual(table1.dtype, table2.dtype)
        if not skip_units:
            for name in table1.columns:
                self.assertEqual(table1[name].unit, table2[name].unit)
                self.assertEqual(table1[name].description, table2[name].description)
                self.assertEqual(table1[name].format, table2[name].format)
        self.assertTrue(np.all(table1 == table2))


class ParquetFormatterArrowNumpyTestCase(unittest.TestCase):
    """Tests for ParquetFormatter, ArrowNumpy, using local file datastore."""

    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")

    def setUp(self):
        """Create a new butler root for each test."""
        self.root = makeTestTempDir(TESTDIR)
        config = Config(self.configFile)
        self.butler = Butler(Butler.makeRepo(self.root, config=config), writeable=True, run="test_run")
        # No dimensions in dataset type so we don't have to worry about
        # inserting dimension data or defining data IDs.
        self.datasetType = DatasetType(
            "data", dimensions=(), storageClass="ArrowNumpy", universe=self.butler.registry.dimensions
        )
        self.butler.registry.registerDatasetType(self.datasetType)

    def tearDown(self):
        removeTestTempDir(self.root)

    def testNumpyTable(self):
        tab1 = _makeSimpleNumpyTable()

        self.butler.put(tab1, self.datasetType, dataId={})
        # Read the whole Table.
        tab2 = self.butler.get(self.datasetType, dataId={})
        self._checkNumpyTableEquality(tab1, tab2)
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
        self._checkNumpyTableEquality(tab1[["a", "c"]], tab3)
        tab4 = self.butler.get(self.datasetType, dataId={}, parameters={"columns": "a"})
        self._checkNumpyTableEquality(
            tab1[
                [
                    "a",
                ]
            ],
            tab4,
        )
        tab5 = self.butler.get(self.datasetType, dataId={}, parameters={"columns": ["index", "a"]})
        self._checkNumpyTableEquality(tab1[["index", "a"]], tab5)
        tab6 = self.butler.get(self.datasetType, dataId={}, parameters={"columns": "ddd"})
        self._checkNumpyTableEquality(
            tab1[
                [
                    "ddd",
                ]
            ],
            tab6,
        )
        # Passing an unrecognized column should be a ValueError.
        with self.assertRaises(ValueError):
            self.butler.get(self.datasetType, dataId={}, parameters={"columns": ["e"]})

    def testWriteNumpyTableReadAsArrowTable(self):
        tab1 = _makeSimpleNumpyTable()

        self.butler.put(tab1, self.datasetType, dataId={})

        tab2 = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowTable")

        tab2_numpy = arrow_to_numpy(tab2)

        self._checkNumpyTableEquality(tab1, tab2_numpy)

    def testWriteNumpyTableReadAsDataFrame(self):
        tab1 = _makeSimpleNumpyTable()

        self.butler.put(tab1, self.datasetType, dataId={})

        tab2 = self.butler.get(self.datasetType, dataId={}, storageClass="DataFrame")

        # Converting this back to numpy gets confused with the index column
        # and changes the datatype of the string column.

        tab1_df = pd.DataFrame(tab1)

        self.assertTrue(tab1_df.equals(tab2))

    def testWriteNumpyTableReadAsAstropyTable(self):
        tab1 = _makeSimpleNumpyTable()

        self.butler.put(tab1, self.datasetType, dataId={})

        tab2 = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowAstropy")

        tab2_numpy = tab2.as_array()

        self._checkNumpyTableEquality(tab1, tab2_numpy)

    def _checkNumpyTableEquality(self, table1, table2):
        """Check if two numpy tables have the same columns/values

        Parameters
        ----------
        table1 : `numpy.ndarray`
        table2 : `numpy.ndarray`
        """
        self.assertEqual(table1.dtype.names, table2.dtype.names)
        for name in table1.dtype.names:
            self.assertEqual(table1.dtype[name], table2.dtype[name])
        self.assertTrue(np.all(table1 == table2))


class ParquetFormatterArrowTableTestCase(unittest.TestCase):
    """Tests for ParquetFormatter, ArrowTable, using local file datastore."""

    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")

    def setUp(self):
        """Create a new butler root for each test."""
        self.root = makeTestTempDir(TESTDIR)
        config = Config(self.configFile)
        self.butler = Butler(Butler.makeRepo(self.root, config=config), writeable=True, run="test_run")
        # No dimensions in dataset type so we don't have to worry about
        # inserting dimension data or defining data IDs.
        self.datasetType = DatasetType(
            "data", dimensions=(), storageClass="ArrowTable", universe=self.butler.registry.dimensions
        )
        self.butler.registry.registerDatasetType(self.datasetType)

    def tearDown(self):
        removeTestTempDir(self.root)

    def testArrowTable(self):
        tab1 = _makeSimpleArrowTable()

        self.butler.put(tab1, self.datasetType, dataId={})
        # Read the whole Table.
        tab2 = self.butler.get(self.datasetType, dataId={})
        self.assertEqual(tab2, tab1)
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
        # Passing an unrecognized column should be a ValueError.
        with self.assertRaises(ValueError):
            self.butler.get(self.datasetType, dataId={}, parameters={"columns": ["e"]})

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

    def tesWritetArrowTableReadAsMultiIndexDataFrame(self):
        df1 = _makeMultiIndexDataFrame()

        self.butler.put(df1, self.datasetType, dataId={})

        # Read back out as a dataframe.
        df2 = self.butler.get(self.datasetType, dataId={}, storageClass="DataFrame")
        self.assertTrue(df1.equals(df2))

        # Read back out as an arrow table, convert to dataframe.
        atab3 = self.butler.get(self.datasetType, dataId={})
        df3 = arrow_to_pandas(atab3)
        self.assertTrue(df1.equals(df3))

    def testWriteArrowTableReadAsAstropyTable(self):
        tab1 = _makeSimpleAstropyTable()

        self.butler.put(tab1, self.datasetType, dataId={})

        # Read back out as an astropy table.
        tab2 = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowAstropy")
        self._checkAstropyTableEquality(tab1, tab2)

        # Read back out as an arrow table, convert to astropy table.
        atab3 = self.butler.get(self.datasetType, dataId={})
        tab3 = arrow_to_astropy(atab3)
        self._checkAstropyTableEquality(tab1, tab3)

    def testWriteArrowTableReadAsNumpyTable(self):
        tab1 = _makeSimpleNumpyTable()

        self.butler.put(tab1, self.datasetType, dataId={})

        # Read back out as a numpy table.
        tab2 = self.butler.get(self.datasetType, dataId={}, storageClass="ArrowNumpy")
        self._checkNumpyTableEquality(tab1, tab2)

        # Read back out as an arrow table, convert to numpy table.
        atab3 = self.butler.get(self.datasetType, dataId={})
        tab3 = arrow_to_numpy(atab3)
        self._checkNumpyTableEquality(tab1, tab3)

    def _checkAstropyTableEquality(self, table1, table2):
        """Check if two astropy tables have the same columns/values

        Parameters
        ----------
        table1 : `astropy.table.Table`
        table2 : `astropy.table.Table`
        """
        self.assertEqual(table1.dtype, table2.dtype)
        for name in table1.columns:
            self.assertEqual(table1[name].unit, table2[name].unit)
            self.assertEqual(table1[name].description, table2[name].description)
            self.assertEqual(table1[name].format, table2[name].format)
        self.assertTrue(np.all(table1 == table2))

    def _checkNumpyTableEquality(self, table1, table2):
        """Check if two numpy tables have the same columns/values

        Parameters
        ----------
        table1 : `numpy.ndarray`
        table2 : `numpy.ndarray`
        """
        self.assertEqual(table1.dtype.names, table2.dtype.names)
        for name in table1.dtype.names:
            self.assertEqual(table1.dtype[name], table2.dtype[name])
        self.assertTrue(np.all(table1 == table2))


class InMemoryArrowTableDelegateTestCase(ParquetFormatterArrowTableTestCase):
    """Tests for InMemoryDatastore, using ArrowTableDelegate."""

    configFile = os.path.join(TESTDIR, "config/basic/butler-inmemory.yaml")

    def testBadInput(self):
        delegate = ArrowTableDelegate("ArrowTable")

        with self.assertRaises(ValueError):
            delegate.handleParameters(inMemoryDataset="not_a_dataframe")

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


if __name__ == "__main__":
    unittest.main()

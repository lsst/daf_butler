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

try:
    import numpy as np
    import pandas as pd
    from astropy.table import Table
except ImportError:
    pd = None

try:
    import pyarrow.parquet
except ImportError:
    pyarrow = None

from lsst.daf.butler import Butler, Config, DatasetType, StorageClassConfig, StorageClassFactory
from lsst.daf.butler.delegates.dataframe import DataFrameDelegate
from lsst.daf.butler.formatters.parquet import ArrowAstropySchema, DataFrameSchema
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
    return Table(data)


@unittest.skipUnless(pd is not None, "Cannot test ParquetFormatter without pandas.")
@unittest.skipUnless(pyarrow is not None, "Cannot test ParquetFormatter without pyarrow.")
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
        # Read the schema
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


@unittest.skipUnless(pd is not None, "Cannot test parquet InMemoryDatastore without pandas.")
class InMemoryDataFrameDelegateTestCase(ParquetFormatterDataFrameTestCase):
    """Tests for InMemoryDatastore, using DataFrameDelegate"""

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

    def testBadInput(self):
        delegate = DataFrameDelegate("DataFrame")

        with self.assertRaises(ValueError):
            delegate.handleParameters(inMemoryDataset="not_a_dataframe")

    def testStorageClass(self):
        df1, allColumns = _makeSingleIndexDataFrame()

        factory = StorageClassFactory()
        factory.addFromConfig(StorageClassConfig())

        storageClass = factory.findStorageClass(type(df1), compare_types=False)
        # Force the name lookup to do name matching
        storageClass._pytype = None
        self.assertEqual(storageClass.name, "DataFrame")

        storageClass = factory.findStorageClass(type(df1), compare_types=True)
        # Force the name lookup to do name matching
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
        # Read the schema
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

    def _checkAstropyTableEquality(self, table1, table2):
        """Check if two astropy tables have the same columns/values

        Parameters
        ----------
        table1 : `astropy.table.Table`
        table2 : `astropy.table.Table`
        """
        self.assertEqual(table1.dtype, table2.dtype)
        self.assertTrue(np.all(table1 == table2))


if __name__ == "__main__":
    unittest.main()

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

from unittest.mock import patch

__all__ = ()

import os
import shutil
import tempfile
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

import astropy
from astropy.table import Table as AstropyTable

from .. import Butler, ButlerConfig, Config, DatasetRef, StorageClassFactory, Timespan
from .._collection_type import CollectionType
from ..datastore import NullDatastore
from ..direct_butler import DirectButler
from ..registry.sql_registry import SqlRegistry
from ..tests import MetricsExample, addDatasetType

if TYPE_CHECKING:
    import unittest

    from lsst.daf.butler import DatasetType

    class TestCaseMixin(unittest.TestCase):
        """Base class for mixin test classes that use TestCase methods."""

        pass

else:

    class TestCaseMixin:
        """Do-nothing definition of mixin base class for regular execution."""

        pass


def makeTestTempDir(default_base: str) -> str:
    """Create a temporary directory for test usage.

    The directory will be created within ``DAF_BUTLER_TEST_TMP`` if that
    environment variable is set, falling back to ``default_base`` if it is
    not.

    Parameters
    ----------
    default_base : `str`
        Default parent directory.

    Returns
    -------
    dir : `str`
        Name of the new temporary directory.
    """
    base = os.environ.get("DAF_BUTLER_TEST_TMP", default_base)
    return tempfile.mkdtemp(dir=base)


def removeTestTempDir(root: str | None) -> None:
    """Attempt to remove a temporary test directory, but do not raise if
    unable to.

    Unlike `tempfile.TemporaryDirectory`, this passes ``ignore_errors=True``
    to ``shutil.rmtree`` at close, making it safe to use on NFS.

    Parameters
    ----------
    root : `str`, optional
        Name of the directory to be removed.  If `None`, nothing will be done.
    """
    if root is not None and os.path.exists(root):
        shutil.rmtree(root, ignore_errors=True)


@contextmanager
def safeTestTempDir(default_base: str) -> Iterator[str]:
    """Return a context manager that creates a temporary directory and then
    attempts to remove it.

    Parameters
    ----------
    default_base : `str`
        Default parent directory, forwarded to `makeTestTempDir`.

    Returns
    -------
    context : `contextlib.ContextManager`
        A context manager that returns the new directory name on ``__enter__``
        and removes the temporary directory (via `removeTestTempDir`) on
        ``__exit__``.
    """
    root = makeTestTempDir(default_base)
    try:
        yield root
    finally:
        removeTestTempDir(root)


def create_populated_sqlite_registry(*args: str) -> Butler:
    """Create an in-memory registry-only sqlite butler and populate it.

    Parameters
    ----------
    *args : `str`
        Paths to export YAML files that should be imported.

    Returns
    -------
    butler : `Butler`
        New butler populated with the specified import files.
    """
    config = ButlerConfig()
    config[".registry.db"] = "sqlite://"
    registry = SqlRegistry.createFromConfig(config["registry"])
    butler = DirectButler(
        config=config,
        registry=registry,
        datastore=NullDatastore(None, None),
        storageClasses=StorageClassFactory(),
    )
    for arg in args:
        butler.import_(filename=arg, without_datastore=True)
    return butler


class ButlerTestHelper:
    """Mixin with helpers for unit tests."""

    assertEqual: Callable
    assertIsInstance: Callable
    maxDiff: int | None

    def assertAstropyTablesEqual(
        self,
        tables: AstropyTable | Sequence[AstropyTable],
        expectedTables: AstropyTable | Sequence[AstropyTable],
        filterColumns: bool = False,
        unorderedRows: bool = False,
    ) -> None:
        """Verify that a list of astropy tables matches a list of expected
        astropy tables.

        Parameters
        ----------
        tables : `astropy.table.Table` or iterable [`astropy.table.Table`]
            The table or tables that should match the expected tables.
        expectedTables : `astropy.table.Table`
                         or iterable [`astropy.table.Table`]
            The tables with expected values to which the tables under test will
            be compared.
        filterColumns : `bool`
            If `True` then only compare columns that exist in
            ``expectedTables``.
        unorderedRows : `bool`, optional
            If `True` (`False` is default), don't require tables to have their
            rows in the same order.
        """
        # If a single table is passed in for tables or expectedTables, put it
        # in a list.
        if isinstance(tables, AstropyTable):
            tables = [tables]
        if isinstance(expectedTables, AstropyTable):
            expectedTables = [expectedTables]
        self.assertEqual(len(tables), len(expectedTables))
        for table, expected in zip(tables, expectedTables, strict=True):
            # Assert that we are testing what we think we are testing:
            self.assertIsInstance(table, AstropyTable)
            self.assertIsInstance(expected, AstropyTable)
            if filterColumns:
                table = table.copy()
                table.keep_columns(expected.colnames)
            if unorderedRows:
                table = table.copy()
                table.sort(table.colnames)
                expected = expected.copy()
                expected.sort(expected.colnames)
            # Assert that they match.
            # Recommendation from Astropy Slack is to format the table into
            # lines for comparison. We do not compare column data types.
            table1 = table.pformat_all()
            expected1 = expected.pformat_all()
            original_max = self.maxDiff
            self.maxDiff = None  # This is required to get the full diff.
            try:
                self.assertEqual(table1, expected1, f"Table:\n{table}\n\nvs Expected:\n{expected}")
            finally:
                self.maxDiff = original_max


def readTable(textTable: str) -> AstropyTable:
    """Read an astropy table from formatted text.

    Contains formatting that causes the astropy table to print an empty string
    instead of "--" for missing/unpopulated values in the text table.

    Parameters
    ----------
    textTable : `str`
        The text version of the table to read.

    Returns
    -------
    table : `astropy.table.Table`
        The table as an astropy table.
    """
    return AstropyTable.read(
        textTable,
        format="ascii",
        data_start=2,  # skip the header row and the header row underlines.
        fill_values=[("", 0, "")],
    )


class MetricTestRepo:
    """Creates and manage a test repository on disk with datasets that
    may be queried and modified for unit tests.

    Parameters
    ----------
    root : `str`
        The location of the repository, to pass to ``Butler.makeRepo``.
    configFile : `str`
        The path to the config file, to pass to ``Butler.makeRepo``.
    forceConfigRoot : `bool`, optional
        If `False`, any values present in the supplied ``config`` that
        would normally be reset are not overridden and will appear
        directly in the output config. Passed to ``Butler.makeRepo``.
    storageClassName : `bool` or `None`, optional
        Name of storage class to use for datasets added to the test repository.
        A default will be used if none is specified.
    """

    METRICS_EXAMPLE_SUMMARY = {"AM1": 5.2, "AM2": 30.6}
    """The summary data included in ``MetricsExample`` objects stored in the
    test repo
    """

    _DEFAULT_RUN = "ingest/run"
    _DEFAULT_TAG = "ingest"
    _DEFAULT_STORAGE_CLASS = "StructuredCompositeReadComp"

    @staticmethod
    def _makeExampleMetrics() -> MetricsExample:
        """Make an object to put into the repository."""
        return MetricsExample(
            MetricTestRepo.METRICS_EXAMPLE_SUMMARY,
            {"a": [1, 2, 3], "b": {"blue": 5, "red": "green"}},
            [563, 234, 456.7, 752, 8, 9, 27],
        )

    def __init__(
        self,
        root: str,
        configFile: str,
        forceConfigRoot: bool = True,
        storageClassName: str | None = None,
    ) -> None:
        self.root = root
        butlerConfigFile = Butler.makeRepo(
            self.root, config=Config(configFile), forceConfigRoot=forceConfigRoot
        )
        butler = Butler.from_config(butlerConfigFile, run=self._DEFAULT_RUN, collections=[self._DEFAULT_TAG])
        self._do_init(butler, butlerConfigFile, storageClassName)

    @classmethod
    def create_from_butler(
        cls, butler: Butler, butler_config_file: str, storageClassName: str | None = None
    ) -> MetricTestRepo:
        """Create a MetricTestRepo from an existing Butler instance.

        Parameters
        ----------
        butler : `Butler`
            `Butler` instance used for setting up the repository.
        butler_config_file : `str`
            Path to the config file used to set up that Butler instance.
        storageClassName : `bool` or `None`, optional
            Name of storage class to use for datasets added to the test
            repository. A default will be used if none is specified.

        Returns
        -------
        repo : `MetricTestRepo`
            New instance of `MetricTestRepo` using the provided `Butler`
            instance.
        """
        self = cls.__new__(cls)
        butler = butler.clone(run=self._DEFAULT_RUN, collections=[self._DEFAULT_TAG])
        self._do_init(butler, butler_config_file, storageClassName)
        return self

    def _do_init(
        self, butler: Butler, butlerConfigFile: str | Config, storageClassName: str | None = None
    ) -> None:
        self.butler = butler
        self.storageClassFactory = StorageClassFactory()
        self.storageClassFactory.addFromConfig(butlerConfigFile)

        # New datasets will be added to run and tag, but we will only look in
        # tag when looking up datasets.
        self.butler.collections.register(self._DEFAULT_TAG, CollectionType.TAGGED)

        if storageClassName is None:
            storageClassName = self._DEFAULT_STORAGE_CLASS

        # Create and register a DatasetType
        self.datasetType = addDatasetType(
            self.butler, "test_metric_comp", {"instrument", "visit"}, storageClassName
        )

        # Add needed Dimensions
        self.butler.registry.insertDimensionData("instrument", {"name": "DummyCamComp"})
        self.butler.registry.insertDimensionData(
            "physical_filter", {"instrument": "DummyCamComp", "name": "d-r", "band": "R"}
        )
        self.butler.registry.insertDimensionData("day_obs", {"instrument": "DummyCamComp", "id": 20200101})
        self.butler.registry.insertDimensionData(
            "visit_system", {"instrument": "DummyCamComp", "id": 1, "name": "default"}
        )
        visitStart = astropy.time.Time("2020-01-01 08:00:00.123456789", scale="tai")
        visitEnd = astropy.time.Time("2020-01-01 08:00:36.66", scale="tai")
        self.butler.registry.insertDimensionData(
            "visit",
            dict(
                instrument="DummyCamComp",
                id=423,
                name="fourtwentythree",
                physical_filter="d-r",
                timespan=Timespan(visitStart, visitEnd),
                day_obs=20200101,
            ),
        )
        self.butler.registry.insertDimensionData(
            "visit",
            dict(
                instrument="DummyCamComp",
                id=424,
                name="fourtwentyfour",
                physical_filter="d-r",
                day_obs=20200101,
            ),
        )

        self.addDataset({"instrument": "DummyCamComp", "visit": 423})
        self.addDataset({"instrument": "DummyCamComp", "visit": 424})

    def addDataset(
        self, dataId: dict[str, Any], run: str | None = None, datasetType: DatasetType | None = None
    ) -> DatasetRef:
        """Create a new example metric and add it to the named run with the
        given dataId.

        Overwrites tags, so this does not try to associate the new dataset with
        existing tags. (If/when tags are needed this can be added to the
        arguments of this function.)

        Parameters
        ----------
        dataId : `dict`
            The dataId for the new metric.
        run : `str`, optional
            The name of the run to create and add a dataset to. If `None`, the
            dataset will be added to the root butler.
        datasetType : ``DatasetType``, optional
            The dataset type of the added dataset. If `None`, will use the
            default dataset type.

        Returns
        -------
        datasetRef : `DatasetRef`
            A reference to the added dataset.
        """
        if run:
            self.butler.collections.register(run)
        else:
            run = self._DEFAULT_RUN
        metric = self._makeExampleMetrics()
        return self.butler.put(
            metric, self.datasetType if datasetType is None else datasetType, dataId, run=run
        )


@contextmanager
def mock_env(new_environment: dict[str, str]) -> Iterator[None]:
    """Context manager to clear the process environment variables, replace them
    with new values, and restore them at the end of the test.

    Parameters
    ----------
    new_environment : `dict`[`str`, `str`]
        New environment variable values.
    """
    with patch.dict(os.environ, new_environment, clear=True):
        yield

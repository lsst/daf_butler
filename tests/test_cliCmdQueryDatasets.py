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

"""Unit tests for daf_butler CLI query-datasets command."""

import os
import unittest

from astropy.table import Table as AstropyTable
from numpy import array

from lsst.daf.butler import CollectionType, InvalidQueryError, StorageClassFactory, script
from lsst.daf.butler.tests import addDatasetType
from lsst.daf.butler.tests.utils import ButlerTestHelper, MetricTestRepo, makeTestTempDir, removeTestTempDir
from lsst.resources import ResourcePath

TESTDIR = os.path.abspath(os.path.dirname(__file__))


def expectedFilesystemDatastoreTables(root: ResourcePath):
    """Return the expected table contents."""
    return (
        AstropyTable(
            array(
                (
                    (
                        "test_metric_comp.data",
                        "ingest/run",
                        "DummyCamComp",
                        "423",
                        "R",
                        "d-r",
                        root.join(
                            "ingest/run/test_metric_comp.data/"
                            "test_metric_comp_v00000423_fDummyCamComp_data.yaml"
                        ),
                    ),
                    (
                        "test_metric_comp.data",
                        "ingest/run",
                        "DummyCamComp",
                        "424",
                        "R",
                        "d-r",
                        root.join(
                            "ingest/run/test_metric_comp.data/"
                            "test_metric_comp_v00000424_fDummyCamComp_data.yaml"
                        ),
                    ),
                )
            ),
            names=("type", "run", "instrument", "visit", "band", "physical_filter", "URI"),
        ),
        AstropyTable(
            array(
                (
                    (
                        "test_metric_comp.output",
                        "ingest/run",
                        "DummyCamComp",
                        "423",
                        "R",
                        "d-r",
                        root.join(
                            "ingest/run/test_metric_comp.output/"
                            "test_metric_comp_v00000423_fDummyCamComp_output.yaml"
                        ),
                    ),
                    (
                        "test_metric_comp.output",
                        "ingest/run",
                        "DummyCamComp",
                        "424",
                        "R",
                        "d-r",
                        root.join(
                            "ingest/run/test_metric_comp.output/"
                            "test_metric_comp_v00000424_fDummyCamComp_output.yaml"
                        ),
                    ),
                )
            ),
            names=("type", "run", "instrument", "visit", "band", "physical_filter", "URI"),
        ),
        AstropyTable(
            array(
                (
                    (
                        "test_metric_comp.summary",
                        "ingest/run",
                        "DummyCamComp",
                        "423",
                        "R",
                        "d-r",
                        root.join(
                            "ingest/run/test_metric_comp.summary/"
                            "test_metric_comp_v00000423_fDummyCamComp_summary.yaml"
                        ),
                    ),
                    (
                        "test_metric_comp.summary",
                        "ingest/run",
                        "DummyCamComp",
                        "424",
                        "R",
                        "d-r",
                        root.join(
                            "ingest/run/test_metric_comp.summary/"
                            "test_metric_comp_v00000424_fDummyCamComp_summary.yaml"
                        ),
                    ),
                )
            ),
            names=("type", "run", "instrument", "visit", "band", "physical_filter", "URI"),
        ),
    )


class QueryDatasetsTest(unittest.TestCase, ButlerTestHelper):
    """Test the query-datasets command-line."""

    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")
    storageClassFactory = StorageClassFactory()

    @staticmethod
    def _queryDatasets(
        repo, glob=(), collections=(), where="", find_first=False, show_uri=False, limit=0, order_by=()
    ):
        query = script.QueryDatasets(
            glob,
            collections,
            where=where,
            find_first=find_first,
            show_uri=show_uri,
            limit=limit,
            order_by=order_by,
            butler=repo,
        )
        return list(query.getTables())

    def setUp(self):
        self.testdir = makeTestTempDir(TESTDIR)
        self.repoDir = os.path.join(self.testdir, "repo")

    def tearDown(self):
        removeTestTempDir(self.testdir)

    def testChained(self):
        testRepo = MetricTestRepo(
            self.repoDir, configFile=os.path.join(TESTDIR, "config/basic/butler-chained.yaml")
        )

        tables = self._queryDatasets(repo=testRepo.butler, show_uri=True, collections="*", glob="*")

        # Want second datastore root since in-memory is ephemeral and
        # all the relevant datasets are stored in the second as well as third
        # datastore.
        roots = testRepo.butler.get_datastore_roots()
        datastore_root = roots[testRepo.butler.get_datastore_names()[1]]

        self.assertAstropyTablesEqual(
            tables,
            expectedFilesystemDatastoreTables(datastore_root),
            filterColumns=True,
        )

    def testShowURI(self):
        """Test for expected output with show_uri=True."""
        testRepo = MetricTestRepo(self.repoDir, configFile=self.configFile)

        tables = self._queryDatasets(repo=testRepo.butler, show_uri=True, collections="*", glob="*")

        roots = testRepo.butler.get_datastore_roots()
        datastore_root = list(roots.values())[0]

        self.assertAstropyTablesEqual(
            tables, expectedFilesystemDatastoreTables(datastore_root), filterColumns=True
        )

    def testShowUriNoDisassembly(self):
        """Test for expected output with show_uri=True and no disassembly."""
        testRepo = MetricTestRepo(
            self.repoDir,
            configFile=self.configFile,
            storageClassName="StructuredCompositeReadCompNoDisassembly",
        )

        tables = self._queryDatasets(repo=testRepo.butler, show_uri=True, collections="*", glob="*")

        roots = testRepo.butler.get_datastore_roots()
        datastore_root = list(roots.values())[0]

        expected = [
            AstropyTable(
                array(
                    (
                        (
                            "test_metric_comp",
                            "ingest/run",
                            "DummyCamComp",
                            "423",
                            "R",
                            "d-r",
                            datastore_root.join(
                                "ingest/run/test_metric_comp/test_metric_comp_v00000423_fDummyCamComp.yaml"
                            ),
                        ),
                        (
                            "test_metric_comp",
                            "ingest/run",
                            "DummyCamComp",
                            "424",
                            "R",
                            "d-r",
                            datastore_root.join(
                                "ingest/run/test_metric_comp/test_metric_comp_v00000424_fDummyCamComp.yaml"
                            ),
                        ),
                    )
                ),
                names=("type", "run", "instrument", "visit", "band", "physical_filter", "URI"),
            ),
        ]

        self.assertAstropyTablesEqual(tables, expected, filterColumns=True)

    def testNoShowURI(self):
        """Test for expected output without show_uri (default is False)."""
        testRepo = MetricTestRepo(self.repoDir, configFile=self.configFile)

        tables = self._queryDatasets(repo=testRepo.butler, collections="*", glob="*")

        expectedTables = (
            AstropyTable(
                array(
                    (
                        ("test_metric_comp", "ingest/run", "DummyCamComp", "423", "R", "d-r"),
                        ("test_metric_comp", "ingest/run", "DummyCamComp", "424", "R", "d-r"),
                    )
                ),
                names=("type", "run", "instrument", "visit", "band", "physical_filter"),
            ),
        )

        self.assertAstropyTablesEqual(tables, expectedTables, filterColumns=True)

    def testWhere(self):
        """Test using the where clause to reduce the number of rows returned by
        queryDatasets.
        """
        testRepo = MetricTestRepo(self.repoDir, configFile=self.configFile)

        for glob in (("*",), ("test_metric_comp",)):
            with self.subTest(glob=glob):
                tables = self._queryDatasets(
                    repo=testRepo.butler,
                    where="instrument='DummyCamComp' AND visit=423",
                    collections="*",
                    glob=glob,
                )

                expectedTables = (
                    AstropyTable(
                        array(("test_metric_comp", "ingest/run", "DummyCamComp", "423", "R", "d-r")),
                        names=("type", "run", "instrument", "visit", "band", "physical_filter"),
                    ),
                )

                self.assertAstropyTablesEqual(tables, expectedTables, filterColumns=True)

                with self.assertRaises(InvalidQueryError):
                    self._queryDatasets(repo=testRepo.butler, collections="*", find_first=True, glob=glob)

    def testGlobDatasetType(self):
        """Test specifying dataset type."""
        # Create and register an additional DatasetType
        testRepo = MetricTestRepo(self.repoDir, configFile=self.configFile)

        testRepo.butler.registry.insertDimensionData(
            "visit",
            {
                "instrument": "DummyCamComp",
                "id": 425,
                "name": "fourtwentyfive",
                "physical_filter": "d-r",
                "day_obs": 20200101,
            },
        )

        datasetType = addDatasetType(
            testRepo.butler, "alt_test_metric_comp", ("instrument", "visit"), "StructuredCompositeReadComp"
        )

        testRepo.addDataset(dataId={"instrument": "DummyCamComp", "visit": 425}, datasetType=datasetType)

        # verify the new dataset type increases the number of tables found:
        tables = self._queryDatasets(repo=testRepo.butler, collections="*", glob="*")

        expectedTables = (
            AstropyTable(
                array(("alt_test_metric_comp", "ingest/run", "DummyCamComp", "425", "R", "d-r")),
                names=("type", "run", "instrument", "visit", "band", "physical_filter"),
            ),
            AstropyTable(
                array(
                    (
                        ("test_metric_comp", "ingest/run", "DummyCamComp", "423", "R", "d-r"),
                        ("test_metric_comp", "ingest/run", "DummyCamComp", "424", "R", "d-r"),
                    )
                ),
                names=("type", "run", "instrument", "visit", "band", "physical_filter"),
            ),
        )

        self.assertAstropyTablesEqual(tables, expectedTables, filterColumns=True)

        # Dataset type (glob) argument will become mandatory soon
        with self.assertWarns(FutureWarning):
            self._queryDatasets(repo=testRepo.butler, collections="*")

    def test_limit_order(self):
        """Test limit and ordering."""
        # Create and register an additional DatasetType
        testRepo = MetricTestRepo(self.repoDir, configFile=self.configFile)

        with self.assertLogs("lsst.daf.butler.script.queryDatasets", level="WARNING") as cm:
            tables = self._queryDatasets(
                repo=testRepo.butler,
                limit=-1,
                order_by=("visit",),
                collections="*",
                glob=("test_metric_comp",),
            )

        self.assertIn("increase this limit", cm.output[0])

        expectedTables = [
            AstropyTable(
                array((("test_metric_comp", "ingest/run", "DummyCamComp", "423", "R", "d-r"),)),
                names=("type", "run", "instrument", "visit", "band", "physical_filter"),
            ),
        ]
        self.assertAstropyTablesEqual(tables, expectedTables, filterColumns=True)

        # Same as previous test, but with positive limit so no warning is
        # issued.
        with self.assertNoLogs("lsst.daf.butler.script.queryDatasets", level="WARNING"):
            tables = self._queryDatasets(
                repo=testRepo.butler,
                limit=1,
                order_by=("visit",),
                collections="*",
                glob=("test_metric_comp",),
            )
        self.assertAstropyTablesEqual(tables, expectedTables, filterColumns=True)

        with self.assertLogs("lsst.daf.butler.script.queryDatasets", level="WARNING") as cm:
            tables = self._queryDatasets(
                repo=testRepo.butler,
                limit=-1,
                order_by=("-visit",),
                collections="*",
                glob=("test_metric_comp",),
            )
        self.assertIn("increase this limit", cm.output[0])

        expectedTables = [
            AstropyTable(
                array((("test_metric_comp", "ingest/run", "DummyCamComp", "424", "R", "d-r"),)),
                names=("type", "run", "instrument", "visit", "band", "physical_filter"),
            ),
        ]
        self.assertAstropyTablesEqual(tables, expectedTables, filterColumns=True)

        # --order-by is not supported by the query backend for multiple dataset
        # types, so we can only provide it for queries with a single dataset
        # type.
        with self.assertRaisesRegex(NotImplementedError, "--order-by"):
            self._queryDatasets(
                repo=testRepo.butler, limit=1, order_by=("visit",), collections="*", glob=["*"]
            )
        with self.assertRaisesRegex(NotImplementedError, "--order-by"):
            self._queryDatasets(
                repo=testRepo.butler,
                limit=1,
                order_by=("visit",),
                collections="*",
                glob=["test_metric_comp", "raw"],
            )

    def testFindFirstAndCollections(self):
        """Test the find-first option, and the collections option, since it
        is required for find-first.
        """
        testRepo = MetricTestRepo(self.repoDir, configFile=self.configFile)

        # Add a new run, and add a dataset to shadow an existing dataset.
        testRepo.addDataset(run="foo", dataId={"instrument": "DummyCamComp", "visit": 424})

        # Add a CHAINED collection to include the two runs, to check that
        # flattening works as well.
        testRepo.butler.collections.register("chain", CollectionType.CHAINED)
        testRepo.butler.collections.redefine_chain("chain", ["foo", "ingest/run"])

        # Verify that without find-first, duplicate datasets are returned
        tables = self._queryDatasets(repo=testRepo.butler, collections=["chain"], show_uri=True, glob="*")

        # The test should be running with a single FileDatastore.
        roots = testRepo.butler.get_datastore_roots()
        assert len(roots) == 1
        datastore_root = list(roots.values())[0]

        expectedTables = (
            AstropyTable(
                array(
                    (
                        (
                            "test_metric_comp.data",
                            "foo",
                            "DummyCamComp",
                            "424",
                            "R",
                            "d-r",
                            datastore_root.join(
                                "foo/test_metric_comp.data/test_metric_comp_v00000424_fDummyCamComp_data.yaml"
                            ),
                        ),
                        (
                            "test_metric_comp.data",
                            "ingest/run",
                            "DummyCamComp",
                            "423",
                            "R",
                            "d-r",
                            datastore_root.join(
                                "ingest/run/test_metric_comp.data/"
                                "test_metric_comp_v00000423_fDummyCamComp_data.yaml"
                            ),
                        ),
                        (
                            "test_metric_comp.data",
                            "ingest/run",
                            "DummyCamComp",
                            "424",
                            "R",
                            "d-r",
                            datastore_root.join(
                                "ingest/run/test_metric_comp.data/"
                                "test_metric_comp_v00000424_fDummyCamComp_data.yaml"
                            ),
                        ),
                    )
                ),
                names=("type", "run", "instrument", "visit", "band", "physical_filter", "URI"),
            ),
            AstropyTable(
                array(
                    (
                        (
                            "test_metric_comp.output",
                            "foo",
                            "DummyCamComp",
                            "424",
                            "R",
                            "d-r",
                            datastore_root.join(
                                "foo/test_metric_comp.output/"
                                "test_metric_comp_v00000424_fDummyCamComp_output.yaml"
                            ),
                        ),
                        (
                            "test_metric_comp.output",
                            "ingest/run",
                            "DummyCamComp",
                            "423",
                            "R",
                            "d-r",
                            datastore_root.join(
                                "ingest/run/test_metric_comp.output/"
                                "test_metric_comp_v00000423_fDummyCamComp_output.yaml"
                            ),
                        ),
                        (
                            "test_metric_comp.output",
                            "ingest/run",
                            "DummyCamComp",
                            "424",
                            "R",
                            "d-r",
                            datastore_root.join(
                                "ingest/run/test_metric_comp.output/"
                                "test_metric_comp_v00000424_fDummyCamComp_output.yaml"
                            ),
                        ),
                    )
                ),
                names=("type", "run", "instrument", "visit", "band", "physical_filter", "URI"),
            ),
            AstropyTable(
                array(
                    (
                        (
                            "test_metric_comp.summary",
                            "foo",
                            "DummyCamComp",
                            "424",
                            "R",
                            "d-r",
                            datastore_root.join(
                                "foo/test_metric_comp.summary/"
                                "test_metric_comp_v00000424_fDummyCamComp_summary.yaml"
                            ),
                        ),
                        (
                            "test_metric_comp.summary",
                            "ingest/run",
                            "DummyCamComp",
                            "423",
                            "R",
                            "d-r",
                            datastore_root.join(
                                "ingest/run/test_metric_comp.summary/"
                                "test_metric_comp_v00000423_fDummyCamComp_summary.yaml"
                            ),
                        ),
                        (
                            "test_metric_comp.summary",
                            "ingest/run",
                            "DummyCamComp",
                            "424",
                            "R",
                            "d-r",
                            datastore_root.join(
                                "ingest/run/test_metric_comp.summary/"
                                "test_metric_comp_v00000424_fDummyCamComp_summary.yaml"
                            ),
                        ),
                    )
                ),
                names=("type", "run", "instrument", "visit", "band", "physical_filter", "URI"),
            ),
        )

        self.assertAstropyTablesEqual(tables, expectedTables, filterColumns=True)

        # Verify that with find first the duplicate dataset is eliminated and
        # the more recent dataset is returned.
        tables = self._queryDatasets(
            repo=testRepo.butler, collections=["chain"], show_uri=True, find_first=True, glob="*"
        )

        expectedTables = (
            AstropyTable(
                array(
                    (
                        (
                            "test_metric_comp.data",
                            "foo",
                            "DummyCamComp",
                            "424",
                            "R",
                            "d-r",
                            datastore_root.join(
                                "foo/test_metric_comp.data/test_metric_comp_v00000424_fDummyCamComp_data.yaml"
                            ),
                        ),
                        (
                            "test_metric_comp.data",
                            "ingest/run",
                            "DummyCamComp",
                            "423",
                            "R",
                            "d-r",
                            datastore_root.join(
                                "ingest/run/test_metric_comp.data/"
                                "test_metric_comp_v00000423_fDummyCamComp_data.yaml"
                            ),
                        ),
                    )
                ),
                names=("type", "run", "instrument", "visit", "band", "physical_filter", "URI"),
            ),
            AstropyTable(
                array(
                    (
                        (
                            "test_metric_comp.output",
                            "foo",
                            "DummyCamComp",
                            "424",
                            "R",
                            "d-r",
                            datastore_root.join(
                                "foo/test_metric_comp.output/"
                                "test_metric_comp_v00000424_fDummyCamComp_output.yaml"
                            ),
                        ),
                        (
                            "test_metric_comp.output",
                            "ingest/run",
                            "DummyCamComp",
                            "423",
                            "R",
                            "d-r",
                            datastore_root.join(
                                "ingest/run/test_metric_comp.output/"
                                "test_metric_comp_v00000423_fDummyCamComp_output.yaml"
                            ),
                        ),
                    )
                ),
                names=("type", "run", "instrument", "visit", "band", "physical_filter", "URI"),
            ),
            AstropyTable(
                array(
                    (
                        (
                            "test_metric_comp.summary",
                            "foo",
                            "DummyCamComp",
                            "424",
                            "R",
                            "d-r",
                            datastore_root.join(
                                "foo/test_metric_comp.summary/"
                                "test_metric_comp_v00000424_fDummyCamComp_summary.yaml"
                            ),
                        ),
                        (
                            "test_metric_comp.summary",
                            "ingest/run",
                            "DummyCamComp",
                            "423",
                            "R",
                            "d-r",
                            datastore_root.join(
                                "ingest/run/test_metric_comp.summary/"
                                "test_metric_comp_v00000423_fDummyCamComp_summary.yaml"
                            ),
                        ),
                    )
                ),
                names=("type", "run", "instrument", "visit", "band", "physical_filter", "URI"),
            ),
        )

        self.assertAstropyTablesEqual(tables, expectedTables, filterColumns=True)

        # Verify that globs are not supported with find_first=True.
        with self.assertRaises(InvalidQueryError):
            self._queryDatasets(
                repo=testRepo.butler, collections=["*"], show_uri=True, find_first=True, glob="*"
            )

        # Collections argument will become mandatory soon
        with self.assertWarns(FutureWarning):
            self._queryDatasets(repo=testRepo.butler, glob="*")


if __name__ == "__main__":
    unittest.main()

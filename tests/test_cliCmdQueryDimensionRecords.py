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

"""Unit tests for daf_butler CLI query-collections command.
"""

import os
import unittest

from astropy.table import Table as AstropyTable
from lsst.daf.butler import Butler, StorageClassFactory
from lsst.daf.butler.cli.butler import cli as butlerCli
from lsst.daf.butler.cli.utils import LogCliRunner, clickResultMsg
from lsst.daf.butler.tests.utils import (
    ButlerTestHelper,
    MetricTestRepo,
    makeTestTempDir,
    readTable,
    removeTestTempDir,
)
from numpy import array

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class QueryDimensionRecordsTest(unittest.TestCase, ButlerTestHelper):
    """Test the query-dimension-records command-line."""

    mockFuncName = "lsst.daf.butler.cli.cmd.commands.script.queryDimensionRecords"

    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")
    storageClassFactory = StorageClassFactory()

    expectedColumnNames = (
        "instrument",
        "id",
        "day_obs",
        "physical_filter",
        "name",
        "seq_num",
        "exposure_time",
        "target_name",
        "observation_reason",
        "science_program",
        "azimuth",
        "zenith_angle",
        "region",
        "timespan (TAI)",
    )

    def setUp(self):
        self.root = makeTestTempDir(TESTDIR)
        self.testRepo = MetricTestRepo(
            self.root, configFile=os.path.join(TESTDIR, "config/basic/butler.yaml")
        )
        self.runner = LogCliRunner()

    def tearDown(self):
        removeTestTempDir(self.root)

    def testBasic(self):
        result = self.runner.invoke(butlerCli, ["query-dimension-records", self.root, "visit"])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        rows = array(
            (
                (
                    "DummyCamComp",
                    "423",
                    "20200101",
                    "d-r",
                    "fourtwentythree",
                    "None",
                    "None",
                    "None",
                    "None",
                    "None",
                    "None",
                    "None",
                    "None",
                    "[2020-01-01T08:00:00, 2020-01-01T08:00:36)",
                ),
                (
                    "DummyCamComp",
                    "424",
                    "20200101",
                    "d-r",
                    "fourtwentyfour",
                    "None",
                    "None",
                    "None",
                    "None",
                    "None",
                    "None",
                    "None",
                    "None",
                    "None",
                ),
            )
        )
        expected = AstropyTable(rows, names=self.expectedColumnNames)
        got = readTable(result.output)
        self.assertAstropyTablesEqual(got, expected)

    def testWhere(self):
        result = self.runner.invoke(
            butlerCli,
            [
                "query-dimension-records",
                self.root,
                "visit",
                "--where",
                "instrument='DummyCamComp' AND visit.name='fourtwentythree'",
            ],
        )
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        rows = array(
            (
                (
                    "DummyCamComp",
                    "423",
                    "20200101",
                    "d-r",
                    "fourtwentythree",
                    "None",
                    "None",
                    "None",
                    "None",
                    "None",
                    "None",
                    "None",
                    "None",
                    "[2020-01-01T08:00:00, 2020-01-01T08:00:36)",
                ),
            )
        )
        expected = AstropyTable(rows, names=self.expectedColumnNames)
        self.assertAstropyTablesEqual(readTable(result.output), expected)

    def testCollection(self):
        butler = Butler.from_config(self.root, run="foo")

        # try replacing the testRepo's butler with the one with the "foo" run.
        self.testRepo.butler = butler

        self.testRepo.butler.registry.insertDimensionData(
            "visit",
            {
                "instrument": "DummyCamComp",
                "id": 425,
                "name": "fourtwentyfive",
                "physical_filter": "d-r",
                "day_obs": 20200101,
            },
        )
        self.testRepo.addDataset(dataId={"instrument": "DummyCamComp", "visit": 425}, run="foo")

        # verify getting records from the "ingest/run" collection
        result = self.runner.invoke(
            butlerCli,
            [
                "query-dimension-records",
                self.root,
                "visit",
                "--collections",
                "ingest/run",
                "--datasets",
                "test_metric_comp",
            ],
        )
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        rows = array(
            (
                (
                    "DummyCamComp",
                    "423",
                    "20200101",
                    "d-r",
                    "fourtwentythree",
                    "None",
                    "None",
                    "None",
                    "None",
                    "None",
                    "None",
                    "None",
                    "None",
                    "[2020-01-01T08:00:00, 2020-01-01T08:00:36)",
                ),
                (
                    "DummyCamComp",
                    "424",
                    "20200101",
                    "d-r",
                    "fourtwentyfour",
                    "None",
                    "None",
                    "None",
                    "None",
                    "None",
                    "None",
                    "None",
                    "None",
                    "None",
                ),
            )
        )
        expected = AstropyTable(rows, names=self.expectedColumnNames)
        self.assertAstropyTablesEqual(readTable(result.output), expected)

        # verify getting records from the "foo" collection
        result = self.runner.invoke(
            butlerCli,
            [
                "query-dimension-records",
                self.root,
                "visit",
                "--collections",
                "foo",
                "--datasets",
                "test_metric_comp",
            ],
        )
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        rows = array(
            (
                (
                    "DummyCamComp",
                    "425",
                    "20200101",
                    "d-r",
                    "fourtwentyfive",
                    "None",
                    "None",
                    "None",
                    "None",
                    "None",
                    "None",
                    "None",
                    "None",
                    "None",
                ),
            )
        )
        expected = AstropyTable(rows, names=self.expectedColumnNames)
        self.assertAstropyTablesEqual(readTable(result.output), expected)

    def testSkymap(self):
        butler = Butler.from_config(self.root, run="foo")
        # try replacing the testRepo's butler with the one with the "foo" run.
        self.testRepo.butler = butler

        skymapRecord = {"name": "example_skymap", "hash": (50).to_bytes(8, byteorder="little")}
        self.testRepo.butler.registry.insertDimensionData("skymap", skymapRecord)

        result = self.runner.invoke(butlerCli, ["query-dimension-records", self.root, "skymap"])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))

        rows = array(("example_skymap", "0x3200000000000000", "None", "None", "None"))
        expected = AstropyTable(rows, names=["name", "hash", "tract_max", "patch_nx_max", "patch_ny_max"])
        self.assertAstropyTablesEqual(readTable(result.output), expected)


if __name__ == "__main__":
    unittest.main()

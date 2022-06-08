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

"""Unit tests for daf_butler CLI query-collections command.
"""

import astropy
from astropy.table import Table as AstropyTable
from astropy.utils.introspection import minversion
from numpy import array
import os
import unittest

from lsst.daf.butler import StorageClassFactory
from lsst.daf.butler import Butler
from lsst.daf.butler.cli.butler import cli as butlerCli
from lsst.daf.butler.cli.utils import clickResultMsg, LogCliRunner
from lsst.daf.butler.tests.utils import (
    ButlerTestHelper,
    makeTestTempDir,
    MetricTestRepo,
    readTable,
    removeTestTempDir,
)


TESTDIR = os.path.abspath(os.path.dirname(__file__))

# Astropy changed the handling of numpy columns in v5.1 so anything
# greater than 5.0 (two digit version) does not need the annotated column.
timespan_columns = "" if minversion(astropy, "5.1") else " [2]"


class QueryDimensionRecordsTest(unittest.TestCase, ButlerTestHelper):

    mockFuncName = "lsst.daf.butler.cli.cmd.commands.script.queryDimensionRecords"

    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")
    storageClassFactory = StorageClassFactory()

    expectedColumnNames = ("instrument", "id", "physical_filter", "visit_system", "name", "day_obs",
                           "exposure_time", "target_name", "observation_reason", "science_program",
                           "zenith_angle", "region", f"timespan{timespan_columns}")

    def setUp(self):
        self.root = makeTestTempDir(TESTDIR)
        self.testRepo = MetricTestRepo(self.root,
                                       configFile=os.path.join(TESTDIR, "config/basic/butler.yaml"))
        self.runner = LogCliRunner()

    def tearDown(self):
        removeTestTempDir(self.root)

    def testBasic(self):
        result = self.runner.invoke(butlerCli, ["query-dimension-records",
                                                self.root,
                                                "visit"])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        rows = array((
            ("DummyCamComp", "423", "d-r", "1", "fourtwentythree", "None", "None", "None", "None", "None",
             "None", "None", "None .. None"),
            ("DummyCamComp", "424", "d-r", "1", "fourtwentyfour", "None", "None", "None", "None", "None",
             "None", "None", "None .. None")
        ))
        expected = AstropyTable(rows, names=self.expectedColumnNames)
        self.assertAstropyTablesEqual(readTable(result.output), expected)

    def testWhere(self):
        result = self.runner.invoke(butlerCli, ["query-dimension-records",
                                                self.root,
                                                "visit",
                                                "--where",
                                                "instrument='DummyCamComp' AND visit.name='fourtwentythree'"])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        rows = array((
            ("DummyCamComp", "423", "d-r", "1", "fourtwentythree", "None", "None", "None", "None", "None",
             "None", "None", "None .. None"),
        ))
        expected = AstropyTable(rows, names=self.expectedColumnNames)
        self.assertAstropyTablesEqual(readTable(result.output), expected)

    def testCollection(self):

        butler = Butler(self.root, run="foo")

        # try replacing the testRepo's butler with the one with the "foo" run.
        self.testRepo.butler = butler

        self.testRepo.butler.registry.insertDimensionData("visit", {"instrument": "DummyCamComp", "id": 425,
                                                                    "name": "fourtwentyfive",
                                                                    "physical_filter": "d-r",
                                                                    "visit_system": 1})
        self.testRepo.addDataset(dataId={"instrument": "DummyCamComp", "visit": 425},
                                 run="foo")

        # verify getting records from the "ingest/run" collection
        result = self.runner.invoke(butlerCli, ["query-dimension-records",
                                                self.root,
                                                "visit",
                                                "--collections", "ingest/run",
                                                "--datasets", "test_metric_comp"
                                                ])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        rows = array((
            ("DummyCamComp", "423", "d-r", "1", "fourtwentythree", "None", "None", "None", "None", "None",
             "None", "None", "None .. None"),
            ("DummyCamComp", "424", "d-r", "1", "fourtwentyfour", "None", "None", "None", "None", "None",
             "None", "None", "None .. None")
        ))
        expected = AstropyTable(rows, names=self.expectedColumnNames)
        self.assertAstropyTablesEqual(readTable(result.output), expected)

        # verify getting records from the "foo" collection
        result = self.runner.invoke(butlerCli, ["query-dimension-records",
                                                self.root,
                                                "visit",
                                                "--collections", "foo",
                                                "--datasets", "test_metric_comp"
                                                ])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        rows = array((
            ("DummyCamComp", "425", "d-r", "1", "fourtwentyfive", "None", "None", "None", "None", "None",
             "None", "None", "None .. None"),
        ))
        expected = AstropyTable(rows, names=self.expectedColumnNames)
        self.assertAstropyTablesEqual(readTable(result.output), expected)


if __name__ == "__main__":
    unittest.main()

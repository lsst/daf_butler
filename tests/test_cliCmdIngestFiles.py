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

"""Unit tests for daf_butler CLI ingest-files command.
"""

import json
import os
import unittest

from astropy.table import Table
from lsst.daf.butler import Butler
from lsst.daf.butler.cli.butler import cli
from lsst.daf.butler.cli.utils import LogCliRunner, clickResultMsg
from lsst.daf.butler.tests import MetricsExample
from lsst.daf.butler.tests.utils import ButlerTestHelper, MetricTestRepo, makeTestTempDir, removeTestTempDir

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class CliIngestFilesTest(unittest.TestCase, ButlerTestHelper):
    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")

    def setUp(self):
        self.root = makeTestTempDir(TESTDIR)
        self.addCleanup(removeTestTempDir, self.root)

        self.testRepo = MetricTestRepo(self.root, configFile=self.configFile)

        self.root2 = makeTestTempDir(TESTDIR)
        self.addCleanup(removeTestTempDir, self.root2)

        # Create some test output files to be ingested
        self.files = []
        self.datasets = []
        for i in range(2):
            data = MetricsExample(summary={"int": i, "string": f"{self.id()}_{i}"})
            outfile = f"test{i}.json"
            with open(os.path.join(self.root2, outfile), "w") as fd:
                json.dump(data._asdict(), fd)
            self.datasets.append(data)
            self.files.append(outfile)

        # The values for the visit and instrument dimensions must correspond
        # to values in the test repo that was created for this test.
        self.visits = [423, 424]
        self.instruments = ["DummyCamComp"] * 2

    def testIngestRelativePath(self):
        """Ingest using relative path with prefix."""
        table = Table([self.files, self.visits, self.instruments], names=["Files", "visit", "instrument"])
        options = ("--prefix", self.root2)
        self.assertIngest(table, options)

    def testIngestAbsoluteWithDataId(self):
        """Ingest with absolute path and factored out dataId override."""
        table = Table(
            [[os.path.join(self.root2, f) for f in self.files], self.visits], names=["Files", "visit"]
        )
        options = ("--data-id", f"instrument={self.instruments[0]}")
        self.assertIngest(table, options)

    def testIngestRelativeWithDataId(self):
        """Ingest with relative path and factored out dataId override."""
        table = Table([self.files, self.visits], names=["Files", "visit"])
        options = ("--data-id", f"instrument={self.instruments[0]}", "--prefix", self.root2)
        self.assertIngest(table, options)

    def assertIngest(self, table, options):
        runner = LogCliRunner()
        with runner.isolated_filesystem():
            table_file = os.path.join(self.root2, f"table_{self.id()}.csv")
            table.write(table_file)

            run = f"u/user/{self.id()}"
            result = runner.invoke(
                cli, ["ingest-files", *options, self.root, "test_metric_comp", run, table_file]
            )
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))

            butler = Butler(self.root)
            refs = list(butler.registry.queryDatasets("test_metric_comp", collections=run))
            self.assertEqual(len(refs), 2)

            for i, data in enumerate(self.datasets):
                butler_data = butler.get(
                    "test_metric_comp", visit=self.visits[i], instrument=self.instruments[i], collections=run
                )
                self.assertEqual(butler_data, data)


if __name__ == "__main__":
    unittest.main()

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

import os
import json
import unittest
from astropy.table import Table

from lsst.daf.butler.cli.butler import cli
from lsst.daf.butler.cli.utils import clickResultMsg, LogCliRunner
from lsst.daf.butler import Butler
from lsst.daf.butler.tests import MetricsExample
from lsst.daf.butler.tests.utils import ButlerTestHelper, makeTestTempDir, MetricTestRepo, removeTestTempDir


TESTDIR = os.path.abspath(os.path.dirname(__file__))


class CliIngestFilesTest(unittest.TestCase, ButlerTestHelper):

    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")

    def setUp(self):
        self.root = makeTestTempDir(TESTDIR)
        self.root2 = makeTestTempDir(TESTDIR)
        self.testRepo = MetricTestRepo(self.root,
                                       configFile=self.configFile)

    def tearDown(self):
        removeTestTempDir(self.root)

    def testSimpleIngest(self):

        # Create a couple of test JSON files.
        files = []
        datasets = []
        for i in range(2):
            data = MetricsExample(summary={"int": i, "string": f"{i}"})
            outfile = f"test{i}.json"
            with open(os.path.join(self.root2, outfile), "w") as fd:
                json.dump(data._asdict(), fd)
            datasets.append(data)
            files.append(outfile)

        # Create some Astropy tables with ingest options.
        tables = (
            # Everything in the table with relative path.
            (Table([files, [423, 424], ["DummyCamComp", "DummyCamComp"]],
                   names=["Files", "visit", "instrument"]),
             ("--prefix", self.root2)),
            # Relative path with instrument factored out.
            (Table([files, [423, 424]],
                   names=["Files", "visit"]),
             ("--data-id", "instrument=DummyCamComp", "--prefix", self.root2)),
            # Absolute path with instrument factored out.
            (Table([[os.path.join(self.root2, f) for f in files], [423, 424]],
                   names=["Files", "visit"]),
             ("--data-id", "instrument=DummyCamComp")),
        )

        runner = LogCliRunner()
        with runner.isolated_filesystem():

            for i, (table, options) in enumerate(tables):

                table_file = os.path.join(self.root2, f"table{i}.csv")
                table.write(table_file)

                run = f"u/user/test{i}"
                result = runner.invoke(cli, ["ingest-files", *options,
                                             self.root, "test_metric_comp", run, table_file])
                self.assertEqual(result.exit_code, 0, clickResultMsg(result))

                butler = Butler(self.root)
                refs = list(butler.registry.queryDatasets("test_metric_comp", collections=run))
                self.assertEqual(len(refs), 2)

                for i, data in enumerate(datasets):
                    butler_data = butler.get("test_metric_comp", visit=423+i, instrument="DummyCamComp",
                                             collections=run)
                    self.assertEqual(butler_data, data)


if __name__ == "__main__":
    unittest.main()

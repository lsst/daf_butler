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

"""Unit tests for daf_butler CLI retrieve-artifacts command."""

import os
import unittest

from lsst.daf.butler import StorageClassFactory
from lsst.daf.butler.cli.butler import cli
from lsst.daf.butler.cli.utils import LogCliRunner, clickResultMsg
from lsst.daf.butler.tests.utils import ButlerTestHelper, MetricTestRepo, makeTestTempDir, removeTestTempDir
from lsst.resources import ResourcePath

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class CliRetrieveArtifactsTest(unittest.TestCase, ButlerTestHelper):
    """Test the retrieve-artifacts command-line."""

    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")
    storageClassFactory = StorageClassFactory()

    def setUp(self):
        self.root = makeTestTempDir(TESTDIR)
        self.testRepo = MetricTestRepo(self.root, configFile=self.configFile)

    def tearDown(self):
        removeTestTempDir(self.root)

    @staticmethod
    def find_files(root: str) -> list[ResourcePath]:
        return list(ResourcePath.findFileResources([root]))

    def testRetrieveAll(self):
        runner = LogCliRunner()
        with runner.isolated_filesystem():
            # When preserving the path the run will be in the directory along
            # with a . in the component name.  When not preserving paths the
            # filename will have an underscore rather than dot.
            for counter, (preserve_path, prefix) in enumerate(
                (
                    ("--preserve-path", "ingest/run/test_metric_comp."),
                    ("--no-preserve-path", "test_metric_comp_"),
                )
            ):
                destdir = f"tmp{counter}/"
                result = runner.invoke(cli, ["retrieve-artifacts", self.root, destdir, preserve_path])
                self.assertEqual(result.exit_code, 0, clickResultMsg(result))
                self.assertTrue(result.stdout.endswith(": 6\n"), f"Expected 6 got: {result.stdout}")

                artifacts = self.find_files(destdir)
                artifacts.sort()
                self.assertEqual(len(artifacts), 7, f"Expected 7 artifacts including index: {artifacts}")
                self.assertIn(f"{destdir}{prefix}", str(artifacts[1]))

    def testRetrieveSubset(self):
        runner = LogCliRunner()
        with runner.isolated_filesystem():
            destdir = "tmp1/"
            result = runner.invoke(
                cli,
                [
                    "retrieve-artifacts",
                    self.root,
                    destdir,
                    "--where",
                    "instrument='DummyCamComp' AND visit=423",
                ],
            )
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertTrue(result.stdout.endswith(": 3\n"), f"Expected 3 got: {result.stdout}")
            artifacts = self.find_files(destdir)
            artifacts.sort()
            self.assertEqual(len(artifacts), 4, f"Expected 4 artifacts including index: {artifacts}")

    def testRetrieveAsZip(self):
        runner = LogCliRunner()
        with runner.isolated_filesystem():
            destdir = "tmp1/"
            result = runner.invoke(
                cli,
                [
                    "retrieve-artifacts",
                    self.root,
                    destdir,
                    "--where",
                    "instrument='DummyCamComp' AND visit=423",
                    "--zip",
                ],
            )
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertIn(".zip", result.stdout)
            artifacts = self.find_files(destdir)
            self.assertEqual(len(artifacts), 1, f"Expected one zip file: {artifacts}")

    def testOverwriteLink(self):
        runner = LogCliRunner()
        with runner.isolated_filesystem():
            destdir = "tmp2/"
            # Force hardlink -- if this fails assume that it is because
            # hardlinks are not supported (/tmp and TESTDIR are on
            # different file systems) and skip the test. There are other
            # tests for the command line itself.
            result = runner.invoke(cli, ["retrieve-artifacts", self.root, destdir, "--transfer", "hardlink"])
            if result.exit_code != 0:
                raise unittest.SkipTest(
                    "hardlink not supported between these directories for this test:"
                    f" {clickResultMsg(result)}"
                )

            # Running again should pass because hard links are the same
            # file.
            result = runner.invoke(cli, ["retrieve-artifacts", self.root, destdir])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))

    def testClobber(self):
        runner = LogCliRunner()
        with runner.isolated_filesystem():
            destdir = "tmp2/"
            # Force copy so we can ensure that overwrite tests will trigger.
            result = runner.invoke(cli, ["retrieve-artifacts", self.root, destdir, "--transfer", "copy"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))

            # Running again should fail
            result = runner.invoke(cli, ["retrieve-artifacts", self.root, destdir])
            self.assertNotEqual(result.exit_code, 0, clickResultMsg(result))

            # But with clobber should pass
            result = runner.invoke(cli, ["retrieve-artifacts", self.root, destdir, "--clobber"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))


if __name__ == "__main__":
    unittest.main()

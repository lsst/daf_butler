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

"""Unit tests for daf_butler CLI config-dump command.
"""

import click
import click.testing
import unittest

from lsst.daf.butler.cli import butler
from lsst.daf.butler.cli.mockeredTest import MockeredTestBase
from lsst.daf.butler.cli.utils import clickResultMsg, Mocker, mockEnvVar


class ImportTestCase(MockeredTestBase):

    defaultExpected = dict(repo=None,
                           transfer="auto",
                           output_run=None,
                           directory=None,
                           export_file=None)

    def test_minimal(self):
        """Test only the required parameters, and omit the optional parameters.
        """
        expected = self.makeExpected(repo="here", directory="foo", output_run="out")
        self.run_test(["import", "here",
                       "foo",
                       "--output-run", "out"], expected)

    def test_almostAll(self):
        """Test all the parameters, except export_file which gets its own test
        case below.
        """
        with self.runner.isolated_filesystem():
            expected = self.makeExpected(repo="here", directory="foo", output_run="out",
                                         transfer="symlink")
            self.run_test(["import", "here",
                           "foo",
                           "--output-run", "out",
                           "--transfer", "symlink"], expected)

    def test_missingArgument(self):
        """Verify the command fails if a positional argument is missing"""
        runner = click.testing.CliRunner(env=mockEnvVar)
        result = runner.invoke(butler.cli, ["import", "foo", "--output-run", "out"])
        self.assertNotEqual(result.exit_code, 0, clickResultMsg(result))


class ExportFileCase(unittest.TestCase):

    didRead = None

    def setUp(self):
        # add a side effect to Mocker so that it will call our method when it
        # is called.
        Mocker.mock.side_effect = self.read_test

    def tearDown(self):
        # reset the Mocker's side effect on our way out!
        Mocker.mock.side_effect = None

    @staticmethod
    def read_test(*args, **kwargs):
        """This gets called by the Mocker's side effect when the Mocker is
        called. Our export_file argument is a File so Click will open it before
        calling the Mocker, and thus before it gets here. A little bit is
        written into the file here and that is verified later.
        """
        print("in read_test")
        ExportFileCase.didRead = kwargs["export_file"].read()

    def test_exportFile(self):
        """Test all the parameters, except export_file.
        """
        runner = click.testing.CliRunner(env=mockEnvVar)
        with runner.isolated_filesystem():
            f = open("output.yaml", "w")
            f.write("foobarbaz")
            f.close()
            result = runner.invoke(butler.cli, ["import", "here", "foo", "--output-run", "out",
                                                "--export-file", "output.yaml"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertEqual("foobarbaz", ExportFileCase.didRead)


if __name__ == "__main__":
    unittest.main()

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

"""Unit tests for daf_butler CLI config-dump command."""

import unittest
import unittest.mock

from lsst.daf.butler.cli.cmd import butler_import
from lsst.daf.butler.tests import CliCmdTestBase


class ImportTestCase(CliCmdTestBase, unittest.TestCase):
    """Test the butler import command-line."""

    mockFuncName = "lsst.daf.butler.cli.cmd.commands.script.butlerImport"

    @staticmethod
    def defaultExpected():
        return dict(
            repo=None,
            transfer="auto",
            directory=None,
            skip_dimensions=(),
            export_file=None,
            track_file_attrs=True,
        )

    @staticmethod
    def command():
        return butler_import

    def test_minimal(self):
        """Test only required parameters, and omit optional parameters."""
        self.run_test(["import", "here", "foo"], self.makeExpected(repo="here", directory="foo"))

    def test_all(self):
        """Test all the parameters."""
        self.run_test(
            ["import", "here", "foo", "--transfer", "symlink", "--export-file", "file"],
            self.makeExpected(repo="here", directory="foo", transfer="symlink", export_file="file"),
        )

    def test_missingArgument(self):
        """Verify the command fails if either of the positional arguments,
        REPO or DIRECTORY, is missing.
        """
        self.run_missing(["import", "foo"], r"Error: Missing argument ['\"]DIRECTORY['\"].")


if __name__ == "__main__":
    unittest.main()

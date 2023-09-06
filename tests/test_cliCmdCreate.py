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

import unittest

from lsst.daf.butler.cli.cmd import create
from lsst.daf.butler.tests import CliCmdTestBase


class CreateTest(CliCmdTestBase, unittest.TestCase):
    """Test the butler create command-line."""

    mockFuncName = "lsst.daf.butler.cli.cmd.commands.script.createRepo"

    @staticmethod
    def defaultExpected():
        return dict(
            repo=None, seed_config=None, dimension_config=None, standalone=False, override=False, outfile=None
        )

    @staticmethod
    def command():
        return create

    def test_minimal(self):
        """Test only required parameters."""
        self.run_test(["create", "here"], self.makeExpected(repo="here"))

    def test_requiredMissing(self):
        """Test that if the required parameter is missing it fails"""
        self.run_missing(["create"], r"Error: Missing argument ['\"]REPO['\"].")

    def test_all(self):
        """Test all parameters."""
        self.run_test(
            [
                "create",
                "here",
                "--seed-config",
                "foo",
                "--dimension-config",
                "/bar/dim.yaml",
                "--standalone",
                "--override",
                "--outfile",
                "bar",
            ],
            self.makeExpected(
                repo="here",
                seed_config="foo",
                dimension_config="/bar/dim.yaml",
                standalone=True,
                override=True,
                outfile="bar",
            ),
        )


if __name__ == "__main__":
    unittest.main()

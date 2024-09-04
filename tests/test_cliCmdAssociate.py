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

"""Unit tests for daf_butler CLI prune-datasets subcommand.
"""

import unittest
from unittest.mock import patch

from lsst.daf.butler.cli.butler import cli as butlerCli
from lsst.daf.butler.cli.utils import LogCliRunner, clickResultMsg


class AssociateTestCase(unittest.TestCase):
    """Tests the ``associate`` ``butler`` subcommand.

    ``script.associate`` contains no logic, so instead of mocking the
    internals, just mock the call to that function to test for expected inputs
    and input types.
    """

    def setUp(self):
        self.runner = LogCliRunner()

    @patch("lsst.daf.butler.script.associate")
    def test_defaults(self, mockAssociate):
        """Test the expected default values & types for optional options."""
        result = self.runner.invoke(butlerCli, ["associate", "myRepo", "myCollection"])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        mockAssociate.assert_called_once_with(
            repo="myRepo",
            collection="myCollection",
            dataset_type=(),
            collections=(),
            where="",
            find_first=False,
            limit=0,
        )

    @patch("lsst.daf.butler.script.associate")
    def test_values(self, mockAssociate):
        """Test expected values & types when passing in options."""
        result = self.runner.invoke(
            butlerCli,
            [
                "associate",
                "myRepo",
                "myCollection",
                "--dataset-type",
                "myDatasetType",
                "--collections",
                "myCollection,otherCollection",
                "--where",
                "'a=b'",
                "--find-first",
                "--limit",
                "-5000",
            ],
        )
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        mockAssociate.assert_called_once_with(
            repo="myRepo",
            collection="myCollection",
            dataset_type=("myDatasetType",),
            collections=("myCollection", "otherCollection"),
            where="'a=b'",
            find_first=True,
            limit=-5000,
        )


if __name__ == "__main__":
    unittest.main()

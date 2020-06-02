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

"""Unit tests for daf_butler CLI install-external-data command.
"""

import unittest

from lsst.daf.butler.tests.mockeredTest import MockeredTestBase


class InstallExternalDataTestCase(MockeredTestBase):

    defaultExpected = dict(repo=None,
                           source=None,
                           tract=0,
                           visit_ccd=())

    def test_minimal(self):
        """Test only the required parameters, and omit the optional parameters.
        """
        self.run_test(["install-external-data", "here"],
                      self.makeExpected(repo="here"))

    def test_all(self):
        """Test all the parameters, except export_file which gets its own test
        case below.
        """
        self.run_test(["install-external-data", "here",
                       "--source", "thesource",
                       "--tract", "42",
                       "--visit-ccd", "1", "23",
                       "--visit-ccd", "2", "34"],
                      self.makeExpected(repo="here",
                                        source="thesource",
                                        tract=42,
                                        visit_ccd=((1, 23), (2, 34))))

    def test_missingArgument(self):
        """Verify the command fails if either of the positional arguments,
        REPO or DIRECTORY, is missing."""
        self.run_missing(["install-external-data"],
                         'Error: Missing argument "REPO".')


if __name__ == "__main__":
    unittest.main()

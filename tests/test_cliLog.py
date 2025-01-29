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

"""Unit tests for the daf_butler dataset-type CLI option."""

import logging
import unittest

from lsst.daf.butler.cli.cliLog import CliLog
from lsst.daf.butler.tests import CliLogTestBase

try:
    import lsst.log as lsstLog
except ModuleNotFoundError:
    lsstLog = None


class CliLogTestCase(CliLogTestBase, unittest.TestCase):
    """Test log initialization, reset, and setting log levels on python
    `logging` and also `lsst.log` if it is setup.

    This will not test use of `lsst.log` in CI because daf_butler does not
    directly depend on that package. When running in an environment where
    `lsst.log` is setup then this will test use of `lsst.log`. This test also
    runs in obs_base which does provide coverage of python `logging` and
    `lsst.log` in CI.
    """

    pass


class ConvertPyLogLevelTestCase(unittest.TestCase):
    """Test python command-line log levels."""

    def test_convertToPyLogLevel(self):
        self.assertEqual(logging.CRITICAL, CliLog._getPyLogLevel("CRITICAL"))
        self.assertEqual(logging.ERROR, CliLog._getPyLogLevel("ERROR"))
        self.assertEqual(logging.WARN, CliLog._getPyLogLevel("WARNING"))
        self.assertEqual(logging.INFO, CliLog._getPyLogLevel("INFO"))
        self.assertEqual(logging.DEBUG, CliLog._getPyLogLevel("DEBUG"))


if __name__ == "__main__":
    unittest.main()

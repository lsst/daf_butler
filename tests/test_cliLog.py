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

"""Unit tests for the daf_butler dataset-type CLI option.
"""

import logging
import unittest

from lsst.daf.butler.cli.cmd import create
from lsst.daf.butler.cli.log import Log
from lsst.daf.butler.tests import CliCmdTestBase

try:
    import lsst.log as lsstLog
except ModuleNotFoundError:
    lsstLog = None


class ConvertLogLevelTestCase(unittest.TestCase):
    """Test that the log levels accepted by the log_level_option are translated
    to lsst.log levels correctly."""

    # convert to lsst log level test is implemented in obs_base, becasue
    # daf_butler does not depend on lsst.log directly and so it can not be
    # tested here.

    def test_convertToPyLogLevel(self):
        self.assertEqual(logging.CRITICAL, Log.getPyLogLevel("CRITICAL"))
        self.assertEqual(logging.ERROR, Log.getPyLogLevel("ERROR"))
        self.assertEqual(logging.WARN, Log.getPyLogLevel("WARNING"))
        self.assertEqual(logging.INFO, Log.getPyLogLevel("INFO"))
        self.assertEqual(logging.DEBUG, Log.getPyLogLevel("DEBUG"))


class LogLevelTestCase(CliCmdTestBase,
                       unittest.TestCase):

    command = create
    defaultExpected = dict(repo=None,
                           seed_config=None,
                           standalone=False,
                           override=False,
                           outfile=None)

    def test_setLevelOnCommand(self):
        """Test setting the level via the butler command. Set the global level
        as well as a component level, and get the levels to verify they have
        been set as expected. Uninitialize the log, and verify that the root
        logger and component loggers level have been returned to NOTSET."""
        component = "lsst.daf.butler"
        self.run_test(["--log-level", "WARNING",
                       "--log-level", f"{component}=DEBUG",
                       "create", "foo"],
                       self.makeExpected(repo="foo"))
        componentLogger = logging.getLogger(component)
        self.assertEqual(componentLogger.level, logging.DEBUG)
        rootLogger = logging.getLogger(None)
        self.assertEqual(rootLogger.level, logging.WARNING)
        self.assertEqual(len(rootLogger.handlers), 1,
                         msg="After init there should be exactly one handler.")
        Log.uninitLog()
        self.assertEqual(len(rootLogger.handlers), 1 if lsstLog is None else 0,
                         msg="After uninit, if the `lsst.log` handler was used it should have been removed, "
                             "leaving zero handlers. If the `lsst.log` handler was not used then the basic "
                             "config handler should still be installed, (leaving one handler).")
        print(len(logging.getLogger().handlers))
        self.assertEqual(componentLogger.level, logging.NOTSET)
        self.assertEqual(rootLogger.level, logging.NOTSET)



if __name__ == "__main__":
    unittest.main()

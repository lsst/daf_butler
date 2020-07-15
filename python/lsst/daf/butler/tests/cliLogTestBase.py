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

"""Unit tests for the daf_butler CliLog utility. Code is implemented in
daf_butler but some only runs if lsst.log.Log can be imported so these parts of
it can't be tested there because daf_butler does not directly depend on
lsst.log, and only uses it if it has been setup by another package."""

import click
from collections import namedtuple
from functools import partial
import logging
import unittest

from lsst.daf.butler.cli.butler import cli as butlerCli
from lsst.daf.butler.cli.cliLog import CliLog
from lsst.daf.butler.cli.utils import clickResultMsg, command_test_env, LogCliRunner
try:
    import lsst.log as lsstLog
except ModuleNotFoundError:
    lsstLog = None


def hasLsstLogHandler(logger):
    """Check if a python logger has an lsst.log.LogHandler installed.

    Parameters
    ----------
    logger : `logging.logger`
        A python logger.

    Returns
    ------
    `bool`
        True if the logger has an lsst.log.LogHander installed, else False.
    """
    if lsstLog is None:
        return False
    for handler in logging.getLogger().handlers:
        if isinstance(handler, lsstLog.LogHandler):
            return True


@click.command()
@click.option("--expected-pyroot-level")
@click.option("--expected-pybutler-level")
@click.option("--expected-lsstroot-level")
@click.option("--expected-lsstbutler-level")
def command_log_settings_test(expected_pyroot_level,
                              expected_pybutler_level,
                              expected_lsstroot_level,
                              expected_lsstbutler_level):
    if lsstLog is not None and not hasLsstLogHandler(logging.getLogger()):
        raise click.ClickException("Expected to find an lsst.log handler in the python root logger's "
                                   "handlers.")

    LogLevel = namedtuple("LogLevel", ("expected", "actual", "name"))

    logLevels = [LogLevel(expected_pyroot_level,
                          logging.getLogger().level,
                          "pyRoot"),
                 LogLevel(expected_pybutler_level,
                          logging.getLogger("lsst.daf.butler").level,
                          "pyButler")]
    if lsstLog is not None:
        logLevels.extend([LogLevel(expected_lsstroot_level,
                                   lsstLog.getLogger("").getLevel(),
                                   "lsstRoot"),
                          LogLevel(expected_lsstbutler_level,
                                   lsstLog.getLogger("lsst.daf.butler").getLevel(),
                                   "lsstButler")])
    for expected, actual, name in logLevels:
        if expected != actual:
            raise(click.ClickException(f"expected {name} level to be {expected}, actual:{actual}"))


class CliLogTestBase():
    """Tests log initialization, reset, and setting log levels."""

    lsstLogHandlerId = None

    def setUp(self):
        self.runner = LogCliRunner()

    def tearDown(self):
        self.lsstLogHandlerId = None

    class PythonLogger:
        """Keeps track of log level of a component and number of handlers
        attached to it at the time this object was initialized."""

        def __init__(self, component):
            self.logger = logging.getLogger(component)
            self.initialLevel = self.logger.level

    class LsstLogger:
        """Keeps track of log level for a component at the time this object was
        initialized."""
        def __init__(self, component):
            self.logger = lsstLog.getLogger(component) if lsstLog else None
            self.initialLevel = self.logger.getLevel() if lsstLog else None

    def runTest(self, cmd):
        """Test that the log context manager works with the butler cli to
        initialize the logging system according to cli inputs for the duration
        of the command execution and resets the logging system to its previous
        state or expected state when command execution finishes."""
        pyRoot = self.PythonLogger(None)
        pyButler = self.PythonLogger("lsst.daf.butler")
        lsstRoot = self.LsstLogger("")
        lsstButler = self.LsstLogger("lsst.daf.butler")

        with command_test_env(self.runner, "lsst.daf.butler.tests.cliLogTestBase",
                                           "command-log-settings-test"):
            result = cmd()
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))

        if lsstLog is not None:
            self.assertFalse(hasLsstLogHandler(logging.getLogger()),
                             msg="CliLog should remove the lsst.log handler it added to the root logger.")
        self.assertEqual(pyRoot.logger.level, logging.INFO)
        self.assertEqual(pyButler.logger.level, pyButler.initialLevel)
        if lsstLog is not None:
            self.assertEqual(lsstRoot.logger.getLevel(), lsstLog.INFO)
            # lsstLogLevel can either be the inital level, or uninitialized or
            # the defined default value.
            expectedLsstLogLevel = ((lsstButler.initialLevel, ) if lsstButler.initialLevel != -1
                                    else(-1, CliLog.defaultLsstLogLevel))
            self.assertIn(lsstButler.logger.getLevel(), expectedLsstLogLevel)

    def test_butlerCliLog(self):
        """Test that the log context manager works with the butler cli to
        initialize the logging system according to cli inputs for the duration
        of the command execution and resets the logging system to its previous
        state or expected state when command execution finishes."""

        self.runTest(partial(self.runner.invoke,
                             butlerCli,
                             ["--log-level", "WARNING",
                              "--log-level", "lsst.daf.butler=DEBUG",
                              "command-log-settings-test",
                              "--expected-pyroot-level", logging.WARNING,
                              "--expected-pybutler-level", logging.DEBUG,
                              "--expected-lsstroot-level", lsstLog.WARN if lsstLog else 0,
                              "--expected-lsstbutler-level", lsstLog.DEBUG if lsstLog else 0]))

    def test_helpLogReset(self):
        """Verify that when a command does not execute, like when the help menu
        is printed instead, that CliLog is still reset."""

        self.runTest(partial(self.runner.invoke, butlerCli, ["command-log-settings-test", "--help"]))


if __name__ == "__main__":
    unittest.main()

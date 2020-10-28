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
from io import StringIO
import logging
import re
import subprocess
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

    def testLongLog(self):
        """Verify the timestamp is in the log messages when the --long-log
        flag is set."""

        # When longlog=True, loglines start with the log level and a
        # timestamp with the following format:
        # "year-month-day T hour-minute-second.millisecond-zoneoffset"
        # If lsst.log is importable then the timestamp will have
        # milliseconds, as described above. If lsst.log is NOT
        # importable then milliseconds (and the preceding ".") are
        # omitted (the python `time` module does not support
        # milliseconds in its format string). Examples of expected
        # strings follow:
        # lsst.log:   "DEBUG 2020-10-29T10:20:31.518-0700 ..."
        # pure python "DEBUG 2020-10-28T10:20:31-0700 ...""
        # The log level name can change, we verify there is an all
        # caps word there but do not verify the word. We do not verify
        # the rest of the log string, assume that if the timestamp is
        # in the string that the rest of the string will appear as
        # expected.
        # N.B. this test is defined in daf_butler which does not depend
        # on lsst.log. However, CliLog may be used in packages that do
        # depend on lsst.log and so both forms of timestamps must be
        # supported. These packages should have a test (the file is
        # usually called test_cliLog.py) that subclasses CliLogTestBase
        # and unittest.TestCase so that these tests are run in that
        # package.
        timestampRegex = re.compile(
            r"^[A-Z]+ [0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(.[0-9]{3})?[-,+][0-9]{4} .*")

        # When longlog=False, log lines start with the module name and
        # log level, for example:
        # lsst.daf.butler.core.config DEBUG: ...
        modulesRegex = re.compile(
            r"^([a-z]+\.)+[a-z]+ [A-Z]+: .*")

        with self.runner.isolated_filesystem():
            for longlog in (True, False):
                # The click test does not capture logging emitted from lsst.log
                # so use subprocess to run the test instead.
                if longlog:
                    args = ("butler", "--log-level", "DEBUG", "--long-log", "create", "here")
                else:
                    args = ("butler", "--log-level", "DEBUG", "create", "here")
                result = subprocess.run(args, capture_output=True)
                output = StringIO((result.stderr.decode()))
                startedWithTimestamp = any([timestampRegex.match(line) for line in output.readlines()])
                output.seek(0)
                startedWithModule = any(modulesRegex.match(line) for line in output.readlines())
                if longlog:
                    self.assertTrue(startedWithTimestamp,
                                    msg=f"did not find timestamp in: \n{output.getvalue()}")
                    self.assertFalse(startedWithModule,
                                     msg=f"found lines starting with module in: \n{output.getvalue()}")
                else:
                    self.assertFalse(startedWithTimestamp,
                                     msg=f"found timestamp in: \n{output.getvalue()}")
                    self.assertTrue(startedWithModule,
                                    msg=f"did not find lines starting with module in: \n{output.getvalue()}")


if __name__ == "__main__":
    unittest.main()

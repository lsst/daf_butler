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

import unittest

from lsst.daf.butler.cli.cliLog import CliLog
from lsst.utils.logging import LogState

try:
    import lsst.log as lsstLog
except ModuleNotFoundError:
    lsstLog = None


class LogStateTestCase(unittest.TestCase):
    """Test python command-line log levels."""

    def test_state(self):
        """Tests states"""
        CliLog.resetLog()
        CliLog.initLog(False, True, (), ())

        state_list = LogState.get_state()
        first = state_list[0]

        self.assertEqual(first[0].__func__, CliLog.initLog.__func__)
        self.assertEqual(first[1], False)
        self.assertEqual(first[2], True)
        self.assertEqual(first[3], ())
        self.assertEqual(first[4], ())

        LogState.clear_state()
        state_list = LogState.get_state()
        self.assertEqual(len(state_list), 0)

    def test_levels(self):
        """Tests setting LogLevel in CliLog and retrieving it from LogState"""
        CliLog.resetLog()
        CliLog.setLogLevels([(None, "CRITICAL")])
        state_list = LogState.get_state()
        first = state_list[0]

        self.assertEqual(first[0].__func__, CliLog._setLogLevel.__func__)
        self.assertEqual(first[1], None)
        self.assertEqual(first[2], "CRITICAL")

    def test_replay(self):
        """Tests replay_log_state"""
        CliLog.resetLog()

        CliLog.setLogLevels([(None, "WARNING")])

        state_list = LogState.get_state()
        LogState.clear_state()

        LogState.replay_state(state_list)
        with self.assertLogs(None, level="WARN"):
            LogState.replay_state(state_list)
        LogState.clear_state()

        LogState.replay_state(state_list)
        state_list = LogState.get_state()
        self.assertEqual(len(state_list), 0)

        CliLog.setLogLevels([(None, "WARNING")])
        state_list = LogState.get_state()
        with self.assertWarns(DeprecationWarning):
            CliLog.replayConfigState(state_list)

    def test_deprecations(self):
        """Test deprecated accessor in CliLog"""
        CliLog.resetLog()

        CliLog.setLogLevels([(None, "WARNING")])
        state_list = LogState.get_state()

        with self.assertWarns(DeprecationWarning):
            x = CliLog.configState  # noqa: F841

        with self.assertWarns(DeprecationWarning):
            CliLog.configState = state_list


if __name__ == "__main__":
    unittest.main()

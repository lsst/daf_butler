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

import unittest
import logging

from lsst.daf.butler import ButlerLogRecordHandler, ButlerLogRecords, VERBOSE


class LoggingTestCase(unittest.TestCase):
    """Test we can capture log messages."""

    def setUp(self):
        self.handler = ButlerLogRecordHandler()

        self.log = logging.getLogger(self.id())
        self.log.addHandler(self.handler)

    def tearDown(self):
        if self.handler and self.log:
            self.log.removeHandler(self.handler)

    def testRecordCapture(self):

        self.log.setLevel(VERBOSE)

        test_messages = (
            (logging.INFO, "This is a log message", True),
            (logging.WARNING, "This is a warning message", True),
            (logging.DEBUG, "This debug message should not be stored", False),
            (VERBOSE, "A verbose message should appear", True),
        )

        for level, message, _ in test_messages:
            self.log.log(level, message)

        expected = [info for info in test_messages if info[2]]

        self.assertEqual(len(self.handler.records), len(expected))

        for given, record in zip(expected, self.handler.records):
            self.assertEqual(given[0], record.levelno)
            self.assertEqual(given[1], record.message)

        # Check that we can serialize the records
        json = self.handler.records.json()

        records = ButlerLogRecords.parse_raw(json)
        for original_record, new_record in zip(self.handler.records, records):
            self.assertEqual(new_record, original_record)
        self.assertEqual(str(records), str(self.handler.records))

    def testExceptionInfo(self):

        self.log.setLevel(logging.DEBUG)
        try:
            raise RuntimeError("A problem has been encountered.")
        except RuntimeError:
            self.log.exception("Caught")

        self.assertIn("A problem has been encountered", self.handler.records[0].exc_info)

        self.log.warning("No exc_info")
        self.assertIsNone(self.handler.records[-1].exc_info)

        try:
            raise RuntimeError("Debug exception log")
        except RuntimeError:
            self.log.debug("A problem", exc_info=1)

        self.assertIn("Debug exception", self.handler.records[-1].exc_info)


if __name__ == "__main__":
    unittest.main()

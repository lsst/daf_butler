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

    def testRecordCapture(self):
        handler = ButlerLogRecordHandler()

        log = logging.getLogger(self.id())
        log.setLevel(VERBOSE)
        log.addHandler(handler)

        test_messages = (
            (logging.INFO, "This is a log message", True),
            (logging.WARNING, "This is a warning message", True),
            (logging.DEBUG, "This debug message should not be stored", False),
            (VERBOSE, "A verbose message should appear", True),
        )

        for level, message, _ in test_messages:
            log.log(level, message)

        expected = [info for info in test_messages if info[2]]

        self.assertEqual(len(handler.records), len(expected))

        for given, record in zip(expected, handler.records):
            self.assertEqual(given[0], record.levelno)
            self.assertEqual(given[1], record.message)

        # Check that we can serialize the records
        json = handler.records.json()

        records = ButlerLogRecords.parse_raw(json)
        for original_record, new_record in zip(handler.records, records):
            self.assertEqual(new_record, original_record)
        self.assertEqual(str(records), str(handler.records))


if __name__ == "__main__":
    unittest.main()

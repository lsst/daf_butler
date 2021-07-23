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
import io
import logging
import tempfile
from logging import StreamHandler, FileHandler

from lsst.daf.butler import ButlerLogRecordHandler, ButlerLogRecords, VERBOSE, JsonFormatter, ButlerLogRecord


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
        """Test basic log capture and serialization."""

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

        # Create stream form of serialization.
        json_stream = "\n".join(record.json() for record in records)

        # Also check we can autodetect the format.
        for raw in (json, json.encode(), json_stream, json_stream.encode()):
            records = ButlerLogRecords.from_raw(json)
            self.assertEqual(records, self.handler.records)

        for raw in ("", b""):
            self.assertEqual(len(ButlerLogRecords.from_raw(raw)), 0)
        self.assertEqual(len(ButlerLogRecords.from_stream(io.StringIO())), 0)

        # Send bad text to the parser and it should fail (both bytes and str).
        bad_text = "x" * 100

        # Include short and long values to trigger different code paths
        # in error message creation.
        for trim in (True, False):
            for bad in (bad_text, bad_text.encode()):
                bad = bad[:10] if trim else bad
                with self.assertRaises(ValueError) as cm:
                    ButlerLogRecords.from_raw(bad)
                if not trim:
                    self.assertIn("...", str(cm.exception))

    def testButlerLogRecords(self):
        """Test the list-like methods of ButlerLogRecords."""

        self.log.setLevel(logging.INFO)

        n_messages = 10
        message = "Message #%d"
        for counter in range(n_messages):
            self.log.info(message, counter)

        records = self.handler.records
        self.assertEqual(len(records), n_messages)

        # Test slicing.
        start = 2
        end = 6
        subset = records[start:end]
        self.assertIsInstance(subset, ButlerLogRecords)
        self.assertEqual(len(subset), end - start)
        self.assertIn(f"#{start}", subset[0].message)

        # Reverse the collection.
        backwards = list(reversed(records))
        self.assertEqual(len(backwards), len(records))
        self.assertEqual(records[0], backwards[-1])

        # Test some of the collection manipulation methods.
        record_0 = records[0]
        records.reverse()
        self.assertEqual(records[-1], record_0)
        self.assertEqual(records.pop(), record_0)
        records[0] = record_0
        self.assertEqual(records[0], record_0)
        len_records = len(records)
        records.insert(2, record_0)
        self.assertEqual(len(records), len_records + 1)
        self.assertEqual(records[0], records[2])

        # Put the subset records back onto the end of the original.
        records.extend(subset)
        self.assertEqual(len(records), n_messages + len(subset))

        # Test slice for deleting
        initial_length = len(records)
        start_del = 1
        end_del = 3
        del records[start_del:end_del]
        self.assertEqual(len(records), initial_length - (end_del - start_del))

        records.clear()
        self.assertEqual(len(records), 0)

        with self.assertRaises(ValueError):
            records.append({})

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


class TestJsonLogging(unittest.TestCase):

    def testJsonLogStream(self):
        log = logging.getLogger(self.id())
        log.setLevel(logging.INFO)

        # Log to a stream and also to a file.
        formatter = JsonFormatter()

        stream = io.StringIO()
        stream_handler = StreamHandler(stream)
        stream_handler.setFormatter(formatter)
        log.addHandler(stream_handler)

        file = tempfile.NamedTemporaryFile(suffix=".json")
        filename = file.name
        file.close()

        file_handler = FileHandler(filename)
        file_handler.setFormatter(formatter)
        log.addHandler(file_handler)

        log.info("A message")
        log.warning("A warning")

        # Rewind the stream and pull messages out of it.
        stream.seek(0)
        records = ButlerLogRecords.from_stream(stream)
        self.assertIsInstance(records[0], ButlerLogRecord)
        self.assertEqual(records[0].message, "A message")
        self.assertEqual(records[1].levelname, "WARNING")

        # Now read from the file.
        file_handler.close()
        file_records = ButlerLogRecords.from_file(filename)
        self.assertEqual(file_records, records)

        # And read the file again in bytes and text.
        for mode in ("rb", "r"):
            with open(filename, mode) as fd:
                file_records = ButlerLogRecords.from_stream(fd)
                self.assertEqual(file_records, records)
                fd.seek(0)
                file_records = ButlerLogRecords.from_raw(fd.read())
                self.assertEqual(file_records, records)

        # Serialize this model to stream.
        stream2 = io.StringIO()
        print(records.json(), file=stream2)
        stream2.seek(0)
        stream_records = ButlerLogRecords.from_stream(stream2)
        self.assertEqual(stream_records, records)


if __name__ == "__main__":
    unittest.main()

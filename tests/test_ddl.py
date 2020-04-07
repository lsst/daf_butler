# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""Unit tests for methods in butler.ddl module.
"""

import unittest

from astropy.time import Time, TimeDelta
from lsst.daf.butler import ddl


class AstropyTimeNsecTaiTestCase(unittest.TestCase):
    """A test case for AstropyTimeNsecTai class
    """

    def setUp(self):
        self.decor = ddl.AstropyTimeNsecTai()
        # We do not do dialect-specific things
        self.dialect = None

    def test_value_none(self):
        """Tests for converting None (to None).
        """
        value = self.decor.process_bind_param(None, self.dialect)
        self.assertIsNone(value)

        value = self.decor.process_result_value(None, self.dialect)
        self.assertIsNone(value)

    def test_time_before_epoch(self):
        """Tests for converting None in bound parameters.
        """
        time = Time("1950-01-01T00:00:00", format="isot", scale="tai")
        value = self.decor.process_bind_param(time, self.dialect)
        self.assertEqual(value, 0)

        value = self.decor.process_result_value(value, self.dialect)
        self.assertEqual(value, ddl.EPOCH)

    def test_max_time(self):
        """Tests for converting None in bound parameters.
        """
        # there are rounding issues, need more complex comparison
        time = Time("2101-01-01T00:00:00", format="isot", scale="tai")
        value = self.decor.process_bind_param(time, self.dialect)

        value_max = self.decor.process_bind_param(ddl.MAX_TIME, self.dialect)
        self.assertEqual(value, value_max)

    def test_round_trip(self):
        """Test precision of round-trip conversion.
        """
        # do tests at random points between epoch and max. time
        times = [
            "1970-01-01T12:00:00.123",
            "2000-01-01T12:00:00.123456",
            "2030-01-01T12:00:00.123456",
            "2075-08-17T00:03:45",
            "2099-12-31T23:00:50",
        ]
        for time in times:
            atime = Time(time, format="isot", scale="tai")
            for sec in range(7):
                # loop over few seconds to add to each time
                for i in range(100):
                    # loop over additional fractions of seconds
                    delta = sec + 0.3e-9 * i
                    in_time = atime + TimeDelta(delta, format="sec")
                    # do round-trip conversion to nsec and back
                    value = self.decor.process_bind_param(in_time, self.dialect)
                    value = self.decor.process_result_value(value, self.dialect)
                    delta2 = value - in_time
                    delta2_sec = delta2.to_value("sec")
                    # absolute precision should be better than half
                    # nanosecond, but there are rounding errors too
                    self.assertLess(abs(delta2_sec), 0.51e-9)


if __name__ == "__main__":
    unittest.main()

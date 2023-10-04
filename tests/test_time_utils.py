# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""Unit tests for methods in butler.time module.
"""

import unittest
import warnings

# As of astropy 4.2, the erfa interface is shipped independently and
# ErfaWarning is no longer an AstropyWarning
try:
    import erfa
except ImportError:
    erfa = None

import astropy.utils.exceptions
from astropy.time import Time, TimeDelta
from lsst.daf.butler.time_utils import TimeConverter


class TimeTestCase(unittest.TestCase):
    """A test case for time module"""

    def test_time_before_epoch(self):
        """Tests for before-the-epoch time."""
        time = Time("1950-01-01T00:00:00", format="isot", scale="tai")
        value = TimeConverter().astropy_to_nsec(time)
        self.assertEqual(value, 0)

        value = TimeConverter().nsec_to_astropy(value)
        self.assertEqual(value, TimeConverter().epoch)

    def test_max_time(self):
        """Tests for after-the-end-of-astronomy time."""
        # there are rounding issues, need more complex comparison
        time = Time("2101-01-01T00:00:00", format="isot", scale="tai")
        value = TimeConverter().astropy_to_nsec(time)

        value_max = TimeConverter().astropy_to_nsec(TimeConverter().max_time)
        self.assertEqual(value, value_max)

        # Astropy will give "dubious year" for UTC five years in the future
        # so hide these expected warnings from the test output
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=astropy.utils.exceptions.AstropyWarning)
            if erfa is not None:
                warnings.simplefilter("ignore", category=erfa.ErfaWarning)
            time = Time("2101-01-01T00:00:00", format="isot", scale="utc")

        # unittest can't test for no warnings so we run the test and
        # trigger our own warning and count all the warnings
        with self.assertWarns(Warning) as cm:
            TimeConverter().astropy_to_nsec(time)
            warnings.warn("deliberate", stacklevel=1)
        self.assertEqual(str(cm.warning), "deliberate")

    def test_round_trip(self):
        """Test precision of round-trip conversion."""
        # do tests at random points between epoch and max. time
        times = [
            "1970-01-01T12:00:00.123",
            "1999-12-31T23:59:57.123456",
            "2000-01-01T12:00:00.123456",
            "2030-01-01T12:00:00.123456789",
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
                    value = TimeConverter().astropy_to_nsec(in_time)
                    value = TimeConverter().nsec_to_astropy(value)
                    delta2 = value - in_time
                    delta2_sec = delta2.to_value("sec")
                    # absolute precision should be better than half
                    # nanosecond, but there are rounding errors too
                    self.assertLess(abs(delta2_sec), 0.51e-9)

    def test_times_equal(self):
        """Test for times_equal method"""
        # time == time should always work
        time1 = Time("2000-01-01T00:00:00.123456789", format="isot", scale="tai")
        self.assertTrue(TimeConverter().times_equal(time1, time1))

        # one nsec difference
        time1 = Time("2000-01-01T00:00:00.123456789", format="isot", scale="tai")
        time2 = Time("2000-01-01T00:00:00.123456788", format="isot", scale="tai")
        self.assertTrue(TimeConverter().times_equal(time1, time2, 2.0))
        self.assertTrue(TimeConverter().times_equal(time2, time1, 2.0))
        self.assertFalse(TimeConverter().times_equal(time1, time2, 0.5))
        self.assertFalse(TimeConverter().times_equal(time2, time1, 0.5))

        # one nsec difference, times in UTC
        time1 = Time("2000-01-01T00:00:00.123456789", format="isot", scale="utc")
        time2 = Time("2000-01-01T00:00:00.123456788", format="isot", scale="utc")
        self.assertTrue(TimeConverter().times_equal(time1, time2, 2.0))
        self.assertTrue(TimeConverter().times_equal(time2, time1, 2.0))
        self.assertFalse(TimeConverter().times_equal(time1, time2, 0.5))
        self.assertFalse(TimeConverter().times_equal(time2, time1, 0.5))

        # 1/2 nsec difference
        time1 = Time("2000-01-01T00:00:00.123456789", format="isot", scale="tai")
        time2 = time1 + TimeDelta(0.5e-9, format="sec")
        self.assertTrue(TimeConverter().times_equal(time1, time2))
        self.assertTrue(TimeConverter().times_equal(time2, time1))
        self.assertFalse(TimeConverter().times_equal(time1, time2, 0.25))
        self.assertFalse(TimeConverter().times_equal(time2, time1, 0.25))

        # 1/2 microsec difference
        time1 = Time("2000-01-01T00:00:00.123456789", format="isot", scale="tai")
        time2 = time1 + TimeDelta(0.5e-6, format="sec")
        self.assertTrue(TimeConverter().times_equal(time1, time2, 501))
        self.assertTrue(TimeConverter().times_equal(time2, time1, 501))
        self.assertFalse(TimeConverter().times_equal(time1, time2, 499))
        self.assertFalse(TimeConverter().times_equal(time2, time1, 499))

        # UTC vs TAI
        time1 = Time("2013-06-17 13:34:45.775000", scale="tai", format="iso")
        time2 = Time("2013-06-17T13:34:10.775", scale="utc", format="isot")
        self.assertTrue(TimeConverter().times_equal(time1, time2))
        self.assertEqual(TimeConverter().astropy_to_nsec(time1), TimeConverter().astropy_to_nsec(time2))


if __name__ == "__main__":
    unittest.main()

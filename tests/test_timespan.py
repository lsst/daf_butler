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
import itertools

import astropy.time

from lsst.daf.butler import Timespan


class TimespanTestCase(unittest.TestCase):
    """Tests for the `Timespan` class.

    Test coverage for the `DatabaseTimespanRepresentation` classes is handled
    by the tests for `Database` and its subclasses.
    """

    def setUp(self):
        start = astropy.time.Time('2020-01-01T00:00:00', format="isot", scale="tai")
        offset = astropy.time.TimeDelta(60, format="sec")
        self.timestamps = [start + offset*n for n in range(3)]
        self.timespans = [Timespan(begin=None, end=None)]
        self.timespans.extend(Timespan(begin=None, end=t) for t in self.timestamps)
        self.timespans.extend(Timespan(begin=t, end=None) for t in self.timestamps)
        self.timespans.extend(Timespan(begin=a, end=b) for a, b in itertools.combinations(self.timestamps, 2))

    def testStrings(self):
        """Test __str__ against expected values and __repr__ with eval
        round-tripping.
        """
        for ts in self.timespans:
            # Uncomment the next line and run this test directly for the most
            # important test: human inspection.
            #    print(str(ts), repr(ts))
            self.assertIn(", ", str(ts))
            self.assertTrue(str(ts).endswith(")"))
            if ts.begin is None:
                self.assertTrue(str(ts).startswith("(-∞, "))
            else:
                self.assertTrue(str(ts).startswith(f"[{ts.begin}, "))
            if ts.end is None:
                self.assertTrue(str(ts).endswith(", ∞)"))
            else:
                self.assertTrue(str(ts).endswith(f", {ts.end})"))
            self.assertEqual(eval(repr(ts)), ts)

    def testOperationConsistency(self):
        """Test that overlaps, intersection, and difference are consistent.
        """
        for a, b in itertools.combinations_with_replacement(self.timespans, 2):
            with self.subTest(a=str(a), b=str(b)):
                c1 = a.intersection(b)
                c2 = b.intersection(a)
                diffs1 = tuple(a.difference(b))
                diffs2 = tuple(b.difference(a))
                if a == b:
                    self.assertFalse(diffs1)
                    self.assertFalse(diffs2)
                else:
                    for t in diffs1:
                        self.assertTrue(a.overlaps(t))
                        self.assertFalse(b.overlaps(t))
                    for t in diffs2:
                        self.assertTrue(b.overlaps(t))
                        self.assertFalse(a.overlaps(t))
                self.assertEqual(c1, c2)
                if a.overlaps(b):
                    self.assertTrue(b.overlaps(a))
                    self.assertIsNotNone(c1)
                else:
                    self.assertFalse(b.overlaps(a))
                    self.assertIsNone(c1)
                    self.assertEqual(diffs1, (a,))
                    self.assertEqual(diffs2, (b,))

    def testPrecision(self):
        """Test that we only use nanosecond precision for equality."""
        ts1 = self.timespans[-1]
        ts2 = Timespan(begin=ts1.begin + astropy.time.TimeDelta(1e-10, format="sec"), end=ts1.end)
        self.assertEqual(ts1, ts2)

        self.assertEqual(Timespan(begin=None, end=None), Timespan(begin=None, end=None))
        self.assertEqual(Timespan(begin=None, end=ts1.end), Timespan(begin=None, end=ts1.end))

        ts2 = Timespan(begin=ts1.begin + astropy.time.TimeDelta(1e-8, format="sec"), end=ts1.end)
        self.assertNotEqual(ts1, ts2)

        ts2 = Timespan(begin=None, end=ts1.end)
        self.assertNotEqual(ts1, ts2)

        t1 = Timespan(begin=astropy.time.Time(2456461.0, val2=0.06580758101851847, format="jd", scale="tai"),
                      end=astropy.time.Time(2456461.0, val2=0.06617994212962963, format="jd", scale="tai"))
        t2 = Timespan(begin=astropy.time.Time(2456461.0, val2=0.06580758101851858, format="jd", scale="tai"),
                      end=astropy.time.Time(2456461.0, val2=0.06617994212962963, format="jd", scale="tai"))
        self.assertEqual(t1, t2)

        # Ensure that == and != work properly
        self.assertTrue(t1 == t2, f"Equality of {t1} and {t2}")
        self.assertFalse(t1 != t2, f"Check != is false for {t1} and {t2}")

    def testTimescales(self):
        """Test time scale conversion occurs on comparison."""
        ts1 = Timespan(begin=astropy.time.Time('2013-06-17 13:34:45.775000', scale='tai', format='iso'),
                       end=astropy.time.Time('2013-06-17 13:35:17.947000', scale='tai', format='iso'))
        ts2 = Timespan(begin=astropy.time.Time('2013-06-17T13:34:10.775', scale='utc', format='isot'),
                       end=astropy.time.Time('2013-06-17T13:34:42.947', scale='utc', format='isot'))
        self.assertEqual(ts1, ts2, f"Compare {ts1} with {ts2}")


if __name__ == "__main__":
    unittest.main()

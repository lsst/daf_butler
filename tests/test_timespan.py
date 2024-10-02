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

import itertools
import unittest
import warnings

import astropy.time
import astropy.utils.exceptions

# As of astropy 4.2, the erfa interface is shipped independently and
# ErfaWarning is no longer an AstropyWarning
try:
    import erfa
except ImportError:
    erfa = None

import pydantic
from lsst.daf.butler import Timespan
from lsst.daf.butler.time_utils import TimeConverter


class TimespanTestCase(unittest.TestCase):
    """Tests for the `Timespan` class.

    Test coverage for the `TimespanDatabaseRepresentation` classes is handled
    by the tests for `Database` and its subclasses.
    """

    def setUp(self):
        start = astropy.time.Time("2020-01-01T00:00:00", format="isot", scale="tai")
        offset = astropy.time.TimeDelta(60, format="sec")
        self.timestamps = [start + offset * n for n in range(3)]
        self.timespans = [Timespan(begin=None, end=None)]
        self.timespans.extend(Timespan(begin=None, end=t) for t in self.timestamps)
        self.timespans.extend(Timespan(begin=t, end=None) for t in self.timestamps)
        self.timespans.extend(Timespan(begin=t, end=t) for t in self.timestamps)
        self.timespans.extend(Timespan(begin=a, end=b) for a, b in itertools.combinations(self.timestamps, 2))

    def testEmpty(self):
        """Test various ways to construct an empty timespan, and that
        operations on empty timespans yield the expected behavior.
        """
        self.assertEqual(
            Timespan.makeEmpty(),
            Timespan(Timespan.EMPTY, Timespan.EMPTY),
        )
        self.assertEqual(
            Timespan.makeEmpty(),
            Timespan(self.timestamps[1], self.timestamps[0]),
        )
        self.assertEqual(
            Timespan.makeEmpty(),
            Timespan(Timespan.EMPTY, self.timestamps[0]),
        )
        self.assertEqual(
            Timespan.makeEmpty(),
            Timespan(self.timestamps[0], Timespan.EMPTY),
        )
        self.assertEqual(
            Timespan.makeEmpty(), Timespan(self.timestamps[0], self.timestamps[0], padInstantaneous=False)
        )
        empty = Timespan.makeEmpty()
        for t in self.timestamps:
            with self.subTest(t=str(t)):
                self.assertFalse(empty < t)
                self.assertFalse(empty > t)
                self.assertFalse(t < empty)
                self.assertFalse(t > empty)
                self.assertFalse(empty.contains(t))
        for t in self.timespans:
            with self.subTest(t=str(t)):
                self.assertTrue(t.contains(empty))
                self.assertFalse(t.overlaps(empty))
                self.assertFalse(empty.overlaps(t))
                self.assertEqual(empty.contains(t), t.isEmpty())
                self.assertFalse(empty < t)
                self.assertFalse(t < empty)
                self.assertFalse(empty > t)
                self.assertFalse(t > empty)

    def testFromInstant(self):
        """Test construction of instantaneous timespans."""
        self.assertEqual(
            Timespan.fromInstant(self.timestamps[0]), Timespan(self.timestamps[0], self.timestamps[0])
        )

    def testInvalid(self):
        """Test that we reject timespans that should not exist."""
        with self.assertRaises(ValueError):
            Timespan(TimeConverter().max_time, None)
        with self.assertRaises(ValueError):
            Timespan(TimeConverter().max_time, TimeConverter().max_time)
        with self.assertRaises(ValueError):
            Timespan(None, TimeConverter().epoch)
        with self.assertRaises(ValueError):
            Timespan(TimeConverter().epoch, TimeConverter().epoch)
        t = TimeConverter().nsec_to_astropy(TimeConverter().max_nsec - 1)
        with self.assertRaises(ValueError):
            Timespan(t, t)
        with self.assertRaises(ValueError):
            Timespan.fromInstant(t)

    def testStrings(self):
        """Test __str__ against expected values and __repr__ with eval
        round-tripping.
        """
        timespans = self.timespans

        # Add timespan that includes Julian day high precision version.
        timespans.append(
            Timespan(
                begin=astropy.time.Time(2458850.0, -0.49930555555555556, format="jd", scale="tai"),
                end=astropy.time.Time(2458850.0, -0.4986111111111111, format="jd", scale="tai"),
            )
        )

        for ts in timespans:
            # Uncomment the next line and run this test directly for the most
            # important test: human inspection.
            #    print(str(ts), repr(ts))
            if ts.isEmpty():
                self.assertEqual("(empty)", str(ts))
            else:
                self.assertIn(", ", str(ts))
                if ts.begin is None:
                    self.assertTrue(str(ts).startswith("(-∞, "))
                else:
                    self.assertTrue(str(ts).startswith(f"[2020-01-01T00:{ts.begin.tai.strftime('%M')}:00, "))
                if ts.end is None:
                    self.assertTrue(str(ts).endswith(", ∞)"))
                else:
                    self.assertTrue(str(ts).endswith(f", 2020-01-01T00:{ts.end.tai.strftime('%M')}:00)"))
                self.assertEqual(eval(repr(ts)), ts)

    def testOperationConsistency(self):
        """Test that overlaps, contains, intersection, and difference are
        consistent.
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
                    self.assertTrue(a.contains(b))
                    self.assertTrue(b.contains(a))
                if a.contains(b):
                    self.assertTrue(a.overlaps(b) or b.isEmpty())
                    self.assertFalse(diffs2)
                if b.contains(a):
                    self.assertTrue(b.overlaps(a) or a.isEmpty())
                    self.assertFalse(diffs1)
                if diffs1 is not None:
                    for t in diffs1:
                        self.assertTrue(a.overlaps(t))
                        self.assertFalse(b.overlaps(t))
                if diffs2 is not None:
                    for t in diffs2:
                        self.assertTrue(b.overlaps(t))
                        self.assertFalse(a.overlaps(t))
                self.assertEqual(c1, c2)
                if a.overlaps(b):
                    self.assertTrue(b.overlaps(a))
                    self.assertFalse(c1.isEmpty())
                else:
                    self.assertTrue(a < b or a > b or a.isEmpty() or b.isEmpty())
                    self.assertFalse(b.overlaps(a))
                    self.assertTrue(c1.isEmpty())
                    if diffs1 is not None:
                        self.assertEqual(diffs1, (a,))
                    if diffs2 is not None:
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

        t1 = Timespan(
            begin=astropy.time.Time(2456461.0, val2=0.06580758101851847, format="jd", scale="tai"),
            end=astropy.time.Time(2456461.0, val2=0.06617994212962963, format="jd", scale="tai"),
        )
        t2 = Timespan(
            begin=astropy.time.Time(2456461.0, val2=0.06580758101851858, format="jd", scale="tai"),
            end=astropy.time.Time(2456461.0, val2=0.06617994212962963, format="jd", scale="tai"),
        )
        self.assertEqual(t1, t2)

        # Ensure that == and != work properly
        self.assertTrue(t1 == t2, f"Equality of {t1} and {t2}")
        self.assertFalse(t1 != t2, f"Check != is false for {t1} and {t2}")

    def testTimescales(self):
        """Test time scale conversion occurs on comparison."""
        ts1 = Timespan(
            begin=astropy.time.Time("2013-06-17 13:34:45.775000", scale="tai", format="iso"),
            end=astropy.time.Time("2013-06-17 13:35:17.947000", scale="tai", format="iso"),
        )
        ts2 = Timespan(
            begin=astropy.time.Time("2013-06-17T13:34:10.775", scale="utc", format="isot"),
            end=astropy.time.Time("2013-06-17T13:34:42.947", scale="utc", format="isot"),
        )
        self.assertEqual(ts1, ts2, f"Compare {ts1} with {ts2}")

    def testFuture(self):
        """Check that we do not get warnings from future dates."""
        # Astropy will give "dubious year" for UTC five years in the future
        # so hide these expected warnings from the test output
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=astropy.utils.exceptions.AstropyWarning)
            if erfa is not None:
                warnings.simplefilter("ignore", category=erfa.ErfaWarning)
            ts1 = Timespan(
                begin=astropy.time.Time(self.timestamps[0], scale="utc", format="iso"),
                end=astropy.time.Time("2099-06-17 13:35:17.947000", scale="utc", format="iso"),
            )
            ts2 = Timespan(
                begin=astropy.time.Time(self.timestamps[0], scale="utc", format="iso"),
                end=astropy.time.Time("2099-06-17 13:35:17.947000", scale="utc", format="iso"),
            )

        # unittest can't test for no warnings so we run the test and
        # trigger our own warning and count all the warnings
        with self.assertWarns(Warning) as cm:
            self.assertEqual(ts1, ts2)
            warnings.warn("deliberate", stacklevel=1)
        self.assertEqual(str(cm.warning), "deliberate")

    def test_serialization(self):
        ts = Timespan(
            begin=astropy.time.Time("2013-06-17 13:34:45.775000", scale="tai", format="iso"),
            end=astropy.time.Time("2013-06-17 13:35:17.947000", scale="tai", format="iso"),
        )
        adapter = pydantic.TypeAdapter(Timespan)
        self.assertIn("TAI", adapter.json_schema()["description"])
        json_roundtripped = adapter.validate_json(adapter.dump_json(ts))
        self.assertIsInstance(json_roundtripped, Timespan)
        self.assertEqual(json_roundtripped, ts)
        python_roundtripped = adapter.validate_python(adapter.dump_python(ts))
        self.assertIsInstance(json_roundtripped, Timespan)
        self.assertEqual(python_roundtripped, ts)
        with self.assertRaises(ValueError):
            adapter.validate_python(12)
        with self.assertRaises(ValueError):
            adapter.validate_json({})

    def test_day_obs(self):
        data = (
            ((20240201, 0), ("2024-02-01T00:00:00.0", "2024-02-02T00:00:00.0")),
            ((19801011, 3600), ("1980-10-11T01:00:00.0", "1980-10-12T01:00:00.0")),
            ((20481231, -7200), ("2048-12-30T22:00:00.0", "2048-12-31T22:00:00.0")),
            ((20481231, 7200), ("2048-12-31T02:00:00.0", "2049-01-01T02:00:00.0")),
        )
        for input, output in data:
            ts1 = Timespan.from_day_obs(input[0], input[1])
            ts2 = Timespan(
                begin=astropy.time.Time(output[0], scale="tai", format="isot"),
                end=astropy.time.Time(output[1], scale="tai", format="isot"),
            )
            self.assertEqual(ts1, ts2)

            with self.assertRaises(ValueError):
                Timespan.from_day_obs(19690101)


if __name__ == "__main__":
    unittest.main()

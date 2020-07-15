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
from __future__ import annotations

__all__ = ("astropy_to_nsec", "nsec_to_astropy", "times_equal")

import logging

import astropy.time


# These constants can be used by client code.
# EPOCH is used to construct times as read from database, its precision is
# used by all those timestamps, set it to 1 microsecond.
EPOCH = astropy.time.Time("1970-01-01 00:00:00", format="iso", scale="tai", precision=6)
"""Epoch for calculating time delta, this is the minimum time that can be
stored in the database.
"""

MAX_TIME = astropy.time.Time("2100-01-01 00:00:00", format="iso", scale="tai")
"""Maximum time value that we can store. Assuming 64-bit integer field we
can actually store higher values but we intentionally limit it to arbitrary
but reasonably high value. Note that this value will be stored in registry
database for eternity, so it should not be changed without proper
consideration.
"""

# number of nanosecons in a day
_NSEC_PER_DAY = 1_000_000_000 * 24 * 3600

_LOG = logging.getLogger(__name__)


def astropy_to_nsec(astropy_time: astropy.time.Time) -> int:
    """Convert astropy time to nanoseconds since epoch.

    Input time is converted to TAI scale before conversion to
    nanoseconds.

    Parameters
    ----------
    astropy_time : `astropy.time.Time`
        Time to be converted.

    Returns
    -------
    time_nsec : `int`
        Nanoseconds since epoch.

    Note
    ----
    Only the limited range of input times is supported by this method as it
    is defined useful in the context of Butler and Registry. If input time is
    earlier than epoch time then this method returns 0. If input time comes
    after the max. time then it returns number corresponding to max. time.
    """
    # sometimes comparison produces warnings if input value is in UTC
    # scale, transform it to TAI before doing anyhting
    value = astropy_time.tai
    # anything before epoch or after MAX_TIME is truncated
    if value < EPOCH:
        _LOG.warning("'%s' is earlier than epoch time '%s', epoch time will be used instead",
                     astropy_time, EPOCH)
        value = EPOCH
    elif value > MAX_TIME:
        _LOG.warning("'%s' is later than max. time '%s', max. time time will be used instead",
                     value, MAX_TIME)
        value = MAX_TIME

    delta = value - EPOCH
    # Special care needed to preserve nanosecond precision.
    # Usually jd1 has no fractional part but just in case.
    jd1, extra_jd2 = divmod(delta.jd1, 1)
    value = int(jd1) * _NSEC_PER_DAY + int(round((delta.jd2 + extra_jd2)*_NSEC_PER_DAY))
    return value


def nsec_to_astropy(time_nsec: int) -> astropy.time.Time:
    """Convert nanoseconds since epoch to astropy time.

    Parameters
    ----------
    time_nsec : `int`
        Nanoseconds since epoch.

    Returns
    -------
    astropy_time : `astropy.time.Time`
        Time to be converted.

    Note
    ----
    Usually the input time for this method is the number returned from
    `astropy_to_nsec` which has a limited range. This method does not check
    that the number falls in the supported range and can produce output
    time that is outside of that range.
    """
    jd1, jd2 = divmod(time_nsec, _NSEC_PER_DAY)
    delta = astropy.time.TimeDelta(float(jd1), float(jd2)/_NSEC_PER_DAY, format="jd", scale="tai")
    value = EPOCH + delta
    return value


def times_equal(time1: astropy.time.Time,
                time2: astropy.time.Time,
                precision_nsec: float = 1.0) -> bool:
    """Check that times are equal within specified precision.

    Parameters
    ----------
    time1, time2 : `astropy.time.Time`
        Times to compare.
    precision_nsec : `float`, optional
        Precision to use for comparison in nanoseconds, default is one
        nanosecond which is larger that round-trip error for conversion
        to/from integer nanoseconds.
    """
    # To compare we need them in common scale, for simplicity just
    # bring them both to TAI scale
    time1 = time1.tai
    time2 = time2.tai
    delta = (time2.jd1 - time1.jd1) + (time2.jd2 - time1.jd2)
    delta *= _NSEC_PER_DAY
    return abs(delta) < precision_nsec

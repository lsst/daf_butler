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
from __future__ import annotations

__all__ = ("TimeConverter",)

import logging
import warnings
from typing import Any, ClassVar

import astropy.time
import astropy.time.formats
import astropy.utils.exceptions
import numpy
import yaml

# As of astropy 4.2, the erfa interface is shipped independently and
# ErfaWarning is no longer an AstropyWarning
try:
    import erfa
except ImportError:
    erfa = None

from lsst.utils.classes import Singleton

_LOG = logging.getLogger(__name__)


class _FastTimeUnixTai(astropy.time.formats.TimeUnixTai):
    """Special astropy time format that skips some checks of the parameters.
    This format is only used internally by TimeConverter so we can trust that
    it passes correct values to astropy Time class.
    """

    # number of seconds in a day
    _SEC_PER_DAY: ClassVar[int] = 24 * 3600

    # Name of this format, it is registered in astropy formats registry.
    name = "unix_tai_fast"

    def _check_val_type(self, val1: Any, val2: Any) -> tuple:
        # We trust everything that is passed to us.
        return val1, val2

    def set_jds(self, val1: numpy.ndarray, val2: numpy.ndarray | None) -> None:
        # Epoch time format is TimeISO with scalar jd1/jd2 arrays, convert them
        # to floats to speed things up.
        epoch = self._epoch._time
        jd1, jd2 = float(epoch._jd1), float(epoch._jd2)

        assert val1.ndim == 0, "Expect scalar"
        whole_days, seconds = divmod(float(val1), self._SEC_PER_DAY)
        if val2 is not None:
            assert val2.ndim == 0, "Expect scalar"
            seconds += float(val2)

        jd1 += whole_days
        jd2 += seconds / self._SEC_PER_DAY
        while jd2 > 0.5:
            jd2 -= 1.0
            jd1 += 1.0

        self._jd1, self._jd2 = numpy.array(jd1), numpy.array(jd2)


class TimeConverter(metaclass=Singleton):
    """A singleton for mapping TAI times to integer nanoseconds.

    This class allows some time calculations to be deferred until first use,
    rather than forcing them to happen at module import time.
    """

    def __init__(self) -> None:
        # EPOCH is used to convert from nanoseconds; its precision is used by
        # all timestamps returned by nsec_to_astropy, and we set it to 1
        # microsecond.
        self.epoch = astropy.time.Time("1970-01-01 00:00:00", format="iso", scale="tai", precision=6)
        self.max_time = astropy.time.Time("2100-01-01 00:00:00", format="iso", scale="tai")
        self.min_nsec = 0
        self.max_nsec = self.astropy_to_nsec(self.max_time)

    def astropy_to_nsec(self, astropy_time: astropy.time.Time) -> int:
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

        Notes
        -----
        Only the limited range of input times is supported by this method as it
        is defined useful in the context of Butler and Registry. If input time
        is earlier `min_time` then this method returns `min_nsec`. If input
        time comes after `max_time` then it returns `max_nsec`.
        """
        # sometimes comparison produces warnings if input value is in UTC
        # scale, transform it to TAI before doing anything but also trap
        # warnings in case we are dealing with simulated data from the future
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=astropy.utils.exceptions.AstropyWarning)
            if erfa is not None:
                warnings.simplefilter("ignore", category=erfa.ErfaWarning)
            value = astropy_time.tai
        # anything before epoch or after max_time is truncated
        if value < self.epoch:
            _LOG.warning(
                "'%s' is earlier than epoch time '%s', epoch time will be used instead",
                astropy_time,
                self.epoch,
            )
            value = self.epoch
        elif value > self.max_time:
            _LOG.warning(
                "'%s' is later than max. time '%s', max. time time will be used instead", value, self.max_time
            )
            value = self.max_time

        delta = value - self.epoch
        # Special care needed to preserve nanosecond precision.
        # Usually jd1 has no fractional part but just in case.
        # Can use "internal" ._time.jd1 interface because we know that both
        # are TAI. This is a few percent faster than using .jd1.
        jd1, extra_jd2 = divmod(delta._time.jd1, 1)
        value = int(jd1) * self._NSEC_PER_DAY + int(round((delta._time.jd2 + extra_jd2) * self._NSEC_PER_DAY))
        return value

    def nsec_to_astropy(self, time_nsec: int) -> astropy.time.Time:
        """Convert nanoseconds since epoch to astropy time.

        Parameters
        ----------
        time_nsec : `int`
            Nanoseconds since epoch.

        Returns
        -------
        astropy_time : `astropy.time.Time`
            Time to be converted.

        Notes
        -----
        Usually the input time for this method is the number returned from
        `astropy_to_nsec` which has a limited range. This method does not check
        that the number falls in the supported range and can produce output
        time that is outside of that range.
        """
        jd1, jd2 = divmod(time_nsec, 1_000_000_000)
        time = astropy.time.Time(
            float(jd1), jd2 / 1_000_000_000, format="unix_tai_fast", scale="tai", precision=6
        )
        # Force the format to be something more obvious to external users.
        # There is a small overhead doing this but it does avoid confusion.
        time.format = "jd"
        return time

    def times_equal(
        self, time1: astropy.time.Time, time2: astropy.time.Time, precision_nsec: float = 1.0
    ) -> bool:
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
        # Hide any warnings from this conversion since they are not relevant
        # to the equality
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=astropy.utils.exceptions.AstropyWarning)
            if erfa is not None:
                warnings.simplefilter("ignore", category=erfa.ErfaWarning)
            time1 = time1.tai
            time2 = time2.tai
        delta = (time2.jd1 - time1.jd1) + (time2.jd2 - time1.jd2)
        delta *= self._NSEC_PER_DAY
        return abs(delta) < precision_nsec

    # number of nanoseconds in a day
    _NSEC_PER_DAY: ClassVar[int] = 1_000_000_000 * 24 * 3600

    epoch: astropy.time.Time
    """Epoch for calculating time delta, this is the minimum time that can be
    stored in the database.
    """

    max_time: astropy.time.Time
    """Maximum time value that the converter can handle (`astropy.time.Time`).

    Assuming 64-bit integer field we can actually store higher values but we
    intentionally limit it to arbitrary but reasonably high value. Note that
    this value will be stored in registry database for eternity, so it should
    not be changed without proper consideration.
    """

    min_nsec: int
    """Minimum value returned by `astropy_to_nsec`, corresponding to
    `epoch` (`int`).
    """

    max_nsec: int
    """Maximum value returned by `astropy_to_nsec`, corresponding to
    `max_time` (`int`).
    """


class _AstropyTimeToYAML:
    """Handle conversion of astropy Time to/from YAML representation.

    This class defines methods that convert astropy Time instances to or from
    YAML representation. On output it converts time to string ISO format in
    TAI scale with maximum precision defining special YAML tag for it. On
    input it does inverse transformation. The methods need to be registered
    with YAML dumper and loader classes.

    Notes
    -----
    Python ``yaml`` module defines special helper base class ``YAMLObject``
    that provides similar functionality but its use is complicated by the need
    to convert ``Time`` instances to instances of ``YAMLObject`` sub-class
    before saving them to YAML. This class avoids this intermediate step but
    it requires separate regisration step.
    """

    yaml_tag = "!butler_time/tai/iso"  # YAML tag name for Time class

    @classmethod
    def to_yaml(cls, dumper: yaml.Dumper, data: astropy.time.Time) -> Any:
        """Convert astropy Time object into YAML format.

        Parameters
        ----------
        dumper : `yaml.Dumper`
            YAML dumper instance.
        data : `astropy.time.Time`
            Data to be converted.
        """
        if data is not None:
            # we store time in ISO format but we need full nanosecond
            # precision so we have to construct intermediate instance to make
            # sure its precision is set correctly.
            data = astropy.time.Time(data.tai, precision=9)
            data = data.to_value("iso")
        return dumper.represent_scalar(cls.yaml_tag, data)

    @classmethod
    def from_yaml(cls, loader: yaml.SafeLoader, node: yaml.ScalarNode) -> astropy.time.Time:
        """Convert YAML node into astropy time.

        Parameters
        ----------
        loader : `yaml.SafeLoader`
            Instance of YAML loader class.
        node : `yaml.ScalarNode`
            YAML node.

        Returns
        -------
        time : `astropy.time.Time`
            Time instance, can be ``None``.
        """
        if node.value is not None:
            return astropy.time.Time(node.value, format="iso", scale="tai")


# Register Time -> YAML conversion method with Dumper class
yaml.Dumper.add_representer(astropy.time.Time, _AstropyTimeToYAML.to_yaml)

# Register YAML -> Time conversion method with Loader, for our use case we
# only need SafeLoader.
yaml.SafeLoader.add_constructor(_AstropyTimeToYAML.yaml_tag, _AstropyTimeToYAML.from_yaml)

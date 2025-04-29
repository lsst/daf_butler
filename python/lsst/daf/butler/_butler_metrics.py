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

from collections.abc import Callable, Iterator
from contextlib import contextmanager

from pydantic import BaseModel

from lsst.utils.logging import LsstLoggers
from lsst.utils.timer import time_this


class ButlerMetrics(BaseModel):
    """Metrics collected during Butler operations."""

    time_in_put: float = 0.0
    """Wall-clock time, in seconds, spent in put()."""

    time_in_get: float = 0.0
    """Wall-clock time, in seconds, spent in get()."""

    n_get: int = 0
    """Number of datasets retrieved with get()."""

    n_put: int = 0
    """Number of datasets stored with put()."""

    def reset(self) -> None:
        """Reset all metrics."""
        self.time_in_put = 0.0
        self.time_in_get = 0.0
        self.n_get = 0
        self.n_put = 0

    def increment_get(self, duration: float) -> None:
        """Increment time for get().

        Parameters
        ----------
        duration : `float`
            Duration to add to the get() statistics.
        """
        self.time_in_get += duration
        self.n_get += 1

    def increment_put(self, duration: float) -> None:
        """Increment time for put().

        Parameters
        ----------
        duration : `float`
            Duration to add to the put() statistics.
        """
        self.time_in_put += duration
        self.n_put += 1

    @contextmanager
    def _timer(
        self, handler: Callable[[float], None], log: LsstLoggers | None = None, msg: str | None = None
    ) -> Iterator[None]:
        with time_this(log=log, msg=msg) as timer:
            yield
        handler(timer.duration)

    @contextmanager
    def instrument_get(self, log: LsstLoggers | None = None, msg: str | None = None) -> Iterator[None]:
        """Run code and increment get statistics.

        Parameters
        ----------
        log : `logging.Logger` or `None`
            Logger to use for any timing information.
        msg : `str` or `None`
            Any message to be included in log output.
        """
        with self._timer(self.increment_get, log=log, msg=msg):
            yield

    @contextmanager
    def instrument_put(self, log: LsstLoggers | None = None, msg: str | None = None) -> Iterator[None]:
        """Run code and increment put statistics.

        Parameters
        ----------
        log : `logging.Logger` or `None`
            Logger to use for any timing information.
        msg : `str` or `None`
            Any message to be included in log output.
        """
        with self._timer(self.increment_put, log=log, msg=msg):
            yield

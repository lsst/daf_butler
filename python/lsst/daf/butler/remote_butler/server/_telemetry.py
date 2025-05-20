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

import os
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from typing import Any, Protocol

try:
    import sentry_sdk

    _SENTRY_AVAILABLE = True
except ImportError:
    _SENTRY_AVAILABLE = False


class TelemetryContext(Protocol):
    """Interface for adding information to trace telemetry."""

    def span(self, name: str) -> AbstractContextManager[None]: ...


class NullTelemetryContext(TelemetryContext):
    """No-op implementation of telemetry used when no telemetry provider is
    configured.
    """

    @contextmanager
    def span(self, name: str) -> Iterator[None]:
        yield


class SentryTelemetryContext(TelemetryContext):
    """Implementation of telemetry using Sentry."""

    @contextmanager
    def span(self, name: str) -> Iterator[None]:
        with sentry_sdk.start_span(name=name):
            yield


_telemetry_context: TelemetryContext = NullTelemetryContext()


def enable_telemetry() -> None:
    """Turn on upload of trace telemetry to Sentry, to allow performance
    debugging of deployed server.
    """
    if not _SENTRY_AVAILABLE:
        return

    # Configuration will be pulled from SENTRY_* environment variables
    # (see https://docs.sentry.io/platforms/python/configuration/options/).
    # If SENTRY_DSN is not present, telemetry is disabled.
    sentry_sdk.init(traces_sampler=_decide_whether_to_sample_trace)

    global _telemetry_context
    _telemetry_context = SentryTelemetryContext()


def get_telemetry_context() -> TelemetryContext:
    """Return an object that can be used to add information to the trace
    telemetry.

    Returns
    -------
    telemetry_context : `TelemetryContext`
        Object that can be used to add information to the trace telemetry.
    """
    return _telemetry_context


def _decide_whether_to_sample_trace(context: dict[str, Any]) -> float:
    asgi_scope = context.get("asgi_scope")
    if asgi_scope is not None:
        # Do not log health check endpoint.
        if asgi_scope.get("path") == "/":
            return 0

    sampling_rate = float(os.getenv("BUTLER_TRACE_SAMPLING_RATE", "0.02"))
    return sampling_rate

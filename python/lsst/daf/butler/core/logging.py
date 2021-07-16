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

__all__ = ("VERBOSE", "ButlerMDC")

import logging

VERBOSE = (logging.INFO + logging.DEBUG) // 2
"""Verbose log level"""

logging.addLevelName(VERBOSE, "VERBOSE")


class MDCDict(dict):
    """Dictionary for MDC data.

    This is internal class used for better formatting of MDC in Python logging
    output. It behaves like `defaultdict(str)` but overrides ``__str__`` and
    ``__repr__`` method to produce output better suited for logging records.
    """

    def __getitem__(self, name: str) -> str:
        """Return value for a given key or empty string for missing key.
        """
        return self.get(name, "")

    def __str__(self) -> str:
        """Return string representation, strings are interpolated without
        quotes.
        """
        items = (f"{k}={self[k]}" for k in sorted(self))
        return "{" + ", ".join(items) + "}"

    def __repr__(self) -> str:
        return str(self)


class ButlerMDC:
    """Handle setting and unsetting of global MDC records.

    The Mapped Diagnostic Context (MDC) can be used to set context
    for log messages.

    Currently there is one global MDC dict. Per-thread MDC is not
    yet supported.
    """

    _MDC = MDCDict()

    @classmethod
    def MDC(cls, key: str, value: str) -> str:
        """Set MDC for this key to the supplied value.

        Parameters
        ----------
        key : `str`
            Key to modify.
        value : `str`
            New value to use.

        Returns
        -------
        old : `str`
            The previous value for this key.
        """
        old_value = cls._MDC[key]
        cls._MDC[key] = value
        return old_value

    @classmethod
    def MDCRemove(cls, key: str) -> None:
        """Clear the MDC value associated with this key.

        Can be called even if the key is not known to MDC.
        """
        cls._MDC.pop(key, None)

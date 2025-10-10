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

__all__ = (
    "butler_server_import_error",
    "butler_server_is_available",
)

butler_server_is_available = True
"""`True` if all dependencies required to use Butler server and RemoteButler
are installed.
"""

butler_server_import_error = ""
"""String containing a human-readable error message explaining why the server
is not available, if ``butler_server_is_available`` is `False`.
"""

try:
    # Dependencies required by Butler server and RemoteButler, but not
    # available in LSST Pipelines Stack.
    import fastapi  # noqa: F401
    import httpx  # noqa: F401
    import safir  # noqa: F401
except ImportError as e:
    butler_server_is_available = False
    butler_server_import_error = f"Server libraries could not be loaded: {str(e)}"

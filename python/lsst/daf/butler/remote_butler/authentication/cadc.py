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

from .interface import RemoteButlerAuthenticationProvider


class CadcAuthenticationProvider(RemoteButlerAuthenticationProvider):
    """Provide HTTP headers required for authenticating the user at the
    Canadian Astronomy Data Centre.
    """

    # NOTE -- This object needs to be pickleable. It will sometimes be
    # serialized and transferred to another process to execute file transfers.

    def __init__(self) -> None:
        # TODO: Load authentication information somehow
        pass

    def get_server_headers(self) -> dict[str, str]:
        # TODO: I think you mentioned that you might not require
        # authentication for the Butler server REST API initially --
        # if so, you can leave this blank.
        return {}

    def get_datastore_headers(self) -> dict[str, str]:
        # TODO: Supply the headers needed to access the Storage Inventory
        # system.
        return {"Authorization": "Bearer stub"}

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


import re
from typing import Any

from fastapi import FastAPI, HTTPException, Request, Response


def add_auth_header_check_middleware(app: FastAPI) -> None:
    """Add a middleware to a FastAPI app to check that Gafaelfawr
    authentication headers are present.

    This is only suitable for testing -- in a real deployment,
    GafaelfawrIngress will handle these headers and convert them to a different
    format.

    Parameters
    ----------
    app : `FastAPI`
        The app the middleware will be added to.
    """

    @app.middleware("http")
    async def check_auth_headers(request: Request, call_next: Any) -> Response:
        if _is_authenticated_endpoint(request.url.path):
            header = request.headers.get("authorization")
            if header is None:
                raise HTTPException(status_code=401, detail="Authorization header is missing")
            if not re.match(r"^Bearer \S+", header):
                raise HTTPException(
                    status_code=401, detail=f"Authorization header not in expected format: {header}"
                )

        return await call_next(request)


def _is_authenticated_endpoint(path: str) -> bool:
    """Return True if the specified path requires authentication in the real
    server deployment.
    """
    if path == "/":
        return False
    if path.endswith("/butler.yaml"):
        return False
    if path.endswith("/butler.json"):
        return False
    if path.startswith("/api/butler/configs"):
        return False

    return True

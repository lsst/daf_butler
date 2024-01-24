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

__all__ = ("create_app",)

from typing import Awaitable, Callable

import safir.dependencies.logger
from fastapi import FastAPI, Request, Response
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from lsst.daf.butler import MissingDatasetTypeError
from safir.logging import configure_logging, configure_uvicorn_logging

from ..server_models import CLIENT_REQUEST_ID_HEADER_NAME
from .handlers._external import external_router
from .handlers._internal import internal_router

configure_logging(name="lsst.daf.butler.remote_butler.server")
configure_uvicorn_logging()


def create_app() -> FastAPI:
    """Create a Butler server FastAPI application."""
    app = FastAPI()
    app.add_middleware(GZipMiddleware, minimum_size=1000)

    # A single instance of the server can serve data from multiple Butler
    # repositories.  This 'repository' path placeholder is consumed by
    # factory_dependency().
    repository_placeholder = "{repository}"
    default_api_path = "/api/butler"
    app.include_router(external_router, prefix=f"{default_api_path}/repo/{repository_placeholder}")
    app.include_router(internal_router)

    # Any time an exception is returned by a handler, add a log message that
    # includes the username and request ID from the client.  This will make it
    # easier to track down user-reported issues in the logs.
    #
    # This middleware is higher in the middleware stack than FastAPI's
    # HttpException and ValidationError handling middleware, so it only
    # catches unhandled errors that would result in a 500 Internal
    # Server Error.
    @app.middleware("http")
    async def _log_exceptions_middleware(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        try:
            return await call_next(request)
        except Exception as e:
            logger = await safir.dependencies.logger.logger_dependency(request)
            await logger.aerror(
                "Exception",
                exc_info=e,
                clientRequestId=request.headers.get(CLIENT_REQUEST_ID_HEADER_NAME, "(unknown)"),
                user=request.headers.get("X-Auth-Request-User", "(unknown)"),
            )
            raise

    @app.exception_handler(MissingDatasetTypeError)
    def missing_dataset_type_exception_handler(
        request: Request, exc: MissingDatasetTypeError
    ) -> JSONResponse:
        # Remove the double quotes around the string form. These confuse
        # the JSON serialization when single quotes are in the message.
        message = str(exc).strip('"')
        return JSONResponse(
            status_code=404,
            content={"detail": message, "exception": "MissingDatasetTypeError"},
        )

    return app

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

__all__ = ("app",)

import logging

from fastapi import FastAPI, Request
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from lsst.daf.butler import Butler, DataCoordinate, MissingDatasetTypeError, SerializedDataCoordinate
from safir.metadata import Metadata, get_metadata

from .handlers._external import external_router

_DEFAULT_API_PATH = "/api/butler"

log = logging.getLogger(__name__)

app = FastAPI()
app.add_middleware(GZipMiddleware, minimum_size=1000)
app.include_router(external_router, prefix=_DEFAULT_API_PATH)


@app.exception_handler(MissingDatasetTypeError)
def missing_dataset_type_exception_handler(request: Request, exc: MissingDatasetTypeError) -> JSONResponse:
    # Remove the double quotes around the string form. These confuse
    # the JSON serialization when single quotes are in the message.
    message = str(exc).strip('"')
    return JSONResponse(
        status_code=404,
        content={"detail": message, "exception": "MissingDatasetTypeError"},
    )


def unpack_dataId(butler: Butler, data_id: SerializedDataCoordinate | None) -> DataCoordinate | None:
    """Convert the serialized dataId back to full DataCoordinate.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The butler to use for registry and universe.
    data_id : `SerializedDataCoordinate` or `None`
        The serialized form.

    Returns
    -------
    dataId : `DataCoordinate` or `None`
        The DataId usable by registry.
    """
    if data_id is None:
        return None
    return DataCoordinate.from_simple(data_id, registry=butler.registry)


@app.get(
    "/",
    description=(
        "Return metadata about the running application. Can also be used as"
        " a health check. This route is not exposed outside the cluster and"
        " therefore cannot be used by external clients."
    ),
    include_in_schema=False,
    response_model=Metadata,
    response_model_exclude_none=True,
    summary="Application metadata",
)
async def get_index() -> Metadata:
    """GET ``/`` (the app's internal root).

    By convention, this endpoint returns only the application's metadata.
    """
    return get_metadata(package_name="lsst.daf.butler", application_name="butler")

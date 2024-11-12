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

from collections.abc import Iterator

import httpx
from pydantic import TypeAdapter

from .._dataset_ref import DatasetRef
from ..dimensions import DimensionUniverse
from ._errors import deserialize_butler_user_error
from .server_models import QueryExecuteResultData

_QueryResultTypeAdapter = TypeAdapter[QueryExecuteResultData](QueryExecuteResultData)


def read_query_results(response: httpx.Response) -> Iterator[QueryExecuteResultData]:
    """Read streaming query results from the server.

    Parameters
    ----------
    response : `httpx.Response`
        HTTPX response object from a request where ``stream=True`` was set.

    Yields
    ------
    pages : `QueryExecuteResultData`
        Pages of result data from the server, excluding out-of-band messages
        like keep-alives and errors.

    Raises
    ------
    ButlerUserError
        Any subclass of ButlerUserError might be raised, to propagate
        server-side exceptions to the client.
    """
    # There is one result page JSON object per line of the response.
    for line in response.iter_lines():
        result: QueryExecuteResultData = _QueryResultTypeAdapter.validate_json(line)
        if result.type == "keep-alive":
            # Server is still in the process of generating the response.
            _received_keep_alive()
        elif result.type == "error":
            # A server-side exception occurred part-way through generating
            # results.
            raise deserialize_butler_user_error(result.error)
        else:
            yield result


def convert_dataset_ref_results(
    result: QueryExecuteResultData, universe: DimensionUniverse
) -> list[DatasetRef]:  # numpydoc ignore=PR01
    """Convert a serialized page of dataset results to `DatasetRef`
    instances.
    """
    assert result.type == "dataset_ref"
    return [DatasetRef.from_simple(r, universe) for r in result.rows]


def _received_keep_alive() -> None:
    """Do nothing.  Gives a place for unit tests to hook in for testing
    keepalive behavior.
    """
    pass

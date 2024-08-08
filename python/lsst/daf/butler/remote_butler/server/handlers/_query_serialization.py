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

from collections.abc import Iterable, Iterator

from ...._exceptions import ButlerUserError
from ....queries.driver import (
    DataCoordinateResultPage,
    DatasetRefResultPage,
    DimensionRecordResultPage,
    GeneralResultPage,
    ResultPage,
    ResultSpec,
)
from ..._errors import serialize_butler_user_error
from ...server_models import (
    DataCoordinateResultModel,
    DatasetRefResultModel,
    DimensionRecordsResultModel,
    GeneralResultModel,
    QueryErrorResultModel,
    QueryExecuteResultData,
)


def serialize_query_pages(
    spec: ResultSpec, pages: Iterable[ResultPage]
) -> Iterator[str]:  # numpydoc ignore=PR01
    """Serialize result pages to pages of result data in JSON format. The
    output contains one page object per line, as newline-delimited JSON records
    in the "JSON Lines" format (https://jsonlines.org/).
    """
    try:
        for page in pages:
            yield _convert_query_page(spec, page).model_dump_json()
            yield "\n"
    except ButlerUserError as e:
        # If a user-facing error occurs, serialize it and send it to the
        # client.
        yield QueryErrorResultModel(error=serialize_butler_user_error(e)).model_dump_json()
        yield "\n"


def _convert_query_page(spec: ResultSpec, page: ResultPage) -> QueryExecuteResultData:
    """Convert pages of result data from the query system to a serializable
    format.

    Parameters
    ----------
    spec : `ResultSpec`
        Definition of the output format for the results.
    pages : `ResultPage`
        Raw page of data from the query driver.
    """
    match spec.result_type:
        case "dimension_record":
            assert isinstance(page, DimensionRecordResultPage)
            return DimensionRecordsResultModel(rows=[record.to_simple() for record in page.rows])
        case "data_coordinate":
            assert isinstance(page, DataCoordinateResultPage)
            return DataCoordinateResultModel(rows=[coordinate.to_simple() for coordinate in page.rows])
        case "dataset_ref":
            assert isinstance(page, DatasetRefResultPage)
            return DatasetRefResultModel(rows=[ref.to_simple() for ref in page.rows])
        case "general":
            assert isinstance(page, GeneralResultPage)
            return _convert_general_result(page)
        case _:
            raise NotImplementedError(f"Unhandled query result type {spec.result_type}")


def _convert_general_result(page: GeneralResultPage) -> GeneralResultModel:
    """Convert GeneralResultPage to a serializable model."""
    columns = page.spec.get_result_columns()
    serializers = [
        columns.get_column_spec(column.logical_table, column.field).serializer() for column in columns
    ]
    rows = [
        tuple(serializer.serialize(value) for value, serializer in zip(row, serializers)) for row in page.rows
    ]
    return GeneralResultModel(rows=rows)

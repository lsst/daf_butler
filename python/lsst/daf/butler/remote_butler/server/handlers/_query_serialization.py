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

from ....queries.driver import (
    DataCoordinateResultPage,
    DatasetRefResultPage,
    DimensionRecordResultPage,
    ResultPage,
    ResultSpec,
)
from ...server_models import (
    DataCoordinateResultModel,
    DatasetRefResultModel,
    DimensionRecordsResultModel,
    QueryExecuteResultData,
)


def convert_query_pages(spec: ResultSpec, pages: Iterator[ResultPage]) -> QueryExecuteResultData:
    """Convert pages of result data from the query system to a serializable
    format.

    Parameters
    ----------
    spec : `ResultSpec`
        Definition of the output format for the results.
    pages : `~collections.abc.Iterator` [ `ResultPage` ]
        Raw pages of data from the query driver.
    """
    match spec.result_type:
        case "dimension_record":
            return _convert_dimension_record_pages(pages)
        case "data_coordinate":
            return _convert_data_coordinate_pages(pages)
        case "dataset_ref":
            return _convert_dataset_ref_pages(pages)
        case _:
            raise NotImplementedError(f"Unhandled query result type {spec.result_type}")


def _convert_dimension_record_pages(pages: Iterator[ResultPage]) -> DimensionRecordsResultModel:
    response = DimensionRecordsResultModel(rows=[])
    for page in pages:
        assert isinstance(page, DimensionRecordResultPage)
        response.rows.extend(record.to_simple() for record in page.rows)
    return response


def _convert_data_coordinate_pages(pages: Iterator[ResultPage]) -> DataCoordinateResultModel:
    response = DataCoordinateResultModel(rows=[])
    for page in pages:
        assert isinstance(page, DataCoordinateResultPage)
        response.rows.extend(coordinate.to_simple() for coordinate in page.rows)
    return response


def _convert_dataset_ref_pages(pages: Iterator[ResultPage]) -> DatasetRefResultModel:
    response = DatasetRefResultModel(rows=[])
    for page in pages:
        assert isinstance(page, DatasetRefResultPage)
        response.rows.extend(ref.to_simple() for ref in page.rows)
    return response

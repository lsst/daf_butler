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

from ...dimensions import DimensionElement, DimensionRecord
from ...queries import DimensionRecordQueryResults, Query
from ...registry.queries import DimensionRecordQueryResults as LegacyDimensionRecordQueryResults
from ._query_common import CommonQueryArguments, LegacyQueryResultsMixin, QueryFactory


class QueryDriverDimensionRecordQueryResults(
    LegacyQueryResultsMixin[DimensionRecordQueryResults],
    LegacyDimensionRecordQueryResults,
):
    """Implementation of the legacy ``DimensionRecordQueryResults`` interface
    using the new query system.

    Parameters
    ----------
    query_factory : `QueryFactory`
        Function that can be called to access the new query system.
    element : `DimensionElement`
        The dimension element to obtain records for.
    args : `CommonQueryArguments`
        User-facing arguments forwarded from
        ``registry.queryDimensionRecords``.
    """

    def __init__(
        self, query_factory: QueryFactory, element: DimensionElement, args: CommonQueryArguments
    ) -> None:
        LegacyQueryResultsMixin.__init__(self, query_factory, args)
        LegacyDimensionRecordQueryResults.__init__(self)
        self._element = element

    @property
    def element(self) -> DimensionElement:
        return self._element

    def __iter__(self) -> Iterator[DimensionRecord]:
        with self._build_query() as result:
            # We have to eagerly fetch the results to prevent
            # leaking the resources associated with QueryDriver.
            records = list(result)
        return iter(records)

    def run(self) -> LegacyDimensionRecordQueryResults:
        return self

    def _build_result(self, query: Query) -> DimensionRecordQueryResults:
        return query.dimension_records(self._element.name)

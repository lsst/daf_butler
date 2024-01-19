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

__all__ = ("convert_dimension_record_results",)

from collections.abc import Iterable

import sqlalchemy

from ..dimensions import DimensionRecordSet
from ..queries.driver import DimensionRecordResultPage, PageKey
from ..queries.result_specs import DimensionRecordResultSpec


def convert_dimension_record_results(
    raw_rows: Iterable[sqlalchemy.Row], spec: DimensionRecordResultSpec, next_key: PageKey | None
) -> DimensionRecordResultPage:
    record_set = DimensionRecordSet(spec.element)
    columns = spec.get_result_columns()
    column_mapping = [
        (field, columns.get_qualified_name(spec.element.name, field)) for field in spec.element.schema.names
    ]
    record_cls = spec.element.RecordClass
    if not spec.element.temporal:
        for raw_row in raw_rows:
            record_set.add(record_cls(**{k: raw_row._mapping[v] for k, v in column_mapping}))
    return DimensionRecordResultPage(spec=spec, next_key=next_key, rows=record_set)

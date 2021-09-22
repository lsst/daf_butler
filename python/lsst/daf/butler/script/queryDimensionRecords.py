# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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

from astropy.table import Table

from .. import Butler
from ..core import Timespan


def queryDimensionRecords(repo, element, datasets, collections, where, no_check):
    # Docstring for supported parameters is the same as
    # Registry.queryDimensionRecords except for ``no_check``, which is the
    # inverse of ``check``.

    butler = Butler(repo)

    records = list(butler.registry.queryDimensionRecords(element,
                                                         datasets=datasets,
                                                         collections=collections,
                                                         where=where,
                                                         check=not no_check))
    if not records:
        return None

    records.sort(key=lambda r: r.dataId)  # use the dataId to sort the rows
    keys = records[0].fields.names  # order the columns the same as the record's `field.names`

    def conform(v):
        if isinstance(v, Timespan):
            v = (v.begin, v.end)
        return v

    return Table([[conform(getattr(record, key, None)) for record in records] for key in keys], names=keys)

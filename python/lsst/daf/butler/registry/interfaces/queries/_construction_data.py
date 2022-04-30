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

from __future__ import annotations

__all__ = (
    "QueryConstructionDataRequest",
    "QueryConstructionDataResult",
)

import dataclasses
from typing import Any

from ....core import DatasetType
from ....core.named import NamedKeyDict, NamedValueSet
from ...summaries import CollectionSummary
from ...wildcards import CollectionWildcard, DatasetTypeWildcard
from .._collections import CollectionRecord


@dataclasses.dataclass
class QueryConstructionDataRequest:
    dataset_types: DatasetTypeWildcard
    collections: CollectionWildcard

    def __init__(self, dataset_types: Any = (), collections: Any = ()):
        self.dataset_types = DatasetTypeWildcard.from_expression(dataset_types)
        self.collections = CollectionWildcard.fromExpression(collections)

    def union(*args: QueryConstructionDataRequest) -> QueryConstructionDataRequest:
        return QueryConstructionDataRequest(
            dataset_types=DatasetTypeWildcard.union(*[arg.dataset_types for arg in args]),
            collections=CollectionWildcard.union(*[arg.collections for arg in args]),
        )


@dataclasses.dataclass
class QueryConstructionDataResult:
    dataset_types: NamedValueSet[DatasetType] = dataclasses.field(default_factory=NamedValueSet)
    collections: NamedKeyDict[CollectionRecord, CollectionSummary] = dataclasses.field(
        default_factory=NamedKeyDict
    )

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

__all__ = ["Quantum"]

from typing import List

from ..core.datasets import CheckedDatasetHandle, DatasetType
from ..core.dimensions import ExpandedDataCoordinate
from ..core.timespan import Timespan
from ..core.utils import NamedKeyDict


class Quantum:
    taskName: str
    taskClass: type
    run: str
    host: str
    timespan: Timespan
    dataId: ExpandedDataCoordinate
    initInputs: NamedKeyDict[DatasetType, CheckedDatasetHandle]
    predictedInputs: NamedKeyDict[DatasetType, List[CheckedDatasetHandle]]
    actualInputs: NamedKeyDict[DatasetType, List[CheckedDatasetHandle]]
    outputs: NamedKeyDict[DatasetType, List[CheckedDatasetHandle]]

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

__all__ = (
    "GeneralResultSpec",
    "GeneralResultPage",
)

from typing import Any, Literal

import pydantic

from .driver import PageKey
from .tree import ColumnReference


class GeneralResultSpec(pydantic.BaseModel):
    """Specification for a query that yields a table with
    an explicit list of columns.
    """

    result_type: Literal["general"] = "general"
    columns: tuple[ColumnReference, ...]


class GeneralResultPage(pydantic.BaseModel):
    """A single page of results from a general query."""

    spec: GeneralResultSpec
    next_key: PageKey | None

    # Raw tabular data, with columns in the same order as spec.columns.
    rows: list[tuple[Any, ...]]

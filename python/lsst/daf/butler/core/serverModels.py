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
    "QueryDatasetsModel",
    "ExpressionQueryParameter",
)

"""Models used for client/server communication."""

from typing import (
    Any,
    ClassVar,
    Dict,
    List,
    Mapping,
    Optional,
    Union,
)

import re

from pydantic import BaseModel

from .dimensions import SerializedDataCoordinate
from .utils import iterable, globToRegex

# DataId and kwargs can only take limited types
DataIdValues = Union[int, str]
SimpleDataId = Mapping[str, DataIdValues]


class ExpressionQueryParameter(BaseModel):
    """Represents a specification for an expression query."""

    _allow_ellipsis: ClassVar[bool] = True
    """Control whether expression can match everything."""

    regex: Optional[List[str]] = None
    """List of regular expression strings."""

    glob: Optional[List[str]] = None
    """List of dataset type names or globs."""

    def expression(self) -> Any:
        """Combine regex and glob lists into single expression."""
        if self.glob is None and self.regex is None:
            if self._allow_ellipsis:
                return ...
            # Rather than matching all, interpret this as no expression
            # at all.
            return None

        expression: List[Union[str, re.Pattern]] = []
        if self.regex is not None:
            for r in self.regex:
                expression.append(re.compile(r))
        if self.glob is not None:
            regexes = globToRegex(self.glob)
            if isinstance(regexes, list):
                expression.extend(regexes)
            else:
                # This avoids mypy needing to import Ellipsis type
                if self._allow_ellipsis:
                    return ...
                raise ValueError("Expression matches everything but that is not allowed.")
        return expression

    @classmethod
    def from_expression(cls, expression: Any) -> ExpressionQueryParameter:
        """Convert a standard dataset type expression to wire form."""
        if expression is ...:
            return cls()

        expressions = iterable(expression)
        params: Dict[str, List[str]] = {"glob": [], "regex": []}
        for expression in expressions:
            if expression is ...:
                # This matches everything
                return cls()

            if isinstance(expression, re.Pattern):
                params["regex"].append(expression.pattern)
            elif isinstance(expression, str):
                params["glob"].append(expression)
            elif hasattr(expression, "name"):
                params["glob"].append(expression.name)
            else:
                raise ValueError(f"Unrecognized type given to expression: {expression!r}")

        # Clean out empty dicts.
        for k in list(params):
            if not params[k]:
                del params[k]

        return cls(**params)


class DatasetsQueryParameter(ExpressionQueryParameter):
    """Represents a specification for a dataset expression query.

    This differs from the standard expression query in that an empty
    expression will return `None` rather than ``...``.
    """

    _allow_ellipsis: ClassVar[bool] = False


class QueryDatasetsModel(BaseModel):
    """Information needed for a registry dataset query."""

    datasetType: ExpressionQueryParameter
    collections: Optional[ExpressionQueryParameter] = None
    dimensions: Optional[List[str]] = None
    dataId: Optional[SerializedDataCoordinate] = None
    where: Optional[str] = None
    findFirst: bool = False
    components: Optional[bool] = None
    bind: Optional[SimpleDataId] = None
    check: bool = True
    keyword_args: Optional[SimpleDataId] = None  # mypy refuses to allow kwargs in model


class QueryDimensionRecordsModel(BaseModel):
    """Information needed to query the dimension records."""

    dataId: Optional[SerializedDataCoordinate] = None
    datasets: Optional[DatasetsQueryParameter] = None
    collections: Optional[ExpressionQueryParameter] = None
    where: Optional[str] = None
    components: Optional[bool] = None
    bind: Optional[SimpleDataId] = None
    check: bool = True
    keyword_args: Optional[SimpleDataId] = None  # mypy refuses to allow kwargs in model

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
    Dict,
    List,
    Mapping,
    Optional,
    Union,
)

import re

from pydantic import BaseModel

from .utils import iterable, globToRegex

# DataId and kwargs can only take limited types
DataIdValues = Union[str, int, float]
DataId = Mapping[str, DataIdValues]


class ExpressionQueryParameter(BaseModel):
    """Represents a specification for an expression query."""

    regex: Optional[List[str]] = None
    """List of regular expression strings."""

    glob: Optional[List[str]] = None
    """List of dataset type names or globs."""

    def expression(self) -> Any:
        """Combine regex and glob lists into single expression."""
        if self.glob is None and self.regex is None:
            return ...

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
                return ...
        return expression

    @classmethod
    def from_expression(cls, expression: Any) -> ExpressionQueryParameter:
        """Convert a standard dataset type expression to wire form."""
        if expression is ...:
            return cls()

        expressions = iterable(expression)
        params: Dict[str, List[str]] = {}
        for expression in expressions:
            if expression is ...:
                # This matches everything
                return cls()

            if isinstance(expression, re.Pattern):
                if (k := "regex") not in params:
                    params[k] = []
                params["regex"].append(expression.pattern)
            else:
                if (k := "glob") not in params:
                    params[k] = []
                params["glob"].append(expression)
        return cls(**params)


class QueryDatasetsModel(BaseModel):
    """Information needed for a registry dataset query."""

    datasetType: ExpressionQueryParameter
    collections: Optional[ExpressionQueryParameter] = None
    dimensions: Optional[List[str]] = None
    dataId: Optional[DataId] = None
    where: Optional[str] = None
    findFirst: bool = False
    components: Optional[bool] = None
    bind: Optional[DataId] = None
    check: bool = True
    keyword_args: Optional[DataId] = None  # mypy refuses to allow kwargs in model

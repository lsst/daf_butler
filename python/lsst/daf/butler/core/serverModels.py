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

"""Models used for client/server communication."""

__all__ = (
    "QueryDatasetsModel",
    "QueryDataIdsModel",
    "QueryDimensionRecordsModel",
    "ExpressionQueryParameter",
    "DatasetsQueryParameter",
)

import re
from collections.abc import Mapping
from typing import Any, ClassVar

import pydantic
from lsst.daf.butler._compat import PYDANTIC_V2, _BaseModelCompat
from lsst.utils.iteration import ensure_iterable
from pydantic import Field

from .dimensions import DataIdValue, SerializedDataCoordinate
from .utils import globToRegex

# Simple scalar python types.
ScalarType = int | bool | float | str

# Bind parameters can have any scalar type.
BindType = dict[str, ScalarType]

# For serialization purposes a data ID key must be a str.
SimpleDataId = Mapping[str, DataIdValue]


# While supporting pydantic v1 and v2 keep this outside the model.
_expression_query_schema_extra = {
    "examples": [
        {
            "regex": ["^cal.*"],
            "glob": ["cal*", "raw"],
        }
    ]
}


class ExpressionQueryParameter(_BaseModelCompat):
    """Represents a specification for an expression query.

    Generally used for collection or dataset type expressions.  This
    implementation returns ``...`` by default.
    """

    _allow_ellipsis: ClassVar[bool] = True
    """Control whether expression can match everything."""

    regex: list[str] | None = Field(
        None,
        title="List of regular expression strings.",
        examples=["^cal.*"],
    )

    glob: list[str] | None = Field(
        None,
        title="List of globs or explicit strings to use in expression.",
        examples=["cal*"],
    )

    if PYDANTIC_V2:
        model_config = {
            "json_schema_extra": _expression_query_schema_extra,  # type: ignore[typeddict-item]
        }
    else:

        class Config:
            """Local configuration overrides for model."""

            schema_extra = _expression_query_schema_extra

    def expression(self) -> Any:
        """Combine regex and glob lists into single expression."""
        if self.glob is None and self.regex is None:
            if self._allow_ellipsis:
                return ...
            # Rather than matching all, interpret this as no expression
            # at all.
            return None

        expression: list[str | re.Pattern] = []
        if self.regex is not None:
            for r in self.regex:
                expression.append(re.compile(r))
        if self.glob is not None:
            regexes = globToRegex(self.glob)
            if isinstance(regexes, list):
                expression.extend(regexes)
            else:
                if self._allow_ellipsis:
                    return ...
                raise ValueError("Expression matches everything but that is not allowed.")
        return expression

    @classmethod
    def from_expression(cls, expression: Any) -> "ExpressionQueryParameter":
        """Convert a standard dataset type expression to wire form."""
        if expression is ...:
            return cls()

        expressions = ensure_iterable(expression)
        params: dict[str, list[str]] = {"glob": [], "regex": []}
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


# Shared field definitions
Where = Field(
    "",
    title="String expression similar to a SQL WHERE clause.",
    examples=["detector = 5 AND instrument = 'HSC'"],
)
Collections = Field(
    None,
    title="An expression that identifies the collections to search.",
)
Datasets = Field(
    None,
    title="An expression that identifies dataset types to search (must not match all datasets).",
)
OptionalDimensions = Field(
    None,
    title="Relevant dimensions to include.",
    examples=["detector", "physical_filter"],
)
Dimensions = Field(
    ...,
    title="Relevant dimensions to include.",
    examples=["detector", "physical_filter"],
)
DataId = Field(
    None,
    title="Data ID to constrain the query.",
)
FindFirst = Field(
    False,
    title="Control whether only first matching dataset ref or type is returned.",
)
Components = Field(
    None,
    title="Control how expressions apply to components.",
)
Bind = Field(
    None,
    title="Mapping to use to inject values into the WHERE parameter clause.",
)
Check = Field(
    True,
    title="Control whether to check the query for consistency.",
)
KeywordArgs = Field(
    None,
    title="Additional parameters to use when standardizing the supplied data ID.",
)


class QueryBaseModel(_BaseModelCompat):
    """Base model for all query models."""

    if PYDANTIC_V2:

        @pydantic.field_validator("keyword_args", check_fields=False)  # type: ignore[attr-defined]
        @classmethod
        def _check_keyword_args(cls, v: SimpleDataId) -> SimpleDataId | None:
            """Convert kwargs into None if empty.

            This retains the property at its default value and can therefore
            remove it from serialization.

            The validator will be ignored if the subclass does not have this
            property in its model.
            """
            if not v:
                return None
            return v

    else:

        @pydantic.validator("keyword_args", check_fields=False)
        def _check_keyword_args(cls, v, values) -> SimpleDataId | None:  # type: ignore # noqa: N805
            """Convert kwargs into None if empty.

            This retains the property at its default value and can therefore
            remove it from serialization.

            The validator will be ignored if the subclass does not have this
            property in its model.
            """
            if not v:
                return None
            return v

    def kwargs(self) -> SimpleDataId:
        """Return keyword args, converting None to a `dict`.

        Returns
        -------
        **kwargs
            The keword arguments stored in the model. `None` is converted
            to an empty dict. Returns empty dict if the ``keyword_args``
            property is not defined.
        """
        try:
            # mypy does not know about the except
            kwargs = self.keyword_args  # type: ignore
        except AttributeError:
            kwargs = {}
        if kwargs is None:
            return {}
        return kwargs


class QueryDatasetsModel(QueryBaseModel):
    """Information needed for a registry dataset query."""

    datasetType: ExpressionQueryParameter = Field(..., title="Dataset types to query. Can match all.")
    collections: ExpressionQueryParameter | None = Collections
    dimensions: list[str] | None = OptionalDimensions
    dataId: SerializedDataCoordinate | None = DataId
    where: str = Where
    findFirst: bool = FindFirst
    components: bool | None = Components
    bind: BindType | None = Bind
    check: bool = Check
    keyword_args: SimpleDataId | None = KeywordArgs  # mypy refuses to allow kwargs in model


class QueryDataIdsModel(QueryBaseModel):
    """Information needed to query data IDs."""

    dimensions: list[str] = Dimensions
    dataId: SerializedDataCoordinate | None = DataId
    datasets: DatasetsQueryParameter | None = Datasets
    collections: ExpressionQueryParameter | None = Collections
    where: str = Where
    components: bool | None = Components
    bind: BindType | None = Bind
    check: bool = Check
    keyword_args: SimpleDataId | None = KeywordArgs  # mypy refuses to allow kwargs in model


class QueryDimensionRecordsModel(QueryBaseModel):
    """Information needed to query the dimension records."""

    dataId: SerializedDataCoordinate | None = DataId
    datasets: DatasetsQueryParameter | None = Datasets
    collections: ExpressionQueryParameter | None = Collections
    where: str = Where
    components: bool | None = Components
    bind: SimpleDataId | None = Bind
    check: bool = Check
    keyword_args: SimpleDataId | None = KeywordArgs  # mypy refuses to allow kwargs in model

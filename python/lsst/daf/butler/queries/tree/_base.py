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
    "QueryTreeBase",
    "ColumnExpressionBase",
    "PredicateBase",
    "DatasetFieldName",
    "InvalidQueryTreeError",
)

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, ClassVar, Literal, TypeAlias, cast

import pydantic

if TYPE_CHECKING:
    from ...column_spec import ColumnType
    from ._column_set import ColumnSet
    from ._predicate import Predicate


DatasetFieldName: TypeAlias = Literal["dataset_id", "ingest_date", "run", "collection", "rank", "timespan"]


class InvalidQueryTreeError(RuntimeError):
    """Exception raised when a query tree is or would not be valid."""


class QueryTreeBase(pydantic.BaseModel):
    """Base class for all non-primitive types in a query tree."""

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid", strict=True)


class ColumnExpressionBase(QueryTreeBase, ABC):
    """Base class for objects that represent non-boolean column expressions in
    a query tree.

    This is a closed hierarchy whose concrete, `~typing.final` derived classes
    are members of the `ColumnExpression` union.  That union should generally
    be used in type annotations rather than the technically-open base class.
    """

    expression_type: str

    is_literal: ClassVar[bool] = False
    """Whether this expression wraps a literal Python value."""

    @property
    @abstractmethod
    def precedence(self) -> int:
        """Operator precedence for this operation.

        Lower values bind more tightly, so parentheses are needed when printing
        an expression where an operand has a higher value than the expression
        itself.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def column_type(self) -> ColumnType:
        """A string enumeration value representing the type of the column
        expression.
        """
        raise NotImplementedError()

    @abstractmethod
    def gather_required_columns(self, columns: ColumnSet) -> None:
        # TODO: docs
        raise NotImplementedError()


class ColumnLiteralBase(ColumnExpressionBase):
    """Base class for objects that represent literal values as column
    expressions in a query tree.

    This is a closed hierarchy whose concrete, `~typing.final` derived classes
    are members of the `ColumnLiteral` union.  That union should generally be
    used in type annotations rather than the technically-open base class.
    """

    is_literal: ClassVar[bool] = True
    """Whether this expression wraps a literal Python value."""

    @property
    def precedence(self) -> int:
        # Docstring inherited.
        return 0

    def gather_required_columns(self, columns: ColumnSet) -> None:
        # Docstring inherited.
        pass

    @property
    def column_type(self) -> ColumnType:
        # Docstring inherited.
        return cast(ColumnType, self.expression_type)


class PredicateBase(QueryTreeBase, ABC):
    """Base class for objects that represent boolean column expressions in a
    query tree.

    A `Predicate` tree is always in conjunctive normal form (ANDs of ORs of
    NOTs).  This is enforced by type annotations (and hence Pydantic
    validation) and the `logical_and`, `logical_or`, and `logical_not` factory
    methods.

    This is a closed hierarchy whose concrete, `~typing.final` derived classes
    are members of the `Predicate` union.  That union should generally be used
    in type annotations rather than the technically-open base class.
    """

    @property
    @abstractmethod
    def precedence(self) -> int:
        """Operator precedence for this operation.

        Lower values bind more tightly, so parentheses are needed when printing
        an expression where an operand has a higher value than the expression
        itself.
        """
        raise NotImplementedError()

    @property
    def column_type(self) -> Literal["bool"]:
        """A string enumeration value representing the type of the column
        expression.
        """
        return "bool"

    @abstractmethod
    def gather_required_columns(self, columns: ColumnSet) -> None:
        pass

    # The 'other' arguments of the methods below are annotated as Any because
    # MyPy doesn't correctly recognize subclass implementations that use
    # @overload, and the signature of this base class doesn't really matter,
    # since it's the union of all concrete implementations that's public;
    # the base class exists largely as a place to hang docstrings.

    @abstractmethod
    def logical_and(self, other: Any) -> Predicate:
        """Return the logical AND of this predicate and another.

        Parameters
        ----------
        other : `Predicate`
            Other operand.

        Returns
        -------
        result : `Predicate`
            A predicate presenting the logical AND.
        """
        raise NotImplementedError()

    @abstractmethod
    def logical_or(self, other: Any) -> Predicate:
        """Return the logical OR of this predicate and another.

        Parameters
        ----------
        other : `Predicate`
            Other operand.

        Returns
        -------
        result : `Predicate`
            A predicate presenting the logical OR.
        """
        raise NotImplementedError()

    @abstractmethod
    def logical_not(self) -> Predicate:
        """Return the logical NOTof this predicate.

        Returns
        -------
        result : `Predicate`
            A predicate presenting the logical OR.
        """
        raise NotImplementedError()

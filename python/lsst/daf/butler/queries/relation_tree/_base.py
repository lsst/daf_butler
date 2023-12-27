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
    "RelationBase",
    "RootRelationBase",
    "ColumnExpressionBase",
    "PredicateBase",
    "StringOrWildcard",
    "DatasetFieldName",
    "InvalidRelationError",
)

from abc import ABC, abstractmethod
from types import EllipsisType
from typing import TYPE_CHECKING, Annotated, Any, Literal, TypeAlias, cast

import pydantic

if TYPE_CHECKING:
    from ...column_spec import ColumnType
    from ...dimensions import DimensionGroup
    from ._column_expression import OrderExpression
    from ._column_reference import ColumnReference
    from ._predicate import Predicate
    from ._relation import Relation, RootRelation


StringOrWildcard = Annotated[
    str | EllipsisType,
    pydantic.PlainSerializer(lambda x: "..." if x is ... else x, return_type=str),
    pydantic.BeforeValidator(lambda x: ... if x == "..." else x),
    pydantic.GetPydanticSchema(lambda _s, h: h(str)),
]


DatasetFieldName: TypeAlias = Literal["dataset_id", "ingest_date", "run", "collection", "rank", "timespan"]


class InvalidRelationError(RuntimeError):
    """Exception raised when an operation would create an invalid relation
    tree.
    """


class RelationTreeBase(pydantic.BaseModel):
    """Base class for all non-primitive types in a relation tree."""

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid", strict=True)


class RelationBase(RelationTreeBase, ABC):
    """Base class for objects that represent relations in a relation tree.

    This is a closed hierarchy whose concrete, `~typing.final` derived classes
    are members of the `Relation` union.  That union should generally be used
    in type annotations rather than the formally-open base class.

    `Relation` types are also required to have a ``dimensions`` attribute of
    type `DimensionGroup`, but are permitted to implement this as a regular
    attribute or `property`, and there is no way to express that in a base
    class.
    """

    @property
    @abstractmethod
    def available_dataset_types(self) -> frozenset[str]:
        """The dataset types whose ID columns (at least) are available from
        this relation.
        """
        raise NotImplementedError()


class RootRelationBase(RelationBase):
    """Base class for relations that can occupy the root of a relation tree.

    This is a closed hierarchy whose concrete, `~typing.final` derived classes
    are members of the `RootRelation` union.  That union should generally be
    used in type annotations rather than the formally-open base class.
    """

    @abstractmethod
    def join(self, other: Relation) -> RootRelation:
        """Return a new relation that represents a join between ``self`` and
        ``other``.

        Parameters
        ----------
        other : `Relation`
            Relation to join to this one.

        Returns
        -------
        result : `RootRelation`
            A new relation that joins ``self`` and ``other``.

        Raises
        ------
        InvalidRelationError
            Raised if the join is ambiguous or otherwise invalid.
        """
        raise NotImplementedError()

    @abstractmethod
    def where(self, *args: Predicate) -> RootRelation:
        """Return a new relation that adds row filtering via a boolean column
        expression.

        Parameters
        ----------
        *args : `Predicate`
            Boolean column expressions that filter rows.  Arguments are
            combined with logical AND.

        Returns
        -------
        result : `RootRelation`
            A new relation that with row filtering.

        Raises
        ------
        InvalidRelationError
            Raised if a column expression requires a dataset column that is not
            already present in the relation tree.

        Notes
        -----
        If an expression references a dimension or dimension element that is
        not already present in the relation tree, it will be joined in, but
        dataset searches must already be joined into a relation tree in order
        to reference their fields in expressions.
        """
        raise NotImplementedError()

    @abstractmethod
    def order_by(self, *terms: OrderExpression, limit: int | None = None, offset: int = 0) -> RootRelation:
        """Return a new relation that sorts and/or applies positional slicing.

        Parameters
        ----------
        *terms : `str` or `OrderExpression`
            Expression objects to use for ordering.
        limit : `int` or `None`, optional
            Upper limit on the number of returned records.
        offset : `int`, optional
            The number of records to skip before returning at most ``limit``
            records.

        Returns
        -------
        result : `RootRelation`
            A new relation object whose results will be sorted and/or
            positionally sliced.

        Raises
        ------
        InvalidRelationError
            Raised if a column expression requires a dataset column that is not
            already present in the relation tree.
        """
        raise NotImplementedError()

    @abstractmethod
    def find_first(self, dataset_type: str, dimensions: DimensionGroup) -> RootRelation:
        """Return a new relation that searches a dataset's collections in the
        for the first match for each dataset type and data ID.

        Parameters
        ----------
        dataset_type : `str`
            Name of the dataset type.  Must be available in the relation tree
            already.
        dimensions : `DimensionGroup`
            Dimensions to group by.  This is typically the dimensions of the
            dataset type, but in certain cases (such as calibration lookups)
            it may be useful to user a superset of the dataset type's
            dimensions.
        """
        raise NotImplementedError()


class ColumnExpressionBase(RelationTreeBase, ABC):
    """Base class for objects that represent non-boolean column expressions in
    a relation tree.

    This is a closed hierarchy whose concrete, `~typing.final` derived classes
    are members of the `ColumnExpression` union.  That union should generally
    be used in type annotations rather than the formally-open base class.
    """

    expression_type: str

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
    def column_type(self) -> ColumnType:
        """A string enumeration value representing the type of the column
        expression.

        The default implementation returns the object's `expression_type` tag,
        which is appropriate only for literal columns.
        """
        return cast(ColumnType, self.expression_type)

    def gather_required_columns(self) -> set[ColumnReference]:
        """Return a `set` containing all `ColumnReference` objects embedded
        recursively in this expression.
        """
        return set()


class PredicateBase(RelationTreeBase, ABC):
    """Base class for objects that represent boolean column expressions in a
    relation tree.

    A `Predicate` tree is always in conjunctive normal form (ANDs of ORs of
    NOTs).  This is enforced by type annotations (and hence Pydantic
    validation) and the `logical_and`, `logical_or`, and `logical_not` factory
    methods.

    This is a closed hierarchy whose concrete, `~typing.final` derived classes
    are members of the `Predicate` union.  That union should generally be used
    in type annotations rather than the formally-open base class.
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

    def gather_required_columns(self) -> set[ColumnReference]:
        """Return a `set` containing all `ColumnReference` objects embedded
        recursively in this expression.
        """
        return set()

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

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
    "CheckVisitor",
    "InspectionVisitor",
    "InspectionSummary",
)

import dataclasses
from collections.abc import Mapping, Sequence, Set
from typing import TYPE_CHECKING, Any

from ...._column_tags import DatasetColumnTag, DimensionKeyColumnTag, DimensionRecordColumnTag
from ....dimensions import DataCoordinate, DataIdValue, Dimension, DimensionGroup, DimensionUniverse
from ..._exceptions import UserExpressionError
from .categorize import ExpressionConstant, categorizeConstant, categorizeElementId
from .normalForm import NormalForm, NormalFormVisitor
from .parser import Node, TreeVisitor

if TYPE_CHECKING:
    import astropy.time
    from lsst.daf.relation import ColumnTag


@dataclasses.dataclass
class InspectionSummary:
    """Base class for objects used by `CheckVisitor` and `InspectionVisitor`
    to gather information about a parsed expression.
    """

    def update(self, other: InspectionSummary) -> None:
        """Update ``self`` with all dimensions and columns from ``other``.

        Parameters
        ----------
        other : `InspectionSummary`
            The other summary object.
        """
        self.dimensions.update(other.dimensions)
        for element, columns in other.columns.items():
            self.columns.setdefault(element, set()).update(columns)
        self.hasIngestDate = self.hasIngestDate or other.hasIngestDate

    dimensions: set[str] = dataclasses.field(default_factory=set)
    """Names of dimensions whose primary keys or dependencies were referenced
    anywhere in this branch (`set` [ `str` ]).
    """

    columns: dict[str, set[str]] = dataclasses.field(default_factory=dict)
    """Names of dimension element tables whose columns were referenced anywhere
    in this branch (`dict` [ `str`, `set` [ `str` ] ]).
    """

    hasIngestDate: bool = False
    """Whether this expression includes the special dataset ingest date
    identifier (`bool`).
    """

    def make_column_tag_set(self, dataset_type_name: str | None) -> set[ColumnTag]:
        """Transform the columns captured here into a set of `ColumnTag`
        objects.

        Parameters
        ----------
        dataset_type_name : `str` or `None`
            Name of the dataset type to assume for unqualified dataset columns,
            or `None` to reject any such identifiers.

        Returns
        -------
        tag_set : `set` [ `ColumnTag` ]
            Set of categorized column tags.
        """
        result: set[ColumnTag] = set()
        if self.hasIngestDate:
            if dataset_type_name is None:
                raise UserExpressionError(
                    "Expression requires an ingest date, which requires exactly one dataset type."
                )
            result.add(DatasetColumnTag(dataset_type_name, "ingest_date"))
        result.update(DimensionKeyColumnTag.generate(self.dimensions))
        for dimension_element, columns in self.columns.items():
            result.update(DimensionRecordColumnTag.generate(dimension_element, columns))
        return result


@dataclasses.dataclass
class TreeSummary(InspectionSummary):
    """Result object used by `InspectionVisitor` to gather information about
    a parsed expression.

    Notes
    -----
    TreeSummary adds attributes that allow dimension equivalence expressions
    (e.g. "tract=4") to be recognized when they appear in simple contexts
    (surrounded only by ANDs and ORs).  When `InspectionVisitor` is used on its
    own (i.e. when ``check=False`` in the query code), these don't do anything,
    but they don't cost much, either.  They are used by `CheckVisitor` when it
    delegates to `InspectionVisitor` to see what governor dimension values are
    set in a branch of the normal-form expression.
    """

    def merge(self, other: TreeSummary, isEq: bool = False) -> TreeSummary:
        """Merge ``other`` into ``self``, making ``self`` a summary of both
        expression tree branches.

        Parameters
        ----------
        other : `TreeSummary`
            The other summary object.
        isEq : `bool`, optional
            If `True` (`False` is default), these summaries are being combined
            via the equality operator.

        Returns
        -------
        self : `TreeSummary`
            The merged summary (updated in-place).
        """
        self.update(other)
        if isEq and self.isDataIdKeyOnly() and other.isDataIdValueOnly():
            self.dataIdValue = other.dataIdValue
        elif isEq and self.isDataIdValueOnly() and other.isDataIdKeyOnly():
            self.dataIdKey = other.dataIdKey
        else:
            self.dataIdKey = None
            self.dataIdValue = None
        return self

    def isDataIdKeyOnly(self) -> bool:
        """Test whether this branch is _just_ a data ID key identifier."""
        return self.dataIdKey is not None and self.dataIdValue is None

    def isDataIdValueOnly(self) -> bool:
        """Test whether this branch is _just_ a literal value that may be
        used as the value in a data ID key-value pair.
        """
        return self.dataIdKey is None and self.dataIdValue is not None

    dataIdKey: Dimension | None = None
    """A `Dimension` that is (if `dataIdValue` is not `None`) or may be
    (if `dataIdValue` is `None`) fully identified by a literal value in this
    branch.
    """

    dataIdValue: str | None = None
    """A literal value that constrains (if `dataIdKey` is not `None`) or may
    constrain (if `dataIdKey` is `None`) a dimension in this branch.

    This is always a `str` or `None`, but it may need to be coerced to `int`
    to reflect the actual user intent.
    """


class InspectionVisitor(TreeVisitor[TreeSummary]):
    """Implements TreeVisitor to identify dimension elements that need
    to be included in a query, prior to actually constructing a SQLAlchemy
    WHERE clause from it.

    Parameters
    ----------
    universe : `DimensionUniverse`
        All known dimensions.
    bind : `~collections.abc.Mapping` [ `str`, `object` ]
        Mapping containing literal values that should be injected into the
        query expression, keyed by the identifiers they replace.
    """

    def __init__(self, universe: DimensionUniverse, bind: Mapping[str, Any]):
        self.universe = universe
        self.bind = bind

    def visitNumericLiteral(self, value: str, node: Node) -> TreeSummary:
        # Docstring inherited from TreeVisitor.visitNumericLiteral
        return TreeSummary(dataIdValue=value)

    def visitStringLiteral(self, value: str, node: Node) -> TreeSummary:
        # Docstring inherited from TreeVisitor.visitStringLiteral
        return TreeSummary(dataIdValue=value)

    def visitTimeLiteral(self, value: astropy.time.Time, node: Node) -> TreeSummary:
        # Docstring inherited from TreeVisitor.visitTimeLiteral
        return TreeSummary()

    def visitIdentifier(self, name: str, node: Node) -> TreeSummary:
        # Docstring inherited from TreeVisitor.visitIdentifier
        if name in self.bind:
            value = self.bind[name]
            if isinstance(value, list | tuple | Set):
                # This can happen on rhs of IN operator, if there is only one
                # element in the list then take it.
                if len(value) == 1:
                    return TreeSummary(dataIdValue=next(iter(value)))
                else:
                    return TreeSummary()
            else:
                return TreeSummary(dataIdValue=value)
        constant = categorizeConstant(name)
        if constant is ExpressionConstant.INGEST_DATE:
            return TreeSummary(hasIngestDate=True)
        elif constant is ExpressionConstant.NULL:
            return TreeSummary()
        assert constant is None, "Enum variant conditionals should be exhaustive."
        element, column = categorizeElementId(self.universe, name)
        if column is None:
            assert isinstance(element, Dimension)
            return TreeSummary(
                dimensions=set(element.minimal_group.names),
                dataIdKey=element,
            )
        else:
            return TreeSummary(dimensions=set(element.minimal_group.names), columns={element.name: {column}})

    def visitUnaryOp(self, operator: str, operand: TreeSummary, node: Node) -> TreeSummary:
        # Docstring inherited from TreeVisitor.visitUnaryOp
        return operand

    def visitBinaryOp(self, operator: str, lhs: TreeSummary, rhs: TreeSummary, node: Node) -> TreeSummary:
        # Docstring inherited from TreeVisitor.visitBinaryOp
        return lhs.merge(rhs, isEq=(operator == "="))

    def visitIsIn(self, lhs: TreeSummary, values: list[TreeSummary], not_in: bool, node: Node) -> TreeSummary:
        # Docstring inherited from TreeVisitor.visitIsIn
        for v in values:
            lhs.merge(v)
        return lhs

    def visitParens(self, expression: TreeSummary, node: Node) -> TreeSummary:
        # Docstring inherited from TreeVisitor.visitParens
        return expression

    def visitTupleNode(self, items: tuple[TreeSummary, ...], node: Node) -> TreeSummary:
        # Docstring inherited from base class
        result = TreeSummary()
        for i in items:
            result.merge(i)
        return result

    def visitRangeLiteral(self, start: int, stop: int, stride: int | None, node: Node) -> TreeSummary:
        # Docstring inherited from TreeVisitor.visitRangeLiteral
        return TreeSummary()

    def visitPointNode(self, ra: TreeSummary, dec: TreeSummary, node: Node) -> TreeSummary:
        # Docstring inherited from base class
        return TreeSummary()


@dataclasses.dataclass
class InnerSummary(InspectionSummary):
    """Result object used by `CheckVisitor` to gather referenced dimensions
    and tables from an inner group of AND'd together expression branches, and
    check them for consistency and completeness.
    """

    dimension_values: dict[str, DataIdValue] = dataclasses.field(default_factory=dict)
    """Mapping containing the values of all dimensions that are equated with
    literal values in this expression branch.
    """

    defaultsNeeded: set[str] = dataclasses.field(default_factory=set)
    """Governor dimensions whose values are needed by the query, not provided
    in the query itself, and present in the default data ID.

    These should be added to the query's data ID when finalizing the WHERE
    clause.
    """


@dataclasses.dataclass
class OuterSummary(InspectionSummary):
    """Result object used by `CheckVisitor` to gather referenced dimensions,
    tables, and governor dimension values from the entire expression.
    """

    dimension_constraints: dict[str, set[DataIdValue]] = dataclasses.field(default_factory=dict)
    """Mapping containing all values that appear in this expression for
    dimensions relevant to the query.

    Dimensions that are absent from this dict are not constrained by this
    expression.
    """

    defaultsNeeded: set[str] = dataclasses.field(default_factory=set)
    """Governor dimensions whose values are needed by the query, not provided
    in the query itself, and present in the default data ID.

    These should be added to the query's data ID when finalizing the WHERE
    clause.
    """


class CheckVisitor(NormalFormVisitor[TreeSummary, InnerSummary, OuterSummary]):
    """An implementation of `NormalFormVisitor` that identifies the dimensions
    and tables that need to be included in a query while performing some checks
    for completeness and consistency.

    Parameters
    ----------
    dataId : `DataCoordinate`
        Dimension values that are fully known in advance.
    dimensions : `DimensionGroup`
        The dimensions the query would include in the absence of this
        expression.
    bind : `~collections.abc.Mapping` [ `str`, `object` ]
        Mapping containing literal values that should be injected into the
        query expression, keyed by the identifiers they replace.
    defaults : `DataCoordinate`
        A data ID containing default for governor dimensions.
    allow_orphans : `bool`, optional
        If `True`, permit expressions to refer to dimensions without providing
        a value for their governor dimensions (e.g. referring to a visit
        without an instrument).  Should be left to default to `False` in
        essentially all new code.
    """

    def __init__(
        self,
        dataId: DataCoordinate,
        dimensions: DimensionGroup,
        bind: Mapping[str, Any],
        defaults: DataCoordinate,
        allow_orphans: bool = False,
    ):
        self.dataId = dataId
        self.dimensions = dimensions
        self.defaults = defaults
        self._branchVisitor = InspectionVisitor(dataId.universe, bind)
        self._allow_orphans = allow_orphans

    @property
    def universe(self) -> DimensionUniverse:
        """Object that defines all dimensions."""
        return self.dimensions.universe

    def visitBranch(self, node: Node) -> TreeSummary:
        # Docstring inherited from NormalFormVisitor.
        return node.visit(self._branchVisitor)

    def visitInner(self, branches: Sequence[TreeSummary], form: NormalForm) -> InnerSummary:
        # Docstring inherited from NormalFormVisitor.
        # Disjunctive normal form means inner branches are AND'd together...
        assert form is NormalForm.DISJUNCTIVE
        # ...and that means each branch we iterate over together below
        # constrains the others, and they all need to be consistent.  Moreover,
        # because outer branches are OR'd together, we also know that if
        # something is missing from one of these branches (like a governor
        # dimension value like the instrument or skymap needed to interpret a
        # visit or tract number), it really is missing, because there's no way
        # some other inner branch can constraint it.
        #
        # That is, except the data ID the visitor was passed at construction;
        # that's AND'd to the entire expression later, and thus it affects all
        # branches.  To take care of that, we add any governor values it
        # contains to the summary in advance.
        summary = InnerSummary()
        summary.dimension_values.update(self.dataId.mapping)
        # Finally, we loop over those branches.
        for branch in branches:
            # Update the sets of dimensions and columns we've seen anywhere in
            # the expression in any context.
            summary.update(branch)
            # Test whether this branch has a form like '<dimension>=<value>'
            # (or equivalent; categorizeIdentifier is smart enough to see that
            # e.g.  'detector.id=4' is equivalent to 'detector=4').  If so,
            # remember that we've constrained it on this branch to later make
            # sure it's consistent with any other constraints on any other
            # branches its AND'd with.
            if branch.dataIdKey is not None and branch.dataIdValue is not None:
                new_value = branch.dataIdKey.primaryKey.getPythonType()(branch.dataIdValue)
                value = summary.dimension_values.setdefault(branch.dataIdKey.name, new_value)
                if value != new_value:
                    # Expression says something like "instrument='HSC' AND
                    # instrument='DECam'", or data ID has one and expression
                    # has the other.
                    if branch.dataIdKey.name in self.dataId:
                        raise UserExpressionError(
                            f"Conflict between expression containing {branch.dataIdKey.name}={new_value!r} "
                            f"and data ID with {branch.dataIdKey.name}={value!r}."
                        )
                    else:
                        raise UserExpressionError(
                            f"Conflicting literal values for {branch.dataIdKey.name} in expression: "
                            f"{value!r} != {branch.dataIdValue!r}."
                        )
        # Now that we know which governor values we've constrained, see if any
        # are missing, i.e. if the expression contains something like "visit=X"
        # without saying what instrument that visit corresponds to.  This rules
        # out a lot of accidents, but it also rules out possibly-legitimate
        # multi-instrument queries like "visit.seeing < 0.7".  But it's not
        # unreasonable to ask the user to be explicit about the instruments
        # they want to consider to work around this restriction, and that's
        # what we do.  Note that if someone does write an expression like
        #
        #  (instrument='HSC' OR instrument='DECam') AND visit.seeing < 0.7
        #
        # then in disjunctive normal form that will become
        #
        #  (instrument='HSC' AND visit.seeing < 0.7)
        #    OR (instrument='DECam' AND visit.seeing < 0.7)
        #
        # i.e. each instrument will get its own outer branch and the logic here
        # still works (that sort of thing is why we convert to normal form,
        # after all).
        governorsNeededInBranch: set[str] = set()
        for dimension in summary.dimensions:
            governorsNeededInBranch.update(self.universe.dimensions[dimension].minimal_group.governors)
        if not governorsNeededInBranch.issubset(summary.dimension_values.keys()):
            missing = governorsNeededInBranch - summary.dimension_values.keys()
            if missing <= self.defaults.dimensions.required:
                summary.defaultsNeeded.update(missing)
            elif not self._allow_orphans:
                still_missing = missing - self.defaults.names
                raise UserExpressionError(
                    f"No value(s) for governor dimensions {still_missing} in expression "
                    "that references dependent dimensions. 'Governor' dimensions must always be specified "
                    "completely in either the query expression (via simple 'name=<value>' terms, not 'IN' "
                    "terms) or in a data ID passed to the query method."
                )
        return summary

    def visitOuter(self, branches: Sequence[InnerSummary], form: NormalForm) -> OuterSummary:
        # Docstring inherited from NormalFormVisitor.
        # Disjunctive normal form means outer branches are OR'd together.
        assert form is NormalForm.DISJUNCTIVE
        summary = OuterSummary()
        if branches:
            # Iterate over branches in first pass to gather all dimensions and
            # columns referenced.  This aggregation is for the full query, so
            # we don't care whether things are joined by AND or OR (or + or -,
            # etc).  Also gather the set of dimensions directly constrained or
            # pulled from defaults in _all_ branches.  This is the set we will
            # be able to bound overall; any dimensions not referenced by even
            # one branch could be unbounded.
            dimensions_in_all_branches = set(self.universe.dimensions.names)
            for branch in branches:
                summary.update(branch)
                summary.defaultsNeeded.update(branch.defaultsNeeded)
                dimensions_in_all_branches.intersection_update(branch.dimension_values)
            # Go back through and set up the dimension bounds.
            summary.dimension_constraints.update(
                {dimension: set() for dimension in dimensions_in_all_branches}
            )
            for dim in dimensions_in_all_branches:
                for branch in branches:
                    summary.dimension_constraints[dim].add(branch.dimension_values[dim])
        # See if we've referenced any dimensions that weren't in the original
        # query graph; if so, we update that to include them.  This is what
        # lets a user say "tract=X" on the command line (well, "skymap=Y AND
        # tract=X" - logic in visitInner checks for that) when running a task
        # like ISR that has nothing to do with skymaps.
        if not summary.dimensions.issubset(self.dimensions.names):
            self.dimensions = self.universe.conform(summary.dimensions | self.dimensions.names)
        for dimension, values in summary.dimension_constraints.items():
            if dimension in summary.defaultsNeeded:
                # One branch contained an explicit value for this dimension
                # while another needed to refer to the default data ID.
                # Even if these refer to the same value, that inconsistency
                # probably indicates user error.
                raise UserExpressionError(
                    f"Governor dimension {dimension} is explicitly "
                    f"constrained to {values} in one or more branches of "
                    "this query where expression, but is left to default "
                    f"to {self.defaults[dimension]!r} in another branch.  "
                    "Defaults and explicit constraints cannot be mixed."
                )
        # If any default data ID values were needed, update self.dataId with
        # them, and then update the governor restriction with them.
        if summary.defaultsNeeded:
            defaultsNeededGraph = self.universe.conform(summary.defaultsNeeded)
            self.dataId = self.dataId.union(self.defaults.subset(defaultsNeededGraph))
            for dimension in summary.defaultsNeeded:
                summary.dimension_constraints[dimension] = {self.defaults[dimension]}

        return summary

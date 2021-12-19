# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

__all__ = (
    "NormalForm",
    "NormalFormExpression",
    "NormalFormVisitor",
)

import enum
from abc import ABC, abstractmethod
from typing import Dict, Generic, Iterator, List, Optional, Sequence, Tuple, TypeVar

import astropy.time

from .parser import BinaryOp, Node, Parens, TreeVisitor, UnaryOp


class LogicalBinaryOperator(enum.Enum):
    """Enumeration for logical binary operators.

    These intentionally have the same names (including capitalization) as the
    string binary operators used in the parser itself.  Boolean values are
    used to enable generic code that works on either operator (particularly
    ``LogicalBinaryOperator(not self)`` as a way to get the other operator).

    Which is `True` and which is `False` is just a convention, but one shared
    by `LogicalBinaryOperator` and `NormalForm`.
    """

    AND = True
    OR = False

    def apply(self, lhs: TransformationWrapper, rhs: TransformationWrapper) -> LogicalBinaryOperation:
        """Return a `TransformationWrapper` object representing this operator
        applied to the given operands.

        This is simply syntactic sugar for the `LogicalBinaryOperation`
        constructor.

        Parameters
        ----------
        lhs: `TransformationWrapper`
            First operand.
        rhs: `TransformationWrapper`
            Second operand.

        Returns
        -------
        operation : `LogicalBinaryOperation`
            Object representing the operation.
        """
        return LogicalBinaryOperation(lhs, self, rhs)


class NormalForm(enum.Enum):
    """Enumeration for boolean normal forms.

    Both normal forms require all NOT operands to be moved "inside" any AND
    or OR operations (i.e. by applying DeMorgan's laws), with either all AND
    operations or all OR operations appearing outside the other (`CONJUNCTIVE`,
    and `DISJUNCTIVE`, respectively).

    Boolean values are used here to enable generic code that works on either
    form, and for interoperability with `LogicalBinaryOperator`.

    Which is `True` and which is `False` is just a convention, but one shared
    by `LogicalBinaryOperator` and `NormalForm`.
    """

    CONJUNCTIVE = True
    """Form in which AND operations may have OR operands, but not the reverse.

    For example, ``A AND (B OR C)`` is in conjunctive normal form, but
    ``A OR (B AND C)`` and ``A AND (B OR (C AND D))`` are not.
    """

    DISJUNCTIVE = False
    """Form in which OR operations may have AND operands, but not the reverse.

    For example, ``A OR (B AND C)`` is in disjunctive normal form, but
    ``A AND (B OR C)`` and ``A OR (B AND (C OR D))`` are not.
    """

    @property
    def inner(self) -> LogicalBinaryOperator:
        """The operator that is not permitted to contain operations of the
        other type in this form.

        Note that this operation may still appear as the outermost operator in
        an expression in this form if there are no operations of the other
        type.
        """
        return LogicalBinaryOperator(not self.value)

    @property
    def outer(self) -> LogicalBinaryOperator:
        """The operator that is not permitted to be contained by operations of
        the other type in this form.

        Note that an operation of this type is not necessarily the outermost
        operator in an expression in this form; the expression need not contain
        any operations of this type.
        """
        return LogicalBinaryOperator(self.value)

    def allows(self, *, inner: LogicalBinaryOperator, outer: LogicalBinaryOperator) -> bool:
        """Test whether this form allows the given operator relationships.

        Parameters
        ----------
        inner : `LogicalBinaryOperator`
            Inner operator in an expression.  May be the same as ``outer``.
        outer : `LogicalBinaryOperator`
            Outer operator in an expression.  May be the same as ``inner``.

        Returns
        -------
        allowed : `bool`
            Whether this operation relationship is allowed.
        """
        return inner == outer or outer is self.outer


_T = TypeVar("_T")
_U = TypeVar("_U")
_V = TypeVar("_V")


class NormalFormVisitor(Generic[_T, _U, _V]):
    """A visitor interface for `NormalFormExpression`.

    A visitor implementation may inherit from both `NormalFormVisitor` and
    `TreeVisitor` and implement `visitBranch` as ``node.visit(self)`` to
    visit each node of the entire tree (or similarly with composition, etc).
    In this case, `TreeVisitor.visitBinaryOp` will never be called with a
    logical OR or AND operation, because these will all have been moved into
    calls to `visitInner` and `visitOuter` instead.

    See also `NormalFormExpression.visit`.
    """

    @abstractmethod
    def visitBranch(self, node: Node) -> _T:
        """Visit a regular `Node` and its child nodes.

        Parameters
        ----------
        node : `Node`
            A branch of the expression tree that contains no AND or OR
            operations.

        Returns
        -------
        result
            Implementation-defined result to be gathered and passed to
            `visitInner`.
        """
        raise NotImplementedError()

    @abstractmethod
    def visitInner(self, branches: Sequence[_T], form: NormalForm) -> _U:
        """Visit a sequence of inner OR (for `~NormalForm.CONJUNCTIVE` form)
        or AND (for `~NormalForm.DISJUNCTIVE`) operands.

        Parameters
        ----------
        branches : `Sequence`
            Sequence of tuples, where the first element in each tuple is the
            result of a call to `visitBranch`, and the second is the `Node` on
            which `visitBranch` was called.
        form : `NormalForm`
            Form this expression is in.  ``form.inner`` is the operator that
            joins the operands in ``branches``.

        Returns
        -------
        result
            Implementation-defined result to be gathered and passed to
            `visitOuter`.
        """
        raise NotImplementedError()

    @abstractmethod
    def visitOuter(self, branches: Sequence[_U], form: NormalForm) -> _V:
        """Visit the sequence of outer AND (for `~NormalForm.CONJUNCTIVE` form)
        or OR (for `~NormalForm.DISJUNCTIVE`) operands.

        Parameters
        ----------
        branches : `Sequence`
            Sequence of return values from calls to `visitInner`.
        form : `NormalForm`
            Form this expression is in.  ``form.outer`` is the operator that
            joins the operands in ``branches``.

        Returns
        -------
        result
            Implementation-defined result to be returned by
            `NormalFormExpression.visitNormalForm`.
        """
        raise NotImplementedError()


class NormalFormExpression:
    """A boolean expression in a standard normal form.

    Most code should use `fromTree` to construct new instances instead of
    calling the constructor directly.  See `NormalForm` for a description of
    the two forms.

    Parameters
    ----------
    nodes : `Sequence` [ `Sequence` [ `Node` ] ]
        Non-AND, non-OR branches of three tree, with the AND and OR operations
        combining them represented by position in the nested sequence - the
        inner sequence is combined via ``form.inner``, and the outer sequence
        combines those via ``form.outer``.
    form : `NormalForm`
        Enumeration value indicating the form this expression is in.
    """

    def __init__(self, nodes: Sequence[Sequence[Node]], form: NormalForm):
        self._form = form
        self._nodes = nodes

    def __str__(self) -> str:
        return str(self.toTree())

    @staticmethod
    def fromTree(root: Node, form: NormalForm) -> NormalFormExpression:
        """Construct a `NormalFormExpression` by normalizing an arbitrary
        expression tree.

        Parameters
        ----------
        root : `Node`
            Root of the tree to be normalized.
        form : `NormalForm`
            Enumeration value indicating the form to normalize to.

        Notes
        -----
        Converting an arbitrary boolean expression to either normal form is an
        NP-hard problem, and this is a brute-force algorithm.  I'm not sure
        what its actual algorithmic scaling is, but it'd definitely be a bad
        idea to attempt to normalize an expression with hundreds or thousands
        of clauses.
        """
        wrapper = root.visit(TransformationVisitor()).normalize(form)
        nodes = []
        for outerOperands in wrapper.flatten(form.outer):
            nodes.append([w.unwrap() for w in outerOperands.flatten(form.inner)])
        return NormalFormExpression(nodes, form=form)

    @property
    def form(self) -> NormalForm:
        """Enumeration value indicating the form this expression is in."""
        return self._form

    def visit(self, visitor: NormalFormVisitor[_T, _U, _V]) -> _V:
        """Apply a visitor to the expression, explicitly taking advantage of
        the special structure guaranteed by the normal forms.

        Parameters
        ----------
        visitor : `NormalFormVisitor`
            Visitor object to apply.

        Returns
        -------
        result
            Return value from calling ``visitor.visitOuter`` after visiting
            all other nodes.
        """
        visitedOuterBranches: List[_U] = []
        for nodeInnerBranches in self._nodes:
            visitedInnerBranches = [visitor.visitBranch(node) for node in nodeInnerBranches]
            visitedOuterBranches.append(visitor.visitInner(visitedInnerBranches, self.form))
        return visitor.visitOuter(visitedOuterBranches, self.form)

    def toTree(self) -> Node:
        """Convert ``self`` back into an equivalent tree, preserving the
        form of the boolean expression but representing it in memory as
        a tree.

        Returns
        -------
        tree : `Node`
            Root of the tree equivalent to ``self``.
        """
        visitor = TreeReconstructionVisitor()
        return self.visit(visitor)


class PrecedenceTier(enum.Enum):
    """An enumeration used to track operator precedence.

    This enum is currently used only to inject parentheses into boolean
    logic that has been manipulated into a new form, and because those
    parentheses are only used for stringification, the goal here is human
    readability, not precision.

    Lower enum values represent tighter binding, but code deciding whether to
    inject parentheses should always call `needsParens` instead of comparing
    values directly.
    """

    TOKEN = 0
    """Precedence tier for literals, identifiers, and expressions already in
    parentheses.
    """

    UNARY = 1
    """Precedence tier for unary operators, which always bind more tightly
    than any binary operator.
    """

    VALUE_BINARY_OP = 2
    """Precedence tier for binary operators that return non-boolean values.
    """

    COMPARISON = 3
    """Precedence tier for binary comparison operators that accept non-boolean
    values and return boolean values.
    """

    AND = 4
    """Precedence tier for logical AND.
    """

    OR = 5
    """Precedence tier for logical OR.
    """

    @classmethod
    def needsParens(cls, outer: PrecedenceTier, inner: PrecedenceTier) -> bool:
        """Test whether parentheses should be added around an operand.

        Parameters
        ----------
        outer : `PrecedenceTier`
            Precedence tier for the operation.
        inner : `PrecedenceTier`
            Precedence tier for the operand.

        Returns
        -------
        needed : `bool`
            If `True`, parentheses should be added.

        Notes
        -----
        This method special cases logical binary operators for readability,
        adding parentheses around ANDs embedded in ORs, and avoiding them in
        chains of the same operator.  If it is ever actually used with other
        binary operators as ``outer``, those would probably merit similar
        attention (or a totally new approach); its current approach would
        aggressively add a lot of unfortunate parentheses because (aside from
        this special-casing) it doesn't know about commutativity.

        In fact, this method is rarely actually used for logical binary
        operators either; the `LogicalBinaryOperation.unwrap` method that calls
        it is never invoked by `NormalFormExpression` (the main public
        interface), because those operators are flattened out (see
        `TransformationWrapper.flatten`) instead.  Parentheses are instead
        added there by `TreeReconstructionVisitor`, which is simpler because
        the structure of operators is restricted.
        """
        if outer is cls.OR and inner is cls.AND:
            return True
        if outer is cls.OR and inner is cls.OR:
            return False
        if outer is cls.AND and inner is cls.AND:
            return False
        return outer.value <= inner.value


BINARY_OPERATOR_PRECEDENCE = {
    "=": PrecedenceTier.COMPARISON,
    "!=": PrecedenceTier.COMPARISON,
    "<": PrecedenceTier.COMPARISON,
    "<=": PrecedenceTier.COMPARISON,
    ">": PrecedenceTier.COMPARISON,
    ">=": PrecedenceTier.COMPARISON,
    "OVERLAPS": PrecedenceTier.COMPARISON,
    "+": PrecedenceTier.VALUE_BINARY_OP,
    "-": PrecedenceTier.VALUE_BINARY_OP,
    "*": PrecedenceTier.VALUE_BINARY_OP,
    "/": PrecedenceTier.VALUE_BINARY_OP,
    "AND": PrecedenceTier.AND,
    "OR": PrecedenceTier.OR,
}


class TransformationWrapper(ABC):
    """A base class for `Node` wrappers that can be used to transform boolean
    operator expressions.

    Notes
    -----
    `TransformationWrapper` instances should only be directly constructed by
    each other or `TransformationVisitor`.  No new subclasses beyond those in
    this module should be added.

    While `TransformationWrapper` and its subclasses contain the machinery
    for transforming expressions to normal forms, it does not provide a
    convenient interface for working with expressions in those forms; most code
    should just use `NormalFormExpression` (which delegates to
    `TransformationWrapper` internally) instead.
    """

    __slots__ = ()

    @abstractmethod
    def __str__(self) -> str:
        raise NotImplementedError()

    @property
    @abstractmethod
    def precedence(self) -> PrecedenceTier:
        """Return the precedence tier for this node (`PrecedenceTier`).

        Notes
        -----
        This is only used when reconstructing a full `Node` tree in `unwrap`
        implementations, and only to inject parenthesis that are necessary for
        correct stringification but nothing else (because the tree structure
        itself embeds the correct order of operations already).
        """
        raise NotImplementedError()

    @abstractmethod
    def not_(self) -> TransformationWrapper:
        """Return a wrapper that represents the logical NOT of ``self``.

        Returns
        -------
        wrapper : `TransformationWrapper`
            A wrapper that represents the logical NOT of ``self``.
        """
        raise NotImplementedError()

    def satisfies(self, form: NormalForm) -> bool:
        """Test whether this expressions is already in a normal form.

        The default implementation is appropriate for "atomic" classes that
        never contain any AND or OR operations at all.

        Parameters
        ----------
        form : `NormalForm`
            Enumeration indicating the form to test for.

        Returns
        -------
        satisfies : `bool`
            Whether ``self`` satisfies the requirements of ``form``.
        """
        return True

    def normalize(self, form: NormalForm) -> TransformationWrapper:
        """Return an expression equivalent to ``self`` in a normal form.

        The default implementation is appropriate for "atomic" classes that
        never contain any AND or OR operations at all.

        Parameters
        ----------
        form : `NormalForm`
            Enumeration indicating the form to convert to.

        Returns
        -------
        normalized : `TransformationWrapper`
            An expression equivalent to ``self`` for which
            ``normalized.satisfies(form)`` returns `True`.

        Notes
        -----
        Converting an arbitrary boolean expression to either normal form is an
        NP-hard problem, and this is a brute-force algorithm.  I'm not sure
        what its actual algorithmic scaling is, but it'd definitely be a bad
        idea to attempt to normalize an expression with hundreds or thousands
        of clauses.
        """
        return self

    def flatten(self, operator: LogicalBinaryOperator) -> Iterator[TransformationWrapper]:
        """Recursively flatten the operands of any nested operators of the
        given type.

        For an expression like ``(A AND ((B OR C) AND D)`` (with
        ``operator == AND``), `flatten` yields ``A, (B OR C), D``.

        The default implementation is appropriate for "atomic" classes that
        never contain any AND or OR operations at all.

        Parameters
        ----------
        operator : `LogicalBinaryOperator`
            Operator whose operands to flatten.

        Returns
        -------
        operands : `Iterator` [ `TransformationWrapper` ]
            Operands that, if combined with ``operator``, yield an expression
            equivalent to ``self``.
        """
        yield self

    def _satisfiesDispatch(
        self, operator: LogicalBinaryOperator, other: TransformationWrapper, *, form: NormalForm
    ) -> bool:
        """Test whether ``operator.apply(self, other)`` is in a normal form.

        The default implementation is appropriate for classes that never
        contain any AND or OR operations at all.

        Parameters
        ----------
        operator : `LogicalBinaryOperator`
            Operator for the operation being tested.
        other : `TransformationWrapper`
            Other operand for the operation being tested.
        form : `NormalForm`
            Normal form being tested for.

        Returns
        -------
        satisfies : `bool`
            Whether ``operator.apply(self, other)`` satisfies the requirements
            of ``form``.

        Notes
        -----
        Caller guarantees that ``self`` and ``other`` are already normalized.
        """
        return other._satisfiesDispatchAtomic(operator, self, form=form)

    def _normalizeDispatch(
        self, operator: LogicalBinaryOperator, other: TransformationWrapper, *, form: NormalForm
    ) -> TransformationWrapper:
        """Return an expression equivalent to ``operator.apply(self, other)``
        in a normal form.

        The default implementation is appropriate for classes that never
        contain any AND or OR operations at all.

        Parameters
        ----------
        operator : `LogicalBinaryOperator`
            Operator for the operation being transformed.
        other : `TransformationWrapper`
            Other operand for the operation being transformed.
        form : `NormalForm`
            Normal form being transformed to.

        Returns
        -------
        normalized : `TransformationWrapper`
            An expression equivalent to ``operator.apply(self, other)``
            for which ``normalized.satisfies(form)`` returns `True`.

        Notes
        -----
        Caller guarantees that ``self`` and ``other`` are already normalized.
        """
        return other._normalizeDispatchAtomic(operator, self, form=form)

    def _satisfiesDispatchAtomic(
        self, operator: LogicalBinaryOperator, other: TransformationWrapper, *, form: NormalForm
    ) -> bool:
        """Test whather ``operator.apply(other, self)`` is in a normal form.

        The default implementation is appropriate for "atomic" classes that
        never contain any AND or OR operations at all.

        Parameters
        ----------
        operator : `LogicalBinaryOperator`
            Operator for the operation being tested.
        other : `TransformationWrapper`
            Other operand for the operation being tested.
        form : `NormalForm`
            Normal form being tested for.

        Returns
        -------
        satisfies : `bool`
            Whether ``operator.apply(other, self)`` satisfies the requirements
            of ``form``.

        Notes
        -----
        Should only be called by `_satisfiesDispatch` implementations; this
        guarantees that ``other`` is atomic and ``self`` is normalized.
        """
        return True

    def _normalizeDispatchAtomic(
        self, operator: LogicalBinaryOperator, other: TransformationWrapper, *, form: NormalForm
    ) -> TransformationWrapper:
        """Return an expression equivalent to ``operator.apply(other, self)``,
        in a normal form.

        The default implementation is appropriate for "atomic" classes that
        never contain any AND or OR operations at all.

        Parameters
        ----------
        operator : `LogicalBinaryOperator`
            Operator for the operation being transformed.
        other : `TransformationWrapper`
            Other operand for the operation being transformed.
        form : `NormalForm`
            Normal form being transformed to.

        Returns
        -------
        normalized : `TransformationWrapper`
            An expression equivalent to ``operator.apply(other, self)``
            for which ``normalized.satisfies(form)`` returns `True`.

        Notes
        -----
        Should only be called by `_normalizeDispatch` implementations; this
        guarantees that ``other`` is atomic and ``self`` is normalized.
        """
        return operator.apply(other, self)

    def _satisfiesDispatchBinary(
        self,
        outer: LogicalBinaryOperator,
        lhs: TransformationWrapper,
        inner: LogicalBinaryOperator,
        rhs: TransformationWrapper,
        *,
        form: NormalForm,
    ) -> bool:
        """Test whether ``outer.apply(self, inner.apply(lhs, rhs))`` is in a
        normal form.

        The default implementation is appropriate for "atomic" classes that
        never contain any AND or OR operations at all.

        Parameters
        ----------
        outer : `LogicalBinaryOperator`
            Outer operator for the expression being tested.
        lhs : `TransformationWrapper`
            One inner operand for the expression being tested.
        inner : `LogicalBinaryOperator`
            Inner operator for the expression being tested.  This may or may
            not be the same as ``outer``.
        rhs: `TransformationWrapper`
            The other inner operand for the expression being tested.
        form : `NormalForm`
            Normal form being transformed to.

        Returns
        -------
        satisfies : `bool`
            Whether ``outer.apply(self, inner.apply(lhs, rhs))`` satisfies the
            requirements of ``form``.

        Notes
        -----
        Should only be called by `_satisfiesDispatch` implementations; this
        guarantees that ``self``, ``lhs``, and ``rhs`` are all normalized.
        """
        return form.allows(inner=inner, outer=outer)

    def _normalizeDispatchBinary(
        self,
        outer: LogicalBinaryOperator,
        lhs: TransformationWrapper,
        inner: LogicalBinaryOperator,
        rhs: TransformationWrapper,
        *,
        form: NormalForm,
    ) -> TransformationWrapper:
        """Return an expression equivalent to
        ``outer.apply(self, inner.apply(lhs, rhs))``, meeting the guarantees of
        `normalize`.

        The default implementation is appropriate for "atomic" classes that
        never contain any AND or OR operations at all.

        Parameters
        ----------
        outer : `LogicalBinaryOperator`
            Outer operator for the expression being transformed.
        lhs : `TransformationWrapper`
            One inner operand for the expression being transformed.
        inner : `LogicalBinaryOperator`
            Inner operator for the expression being transformed.
        rhs: `TransformationWrapper`
            The other inner operand for the expression being transformed.
        form : `NormalForm`
            Normal form being transformed to.

        Returns
        -------
        normalized : `TransformationWrapper`
            An expression equivalent to
            ``outer.apply(self, inner.apply(lhs, rhs))`` for which
            ``normalized.satisfies(form)`` returns `True`.

        Notes
        -----
        Should only be called by `_normalizeDispatch` implementations; this
        guarantees that ``self``, ``lhs``, and ``rhs`` are all normalized.
        """
        if form.allows(inner=inner, outer=outer):
            return outer.apply(inner.apply(lhs, rhs), self)
        else:
            return inner.apply(
                outer.apply(lhs, self).normalize(form),
                outer.apply(rhs, self).normalize(form),
            )

    @abstractmethod
    def unwrap(self) -> Node:
        """Return an transformed expression tree.

        Return
        ------
        tree : `Node`
            Tree node representing the same expression (and form) as ``self``.
        """
        raise NotImplementedError()


class Opaque(TransformationWrapper):
    """A `TransformationWrapper` implementation for tree nodes that do not need
    to be modified in boolean expression transformations.

    This includes all identifiers, literals, and operators whose arguments are
    not boolean.

    Parameters
    ----------
    node : `Node`
        Node wrapped by ``self``.
    precedence : `PrecedenceTier`
        Enumeration indicating how tightly this node is bound.
    """

    def __init__(self, node: Node, precedence: PrecedenceTier):
        self._node = node
        self._precedence = precedence

    __slots__ = ("_node", "_precedence")

    def __str__(self) -> str:
        return str(self._node)

    @property
    def precedence(self) -> PrecedenceTier:
        # Docstring inherited from `TransformationWrapper`.
        return self._precedence

    def not_(self) -> TransformationWrapper:
        # Docstring inherited from `TransformationWrapper`.
        return LogicalNot(self)

    def unwrap(self) -> Node:
        # Docstring inherited from `TransformationWrapper`.
        return self._node


class LogicalNot(TransformationWrapper):
    """A `TransformationWrapper` implementation for logical NOT operations.

    Parameters
    ----------
    operand : `TransformationWrapper`
        Wrapper representing the operand of the NOT operation.

    Notes
    -----
    Instances should always be created by calling `not_` on an existing
    `TransformationWrapper` instead of calling `LogicalNot` directly.
    `LogicalNot` should only be called directly by `Opaque.not_`.  This
    guarantees that double-negatives are simplified away and NOT operations
    are moved inside any OR and AND operations at construction.
    """

    def __init__(self, operand: Opaque):
        self._operand = operand

    __slots__ = ("_operand",)

    def __str__(self) -> str:
        return f"not({self._operand})"

    @property
    def precedence(self) -> PrecedenceTier:
        # Docstring inherited from `TransformationWrapper`.
        return PrecedenceTier.UNARY

    def not_(self) -> TransformationWrapper:
        # Docstring inherited from `TransformationWrapper`.
        return self._operand

    def unwrap(self) -> Node:
        # Docstring inherited from `TransformationWrapper`.
        node = self._operand.unwrap()
        if PrecedenceTier.needsParens(self.precedence, self._operand.precedence):
            node = Parens(node)
        return UnaryOp("NOT", node)


class LogicalBinaryOperation(TransformationWrapper):
    """A `TransformationWrapper` implementation for logical OR and NOT
    implementations.

    Parameters
    ----------
    lhs : `TransformationWrapper`
        First operand.
    operator : `LogicalBinaryOperator`
        Enumeration representing the operator.
    rhs : `TransformationWrapper`
        Second operand.
    """

    def __init__(
        self, lhs: TransformationWrapper, operator: LogicalBinaryOperator, rhs: TransformationWrapper
    ):
        self._lhs = lhs
        self._operator = operator
        self._rhs = rhs
        self._satisfiesCache: Dict[NormalForm, bool] = {}

    __slots__ = ("_lhs", "_operator", "_rhs", "_satisfiesCache")

    def __str__(self) -> str:
        return f"{self._operator.name.lower()}({self._lhs}, {self._rhs})"

    @property
    def precedence(self) -> PrecedenceTier:
        # Docstring inherited from `TransformationWrapper`.
        return BINARY_OPERATOR_PRECEDENCE[self._operator.name]

    def not_(self) -> TransformationWrapper:
        # Docstring inherited from `TransformationWrapper`.
        return LogicalBinaryOperation(
            self._lhs.not_(),
            LogicalBinaryOperator(not self._operator.value),
            self._rhs.not_(),
        )

    def satisfies(self, form: NormalForm) -> bool:
        # Docstring inherited from `TransformationWrapper`.
        r = self._satisfiesCache.get(form)
        if r is None:
            r = (
                self._lhs.satisfies(form)
                and self._rhs.satisfies(form)
                and self._lhs._satisfiesDispatch(self._operator, self._rhs, form=form)
            )
            self._satisfiesCache[form] = r
        return r

    def normalize(self, form: NormalForm) -> TransformationWrapper:
        # Docstring inherited from `TransformationWrapper`.
        if self.satisfies(form):
            return self
        lhs = self._lhs.normalize(form)
        rhs = self._rhs.normalize(form)
        return lhs._normalizeDispatch(self._operator, rhs, form=form)

    def flatten(self, operator: LogicalBinaryOperator) -> Iterator[TransformationWrapper]:
        # Docstring inherited from `TransformationWrapper`.
        if operator is self._operator:
            yield from self._lhs.flatten(operator)
            yield from self._rhs.flatten(operator)
        else:
            yield self

    def _satisfiesDispatch(
        self, operator: LogicalBinaryOperator, other: TransformationWrapper, *, form: NormalForm
    ) -> bool:
        # Docstring inherited from `TransformationWrapper`.
        return other._satisfiesDispatchBinary(operator, self._lhs, self._operator, self._rhs, form=form)

    def _normalizeDispatch(
        self, operator: LogicalBinaryOperator, other: TransformationWrapper, *, form: NormalForm
    ) -> TransformationWrapper:
        # Docstring inherited from `TransformationWrapper`.
        return other._normalizeDispatchBinary(operator, self._lhs, self._operator, self._rhs, form=form)

    def _satisfiesDispatchAtomic(
        self, operator: LogicalBinaryOperator, other: TransformationWrapper, *, form: NormalForm
    ) -> bool:
        # Docstring inherited from `TransformationWrapper`.
        return form.allows(outer=operator, inner=self._operator)

    def _normalizeDispatchAtomic(
        self, operator: LogicalBinaryOperator, other: TransformationWrapper, *, form: NormalForm
    ) -> TransformationWrapper:
        # Docstring inherited from `TransformationWrapper`.
        # Normalizes an expression of the form:
        #
        #     operator.apply(
        #         other,
        #         self._operator.apply(self._lhs, self._rhs),
        #     )
        #
        if form.allows(outer=operator, inner=self._operator):
            return operator.apply(other, self)
        else:
            return self._operator.apply(
                operator.apply(other, self._lhs).normalize(form),
                operator.apply(other, self._rhs).normalize(form),
            )

    def _satisfiesDispatchBinary(
        self,
        outer: LogicalBinaryOperator,
        lhs: TransformationWrapper,
        inner: LogicalBinaryOperator,
        rhs: TransformationWrapper,
        *,
        form: NormalForm,
    ) -> bool:
        # Docstring inherited from `TransformationWrapper`.
        return form.allows(outer=outer, inner=inner) and form.allows(outer=outer, inner=self._operator)

    def _normalizeDispatchBinary(
        self,
        outer: LogicalBinaryOperator,
        lhs: TransformationWrapper,
        inner: LogicalBinaryOperator,
        rhs: TransformationWrapper,
        *,
        form: NormalForm,
    ) -> TransformationWrapper:
        # Docstring inherited from `TransformationWrapper`.
        # Normalizes an expression of the form:
        #
        #     outer.apply(
        #         inner.apply(lhs, rhs),
        #         self._operator.apply(self._lhs, self._rhs),
        #     )
        #
        if form.allows(inner=inner, outer=outer):
            other = inner.apply(lhs, rhs)
            if form.allows(inner=self._operator, outer=outer):
                return outer.apply(other, self)
            else:
                return self._operator.apply(
                    outer.apply(other, self._lhs).normalize(form),
                    outer.apply(other, self._rhs).normalize(form),
                )
        else:
            if form.allows(inner=self._operator, outer=outer):
                return inner.apply(
                    outer.apply(lhs, self).normalize(form), outer.apply(rhs, self).normalize(form)
                )
            else:
                assert form.allows(inner=inner, outer=self._operator)
                return self._operator.apply(
                    inner.apply(
                        outer.apply(lhs, self._lhs).normalize(form),
                        outer.apply(lhs, self._rhs).normalize(form),
                    ),
                    inner.apply(
                        outer.apply(rhs, self._lhs).normalize(form),
                        outer.apply(rhs, self._rhs).normalize(form),
                    ),
                )

    def unwrap(self) -> Node:
        # Docstring inherited from `TransformationWrapper`.
        lhsNode = self._lhs.unwrap()
        if PrecedenceTier.needsParens(self.precedence, self._lhs.precedence):
            lhsNode = Parens(lhsNode)
        rhsNode = self._rhs.unwrap()
        if PrecedenceTier.needsParens(self.precedence, self._rhs.precedence):
            rhsNode = Parens(rhsNode)
        return BinaryOp(lhsNode, self._operator.name, rhsNode)


class TransformationVisitor(TreeVisitor[TransformationWrapper]):
    """A `TreeVisitor` implementation that constructs a `TransformationWrapper`
    tree when applied to a `Node` tree.
    """

    def visitNumericLiteral(self, value: str, node: Node) -> TransformationWrapper:
        # Docstring inherited from TreeVisitor.visitNumericLiteral
        return Opaque(node, PrecedenceTier.TOKEN)

    def visitStringLiteral(self, value: str, node: Node) -> TransformationWrapper:
        # Docstring inherited from TreeVisitor.visitStringLiteral
        return Opaque(node, PrecedenceTier.TOKEN)

    def visitTimeLiteral(self, value: astropy.time.Time, node: Node) -> TransformationWrapper:
        # Docstring inherited from TreeVisitor.visitTimeLiteral
        return Opaque(node, PrecedenceTier.TOKEN)

    def visitRangeLiteral(
        self, start: int, stop: int, stride: Optional[int], node: Node
    ) -> TransformationWrapper:
        # Docstring inherited from TreeVisitor.visitRangeLiteral
        return Opaque(node, PrecedenceTier.TOKEN)

    def visitIdentifier(self, name: str, node: Node) -> TransformationWrapper:
        # Docstring inherited from TreeVisitor.visitIdentifier
        return Opaque(node, PrecedenceTier.TOKEN)

    def visitUnaryOp(
        self,
        operator: str,
        operand: TransformationWrapper,
        node: Node,
    ) -> TransformationWrapper:
        # Docstring inherited from TreeVisitor.visitUnaryOp
        if operator == "NOT":
            return operand.not_()
        else:
            return Opaque(node, PrecedenceTier.UNARY)

    def visitBinaryOp(
        self,
        operator: str,
        lhs: TransformationWrapper,
        rhs: TransformationWrapper,
        node: Node,
    ) -> TransformationWrapper:
        # Docstring inherited from TreeVisitor.visitBinaryOp
        logical = LogicalBinaryOperator.__members__.get(operator)
        if logical is not None:
            return LogicalBinaryOperation(lhs, logical, rhs)
        return Opaque(node, BINARY_OPERATOR_PRECEDENCE[operator])

    def visitIsIn(
        self,
        lhs: TransformationWrapper,
        values: List[TransformationWrapper],
        not_in: bool,
        node: Node,
    ) -> TransformationWrapper:
        # Docstring inherited from TreeVisitor.visitIsIn
        return Opaque(node, PrecedenceTier.COMPARISON)

    def visitParens(self, expression: TransformationWrapper, node: Node) -> TransformationWrapper:
        # Docstring inherited from TreeVisitor.visitParens
        return expression

    def visitTupleNode(self, items: Tuple[TransformationWrapper, ...], node: Node) -> TransformationWrapper:
        # Docstring inherited from TreeVisitor.visitTupleNode
        return Opaque(node, PrecedenceTier.TOKEN)

    def visitPointNode(
        self, ra: TransformationWrapper, dec: TransformationWrapper, node: Node
    ) -> TransformationWrapper:
        # Docstring inherited from TreeVisitor.visitPointNode
        raise NotImplementedError("POINT() function is not supported yet")


class TreeReconstructionVisitor(NormalFormVisitor[Node, Node, Node]):
    """A `NormalFormVisitor` that reconstructs complete expression tree.

    Outside code should use `NormalFormExpression.toTree` (which delegates to
    this visitor) instead.
    """

    def visitBranch(self, node: Node) -> Node:
        # Docstring inherited from NormalFormVisitor.
        return node

    def _visitSequence(self, branches: Sequence[Node], operator: LogicalBinaryOperator) -> Node:
        """Common recursive implementation for `visitInner` and `visitOuter`.

        Parameters
        ----------
        branches : `Sequence`
            Sequence of return values from calls to `visitBranch`, representing
            a visited set of operands combined in the expression by
            ``operator``.
        operator : `LogicalBinaryOperator`
            Operator that joins the elements of ``branches``.

        Returns
        -------
        result
            Result of the final call to ``visitBranch(node)``.
        node : `Node`
            Hierarchical expression tree equivalent to joining ``branches``
            with ``operator``.
        """
        first, *rest = branches
        if not rest:
            return first
        merged = self._visitSequence(rest, operator)
        node = BinaryOp(first, operator.name, merged)
        return self.visitBranch(node)

    def visitInner(self, branches: Sequence[Node], form: NormalForm) -> Node:
        # Docstring inherited from NormalFormVisitor.
        node = self._visitSequence(branches, form.inner)
        if len(branches) > 1:
            node = Parens(node)
        return node

    def visitOuter(self, branches: Sequence[Node], form: NormalForm) -> Node:
        # Docstring inherited from NormalFormVisitor.
        node = self._visitSequence(branches, form.outer)
        if isinstance(node, Parens):
            node = node.expr
        return node

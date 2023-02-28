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
    "CategorizedWildcard",
    "CollectionWildcard",
    "CollectionSearch",
    "DatasetTypeWildcard",
)

import dataclasses
import re
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from typing import Any

from deprecated.sphinx import deprecated
from lsst.utils.ellipsis import Ellipsis, EllipsisType
from lsst.utils.iteration import ensure_iterable
from pydantic import BaseModel

from ..core import DatasetType
from ..core.utils import globToRegex
from ._exceptions import CollectionExpressionError, DatasetTypeExpressionError


@dataclasses.dataclass
class CategorizedWildcard:
    """The results of preprocessing a wildcard expression to separate match
    patterns from strings.

    The `fromExpression` method should almost always be used to construct
    instances, as the regular constructor performs no checking of inputs (and
    that can lead to confusing error messages downstream).
    """

    @classmethod
    def fromExpression(
        cls,
        expression: Any,
        *,
        allowAny: bool = True,
        allowPatterns: bool = True,
        coerceUnrecognized: Callable[[Any], tuple[str, Any] | str] | None = None,
        coerceItemValue: Callable[[Any], Any] | None = None,
        defaultItemValue: Any | None = None,
    ) -> CategorizedWildcard | EllipsisType:
        """Categorize a wildcard expression.

        Parameters
        ----------
        expression
            The expression to categorize.  May be any of:
             - `str` (including glob patterns if ``allowPatterns`` is `True`);
             - `re.Pattern` (only if ``allowPatterns`` is `True`);
             - objects recognized by ``coerceUnrecognized`` (if provided);
             - two-element tuples of (`str`, value) where value is recognized
               by ``coerceItemValue`` (if provided);
             - a non-`str`, non-mapping iterable containing any of the above;
             - the special value `...` (only if ``allowAny`` is `True`), which
               matches anything;
             - a mapping from `str` to a value are recognized by
               ``coerceItemValue`` (if provided);
             - a `CategorizedWildcard` instance (passed through unchanged if
               it meets the requirements specified by keyword arguments).
        allowAny: `bool`, optional
            If `False` (`True` is default) raise `TypeError` if `...` is
            encountered.
        allowPatterns: `bool`, optional
            If `False` (`True` is default) raise `TypeError` if a `re.Pattern`
            is encountered, or if ``expression`` is a `CategorizedWildcard`
            with `patterns` not empty.
        coerceUnrecognized: `Callable`, optional
            A callback that takes a single argument of arbitrary type and
            returns either a `str` - appended to `strings` - or a `tuple` of
            (`str`, `Any`) to be appended to `items`.  This will be called on
            objects of unrecognized type. Exceptions will be reraised as
            `TypeError` (and chained).
        coerceItemValue: `Callable`, optional
            If provided, ``expression`` may be a mapping from `str` to any
            type that can be passed to this function; the result of that call
            will be stored instead as the value in ``self.items``.
        defaultItemValue: `Any`, optional
            If provided, combine this value with any string values encountered
            (including any returned by ``coerceUnrecognized``) to form a
            `tuple` and add it to `items`, guaranteeing that `strings` will be
            empty.  Patterns are never added to `items`.

        Returns
        -------
        categorized : `CategorizedWildcard` or ``...``.
            The struct describing the wildcard.  ``...`` is passed through
            unchanged.

        Raises
        ------
        TypeError
            Raised if an unsupported type is found in the expression.
        """
        assert expression is not None
        # See if we were given ...; just return that if we were.
        if expression is Ellipsis:
            if not allowAny:
                raise TypeError("This expression may not be unconstrained.")
            return Ellipsis
        if isinstance(expression, cls):
            # This is already a CategorizedWildcard.  Make sure it meets the
            # reqs. implied by the kwargs we got.
            if not allowPatterns and expression.patterns:
                raise TypeError(
                    f"Regular expression(s) {expression.patterns} are not allowed in this context."
                )
            if defaultItemValue is not None and expression.strings:
                if expression.items:
                    raise TypeError(
                        "Incompatible preprocessed expression: an ordered sequence of str is "
                        "needed, but the original order was lost in the preprocessing."
                    )
                return cls(
                    strings=[],
                    patterns=expression.patterns,
                    items=[(k, defaultItemValue) for k in expression.strings],
                )
            elif defaultItemValue is None and expression.items:
                if expression.strings:
                    raise TypeError(
                        "Incompatible preprocessed expression: an ordered sequence of items is "
                        "needed, but the original order was lost in the preprocessing."
                    )
                return cls(strings=[k for k, _ in expression.items], patterns=expression.patterns, items=[])
            else:
                # Original expression was created with keyword arguments that
                # were at least as restrictive as what we just got; pass it
                # through.
                return expression

        # If we get here, we know we'll be creating a new instance.
        # Initialize an empty one now.
        self = cls(strings=[], patterns=[], items=[])

        # If mappings are allowed, see if we were given a single mapping by
        # trying to get items.
        if coerceItemValue is not None:
            rawItems = None
            try:
                rawItems = expression.items()
            except AttributeError:
                pass
            if rawItems is not None:
                for k, v in rawItems:
                    try:
                        self.items.append((k, coerceItemValue(v)))
                    except Exception as err:
                        raise TypeError(f"Could not coerce mapping value '{v}' for key '{k}'.") from err
                return self

        # Not ..., a CategorizedWildcard instance, or a mapping.  Just
        # process scalars or an iterable.  We put the body of the loop inside
        # a local function so we can recurse after coercion.

        def process(element: Any, alreadyCoerced: bool = False) -> EllipsisType | None:
            if isinstance(element, str):
                if defaultItemValue is not None:
                    self.items.append((element, defaultItemValue))
                    return None
                else:
                    # This returns a list but we know we only passed in
                    # single value.
                    converted = globToRegex(element)
                    if converted is Ellipsis:
                        return Ellipsis
                    element = converted[0]
                    # Let regex and ... go through to the next check
                    if isinstance(element, str):
                        self.strings.append(element)
                        return None
            if allowPatterns and isinstance(element, re.Pattern):
                self.patterns.append(element)
                return None
            if alreadyCoerced:
                try:
                    k, v = element
                except TypeError:
                    raise TypeError(
                        f"Object '{element!r}' returned by coercion function must be `str` or `tuple`."
                    ) from None
                else:
                    self.items.append((k, v))
                    return None
            if coerceItemValue is not None:
                try:
                    k, v = element
                except TypeError:
                    pass
                else:
                    if not isinstance(k, str):
                        raise TypeError(f"Item key '{k}' is not a string.")
                    try:
                        v = coerceItemValue(v)
                    except Exception as err:
                        raise TypeError(f"Could not coerce tuple item value '{v}' for key '{k}'.") from err
                    self.items.append((k, v))
                    return None
            if coerceUnrecognized is not None:
                try:
                    # This should be safe but flake8 cant tell that the
                    # function will be re-declared next function call
                    process(coerceUnrecognized(element), alreadyCoerced=True)  # noqa: F821
                except Exception as err:
                    raise TypeError(f"Could not coerce expression element '{element!r}'.") from err
            else:
                extra = "."
                if isinstance(element, re.Pattern):
                    extra = " and patterns are not allowed."
                raise TypeError(f"Unsupported object in wildcard expression: '{element!r}'{extra}")
            return None

        for element in ensure_iterable(expression):
            retval = process(element)
            if retval is Ellipsis:
                # One of the globs matched everything
                if not allowAny:
                    raise TypeError("This expression may not be unconstrained.")
                return Ellipsis
        del process
        return self

    strings: list[str]
    """Explicit string values found in the wildcard (`list` [ `str` ]).
    """

    patterns: list[re.Pattern]
    """Regular expression patterns found in the wildcard
    (`list` [ `re.Pattern` ]).
    """

    items: list[tuple[str, Any]]
    """Two-item tuples that relate string values to other objects
    (`list` [ `tuple` [ `str`, `Any` ] ]).
    """


@deprecated(
    reason="Tuples of string collection names are now preferred.  Will be removed after v26.",
    version="v25.0",
    category=FutureWarning,
)
class CollectionSearch(BaseModel, Sequence[str]):
    """An ordered search path of collections.

    The `fromExpression` method should almost always be used to construct
    instances, as the regular constructor performs no checking of inputs (and
    that can lead to confusing error messages downstream).

    Parameters
    ----------
    collections : `tuple` [ `str` ]
        Tuple of collection names, ordered from the first searched to the last
        searched.

    Notes
    -----
    A `CollectionSearch` is used to find a single dataset (or set of datasets
    with different dataset types or data IDs) according to its dataset type and
    data ID, giving preference to collections in the order in which they are
    specified.  A `CollectionWildcard` can be constructed from a broader range
    of expressions but does not order the collections to be searched.

    `CollectionSearch` is an immutable sequence of `str` collection names.

    A `CollectionSearch` instance constructed properly (e.g. via
    `fromExpression`) is a unique representation of a particular search path;
    it is exactly the same internally and compares as equal to any
    `CollectionSearch` constructed from an equivalent expression, regardless of
    how different the original expressions appear.
    """

    __root__: tuple[str, ...]

    @classmethod
    def fromExpression(cls, expression: Any) -> CollectionSearch:
        """Process a general expression to construct a `CollectionSearch`
        instance.

        Parameters
        ----------
        expression
            May be:
             - a `str` collection name;
             - an iterable of `str` collection names;
             - another `CollectionSearch` instance (passed through
               unchanged).

            Duplicate entries will be removed (preserving the first appearance
            of each collection name).

        Returns
        -------
        collections : `CollectionSearch`
            A `CollectionSearch` instance.
        """
        # First see if this is already a CollectionSearch; just pass that
        # through unchanged.  This lets us standardize expressions (and turn
        # single-pass iterators into multi-pass iterables) in advance and pass
        # them down to other routines that accept arbitrary expressions.
        if isinstance(expression, cls):
            return expression
        try:
            wildcard = CategorizedWildcard.fromExpression(
                expression,
                allowAny=False,
                allowPatterns=False,
            )
        except TypeError as err:
            raise CollectionExpressionError(str(err)) from None
        assert wildcard is not Ellipsis
        assert not wildcard.patterns
        assert not wildcard.items
        deduplicated = []
        for name in wildcard.strings:
            if name not in deduplicated:
                deduplicated.append(name)
        return cls(__root__=tuple(deduplicated))

    def explicitNames(self) -> Iterator[str]:
        """Iterate over collection names that were specified explicitly."""
        yield from self.__root__

    def __iter__(self) -> Iterator[str]:  # type: ignore
        yield from self.__root__

    def __len__(self) -> int:
        return len(self.__root__)

    def __getitem__(self, index: Any) -> str:
        return self.__root__[index]

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, CollectionSearch):
            return self.__root__ == other.__root__
        return False

    def __str__(self) -> str:
        return "[{}]".format(", ".join(self))

    def __repr__(self) -> str:
        return f"CollectionSearch({self.__root__!r})"


@dataclasses.dataclass(frozen=True)
class CollectionWildcard:
    """A validated wildcard for collection names

    The `from_expression` method should almost always be used to construct
    instances, as the regular constructor performs no checking of inputs (and
    that can lead to confusing error messages downstream).

    Notes
    -----
    `CollectionWildcard` is expected to be rarely used outside of `Registry`
    (which uses it to back several of its "query" methods that take general
    expressions for collections), but it may occasionally be useful outside
    `Registry` as a way to preprocess expressions that contain single-pass
    iterators into a form that can be used to call those `Registry` methods
    multiple times.
    """

    strings: tuple[str, ...] = ()
    """An an ordered list of explicitly-named collections. (`tuple` [ `str` ]).
    """

    patterns: tuple[re.Pattern, ...] | EllipsisType = Ellipsis
    """Regular expression patterns to match against collection names, or the
    special value ``...`` indicating all collections.

    `...` must be accompanied by ``strings=()``.
    """

    def __post_init__(self) -> None:
        if self.patterns is Ellipsis and self.strings:
            raise ValueError(
                f"Collection wildcard matches any string, but still has explicit strings {self.strings}."
            )

    @classmethod
    def from_expression(cls, expression: Any, require_ordered: bool = False) -> CollectionWildcard:
        """Process a general expression to construct a `CollectionWildcard`
        instance.

        Parameters
        ----------
        expression
            May be:
             - a `str` collection name;
             - an `re.Pattern` instance to match (with `re.Pattern.fullmatch`)
               against collection names;
             - any iterable containing any of the above;
             - another `CollectionWildcard` instance (passed through
               unchanged).

            Duplicate collection names will be removed (preserving the first
            appearance of each collection name).
        require_ordered : `bool`, optional
            If `True` (`False` is default) require the expression to be
            ordered, and raise `CollectionExpressionError` if it is not.

        Returns
        -------
        wildcard : `CollectionWildcard`
            A `CollectionWildcard` instance.

        Raises
        ------
        CollectionExpressionError
            Raised if the patterns has regular expression, glob patterns, or
            the ``...`` wildcard, and ``require_ordered=True``.
        """
        if isinstance(expression, cls):
            return expression
        if expression is Ellipsis:
            return cls()
        wildcard = CategorizedWildcard.fromExpression(
            expression,
            allowAny=True,
            allowPatterns=True,
        )
        if wildcard is Ellipsis:
            return cls()
        result = cls(
            strings=tuple(wildcard.strings),
            patterns=tuple(wildcard.patterns),
        )
        if require_ordered:
            result.require_ordered()
        return result

    @classmethod
    def from_names(cls, names: Iterable[str]) -> CollectionWildcard:
        """Construct from an iterable of explicit collection names.

        Parameters
        ----------
        names : `Iterable` [ `str` ]
            Iterable of collection names.

        Returns
        -------
        wildcard : ~CollectionWildcard`
            A `CollectionWildcard` instance.  `require_ordered` is guaranteed
            to succeed and return the given names in order.
        """
        return cls(strings=tuple(names), patterns=())

    def require_ordered(self) -> tuple[str, ...]:
        """Require that this wildcard contains no patterns, and return the
        ordered tuple of names that it does hold.

        Returns
        -------
        names : `tuple` [ `str` ]
            Ordered tuple of collection names.

        Raises
        ------
        CollectionExpressionError
            Raised if the patterns has regular expression, glob patterns, or
            the ``...`` wildcard.
        """
        if self.patterns:
            raise CollectionExpressionError(
                f"An ordered collection expression is required; got patterns {self.patterns}."
            )
        return self.strings

    def __str__(self) -> str:
        if self.patterns is Ellipsis:
            return "..."
        else:
            terms = list(self.strings)
            terms.extend(str(p) for p in self.patterns)
            return "[{}]".format(", ".join(terms))


@dataclasses.dataclass
class DatasetTypeWildcard:
    """A validated expression that resolves to one or more dataset types.

    The `from_expression` method should almost always be used to construct
    instances, as the regular constructor performs no checking of inputs (and
    that can lead to confusing error messages downstream).
    """

    values: Mapping[str, DatasetType | None] = dataclasses.field(default_factory=dict)
    """A mapping with `str` dataset type name keys and optional `DatasetType`
    instances.
    """

    patterns: tuple[re.Pattern, ...] | EllipsisType = Ellipsis
    """Regular expressions to be matched against dataset type names, or the
    special value ``...`` indicating all dataset types.

    Any pattern matching a dataset type is considered an overall match for
    the expression.
    """

    @classmethod
    def from_expression(cls, expression: Any) -> DatasetTypeWildcard:
        """Construct an instance by analyzing the given expression.

        Parameters
        ----------
        expression
            Expression to analyze.  May be any of the following:

            - a `str` dataset type name;
            - a `DatasetType` instance;
            - a `re.Pattern` to match against dataset type names;
            - an iterable whose elements may be any of the above (any dataset
              type matching any element in the list is an overall match);
            - an existing `DatasetTypeWildcard` instance;
            - the special ``...`` ellipsis object, which matches any dataset
              type.

        Returns
        -------
        query : `DatasetTypeWildcard`
            An instance of this class (new unless an existing instance was
            passed in).

        Raises
        ------
        DatasetTypeExpressionError
            Raised if the given expression does not have one of the allowed
            types.
        """
        if isinstance(expression, cls):
            return expression
        try:
            wildcard = CategorizedWildcard.fromExpression(
                expression, coerceUnrecognized=lambda d: (d.name, d)
            )
        except TypeError as err:
            raise DatasetTypeExpressionError(f"Invalid dataset type expression: {expression!r}.") from err
        if wildcard is Ellipsis:
            return cls()
        values: dict[str, DatasetType | None] = {}
        for name in wildcard.strings:
            values[name] = None
        for name, item in wildcard.items:
            if not isinstance(item, DatasetType):
                raise DatasetTypeExpressionError(
                    f"Invalid value '{item}' of type {type(item)} in dataset type expression; "
                    "expected str, re.Pattern, DatasetType objects, iterables thereof, or '...'."
                )
            values[name] = item
        return cls(values, patterns=tuple(wildcard.patterns))

    def __str__(self) -> str:
        if self.patterns is Ellipsis:
            return "..."
        else:
            terms = list(self.values.keys())
            terms.extend(str(p) for p in self.patterns)
            return "[{}]".format(", ".join(terms))

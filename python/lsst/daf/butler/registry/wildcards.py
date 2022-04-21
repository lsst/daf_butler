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

from lsst.daf.butler.core.named import NamedValueAbstractSet

__all__ = (
    "CategorizedWildcard",
    "CollectionWildcard",
    "CollectionSearch",
    "DatasetTypeWildcard",
)

import logging
import re
from collections import defaultdict
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

import sqlalchemy
from lsst.utils.iteration import ensure_iterable
from lsst.utils.sets.ellipsis import Ellipsis, EllipsisType
from pydantic import BaseModel

from ..core import DatasetType
from ..core.utils import globToRegex
from ._collectionType import CollectionType
from ._exceptions import DatasetTypeError, DatasetTypeExpressionError, MissingCollectionError

if TYPE_CHECKING:
    from .interfaces import CollectionRecord


_LOG = logging.getLogger(__name__)


@dataclass
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
        coerceUnrecognized: Optional[Callable[[Any], Union[Tuple[str, Any], str]]] = None,
        coerceItemValue: Optional[Callable[[Any], Any]] = None,
        defaultItemValue: Optional[Any] = None,
    ) -> Union[CategorizedWildcard, EllipsisType]:
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
            objects of unrecognized type, with the return value added to
            `strings`. Exceptions will be reraised as `TypeError` (and
            chained).
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

        def process(element: Any, alreadyCoerced: bool = False) -> Union[EllipsisType, None]:
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
            if coerceItemValue is not None:
                try:
                    k, v = element
                except TypeError:
                    pass
                else:
                    if not alreadyCoerced:
                        if not isinstance(k, str):
                            raise TypeError(f"Item key '{k}' is not a string.")
                        try:
                            v = coerceItemValue(v)
                        except Exception as err:
                            raise TypeError(
                                f"Could not coerce tuple item value '{v}' for key '{k}'."
                            ) from err
                    self.items.append((k, v))
                    return None
            if alreadyCoerced:
                raise TypeError(f"Object '{element!r}' returned by coercion function is still unrecognized.")
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

    def makeWhereExpression(
        self, column: sqlalchemy.sql.ColumnElement
    ) -> Optional[sqlalchemy.sql.ColumnElement]:
        """Transform the wildcard into a SQLAlchemy boolean expression suitable
        for use in a WHERE clause.

        Parameters
        ----------
        column : `sqlalchemy.sql.ColumnElement`
            A string column in a table or query that should be compared to the
            wildcard expression.

        Returns
        -------
        where : `sqlalchemy.sql.ColumnElement` or `None`
            A boolean SQL expression that evaluates to true if and only if
            the value of ``column`` matches the wildcard.  `None` is returned
            if both `strings` and `patterns` are empty, and hence no match is
            possible.
        """
        if self.items:
            raise NotImplementedError(
                "Expressions that are processed into items cannot be transformed "
                "automatically into queries."
            )
        if self.patterns:
            raise NotImplementedError("Regular expression patterns are not yet supported here.")
        terms = []
        if len(self.strings) == 1:
            terms.append(column == self.strings[0])
        elif len(self.strings) > 1:
            terms.append(column.in_(self.strings))
        # TODO: append terms for regular expressions
        if not terms:
            return None
        return sqlalchemy.sql.or_(*terms)

    strings: List[str]
    """Explicit string values found in the wildcard (`list` [ `str` ]).
    """

    patterns: List[re.Pattern]
    """Regular expression patterns found in the wildcard
    (`list` [ `re.Pattern` ]).
    """

    items: List[Tuple[str, Any]]
    """Two-item tuples that relate string values to other objects
    (`list` [ `tuple` [ `str`, `Any` ] ]).
    """


def _yieldCollectionRecords(
    all_records: NamedValueAbstractSet[CollectionRecord],
    record: CollectionRecord,
    collectionTypes: AbstractSet[CollectionType] = CollectionType.all(),
    done: Optional[Set[str]] = None,
    flattenChains: bool = True,
    includeChains: Optional[bool] = None,
) -> Iterator[CollectionRecord]:
    """A helper function containing common logic for `CollectionSearch.iter`
    and `CollectionWildcard.iter`: recursively yield `CollectionRecord` only if
    they match the criteria given in other arguments.

    Parameters
    ----------
    all_records : `NamedValueAbstractSet[CollectionRecord]`
        Object responsible for managing the collection tables in a `Registry`.
    record : `CollectionRecord`
        Record to conditionally yield.
    collectionTypes : `AbstractSet` [ `CollectionType` ], optional
        If provided, only yield collections of these types.
    done : `set` [ `str` ], optional
        A `set` of already-yielded collection names; if provided, ``record``
        will only be yielded if it is not already in ``done``, and ``done``
        will be updated to include it on return.
    flattenChains : `bool`, optional
        If `True` (default) recursively yield the child collections of
        `~CollectionType.CHAINED` collections.
    includeChains : `bool`, optional
        If `False`, return records for `~CollectionType.CHAINED` collections
        themselves.  The default is the opposite of ``flattenChains``: either
        return records for CHAINED collections or their children, but not both.

    Yields
    ------
    record : `CollectionRecord`
        Matching collection records.
    """
    if done is None:
        done = set()
    includeChains = includeChains if includeChains is not None else not flattenChains
    if record.type in collectionTypes:
        done.add(record.name)
        if record.type is not CollectionType.CHAINED or includeChains:
            yield record
    if flattenChains and record.type is CollectionType.CHAINED:
        done.add(record.name)
        # We know this is a ChainedCollectionRecord because of the enum value,
        # but MyPy doesn't.
        yield from record.children.iter(  # type: ignore
            all_records,
            collectionTypes=collectionTypes,
            done=done,
            flattenChains=flattenChains,
            includeChains=includeChains,
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

    __root__: Tuple[str, ...]

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
        wildcard = CategorizedWildcard.fromExpression(
            expression,
            allowAny=False,
            allowPatterns=False,
        )
        assert wildcard is not Ellipsis
        assert not wildcard.patterns
        assert not wildcard.items
        deduplicated = []
        for name in wildcard.strings:
            if name not in deduplicated:
                deduplicated.append(name)
        return cls(__root__=tuple(deduplicated))

    def iter(
        self,
        all_records: NamedValueAbstractSet[CollectionRecord],
        *,
        datasetType: Optional[DatasetType] = None,
        collectionTypes: AbstractSet[CollectionType] = CollectionType.all(),
        done: Optional[Set[str]] = None,
        flattenChains: bool = True,
        includeChains: Optional[bool] = None,
    ) -> Iterator[CollectionRecord]:
        """Iterate over collection records that match this instance and the
        given criteria, in order.

        This method is primarily intended for internal use by `Registry`;
        other callers should generally prefer `Registry.findDatasets` or
        other `Registry` query methods.

        Parameters
        ----------
        all_records : `NamedValueAbstractSet[CollectionRecord]`
            Object responsible for managing the collection tables in a
            `Registry`.
        collectionTypes : `AbstractSet` [ `CollectionType` ], optional
            If provided, only yield collections of these types.
        done : `set`, optional
            A `set` containing the names of all collections already yielded;
            any collections whose names are already present in this set will
            not be yielded again, and those yielded will be added to it while
            iterating.  If not provided, an empty `set` will be created and
            used internally to avoid duplicates.
        flattenChains : `bool`, optional
            If `True` (default) recursively yield the child collections of
            `~CollectionType.CHAINED` collections.
        includeChains : `bool`, optional
            If `False`, return records for `~CollectionType.CHAINED`
            collections themselves.  The default is the opposite of
            ``flattenChains``: either return records for CHAINED collections or
            their children, but not both.

        Yields
        ------
        record : `CollectionRecord`
            Matching collection records.

        Raises
        ------
        MissingCollectionError
            Raised if the given collection does not exist.

        """
        if done is None:
            done = set()
        for name in self:
            if name not in done:
                try:
                    record = all_records[name]
                except KeyError:
                    raise MissingCollectionError(f"No collection with name '{name}' found.") from None
                yield from _yieldCollectionRecords(
                    all_records,
                    record,
                    collectionTypes=collectionTypes,
                    done=done,
                    flattenChains=flattenChains,
                    includeChains=includeChains,
                )

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


class CollectionWildcard:
    """An unordered query for collections and dataset type restrictions.

    The `fromExpression` method should almost always be used to construct
    instances, as the regular constructor performs no checking of inputs (and
    that can lead to confusing error messages downstream).

    Parameters
    ----------
    search : `CollectionSearch` or `...`
        An object representing an ordered search for explicitly-named
        collections (to be interpreted here as unordered), or the special
        value `...` indicating all collections.  `...` must be accompanied
        by ``patterns=None``.
    patterns : `tuple` of `re.Pattern`
        Regular expression patterns to match against collection names.

    Notes
    -----
    A `CollectionWildcard` is used to find all matching datasets in any number
    of collections, or to find collections themselves.

    `CollectionWildcard` is expected to be rarely used outside of `Registry`
    (which uses it to back several of its "query" methods that take general
    expressions for collections), but it may occassionally be useful outside
    `Registry` as a way to preprocess expressions that contain single-pass
    iterators into a form that can be used to call those `Registry` methods
    multiple times.
    """

    def __init__(
        self,
        search: Union[CollectionSearch, EllipsisType] = Ellipsis,
        patterns: Tuple[re.Pattern, ...] = (),
    ):
        self._search = search
        self._patterns = patterns

    __slots__ = ("_search", "_patterns")

    @classmethod
    def fromExpression(cls, expression: Any) -> CollectionWildcard:
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
             - a `CollectionSearch` instance;
             - another `CollectionWildcard` instance (passed through
               unchanged).

            Duplicate collection names will be removed (preserving the first
            appearance of each collection name).

        Returns
        -------
        collections : `CollectionWildcard`
            A `CollectionWildcard` instance.
        """
        if isinstance(expression, cls):
            return expression
        if expression is Ellipsis:
            return cls()
        if isinstance(expression, CollectionSearch):
            return cls(search=expression, patterns=())
        wildcard = CategorizedWildcard.fromExpression(
            expression,
            allowAny=True,
            allowPatterns=True,
        )
        if wildcard is Ellipsis:
            return cls()
        assert (
            not wildcard.items
        ), "We should no longer be transforming to (str, DatasetTypeRestriction) tuples."
        return cls(
            search=CollectionSearch.fromExpression(wildcard.strings),
            patterns=tuple(wildcard.patterns),
        )

    def iter(
        self,
        all_records: NamedValueAbstractSet[CollectionRecord],
        *,
        collectionTypes: AbstractSet[CollectionType] = CollectionType.all(),
        flattenChains: bool = True,
        includeChains: Optional[bool] = None,
    ) -> Iterator[CollectionRecord]:
        """Iterate over collection records that match this instance and the
        given criteria, in an arbitrary order.

        This method is primarily intended for internal use by `Registry`;
        other callers should generally prefer `Registry.queryDatasets` or
        other `Registry` query methods.

        Parameters
        ----------
        all_records : `NamedValueAbstractSet[CollectionRecord]`
            Object responsible for managing the collection tables in a
            `Registry`.
        collectionTypes : `AbstractSet` [ `CollectionType` ], optional
            If provided, only yield collections of these types.
        flattenChains : `bool`, optional
            If `True` (default) recursively yield the child collections of
            `~CollectionType.CHAINED` collections.
        includeChains : `bool`, optional
            If `False`, return records for `~CollectionType.CHAINED`
            collections themselves.  The default is the opposite of
            ``flattenChains``: either return records for CHAINED collections or
            their children, but not both.

        Yields
        ------
        record : `CollectionRecord`
            Matching collection records.
        """
        if self._search is Ellipsis:
            for record in all_records:
                yield from _yieldCollectionRecords(
                    all_records,
                    record,
                    collectionTypes=collectionTypes,
                    flattenChains=flattenChains,
                    includeChains=includeChains,
                )
        else:
            done: Set[str] = set()
            yield from self._search.iter(
                all_records,
                collectionTypes=collectionTypes,
                done=done,
                flattenChains=flattenChains,
                includeChains=includeChains,
            )
            for record in all_records:
                if record.name not in done and any(p.fullmatch(record.name) for p in self._patterns):
                    yield from _yieldCollectionRecords(
                        all_records,
                        record,
                        collectionTypes=collectionTypes,
                        done=done,
                        flattenChains=flattenChains,
                        includeChains=includeChains,
                    )

    def explicitNames(self) -> Iterator[str]:
        """Iterate over collection names that were specified explicitly."""
        if isinstance(self._search, CollectionSearch):
            yield from self._search.explicitNames()

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, CollectionWildcard):
            return self._search == other._search and self._patterns == other._patterns
        else:
            return False

    def __str__(self) -> str:
        if self._search is Ellipsis:
            return "..."
        else:
            terms = list(self._search)
            terms.extend(str(p) for p in self._patterns)
            return "[{}]".format(", ".join(terms))

    def __repr__(self) -> str:
        return f"CollectionWildcard({self._search!r}, {self._patterns!r})"

    def union(*args: CollectionWildcard) -> CollectionWildcard:
        """Return a new `CollectionWildcard` that matches any collection
        matched by any of the given `CollectionWildcard` objects.

        Parameters
        ----------
        *others : `CollectionWildcard`
            Expressions to merge.

        Returns
        -------
        merged : `CollectionWildcard`
            Union expression.
        """
        names: Set[str] = set()
        patterns: Set[re.Pattern] = set()
        for q in args:
            if q._search is Ellipsis:
                return q
            names.update(q._search)
            patterns.update(q._patterns)
        return CollectionWildcard(CollectionSearch.fromExpression(names), tuple(patterns))


class DatasetTypeWildcard:
    """A validated expression that resolves to one or more dataset types.

    The `fromExpression` method should almost always be used to construct
    instances, as the regular constructor performs no checking of inputs (and
    that can lead to confusing error messages downstream).

    Parameters
    ----------
    explicit : `Mapping` [ `str`, `DatasetType` or `None` ] or ``...``
        Either a mapping with `str` dataset type name keys and optional
        `DatasetType` instances, or the special value ``...`` for an expression
        that matches any dataset type.
    patterns : `tuple` [ `re.Pattern` ]
        Regular expressions to be matched against dataset type names; any
        pattern matching the dataset type is considered an overall match for
        the expression.
    """

    def __init__(
        self,
        explicit: Union[Mapping[str, Optional[DatasetType]], EllipsisType] = Ellipsis,
        patterns: Tuple[re.Pattern, ...] = (),
    ):
        self._explicit = explicit
        self._patterns = patterns

    __slots__ = ("_explicit", "_patterns")

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
            - an existing `DatasetTypeWildcard instance;
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
            wildcard = CategorizedWildcard.fromExpression(expression, coerceUnrecognized=lambda d: d.name)
        except TypeError as err:
            raise DatasetTypeExpressionError(f"Invalid dataset type expression: {expression!r}.") from err
        if wildcard is Ellipsis:
            return cls()
        explicit: Dict[str, Optional[DatasetType]] = {}
        for name in wildcard.strings:
            explicit[name] = None
        for name, item in wildcard.items:
            if not isinstance(item, DatasetType):
                raise DatasetTypeExpressionError(
                    f"Invalid value '{item}' of type {type(item)} in dataset type expression; "
                    "expected str, re.Pattern, DatasetType objects, iterables thereof, or '...'"
                )
            explicit[name] = item
        return cls(explicit, patterns=tuple(wildcard.patterns))

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, DatasetTypeWildcard):
            return self._explicit == other._explicit and self._patterns == other._patterns
        else:
            return False

    def __str__(self) -> str:
        if self._explicit is Ellipsis:
            return "..."
        else:
            terms = list(self._explicit)
            terms.extend(str(p) for p in self._patterns)
            return "[{}]".format(", ".join(terms))

    def resolve_dataset_types(
        self,
        candidate_parents: NamedValueAbstractSet[DatasetType],
        components: Optional[bool] = None,
        missing: Optional[List[str]] = None,
    ) -> Iterator[DatasetType]:
        """Apply the expression to a set of `DatasetType` instances and yield
        those that match.

        Parameters
        ----------
        candidate_parents : `NamedValueAbstractSet` [ `DatasetType` ]
            A set-like object containing the parent `DatasetType` instances to
            check the expression against.
        components : `bool`, optional
            If `True`, yield component dataset types that match the expression.
            If `False,` do not yield any component dataset types.  If `None`,
            yield only component dataset types whose full names are given
            explicitly, or whose parents did not match the expression.
        missing : `list` [ `str` ], optional
            A `list` of full dataset type names that were given explicitly
            (i.e. as `str` or `DatasetType` instances) that were not found.

        Notes
        -----
        DatasetTypeError
            Raised if a `DatasetType` instance was given, but the match dataset
            type with the same name had a different definition.
        """
        if self._explicit is Ellipsis:
            for parent in candidate_parents:
                yield parent
                if components:
                    try:
                        yield from parent.makeAllComponentDatasetTypes()
                    except KeyError as err:
                        _LOG.warning(
                            f"Could not load storage class {err} for {parent.name}; "
                            "if it has components they will not be included in query results."
                        )
            return
        done: Set[DatasetType] = set()
        for name, dataset_type in self._explicit.items():
            parent_name, component_name = DatasetType.splitDatasetTypeName(name)
            if (found := candidate_parents.get(parent_name)) is not None:
                if component_name is not None:
                    found = found.makeComponentDatasetType(component_name)
                if dataset_type is not None and found != dataset_type:
                    # TODO: allow dataset types with compatible storage types.
                    raise DatasetTypeError(
                        f"Dataset type definition in query expression {dataset_type} does "
                        f"not match the registered type {found}."
                    )
                done.add(found)
                yield found
            elif missing is not None:
                missing.append(name)
        if self._patterns:
            for parent in candidate_parents:
                if parent not in done and any(p.fullmatch(parent.name) for p in self._patterns):
                    done.add(parent)
                    yield parent
            if components is not False:
                for parent in candidate_parents:
                    if components is None and parent in done:
                        continue
                    try:
                        components_for_parent = parent.makeAllComponentDatasetTypes()
                    except KeyError as err:
                        _LOG.warning(
                            f"Could not load storage class {err} for {parent.name}; "
                            "if it has components they will not be included in query results."
                        )
                        continue
                    for component in components_for_parent:
                        if component not in done and any(p.fullmatch(component.name) for p in self._patterns):
                            done.add(component)
                            yield component

    def union(*args: DatasetTypeWildcard) -> DatasetTypeWildcard:
        """Return a new `DatasetTypeWildcard` that matches any dataset type
        matched by the given `DatasetTypeWildcard` objects.

        Parameters
        ----------
        *args : `DatasetTypeWildcard`
            Expressions to merge with ``self``.

        Returns
        -------
        merged : `DatasetTypeWildcard`
            Union expression.
        """
        explicit_all: defaultdict[str, set[Optional[DatasetType]]] = defaultdict(set)
        patterns: Set[re.Pattern] = set()
        for q in args:
            if q._explicit is Ellipsis:
                return q
            for name, dataset_type in q._explicit.items():
                explicit_all[name].add(dataset_type)
            patterns.update(q._patterns)
        explicit: dict[str, Optional[DatasetType]] = {}
        for name, dataset_types in explicit_all.items():
            dataset_types.discard(None)
            if not dataset_types:
                explicit[name] = None
            else:
                (first, *rest) = dataset_types
                if not rest:
                    explicit[name] = first
                else:
                    raise DatasetTypeError(
                        f"Cannot merge dataset type expressions with different definitions for {name}: "
                        f"{rest}."
                    )
        return DatasetTypeWildcard(explicit, tuple(patterns))

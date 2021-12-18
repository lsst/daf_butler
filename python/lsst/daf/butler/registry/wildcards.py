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
    "CollectionQuery",
    "CollectionSearch",
)

import re
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Any,
    Callable,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

import sqlalchemy
from lsst.utils.iteration import ensure_iterable
from pydantic import BaseModel

from ..core import DatasetType
from ..core.utils import globToRegex
from ._collectionType import CollectionType

if TYPE_CHECKING:
    # Workaround for `...` not having an exposed type in Python, borrowed from
    # https://github.com/python/typing/issues/684#issuecomment-548203158
    # Along with that, we need to either use `Ellipsis` instead of `...` for
    # the actual sentinal value internally, and tell MyPy to ignore conversions
    # from `...` to `Ellipsis` at the public-interface boundary.
    #
    # `Ellipsis` and `EllipsisType` should be directly imported from this
    # module by related code that needs them; hopefully that will stay confined
    # to `lsst.daf.butler.registry`.  Putting these in __all__ is bad for
    # Sphinx, and probably more confusing than helpful overall.
    from enum import Enum

    from .interfaces import CollectionManager, CollectionRecord

    class EllipsisType(Enum):
        Ellipsis = "..."

    Ellipsis = EllipsisType.Ellipsis

else:
    EllipsisType = type(Ellipsis)
    Ellipsis = Ellipsis


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
    manager: CollectionManager,
    record: CollectionRecord,
    collectionTypes: AbstractSet[CollectionType] = CollectionType.all(),
    done: Optional[Set[str]] = None,
    flattenChains: bool = True,
    includeChains: Optional[bool] = None,
) -> Iterator[CollectionRecord]:
    """A helper function containing common logic for `CollectionSearch.iter`
    and `CollectionQuery.iter`: recursively yield `CollectionRecord` only if
    they match the criteria given in other arguments.

    Parameters
    ----------
    manager : `CollectionManager`
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
            manager,
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
    specified.  A `CollectionQuery` can be constructed from a broader range of
    expressions but does not order the collections to be searched.

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
        manager: CollectionManager,
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
        manager : `CollectionManager`
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
        """
        if done is None:
            done = set()
        for name in self:
            if name not in done:
                yield from _yieldCollectionRecords(
                    manager,
                    manager.find(name),
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


class CollectionQuery:
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
    universe : `DimensionUniverse`
        Object managing all dimensions.

    Notes
    -----
    A `CollectionQuery` is used to find all matching datasets in any number
    of collections, or to find collections themselves.

    `CollectionQuery` is expected to be rarely used outside of `Registry`
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
    def fromExpression(cls, expression: Any) -> CollectionQuery:
        """Process a general expression to construct a `CollectionQuery`
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
             - another `CollectionQuery` instance (passed through unchanged).

            Duplicate collection names will be removed (preserving the first
            appearance of each collection name).

        Returns
        -------
        collections : `CollectionQuery`
            A `CollectionQuery` instance.
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
        manager: CollectionManager,
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
        manager : `CollectionManager`
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
            for record in manager:
                yield from _yieldCollectionRecords(
                    manager,
                    record,
                    collectionTypes=collectionTypes,
                    flattenChains=flattenChains,
                    includeChains=includeChains,
                )
        else:
            done: Set[str] = set()
            yield from self._search.iter(
                manager,
                collectionTypes=collectionTypes,
                done=done,
                flattenChains=flattenChains,
                includeChains=includeChains,
            )
            for record in manager:
                if record.name not in done and any(p.fullmatch(record.name) for p in self._patterns):
                    yield from _yieldCollectionRecords(
                        manager,
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
        if isinstance(other, CollectionQuery):
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
        return f"CollectionQuery({self._search!r}, {self._patterns!r})"

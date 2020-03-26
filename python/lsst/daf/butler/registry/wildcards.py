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

__all__ = ["CategorizedWildcard", "CollectionQuery", "CollectionSearch", "DatasetTypeRestriction"]


from dataclasses import dataclass
import itertools
import operator
import re
from typing import (
    Any,
    Callable,
    FrozenSet,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    TYPE_CHECKING,
    Union,
)

import sqlalchemy

from ..core import DatasetType
from ..core.utils import iterable
from ._collectionType import CollectionType

if TYPE_CHECKING:
    from .interfaces import CollectionManager, CollectionRecord


@dataclass
class CategorizedWildcard:
    """The results of preprocessing a wildcard expression to separate match
    patterns from strings.

    The `fromExpression` method should almost always be used to construct
    instances, as the regular constructor performs no checking of inputs (and
    that can lead to confusing error messages downstream).
    """

    @classmethod
    def fromExpression(cls, expression: Any, *,
                       allowAny: bool = True,
                       allowPatterns: bool = True,
                       coerceUnrecognized: Optional[Callable[[Any], Union[Tuple[str, Any], str]]] = None,
                       coerceItemValue: Optional[Callable[[Any], Any]] = None,
                       defaultItemValue: Optional[Any] = None,
                       ) -> Union[CategorizedWildcard, type(...)]:
        """Categorize a wildcard expression.

        Parameters
        ----------
        expression
            The expression to categorize.  May be any of:
             - `str`;
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
        if expression is ...:
            if not allowAny:
                raise TypeError("This expression may not be unconstrained.")
            return ...
        if isinstance(expression, cls):
            # This is already a CategorizedWildcard.  Make sure it meets the
            # reqs. implied by the kwargs we got.
            if not allowPatterns and expression.patterns:
                raise TypeError(f"Regular expression(s) {expression.patterns} "
                                f"are not allowed in this context.")
            if defaultItemValue is not None and expression.strings:
                if expression.items:
                    raise TypeError("Incompatible preprocessed expression: an ordered sequence of str is "
                                    "needed, but the original order was lost in the preprocessing.")
                return cls(strings=[], patterns=expression.patterns,
                           items=[(k, defaultItemValue) for k in expression.strings])
            elif defaultItemValue is None and expression.items:
                if expression.strings:
                    raise TypeError("Incompatible preprocessed expression: an ordered sequence of items is "
                                    "needed, but the original order was lost in the preprocessing.")
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

        def process(element: Any, alreadyCoerced: bool = False):
            if isinstance(element, str):
                if defaultItemValue is not None:
                    self.items.append((element, defaultItemValue))
                else:
                    self.strings.append(element)
                return
            if allowPatterns and isinstance(element, re.Pattern):
                self.patterns.append(element)
                return
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
                            raise TypeError(f"Could not coerce tuple item value '{v}' for key '{k}'."
                                            ) from err
                    self.items.append((k, v))
                    return
            if alreadyCoerced:
                raise TypeError(f"Object '{element}' returned by coercion function is still unrecognized.")
            if coerceUnrecognized is not None:
                try:
                    process(coerceUnrecognized(element), alreadyCoerced=True)
                except Exception as err:
                    raise TypeError(f"Could not coerce expression element '{element}'.") from err
            else:
                raise TypeError(f"Unsupported object in wildcard expression: '{element}'.")

        for element in iterable(expression):
            process(element)
        return self

    def makeWhereExpression(self, column: sqlalchemy.sql.ColumnElement
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
            raise NotImplementedError("Expressions that are processed into items cannot be transformed "
                                      "automatically into queries.")
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


class DatasetTypeRestriction:
    """An immutable set-like object that represents a restriction on the
    dataset types to search for within a collection.

    The `fromExpression` method should almost always be used to construct
    instances, as the regular constructor performs no checking of inputs (and
    that can lead to confusing error messages downstream).

    Parameters
    ----------
    names : `frozenset` [`str`] or `...`
        The names of the dataset types included in the restriction, or `...`
        to permit a search for any dataset type.

    Notes
    -----
    This class does not inherit from `collections.abc.Set` (and does not
    implement the full set interface) because is not always iterable and
    sometimes has no length (i.e. when ``names`` is ``...``).
    """
    def __init__(self, names: Union[FrozenSet[str], type(...)]):
        self.names = names

    __slots__ = ("names",)

    @classmethod
    def fromExpression(cls, expression: Any) -> DatasetTypeRestriction:
        """Process a general expression to construct a `DatasetTypeRestriction`
        instance.

        Parameters
        ----------
        expression
            May be:
             - a `DatasetType` instance;
             - a `str` dataset type name;
             - any non-mapping iterable containing either of the above;
             - the special value `...`;
             - another `DatasetTypeRestriction` instance (passed through
               unchanged).

        Returns
        -------
        restriction : `DatasetTypeRestriction`
            A `DatasetTypeRestriction` instance.
        """
        if isinstance(expression, cls):
            return expression
        wildcard = CategorizedWildcard.fromExpression(expression, allowPatterns=False,
                                                      coerceUnrecognized=lambda d: d.name)
        if wildcard is ...:
            return cls.any
        else:
            return cls(frozenset(wildcard.strings))

    def __contains__(self, datasetType: DatasetType) -> bool:
        return (self.names is ... or datasetType.name in self.names
                or (datasetType.isComponent()
                    and DatasetType.splitDatasetTypeName(datasetType.name)[0] in self.names))

    def __eq__(self, other: CollectionSearch) -> bool:
        return self.names == other.names

    def __str__(self) -> str:
        if self.names is ...:
            return "..."
        else:
            return "{{{}}}".format(", ".join(self.names))

    def __repr__(self) -> str:
        if self.names is ...:
            return f"DatasetTypeRestriction(...)"
        else:
            return f"DatasetTypeRestriction({self.names!r})"

    @staticmethod
    def union(*args: DatasetTypeRestriction) -> DatasetTypeRestriction:
        """Merge one or more `DatasetTypeRestriction` instances, returning one
        that allows any of the dataset types included in any of them.

        Parameters
        ----------
        args
            Positional arguments are `DatasetTypeRestriction` instances.
        """
        if any(a.names is ... for a in args):
            return DatasetTypeRestriction.any
        return DatasetTypeRestriction(frozenset.union(*tuple(a.names for a in args)))

    names: Union[FrozenSet[str], type(...)]
    """The names of the dataset types included (i.e. permitted) by the
    restriction, or the special value `...` to permit all dataset types
    (`frozenset` [ `str` ] or `...`).
    """


DatasetTypeRestriction.any = DatasetTypeRestriction(...)
"""A special `DatasetTypeRestriction` instance that permits any dataset type.

This instance should be preferred instead of constructing a new one with `...`,
when possible, but it should not be assumed to be the only such instance (i.e.
don't use ``is`` instead of ``==`` for comparisons).
"""


def _yieldCollectionRecords(
    manager: CollectionManager,
    record: CollectionRecord,
    restriction: DatasetTypeRestriction,
    datasetType: Optional[DatasetType] = None,
    collectionType: Optional[CollectionType] = None,
    withRestrictions: bool = False,
    done: Optional[Set[str]] = None,
    flattenChains: bool = True,
    includeChains: Optional[bool] = None,
) -> Iterator[CollectionRecord]:
    """A helper function containing common logic for `CollectionSearch.iter`
    and `CollectionQuery.iter`: yield a single `CollectionRecord` only if it
    matches the criteria given in other arguments.

    Parameters
    ----------
    manager : `CollectionManager`
        Object responsible for managing the collection tables in a `Registry`.
    record : `CollectionRecord`
        Record to conditionally yield.
    restriction : `DatasetTypeRestriction`
        A restriction that must match ``datasetType`` (if given) in order to
        yield ``record``.
    datasetType : `DatasetType`, optional
        If given, a `DatasetType` instance that must be included in
        ``restriction`` in order to yield ``record``.
    collectionType : `CollectionType`, optional
        If given, a `CollectionType` enumeration value that must match
        ``record.type`` in order for ``record`` to be yielded.
    withRestrictions : `bool`, optional
        If `True` (`False` is default), yield ``restriction`` along with
        ``record``.
    done : `set` [ `str` ], optional
        A `set` of already-yielded collection names; if provided, ``record``
        will only be yielded if it is not already in ``done``, and ``done``
        will be updated to include it on return.
    flattenChains : `bool`, optional
        If `True` (default) recursively yield the child collections of
        `~CollectionType.CHAINED` colelctions.
    includeChains : `bool`, optional
        If `False`, return records for `~CollectionType.CHAINED` collections
        themselves.  The default is the opposite of ``flattenChains``: either
        return records for CHAINED collections or their children, but not both.

    Yields
    ------
    record : `CollectionRecord`
        The given collection record.
    restriction : `DatasetTypeRestriction`
        The given dataset type restriction; yielded only if
        ``withRestrictions`` is `True`.
    """
    if done is None:
        done = set()
    includeChains = includeChains if includeChains is not None else not flattenChains
    if collectionType is None or record.type is collectionType:
        done.add(record.name)
        if record.type is not CollectionType.CHAINED or includeChains:
            if withRestrictions:
                yield record, restriction
            else:
                yield record
    if flattenChains and record.type is CollectionType.CHAINED:
        done.add(record.name)
        yield from record.children.iter(
            manager,
            datasetType=datasetType,
            collectionType=collectionType,
            withRestrictions=withRestrictions,
            done=done,
            flattenChains=flattenChains,
            includeChains=includeChains,
        )


class CollectionSearch:
    """An ordered search path of collections and dataset type restrictions.

    The `fromExpression` method should almost always be used to construct
    instances, as the regular constructor performs no checking of inputs (and
    that can lead to confusing error messages downstream).

    Parameters
    ----------
    items : `list` [ `tuple` [ `str`, `DatasetTypeRestriction` ] ]
        Tuples that relate a collection name to the restriction on dataset
        types to search for within it.  This is not a mapping because the
        same collection name may appear multiple times with different
        restrictions.

    Notes
    -----
    A `CollectionSearch` is used to find a single dataset according to its
    dataset type and data ID, giving preference to collections in which the
    order they are specified.  A `CollectionQuery` can be constructed from
    a broader range of expressions but does not order the collections to be
    searched.

    `CollectionSearch` is iterable, yielding two-element tuples of `str`
    (collection name) and `DatasetTypeRestriction`.

    A `CollectionSearch` instance constructed properly (e.g. via
    `fromExpression`) is a unique representation of a particular search path;
    it is exactly the same internally and compares as equal to any
    `CollectionSearch` constructed from an equivalent expression,
    regardless of how different the original expressions appear.
    """
    def __init__(self, items: List[Tuple[[str, DatasetTypeRestriction]]]):
        assert all(isinstance(v, DatasetTypeRestriction) for _, v in items)
        self._items = items

    __slots__ = ("_items")

    @classmethod
    def fromExpression(cls, expression: Any) -> CollectionSearch:
        """Process a general expression to construct a `CollectionSearch`
        instance.

        Parameters
        ----------
        expression
            May be:
             - a `str` collection name;
             - a two-element `tuple` containing a `str` and any expression
               accepted by `DatasetTypeRestriction.fromExpression`;
             - any non-mapping iterable containing either of the above;
             - a mapping from `str` to any expression accepted by
               `DatasetTypeRestriction`.
             - another `CollectionSearch` instance (passed through
               unchanged).

            Multiple consecutive entries for the same collection with different
            restrictions will be merged.  Non-consecutive entries will not,
            because that actually represents a different search path.

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
        wildcard = CategorizedWildcard.fromExpression(expression,
                                                      allowAny=False,
                                                      allowPatterns=False,
                                                      coerceItemValue=DatasetTypeRestriction.fromExpression,
                                                      defaultItemValue=DatasetTypeRestriction.any)
        assert wildcard is not ...
        assert not wildcard.patterns
        assert not wildcard.strings
        return cls(
            # Consolidate repetitions of the same collection name.
            [(name, DatasetTypeRestriction.union(*tuple(item[1] for item in items)))
             for name, items in itertools.groupby(wildcard.items, key=operator.itemgetter(0))]
        )

    def iter(
        self, manager: CollectionManager, *,
        datasetType: Optional[DatasetType] = None,
        collectionType: Optional[CollectionType] = None,
        withRestrictions: bool = False,
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
        datasetType : `DatasetType`, optional
            If given, only yield collections whose dataset type restrictions
            include this dataset type.
        collectionType : `CollectionType`, optional
            If given, only yield collections of this type.
        withRestrictions : `bool`, optional
            If `True` (`False` is default) yield the associated
            `DatasetTypeRestriction` along with each `CollectionRecord`.
        done : `set`, optional
            A `set` containing the names of all collections already yielded;
            any collections whose names are already present in this set will
            not be yielded again, and those yielded will be added to it while
            iterating.  If not provided, an empty `set` will be created and
            used internally to avoid duplicates.
        flattenChains : `bool`, optional
            If `True` (default) recursively yield the child collections of
            `~CollectionType.CHAINED` colelctions.
        includeChains : `bool`, optional
            If `False`, return records for `~CollectionType.CHAINED`
            collections themselves.  The default is the opposite of
            ``flattenChains``: either return records for CHAINED collections or
            their children, but not both.
        """
        if done is None:
            done = set()
        for name, restriction in self._items:
            if name not in done and (datasetType is None or datasetType in restriction):
                yield from _yieldCollectionRecords(
                    manager,
                    manager.find(name),
                    restriction,
                    datasetType=datasetType,
                    collectionType=collectionType,
                    withRestrictions=withRestrictions,
                    done=done,
                    flattenChains=flattenChains,
                    includeChains=includeChains,
                )

    def __iter__(self) -> Iterator[Tuple[str, DatasetTypeRestriction]]:
        yield from self._items

    def __len__(self) -> bool:
        return len(self._items)

    def __eq__(self, other: CollectionSearch) -> bool:
        return self._items == other._items

    def __str__(self) -> str:
        return "[{}]".format(", ".join(f"{k}: {v}" for k, v in self._items))

    def __repr__(self) -> str:
        return f"CollectionSearch({self._items!r})"


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
    def __init__(self, search: Union[CollectionSearch, type(...)], patterns: Optional[Tuple[str, ...]]):
        self._search = search
        self._patterns = patterns

    __slots__ = ("_search", "_patterns")

    @classmethod
    def fromExpression(cls, expression: Any) -> Union[CollectionQuery, type(...)]:
        """Process a general expression to construct a `CollectionQuery`
        instance.

        Parameters
        ----------
        expression
            May be:
             - a `str` collection name;
             - a two-element `tuple` containing a `str` and any expression
               accepted by `DatasetTypeRestriction.fromExpression`;
             - an `re.Pattern` instance to match (with `re.Pattern.fullmatch`)
               against collection names;
             - any non-mapping iterable containing any of the above;
             - a mapping from `str` to any expression accepted by
               `DatasetTypeRestriction`.
             - a `CollectionSearch` instance;
             - another `CollectionQuery` instance (passed through unchanged).

            Multiple consecutive entries for the same collection with different
            restrictions will be merged.  Non-consecutive entries will not,
            because that actually represents a different search path.

        Returns
        -------
        collections : `CollectionQuery`
            A `CollectionQuery` instance.
        """
        if isinstance(expression, cls):
            return expression
        if expression is ...:
            return cls.any
        if isinstance(expression, CollectionSearch):
            return cls(search=expression, patterns=())
        wildcard = CategorizedWildcard.fromExpression(expression,
                                                      allowAny=True,
                                                      allowPatterns=True,
                                                      coerceItemValue=DatasetTypeRestriction.fromExpression,
                                                      defaultItemValue=DatasetTypeRestriction.any)
        if wildcard is ...:
            return cls.any
        assert not wildcard.strings
        return cls(search=CollectionSearch.fromExpression(wildcard),
                   patterns=tuple(wildcard.patterns))

    def iter(
        self, manager: CollectionManager, *,
        datasetType: Optional[DatasetType] = None,
        collectionType: Optional[CollectionType] = None,
        withRestrictions: bool = False,
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
        datasetType : `DatasetType`, optional
            If given, only yield collections whose dataset type restrictions
            include this dataset type.
        collectionType : `CollectionType`, optional
            If given, only yield collections of this type.
        withRestrictions : `bool`, optional
            If `True` (`False` is default) yield the associated
            `DatasetTypeRestriction` along with each `CollectionRecord`.
        flattenChains : `bool`, optional
            If `True` (default) recursively yield the child collections of
            `~CollectionType.CHAINED` colelctions.
        includeChains : `bool`, optional
            If `False`, return records for `~CollectionType.CHAINED`
            collections themselves.  The default is the opposite of
            ``flattenChains``: either return records for CHAINED collections or
            their children, but not both.
        """
        if self._search is ...:
            for record in manager:
                yield from _yieldCollectionRecords(
                    manager,
                    record,
                    DatasetTypeRestriction.any,
                    datasetType=datasetType,
                    collectionType=collectionType,
                    withRestrictions=withRestrictions,
                    flattenChains=flattenChains,
                    includeChains=includeChains,
                )
        else:
            done = set()
            yield from self._search.iter(
                manager,
                datasetType=datasetType,
                collectionType=collectionType,
                withRestrictions=withRestrictions,
                done=done,
                flattenChains=flattenChains,
                includeChains=includeChains,
            )
            for record in manager:
                if record.name not in done and any(p.fullmatch(record.name) for p in self._patterns):
                    yield from _yieldCollectionRecords(
                        manager,
                        record,
                        DatasetTypeRestriction.any,
                        datasetType=datasetType,
                        collectionType=collectionType,
                        withRestrictions=withRestrictions,
                        done=done,
                        flattenChains=flattenChains,
                        includeChains=includeChains,
                    )


CollectionQuery.any = CollectionQuery(..., None)
"""A special `CollectionQuery` instance that matches any collection.

This instance should be preferred instead of constructing a new one with `...`,
when possible, but it should not be assumed to be the only such instance.
"""

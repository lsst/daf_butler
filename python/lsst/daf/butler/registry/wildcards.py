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
    "CollectionContentRestriction",
    "CollectionQuery",
    "CollectionSearch",
    "DatasetTypeRestriction",
    "GovernorDimensionRestriction",
)

from collections import defaultdict
from dataclasses import dataclass
import itertools
import operator
import re
from typing import (
    AbstractSet,
    Any,
    Callable,
    ClassVar,
    Dict,
    FrozenSet,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    TYPE_CHECKING,
    Union,
)

import sqlalchemy

from ..core import DataCoordinate, DatasetType, DimensionUniverse, GovernorDimension
from ..core.named import NamedKeyDict, NamedKeyMapping
from ..core.utils import iterable
from ._collectionType import CollectionType

if TYPE_CHECKING:
    from .interfaces import CollectionManager, CollectionRecord

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
    def fromExpression(cls, expression: Any, *,
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
        if expression is Ellipsis:
            if not allowAny:
                raise TypeError("This expression may not be unconstrained.")
            return Ellipsis
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

        def process(element: Any, alreadyCoerced: bool = False) -> None:
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
    def __init__(self, names: Union[FrozenSet[str], EllipsisType]):
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
        if wildcard is Ellipsis:
            return cls.any
        else:
            return cls(frozenset(wildcard.strings))

    def __contains__(self, datasetType: DatasetType) -> bool:
        return (self.names is Ellipsis or datasetType.name in self.names
                or (datasetType.isComponent()
                    and DatasetType.splitDatasetTypeName(datasetType.name)[0] in self.names))

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, DatasetTypeRestriction):
            return self.names == other.names
        else:
            return False

    def __str__(self) -> str:
        if self.names is Ellipsis:
            return "..."
        else:
            return "{{{}}}".format(", ".join(self.names))

    def __repr__(self) -> str:
        if self.names is Ellipsis:
            return "DatasetTypeRestriction(...)"
        else:
            return f"DatasetTypeRestriction({self.names!r})"

    @staticmethod
    def union(*args: DatasetTypeRestriction) -> DatasetTypeRestriction:
        """Merge one or more `DatasetTypeRestriction` instances, returning one
        that allows any of the dataset types included in any of them.

        Parameters
        ----------
        *args
            Positional arguments are `DatasetTypeRestriction` instances.
        """
        result: Set[str] = set()
        for a in args:
            if a.names is Ellipsis:
                return DatasetTypeRestriction.any
            else:
                result.update(a.names)
        return DatasetTypeRestriction(frozenset(result))

    names: Union[FrozenSet[str], EllipsisType]
    """The names of the dataset types included (i.e. permitted) by the
    restriction, or the special value ``...`` to permit all dataset types
    (`frozenset` [ `str` ] or ``...``).
    """

    any: ClassVar[DatasetTypeRestriction]
    """A special `DatasetTypeRestriction` instance that permits any dataset
    type.

    This instance should be preferred instead of constructing a new one with
    ``...``, when possible, but it should not be assumed to be the only such
    instance (i.e. don't use ``is`` instead of ``==`` for comparisons).
    """


DatasetTypeRestriction.any = DatasetTypeRestriction(Ellipsis)


class GovernorDimensionRestriction:
    """An object that represents a restriction on some entity to only certain
    values of the governor dimensions.

    Parameters
    ----------
    universe : `DimensionUniverse`
        Object managing all dimensions.
    kwargs : `str` or `Iterable` [ `str` ]
        Dimension values to restrict to, keyed by dimension name.
    """
    def __init__(self, universe: DimensionUniverse, **kwargs: Union[str, Iterable[str], EllipsisType]):
        self.universe = universe
        self._dict: NamedKeyDict[GovernorDimension, Set[str]] = NamedKeyDict()
        for dimension in universe.getGovernorDimensions():
            value = kwargs.pop(dimension.name, Ellipsis)
            if value is not Ellipsis:
                self._dict[dimension] = set(iterable(value))
        if kwargs:
            raise ValueError(
                f"Invalid keyword argument(s): {kwargs.keys()} (must be governor dimension names)."
            )

    @staticmethod
    def union(
        universe: DimensionUniverse,
        *args: GovernorDimensionRestriction
    ) -> GovernorDimensionRestriction:
        """Merge one or more `GovernorDimensionRestriction` instances.

        Parameters
        ----------
        universe : `DimensionUniverse`
            Object managing all known dimensions.
        *args
            Additional positional arguments are `GovernorDimensionRestriction`
            instances.

        Returns
        -------
        merged : `GovernorDimensionRestriction`
            A `GovernorDimensionRestriction` that allows any of the dimension
            values permitted by any of the inputs.
        """
        mapping: Dict[str, Union[Set[str], EllipsisType]] = defaultdict(set)
        for a in args:
            for dimension in universe.getGovernorDimensions():
                new_values = a.mapping.get(dimension, Ellipsis)
                if new_values is Ellipsis:
                    mapping[dimension.name] = Ellipsis
                else:
                    accumulated = mapping[dimension.name]
                    if accumulated is not Ellipsis:
                        accumulated.update(new_values)
        return GovernorDimensionRestriction(universe, **mapping)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, GovernorDimensionRestriction):
            return False
        return self.mapping == other.mapping

    def __str__(self) -> str:
        return "({})".format(
            ", ".join(f"{dimension.name}: {values}" for dimension, values in self.mapping.items())
        )

    def __repr__(self) -> str:
        return "GovernorDimensionRestriction(<universe>, {})".format(
            ", ".join(f"{dimension.name}={values!r}" for dimension, values in self.mapping.items())
        )

    def isConsistentWith(self, dataId: DataCoordinate) -> bool:
        """Test whether this restriction is consistent with the given data ID.

        Parameters
        ----------
        dataId : `DataCoordinate`
            Data ID to test.

        Returns
        -------
        consistent : `bool`
            `True` if all values the data ID are either not restricted by
            ``self``, or are included in ``self``.
        """
        for dimension in self._dict.keys() & dataId.graph.dimensions:
            if dataId[dimension] in self._dict[dimension]:
                return False
        return True

    @property
    def mapping(self) -> NamedKeyMapping[GovernorDimension, AbstractSet[str]]:
        """A `NamedKeyMapping` view of this restriction, with all restricted
        dimensions as keys and sets of allowed data ID values as dictionary
        values.
        """
        return self._dict

    universe: DimensionUniverse
    """Object that manages all known dimensions (`DimensionUniverse`).
    """


class CollectionContentRestriction:
    """All restrictions that can be applied to what datasets can be included in
    a collection.

    Parameters
    ----------
    datasetTypes : `DatasetTypeRestriction`, optional
        Restriction on dataset types.
    dimensions : `GovernorDimensionRestriction`, optional
        Restriction on governor dimension values.
    universe : `DimensionUniverse`
        Object managing all known dimensions.
    """
    def __init__(
        self,
        datasetTypes: DatasetTypeRestriction = DatasetTypeRestriction.any,
        dimensions: Optional[GovernorDimensionRestriction] = None,
        *,
        universe: Optional[DimensionUniverse] = None,
    ):
        self.datasetTypes = datasetTypes
        if dimensions is None:
            if universe is None:
                raise TypeError("At least one of 'dimensions' and 'universe' must be provided.")
            dimensions = GovernorDimensionRestriction(universe)
        self.dimensions = dimensions

    @classmethod
    def fromExpression(cls, expression: Any, universe: DimensionUniverse) -> CollectionContentRestriction:
        """Construct a new restriction instance from an expression.

        Parameters
        ----------
        expression
            Either an existing `CollectionContentRestriction` instance (passed
            through unchanged) or any of the objects described in
            `DatasetTypeRestriction.fromExpression`.
        universe : `DimensionUniverse`
            Object managing all known dimensions.
        """
        if isinstance(expression, cls):
            return expression
        return cls(
            datasetTypes=DatasetTypeRestriction.fromExpression(expression),
            universe=universe,
        )

    @staticmethod
    def union(
        universe: DimensionUniverse,
        *args: CollectionContentRestriction
    ) -> CollectionContentRestriction:
        """Merge one or more `CollectionContentRestriction` instances,
        returning one that allows any of the dataset types or governor
        dimension valuesincluded in any of them.

        Parameters
        ----------
        universe : `DimensionUniverse`
            Object managing all known dimensions.
        args
            Positional arguments are `CollectionContentRestriction` instances.
        """
        return CollectionContentRestriction(
            DatasetTypeRestriction.union(*[arg.datasetTypes for arg in args]),
            GovernorDimensionRestriction.union(universe, *[arg.dimensions for arg in args]),
        )

    @classmethod
    def fromPairs(
        cls,
        pairs: Iterable[Tuple[str, Optional[str]]],
        universe: DimensionUniverse,
    ) -> CollectionContentRestriction:
        """Construct a restriction from a set of tuples that can be more easily
        mapped to a database representation.

        Parameters
        ----------
        pairs : `Iterable` [ `Tuple` [ `str`, `str` or `None` ] ]
            Pairs to interpret.  The first element of each tuple is either a
            governor dimension name or the special string "dataset_type".  The
            second element is the value of the dimension, the name of the
            dataset type, or `None` to indicate that there is no restriction
            on that dimension or on dataset types.
        universe : `DimensionUniverse`
            Object managing all known dimensions.

        Returns
        -------
        restriction : `CollectionContentRestriction`
            New restriction instance.
        """
        dimensions = defaultdict(set)
        datasetTypeNames: Optional[Set[str]] = set()
        for key, value in pairs:
            if key == "dataset_type":
                if value is None:
                    datasetTypeNames = None
                elif datasetTypeNames is None:
                    raise RuntimeError("Inconsistent collection content restriction.")
                else:
                    datasetTypeNames.add(value)
            else:
                dimensions[key].add(value)
        return cls(
            DatasetTypeRestriction(frozenset(datasetTypeNames) if datasetTypeNames is not None else Ellipsis),
            GovernorDimensionRestriction(universe, **dimensions),
        )

    def toPairs(self) -> Iterator[Tuple[str, Optional[str]]]:
        """Transform the restriction to a set of tuples that can be more easily
        mapped to a database representation.

        Yields
        ------
        key : `str`
            Either a governor dimension name or the special string
            "dataset_type".
        value : `str` or `None`
            The value of the dimension, the name of the dataset type, or `None`
            to indicate that there is no restriction on that dimension or on
            dataset types.
        """
        if self.datasetTypes.names is Ellipsis:
            yield ("dataset_type", None)
        else:
            yield from (("dataset_type", name) for name in sorted(self.datasetTypes.names))
        for dimension, values in self.dimensions.mapping.items():
            yield from ((dimension.name, v) for v in sorted(values))

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, CollectionContentRestriction):
            return False
        return self.datasetTypes == other.datasetTypes and self.dimensions == other.dimensions

    def __str__(self) -> str:
        terms = [f"datasetTypes: {self.datasetTypes}"]
        for dimension, values in self.dimensions.mapping.items():
            terms.append(f"{dimension.name}: {values}")
        return "({})".format(", ".join(terms))

    def __repr__(self) -> str:
        return f"CollectionContentRestriction({self.datasetTypes!r}, {self.dimensions!r})"


def _yieldCollectionRecords(
    manager: CollectionManager,
    record: CollectionRecord,
    restriction: CollectionContentRestriction,
    datasetType: Optional[DatasetType] = None,
    collectionTypes: AbstractSet[CollectionType] = CollectionType.all(),
    done: Optional[Set[str]] = None,
    flattenChains: bool = True,
    includeChains: Optional[bool] = None,
) -> Iterator[Tuple[CollectionRecord, CollectionContentRestriction]]:
    """A helper function containing common logic for `CollectionSearch.iter`
    and `CollectionQuery.iter`: recursively yield `CollectionRecord` only if
    they match the criteria given in other arguments.

    Parameters
    ----------
    manager : `CollectionManager`
        Object responsible for managing the collection tables in a `Registry`.
    record : `CollectionRecord`
        Record to conditionally yield.
    restriction : `CollectionContentRestriction`
        A restriction that must match ``datasetType`` (if given) in order to
        yield ``record``.
    datasetType : `DatasetType`, optional
        If given, a `DatasetType` instance that must be included in
        ``restriction`` in order to yield ``record``.
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
    restriction : `CollectionContentRestriction`
        The given dataset type restriction.
    """
    if done is None:
        done = set()
    includeChains = includeChains if includeChains is not None else not flattenChains
    if record.type in collectionTypes:
        done.add(record.name)
        if record.type is not CollectionType.CHAINED or includeChains:
            yield record, restriction
    if flattenChains and record.type is CollectionType.CHAINED:
        done.add(record.name)
        # We know this is a ChainedCollectionRecord because of the enum value,
        # but MyPy doesn't.
        yield from record.children.iterPairs(  # type: ignore
            manager,
            datasetType=datasetType,
            collectionTypes=collectionTypes,
            done=done,
            flattenChains=flattenChains,
            includeChains=includeChains,
        )


class CollectionSearch:
    """An ordered search path of collections and their content restrictions.

    The `fromExpression` method should almost always be used to construct
    instances, as the regular constructor performs no checking of inputs (and
    that can lead to confusing error messages downstream).

    Parameters
    ----------
    items : `list` [ `tuple` [ `str`, `CollectionContentRestriction` ] ]
        Tuples that relate a collection name to the restriction on dataset
        types to search for within it.  This is not a mapping because the
        same collection name may appear multiple times with different
        restrictions.
    universe : `DimensionUniverse`
        Object managing all known dimensions.

    Notes
    -----
    A `CollectionSearch` is used to find a single dataset according to its
    dataset type and data ID, giving preference to collections in which the
    order they are specified.  A `CollectionQuery` can be constructed from
    a broader range of expressions but does not order the collections to be
    searched.

    `CollectionSearch` is iterable, yielding two-element tuples of `str`
    (collection name) and `CollectionContentRestriction`.

    A `CollectionSearch` instance constructed properly (e.g. via
    `fromExpression`) is a unique representation of a particular search path;
    it is exactly the same internally and compares as equal to any
    `CollectionSearch` constructed from an equivalent expression,
    regardless of how different the original expressions appear.
    """
    def __init__(self, items: List[Tuple[str, CollectionContentRestriction]], universe: DimensionUniverse):
        assert all(isinstance(v, CollectionContentRestriction) for _, v in items)
        self._items = items
        self.universe = universe

    __slots__ = ("_items", "universe")

    @classmethod
    def fromExpression(cls, expression: Any, universe: DimensionUniverse) -> CollectionSearch:
        """Process a general expression to construct a `CollectionSearch`
        instance.

        Parameters
        ----------
        expression
            May be:
             - a `str` collection name;
             - a two-element `tuple` containing a `str` and any expression
               accepted by `CollectionContentRestriction.fromExpression`;
             - any non-mapping iterable containing either of the above;
             - a mapping from `str` to any expression accepted by
               `CollectionContentRestriction.fromExpression`.
             - another `CollectionSearch` instance (passed through
               unchanged).

            Multiple consecutive entries for the same collection with different
            restrictions will be merged.  Non-consecutive entries will not,
            because that actually represents a different search path.
        universe : `DimensionUniverse`
            Object managing all dimensions.

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
            coerceItemValue=lambda x: CollectionContentRestriction.fromExpression(x, universe),
            defaultItemValue=CollectionContentRestriction(universe=universe)
        )
        assert wildcard is not Ellipsis
        assert not wildcard.patterns
        assert not wildcard.strings
        return cls(
            # Consolidate repetitions of the same collection name.
            [(name, CollectionContentRestriction.union(universe, *tuple(item[1] for item in items)))
             for name, items in itertools.groupby(wildcard.items, key=operator.itemgetter(0))],
            universe
        )

    def iterPairs(
        self, manager: CollectionManager, *,
        datasetType: Optional[DatasetType] = None,
        collectionTypes: AbstractSet[CollectionType] = CollectionType.all(),
        done: Optional[Set[str]] = None,
        flattenChains: bool = True,
        includeChains: Optional[bool] = None,
    ) -> Iterator[Tuple[CollectionRecord, CollectionContentRestriction]]:
        """Like `iter`, but yield pairs of `CollectionRecord`,
        `CollectionContentRestriction` instead of just the former.

        See `iter` for all parameter descriptions.

        Yields
        ------
        record : `CollectionRecord`
            Matching collection records.
        restriction : `CollectionContentRestriction`
            The given collection content restriction.
        """
        if done is None:
            done = set()
        for name, restriction in self._items:
            if name not in done and (datasetType is None or datasetType in restriction.datasetTypes):
                yield from _yieldCollectionRecords(
                    manager,
                    manager.find(name),
                    restriction,
                    datasetType=datasetType,
                    collectionTypes=collectionTypes,
                    done=done,
                    flattenChains=flattenChains,
                    includeChains=includeChains,
                )

    def iter(
        self, manager: CollectionManager, *,
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
        datasetType : `DatasetType`, optional
            If given, only yield collections whose dataset type restrictions
            include this dataset type.
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
        for record, _ in self.iterPairs(manager, datasetType=datasetType, collectionTypes=collectionTypes,
                                        done=done, flattenChains=flattenChains, includeChains=includeChains):
            yield record

    def __iter__(self) -> Iterator[Tuple[str, CollectionContentRestriction]]:
        yield from self._items

    def __len__(self) -> int:
        return len(self._items)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, CollectionSearch):
            return self._items == other._items
        return False

    def __str__(self) -> str:
        return "[{}]".format(", ".join(f"{k}: {v}" for k, v in self._items))

    def __repr__(self) -> str:
        return f"CollectionSearch({self._items!r})"

    universe: DimensionUniverse
    """Object that manages all known dimensions (`DimensionUniverse`).
    """


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
        patterns: Tuple[re.Pattern, ...] = (), *,
        universe: DimensionUniverse,
    ):
        self._search = search
        self._patterns = patterns
        self.universe = universe

    __slots__ = ("_search", "_patterns", "universe")

    @classmethod
    def fromExpression(cls, expression: Any, universe: DimensionUniverse) -> CollectionQuery:
        """Process a general expression to construct a `CollectionQuery`
        instance.

        Parameters
        ----------
        expression
            May be:
             - a `str` collection name;
             - a two-element `tuple` containing a `str` and any expression
               accepted by `CollectionContentRestriction.fromExpression`;
             - an `re.Pattern` instance to match (with `re.Pattern.fullmatch`)
               against collection names;
             - any non-mapping iterable containing any of the above;
             - a mapping from `str` to any expression accepted by
               `CollectionContentRestriction`.
             - a `CollectionSearch` instance;
             - another `CollectionQuery` instance (passed through unchanged).

            Multiple consecutive entries for the same collection with different
            restrictions will be merged.  Non-consecutive entries will not,
            because that actually represents a different search path.
        universe : `DimensionUniverse`
            Object managing all dimensions.

        Returns
        -------
        collections : `CollectionQuery`
            A `CollectionQuery` instance.
        """
        if isinstance(expression, cls):
            return expression
        if expression is Ellipsis:
            return cls(universe=universe)
        if isinstance(expression, CollectionSearch):
            return cls(search=expression, patterns=(), universe=universe)
        wildcard = CategorizedWildcard.fromExpression(
            expression,
            allowAny=True,
            allowPatterns=True,
            coerceItemValue=lambda x: CollectionContentRestriction.fromExpression(x, universe),
            defaultItemValue=CollectionContentRestriction(universe=universe)
        )
        if wildcard is Ellipsis:
            return cls(universe=universe)
        assert not wildcard.strings, \
            "All bare strings should be transformed to (str, DatasetTypeRestriction) tuples."
        return cls(
            search=CollectionSearch.fromExpression(wildcard.items, universe),
            patterns=tuple(wildcard.patterns),
            universe=universe,
        )

    def iterPairs(
        self, manager: CollectionManager, *,
        datasetType: Optional[DatasetType] = None,
        collectionTypes: AbstractSet[CollectionType] = CollectionType.all(),
        flattenChains: bool = True,
        includeChains: Optional[bool] = None,
    ) -> Iterator[Tuple[CollectionRecord, CollectionContentRestriction]]:
        """Like `iter`, but yield pairs of `CollectionRecord`,
        `CollectionContentRestriction` instead of just the former.

        See `iter` for all parameter descriptions.

        Yields
        ------
        record : `CollectionRecord`
            Matching collection records.
        restriction : `CollectionContentRestriction`
            The given dataset type restriction.

        """
        unrestricted = CollectionContentRestriction(universe=self.universe)
        if self._search is Ellipsis:
            for record in manager:
                yield from _yieldCollectionRecords(
                    manager,
                    record,
                    unrestricted,
                    datasetType=datasetType,
                    collectionTypes=collectionTypes,
                    flattenChains=flattenChains,
                    includeChains=includeChains,
                )
        else:
            done: Set[str] = set()
            yield from self._search.iterPairs(
                manager,
                datasetType=datasetType,
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
                        unrestricted,
                        datasetType=datasetType,
                        collectionTypes=collectionTypes,
                        done=done,
                        flattenChains=flattenChains,
                        includeChains=includeChains,
                    )

    def iter(
        self, manager: CollectionManager, *,
        datasetType: Optional[DatasetType] = None,
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
        datasetType : `DatasetType`, optional
            If given, only yield collections whose dataset type restrictions
            include this dataset type.
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
        for record, _ in self.iterPairs(manager, datasetType=datasetType, collectionTypes=collectionTypes,
                                        flattenChains=flattenChains, includeChains=includeChains):
            yield record

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, CollectionQuery):
            return self._search == other._search and self._patterns == other._patterns
        else:
            return False

    universe: DimensionUniverse
    """Object that manages all known dimensions (`DimensionUniverse`).
    """

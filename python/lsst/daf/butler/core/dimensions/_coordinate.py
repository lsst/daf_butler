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

#
# Design notes for this module are in
# doc/lsst.daf.butler/dev/dataCoordinate.py.
#

from __future__ import annotations

__all__ = (
    "DataCoordinate",
    "DataCoordinateCommonState",
    "DataId",
    "DataIdKey",
    "DataIdValue",
    "InconsistentDataIdError",
    "SerializedDataCoordinate",
)

from abc import abstractmethod
from dataclasses import dataclass
from typing import (
    AbstractSet,
    Any,
    Dict,
    Iterator,
    Mapping,
    Optional,
    Set,
    Tuple,
    TYPE_CHECKING,
    Union,
)
from pydantic import BaseModel

from lsst.sphgeom import Region
from ..named import NamedKeyDict, NamedKeyMapping, NameLookupMapping, NamedValueAbstractSet
from ..timespan import Timespan
from ._elements import Dimension, DimensionElement
from ._graph import DimensionGraph
from ._records import DimensionRecord, SerializedDimensionRecord
from ..json import from_json_pydantic, to_json_pydantic

if TYPE_CHECKING:  # Imports needed only for type annotations; may be circular.
    from ._universe import DimensionUniverse
    from .._containers import HeterogeneousDimensionRecordAbstractSet
    from ...registry import Registry

DataIdKey = Union[str, Dimension]
"""Type annotation alias for the keys that can be used to index a
DataCoordinate.
"""

# Pydantic will cast int to str if str is first in the Union.
DataIdValue = Union[int, str, None]
"""Type annotation alias for the values that can be present in a
DataCoordinate or other data ID.
"""


class InconsistentDataIdError(ValueError):
    """Exception raised when a data ID contains contradictory key-value pairs,
    according to dimension relationships.
    """


class SerializedDataCoordinate(BaseModel):
    """Simplified model for serializing a `DataCoordinate`."""

    dataId: Dict[str, DataIdValue]
    records: Optional[Dict[str, SerializedDimensionRecord]] = None


def _intersectRegions(*args: Region) -> Optional[Region]:
    """Return the intersection of several regions.

    For internal use by `ExpandedDataCoordinate` only.

    If no regions are provided, returns `None`.

    This is currently a placeholder; it actually returns `NotImplemented`
    (it does *not* raise an exception) when multiple regions are given, which
    propagates to `ExpandedDataCoordinate`.  This reflects the fact that we
    don't want to fail to construct an `ExpandedDataCoordinate` entirely when
    we can't compute its region, and at present we don't have a high-level use
    case for the regions of these particular data IDs.
    """
    if len(args) == 0:
        return None
    elif len(args) == 1:
        return args[0]
    else:
        return NotImplemented


@dataclass(frozen=True)
class DataCoordinateCommonState:
    """Helper struct for `DataCoordinate` and containers of `DataCoordinate`.

    Notes
    -----
    Containers of data IDs generally require that their elements have the same
    common state - both the dimensions identified and the `has_full` /
    `has_records` flags.  This structure is both a convenient way to pass
    around that combination and a place to put the algorithms that transform
    data IDs between states.  It should rarely be used directly outside of
    the `lsst.daf.butler` package.

    `DataCoordinateCommonState` defines the ``<=`` and ``>=`` operators as
    subset/superset relations: ``a <= b`` if and only if a `DataCoordinate`
    with state ``a`` has all information necessary to construct a
    `DataCoordinate` with state ``b``.  Strict subset (``<``) and superset
    (``>``) are *not* defined, as they seem more likely to be used
    accidentally than intentionally.
    """

    __slots__ = ("graph", "has_full", "has_records")

    graph: DimensionGraph
    """Dimensions that the data ID or data IDs identify."""

    has_full: bool
    """`True` if all data IDs satisfy `DataCoordinate.has_full`; `False` if
    none do (mixed containers are not permitted).
    """

    has_records: bool
    """Like ``has_full``, but for `DataCoordinate.has_records`."""

    def __post_init__(self) -> None:
        if not self.graph.implied and not self.has_full:
            # If there are no implied dimensions, we always have values for
            # them.  We use object.__setattr__ because this is a frozen
            # dataclass (it's okay because this is still part of
            # initialization).
            object.__setattr__(self, "has_full", True)
        if not self.graph.elements:
            object.__setattr__(self, "has_records", True)

    def __le__(self, other: DataCoordinateCommonState) -> bool:
        return self.known <= other.known and (
            not self.has_records
            or (other.has_records and self.graph.elements <= other.graph.elements)
        )

    def __ge__(self, other: DataCoordinateCommonState) -> bool:
        return other <= self

    @property
    def known(self) -> NamedValueAbstractSet[Dimension]:
        """The set of dimensions whose values are identified by a
        DataCoordinate with this state.

        This has the same iteration order as `DataCoordinate` (required
        dimensions followed by implied dimensions, if present).
        """
        if self.has_full:
            return self.graph._dataCoordinateIndices.keys()
        else:
            return self.graph.required

    @classmethod
    def from_data_coordinate(
        cls,
        data_id: DataCoordinate,
    ) -> DataCoordinateCommonState:
        """Extract common state from a `DataCoordinate` instance.

        Parameters
        ----------
        data_id : `DataCoordinate`
            Data ID to extract state from.

        Returns
        -------
        common_state : `DataCoordinateCommonState`
            Common state for the given data ID.
        """
        return cls(data_id.graph, has_full=data_id.has_full, has_records=data_id.has_records)

    @classmethod
    def calculate(
        cls,
        input_state: Optional[DataCoordinateCommonState] = None,
        input_keys: Optional[AbstractSet[str]] = None,
        target_graph: Optional[DimensionGraph] = None,
        defaults: Optional[DimensionGraph] = None,
        records: Optional[HeterogeneousDimensionRecordAbstractSet] = None,
        has_full: Optional[bool] = None,
        has_records: Optional[bool] = None,
        universe: Optional[DimensionUniverse] = None,
    ) -> DataCoordinateCommonState:
        """Calculate state from common arguments used to construct data IDs.

        This is an internal interface that should rarely be used outside
        `lsst.daf.butler`.  Most callers should use `DataCoordinate` methods
        (e.g. `~DataCoordinate.standardize`, `~DataCoordinate.subset`) or
        `Registry.expandDataId` instead.

        Parameters
        ----------
        input_state : `DataCoordinateCommonState`, optional
            Common state for an existing input `DataCoordinate`.
        input_keys : `collections.abc.Set` [ `str` ], optional
            Set-like object of `str` dimension names for all input data ID
            key-value pairs come from regular dictionaries and/or keyword
            arguments.
        target_graph : `DimensionGraph`, optional
            Dimensions the output data ID should identify.  If provided, this
            directly sets `DataCoordinateCommonState.graph`. If not provided,
            it is inferred from ``input_state`` and ``input_keys`` as the
            `DimensionGraph` that includes all of the dimensions they
            reference.  This may also include dimensions they do not reference,
            which means that there may not always be enough information
            provided to populate the `DataCoordinate` (resulting in `KeyError`
            being raised).
        defaults : `DataCoordinate`, optional
            A fully-expanded data ID with default key-value pairs and records
            for governor dimensions.
        records : `HeterogeneousDimensionRecordAbstractSet`, optional
            An external container of `DimensionRecord` objects that can be used
            to attach records and fill in implied dimension values.  If
            provided, it must include records for everything in
            `DimensionGraph.elements`, except those that do not actually exist
            in the `Registry`.
        has_full : `bool`, optional
            If not `None`, force `DataCoordinateCommonState.has_full` to this
            value on the returned object.  If `True`, this means `KeyError`
            will be raised if implied dimension values are not available from
            these inputs.  If `False`, this means implied dimension values
            will not be included in the `DataCoordinate` objects returned by
            `conform` even if they are available from the inputs.  If `None`,
            implied dimension values are included if they are available.
        has_records : `bool`, optional
            If not `None`, force `DataCoordinateCommonState.has_records` to
            this value on the returned object.  If `True`, this means
            `KeyError` will be raised if external ``records`` is not provided
            and ``input_state`` either has no records or does not have records
            for all elements.  If `False`, records will not be included in the
            `DataCoordinate` objects returned by `conform` even they are
            available from the inputs.  If `None`, records are included if they
            are available.
        universe : `DimensionUniverse`, optional
            Object that defines all dimensions.  Must be provided explicitly
            unless at least one of ``input_graph``, ``target_graph``,
            ``defaults``, or ``records`` is.

        Returns
        -------
        target_state : `DataCoordinateCommonState`
            Common state object consistent with the given inputs.

        Raises
        ------
        TypeError
            Raised if arguments are fundamentally inconsistent, e.g.
            ``has_full=False`` but ``has_records=True``.
        KeyError
            Raised if these inputs are not sufficient to construct data IDs
            with the requested state.  This can occur if:

            - there are no values for required dimensions;

            - if ``has_full=True`` and there are no values for one or
              more implied dimensions;

            - if ``has_records=True`` and no records for one or more elements
              were provided.

        Notes
        -----
        This method is tightly coupled to `conform`; `calculate` is called once
        to determine the state for the output `DataCoordinate` instance or
        instances, and then `conform` is called one more times to transform
        those arguments into one or more `DataCoordinate` instances.
        """
        if has_records and not has_full:
            raise TypeError("has_records=True without has_full=True is an invalid combination.")
        # Gather the data ID keys we know from the inputs.
        keys_known: Set[str] = set()
        if input_keys is not None:
            keys_known.update(input_keys)
        if input_state is not None:
            keys_known.update(input_state.known.names)
            universe = input_state.graph.universe
        if target_graph is None:
            if universe is None:
                if defaults is None:
                    if records is None:
                        raise TypeError(
                            "Universe must be provided, either directly or "
                            "passing another argument that has one."
                        )
                    else:
                        universe = records.universe
                else:
                    universe = defaults.universe
            target_graph = DimensionGraph(universe, names=keys_known)
        else:
            universe = target_graph.universe
        assert universe is not None, "Should be guaranteed by earlier logic."
        assert target_graph is not None, "Should be guaranteed by earlier logic."
        if input_keys is not None and not (input_keys <= universe.getStaticDimensions().names):
            # We silently ignore keys that aren't relevant for this particular
            # data ID, but keys that aren't relevant for any possible data ID
            # are a bug that we want to report to the user.  This is especially
            # important because other code frequently forwards unrecognized
            # kwargs here.
            raise ValueError(
                f"Unrecognized key(s) for data ID: {input_keys - universe.getStaticDimensions().names}."
            )
        # We also might have known keys from defaults, but we can't merge those
        # in earlier because they shouldn't be used to infer target_graph.
        if defaults is not None:
            keys_known.update(defaults.names)
        # Do we have enough keys to identify just the required dimensions?
        if not keys_known.issuperset(target_graph.required.names):
            raise KeyError(
                f"Missing dimensions {target_graph.required.names - keys_known} in {keys_known} for "
                f"minimal data ID with dimensions {target_graph}."
            )
        # Figure out whether we can identify implied dimensions, too.
        if not target_graph.implied:
            # There are no implied dimensions, so has_full must be true,
            # even if caller said has_full=False.
            full_known = True
            has_full = True
        else:
            full_known = records is not None or keys_known.issuperset(target_graph.implied.names)
            if has_full is None:
                has_full = full_known
            elif has_full and not full_known:
                raise KeyError(
                    f"Missing dimensions {target_graph.names - keys_known} in {keys_known} for "
                    f"full data ID with dimensions {target_graph}; ."
                )
        # Figure out whether we can attach records for all identified dimension
        # elements.
        records_known = (
            records is not None or (
                input_state is not None
                and input_state.has_records
                and input_state.graph.issuperset(target_graph)
            )
        )
        if has_records is None:
            has_records = has_full and records_known
        elif has_records and not records_known:
            raise KeyError(
                f"No records or not enough records for data ID with dimensions {target_graph}."
            )
        return cls(target_graph, has_full=has_full, has_records=has_records)

    def conform(
        self,
        input_state: Optional[DataCoordinateCommonState] = None,
        mapping: Optional[NameLookupMapping[Dimension, DataIdValue]] = None,
        defaults: Optional[DataCoordinate] = None,
        records: Optional[HeterogeneousDimensionRecordAbstractSet] = None,
        **kwargs: DataIdValue,
    ) -> DataCoordinate:
        """Create new `DataCoordinate` instances with the state defined by this
        object.

        This method is usually called only after first calling
        `DataCoordinateCommonState.calculate` with consistent arguments.

        Parameters
        ----------
        input_state : `DataCoordinateCommonState`, optional
            Common state for an existing input `DataCoordinate`.  Must be a
            superset of the object passed as the `calculate` argument of the
            same name.  Should be not `None` only if ``mapping`` is a
            `DataCoordinate` instance.
        mapping : `Mapping`, optional
            Input dimension key-value pairs: a `DataCoordinate` instance (if
            and only if ``input_state`` is not `None`, with state a superset
            of ``input_state``), or a mapping with `str` or `Dimension` keys
            (in which case these keys should have been included in the
            ``input_keys`` argument to `calculate`).
        defaults : `DataCoordinate`, optional
            A fully-expanded data ID with default key-value pairs and records
            for governor dimensions.  Must be the same object passed as the
            `calculate` argument of the same name.
        records : `HeterogeneousDimensionRecordAbstractSet`, optional
            An external container of `DimensionRecord` objects that can be used
            to attach records and fill in implied dimension values.  Must be
            the same object passed as the `calculate` argument of the same
            name.
        **kwargs
            Additional dimension key-value pairs.  Keys must have been included
            in the ``input_keys`` argument to `calculate`.

        Raises
        ------
        RuntimeError
            Raised if the parameters are inconsistent with those of the call to
            `calculate` that constructed ``self``.  This is always a logic bug
            somewhere, but it may be in user code that should be passing a
            homogeneous list of data IDs to a `lsst.daf.butler` interface but
            isn't, where "homogenenous" is defined as:

            - all `DataCoordinate` objects with the same
              `DataCoordinateCommonState`

            - all mapping objects of some other (consistent) type, with the
              same keys.
        """
        # Extract key-value pairs are records from various sources.
        values_by_name = {} if defaults is None else defaults.full.byName()
        records_by_name: Dict[str, Optional[DimensionRecord]] = {}
        if input_state is not None:
            if not isinstance(mapping, DataCoordinate):
                raise RuntimeError(
                    "Inconsistency detected while processing data IDs; "
                    "a true DataCoordinate instance was expected, but "
                    f"{mapping} of type {type(mapping).__name__} was given.  "
                    "This can occur if an interface that expects a "
                    "homogeneous iterable of data IDs was instead passed "
                    "a mix of objects."
                )
            if input_state == self:
                # Shortcut: input mapping is already standardized exactly as
                # desired.
                return mapping
            elif input_state.has_full:
                values_by_name.update(mapping.full.byName())
                if input_state.has_records:
                    records_by_name.update(mapping.records.byName())
            else:
                values_by_name.update(mapping.byName())
        elif isinstance(mapping, NamedKeyMapping):
            values_by_name.update(mapping.byName())
        elif mapping is not None:
            values_by_name.update(mapping)
        values_by_name.update(kwargs)
        # If we are expecting to return records and don't have the ones we need
        # yet, or we need to use records to find implied key-value pairs,
        # delegate to the external records container to get the records we need
        # and expand values_by_name.
        if (
            self.has_records and not (records_by_name.keys() >= self.graph.elements.names)
            or not (values_by_name.keys() >= self.known.names)
        ):
            if records is None:
                raise RuntimeError(
                    "Inconsistency detected while processing data IDs; "
                    "external records are needed to transform data ID "
                    f"{values_by_name} to state {self}.  "
                    "This can occur if an interface that expects a "
                    "homogeneous iterable of data IDs was instead passed "
                    "a mix of objects or dictionaries with different keys."
                )
            records.expand_data_id_dict(values_by_name, self.graph, related_records=records_by_name)
        # Put values into tuple, since that's what actually backs our
        # DataCoordinate implementations.
        values = tuple(
            dimension.validated(values_by_name[dimension.name]) for dimension in self.known
        )
        result: DataCoordinate = _BasicTupleDataCoordinate(self.graph, values)
        if self.has_records:
            result = result.expanded(records_by_name)
        return result


class DataCoordinate(NamedKeyMapping[Dimension, DataIdValue]):
    """Data ID dictionary.

    An immutable data ID dictionary that guarantees that its key-value pairs
    identify at least all required dimensions in a `DimensionGraph`.

    `DataCoordinateSet` itself is an ABC, but provides `staticmethod` factory
    functions for private concrete implementations that should be sufficient
    for most purposes.  `standardize` is the most flexible and safe of these;
    the others (`makeEmpty`, `fromRequiredValues`, and `fromFullValues`) are
    more specialized and perform little or no checking of inputs.

    Notes
    -----
    Like any data ID class, `DataCoordinate` behaves like a dictionary, but
    with some subtleties:

     - Both `Dimension` instances and `str` names thereof may be used as keys
       in lookup operations, but iteration (and `keys`) will yield `Dimension`
       instances.  The `names` property can be used to obtain the corresponding
       `str` names.

     - Lookups for implied dimensions (those in ``self.graph.implied``) are
       supported if and only if `has_full` is `True`, and are never
       included in iteration or `keys`.  The `full` property may be used to
       obtain a mapping whose keys do include implied dimensions.

     - Equality comparison with other mappings is supported, but it always
       considers only required dimensions (as well as requiring both operands
       to identify the same dimensions).  This is not quite consistent with the
       way mappings usually work - normally differing keys imply unequal
       mappings - but it makes sense in this context because data IDs with the
       same values for required dimensions but different values for implied
       dimensions represent a serious problem with the data that
       `DataCoordinate` cannot generally recognize on its own, and a data ID
       that knows implied dimension values should still be able to compare as
       equal to one that does not.  This is of course not the way comparisons
       between simple `dict` data IDs work, and hence using a `DataCoordinate`
       instance for at least one operand in any data ID comparison is strongly
       recommended.
    """

    __slots__ = ()

    _serializedType = SerializedDataCoordinate

    @staticmethod
    def standardize(
        mapping: Optional[NameLookupMapping[Dimension, DataIdValue]] = None,
        *,
        graph: Optional[DimensionGraph] = None,
        universe: Optional[DimensionUniverse] = None,
        has_full: Optional[bool] = None,
        has_records: Optional[bool] = None,
        defaults: Optional[DataCoordinate] = None,
        records: Optional[HeterogeneousDimensionRecordAbstractSet] = None,
        **kwargs: DataIdValue,
    ) -> DataCoordinate:
        """Standardize the supplied dataId.

        Adapts an arbitrary mapping and/or additional arguments into a true
        `DataCoordinate`, or augment an existing one.

        Parameters
        ----------
        mapping : `~collections.abc.Mapping`, optional
            An informal data ID that maps dimensions or dimension names to
            their primary key values (may also be a true `DataCoordinate`).
        graph : `DimensionGraph`
            The dimensions to be identified by the new `DataCoordinate`.
            If not provided, will be inferred from the keys of ``mapping`` and
            ``**kwargs``, and ``universe`` must be provided unless ``mapping``
            is already a `DataCoordinate`.
        universe : `DimensionUniverse`
            All known dimensions and their relationships; used to expand
            and validate dependencies when ``graph`` is not provided.
        has_full : `bool`
            `True` if the result must satisfy `DataCoordinate.has_full`;
            `False` if it must not.
        has_records : `bool`
            Like ``has_full``, but for `DataCoordinate.has_records`.
        defaults : `DataCoordinate`, optional
            Default dimension key-value pairs to use when needed.  These are
            never used to infer ``graph``, and are ignored if a different value
            is provided for the same key in ``mapping`` or `**kwargs``.
        records : `HeterogeneousDimensionRecordAbstractSet`, optional
            Container of `DimensionRecord` instances that may be used to
            fill in missing keys and/or attach records.  If provided, the
            returned object is guaranteed to have `has_records` is `True`
            unless ``has_records=False`` was passed explicitly.
        **kwargs
            Additional keyword arguments are treated like additional key-value
            pairs in ``mapping``, overriding those present.

        Returns
        -------
        coordinate : `DataCoordinate`
            A validated `DataCoordinate` instance.

        Raises
        ------
        TypeError
            Raised if the set of optional arguments provided is not supported.
        KeyError
            Raised if a key-value pair for a required dimension is missing.
        """
        input_state = None
        input_keys = set(kwargs.keys())
        if isinstance(mapping, DataCoordinate):
            input_state = DataCoordinateCommonState.from_data_coordinate(mapping)
        elif isinstance(mapping, NamedKeyMapping):
            input_keys.update(mapping.names)
        elif mapping is not None:
            input_keys.update(mapping.keys())
        common_state = DataCoordinateCommonState.calculate(
            input_state=input_state,
            input_keys=input_keys,
            target_graph=graph,
            defaults=defaults.graph if defaults is not None else None,
            records=records,
            universe=universe,
            has_full=has_full,
            has_records=has_records,
        )
        return common_state.conform(
            input_state,
            mapping,
            defaults=defaults,
            records=records,
            **kwargs,
        )

    @staticmethod
    def makeEmpty(universe: DimensionUniverse) -> DataCoordinate:
        """Return an empty `DataCoordinate`.

        It identifies the null set of dimensions.

        Parameters
        ----------
        universe : `DimensionUniverse`
            Universe to which this null dimension set belongs.

        Returns
        -------
        dataId : `DataCoordinate`
            A data ID object that identifies no dimensions.  `has_full` and
            `has_records` are guaranteed to be `True`, because both `full`
            and `records` are just empty mappings.
        """
        return _ExpandedTupleDataCoordinate(universe.empty, (), {})

    @staticmethod
    def fromRequiredValues(graph: DimensionGraph, values: Tuple[DataIdValue, ...]) -> DataCoordinate:
        """Construct a `DataCoordinate` from required dimension values.

        This is a low-level interface with at most assertion-level checking of
        inputs.  Most callers should use `standardize` instead.

        Parameters
        ----------
        graph : `DimensionGraph`
            Dimensions this data ID will identify.
        values : `tuple` [ `int` or `str` ]
            Tuple of primary key values corresponding to ``graph.required``,
            in that order.

        Returns
        -------
        dataId : `DataCoordinate`
            A data ID object that identifies the given dimensions.
            ``dataId.has_full`` will be `True` if and only if ``graph.implied``
            is empty, and ``dataId.has_records`` will never be `True`.
        """
        assert len(graph.required) == len(values), \
            f"Inconsistency between dimensions {graph.required} and required values {values}."
        return _BasicTupleDataCoordinate(graph, values)

    @staticmethod
    def fromFullValues(graph: DimensionGraph, values: Tuple[DataIdValue, ...]) -> DataCoordinate:
        """Construct a `DataCoordinate` from all dimension values.

        This is a low-level interface with at most assertion-level checking of
        inputs.  Most callers should use `standardize` instead.

        Parameters
        ----------
        graph : `DimensionGraph`
            Dimensions this data ID will identify.
        values : `tuple` [ `int` or `str` ]
            Tuple of primary key values corresponding to
            ``itertools.chain(graph.required, graph.implied)``, in that order.
            Note that this is _not_ the same order as ``graph.dimensions``,
            though these contain the same elements.

        Returns
        -------
        dataId : `DataCoordinate`
            A data ID object that identifies the given dimensions.
            ``dataId.has_full`` will be `True` if and only if ``graph.implied``
            is empty, and ``dataId.has_records`` will never be `True`.
        """
        assert len(graph.dimensions) == len(values), \
            f"Inconsistency between dimensions {graph.dimensions} and full values {values}."
        return _BasicTupleDataCoordinate(graph, values)

    def __hash__(self) -> int:
        return hash((self.graph,) + tuple(self[d.name] for d in self.graph.required))

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, DataCoordinate):
            other = DataCoordinate.standardize(other, universe=self.universe)
        return self.graph == other.graph and all(self[d.name] == other[d.name] for d in self.graph.required)

    def __repr__(self) -> str:
        # We can't make repr yield something that could be exec'd here without
        # printing out the whole DimensionUniverse the graph is derived from.
        # So we print something that mostly looks like a dict, but doesn't
        # quote its keys: that's both more compact and something that can't
        # be mistaken for an actual dict or something that could be exec'd.
        terms = [f"{d}: {self[d]!r}" for d in self.graph.required.names]
        if self.has_full and self.graph.required != self.graph.dimensions:
            terms.append("...")
        return "{{{}}}".format(', '.join(terms))

    def __lt__(self, other: Any) -> bool:
        # Allow DataCoordinate to be sorted
        if not isinstance(other, DataCoordinate):
            return NotImplemented
        # Form tuple of tuples for each DataCoordinate:
        # Unlike repr() we only use required keys here to ensure that
        # __eq__ can not be true simultaneously with __lt__ being true.
        self_kv = tuple(self.items())
        other_kv = tuple(other.items())

        return self_kv < other_kv

    def __iter__(self) -> Iterator[Dimension]:
        return iter(self.keys())

    def __len__(self) -> int:
        return len(self.keys())

    def keys(self) -> NamedValueAbstractSet[Dimension]:
        return self.graph.required

    @property
    def names(self) -> AbstractSet[str]:
        """Names of the required dimensions identified by this data ID.

        They are returned in the same order as `keys`
        (`collections.abc.Set` [ `str` ]).
        """
        return self.keys().names

    def subset(self, graph: DimensionGraph) -> DataCoordinate:
        """Return a `DataCoordinate` whose graph is a subset of ``self.graph``.

        Parameters
        ----------
        graph : `DimensionGraph`
            The dimensions identified by the returned `DataCoordinate`.

        Returns
        -------
        coordinate : `DataCoordinate`
            A `DataCoordinate` instance that identifies only the given
            dimensions.  May be ``self`` if ``graph == self.graph`` and state
            flags are unchanged.

        Raises
        ------
        KeyError
            Raised if needed information (dimension values or records) are not
            present.  This will always happen if ``graph.issubset(self.graph)``
            is `False`. it may also happen even if only a minimal data ID is
            requested and ``graph.issubset(self.graph)`` is `True`, if
            ``graph.required.issubset(self.graph.required)`` is `False`.  As an
            example of the latter case, consider trying to go from a data ID
            with dimensions {instrument, physical_filter, band} to just
            {instrument, band}; band is implied by physical_filter and hence
            would have no value in the original data ID if ``self.has_full`` is
            `False`.
        """
        current_state = DataCoordinateCommonState.from_data_coordinate(self)
        target_state = DataCoordinateCommonState.calculate(current_state, target_graph=graph,)
        return target_state.conform(current_state, self)

    @abstractmethod
    def union(self, other: DataCoordinate) -> DataCoordinate:
        """Combine two data IDs.

        Yields a new one that identifies all dimensions that either of them
        identify.

        Parameters
        ----------
        other : `DataCoordinate`
            Data ID to combine with ``self``.

        Returns
        -------
        unioned : `DataCoordinate`
            A `DataCoordinate` instance that satisfies
            ``unioned.graph == self.graph.union(other.graph)``.  Will preserve
            ``has_full`` and ``has_records`` whenever possible.

        Notes
        -----
        No checking for consistency is performed on values for keys that
        ``self`` and ``other`` have in common, and which value is included in
        the returned data ID is not specified.
        """
        raise NotImplementedError()

    @abstractmethod
    def expanded(self, records: NameLookupMapping[DimensionElement, Optional[DimensionRecord]]
                 ) -> DataCoordinate:
        """Return a `DataCoordinate` that holds the given records.

        Guarantees that `has_records` is `True`.

        This is a low-level interface with at most assertion-level checking of
        inputs.  Most callers should use `Registry.expandDataId` instead.

        Parameters
        ----------
        records : `Mapping` [ `str`, `DimensionRecord` or `None` ]
            A `NamedKeyMapping` with `DimensionElement` keys or a regular
            `Mapping` with `str` (`DimensionElement` name) keys and
            `DimensionRecord` values.  Keys must cover all elements in
            ``self.graph.elements``.  Values may be `None`, but only to reflect
            actual NULL values in the database, not just records that have not
            been fetched.
        """
        raise NotImplementedError()

    @property
    def universe(self) -> DimensionUniverse:
        """Universe that defines all known compatible dimensions.

        The univers will be compatible with this coordinate
        (`DimensionUniverse`).
        """
        return self.graph.universe

    @property
    @abstractmethod
    def graph(self) -> DimensionGraph:
        """Dimensions identified by this data ID (`DimensionGraph`).

        Note that values are only required to be present for dimensions in
        ``self.graph.required``; all others may be retrieved (from a
        `Registry`) given these.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def has_full(self) -> bool:
        """Whether this data ID contains implied and required values.

        Notes
        -----
        If `True`, `__getitem__`, `get`, and `__contains__` (but not
        `keys`!) will act as though the mapping includes key-value pairs
        for implied dimensions, and the `full` property may be used.  If
        `False`, these operations only include key-value pairs for required
        dimensions, and accessing `full` is an error.  Always `True` if
        there are no implied dimensions.
        """
        raise NotImplementedError()

    def hasFull(self) -> bool:
        """Backwards compatibility method getter for `has_full`.

        New code should use the `has_full` property instead.
        """
        return self.has_full

    @property
    def full(self) -> NamedKeyMapping[Dimension, DataIdValue]:
        """Return mapping for all dimensions in ``self.graph``.

        The mapping includes key-value pairs for all dimensions in
        ``self.graph``, including implied (`NamedKeyMapping`).

        Accessing this attribute if `has_full` is `False` is a logic error
        that may raise an exception of unspecified type either immediately or
        when implied keys are accessed via the returned mapping, depending on
        the implementation and whether assertions are enabled.
        """
        assert self.has_full, "full may only be accessed if has_full is True."
        return _DataCoordinateFullView(self)

    @property
    @abstractmethod
    def has_records(self) -> bool:
        """Whether this data ID contains records.

        Notes
        -----
        If `True`, the following attributes may be accessed:

        - `records`
        - `region`
        - `timespan`
        - `pack`

        If `False`, accessing any of these is considered a logic error.
        """
        raise NotImplementedError()

    def hasRecords(self) -> bool:
        """Backwards compatibility method getter for `has_records`.

        New code should use the `has_records` property instead.
        """
        return self.has_records

    @property
    def records(self) -> NamedKeyMapping[DimensionElement, Optional[DimensionRecord]]:
        """Return the records.

        Returns a  mapping that contains `DimensionRecord` objects for all
        elements identified by this data ID (`NamedKeyMapping`).

        The values of this mapping may be `None` if and only if there is no
        record for that element with these dimensions in the database (which
        means some foreign key field must have a NULL value).

        Accessing this attribute if `has_records` is `False` is a logic
        error that may raise an exception of unspecified type either
        immediately or when the returned mapping is used, depending on the
        implementation and whether assertions are enabled.
        """
        assert self.has_records, "records may only be accessed if has_records is True."
        return _DataCoordinateRecordsView(self)

    @abstractmethod
    def _record(self, name: str) -> Optional[DimensionRecord]:
        """Protected implementation hook that backs the ``records`` attribute.

        Parameters
        ----------
        name : `str`
            The name of a `DimensionElement`, guaranteed to be in
            ``self.graph.elements.names``.

        Returns
        -------
        record : `DimensionRecord` or `None`
            The dimension record for the given element identified by this
            data ID, or `None` if there is no such record.
        """
        raise NotImplementedError()

    @property
    def region(self) -> Optional[Region]:
        """Spatial region associated with this data ID.

        (`lsst.sphgeom.Region` or `None`).

        This is `None` if and only if ``self.graph.spatial`` is empty.

        Accessing this attribute if `has_records` is `False` is a logic
        error that may or may not raise an exception, depending on the
        implementation and whether assertions are enabled.
        """
        assert self.has_records, "region may only be accessed if has_records is True."
        regions = []
        for family in self.graph.spatial:
            element = family.choose(self.graph.elements)
            record = self._record(element.name)
            if record is None or record.region is None:
                return None
            else:
                regions.append(record.region)
        return _intersectRegions(*regions)

    @property
    def timespan(self) -> Optional[Timespan]:
        """Temporal interval associated with this data ID.

        (`Timespan` or `None`).

        This is `None` if and only if ``self.graph.timespan`` is empty.

        Accessing this attribute if `has_records` is `False` is a logic
        error that may or may not raise an exception, depending on the
        implementation and whether assertions are enabled.
        """
        assert self.has_records, "timespan may only be accessed if has_records is True."
        timespans = []
        for family in self.graph.temporal:
            element = family.choose(self.graph.elements)
            record = self._record(element.name)
            # DimensionRecord subclasses for temporal elements always have
            # .timespan, but they're dynamic so this can't be type-checked.
            if record is None or record.timespan is None:
                return None
            else:
                timespans.append(record.timespan)
        return Timespan.intersection(*timespans)

    def pack(self, name: str, *, returnMaxBits: bool = False) -> Union[Tuple[int, int], int]:
        """Pack this data ID into an integer.

        Parameters
        ----------
        name : `str`
            Name of the `DimensionPacker` algorithm (as defined in the
            dimension configuration).
        returnMaxBits : `bool`, optional
            If `True` (`False` is default), return the maximum number of
            nonzero bits in the returned integer across all data IDs.

        Returns
        -------
        packed : `int`
            Integer ID.  This ID is unique only across data IDs that have
            the same values for the packer's "fixed" dimensions.
        maxBits : `int`, optional
            Maximum number of nonzero bits in ``packed``.  Not returned unless
            ``returnMaxBits`` is `True`.

        Notes
        -----
        Accessing this attribute if `has_records` is `False` is a logic
        error that may or may not raise an exception, depending on the
        implementation and whether assertions are enabled.
        """
        assert self.has_records, "pack() may only be called if has_records is True."
        return self.universe.makePacker(name, self).pack(self, returnMaxBits=returnMaxBits)

    def to_simple(self, minimal: bool = False) -> SerializedDataCoordinate:
        """Convert this class to a simple python type.

        This is suitable for serialization.

        Parameters
        ----------
        minimal : `bool`, optional
            Use minimal serialization. If set the records will not be attached.

        Returns
        -------
        simple : `SerializedDataCoordinate`
            The object converted to simple form.
        """
        # Convert to a dict form
        if self.has_full:
            dataId = self.full.byName()
        else:
            dataId = self.byName()
        records: Optional[Dict[str, SerializedDimensionRecord]]
        if not minimal and self.has_records:
            records = {k: v.to_simple() for k, v in self.records.byName().items() if v is not None}
        else:
            records = None

        return SerializedDataCoordinate(dataId=dataId, records=records)

    @classmethod
    def from_simple(cls, simple: SerializedDataCoordinate,
                    universe: Optional[DimensionUniverse] = None,
                    registry: Optional[Registry] = None) -> DataCoordinate:
        """Construct a new object from the simplified form.

        The data is assumed to be of the form returned from the `to_simple`
        method.

        Parameters
        ----------
        simple : `dict` of [`str`, `Any`]
            The `dict` returned by `to_simple()`.
        universe : `DimensionUniverse`
            The special graph of all known dimensions.
        registry : `lsst.daf.butler.Registry`, optional
            Registry from which a universe can be extracted. Can be `None`
            if universe is provided explicitly.

        Returns
        -------
        dataId : `DataCoordinate`
            Newly-constructed object.
        """
        if universe is None and registry is None:
            raise ValueError("One of universe or registry is required to convert a dict to a DataCoordinate")
        if universe is None and registry is not None:
            universe = registry.dimensions
        if universe is None:
            # this is for mypy
            raise ValueError("Unable to determine a usable universe")

        dataId = cls.standardize(simple.dataId, universe=universe)
        if simple.records:
            dataId = dataId.expanded({k: DimensionRecord.from_simple(v, universe=universe)
                                      for k, v in simple.records.items()})
        return dataId

    to_json = to_json_pydantic
    from_json = classmethod(from_json_pydantic)


DataId = Union[DataCoordinate, Mapping[str, Any]]
"""A type-annotation alias for signatures that accept both informal data ID
dictionaries and validated `DataCoordinate` instances.
"""


class _DataCoordinateFullView(NamedKeyMapping[Dimension, DataIdValue]):
    """View class for `DataCoordinate.full`.

    Provides the default implementation for
    `DataCoordinate.full`.

    Parameters
    ----------
    target : `DataCoordinate`
        The `DataCoordinate` instance this object provides a view of.
    """

    def __init__(self, target: DataCoordinate):
        self._target = target

    __slots__ = ("_target",)

    def __repr__(self) -> str:
        terms = [f"{d}: {self[d]!r}" for d in self._target.graph.dimensions.names]
        return "{{{}}}".format(', '.join(terms))

    def __getitem__(self, key: DataIdKey) -> DataIdValue:
        return self._target[key]

    def __iter__(self) -> Iterator[Dimension]:
        return iter(self.keys())

    def __len__(self) -> int:
        return len(self.keys())

    def keys(self) -> NamedValueAbstractSet[Dimension]:
        return self._target.graph.dimensions

    @property
    def names(self) -> AbstractSet[str]:
        # Docstring inherited from `NamedKeyMapping`.
        return self.keys().names


class _DataCoordinateRecordsView(NamedKeyMapping[DimensionElement, Optional[DimensionRecord]]):
    """View class for `DataCoordinate.records`.

    Provides the default implementation for
    `DataCoordinate.records`.

    Parameters
    ----------
    target : `DataCoordinate`
        The `DataCoordinate` instance this object provides a view of.
    """

    def __init__(self, target: DataCoordinate):
        self._target = target

    __slots__ = ("_target",)

    def __repr__(self) -> str:
        terms = [f"{d}: {self[d]!r}" for d in self._target.graph.elements.names]
        return "{{{}}}".format(', '.join(terms))

    def __str__(self) -> str:
        return "\n".join(str(v) for v in self.values())

    def __getitem__(self, key: Union[DimensionElement, str]) -> Optional[DimensionRecord]:
        if isinstance(key, DimensionElement):
            key = key.name
        return self._target._record(key)

    def __iter__(self) -> Iterator[DimensionElement]:
        return iter(self.keys())

    def __len__(self) -> int:
        return len(self.keys())

    def keys(self) -> NamedValueAbstractSet[DimensionElement]:
        return self._target.graph.elements

    @property
    def names(self) -> AbstractSet[str]:
        # Docstring inherited from `NamedKeyMapping`.
        return self.keys().names


class _BasicTupleDataCoordinate(DataCoordinate):
    """Standard implementation of `DataCoordinate`.

    Backed by a tuple of values.

    This class should only be accessed outside this module via the
    `DataCoordinate` interface, and should only be constructed via the static
    methods there.

    Parameters
    ----------
    graph : `DimensionGraph`
        The dimensions to be identified.
    values : `tuple` [ `int` or `str` ]
        Data ID values, ordered to match ``graph._dataCoordinateIndices``.  May
        include values for just required dimensions (which always come first)
        or all dimensions.
    """

    def __init__(self, graph: DimensionGraph, values: Tuple[DataIdValue, ...]):
        self._graph = graph
        self._values = values

    __slots__ = ("_graph", "_values")

    @property
    def graph(self) -> DimensionGraph:
        # Docstring inherited from DataCoordinate.
        return self._graph

    def __getitem__(self, key: DataIdKey) -> DataIdValue:
        # Docstring inherited from DataCoordinate.
        index = self._graph._dataCoordinateIndices[key]
        try:
            return self._values[index]
        except IndexError:
            # Caller asked for an implied dimension, but this object only has
            # values for the required ones.
            raise KeyError(key) from None

    def union(self, other: DataCoordinate) -> DataCoordinate:
        # Docstring inherited from DataCoordinate.
        graph = self.graph.union(other.graph)
        # See if one or both input data IDs is already what we want to return;
        # if so, return the most complete one we have.
        if other.graph == graph:
            if self.graph == graph:
                # Input data IDs have the same graph (which is also the result
                # graph), but may not have the same content.
                # other might have records; self does not, so try other first.
                # If it at least has full values, it's no worse than self.
                if other.has_full:
                    return other
                else:
                    return self
            elif other.has_full:
                return other
            # There's some chance that neither self nor other has full values,
            # but together provide enough to the union to.  Let the general
            # case below handle that.
        elif self.graph == graph:
            # No chance at returning records.  If self has full values, it's
            # the best we can do.
            if self.has_full:
                return self
        # General case with actual merging of dictionaries.
        values = self.full.byName() if self.has_full else self.byName()
        values.update(other.full.byName() if other.has_full else other.byName())
        return DataCoordinate.standardize(values, graph=graph)

    def expanded(self, records: NameLookupMapping[DimensionElement, Optional[DimensionRecord]]
                 ) -> DataCoordinate:
        # Docstring inherited from DataCoordinate
        values = self._values
        if not self.has_full:
            # Extract a complete values tuple from the attributes of the given
            # records.  It's possible for these to be inconsistent with
            # self._values (which is a serious problem, of course), but we've
            # documented this as a no-checking API.
            values += tuple(getattr(records[d.name], d.primaryKey.name) for d in self._graph.implied)
        return _ExpandedTupleDataCoordinate(self._graph, values, records)

    @property
    def has_full(self) -> bool:
        # Docstring inherited from DataCoordinate.
        return len(self._values) == len(self._graph._dataCoordinateIndices)

    @property
    def has_records(self) -> bool:
        # Docstring inherited from DataCoordinate.
        return False

    def _record(self, name: str) -> Optional[DimensionRecord]:
        # Docstring inherited from DataCoordinate.
        assert False


class _ExpandedTupleDataCoordinate(_BasicTupleDataCoordinate):
    """A `DataCoordinate` implementation that can hold `DimensionRecord`
    objects.

    This class should only be accessed outside this module via the
    `DataCoordinate` interface, and should only be constructed via calls to
    `DataCoordinate.expanded`.

    Parameters
    ----------
    graph : `DimensionGraph`
        The dimensions to be identified.
    values : `tuple` [ `int` or `str` ]
        Data ID values, ordered to match ``graph._dataCoordinateIndices``.
        May include values for just required dimensions (which always come
        first) or all dimensions.
    records : `Mapping` [ `str`, `DimensionRecord` or `None` ]
        A `NamedKeyMapping` with `DimensionElement` keys or a regular
        `Mapping` with `str` (`DimensionElement` name) keys and
        `DimensionRecord` values.  Keys must cover all elements in
        ``self.graph.elements``.  Values may be `None`, but only to reflect
        actual NULL values in the database, not just records that have not
        been fetched.
    """

    def __init__(self, graph: DimensionGraph, values: Tuple[DataIdValue, ...],
                 records: NameLookupMapping[DimensionElement, Optional[DimensionRecord]]):
        super().__init__(graph, values)
        assert super().has_full, "This implementation requires full dimension records."
        self._records = records

    __slots__ = ("_records",)

    def expanded(self, records: NameLookupMapping[DimensionElement, Optional[DimensionRecord]]
                 ) -> DataCoordinate:
        # Docstring inherited from DataCoordinate.
        return self

    def union(self, other: DataCoordinate) -> DataCoordinate:
        # Docstring inherited from DataCoordinate.
        graph = self.graph.union(other.graph)
        # See if one or both input data IDs is already what we want to return;
        # if so, return the most complete one we have.
        if self.graph == graph:
            # self has records, so even if other is also a valid result, it's
            # no better.
            return self
        if other.graph == graph:
            # If other has full values, and self does not identify some of
            # those, it's the base we can do.  It may have records, too.
            if other.has_full:
                return other
            # If other does not have full values, there's a chance self may
            # provide the values needed to complete it.  For example, self
            # could be {band} while other could be
            # {instrument, physical_filter, band}, with band unknown.
        # General case with actual merging of dictionaries.
        values = self.full.byName()
        values.update(other.full.byName() if other.has_full else other.byName())
        basic = DataCoordinate.standardize(values, graph=graph)
        # See if we can add records.
        if self.has_records and other.has_records:
            # Sometimes the elements of a union of graphs can contain elements
            # that weren't in either input graph (because graph unions are only
            # on dimensions).  e.g. {visit} | {detector} brings along
            # visit_detector_region.
            elements = set(graph.elements.names)
            elements -= self.graph.elements.names
            elements -= other.graph.elements.names
            if not elements:
                records = NamedKeyDict[DimensionElement, Optional[DimensionRecord]](self.records)
                records.update(other.records)
                return basic.expanded(records.freeze())
        return basic

    @property
    def has_full(self) -> bool:
        # Docstring inherited from DataCoordinate.
        return True

    @property
    def has_records(self) -> bool:
        # Docstring inherited from DataCoordinate.
        return True

    def _record(self, name: str) -> Optional[DimensionRecord]:
        # Docstring inherited from DataCoordinate.
        return self._records[name]

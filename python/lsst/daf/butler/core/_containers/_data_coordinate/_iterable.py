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
    "DataCoordinateIterable",
    "SerializedDataCoordinateList",
)

from abc import abstractmethod
import itertools
from typing import (
    TYPE_CHECKING,
    Callable,
    ClassVar,
    Iterable,
    List,
    Optional,
    Type,
    TypeVar,
)

import pydantic
import sqlalchemy

from ...dimensions import (
    DataCoordinate,
    DataCoordinateCommonState,
    DataId,
    DataIdValue,
    DimensionGraph,
    DimensionUniverse,
    SerializedDataCoordinate,
    SerializedDimensionGraph,
    SerializedDimensionRecord,
)
from ...named import NamedKeyMapping
from ...json import get_universe_for_deserialize, to_json_pydantic, from_json_pydantic
from ...simpleQuery import SimpleQuery

if TYPE_CHECKING:
    from ....registry import Registry
    from .._dimension_record import (
        HeterogeneousDimensionRecordAbstractSet,
        HeterogeneousDimensionRecordMutableSet,
    )
    from ._abstract_set import DataCoordinateAbstractSet
    from ._sequence import DataCoordinateSequence


_S = TypeVar("_S", bound="DataCoordinateIterable")


class SerializedDataCoordinateList(pydantic.BaseModel):
    """Simplified model for saving a homogeneous iterable of IDs."""

    graph: SerializedDimensionGraph
    """Exact dimensions identified by all data IDs.
    """

    dataIds: List[SerializedDataCoordinate]
    """List of serialized data IDs (always saved without records).
    """

    hasFull: bool
    """Whether data IDs identify all dimensions, not just required dimensions.
    """

    records: Optional[List[SerializedDimensionRecord]] = None
    """Deduplicated list of records to attach to data IDs.
    """


class DataCoordinateIterable(Iterable[DataCoordinate]):
    """An abstract base class for homogeneous iterables of data IDs.

    All elements of a `DataCoordinateIterable` identify the same set of
    dimensions (given by the `graph` property) and generally have the same
    `DataCoordinate.has_full` and `DataCoordinate.has_records` flag values.
    """

    __slots__ = ()

    @classmethod
    def standardize(
        cls: Type[_S],
        mappings: Iterable[DataId],
        *,
        graph: Optional[DimensionGraph] = None,
        universe: Optional[DimensionUniverse] = None,
        has_full: Optional[bool] = None,
        has_records: Optional[bool] = None,
        defaults: Optional[DataCoordinate] = None,
        records: Optional[HeterogeneousDimensionRecordAbstractSet] = None,
        **kwargs: DataIdValue,
    ) -> _S:
        """Return an iterable with standardized versions of the given data IDs.

        Parameters
        ----------
        mappings : `Iterable` [ `DataId` ]
            Data IDs to standardize.  Each may be a mapping with `str` keys or
            a `NamedKeyMapping` with `Dimension` keys such as a
            `DataCoordinate` instance.  If these are `DataCoordinate`
            instances, they must all have the same state (dimensions and
            `has_full` / `has_records` flags).  If these are other mappings,
            they must all have the same keys.
        graph : `DimensionGraph`
            The dimensions to be identified by `DataCoordinate` objects in the
            new container.  If not provided, will be inferred from the keys of
            ``mapping`` and ``**kwargs``, and ``universe`` must be provided
            unless ``mapping`` is already a `DataCoordinate`.
        universe : `DimensionUniverse`
            All known dimensions and their relationships; used to expand
            and validate dependencies when ``graph`` is not provided.
        has_full : `bool`
            `True` if all data IDs satisfy `DataCoordinate.has_full`; `False`
            if none do (mixed containers are not permitted).
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
            pairs in each element in ``mappings``, overriding those present.
        Returns
        -------
        standardized : `DataCoordinateIterable` subclass instance
            A `DataCoordinateIterable` with ``subset.graph == graph``.
            May be ``data_ids`` if it is already a `DataCoordinateIterable`
            for immutable classes only if ``graph == self.graph``.  Elements
            are equivalent to those that would be created by calling
            `DataCoordinate.standardize` on all elements in ``self``.

        Notes
        -----
        This method is not abstract itself, but it delegates to abstract
        methods and hence is only callable via concrete subclasses, which
        should always return an instance of their own type (or, in the case of
        subclasses that are intrinsically views, a non-view concrete class with
        the same overall behavior).
        """
        mapping_iter = iter(mappings)
        try:
            first_mapping = next(mapping_iter)
        except StopIteration:
            # No elements; make an empty container.
            if graph is None:
                graph = DimensionUniverse.empty
            if has_full is None:
                has_full = True
            if has_records is None:
                has_records = True
            return cls._wrap((), DataCoordinateCommonState(graph, has_full=has_full, has_records=has_records))
        # Infer output state from first mapping in the iterable and other
        # args.  This is copies the beginning of `DataCoordinate.standardize`
        # a bit, but the real logic is all shared inside the call to
        # `calculate`.
        input_state = None
        input_keys = set(kwargs.keys())
        if isinstance(first_mapping, DataCoordinate):
            input_state = DataCoordinateCommonState.from_data_coordinate(first_mapping)
        elif isinstance(first_mapping, NamedKeyMapping):
            input_keys.update(first_mapping.names)
        elif first_mapping is not None:
            input_keys.update(first_mapping.keys())
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
        # Conform each data ID to the target state, using itertools.chain
        # to put the first element back with the rest, in case we were given a
        # single-pass iterable.
        return cls._wrap(
            itertools.chain(
                (common_state.conform(input_state, first_mapping, defaults=defaults, records=records),),
                (common_state.conform(input_state, each_mapping, defaults=defaults, records=records)
                 for each_mapping in mapping_iter)
            ),
            common_state
        )

    @property
    def graph(self) -> DimensionGraph:
        """Dimensions identified by these data IDs (`DimensionGraph`)."""
        return self._common_state.graph

    @property
    def universe(self) -> DimensionUniverse:
        """Universe that defines all known compatible dimensions
        (`DimensionUniverse`).
        """
        return self.graph.universe

    @classmethod
    @abstractmethod
    def _wrap(cls: Type[_S], iterable: Iterable[DataCoordinate], common: DataCoordinateCommonState) -> _S:
        """Construct a new container from the given data IDs.

        Parameters
        ----------
        iterable : `Iterable` [ `DataCoordinate` ]
            Data IDs to include in the container.
        common : `DataCoordinateCommonState`
            Common state (dimensions, `has_full` / `has_records` flags) that
            all data IDs in ``iterable`` are guaranteed to have;
            implementations need not check this on their own.

        Returns
        -------
        container : `DataCoordinateIterable`
            An instance of a `DataCoordinateIterable` subclass.  Concrete
            subclasses should generally return an instance of their own type;
            when impossible (e.g. classes that are views into other containers)
            they should return a concrete type with similar behavior (i.e.
            another subclass of their immediate ABC).

        Notes
        -----
        This method is conceptually "protected"; it may be called by
        subclasses, but not other code.

        Set-like containers are responsible for removing duplicates, but may
        reorder, while sequence containers should not remove duplicates and
        may not reorder.
        """
        raise NotImplementedError()

    def _unwrap(self) -> Iterable[DataCoordinate]:
        """Return a Python built-in iterable with the same elements as self, if
        possible.

        Returns
        -------
        unwrapped : `Iterable` [ `DataCoordinate` ]
            A Python built-in, if possible, or ``self``, if not.

        Notes
        -----
        This method is conceptually "protected"; it should be called (as well
        as frequently reimplemented) by subclasses, but not other code.  The
        returned object may be internal mutable state (to avoid unnecessary
        copies), but should never actually be modified by the caller.  This
        can be guarded against by always annotating the return type with an
        ABC that has no mutators (e.g. `typing.Sequence` instead of
        `typing.List`).

        Subclasses that define interfaces that mirror specific Python native
        containers should redefine this method with a more specific return
        type.

        The default implementation simply returns `self`.
        """
        return self

    @property
    @abstractmethod
    def _common_state(self) -> DataCoordinateCommonState:
        """State common to data IDs (`DataCoordinateCommonState`)."""
        raise NotImplementedError()

    @property
    def has_full(self) -> bool:
        """Whether data IDs in this iterable identify all dimensions, not just
        required dimensions.

        If `True`, ``all(d.has_full for d in iterable)`` is guaranteed.  If
        `False`, ``all(not d.has_full for d in iterable)`` is guaranteed.
        Mixed iterables are not supported.
        """
        return self._common_state.has_full

    def hasFull(self) -> bool:
        """Backwards compatibility method getter for `has_full`.

        New code should use the `has_full` property instead.
        """
        return self.has_full

    @property
    def has_records(self) -> bool:
        """Whether data IDs in this iterable contain records.

        If `True`, ``all(d.hasRecords() for d in iterable)`` is guaranteed.
        If `False`, ``all(not d.hasRecords() for d in iterable)`` is
        guaranteed.  Mixed iterables are not supported.
        """
        return self._common_state.has_records

    def hasRecords(self) -> bool:
        """Backwards compatibility method getter for `has_records`.

        New code should use the `has_records` property instead.
        """
        return self.has_records

    def to_set(self) -> DataCoordinateAbstractSet:
        """Transform this iterable into a `DataCoordinateAbstractSet`.

        Returns
        -------
        set : `DataCoordinateAbstractSet`
            A `DataCoordinateAbstractSet` instance with the same elements as
            ``self``, after removing any duplicates.  May be ``self`` if it is
            already a `DataCoordinateAbstractSet`.

        Notes
        -----
        This method may consume iterables backed by a single pass iterator.
        """
        from ._frozen_set import DataCoordinateFrozenSet

        return DataCoordinateFrozenSet._wrap(self._unwrap(), self._common_state)

    def to_sequence(self) -> DataCoordinateSequence:
        """Transform this iterable into a `DataCoordinateSequence`.

        Returns
        -------
        seq : `DataCoordinateSequence`
            A new `DatasetCoordinateSequence` with the same elements as
            ``self``, in the same order.  May be ``self`` if it is already a
            `DataCoordinateSequence`.

        Notes
        -----
        This method may consume iterables backed by a single pass iterator.
        """
        from ._tuple import DataCoordinateTuple

        return DataCoordinateTuple._wrap(self._unwrap(), self._common_state)

    def expect_one(self) -> DataCoordinate:
        """Assume that this container has exactly one data ID, and return it.

        Returns
        -------
        record : `DataCoordinate`
            The only data ID in this container.

        Raises
        ------
        RuntimeError
            Raised if the container has zero data IDs, or more than one.

        Notes
        -----
        This may exhaust ``self`` if it is a single-pass iterator.
        """
        try:
            (result,) = self
        except ValueError as err:
            raise RuntimeError("Expected exactly one record.") from err
        return result

    def constrain(self, query: SimpleQuery, columns: Callable[[str], sqlalchemy.sql.ColumnElement]) -> None:
        """Constrain a SQL query to include or relate to only known data IDs.

        Parameters
        ----------
        query : `SimpleQuery`
            Struct that represents the SQL query to constrain, either by
            appending to its WHERE clause, joining a new table or subquery,
            or both.
        columns : `Callable`
            A callable that accepts `str` dimension names and returns
            SQLAlchemy objects representing a column for that dimension's
            primary key value in the query.

        Notes
        -----
        This method may consume iterables backed by a single pass iterator.
        """
        toOrTogether: List[sqlalchemy.sql.ColumnElement] = []
        for dataId in self:
            toOrTogether.append(
                sqlalchemy.sql.and_(
                    *[columns(dimension.name) == dataId[dimension.name] for dimension in self.graph.required]
                )
            )
        query.where.append(sqlalchemy.sql.or_(*toOrTogether))

    def subset(self, graph: DimensionGraph) -> DataCoordinateIterable:
        """Return a `DataCoordinateIterable` whose dimensions are a subset of
        ``self.graph``.

        This is equivalent to calling `DataCoordinate.subset` on each data ID
        in the iterable.

        Parameters
        ----------
        graph : `DimensionGraph`
            The dimensions identified by the returned `DataCoordinate`.

        Returns
        -------
        iterable : `DataCoordinateIterable`
            A `DataCoordinateIterable` instance that identifies only the given
            dimensions.

        Raises
        ------
        KeyError
            Raised if needed dimension values are not present.  See
            `DataCoordinate.subset` for details.
        """
        target_state = DataCoordinateCommonState.calculate(self._common_state, target_graph=graph)
        return self._wrap(
            (target_state.conform(self._common_state, data_id) for data_id in self),
            target_state,
        )

    def to_simple(self, minimal: bool = False) -> SerializedDataCoordinateList:
        """Convert this class to a simple Python type.

        This is suitable for serialization.

        Parameters
        ----------
        minimal : bool, optional
            Use minimal serialization.  Does nothing for this class.

        Returns
        -------
        simple : `SerializedDataCoordinateList`
            The object converted to simple form.

        """
        from .._dimension_record import HeterogeneousDimensionRecordSet

        normalized_records = HeterogeneousDimensionRecordSet(self.universe)
        serialized_data_ids = []
        for data_id in self:  # Need to iterate in one pass in case this is an iterator
            serialized_data_ids.append(data_id.to_simple(minimal=True))
            normalized_records.update_from(data_id)
        return SerializedDataCoordinateList(
            graph=self.graph.to_simple(),
            dataIds=serialized_data_ids,
            hasFull=self.hasFull(),
            records=[r.to_simple() for r in normalized_records] if normalized_records else None,
        )

    @classmethod
    def from_simple(
        cls,
        simple: SerializedDataCoordinateList,
        universe: Optional[DimensionUniverse] = None,
        registry: Optional[Registry] = None,
        records: Optional[HeterogeneousDimensionRecordMutableSet] = None,
    ) -> DataCoordinateIterable:
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
        records : `HeterogeneousDimensionRecordMutableSet`, optional
            Container of `DimensionRecord` instances that may be used to
            fill in missing keys and/or attach records.  If provided, the
            returned object is guaranteed to have `hasRecords` return `True`.
            If provided and records were also serialized directly with the
            data IDs, the serialized records take precedence, and any found
            are inserted into this container.

        Returns
        -------
        dataIds : `DataCoordinateIterable`
            Newly-constructed object.
        """
        # TODO: allow hasFull/hasRecords to be passed and used here.
        from .._dimension_record import HeterogeneousDimensionRecordSet
        universe = get_universe_for_deserialize("DataCoordinateIterable", universe, registry, records)
        records = HeterogeneousDimensionRecordSet.deserialize_and_merge(universe, simple.records, records)
        graph = DimensionGraph.from_simple(simple.graph, universe=universe, registry=registry)
        return cls._wrap(
            (
                DataCoordinate.from_simple(serialized_data_id, universe=universe, records=records)
                for serialized_data_id in simple.dataIds
            ),
            DataCoordinateCommonState(graph, has_full=simple.hasFull, has_records=bool(records)),
        )

    _serializedType: ClassVar[type] = SerializedDataCoordinateList

    to_json = to_json_pydantic
    from_json = classmethod(from_json_pydantic)

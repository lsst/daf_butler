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
    "DatasetAbstractSet",
    "DatasetMutableSet",
    "GroupingDatasetAbstractSet",
    "GroupingDatasetMutableSet",
    "HomogeneousDatasetAbstractSet",
    "HomogeneousDatasetMutableSet",
)

import itertools
from abc import abstractmethod
from typing import (
    Any,
    Collection,
    Dict,
    Generic,
    Iterable,
    Iterator,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Type,
    TypeVar,
    TYPE_CHECKING,
)

import pydantic

from ...datasets import DatasetRef, DatasetType, SerializedDatasetType, SerializedDatasetRef
from ...dimensions import DimensionUniverse, SerializedDimensionRecord
from ...json import get_universe_for_deserialize, from_json_pydantic, to_json_pydantic
from ...named import NamedKeyDict, NamedKeyMapping, NamedKeyMutableMapping
from .._dimension_record import HeterogeneousDimensionRecordMutableSet, HeterogeneousDimensionRecordSet
from ._iterables import DatasetIterable, HomogeneousDatasetIterable

if TYPE_CHECKING:
    from ....registry import Registry

_K = TypeVar("_K")
_G = TypeVar("_G", bound="GroupingDatasetAbstractSet")
_H = TypeVar("_H", bound="HomogeneousDatasetAbstractSet")
_H_mut = TypeVar("_H_mut", bound="HomogeneousDatasetMutableSet")


class SerializedHomogeneousDatasetList(pydantic.BaseModel):
    """Simplified model for saving homogeneous sets of DatasetRefs."""

    datasetType: SerializedDatasetType
    """Full definitions of all DatasetTypes represented in the iterable.
    """

    refs: List[SerializedDatasetRef]
    """Serialized DatasetRefs (without DatasetType or records).
    """

    allResolved: bool
    """`True` if all datasets in this container are required to be resolved
    (i.e. have `DatasetRef.id` not `None`), `False` otherwise (`bool`).
    """

    allUnresolved: bool
    """`True` if all datasets in this container are required to be unresolved
    (have a `DatasetRef.id` that is `None`), `False` otherwise (`bool`).
    """

    records: Optional[List[SerializedDimensionRecord]] = None
    """Deduplicated list of records to attach to data IDs.

    This may be `None` either if there are no records, or if they are being
    saved at a higher level.
    """


class SerializedGroupingDatasetList(pydantic.BaseModel):
    """Simplified model for saving heterogeneous sets of DatasetRefs."""

    byDatasetType: List[SerializedHomogeneousDatasetList]
    """Nested homogeneous dataset containers.
    """

    allResolved: bool
    """`True` if all datasets in this container are required to be resolved
    (i.e. have `DatasetRef.id` not `None`), `False` otherwise (`bool`).
    """

    allUnresolved: bool
    """`True` if all datasets in this container are required to be unresolved
    (have a `DatasetRef.id` that is `None`), `False` otherwise (`bool`).
    """

    records: Optional[List[SerializedDimensionRecord]] = None
    """Deduplicated list of records to attach to data IDs.
    """


class DatasetAbstractSet(DatasetIterable, Collection[DatasetRef], Generic[_K]):
    """Generic abstract base class for set-like dataset containers.

    Notes
    -----
    This class is only informally set-like; it does not inherit from or fully
    implement the `collections.abc.Set` interface, but it is a unique-element
    container with no ordering guarantees.  This is for three reasons:

    - simplicity (the `~collections.abc.Set` methods that return new objects
      are subtly tricky to get right);

    - to allow us better guard against accidental misuse by raising exceptions
      when a true `~collections.abc.Set` would not;

    - to deal with the fact that `DatasetRef` equality requires all of
      `DatasetRef.datasetType`, `DatasetRef.id`, and `DatasetRef.dataId` to be
      equal, but useful sets of `DatasetRef` only want to define equality on
      `DatasetType` and *one of* the latter two.

    Additional `~collections.abc.Set` methods may be added in the future as
    needed.

    This class is both generic over the type of *and* expected to be
    specialized on the `DatasetRef` attribute that is used (with the
    `DatasetType`) to define element equality, e.g. `DataCoordinate` or
    `DatasetId`.

    This class provides default implementations for `__iter__`, `__len__`, and
    `__contains__` that delegate to the abstract `by_dataset_type` property,
    assuming an implementation backed by a mapping of homogeneous abstract
    sets.  When this is not true (e.g. for homogeneneous sets for which
    `by_dataset_types` includes ``self``), these methods must be reimplemented.
    """

    @property
    @abstractmethod
    def by_dataset_type(self) -> NamedKeyMapping[DatasetType, HomogeneousDatasetAbstractSet[_K]]:
        """A mapping view of the set that groups by `DatasetType`.

        Notes
        -----
        When there are no `DatasetRef` objects with a particular `DatasetType`
        in this iterable, there may or may not be a `DatasetType` key
        associated with an empty-iterable values in this view.  This means that
        `by_dataset_type.keys()` is *not* a reliable way to extract the
        dataset types actually present in the container.

        Specializations that provide a mutable view must be mutated accordingly
        when the view is mutated.
        """
        raise NotImplementedError()


class DatasetMutableSet(DatasetAbstractSet[_K]):
    """Generic abstract base class for mutable set-like dataset containers.

    Notes
    -----
    This class is only informally set-like; it does not inherit from or fully
    implement the `collections.abc.Set` interface.  See `DatasetAbstractSet`
    for details.
    """

    @abstractmethod
    def add(self, ref: DatasetRef) -> None:
        """Add a new dataset to the container.

        Parameters
        ----------
        ref : `DatasetRef`
            Dataset to add.  If `all_unresolved` is `True`, an unresolved
            version of the reference is added.

        Raises
        ------
        AmbiguousDatasetError
            Raised if `DatasetRef.id` is `None` but `all_resolved` is `True`.
        TypeError
            Raised if ``self`` is a homogeneous set and
            ``DatasetRef.datasetType` is not equal to ``self.dataset_type``.

        Notes
        -----
        Provides strong exception safety; never modifies the container if an
        error occurs.
        """
        raise NotImplementedError()

    @abstractmethod
    def update(self, refs: Iterable[DatasetRef]) -> None:
        """Add multiple new datasets to the container.

        Parameters
        ----------
        refs : `Iterable` [ `DatasetRef` ]
            Datasets to add.  If `all_unresolved` is `True`, an unresolved
            version of each reference is added.

        Raises
        ------
        AmbiguousDatasetError
            Raised if `DatasetRef.id` is `None` for any given dataset but
            `all_resolved` is `True`.
        TypeError
            Raised if ``self`` is a homogeneous set and any
            ``DatasetRef.datasetType` is not equal to ``self.dataset_type``.

        Notes
        -----
        This interface provides only basic exception safety; if an error
        occurs, some updates may nevertheless occur (specializations *may*
        provide additional strong safety).
        """
        raise NotImplementedError()

    @abstractmethod
    def remove(self, ref: DatasetRef) -> None:
        """Remove a dataset from the container.

        Parameters
        ----------
        ref : `DatasetRef`
            Dataset to remove. If `all_unresolved` is `True`, an unresolved
            version of the reference is removed.  If not, the resolution of
            the given reference must match the resolution of the reference
            in the container.

        Raises
        ------
        KeyError
            Raised if this dataset ref is not in the set.

        Notes
        -----
        Provides strong exception safety; never modifies the container if an
        error occurs.
        """
        raise NotImplementedError()

    def clear(self) -> None:
        """Remove all datasets from the container."""
        raise NotImplementedError()


class HomogeneousDatasetAbstractSet(HomogeneousDatasetIterable, DatasetAbstractSet[_K]):
    """Generic abstract base class for set-like dataset containers that are
    constrained to a single `DatasetType`.

    Notes
    -----
    This class is only informally set-like; it does not inherit from or fully
    implement the `collections.abc.Set` interface.  See `DatasetAbstractSet`
    for details.

    This class is both generic over the type of *and* expected to be
    specialized on the `DatasetRef` attribute that is used (with the
    `DatasetType`) to define element equality, e.g. `DataCoordinate` or
    `DatasetId`.

    This class provides default implementations for `__iter__`, `__len__`, and
    `__contains__` that delegate to the abstract `as_mapping` property.  It
    provides an implementation of `by_dataset_type` that returns ``self``.
    """

    def __iter__(self) -> Iterator[DatasetRef]:
        return iter(self.as_mapping.values())

    def __len__(self) -> int:
        return len(self.as_mapping)

    def __contains__(self, key: Any) -> bool:
        if isinstance(key, DatasetRef):
            return self._get_key(key) in self.as_mapping
        raise TypeError(f"Only DatasetRefs can be present in this container; got {key} of type {type(key)}.")

    @property
    def by_dataset_type(self) -> NamedKeyMapping[DatasetType, HomogeneousDatasetAbstractSet]:
        # Docstring inherited.
        if self:
            return NamedKeyDict({self.dataset_type: self}).freeze()
        else:
            return NamedKeyDict().freeze()

    @property
    @abstractmethod
    def as_mapping(self) -> Mapping[_K, DatasetRef]:
        """A mapping view of the container."""
        raise NotImplementedError()

    def to_simple(
        self, minimal: bool = False, records: Optional[HeterogeneousDimensionRecordMutableSet] = None
    ) -> SerializedHomogeneousDatasetList:
        """Construct an equivalent simple object suitable for serialization.

        Parameters
        ----------
        minimal : `bool`, optional
            If `True`, drop information that can be reconstructed given a
            `Registry`.
        records : `HeterogeneousDimensionRecordMutableSet`, optional
            If provided, an external set of `DimensionRecord` objects that
            any nested records should be added to instead of serializing them
            internally.  This allows these records to be normalized and saved
            with less duplication by higher-level objects.

        Returns
        -------
        simple : `SerializedHomogeneousDatasetList`
            A serializable version of this object.
        """
        if records is None:
            records = HeterogeneousDimensionRecordSet(self.universe)
            internal_records = True
        else:
            internal_records = False
        refs = []
        for ref in self:
            refs.append(ref.to_simple(minimal=minimal, normalized=True))
            records.update_from(ref.dataId)
        return SerializedHomogeneousDatasetList(
            datasetType=self.dataset_type.to_simple(minimal=False),
            refs=refs,
            allResolved=self.all_resolved,
            allUnresolved=self.all_unresolved,
            records=[r.to_simple(minimal=minimal) for r in records] if internal_records else None,
        )

    @classmethod
    def from_simple(
        cls: Type[_H],
        simple: SerializedHomogeneousDatasetList,
        universe: Optional[DimensionUniverse] = None,
        registry: Optional[Registry] = None,
        records: Optional[HeterogeneousDimensionRecordMutableSet] = None,
    ) -> _H:
        """Construct a new object from the simplified form.

        The data is assumed to be of the form returned from the `to_simple`
        method.

        Parameters
        ----------
        simple : `SerializedHomogeneousDatasetList`
            An object returned by `to_simple`.
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
        datasets : `HomogeneousDatasetAbstractSet`
            Newly-constructed object.

        Notes
        -----
        This class method can only be invoked on concrete subclasses; it is not
        abstract itself, but it delegates to `_from_iterable`, which is.

        Serialized objects written by one `HomogeneousDatasetAbstractSet`
        subclass can generally be read by any other, as long as the
        `DatasetRef` objects satisfy the resolution requirements of the set
        doing the reading (in particular, sets that use dataset ID equality
        require resolved datasets).
        """
        universe = get_universe_for_deserialize("HomogeneousDatasetAbstractSet", universe, registry, records)
        records = HeterogeneousDimensionRecordSet.deserialize_and_merge(universe, simple.records, records)
        dataset_type = DatasetType.from_simple(simple.datasetType, universe=universe, registry=registry)
        return cls._from_iterable(
            dataset_type,
            (
                DatasetRef.from_simple(s, universe=universe, records=records, registry=registry)
                for s in simple.refs
            ),
            all_resolved=simple.allResolved,
            all_unresolved=simple.allUnresolved,
        )

    _serializedType: SerializedHomogeneousDatasetList
    to_json = to_json_pydantic
    from_json = classmethod(from_json_pydantic)

    @classmethod
    @abstractmethod
    def _key_type(cls) -> Any:
        """Return an equality-comparable object that describes the type of the
        `DatsetRef` attribute used (with `DatasetType`) for equality.

        In other words, this is always either `DataCoordinate` or `DatasetId`.

        This is a conceptually protected method: it should be implemented by
        derived classes but not called by external code.

        Notes
        -----
        We can't declare that this returns a `type` because `DatasetId` is
        actually a type-annotation `typing.Union` instance.  But that works
        fine because all we need is equality comparision.
        """
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def _get_key(cls, ref: DatasetRef) -> _K:
        """Extract the attribute used (with `DatasetType`) to define equality
        from a `DatasetRef`.

        This is a conceptually protected method: it should be implemented by
        derived classes but not called by external code.

        Parameters
        ----------
        ref : `DatasetRef`
            Reference to extract the key attribute from.

        Returns
        -------
        key : `int`, `uuid.UUID`, or `DataCoordinate`
            Either `DatasetRef.id` or `DatasetRef.dataId`.
        """
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def _from_iterable(
        cls: Type[_H],
        dataset_type: DatasetType,
        dict: Iterable[DatasetRef],
        all_resolved: bool,
        all_unresolved: bool,
    ) -> _H:
        """Construct an instance of this class from an iterable of
        `DatasetRef` objects.

        This is a conceptually protected method: it should be implemented by
        derived classes but not called by external code.

        Parameters
        ----------
        dataset_type : `DatasetType`
            Dataset type for all datasets in this container.
        datasets : `Iterable` [ `DatasetRef` ]
            Initial datasets for this container.
        all_resolved : `bool`
            Whether all `DatasetRef` instances in this container must be
            resolved (have a `DatasetRef.id` that is not `None`).
        all_unresolved : `bool`
            Whether all `DatasetRef` instances in this container must be
            unresolved (have a `DatasetRef.id` that is `None`).

        Raises
        ------
        AmbiguousDatasetError
            Raised if `DatasetRef.id` is `None` for any given dataset but
            `all_resolved` is `True`.
        TypeError
            Raised if ``ref.datasetType != self.dataset_type`` for any input
            `DatasetRef`.

        Notes
        -----
        It is expected that most concrete subclasses will have a constructor
        with a signature very similar to this, but having the base class call
        a `classmethod` instead is both more flexible and more explicit.
        """
        raise NotImplementedError()

    def _add_to_mapping(self, ref: DatasetRef, mapping: MutableMapping[_K, DatasetRef]) -> None:
        """Helper method that checks that a `DatasetRef` meets the requirements
        of this container and then adds it to a mutable mapping.

        This is a conceptually protected method: it may be called by derived
        classes but not external code.

        Parameters
        ----------
        ref : `DatasetRef`
            Dataset to add.
        mapping : `dict`
            Dictionary to add the dataset to.

        Notes
        -----
        This may be called in constructors, but only after any state used to
        implement the `dataset_type`, `all_resolved`, and `all_unresolved`
        properties has been initialized.
        """
        if ref.datasetType != self.dataset_type:
            raise TypeError(
                f"Incorrect dataset type for this container; expected {self.dataset_type}, but got {ref}."
            )
        if self.all_resolved:
            ref.getCheckedId()
        elif self.all_unresolved:
            ref = ref.unresolved()
        mapping[self._get_key(ref)] = ref

    def _update_mapping(self, refs: Iterable[DatasetRef], mapping: MutableMapping[_K, DatasetRef]) -> None:
        """Helper method that checks that multiple `DatasetRef` instances meet
        the requirements of this container and then adds them to a mutable
        mapping.

        This is a conceptually protected method: it may be called by derived
        classes but not external code.

        Parameters
        ----------
        refs : `Iterable` [ `DatasetRef` ]
            Datasets to add.
        mapping : `dict`
            Dictionary to add the dataset to.

        Notes
        -----
        This may be called in constructors, but only after any state used to
        implement the `dataset_type`, `all_resolved`, and `all_unresolved`
        properties has been initialized.
        """
        if (
            isinstance(refs, HomogeneousDatasetAbstractSet)
            and refs._key_type() == self._key_type()
            and refs.dataset_type == self.dataset_type
            and (not self.all_resolved or refs.all_resolved)
            and (not self.all_unresolved or refs.all_unresolved)
        ):
            # Fast case: given refs are in a container that makes the same
            # guarantees as self, so we can delegate to MutableMapping.update
            # (which probably boils down to a C loop, because both args are
            # probably dicts).
            mapping.update(refs.as_mapping)
        else:
            # Slow, more general case.
            # Extract into a temporary dict first for exception safety.
            tmp: Dict[_K, DatasetRef] = {}
            for ref in refs:
                self._add_to_mapping(ref, tmp)
            mapping.update(mapping)


class HomogeneousDatasetMutableSet(HomogeneousDatasetAbstractSet[_K], DatasetMutableSet[_K]):
    """Generic abstract base class for mutable set-like dataset containers that
    are constrained to a single `DatasetType`.

    Notes
    -----
    This class is only informally set-like; it does not inherit from or fully
    implement the `collections.abc.Set` interface.  See `DatasetAbstractSet`
    for details.

    This class is both generic over the type of *and* expected to be
    specialized on the `DatasetRef` attribute that is used (with the
    `DatasetType`) to define element equality, e.g. `DataCoordinate` or
    `DatasetId`.

    This class provides default implementations for `add`, `update`, `remove`,
    and `clear`, that delegate to the abstract `as_mapping` property.
    """

    @property
    @abstractmethod
    def as_mapping(self) -> MutableMapping[_K, DatasetRef]:
        # Docstring inherited.
        raise NotImplementedError()

    def add(self, ref: DatasetRef) -> None:
        # Docstring inherited.
        self._add_to_mapping(ref, self.as_mapping)

    def update(self, refs: Iterable[DatasetRef]) -> None:
        # Docstring inherited.
        self._update_mapping(refs, self.as_mapping)

    def remove(self, ref: DatasetRef) -> None:
        # Docstring inherited.
        del self.as_mapping[self._get_key(ref)]

    def clear(self) -> None:
        # Docstring inherited.
        self.as_mapping.clear()


class GroupingDatasetAbstractSet(DatasetAbstractSet[_K], Generic[_K, _H]):
    """Generic abstract base class for set-like dataset containers that
    are backed by a mapping of homogeneous set-like containers.

    Notes
    -----
    This class is only informally set-like; it does not inherit from or fully
    implement the `collections.abc.Set` interface.  See `DatasetAbstractSet`
    for details.

    This class has two type parameters.  The first is the type of the
    `DatasetRef` attribute it uses (with the `DatasetType`) to define equality,
    i.e. `DataCoordinate` or `DatasetId`.  The second is the type of the
    homogeneous sets that are used as the values of `by_dataset_type`; this
    must use the same equality attribute.  This avoids subtle problems with
    type covariance and mutable mappings.

    This class provides default implementations for `__iter__`, `__len__`, and
    `__contains__` that delegate to the abstract `by_dataset_type` property,
    assuming an implementation backed by a mapping of homogeneous abstract
    sets.
    """

    @property
    @abstractmethod
    def by_dataset_type(self) -> NamedKeyMapping[DatasetType, _H]:
        # Docstring inherited.
        raise NotImplementedError()

    def __iter__(self) -> Iterator[DatasetRef]:
        return itertools.chain.from_iterable(self.by_dataset_type.values())

    def __len__(self) -> int:
        return sum(len(v) for v in self.by_dataset_type.values())

    def __contains__(self, key: Any) -> bool:
        if isinstance(key, DatasetRef):
            if (nested := self.by_dataset_type.get(key.datasetType)) is not None:
                return key in nested
            else:
                return False
        raise TypeError(f"Only DatasetRefs can be present in this container; got {key} of type {type(key)}.")

    def to_simple(
        self, minimal: bool = False, records: Optional[HeterogeneousDimensionRecordMutableSet] = None
    ) -> SerializedGroupingDatasetList:
        """Construct an equivalent simple object suitable for serialization.

        Parameters
        ----------
        minimal : `bool`, optional
            If `True`, drop information that can be reconstructed given a
            `Registry`.
        records : `HeterogeneousDimensionRecordMutableSet`, optional
            If provided, an external set of `DimensionRecord` objects that
            any nested records should be added to instead of serializing them
            internally.  This allows these records to be normalized and saved
            with less duplication by higher-level objects.

        Returns
        -------
        simple : `SerializedGroupingDatasetList`
            A serializable version of this object.
        """
        if records is None:
            records = HeterogeneousDimensionRecordSet(self.universe)
            internal_records = True
        else:
            internal_records = False
        byDatasetType = [
            refs.to_simple(minimal=minimal, records=records) for refs in self.by_dataset_type.values()
        ]
        return SerializedGroupingDatasetList(
            byDatasetType=byDatasetType,
            allResolved=self.all_resolved,
            allUnresolved=self.all_unresolved,
            records=[r.to_simple(minimal=minimal) for r in records] if internal_records else None,
        )

    @classmethod
    def from_simple(
        cls: Type[_G],
        simple: SerializedGroupingDatasetList,
        universe: Optional[DimensionUniverse] = None,
        registry: Optional[Registry] = None,
        records: Optional[HeterogeneousDimensionRecordMutableSet] = None,
    ) -> _G:
        """Construct a new object from the simplified form.

        The data is assumed to be of the form returned from the `to_simple`
        method.

        Parameters
        ----------
        simple : `SerializedGroupingDatasetList`
            An object returned by `to_simple`.
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
        datasets : `GroupingDatasetAbstractSet`
            Newly-constructed object.

        Notes
        -----
        This class method can only be invoked on concrete subclasses; it is not
        abstract itself, but it delegates to `_from_nested`, which is.

        Serialized objects written by one `GroupingDatasetAbstractSet`
        subclass can generally be read by any other, as long as the
        `DatasetRef` objects satisfy the resolution requirements of the set
        doing the reading (in particular, sets that use dataset ID equality
        require resolved datasets).
        """
        universe = get_universe_for_deserialize("GroupingDatasetAbstractSet", universe, registry, records)
        records = HeterogeneousDimensionRecordSet.deserialize_and_merge(universe, simple.records, records)
        return cls._from_nested(
            universe,
            (
                cls._nested_type().from_simple(s, universe=universe, registry=registry, records=records)
                for s in simple.byDatasetType
            ),
            all_resolved=simple.allResolved,
            all_unresolved=simple.allUnresolved,
        )

    _serializedType: SerializedGroupingDatasetList
    to_json = to_json_pydantic
    from_json = classmethod(from_json_pydantic)

    @classmethod
    @abstractmethod
    def _from_nested(
        cls: Type[_G],
        universe: DimensionUniverse,
        nested: Iterable[_H],
        all_resolved: bool,
        all_unresolved: bool,
    ) -> _G:
        # TODO: docs
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def _nested_type(cls) -> Type[_H]:
        """Return the `type` of the homogeneous set used as values in
        `by_dataset_type`.

        This is a conceptually protected method: it should be implemented by
        derived classes but not called by external code.
        """
        raise NotImplementedError()


class GroupingDatasetMutableSet(GroupingDatasetAbstractSet[_K, _H_mut], DatasetMutableSet[_K]):
    """Generic abstract base class for mutable set-like dataset containers that
    are backed by a mutable mapping of homogeneous set-like containers.

    Notes
    -----
    This class is only informally set-like; it does not inherit from or fully
    implement the `collections.abc.Set` interface.  See `DatasetAbstractSet`
    for details.

    This class has two type parameters; see `GroupingDatasetAbstractSet` for
    details.

    This class provides implementations of `add`, `update`, `remove`, and
    `clear` that delegate to `by_dataset_type` (which must be a mutable
    mapping) and `_make_nested`.
    """

    @property
    @abstractmethod
    def by_dataset_type(self) -> NamedKeyMutableMapping[DatasetType, _H_mut]:
        # Docstring inherited.
        raise NotImplementedError()

    def add(self, ref: DatasetRef) -> None:
        # Docstring inherited.
        if (nested := self.by_dataset_type.get(ref.datasetType)) is None:
            nested = self._nested_type()._from_iterable(
                ref.datasetType,
                (ref,),
                all_resolved=self.all_resolved,
                all_unresolved=self.all_unresolved,
            )
            self.by_dataset_type[ref.datasetType] = nested
        else:
            nested.add(ref)

    def update(self, refs: Iterable[DatasetRef]) -> None:
        # Docstring inherited.
        if isinstance(refs, DatasetAbstractSet):
            # *Potentially* fast path: extract nested homogeneous sets and
            # update each of them.  Whether this is actually faster depends on
            # whether the nested homogeneous sets can take their own fast path,
            # too.
            for dataset_type, refs_for_dataset_type in refs.by_dataset_type.items():
                if (nested := self.by_dataset_type.get(dataset_type)) is None:
                    nested = self._nested_type()._from_iterable(
                        dataset_type,
                        refs_for_dataset_type,
                        all_resolved=self.all_resolved,
                        all_unresolved=self.all_unresolved,
                    )
                    self.by_dataset_type[dataset_type] = nested
                else:
                    nested.update(refs_for_dataset_type)
        else:
            # More general slower path for single-pass or ungrouped iterables.
            for ref in refs:
                self.add(ref)

    def remove(self, ref: DatasetRef) -> None:
        # Docstring inherited.
        nested = self.by_dataset_type[ref.datasetType]
        nested.remove(ref)
        if not nested:
            del self.by_dataset_type[nested.dataset_type]

    def clear(self) -> None:
        # Docstring inherited.
        self.by_dataset_type.clear()

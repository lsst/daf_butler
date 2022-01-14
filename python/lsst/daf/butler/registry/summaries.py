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
    "CollectionSummary",
    "GovernorDimensionRestriction",
)

import itertools
from dataclasses import dataclass
from typing import (
    AbstractSet,
    Any,
    ItemsView,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Set,
    Union,
    ValuesView,
)

from lsst.utils.iteration import ensure_iterable

from ..core import (
    DataCoordinate,
    DatasetType,
    DimensionUniverse,
    GovernorDimension,
    NamedKeyDict,
    NamedKeyMapping,
    NamedValueAbstractSet,
    NamedValueSet,
)


class GovernorDimensionRestriction(NamedKeyMapping[GovernorDimension, AbstractSet[str]]):
    """A custom mapping that represents a restriction on the values one or
    more governor dimensions may take in some context.

    Parameters
    ----------
    mapping : `NamedKeyDict` [ `GovernorDimension`, `Set` [ `str` ]]
        Mapping from governor dimension to the values it may take.  Dimensions
        not present in the mapping are not constrained at all.
    """

    def __init__(self, mapping: NamedKeyDict[GovernorDimension, Set[str]]):
        self._mapping = mapping

    @classmethod
    def makeEmpty(cls, universe: DimensionUniverse) -> GovernorDimensionRestriction:
        """Construct a `GovernorDimensionRestriction` that allows no values
        for any governor dimension in the given `DimensionUniverse`.

        Parameters
        ----------
        universe : `DimensionUniverse`
            Object that manages all dimensions.

        Returns
        -------
        restriction : `GovernorDimensionRestriction`
            Restriction instance that maps all governor dimensions to an empty
            set.
        """
        return cls(NamedKeyDict((k, set()) for k in universe.getGovernorDimensions()))

    @classmethod
    def makeFull(cls) -> GovernorDimensionRestriction:
        """Construct a `GovernorDimensionRestriction` that allows any value
        for any governor dimension.

        Returns
        -------
        restriction : `GovernorDimensionRestriction`
            Restriction instance that contains no keys, and hence contains
            allows any value for any governor dimension.
        """
        return cls(NamedKeyDict())

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, GovernorDimensionRestriction):
            return False
        return self._mapping == other._mapping

    def __str__(self) -> str:
        return "({})".format(
            ", ".join(f"{dimension.name}: {values}" for dimension, values in self._mapping.items())
        )

    def __repr__(self) -> str:
        return "GovernorDimensionRestriction({})".format(
            ", ".join(f"{dimension.name}={values}" for dimension, values in self._mapping.items())
        )

    def __iter__(self) -> Iterator[GovernorDimension]:
        return iter(self._mapping)

    def __len__(self) -> int:
        return len(self._mapping)

    @property
    def names(self) -> AbstractSet[str]:
        # Docstring inherited.
        return self._mapping.names

    def keys(self) -> NamedValueAbstractSet[GovernorDimension]:  # type: ignore
        return self._mapping.keys()

    def values(self) -> ValuesView[AbstractSet[str]]:
        return self._mapping.values()

    def items(self) -> ItemsView[GovernorDimension, AbstractSet[str]]:
        return self._mapping.items()

    def __getitem__(self, key: Union[str, GovernorDimension]) -> AbstractSet[str]:
        return self._mapping[key]

    def copy(self) -> GovernorDimensionRestriction:
        """Return a deep copy of this object.

        Returns
        -------
        copy : `GovernorDimensionRestriction`
            A copy of ``self`` that can be modified without modifying ``self``
            at all.
        """
        return GovernorDimensionRestriction(NamedKeyDict((k, set(v)) for k, v in self.items()))

    def add(self, dimension: GovernorDimension, value: str) -> None:
        """Add a single dimension value to the restriction.

        Parameters
        ----------
        dimension : `GovernorDimension`
            Dimension to update.
        value : `str`
            Value to allow for this dimension.
        """
        current = self._mapping.get(dimension)
        if current is not None:
            current.add(value)

    def update(self, other: Mapping[GovernorDimension, Union[str, Iterable[str]]]) -> None:
        """Update ``self`` to include all dimension values in either ``self``
        or ``other``.

        Parameters
        ----------
        other : `Mapping` [ `Dimension`, `str` or `Iterable` [ `str` ] ]
            Mapping to union into ``self``.  This may be another
            `GovernorDimensionRestriction` or any other mapping from dimension
            to `str` or iterable of `str`.
        """
        for dimension in self.keys() - other.keys():
            self._mapping.pop(dimension, None)
        for dimension in self.keys() & other.keys():
            self._mapping[dimension].update(ensure_iterable(other[dimension]))
        # Dimensions that are in 'other' but not in 'self' are ignored, because
        # 'self' says they are already unconstrained.

    def union(
        self, *others: Mapping[GovernorDimension, Union[str, Iterable[str]]]
    ) -> GovernorDimensionRestriction:
        """Construct a restriction that permits any values permitted by any of
        the input restrictions.

        Parameters
        ----------
        *others : `Mapping` [ `Dimension`, `str` or `Iterable` [ `str` ] ]
            Mappings to union into ``self``.  These may be other
            `GovernorDimensionRestriction` instances or any other kind of
            mapping from dimension to `str` or iterable of `str`.

        Returns
        -------
        unioned : `GovernorDimensionRestriction`
            New restriction object that represents the union of ``self`` with
            ``others``.
        """
        result = self.copy()
        for other in others:
            result.update(other)
        return result

    def intersection_update(self, other: Mapping[GovernorDimension, Union[str, Iterable[str]]]) -> None:
        """Update ``self`` to include only dimension values in both ``self``
        and ``other``.

        Parameters
        ----------
        other : `Mapping` [ `Dimension`, `str` or `Iterable` [ `str` ] ]
            Mapping to intersect into ``self``.  This may be another
            `GovernorDimensionRestriction` or any other mapping from dimension
            to `str` or iterable of `str`.
        """
        for dimension, values in other.items():
            new_values = set(ensure_iterable(values))
            # Yes, this will often result in a (no-op) self-intersection on the
            # inner set, but this is easier to read (and obviously more or less
            # efficient) than adding a check to avoid it.
            self._mapping.setdefault(dimension, new_values).intersection_update(new_values)

    def intersection(
        self, *others: Mapping[GovernorDimension, Union[str, Iterable[str]]]
    ) -> GovernorDimensionRestriction:
        """Construct a restriction that permits only values permitted by all of
        the input restrictions.

        Parameters
        ----------
        *others : `Mapping` [ `Dimension`, `str` or `Iterable` [ `str` ] ]
            Mappings to intersect with ``self``.  These may be other
            `GovernorDimensionRestriction` instances or any other kind of
            mapping from dimension to `str` or iterable of `str`.
        Returns
        -------
        intersection : `GovernorDimensionRestriction`
            New restriction object that represents the intersection of ``self``
            with ``others``.
        """
        result = self.copy()
        for other in others:
            result.intersection_update(other)
        return result

    def update_extract(self, data_id: DataCoordinate) -> None:
        """Update ``self`` to include all governor dimension values in the
        given data ID (in addition to those already in ``self``).

        Parameters
        ----------
        data_id : `DataCoordinate`
            Data ID from which governor dimension values should be extracted.
            Values for non-governor dimensions are ignored.
        """
        for dimension in data_id.graph.governors:
            current = self._mapping.get(dimension)
            if current is not None:
                current.add(data_id[dimension])


@dataclass
class CollectionSummary:
    """A summary of the datasets that can be found in a collection."""

    @classmethod
    def makeEmpty(cls, universe: DimensionUniverse) -> CollectionSummary:
        """Construct a `CollectionSummary` for a collection with no
        datasets.

        Parameters
        ----------
        universe : `DimensionUniverse`
            Object that manages all dimensions.

        Returns
        -------
        summary : `CollectionSummary`
            Summary object with no dataset types and no governor dimension
            values.
        """
        return cls(
            datasetTypes=NamedValueSet(),
            dimensions=GovernorDimensionRestriction.makeEmpty(universe),
        )

    def copy(self) -> CollectionSummary:
        """Return a deep copy of this object.

        Returns
        -------
        copy : `CollectionSummary`
            A copy of ``self`` that can be modified without modifying ``self``
            at all.
        """
        return CollectionSummary(datasetTypes=self.datasetTypes.copy(), dimensions=self.dimensions.copy())

    def union(self, *others: CollectionSummary) -> CollectionSummary:
        """Construct a summary that contains all dataset types and governor
        dimension values in any of the inputs.

        Parameters
        ----------
        *others : `CollectionSummary`
            Restrictions to combine with ``self``.

        Returns
        -------
        unioned : `CollectionSummary`
            New summary object that represents the union of ``self`` with
            ``others``.
        """
        if not others:
            return self
        datasetTypes = NamedValueSet(self.datasetTypes)
        datasetTypes.update(itertools.chain.from_iterable(o.datasetTypes for o in others))
        dimensions = self.dimensions.union(*[o.dimensions for o in others])
        return CollectionSummary(datasetTypes, dimensions)

    def is_compatible_with(
        self,
        datasetType: DatasetType,
        restriction: GovernorDimensionRestriction,
        rejections: Optional[List[str]] = None,
        name: Optional[str] = None,
    ) -> bool:
        """Test whether the collection summarized by this object should be
        queried for a given dataset type and governor dimension values.

        Parameters
        ----------
        datasetType : `DatasetType`
            Dataset type being queried.  If this collection has no instances of
            this dataset type (or its parent dataset type, if it is a
            component), `False` will always be returned.
        restriction : `GovernorDimensionRestriction`
            Restriction on the values governor dimensions can take in the
            query, usually from a WHERE expression.  If this is disjoint with
            the data IDs actually present in the collection, `False` will be
            returned.
        rejections : `list` [ `str` ], optional
            If provided, a list that will be populated with a log- or
            exception-friendly message explaining why this dataset is
            incompatible with this collection when `False` is returned.
        name : `str`, optional
            Name of the collection this object summarizes, for use in messages
            appended to ``rejections``.  Ignored if ``rejections`` is `None`.

        Returns
        -------
        compatible : `bool`
            `True` if the dataset query described by this summary and the given
            arguments might yield non-empty results; `False` if the result from
            such a query is definitely empty.
        """
        parent = datasetType if not datasetType.isComponent() else datasetType.makeCompositeDatasetType()
        if parent not in self.datasetTypes:
            if rejections is not None:
                rejections.append(f"No datasets of type {parent.name} in collection {name!r}.")
            return False
        for governor in datasetType.dimensions.governors:
            if (values_in_self := self.dimensions.get(governor)) is not None:
                if (values_in_other := restriction.get(governor)) is not None:
                    if values_in_self.isdisjoint(values_in_other):
                        assert values_in_other, f"No valid values in restriction for dimension {governor}."
                        if rejections is not None:
                            rejections.append(
                                f"No datasets with {governor.name} in {values_in_other} "
                                f"in collection {name!r}."
                            )
                        return False
        return True

    datasetTypes: NamedValueSet[DatasetType]
    """Dataset types that may be present in the collection
    (`NamedValueSet` [ `DatasetType` ]).

    A dataset type not in this set is definitely not in the collection, but
    the converse is not necessarily true.
    """

    dimensions: GovernorDimensionRestriction
    """Governor dimension values that may be present in the collection
    (`GovernorDimensionRestriction`).

    A dimension value not in this restriction is definitely not in the
    collection, but the converse is not necessarily true.
    """

# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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

__all__ = ("CollectionSummary", "SerializedCollectionSummary")

import dataclasses
from collections.abc import Generator, Iterable, Mapping, Set
from typing import cast

import pydantic

from .._dataset_ref import DatasetRef
from .._dataset_type import DatasetType, SerializedDatasetType
from .._named import NamedValueSet
from ..dimensions import DataCoordinate, DimensionUniverse


@dataclasses.dataclass
class CollectionSummary:
    """A summary of the datasets that can be found in a collection."""

    def copy(self) -> CollectionSummary:
        """Return a deep copy of this object.

        Returns
        -------
        copy : `CollectionSummary`
            A copy of ``self`` that can be modified without modifying ``self``
            at all.
        """
        return CollectionSummary(
            dataset_types=self.dataset_types.copy(), governors=_copy_governors(self.governors)
        )

    def add_datasets_generator(self, refs: Iterable[DatasetRef]) -> Generator[DatasetRef, None, None]:
        """Include the given datasets in the summary, yielding them back as a
        generator.

        Parameters
        ----------
        refs : `~collections.abc.Iterable` [ `DatasetRef` ]
            Datasets to include.

        Yields
        ------
        ref : `DatasetRef`
            The same dataset references originally passed in.

        Notes
        -----
        As a generator, this method does nothing if its return iterator is not
        used.  Call `add_datasets` instead to avoid this; this method is
        intended for the case where the given iterable may be single-pass and a
        copy is not desired, but other processing needs to be done on its
        elements.
        """
        for ref in refs:
            self.dataset_types.add(ref.datasetType)
            for gov in ref.dataId.dimensions.governors:
                self.governors.setdefault(gov, set()).add(cast(str, ref.dataId[gov]))
            yield ref

    def add_datasets(self, refs: Iterable[DatasetRef]) -> None:
        """Include the given datasets in the summary.

        Parameters
        ----------
        refs : `~collections.abc.Iterable` [ `DatasetRef` ]
            Datasets to include.
        """
        for _ in self.add_datasets_generator(refs):
            pass

    def add_data_ids_generator(
        self, dataset_type: DatasetType, data_ids: Iterable[DataCoordinate]
    ) -> Generator[DataCoordinate, None, None]:
        """Include the given dataset type and data IDs in the summary, yielding
        them back as a generator.

        Parameters
        ----------
        dataset_type : `DatasetType`
            Dataset type to include.
        data_ids : `~collections.abc.Iterable` [ `DataCoordinate` ]
            Data IDs to include.

        Yields
        ------
        data_id : `DataCoordinate`
            The same data IDs originally passed in.

        Notes
        -----
        As a generator, this method does nothing if its return iterator is not
        used.  Call `add_data_ids` instead to avoid this; this method is
        intended for the case where the given iterable may be single-pass and a
        copy is not desired, but other processing needs to be done on its
        elements.
        """
        self.dataset_types.add(dataset_type)
        for data_id in data_ids:
            for gov in data_id.dimensions.governors:
                self.governors.setdefault(gov, set()).add(cast(str, data_id[gov]))
            yield data_id

    def add_data_ids(self, dataset_type: DatasetType, data_ids: Iterable[DataCoordinate]) -> None:
        """Include the given dataset type and data IDs in the summary.

        Parameters
        ----------
        dataset_type : `DatasetType`
            Dataset type to include.
        data_ids : `~collections.abc.Iterable` [ `DataCoordinate` ]
            Data IDs to include.
        """
        for _ in self.add_data_ids_generator(dataset_type, data_ids):
            pass

    def update(self, *args: CollectionSummary) -> None:
        """Update this summary with dataset types and governor dimension values
        from other summaries.

        Parameters
        ----------
        *args : `CollectionSummary`
            Summaries to include in ``self``.
        """
        for arg in args:
            self.dataset_types.update(arg.dataset_types)
            for gov, values in arg.governors.items():
                self.governors.setdefault(gov, set()).update(values)

    def union(*args: CollectionSummary) -> CollectionSummary:
        """Construct a summary that contains all dataset types and governor
        dimension values in any of the inputs.

        Parameters
        ----------
        *args : `CollectionSummary`
            Summaries to combine.

        Returns
        -------
        unioned : `CollectionSummary`
            New summary object that represents the union of the given ones.
        """
        result = CollectionSummary()
        result.update(*args)
        return result

    def is_compatible_with(
        self,
        dataset_type: DatasetType,
        dimensions: Mapping[str, Set[str]],
        rejections: list[str] | None = None,
        name: str | None = None,
    ) -> bool:
        """Test whether the collection summarized by this object should be
        queried for a given dataset type and governor dimension values.

        Parameters
        ----------
        dataset_type : `DatasetType`
            Dataset type being queried.  If this collection has no instances of
            this dataset type (or its parent dataset type, if it is a
            component), `False` will always be returned.
        dimensions : `~collections.abc.Mapping`
            Bounds on the values governor dimensions can take in the query,
            usually from a WHERE expression, as a mapping from dimension name
            to a set of `str` governor dimension values.
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
        parent = dataset_type if not dataset_type.isComponent() else dataset_type.makeCompositeDatasetType()
        if parent.name not in self.dataset_types.names:
            if rejections is not None:
                rejections.append(f"No datasets of type {parent.name} in collection {name!r}.")
            return False
        for gov_name in self.governors.keys() & dataset_type.dimensions.names & dimensions.keys():
            values_in_collection = self.governors[gov_name]
            values_given = dimensions[gov_name]
            if values_in_collection.isdisjoint(values_given):
                if rejections is not None:
                    rejections.append(
                        f"No datasets with {gov_name} in {values_given} in collection {name!r}."
                    )
                return False
        return True

    def to_simple(self) -> SerializedCollectionSummary:
        return SerializedCollectionSummary(
            dataset_types=[x.to_simple() for x in self.dataset_types],
            governors=_copy_governors(self.governors),
        )

    @staticmethod
    def from_simple(simple: SerializedCollectionSummary, universe: DimensionUniverse) -> CollectionSummary:
        summary = CollectionSummary()
        summary.dataset_types = NamedValueSet(
            [DatasetType.from_simple(x, universe) for x in simple.dataset_types]
        )
        summary.governors = _copy_governors(simple.governors)
        return summary

    dataset_types: NamedValueSet[DatasetType] = dataclasses.field(default_factory=NamedValueSet)
    """Dataset types that may be present in the collection
    (`NamedValueSet` [ `DatasetType` ]).

    A dataset type not in this set is definitely not in the collection, but
    the converse is not necessarily true.
    """

    governors: dict[str, set[str]] = dataclasses.field(default_factory=dict)
    """Governor data ID values that are present in the collection's dataset
    data IDs (`dict` [ `str`, `set` [ `str` ] ]).

    A data ID value not in this restriction is not necessarily inconsistent
    with a query in the collection; such a search may only involve dataset
    types that do not include one or more governor dimensions in their data
    IDs, and hence the values of those data IDs are unconstrained by this
    collection in the query.
    """


def _copy_governors(governors: dict[str, set[str]]) -> dict[str, set[str]]:
    """Make an independent copy of the 'governors' data structure."""
    return {k: v.copy() for k, v in governors.items()}


class SerializedCollectionSummary(pydantic.BaseModel):
    """Serialized version of CollectionSummary."""

    dataset_types: list[SerializedDatasetType]
    governors: dict[str, set[str]]

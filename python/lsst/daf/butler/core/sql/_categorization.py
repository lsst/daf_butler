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

__all__ = ("ColumnCategorization",)

from typing import AbstractSet, Iterable, Optional, Union

from lsst.utils.classes import cached_getter

from .._spatial_regions import SpatialRegionDatabaseRepresentation
from ..datasets import DatasetType
from ..dimensions import Dimension, DimensionElement, DimensionGraph, DimensionUniverse
from ..named import NamedValueAbstractSet, NamedValueSet
from ..timespan import TimespanDatabaseRepresentation
from ._column_tags import ColumnTag, DatasetColumnTag, DimensionKeyColumnTag, DimensionRecordColumnTag


class ColumnCategorization:

    _DATASET_IDENTIFIER_FIELDS = frozenset({"dataset_id", "ingest_date", "timespan"})

    def __init__(
        self,
        universe: DimensionUniverse,
        all_parent_dataset_types: NamedValueAbstractSet[DatasetType],
        tags: Iterable[ColumnTag] = (),
        *,
        dimension_keys: AbstractSet[str] = frozenset(),
        dimension_record_elements: AbstractSet[str] = frozenset(),
        dataset_types: AbstractSet[str] = frozenset(),
        spatial_regions: AbstractSet[DimensionRecordColumnTag] = frozenset(),
        timespans: AbstractSet[Union[DimensionRecordColumnTag, DatasetColumnTag]] = frozenset(),
    ):
        self.universe = universe
        self.all_parent_dataset_types = all_parent_dataset_types
        self._dimension_keys = set(dimension_keys)
        self._dimension_record_elements = set(dimension_record_elements)
        self._dataset_types = set(dataset_types)
        self._spatial_regions = set(spatial_regions)
        self._timespans = set(timespans)
        self._update(tags)

    def new_from(self, tags: Iterable[ColumnTag]) -> ColumnCategorization:
        return ColumnCategorization(self.universe, self.all_parent_dataset_types, tags)

    @property  # type: ignore
    @cached_getter
    def spanning_dimensions(self) -> DimensionGraph:
        spanning_dimensions: set[str] = set()
        for dimension_name in self._dimension_keys:
            spanning_dimensions.update(self.universe[dimension_name].graph.names)
        for element_name in self._dimension_record_elements:
            spanning_dimensions.update(self.universe[element_name].graph.names)
        for dataset_type_name in self._dataset_types:
            spanning_dimensions.update(self.all_parent_dataset_types[dataset_type_name].dimensions.names)
        return DimensionGraph(
            self.universe,
            names=spanning_dimensions,
        )

    @property  # type: ignore
    @cached_getter
    def dimension_keys(self) -> NamedValueAbstractSet[Dimension]:
        return NamedValueSet(
            self.universe.getStaticDimensions()[name] for name in self._dimension_keys
        ).freeze()

    @property
    def dimension_record_elements(self) -> NamedValueAbstractSet[DimensionElement]:
        return NamedValueSet(self.universe[name] for name in self._dimension_record_elements).freeze()

    @property
    def dataset_types(self) -> NamedValueAbstractSet[DatasetType]:
        return NamedValueSet(self.all_parent_dataset_types[name] for name in self._dataset_types)

    @property
    def spatial_regions(self) -> AbstractSet[DimensionRecordColumnTag]:
        return self._spatial_regions

    @property
    def timespans(self) -> AbstractSet[Union[DimensionRecordColumnTag, DatasetColumnTag]]:
        return self._timespans

    def _update(self, tags: Iterable[ColumnTag]) -> None:
        for tag in tags:
            if type(tag) is DimensionKeyColumnTag:
                assert tag.dimension in self.universe.getStaticDimensions().names
                self._dimension_keys.add(tag.dimension)
            elif type(tag) is DimensionRecordColumnTag:
                assert tag.element in self.universe.getStaticElements().names
                self._dimension_record_elements.add(tag.element)
                if tag.column == SpatialRegionDatabaseRepresentation.NAME:
                    self._spatial_regions.add(tag)
                elif tag.column == TimespanDatabaseRepresentation.NAME:
                    self._timespans.add(tag)
            elif type(tag) is DatasetColumnTag:
                assert tag.dataset_type in self.all_parent_dataset_types.names
                self._dataset_types.add(tag.dataset_type)
                if tag.column == TimespanDatabaseRepresentation.NAME:
                    self._timespans.add(tag)
            else:
                raise TypeError(f"Unrecognized tag type: {tag}")

    def interpret_identifier(
        self,
        identifier: str,
        require_known: bool = False,
    ) -> tuple[ColumnTag, Optional[str]]:
        prefix, _, suffix = identifier.partition(".")
        if not suffix:
            if prefix in self.universe.getStaticDimensions().names:
                if require_known and prefix not in self._dimension_keys:
                    raise RuntimeError(
                        f"Dimension {prefix!r} is not valid here because it has not " "already been included."
                    )
                return DimensionKeyColumnTag(prefix), None
            elif prefix.lower() == "null":
                raise RuntimeError("NULL is not valid in this context.")
            else:
                matching: list[ColumnTag] = []
                for dataset_type_name in self._dataset_types:
                    if prefix.lower() in self._DATASET_IDENTIFIER_FIELDS:
                        matching.append(DatasetColumnTag(dataset_type_name, prefix))
                for element_name in self._dimension_record_elements:
                    if prefix.lower() in self.universe[element_name].RecordClass.fields.names:
                        matching.append(DimensionRecordColumnTag(element_name, prefix))
                if len(matching) == 1:
                    return matching[0], None
                else:
                    raise RuntimeError(
                        f"Cannot interpret unqualified column name {prefix!r} unless there "
                        "is exactly one dimension element or dataset type that matches it."
                    )
        else:
            subfield: Optional[str]
            column, _, subfield = suffix.partition(".")
            if not subfield:
                subfield = None
            elif subfield != "begin" and subfield != "end" or column != "timespan":
                raise RuntimeError(f"Unrecognized three-part identifier {identifier!r}.")
            if (dimension_element := self.universe.getStaticElements().get(prefix)) is not None:
                tag: ColumnTag
                if require_known and dimension_element.name not in self._dimension_record_elements:
                    raise RuntimeError(
                        f"Dimension column {identifier!r} is not valid here "
                        f"because {dimension_element.name} is not already included."
                    )
                if column == dimension_element.primaryKey.name:
                    return DimensionKeyColumnTag(dimension_element.name), None
                elif column in dimension_element.RecordClass.fields.names:
                    tag = DimensionRecordColumnTag(dimension_element.name, column)
                    return tag, subfield
                else:
                    raise RuntimeError(
                        f"{column!r} is not a valid column for dimension table {dimension_element.name!r}."
                    )
            elif (dataset_type := self.all_parent_dataset_types.get(prefix)) is not None:
                if require_known and dataset_type.name not in self._dataset_types:
                    raise RuntimeError(
                        f"Dataset column {identifier!r} is not valid here "
                        f"because {dataset_type.name} is not already included."
                    )
                if column in self._DATASET_IDENTIFIER_FIELDS:
                    tag = DatasetColumnTag(dataset_type.name, column)
                    return tag, subfield
                else:
                    raise RuntimeError(f"{column!r} is not a valid column for dataset {dataset_type.name}.")
            elif subfield is None and prefix == TimespanDatabaseRepresentation.NAME:
                # This is actually an unqualified timespan subfield, not a
                # qualified column.
                subfield = column
                matching = []
                for dataset_type_name in self._dataset_types:
                    matching.append(DatasetColumnTag(dataset_type_name, prefix))
                for element_name in self._dimension_record_elements:
                    if self.universe[element_name].spatial:
                        matching.append(DimensionRecordColumnTag(element_name, prefix))
                if len(matching) == 1:
                    return matching[0], subfield
                else:
                    raise RuntimeError(
                        "Cannot interpret unqualified timespan unless there is "
                        "exactly one temporal dimension element or dataset type."
                    )
            else:
                raise RuntimeError(f"Unrecognized table {prefix!r} in identifier {identifier!r}.")

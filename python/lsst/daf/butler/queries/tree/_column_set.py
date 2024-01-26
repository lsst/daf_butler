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

__all__ = ("ColumnSet",)

from collections.abc import Iterable, Iterator, Mapping
from typing import Literal

from ... import column_spec
from ..._utilities.nonempty_mapping import NonemptyMapping
from ...dimensions import DimensionGroup
from ._base import DATASET_FIELD_NAMES, DatasetFieldName


class ColumnSet:
    def __init__(self, dimensions: DimensionGroup) -> None:
        self._dimensions = dimensions
        self._removed_dimension_keys: set[str] = set()
        self._dimension_fields: dict[str, set[str]] = {name: set() for name in dimensions.elements}
        self._dataset_fields = NonemptyMapping[str, set[DatasetFieldName]](set)

    @property
    def dimensions(self) -> DimensionGroup:
        return self._dimensions

    @property
    def dimension_fields(self) -> Mapping[str, set[str]]:
        return self._dimension_fields

    @property
    def dataset_fields(self) -> Mapping[str, set[DatasetFieldName]]:
        return self._dataset_fields

    def __bool__(self) -> bool:
        return bool(self._dimensions) or any(self._dataset_fields.values())

    def issubset(self, other: ColumnSet) -> bool:
        return (
            self._dimensions.issubset(other._dimensions)
            and all(
                fields.issubset(other._dimension_fields[element_name])
                for element_name, fields in self._dimension_fields.items()
            )
            and all(
                fields.issubset(other._dataset_fields.get(dataset_type, frozenset()))
                for dataset_type, fields in self._dataset_fields.items()
            )
        )

    def issuperset(self, other: ColumnSet) -> bool:
        return other.issubset(self)

    def isdisjoint(self, other: ColumnSet) -> bool:
        # Note that if the dimensions are disjoint, the dimension fields are
        # also disjoint, and if the dimensions are not disjoint, we already
        # have our answer.  The same is not true for dataset fields only for
        # the edge case of dataset types with empty dimensions.
        return self._dimensions.isdisjoint(other._dimensions) and (
            self._dataset_fields.keys().isdisjoint(other._dataset_fields)
            or all(
                fields.isdisjoint(other._dataset_fields[dataset_type])
                for dataset_type, fields in self._dataset_fields.items()
            )
        )

    def copy(self) -> ColumnSet:
        result = ColumnSet(self._dimensions)
        for element_name, element_fields in self._dimension_fields.items():
            result._dimension_fields[element_name].update(element_fields)
        for dataset_type, dataset_fields in self._dataset_fields.items():
            result._dimension_fields[dataset_type].update(dataset_fields)
        return result

    def update_dimensions(self, dimensions: DimensionGroup) -> None:
        if not dimensions.issubset(self._dimensions):
            self._dimensions = dimensions
            self._dimension_fields = {
                name: self._dimension_fields.get(name, set()) for name in self._dimensions.elements
            }

    def drop_dimension_keys(self, names: Iterable[str]) -> None:
        self._removed_dimension_keys.update(names)

    def drop_implied_dimension_keys(self) -> None:
        self._removed_dimension_keys.update(self._dimensions.implied)

    def __iter__(self) -> Iterator[tuple[str, str | None]]:
        for dimension_name in self._dimensions.data_coordinate_keys:
            if dimension_name not in self._removed_dimension_keys:
                yield dimension_name, None
        # We iterate over DimensionElements and their DimensionRecord columns
        # in order to make sure that's predictable.  We might want to extract
        # these query results positionally in some contexts.
        for element_name in self._dimensions.elements:
            element = self._dimensions.universe[element_name]
            fields_for_element = self._dimension_fields[element_name]
            for spec in element.schema.remainder:
                if spec.name in fields_for_element:
                    yield element_name, spec.name
        # We sort dataset types and lexicographically just to keep our queries
        # from having any dependence on set-iteration order.
        for dataset_type in sorted(self._dataset_fields):
            fields_for_dataset_type = self._dataset_fields[dataset_type]
            for field in DATASET_FIELD_NAMES:
                if field in fields_for_dataset_type:
                    yield dataset_type, field

    def is_timespan(self, logical_table: str, field: str | None) -> bool:
        return field == "timespan"

    @staticmethod
    def get_qualified_name(logical_table: str, field: str | None) -> str:
        return logical_table if field is None else f"{logical_table}:{field}"

    def get_uniqueness_category(self, logical_table: str, field: str | None) -> Literal["key", "natural"]:
        if field is None:
            # Dimensions in the required subset are always unique keys, while
            # those in the implied subset are then naturally unique.
            return "key" if logical_table in self._dimensions.required else "natural"
        if logical_table in self._dimension_fields:
            # Other dimension element fields are always naturally unique if
            # their dimensions are.
            assert (
                field in self._dimensions.universe[logical_table].schema.remainder.names
            ), "long forms (e.g. visit.id) of dimension key columns should not appear here"
            return "natural"
        if "dataset_id" not in self._dataset_fields[logical_table]:
            raise RuntimeError(
                f"Uniqueness for dataset field {logical_table}.{field} is undefined if "
                "dataset_id is not included."
            )
        return (
            "key"
            if (
                # The dataset ID is always a unique key if it is present.
                field == "dataset_id"
                # Rank is a unique key if it's present, because the same
                # dataset ID can appear in multiple collections and that means
                # different ranks.
                or field == "rank"
                # Timespan is a unique key because the same dataset ID can be
                # associated with multiple validity ranges in a single
                # CALIBRATION collection.
                or field == "timespan"
                # Collection is a unique key if rank is not already present;
                # otherwise it's naturally unique given rank.
                or (field == "collection" and "rank" not in self._dataset_fields[logical_table])
            )
            else "natural"
        )

    def get_column_spec(self, logical_table: str, field: str | None) -> column_spec.ColumnSpec:
        qualified_name = self.get_qualified_name(logical_table, field)
        if field is None:
            return self._dimensions.universe.dimensions[logical_table].primary_key.model_copy(
                update=dict(name=qualified_name)
            )
        if logical_table in self._dimension_fields:
            return (
                self._dimensions.universe[logical_table]
                .schema.all[field]
                .model_copy(update=dict(name=qualified_name))
            )
        match field:
            case "dataset_id":
                return column_spec.UUIDColumnSpec.model_construct(name=qualified_name, nullable=False)
            case "ingest_date":
                return column_spec.DateTimeColumnSpec.model_construct(name=qualified_name)
            case "run":
                # TODO: string length matches the one defined in the
                # CollectionManager implementations; we need to find a way to
                # avoid hard-coding the value in multiple places.
                return column_spec.StringColumnSpec.model_construct(
                    name=qualified_name, nullable=False, length=128
                )
            case "collection":
                return column_spec.StringColumnSpec.model_construct(
                    name=qualified_name, nullable=False, length=128
                )
            case "rank":
                return column_spec.IntColumnSpec.model_construct(name=qualified_name, nullable=False)
            case "timespan":
                return column_spec.TimespanColumnSpec.model_construct(name=qualified_name, nullable=False)
        raise AssertionError(f"Unrecognized column identifiers: {logical_table}, {field}.")

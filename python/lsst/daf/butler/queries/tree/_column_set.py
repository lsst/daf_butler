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

__all__ = ("ColumnOrder", "ColumnSet", "ResultColumn")

from collections.abc import Iterable, Iterator, Mapping, Sequence, Set
from typing import NamedTuple, cast

from ... import column_spec
from ...dimensions import DataIdValue, DimensionGroup
from ...nonempty_mapping import NonemptyMapping
from ._base import ANY_DATASET, AnyDatasetType


class ColumnSet:
    """A set-like hierarchical container for the columns in a query.

    Parameters
    ----------
    dimensions : `DimensionGroup`
        The dimensions that bound the set of columns, and by default specify
        the set of dimension key columns present.

    Notes
    -----
    This class does not inherit from `collections.abc.Set` because that brings
    in a lot of requirements we don't need (particularly interoperability with
    other set-like objects).

    This class is iterable over tuples of ``(logical_table, field)``, where
    ``logical_table`` is a dimension element name or dataset type name, and
    ``field`` is a column associated with one of those, or `None` for dimension
    key columns.  Iteration order is guaranteed to be deterministic and to
    start with all included dimension keys in
    `DimensionGroup.data_coordinate_keys`.
    """

    def __init__(self, dimensions: DimensionGroup) -> None:
        self._dimensions = dimensions
        self._removed_dimension_keys: set[str] = set()
        self._dimension_fields: dict[str, set[str]] = {name: set() for name in dimensions.elements}
        self._dataset_fields = NonemptyMapping[str | AnyDatasetType, set[str]](set)

    @property
    def dimensions(self) -> DimensionGroup:
        """The dimensions that bound all columns in the set."""
        return self._dimensions

    @property
    def dimension_fields(self) -> Mapping[str, set[str]]:
        """Dimension record fields included in the set, grouped by dimension
        element name.

        The keys of this mapping are always ``self.dimensions.elements``, and
        nested sets may be empty.
        """
        return self._dimension_fields

    @property
    def dataset_fields(self) -> NonemptyMapping[str | AnyDatasetType, set[str]]:
        """Dataset fields included in the set, grouped by dataset type name.

        The keys of this mapping are just those that actually have nonempty
        nested sets.
        """
        return self._dataset_fields

    def __bool__(self) -> bool:
        return bool(self._dimensions) or any(self._dataset_fields.values())

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ColumnSet):
            return False
        return (
            self._dimensions == other._dimensions
            and self._removed_dimension_keys == other._removed_dimension_keys
            and self._dimension_fields == other._dimension_fields
            and self._dataset_fields == other._dataset_fields
        )

    def __str__(self) -> str:
        return f"{{{', '.join(self.get_qualified_name(k, v) for k, v in self)}}}"

    def issubset(self, other: ColumnSet) -> bool:
        """Test whether all columns in this set are also in another.

        Parameters
        ----------
        other : `ColumnSet`
            Set of columns to compare to.

        Returns
        -------
        issubset : `bool`
            Whether all columns in ``self`` are also in ``other``.
        """
        return (
            (self._get_dimension_keys() <= other._get_dimension_keys())
            and all(
                fields.issubset(other._dimension_fields.get(element_name, frozenset()))
                for element_name, fields in self._dimension_fields.items()
            )
            and all(
                fields.issubset(other._dataset_fields.get(dataset_type, frozenset()))
                for dataset_type, fields in self._dataset_fields.items()
            )
        )

    def issuperset(self, other: ColumnSet) -> bool:
        """Test whether all columns another set are also in this one.

        Parameters
        ----------
        other : `ColumnSet`
            Set of columns to compare to.

        Returns
        -------
        issuperset : `bool`
            Whether all columns in ``other`` are also in ``self``.
        """
        return other.issubset(self)

    def isdisjoint(self, other: ColumnSet) -> bool:
        """Test whether there are no columns in both this set and another.

        Parameters
        ----------
        other : `ColumnSet`
            Set of columns to compare to.

        Returns
        -------
        isdisjoint : `bool`
            Whether there are any columns in both ``self`` and ``other``.
        """
        return (
            self._get_dimension_keys().isdisjoint(other._get_dimension_keys())
            and all(
                fields.isdisjoint(other._dimension_fields.get(element, frozenset()))
                for element, fields in self._dimension_fields.items()
            )
            and all(
                fields.isdisjoint(other._dataset_fields.get(dataset_type, frozenset()))
                for dataset_type, fields in self._dataset_fields.items()
            )
        )

    def copy(self) -> ColumnSet:
        """Return a copy of this set.

        Returns
        -------
        copy : `ColumnSet`
            New column set that can be modified without changing the original.
        """
        result = ColumnSet(self._dimensions)
        for element_name, element_fields in self._dimension_fields.items():
            result._dimension_fields[element_name].update(element_fields)
        for dataset_type, dataset_fields in self._dataset_fields.items():
            result._dataset_fields[dataset_type].update(dataset_fields)
        return result

    def update_dimensions(self, dimensions: DimensionGroup) -> None:
        """Add new dimensions to the set.

        Parameters
        ----------
        dimensions : `DimensionGroup`
            Dimensions to be included.
        """
        if not dimensions.issubset(self._dimensions):
            self._dimensions = dimensions.union(self._dimensions)
            self._dimension_fields = {
                name: self._dimension_fields.get(name, set()) for name in self._dimensions.elements
            }
            self._removed_dimension_keys.intersection_update(dimensions.names)

    def update(self, other: ColumnSet) -> None:
        """Add columns from another set to this one.

        Parameters
        ----------
        other : `ColumnSet`
            Column set whose columns should be included in this one.
        """
        self.update_dimensions(other.dimensions)
        self._removed_dimension_keys.intersection_update(other._removed_dimension_keys)
        for element_name, element_fields in other._dimension_fields.items():
            self._dimension_fields[element_name].update(element_fields)
        for dataset_type, dataset_fields in other._dataset_fields.items():
            self._dataset_fields[dataset_type].update(dataset_fields)

    def drop_dimension_keys(self, names: Iterable[str]) -> ColumnSet:
        """Remove the given dimension key columns from the set.

        Parameters
        ----------
        names : `~collections.abc.Iterable` [ `str` ]
            Names of the dimensions to remove.

        Returns
        -------
        self : `ColumnSet`
            This column set, modified in place.
        """
        self._removed_dimension_keys.update(names)
        return self

    def drop_implied_dimension_keys(self) -> ColumnSet:
        """Remove dimension key columns that are implied by others.

        Returns
        -------
        self : `ColumnSet`
            This column set, modified in place.
        """
        return self.drop_dimension_keys(self._dimensions.implied)

    def restore_dimension_keys(self) -> None:
        """Restore all removed dimension key columns."""
        self._removed_dimension_keys.clear()

    def __iter__(self) -> Iterator[ResultColumn]:
        yield from self.get_column_order().columns()

    def get_column_order(self) -> ColumnOrder:
        dimension_names: list[ResultColumn] = []
        for dimension_name in self._dimensions.data_coordinate_keys:
            if dimension_name not in self._removed_dimension_keys:
                dimension_names.append(ResultColumn(dimension_name, None))

        # We iterate over DimensionElements and their DimensionRecord columns
        # in order to make sure that's predictable.  We might want to extract
        # these query results positionally in some contexts.
        dimension_elements: list[ResultColumn] = []
        for element_name in self._dimensions.elements:
            element = self._dimensions.universe[element_name]
            fields_for_element = self._dimension_fields[element_name]
            for spec in element.schema.remainder:
                if spec.name in fields_for_element:
                    dimension_elements.append(ResultColumn(element_name, spec.name))

        # We sort dataset types and their fields lexicographically just to keep
        # our queries from having any dependence on set-iteration order.
        dataset_fields: list[ResultColumn] = []
        for dataset_type in sorted(self._dataset_fields, key=str):  # transform ANY_DATASET to str for sort
            for field in sorted(self._dataset_fields[dataset_type]):
                dataset_fields.append(ResultColumn(dataset_type, field))

        return ColumnOrder(dimension_names, dimension_elements, dataset_fields)

    def is_timespan(self, logical_table: AnyDatasetType | str, field: str | None) -> bool:
        """Test whether the given column is a timespan.

        Parameters
        ----------
        logical_table : `str` or ``ANY_DATASET``
            Name of the dimension element or dataset type the column belongs
            to.  ``ANY_DATASET`` is used to represent any dataset type.
        field : `str` or `None`
            Column within the logical table, or `None` for dimension key
            columns.

        Returns
        -------
        is_timespan : `bool`
            Whether this column is a timespan.
        """
        return field == "timespan"

    @staticmethod
    def get_qualified_name(logical_table: AnyDatasetType | str, field: str | None) -> str:
        """Return string that should be used to fully identify a column.

        Parameters
        ----------
        logical_table : `str` or ``ANY_DATASET```
            Name of the dimension element or dataset type the column belongs
            to.  ``ANY_DATASET`` is used to represent any dataset type.
        field : `str` or `None`
            Column within the logical table, or `None` for dimension key
            columns.

        Returns
        -------
        name : `str`
            Fully-qualified name.
        """
        return str(logical_table) if field is None else f"{logical_table}:{field}"

    def get_column_spec(
        self, logical_table: AnyDatasetType | str, field: str | None
    ) -> column_spec.ColumnSpec:
        """Return a complete description of a column.

        Parameters
        ----------
        logical_table : `str` or ``ANY_DATASET``
            Name of the dimension element or dataset type the column belongs
            to. ``ANY_DATASET`` is used to represent any dataset type.
        field : `str` or `None`
            Column within the logical table, or `None` for dimension key
            columns.

        Returns
        -------
        spec : `.column_spec.ColumnSpec`
            Description of the column.
        """
        qualified_name = self.get_qualified_name(logical_table, field)
        if field is None:
            assert logical_table is not ANY_DATASET
            return self._dimensions.universe.dimensions[logical_table].primary_key.model_copy(
                update=dict(name=qualified_name)
            )
        if logical_table in self._dimension_fields:
            assert logical_table is not ANY_DATASET
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
                return column_spec.StringColumnSpec.model_construct(
                    name=qualified_name, nullable=False, length=column_spec.COLLECTION_NAME_MAX_LENGTH
                )
            case "collection":
                return column_spec.StringColumnSpec.model_construct(
                    name=qualified_name, nullable=False, length=column_spec.COLLECTION_NAME_MAX_LENGTH
                )
            case "timespan":
                return column_spec.TimespanColumnSpec.model_construct(name=qualified_name, nullable=False)
        raise AssertionError(f"Unrecognized column identifiers: {logical_table}, {field}.")

    def _get_dimension_keys(self) -> Set[str]:
        if not self._removed_dimension_keys:
            return self._dimensions.names
        else:
            return self._dimensions.names - self._removed_dimension_keys


class ResultColumn(NamedTuple):
    """Defines a column that can be output from a query."""

    logical_table: AnyDatasetType | str
    """Dimension element name or dataset type name."""

    field: str | None
    """Column associated with the dimension element or dataset type, or `None`
    if it is a dimension key column."""

    def __str__(self) -> str:
        return str(self.logical_table) if self.field is None else f"{self.logical_table}.{self.field}"


class ColumnOrder:
    """Defines the position of columns within a result row and provides helper
    methods for accessing subsets of columns in a row.

    Parameters
    ----------
    dimension_keys : `~collections.abc.Iterable` [ `ResultColumn` ]
        Columns corresponding to dimension primary keys.
    dimension_elements : `~collections.abc.Iterable` [ `ResultColumn` ]
        Columns corresponding to DimensionElements and their DimensionRecord
        columns.
    dataset_fields : `~collections.abc.Iterable` [ `ResultColumn` ]
        Columns corresponding to dataset types and their fields.
    """

    def __init__(
        self,
        dimension_keys: Iterable[ResultColumn],
        dimension_elements: Iterable[ResultColumn],
        dataset_fields: Iterable[ResultColumn],
    ):
        self._dimension_keys = tuple(dimension_keys)
        self._dimension_elements = tuple(dimension_elements)
        self._dataset_fields = tuple(dataset_fields)

    def columns(self) -> Iterator[ResultColumn]:
        # When editing this method, take care to update the other methods on
        # this object to correspond to the new order.
        yield from self._dimension_keys
        yield from self._dimension_elements
        yield from self._dataset_fields

    @property
    def dimension_key_names(self) -> list[str]:
        """Return the names of the dimension key columns included in result
        rows, in the order they appear in the row.
        """
        return [cast(str, column.logical_table) for column in self._dimension_keys]

    def extract_dimension_key_columns(self, row: Sequence[DataIdValue]) -> Sequence[DataIdValue]:
        """Given a full result row, return just the dimension key columns.

        Parameters
        ----------
        row : `Sequence` [ `DataIdValue` ]
            A row output by the SQL query associated with these columns.
        """
        return row[: len(self._dimension_keys)]

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

__all__ = ("DimensionRecordTable",)

from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING, Any, final, overload

import pyarrow as pa
import pyarrow.compute as pc

from lsst.utils.iteration import chunk_iterable

if TYPE_CHECKING:
    from ._elements import DimensionElement
    from ._records import DimensionRecord
    from ._universe import DimensionUniverse


@final
class DimensionRecordTable:
    """A table-like container for `DimensionRecord` objects.

    Parameters
    ----------
    element : `DimensionElement` or `str`, optional
        The dimension element that defines the records held by this table. If
        not a `DimensionElement` instance, ``universe`` must be provided.  If
        not provided, ``table`` must have an "element" entry in its metadata
        (as is the case for tables returned by the `to_arrow` method).
    records : `~collections.abc.Iterable` [ `DimensionRecord` ], optional
        Dimension records to add to the table.
    universe : `DimensionUniverse`, optional
        Object that defines all dimensions.  Ignored if ``element`` is a
        `DimensionElement` instance.
    table : `pyarrow.Table`
        Arrow table to copy columns from.  Must have schema returned by
        `make_arrow_schema` for this element.  This argument is primarily
        intended to serve as the way to reconstruct a `DimensionRecordTable`
        that has been serialized to an Arrow-supported file or IPC format.
    batch_size : `int`, optional
        How many elements of ``records`` should be processed at a time, with
        each batch yielding a `pyarrow.RecordBatch` in the created table.
        Smaller values will reduce peak memory usage for large iterables.
        Ignored if ``records`` is empty.

    Notes
    -----
    `DimensionRecordTable` should generally have a smaller memory footprint
    than `DimensionRecordSet` if its rows are unique, and it provides fast
    column-oriented access and Arrow interoperability that `DimensionRecordSet`
    lacks entirely.  In other respects `DimensionRecordSet` is more
    featureful and simpler to use efficiently.
    """

    def __init__(
        self,
        element: DimensionElement | str | None = None,
        records: Iterable[DimensionRecord] = (),
        universe: DimensionUniverse | None = None,
        table: pa.Table | None = None,
        batch_size: int | None = None,
    ):
        if element is None:
            if table is not None and b"element" in table.schema.metadata:
                element = table.schema.metadata[b"element"].decode()
            else:
                raise TypeError("If 'element' is not provided it must be present in 'table.schema.metadata'.")
        if isinstance(element, str):
            if universe is None:
                raise TypeError("'universe' must be provided if 'element' is not a DimensionElement.")
            element = universe[element]
        else:
            universe = element.universe
        self._element = element
        self._converters = element.schema.to_arrow()
        arrow_schema = pa.schema(
            [converter.field for converter in self._converters],
            {
                b"element": element.name.encode(),
                # Since the Arrow table might be saved to a file on its own, we
                # include the dimension universe's identifiers in its metadata.
                b"namespace": element.universe.namespace.encode(),
                b"version": str(element.universe.version).encode(),
            },
        )
        self._required_value_fields = [pc.field(name) for name in self._element.schema.required.names]
        if batch_size is None:
            batches = [self._make_batch(records, arrow_schema)]
        else:
            batches = [
                self._make_batch(record_chunk, arrow_schema)
                for record_chunk in chunk_iterable(records, chunk_size=batch_size)
            ]
        if table is not None:
            batches.extend(table.to_batches())
        self._table: pa.Table = pa.Table.from_batches(batches, arrow_schema)

    @classmethod
    def make_arrow_schema(cls, element: DimensionElement) -> pa.Schema:
        """Return the Arrow schema of the table returned by `to_arrow` with the
        given dimension element.

        Parameters
        ----------
        element : `DimensionElement`
            Dimension element that defines the schema.

        Returns
        -------
        schema : `pyarrow.Schema`
            Arrow schema.
        """
        return pa.schema([converter.field for converter in element.schema.to_arrow()])

    @property
    def element(self) -> DimensionElement:
        """The dimension element that defines the records of this table."""
        return self._element

    def __len__(self) -> int:
        return self._table.num_rows

    def __iter__(self) -> Iterator[DimensionRecord]:
        for i in range(self._table.num_rows):
            yield self._get_record_at(self._table, i)

    @overload
    def __getitem__(self, index: int) -> DimensionRecord: ...

    @overload
    def __getitem__(self, index: slice) -> DimensionRecordTable: ...

    def __getitem__(self, index: int | slice) -> DimensionRecord | DimensionRecordTable:
        if isinstance(index, slice):
            result = object.__new__(DimensionRecordTable)
            result._element = self._element
            result._converters = self._converters
            result._table = self._table[index]
            return result
        else:
            return self._get_record_at(self._table, index)

    def extend(self, records: Iterable[DimensionRecord]) -> None:
        """Add new rows to the end of the table.

        Parameters
        ----------
        records : `~collections.abc.Iterable` [ `DimensionRecord` ]
            Dimension records to add to the table.
        """
        batches: list[pa.RecordBatch] = self._table.to_batches()
        batches.append(self._make_batch(records, self._table.schema))
        self._table = pa.Table.from_batches(batches, self._table.schema)

    def column(self, name: str) -> pa.ChunkedArray:
        """Return a single column from the table as an array.

        Parameters
        ----------
        name : `str`
            Name of the column.  Valid options are given by
            `DimensionElement.schema.names`, and are the same as the attributes
            of the dimension records.

        Returns
        -------
        array : `pyarrow.ChunkedArray`
            An array view of the column.
        """
        return self._table.column(name)

    def to_arrow(self) -> pa.Table:
        """Return a Arrow table holding the same records."""
        return self._table

    def _make_batch(self, records: Iterable[DimensionRecord], arrow_schema: pa.Schema) -> pa.RecordBatch:
        """Make a `pyarrow.RecordBatch` from an iterable of `DimensionRecord`.

        Parameters
        ----------
        records : `~collections.abc.Iterable` [ `DimensionRecord` ]
            Records to add.
        arrow_schema : `pyarrow.Schema`
            Arrow schema for the record batch.

        Returns
        -------
        batch : `pyarrow.RecordBatch`
            Record batch holding the records.
        """
        list_columns: list[list[Any]] = [list() for _ in self._converters]
        for record in records:
            for converter, column in zip(self._converters, list_columns):
                converter.append(getattr(record, converter.name), column)
        array_columns = [
            converter.finish(column) for converter, column in zip(self._converters, list_columns)
        ]
        return pa.record_batch(array_columns, arrow_schema)

    def _get_record_at(self, table: pa.Table | pa.RecordBatch, index: int) -> DimensionRecord:
        """Construct a `DimensionRecord` from a row in the table.

        Parameters
        ----------
        table : `pyarrow.Table` or `pyarrow.RecordBatch`
            Table or record batch to get values from.
        index : `int`
            Index of the row to extract.

        Returns
        -------
        record : `DimensionRecord`
            Dimension record representing a table row.
        """
        return self._element.RecordClass(
            **{k: table.column(j)[index].as_py() for j, k in enumerate(self._element.schema.all.names)}
        )

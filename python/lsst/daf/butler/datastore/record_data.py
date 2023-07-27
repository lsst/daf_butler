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

"""Support for generic data stores."""

from __future__ import annotations

__all__ = ("DatastoreRecordData", "SerializedDatastoreRecordData")

import dataclasses
import uuid
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeAlias

from lsst.daf.butler._compat import PYDANTIC_V2, _BaseModelCompat

from .._dataset_ref import DatasetId
from ..dimensions import DimensionUniverse
from ..persistence_context import PersistenceContextVars
from .stored_file_info import StoredDatastoreItemInfo

if TYPE_CHECKING:
    from ..registry import Registry

# Pydantic 2 requires we be explicit about the types that are used in
# datastore records. Without this UUID can not be handled. Pydantic v1
# wants the opposite and does not work unless we use Any.
if PYDANTIC_V2:
    _Record: TypeAlias = dict[str, int | str | uuid.UUID | None]
else:
    _Record: TypeAlias = dict[str, Any]  # type: ignore


class SerializedDatastoreRecordData(_BaseModelCompat):
    """Representation of a `DatastoreRecordData` suitable for serialization."""

    dataset_ids: list[uuid.UUID]
    """List of dataset IDs"""

    records: Mapping[str, Mapping[str, Mapping[str, list[_Record]]]]
    """List of records indexed by record class name, dataset ID (encoded as
    str, because JSON), and opaque table name.
    """

    @classmethod
    def direct(
        cls,
        *,
        dataset_ids: list[str | uuid.UUID],
        records: dict[str, dict[str, dict[str, list[_Record]]]],
    ) -> SerializedDatastoreRecordData:
        """Construct a `SerializedDatastoreRecordData` directly without
        validators.

        This differs from the pydantic "construct" method in that the
        arguments are explicitly what the model requires, and it will recurse
        through members, constructing them from their corresponding `direct`
        methods.

        This method should only be called when the inputs are trusted.
        """
        data = cls.model_construct(
            _fields_set={"dataset_ids", "records"},
            # JSON makes strings out of UUIDs, need to convert them back
            dataset_ids=[uuid.UUID(id) if isinstance(id, str) else id for id in dataset_ids],
            records=records,
        )

        return data


@dataclasses.dataclass
class DatastoreRecordData:
    """A struct that represents a tabular data export from a single
    datastore.
    """

    records: dict[DatasetId, dict[str, list[StoredDatastoreItemInfo]]] = dataclasses.field(
        default_factory=dict
    )
    """Opaque table data, indexed by dataset ID and grouped by opaque table
    name."""

    def update(self, other: DatastoreRecordData) -> None:
        """Update contents of this instance with data from another instance.

        Parameters
        ----------
        other : `DatastoreRecordData`
            Records to merge into this instance.

        Notes
        -----
        If a ``(dataset_id, table_name)`` combination has any records in
        ``self``, it is assumed that all records for that combination are
        already present.  This allows duplicates of the same dataset to be
        handled gracefully.
        """
        for dataset_id, table_records in other.records.items():
            this_table_records = self.records.setdefault(dataset_id, {})
            for table_name, records in table_records.items():
                # If this (dataset_id, table_name) combination already has
                # records in `self`, we assume that means all of the records
                # for that combination; we require other code to ensure entire
                # (parent) datasets are exported to these data structures
                # (never components).
                if not (this_records := this_table_records.setdefault(table_name, [])):
                    this_records.extend(records)

    def subset(self, dataset_ids: set[DatasetId]) -> DatastoreRecordData | None:
        """Extract a subset of the records that match given dataset IDs.

        Parameters
        ----------
        dataset_ids : `set` [ `DatasetId` ]
            Dataset IDs to match.

        Returns
        -------
        record_data : `DatastoreRecordData` or `None`
            `None` is returned if there are no matching refs.

        Notes
        -----
        Records in the returned instance are shared with this instance, clients
        should not update or extend records in the returned instance.
        """
        matching_records: dict[DatasetId, dict[str, list[StoredDatastoreItemInfo]]] = {}
        for dataset_id in dataset_ids:
            if (id_records := self.records.get(dataset_id)) is not None:
                matching_records[dataset_id] = id_records
        if matching_records:
            return DatastoreRecordData(records=matching_records)
        else:
            return None

    def to_simple(self, minimal: bool = False) -> SerializedDatastoreRecordData:
        """Make representation of the object for serialization.

        Implements `~lsst.daf.butler.json.SupportsSimple` protocol.

        Parameters
        ----------
        minimal : `bool`, optional
            If True produce minimal representation, not used by this method.

        Returns
        -------
        simple : `dict`
            Representation of this instance as a simple dictionary.
        """
        records: dict[str, dict[str, dict[str, list[_Record]]]] = {}
        for dataset_id, table_data in self.records.items():
            for table_name, table_records in table_data.items():
                class_name, infos = StoredDatastoreItemInfo.to_records(table_records)
                class_records = records.setdefault(class_name, {})
                dataset_records = class_records.setdefault(dataset_id.hex, {})
                dataset_records.setdefault(table_name, []).extend(dict(info) for info in infos)
        return SerializedDatastoreRecordData(dataset_ids=list(self.records.keys()), records=records)

    @classmethod
    def from_simple(
        cls,
        simple: SerializedDatastoreRecordData,
        universe: DimensionUniverse | None = None,
        registry: Registry | None = None,
    ) -> DatastoreRecordData:
        """Make an instance of this class from serialized data.

        Implements `~lsst.daf.butler.json.SupportsSimple` protocol.

        Parameters
        ----------
        data : `dict`
            Serialized representation returned from `to_simple` method.
        universe : `DimensionUniverse`, optional
            Dimension universe, not used by this method.
        registry : `Registry`, optional
            Registry instance, not used by this method.

        Returns
        -------
        item_info : `StoredDatastoreItemInfo`
            De-serialized instance of `StoredDatastoreItemInfo`.
        """
        cache = PersistenceContextVars.dataStoreRecords.get()
        key = frozenset(simple.dataset_ids)
        if cache is not None and (cachedRecord := cache.get(key)) is not None:
            return cachedRecord
        records: dict[DatasetId, dict[str, list[StoredDatastoreItemInfo]]] = {}
        # make sure that all dataset IDs appear in the dict even if they don't
        # have records.
        for dataset_id in simple.dataset_ids:
            records[dataset_id] = {}
        for class_name, class_data in simple.records.items():
            for dataset_id_str, dataset_data in class_data.items():
                for table_name, table_records in dataset_data.items():
                    try:
                        infos = StoredDatastoreItemInfo.from_records(class_name, table_records)
                    except TypeError as exc:
                        raise RuntimeError(
                            "The class specified in the SerializedDatastoreRecordData "
                            f"({class_name}) is not a StoredDatastoreItemInfo."
                        ) from exc
                    dataset_records = records.setdefault(uuid.UUID(dataset_id_str), {})
                    dataset_records.setdefault(table_name, []).extend(infos)
        newRecord = cls(records=records)
        if cache is not None:
            cache[key] = newRecord
        return newRecord

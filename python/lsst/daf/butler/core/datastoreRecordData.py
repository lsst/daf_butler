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

"""Support for generic data stores."""

from __future__ import annotations

__all__ = ("DatastoreRecordData", "SerializedDatastoreRecordData")

import dataclasses
import uuid
from collections import defaultdict
from typing import TYPE_CHECKING, AbstractSet, Any, Dict, List, Optional, Set

from lsst.utils import doImportType
from lsst.utils.introspection import get_full_type_name
from pydantic import BaseModel, validator

from .datasets import DatasetId
from .dimensions import DimensionUniverse
from .storedFileInfo import StoredDatastoreItemInfo

if TYPE_CHECKING:

    from ..registry import Registry

_Record = Dict[str, Any]


def _str_to_uuid(dataset_id: Any) -> uuid.UUID:
    """Convert dataset_id from string back to UUID, e.g. after JSON
    round-trip.
    """
    if isinstance(dataset_id, str):
        return uuid.UUID(dataset_id)
    else:
        assert isinstance(dataset_id, uuid.UUID), "Expecting UUIDs"
        return dataset_id


class SerializedDatastoreRecordData(BaseModel):
    """Representation of a `DatastoreRecordData` suitable for serialization."""

    dataset_ids: List[uuid.UUID]
    """List of dataset IDs"""

    records: Dict[str, Dict[str, List[_Record]]]
    """List of records indexed by record class name and table name."""

    @validator("dataset_ids")
    def dataset_ids_to_uuid(cls: Any, ids: List[Any]) -> List[uuid.UUID]:  # noqa: N805
        """Convert string dataset IDs to UUIDs"""
        return [_str_to_uuid(id) for id in ids]

    @validator("records")
    def record_ids_to_uuid(
        cls: Any, records: Dict[str, Dict[str, List[_Record]]]  # noqa: N805
    ) -> Dict[str, Dict[str, List[_Record]]]:
        """Convert string dataset IDs to UUIDs"""
        for table_data in records.values():
            for table_records in table_data.values():
                for record in table_records:
                    # This only checks dataset_id value, if there are any other
                    # columns that are UUIDs we'd need more generic approach.
                    if (dataset_id := record.get("dataset_id")) is not None:
                        record["dataset_id"] = _str_to_uuid(dataset_id)
        return records

    @classmethod
    def direct(
        cls,
        *,
        dataset_ids: List[uuid.UUID],
        records: Dict[str, Dict[str, List[_Record]]],
    ) -> SerializedDatastoreRecordData:
        """Construct a `SerializedDatastoreRecordData` directly without
        validators.

        This differs from the pydantic "construct" method in that the
        arguments are explicitly what the model requires, and it will recurse
        through members, constructing them from their corresponding `direct`
        methods.

        This method should only be called when the inputs are trusted.
        """
        data = SerializedDatastoreRecordData.__new__(cls)
        setter = object.__setattr__
        setter(data, "dataset_ids", [_str_to_uuid(dataset_id) for dataset_id in dataset_ids])
        # See also comments in record_ids_to_uuid()
        for table_data in records.values():
            for table_records in table_data.values():
                for record in table_records:
                    if (dataset_id := record.get("dataset_id")) is not None:
                        record["dataset_id"] = _str_to_uuid(dataset_id)
        setter(data, "records", records)
        return data


@dataclasses.dataclass
class DatastoreRecordData:
    """A struct that represents a tabular data export from a single
    datastore.
    """

    dataset_ids: Set[DatasetId] = dataclasses.field(default_factory=set)
    """List of DatasetIds known to this datastore.
    """

    records: Dict[DatasetId, Dict[str, List[StoredDatastoreItemInfo]]] = dataclasses.field(
        default_factory=lambda: defaultdict(lambda: defaultdict(list))
    )
    """Opaque table data, indexed by dataset ID and grouped by opaque table
    name."""

    def update(self, other: DatastoreRecordData) -> None:
        """Update contents of this instance with data from another instance.

        Parameters
        ----------
        other : `DatastoreRecordData`
            Records tho merge into this instance.
        """
        self.dataset_ids.update(other.dataset_ids)
        for dataset_id, table_records in other.records.items():
            this_table_records = self.records.setdefault(dataset_id, {})
            for table_name, records in table_records.items():
                this_table_records.setdefault(table_name, []).extend(records)

    def subset(self, dataset_ids: AbstractSet[DatasetId]) -> Optional[DatastoreRecordData]:
        """Extract a subset of the records that match given dataset IDs.

        Parameters
        ----------
        dataset_ids : `set` [ `DatasetId` ]
            Dataset IDs to match.

        Returns
        -------
        record_data : `DatastoreRecordData` or `None`
            `None` is returned if there are no matching refs.
        """
        matching_ids = self.dataset_ids & dataset_ids
        matching_records: Dict[DatasetId, Dict[str, List[StoredDatastoreItemInfo]]] = {}
        for dataset_id in matching_ids:
            if (id_records := self.records.get(dataset_id)) is not None:
                matching_records[dataset_id] = id_records
        if matching_ids or matching_records:
            return DatastoreRecordData(dataset_ids=matching_ids, records=matching_records)
        else:
            return None

    def to_simple(self, minimal: bool = False) -> SerializedDatastoreRecordData:
        """Make representation of the object for serialization.

        Implements `~lsst.daf.butler.core.json.SupportsSimple` protocol.

        Parameters
        ----------
        minimal : `bool`, optional
            If True produce minimal representation, not used by this method.

        Returns
        -------
        simple : `dict`
            Representation of this instance as a simple dictionary.
        """

        def _class_name(records: list[StoredDatastoreItemInfo]) -> str:
            """Get fully qualified class name for the records. Empty string
            returned if list is empty. Exception is raised if records are of
            different classes.
            """
            if not records:
                return ""
            classes = set(record.__class__ for record in records)
            assert len(classes) == 1, f"Records have to be of the same class: {classes}"
            return get_full_type_name(classes.pop())

        records: Dict[str, Dict[str, List[_Record]]] = defaultdict(lambda: defaultdict(list))
        for table_data in self.records.values():
            for table_name, table_records in table_data.items():
                class_name = _class_name(table_records)
                records[class_name][table_name].extend([record.to_record() for record in table_records])
        return SerializedDatastoreRecordData(dataset_ids=list(self.dataset_ids), records=records)

    @classmethod
    def from_simple(
        cls,
        simple: SerializedDatastoreRecordData,
        universe: Optional[DimensionUniverse] = None,
        registry: Optional[Registry] = None,
    ) -> DatastoreRecordData:
        """Make an instance of this class from serialized data.

        Implements `~lsst.daf.butler.core.json.SupportsSimple` protocol.

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
        records: Dict[DatasetId, Dict[str, List[StoredDatastoreItemInfo]]] = defaultdict(
            lambda: defaultdict(list)
        )
        for class_name, table_data in simple.records.items():
            klass = doImportType(class_name)
            for table_name, table_records in table_data.items():
                for record in table_records:
                    info = klass.from_record(record)
                    records[info.dataset_id][table_name].append(info)
        return cls(dataset_ids=set(simple.dataset_ids), records=records)

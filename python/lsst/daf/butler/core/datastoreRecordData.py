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
from typing import TYPE_CHECKING, AbstractSet, Dict, List, Optional

from pydantic import BaseModel

from .datasets import DatasetId
from .dimensions import DimensionUniverse
from .storedFileInfo import SerializedStoredDatastoreItemInfo, StoredDatastoreItemInfo

if TYPE_CHECKING:

    from ..registry import Registry
    from ..registry.interfaces import DatasetIdRef


class SerializedDatastoreRecordData(BaseModel):
    """Representation of a `DatastoreRecordData` suitable for serialization."""

    ref_ids: List[uuid.UUID]
    records: Dict[str, List[SerializedStoredDatastoreItemInfo]]

    @classmethod
    def direct(
        cls,
        *,
        ref_ids: List[uuid.UUID],
        records: Dict[str, List[Dict]],
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
        setter(data, "ref_ids", ref_ids)
        setter(
            data,
            "records",
            {k: [SerializedStoredDatastoreItemInfo.direct(**item) for item in v] for k, v in records.items()},
        )
        return data


@dataclasses.dataclass
class DatastoreRecordData:
    """A struct that represents a tabular data export from a single
    datastore.
    """

    refs: List["DatasetIdRef"] = dataclasses.field(default_factory=list)
    """List of DatasetRefs known to this datastore.
    """

    records: Dict[str, List[StoredDatastoreItemInfo]] = dataclasses.field(
        default_factory=lambda: defaultdict(list)
    )
    """Opaque table data, grouped by opaque table name.
    """

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
        matching_refs = [ref for ref in self.refs if ref.id in dataset_ids]
        records: Dict[str, List[StoredDatastoreItemInfo]] = {}
        for table_name, record_data in self.records.items():
            matching_records = [record for record in record_data if record.dataset_id in dataset_ids]
            if matching_records:
                records[table_name] = matching_records
        if matching_refs or matching_records:
            return DatastoreRecordData(refs=matching_refs, records=records)
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
        ref_ids = [ref.id for ref in self.refs]
        record_data = {
            table_name: [record.to_simple() for record in records]
            for table_name, records in self.records.items()
        }
        return SerializedDatastoreRecordData(ref_ids=ref_ids, records=record_data)

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
        # this causes dependency cycle if imported at top level
        from ..registry.interfaces import FakeDatasetRef

        refs: List[DatasetIdRef] = [FakeDatasetRef(id) for id in simple.ref_ids]
        record_data = {
            table_name: [StoredDatastoreItemInfo.from_simple(record) for record in records]
            for table_name, records in simple.records.items()
        }
        return cls(refs=refs, records=record_data)

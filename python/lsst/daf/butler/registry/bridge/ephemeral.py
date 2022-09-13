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

__all__ = ("EphemeralDatastoreRegistryBridge",)

from contextlib import contextmanager
from typing import TYPE_CHECKING, Iterable, Iterator, Optional, Set, Tuple, Type

from ...core import DatasetId
from ..interfaces import DatasetIdRef, DatastoreRegistryBridge, FakeDatasetRef, OpaqueTableStorage

if TYPE_CHECKING:
    from ...core import StoredDatastoreItemInfo
    from ...core.datastore import DatastoreTransaction


class EphemeralDatastoreRegistryBridge(DatastoreRegistryBridge):
    """An implementation of `DatastoreRegistryBridge` for ephemeral datastores
    - those whose artifacts never outlive the current process.

    Parameters
    ----------
    datastoreName : `str`
        Name of the `Datastore` as it should appear in `Registry` tables
        referencing it.

    Notes
    -----
    The current implementation just uses a Python set to remember the dataset
    IDs associated with the datastore.  This will probably need to be converted
    to use in-database temporary tables instead in the future to support
    "in-datastore" constraints in `Registry.queryDatasets`.
    """

    def __init__(self, datastoreName: str):
        super().__init__(datastoreName)
        self._datasetIds: Set[DatasetId] = set()
        self._trashedIds: Set[DatasetId] = set()

    def insert(self, refs: Iterable[DatasetIdRef]) -> None:
        # Docstring inherited from DatastoreRegistryBridge
        self._datasetIds.update(ref.getCheckedId() for ref in refs)

    def forget(self, refs: Iterable[DatasetIdRef]) -> None:
        self._datasetIds.difference_update(ref.id for ref in refs)

    def _rollbackMoveToTrash(self, refs: Iterable[DatasetIdRef]) -> None:
        """Rollback a moveToTrash call."""
        for ref in refs:
            self._trashedIds.remove(ref.getCheckedId())

    def moveToTrash(self, refs: Iterable[DatasetIdRef], transaction: Optional[DatastoreTransaction]) -> None:
        # Docstring inherited from DatastoreRegistryBridge
        if transaction is None:
            raise RuntimeError("Must be called with a defined transaction.")
        ref_list = list(refs)
        with transaction.undoWith(f"Trash {len(ref_list)} datasets", self._rollbackMoveToTrash, ref_list):
            self._trashedIds.update(ref.getCheckedId() for ref in ref_list)

    def check(self, refs: Iterable[DatasetIdRef]) -> Iterable[DatasetIdRef]:
        # Docstring inherited from DatastoreRegistryBridge
        yield from (ref for ref in refs if ref in self)

    def __contains__(self, ref: DatasetIdRef) -> bool:
        return ref.getCheckedId() in self._datasetIds and ref.getCheckedId() not in self._trashedIds

    @contextmanager
    def emptyTrash(
        self,
        records_table: Optional[OpaqueTableStorage] = None,
        record_class: Optional[Type[StoredDatastoreItemInfo]] = None,
        record_column: Optional[str] = None,
    ) -> Iterator[
        Tuple[Iterable[Tuple[DatasetIdRef, Optional[StoredDatastoreItemInfo]]], Optional[Set[str]]]
    ]:
        # Docstring inherited from DatastoreRegistryBridge
        matches: Iterable[Tuple[FakeDatasetRef, Optional[StoredDatastoreItemInfo]]] = ()
        if isinstance(records_table, OpaqueTableStorage):
            if record_class is None:
                raise ValueError("Record class must be provided if records table is given.")
            matches = (
                (FakeDatasetRef(id), record_class.from_record(record))
                for id in self._trashedIds
                for record in records_table.fetch(dataset_id=id)
            )
        else:
            matches = ((FakeDatasetRef(id), None) for id in self._trashedIds)

        # Indicate to caller that we do not know about artifacts that
        # should be retained.
        yield ((matches, None))

        if isinstance(records_table, OpaqueTableStorage):
            # Remove the records entries
            records_table.delete(["dataset_id"], *[{"dataset_id": id} for id in self._trashedIds])

        # Empty the trash table
        self._datasetIds.difference_update(self._trashedIds)
        self._trashedIds = set()

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

__all__ = ("FileDataset", "SerializedFileDatasetList")

from collections.abc import Iterable
from dataclasses import dataclass
from itertools import groupby
from typing import Any, Literal, NamedTuple

import pydantic

from lsst.resources import ResourcePath, ResourcePathExpression

from ._dataset_ref import DatasetId, DatasetRef
from ._formatter import FormatterParameter
from .dimensions import SerializedDataId


@dataclass
class FileDataset:
    """A struct that represents a dataset exported to a file.

    Parameters
    ----------
    path : `lsst.resources.ResourcePath` or `str`
        Path to the dataset (`lsst.resources.ResourcePath` or `str`).

        If the dataset was exported with ``transfer=None`` (i.e. in-place),
        this is relative to the datastore root (only datastores that have a
        well-defined root in the local filesystem can be expected to support
        in-place exports).  Otherwise this is relative to the directory passed
        to `Datastore.export`.
    refs : `list` [ `DatasetRef` ]
        Registry information about the dataset.
    formatter : `Formatter` or `str` or `None`, optional
        A `Formatter` class or fully-qualified name.
    """

    __slots__ = ("refs", "path", "formatter")

    refs: list[DatasetRef]
    path: str | ResourcePath
    formatter: FormatterParameter | None

    def __init__(
        self,
        path: ResourcePathExpression,
        refs: DatasetRef | list[DatasetRef],
        *,
        formatter: FormatterParameter | None = None,
    ):
        # Do not want to store all possible options supported by ResourcePath
        # so force a conversion for the non-str parameters.
        self.path = path if isinstance(path, str) else ResourcePath(path, forceAbsolute=False)
        if isinstance(refs, DatasetRef):
            refs = [refs]
        runs = {ref.run for ref in refs}
        if len(runs) != 1:
            raise ValueError(f"Supplied refs must all share the same run. Got: {runs}")
        self.refs = refs
        self.formatter = formatter

    def __lt__(self, other: Any) -> bool:
        # Sort on path alone
        if not isinstance(other, type(self)):
            return NotImplemented
        return str(self.path) < str(other.path)


class SerializedFileDatasetList(pydantic.BaseModel):
    """Serializable list of `FileDataset` objects, with records sharing a
    single dataset type and run collection.

    Notes
    -----
    This is identical to the YAML export format that has existed for many
    years.
    """

    type: Literal["dataset"]
    dataset_type: str
    run: str
    records: list[SerializedFileDatasetRecord]

    @staticmethod
    def from_file_datasets(datasets: Iterable[FileDataset]) -> list[SerializedFileDatasetList]:
        groups = groupby(sorted(datasets, key=_get_dataset_type_and_run), key=_get_dataset_type_and_run)
        output = []
        for key, group in groups:
            output.append(
                SerializedFileDatasetList.from_file_datasets_of_single_type_and_run(
                    key.dataset_type, key.run, group
                )
            )

        return output

    @staticmethod
    def from_file_datasets_of_single_type_and_run(
        dataset_type_name: str, run: str, datasets: Iterable[FileDataset]
    ) -> SerializedFileDatasetList:
        return SerializedFileDatasetList(
            type="dataset",
            dataset_type=dataset_type_name,
            run=run,
            records=[SerializedFileDatasetRecord.from_file_dataset(dataset) for dataset in datasets],
        )


class SerializedFileDatasetRecord(pydantic.BaseModel):
    """Inner record for `SerializedFileDatasetList` representing a single
    `FileDataset`.
    """

    dataset_id: list[DatasetId]
    data_id: list[SerializedDataId]
    path: str
    formatter: str | None = None

    @staticmethod
    def from_file_dataset(dataset: FileDataset) -> SerializedFileDatasetRecord:
        if dataset.formatter is None:
            formatter = None
        elif isinstance(dataset.formatter, str):
            formatter = dataset.formatter
        else:
            formatter = dataset.formatter.name()

        sorted_refs = sorted(dataset.refs)

        return SerializedFileDatasetRecord(
            dataset_id=[ref.id for ref in sorted_refs],
            data_id=[dict(ref.dataId.required) for ref in sorted_refs],
            path=str(dataset.path),
            formatter=formatter,
        )


class _DatasetTypeAndRun(NamedTuple):
    dataset_type: str
    run: str


def _get_dataset_type_and_run(dataset: FileDataset) -> _DatasetTypeAndRun:
    """Return the dataset type and run collection associated with a
    FileDataset.
    """
    assert len(dataset.refs) > 0, "Expected at least one DatasetRef in the list"
    dataset_type_names = set()
    runs = set()
    for ref in dataset.refs:
        dataset_type_names.add(ref.datasetType.name)
        runs.add(ref.run)
    assert len(dataset_type_names) == 1, "Expected all refs to share a single DatasetType"
    assert len(runs) == 1, "Expected all refs to share a single run"
    return _DatasetTypeAndRun(dataset_type=dataset_type_names.pop(), run=runs.pop())

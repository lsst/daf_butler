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

__all__ = ("DatasetProvenance",)

import typing
import uuid

import pydantic

from ._dataset_ref import DatasetRef, SerializedDatasetRef


class DatasetProvenance(pydantic.BaseModel):
    """Provenance of a single `DatasetRef`."""

    inputs: list[SerializedDatasetRef] = pydantic.Field(default_factory=list)
    """The input datasets."""
    quantum_id: uuid.UUID | None = None
    """Identifier of the Quantum that was executed."""
    extras: dict[uuid.UUID, dict[str, int | float | str | bool]] = pydantic.Field(default_factory=dict)
    """Extra provenance information associated with a particular dataset."""
    _uuids: set[uuid.UUID] = pydantic.PrivateAttr(default_factory=set)

    @pydantic.model_validator(mode="after")
    def populate_cache(self) -> typing.Self:
        for ref in self.inputs:
            self._uuids.add(ref.id)
        return self

    def add_input(self, ref: DatasetRef) -> None:
        """Add an input dataset to the provenance.

        Parameters
        ----------
        ref : `DatasetRef`
            A dataset to register as an input.
        """
        if ref.id in self._uuids:
            # Already registered.
            return
        self._uuids.add(ref.id)
        self.inputs.append(ref.to_simple())

    def add_extra_provenance(self, dataset_id: uuid.UUID, extra: dict[str, int | float | str | bool]) -> None:
        """Attach extra provenance to a specific dataset.

        Parameters
        ----------
        dataset_id : `uuid.UUID`
            The ID of the dataset to receive this provenance.
        extra : `dict` [ `str`, `typing.Any` ]
            The extra provenance information as a dictionary. The values
            must be simple Python scalars.
        """
        if dataset_id not in self._uuids:
            raise ValueError(f"The given dataset ID {dataset_id} is not known to this provenance instance.")
        self.extras.setdefault(dataset_id, {}).update(extra)

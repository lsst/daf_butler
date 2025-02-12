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

import uuid
from typing import TYPE_CHECKING, Any, Self

import pydantic

from ._dataset_ref import DatasetRef, SerializedDatasetRef

if TYPE_CHECKING:
    from .dimensions import DataIdValue


class DatasetProvenance(pydantic.BaseModel):
    """Provenance of a single `DatasetRef`."""

    inputs: list[SerializedDatasetRef] = pydantic.Field(default_factory=list)
    """The input datasets."""
    quantum_id: uuid.UUID | None = None
    """Identifier of the Quantum that was executed."""
    extras: dict[uuid.UUID, dict[str, Any]] = pydantic.Field(default_factory=dict)
    """Extra provenance information associated with a particular dataset."""
    _uuids: set[uuid.UUID] = pydantic.PrivateAttr(default_factory=set)

    @pydantic.model_validator(mode="after")
    def populate_cache(self) -> Self:
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

    def add_extra_provenance(self, dataset_id: uuid.UUID, extra: dict[str, Any]) -> None:
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

    def to_flat_dict(
        self,
        ref: DatasetRef | None,
        /,
        *,
        prefix: str = "",
        sep: str = ".",
        simple_types: bool = False,
    ) -> dict[str, int | str | bool | float | uuid.UUID | DataIdValue]:
        """Return provenance as a flattened dictionary.

        Parameters
        ----------
        ref : `DatasetRef` or `None`
            If given, a dataset for which this provenance is relevant and
            should be included.
        prefix : `str`, optional
            A prefix to use for each key in the provenance dictionary.
        sep : `str`, optional
            Separator to use to represent hierarchy.
        simple_types : `bool`, optional
            If `True` only simple Python types will be used in the returned
            dictionary, specifically UUIDs will be returned as `str`. If
            `False`, UUIDs will be returned as `uuid.UUID`. Complex types
            found in `DatasetProvenance.extras` will be cast to a `str`
            if `True`.

        Returns
        -------
        prov : `dict`
            Dictionary representing the provenance. The keys are defined
            in the notes below.

        Notes
        -----
        The provenance keys created by this method are defined below. The
        case of the keys will match the case of the first character of the
        prefix (defined by whether `str.isupper()` returns true, else they will
        be lower case).

        Keys from the given dataset (all optional if no dataset is given):

        :id: UUID of the given dataset.
        :run: Run of the given dataset.
        :datasettype: Dataset type of the given dataset.
        :dataid x: An entry for each required dimension in the data ID.

        Each input dataset will have the ``id``, ``run``, and ``datasettype``
        keys as defined above (but no ``dataid`` key) with an ``input N``
        prefix where ``N`` starts counting at 0.

        The quantum ID, if present, will use key ``quantum``.
        """
        use_upper = prefix[0].isupper() if prefix else False

        def _make_key(*keys: str | int) -> str:
            """Make the key in the correct form."""
            start = prefix
            if start:
                start += sep
            k = sep.join(str(kk) for kk in keys)
            if use_upper:
                k = k.upper()
            return f"{start}{k}"

        prov: dict[str, int | float | str | bool | uuid.UUID | DataIdValue] = {}
        if ref is not None:
            prov[_make_key("id")] = ref.id if not simple_types else str(ref.id)
            prov[_make_key("run")] = ref.run
            prov[_make_key("datasettype")] = ref.datasetType.name
            for k, v in sorted(ref.dataId.required.items()):
                prov[_make_key("dataid", k)] = v

        if self.quantum_id is not None:
            prov[_make_key("quantum")] = self.quantum_id if not simple_types else str(self.quantum_id)

        for i, input in enumerate(self.inputs):
            prov[_make_key("input", i, "id")] = input.id if not simple_types else str(input.id)
            if input.run is not None:  # for mypy
                prov[_make_key("input", i, "run")] = input.run
            if input.datasetType is not None:  # for mypy
                prov[_make_key("input", i, "datasettype")] = input.datasetType.name

            if input.id in self.extras:
                for xk, xv in self.extras[input.id].items():
                    if simple_types and not isinstance(xv, str | float | int | bool):
                        xv = str(xv)
                    prov[_make_key("input", i, xk)] = xv

        return prov

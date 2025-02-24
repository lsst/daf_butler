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

import re
import uuid
from typing import TYPE_CHECKING, Any, Self, TypeAlias

import pydantic

from ._dataset_ref import DatasetRef, SerializedDatasetRef
from .dimensions import DataIdValue

if TYPE_CHECKING:
    from collections.abc import Mapping, MutableMapping

    from ._butler import Butler

# Types that can be stored in a provenance dictionary.
_PROV_TYPES: TypeAlias = str | int | float | bool | DataIdValue | uuid.UUID


class DatasetProvenance(pydantic.BaseModel):
    """Provenance of a single `DatasetRef`."""

    inputs: list[SerializedDatasetRef] = pydantic.Field(default_factory=list)
    """The input datasets."""
    quantum_id: uuid.UUID | None = None
    """Identifier of the Quantum that was executed."""
    extras: dict[uuid.UUID, dict[str, _PROV_TYPES]] = pydantic.Field(default_factory=dict)
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

    def add_extra_provenance(self, dataset_id: uuid.UUID, extra: Mapping[str, _PROV_TYPES]) -> None:
        """Attach extra provenance to a specific dataset.

        Parameters
        ----------
        dataset_id : `uuid.UUID`
            The ID of the dataset to receive this provenance.
        extra : `~collections.abc.Mapping` [ `str`, `typing.Any` ]
            The extra provenance information as a dictionary. The values
            must be simple Python scalars or scalars that can be serialized
            by Pydantic and convert to a simple string value.

        Notes
        -----
        The keys in the extra provenance can not include provenance keys of
        ``run``, ``id``, or ``datasettype`` (in upper or lower case).
        """
        if dataset_id not in self._uuids:
            raise ValueError(f"The given dataset ID {dataset_id} is not known to this provenance instance.")
        extra_keys = {k.lower() for k in extra.keys()}
        if overlap := (extra_keys & {"run", "id", "datasettype"}):
            raise ValueError(f"Extra provenance includes a reserved provenance key: {overlap}")

        self.extras.setdefault(dataset_id, {}).update(extra)

    def to_flat_dict(
        self,
        ref: DatasetRef | None,
        /,
        *,
        prefix: str = "",
        sep: str = ".",
        simple_types: bool = False,
        use_upper: bool | None = None,
    ) -> dict[str, _PROV_TYPES]:
        """Return provenance as a flattened dictionary.

        Parameters
        ----------
        ref : `DatasetRef` or `None`
            If given, a dataset for which this provenance is relevant and
            should be included.
        prefix : `str`, optional
            A prefix to use for each key in the provenance dictionary.
        sep : `str`, optional
            Separator to use to represent hierarchy. Must be a single
            character. Can not be a number, letter, or underscore (to avoid
            confusion with provenance keys themselves).
        simple_types : `bool`, optional
            If `True` only simple Python types will be used in the returned
            dictionary, specifically UUIDs will be returned as `str`. If
            `False`, UUIDs will be returned as `uuid.UUID`. Complex types
            found in `DatasetProvenance.extras` will be cast to a `str`
            if `True`.
        use_upper : `bool` or `None`, optional
            If `None` the case of the keys matches the case of the first
            character of the prefix (defined by whether `str.isupper()` returns
            true, else they will be lower case). If `False` the case will be
            lower case, and if `True` the case will be upper case.

        Returns
        -------
        prov : `dict`
            Dictionary representing the provenance. The keys are defined
            in the notes below.

        Notes
        -----
        Keys from the given dataset (all optional if no dataset is given):

        :id: UUID of the given dataset.
        :run: Run of the given dataset.
        :datasettype: Dataset type of the given dataset.
        :dataid x: An entry for each required dimension, "x", in the data ID.

        Each input dataset will have the ``id``, ``run``, and ``datasettype``
        keys as defined above (but no ``dataid`` key) with an ``input N``
        prefix where ``N`` starts counting at 0.

        The quantum ID, if present, will use key ``quantum``.

        Examples
        --------
        >>> provenance.to_flat_dict(
        ...     ref, prefix="lsst.butler", sep=".", simple_types=True
        ... )
        {
            "lsst.butler.id": "ae0fa83d-cc89-41dd-9680-f97ede49f01e",
            "lsst.butler.run": "test_run",
            "lsst.butler.datasettype": "data",
            "lsst.butler.dataid.detector": 10,
            "lsst.butler.dataid.instrument": "LSSTCam",
            "lsst.butler.quantum": "d93a735b-08f0-477d-bc95-2cc32d6d898b",
            "lsst.butler.input.0.id": "3dfd7ba5-5e35-4565-9d87-4b33880ed06c",
            "lsst.butler.input.0.run": "other_run",
            "lsst.butler.input.0.datasettype": "astropy_parquet",
            "lsst.butler.input.1.id": "7a99f6e9-4035-3d68-842e-58ecce1dc935",
            "lsst.butler.input.1.run": "other_run",
            "lsst.butler.input.1.datasettype": "astropy_parquet",
        }

        Raises
        ------
        ValueError
            Raised if the separator is not a single character.
        """
        if len(sep) != 1:
            raise ValueError(f"Separator for provenance keys must be a single character. Got {sep!r}.")
        if re.match(r"[_\w\d]$", sep):
            raise ValueError(
                f"Separator for provenance keys can not be word character or underscore. Got {sep!r}."
            )

        def _make_key(*keys: str | int) -> str:
            """Make the key in the correct form with simpler API."""
            return self._make_provenance_key(prefix, sep, use_upper, *keys)

        prov: dict[str, _PROV_TYPES] = {}
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

    @staticmethod
    def _make_provenance_key(prefix: str, sep: str, use_upper: bool | None, *keys: str | int) -> str:
        """Construct provenance key from prefix and separator.

        Parameters
        ----------
        prefix : `str`
            A prefix to use for each key in the provenance dictionary.
        sep : `str`
            Separator to use to represent hierarchy. Must be a single
            character.
        use_upper : `bool` or `None`
            If `True` use upper case for provenance keys, if `False` use lower
            case, if `None` match the case of the prefix.
        keys : `tuple` of `str` | `int`
            Components of key to combine with prefix and separator.

        Returns
        -------
        key : `str`
            Key to use in dictionary. Case of result will match case of
            prefix (defaulting to lower case if the first character of
            prefix has no case).
        """
        if use_upper is None:
            use_upper = prefix[0].isupper() if prefix else False
        if prefix:
            prefix += sep
        k = sep.join(str(kk) for kk in keys)
        if use_upper:
            k = k.upper()
        return f"{prefix}{k}"

    @staticmethod
    def _find_prefix_and_sep(prov_dict: Mapping[str, Any]) -> tuple[str, str] | tuple[None, None]:
        """Given a mapping try to determine the prefix and separator for
        provenance keys.

        Parameters
        ----------
        prov_dict : `collections.abc.Mapping`
            Mapping to scan. Assumed to include keys populated by
            `to_flat_dict`.

        Returns
        -------
        prefix : `str`
            Prefix given to `to_flat_dict`. `None` if no provenance headers
            were found.
        sep : `str`
            Separator given to `to_flat_dict`. `None` if no provenance headers
            were found.

        Raises
        ------
        ValueError
            Raised if more than one value was found for either the prefix or
            separator.
        """
        # Best keys to look for are dataid and input 0.
        # dataid is only used in ref provenance and always has a separator.
        # input 0 is always present if there is any input provenance.

        def _update_matches(match: re.Match, prefixes: set[str], separators: set[str]) -> None:
            prefix, *seps = match.groups()
            if prefix:
                # Will have a separator at the end.
                prefix, sep = prefix[:-1], prefix[-1]
                separators.add(sep)
            prefixes.add(prefix)
            separators |= set(seps)

        separators: set[str] = set()
        prefixes: set[str] = set()

        # It is possible for there to be no inputs and just a reference
        # dataset. If that reference dataset has no DATAID then the simple
        # logic will not work. In that scenario look for the presence
        # of RUN, DATASETTYPE and ID to spot that provenance exists.
        # In this case it may not be possible to determine a sep value.
        backup = {}

        for k in prov_dict:
            if match := re.match("(.*)input(.)0(.)(?:id|datasettype|run)$", k, flags=re.IGNORECASE):
                _update_matches(match, prefixes, separators)
            elif match := re.match(
                # Data coordinates are a-z or underscore.
                "(.*)dataid(.)[a-z_]+$",
                k,
                flags=re.IGNORECASE,
            ):
                _update_matches(match, prefixes, separators)
            elif match := re.match(r"(.*)\b(id|datasettype|run)$", k, flags=re.IGNORECASE):
                prefix, key = match.groups()
                backup[key.lower()] = prefix

        if not prefixes:
            if "run" in backup and "datasettype" in backup and "id" in backup:
                # Looks like there is a provenance after all. All 3 must be
                # present.
                for prefix in backup.values():
                    if prefix:
                        prefix, sep = prefix[:-1], prefix[-1]
                    else:
                        sep = " "  # Will not be used.
                    prefixes.add(prefix)
                    separators.add(sep)

        if not prefixes:
            return None, None

        if len(separators) > 1:
            raise ValueError(
                f"Inconsistent values found for separators in provenance header. Got {separators}."
            )
        if len(prefixes) > 1:
            raise ValueError(f"Inconsistent values for prefix found in provenance headers. Got {prefixes}.")
        return prefixes.pop(), separators.pop()

    @classmethod
    def _find_provenance_keys_in_flat_dict(cls, prov_dict: Mapping[str, Any]) -> dict[str, str]:
        """Find the provenance keys in a dictionary.

        Parameters
        ----------
        prov_dict : `collections.abc.Mapping`
            Dictionary to be analyzed. Assumed to have been populated by
            `to_flat_dict`.

        Returns
        -------
        prov_keys : `dict` [ `str`, `str` ]
            Provenance key as found in the given header mapping to the
            standardized provenance key (with prefix removed and "."
            separator).
        """
        prefix, sep = cls._find_prefix_and_sep(prov_dict)

        if prefix is None:
            return {}
        # for mypy which can not work out that the above method returns
        # str, str or None, None.
        if sep is None:
            return {}
        if prefix:
            # Prefix will always include the separator if it is defined.
            prefix += sep

        core_provenance = tuple(f"{prefix}{k}".lower() for k in ("run", "id", "datasettype", "quantum"))

        # Need to escape the prefix and separator for regex usage.
        esc_sep = re.escape(sep)
        esc_prefix = re.escape(prefix)
        prov_keys: dict[str, str] = {}
        for k in list(prov_dict):
            # the input provenance can include extra keys that we cannot
            # know so have to match solely on INPUT N.
            found_key = False
            if re.match(rf"{esc_prefix}input{esc_sep}(\d+){esc_sep}(.*)$", k, flags=re.IGNORECASE):
                found_key = True
            elif k.lower() in core_provenance:
                found_key = True
            elif re.match(f"{esc_prefix}dataid{esc_sep}[a-z_]+$", k, flags=re.IGNORECASE):
                found_key = True

            if found_key:
                standard = k.removeprefix(prefix)
                standard = standard.replace(sep, ".")
                prov_keys[k] = standard.lower()

        return prov_keys

    @classmethod
    def strip_provenance_from_flat_dict(cls, prov_dict: MutableMapping[str, Any]) -> None:
        """Remove provenance keys from a mapping that had been populated
        by `to_flat_dict`.

        Parameters
        ----------
        prov_dict : `collections.abc.MutableMapping`
            Dictionary to modify.
        """
        for prov_key in cls._find_provenance_keys_in_flat_dict(prov_dict):
            del prov_dict[prov_key]

        return

    @classmethod
    def from_flat_dict(cls, prov_dict: Mapping[str, Any], butler: Butler) -> tuple[Self, DatasetRef | None]:
        """Create a provenance object from a provenance dictionary.

        Parameters
        ----------
        prov_dict : `collections.abc.Mapping`
            Dictionary populated by `to_flat_dict`.
        butler : `lsst.daf.butler.Butler`
            Butler to query to find references datasets.

        Returns
        -------
        prov : `DatasetProvenance`
            Provenance extracted from this object.
        ref : `DatasetRef` or `None`
            Dataset associated with this provenance. Can be `None` if no
            provenance found.

        Raises
        ------
        ValueError
            Raised if no provenance values are found in the dictionary.
        RuntimeError
            Raised if a referenced dataset is not known to the given butler.
        """
        prov_keys = cls._find_provenance_keys_in_flat_dict(prov_dict)
        if not prov_keys:
            raise ValueError("No provenance information found in header.")

        def _coerce_id(id_: str | uuid.UUID) -> uuid.UUID:
            if isinstance(id_, uuid.UUID):
                return id_
            return uuid.UUID(hex=id_)

        quantum_id = None
        ref_id = None
        input_ids = {}
        extras: dict[int, dict[str, Any]] = {}

        for k, standard in prov_keys.items():
            if standard == "id":
                ref_id = _coerce_id(prov_dict[k])
            elif standard == "quantum":
                quantum_id = _coerce_id(prov_dict[k])
            elif match := re.match(r"input.(\d+).([a-z_]+)$", standard):
                input_num = int(match.group(1))
                subkey = match.group(2)
                if subkey == "id":
                    input_ids[input_num] = _coerce_id(prov_dict[k])
                elif subkey not in ("datasettype", "run"):
                    # Extra information. Can not know the original case
                    # but can match to the original dictionary.
                    if k[0].isupper():
                        subkey = subkey.upper()
                    extras.setdefault(input_num, {})[subkey] = prov_dict[k]

        ref = None
        if ref_id is not None:
            ref = butler.get_dataset(ref_id)
            if ref is None:
                raise ValueError(
                    f"Dataset associated with this provenance ({ref_id}) is not known to this butler."
                )

        provenance = cls(quantum_id=quantum_id)

        for i in sorted(input_ids):
            input_ref = butler.get_dataset(input_ids[i])
            if input_ref is None:
                raise ValueError(f"Input dataset ({input_ids[i]}) is not known to this butler.")
            provenance.add_input(input_ref)
            if i in extras:
                provenance.add_extra_provenance(input_ref.id, extras[i])

        return provenance, ref

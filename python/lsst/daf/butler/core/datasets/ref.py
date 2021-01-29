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

__all__ = ["AmbiguousDatasetError", "DatasetRef"]

from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
)

from ..dimensions import DataCoordinate, DimensionGraph, DimensionUniverse
from ..configSupport import LookupKey
from ..utils import immutable
from ..named import NamedKeyDict
from .type import DatasetType
from ..json import from_json_generic, to_json_generic

if TYPE_CHECKING:
    from ...registry import Registry


class AmbiguousDatasetError(Exception):
    """Exception raised when a `DatasetRef` is not resolved (has no ID or run),
    but the requested operation requires one of them.
    """


@immutable
class DatasetRef:
    """Reference to a Dataset in a `Registry`.

    A `DatasetRef` may point to a Dataset that currently does not yet exist
    (e.g., because it is a predicted input for provenance).

    Parameters
    ----------
    datasetType : `DatasetType`
        The `DatasetType` for this Dataset.
    dataId : `DataCoordinate`
        A mapping of dimensions that labels the Dataset within a Collection.
    id : `int`, optional
        The unique integer identifier assigned when the dataset is created.
    run : `str`, optional
        The name of the run this dataset was associated with when it was
        created.  Must be provided if ``id`` is.
    conform : `bool`, optional
        If `True` (default), call `DataCoordinate.standardize` to ensure that
        the data ID's dimensions are consistent with the dataset type's.
        `DatasetRef` instances for which those dimensions are not equal should
        not be created in new code, but are still supported for backwards
        compatibility.  New code should only pass `False` if it can guarantee
        that the dimensions are already consistent.

    Raises
    ------
    ValueError
        Raised if ``run`` is provided but ``id`` is not, or if ``id`` is
        provided but ``run`` is not.
    """

    __slots__ = ("id", "datasetType", "dataId", "run",)

    def __init__(
        self,
        datasetType: DatasetType, dataId: DataCoordinate, *,
        id: Optional[int] = None,
        run: Optional[str] = None,
        conform: bool = True
    ):
        self.id = id
        self.datasetType = datasetType
        if conform:
            self.dataId = DataCoordinate.standardize(dataId, graph=datasetType.dimensions)
        else:
            self.dataId = dataId
        if self.id is not None:
            if run is None:
                raise ValueError(f"Cannot provide id without run for dataset with id={id}, "
                                 f"type={datasetType}, and dataId={dataId}.")
            self.run = run
        else:
            if run is not None:
                raise ValueError("'run' cannot be provided unless 'id' is.")
            self.run = None

    def __eq__(self, other: Any) -> bool:
        try:
            return (self.datasetType, self.dataId, self.id) == (other.datasetType, other.dataId, other.id)
        except AttributeError:
            return NotImplemented

    def __hash__(self) -> int:
        return hash((self.datasetType, self.dataId, self.id))

    @property
    def dimensions(self) -> DimensionGraph:
        """The dimensions associated with the underlying `DatasetType`
        """
        return self.datasetType.dimensions

    def __repr__(self) -> str:
        # We delegate to __str__ (i.e use "!s") for the data ID) below because
        # DataCoordinate's __repr__ - while adhering to the guidelines for
        # __repr__ - is much harder to users to read, while its __str__ just
        # produces a dict that can also be passed to DatasetRef's constructor.
        if self.id is not None:
            return (f"DatasetRef({self.datasetType!r}, {self.dataId!s}, id={self.id}, run={self.run!r})")
        else:
            return f"DatasetRef({self.datasetType!r}, {self.dataId!s})"

    def __str__(self) -> str:
        s = f"{self.datasetType.name}@{self.dataId!s}, sc={self.datasetType.storageClass.name}]"
        if self.id is not None:
            s += f" (id={self.id})"
        return s

    def __lt__(self, other: Any) -> bool:
        # Sort by run, DatasetType name and then by DataCoordinate
        # The __str__ representation is probably close enough but we
        # need to ensure that sorting a DatasetRef matches what you would
        # get if you sorted DatasetType+DataCoordinate
        if not isinstance(other, type(self)):
            return NotImplemented

        # Group by run if defined, takes precedence over DatasetType
        self_run = "" if self.run is None else self.run
        other_run = "" if other.run is None else other.run

        # Compare tuples in the priority order
        return (self_run, self.datasetType, self.dataId) < (other_run, other.datasetType, other.dataId)

    def to_simple(self, minimal: bool = False) -> Dict:
        """Convert this class to a simple python type suitable for
        serialization.

        Parameters
        ----------
        minimal : `bool`, optional
            Use minimal serialization. Requires Registry to convert
            back to a full type.

        Returns
        -------
        simple : `dict` or `int`
            The object converted to a dictionary.
        """
        if minimal and self.id is not None:
            # The only thing needed to uniquely define a DatasetRef
            # is the integer id so that can be used directly if it is
            # resolved and if it is not a component DatasetRef.
            # Store is in a dict to allow us to easily add the planned
            # origin information later without having to support
            # an int and dict in simple form.
            simple: Dict[str, Any] = {"id": self.id}
            if self.isComponent():
                # We can still be a little minimalist with a component
                # but we will also need to record the datasetType component
                simple["component"] = self.datasetType.component()
            return simple

        # Convert to a dict form
        as_dict: Dict[str, Any] = {"datasetType": self.datasetType.to_simple(minimal=minimal),
                                   "dataId": self.dataId.to_simple(),
                                   }

        # Only include the id entry if it is defined
        if self.id is not None:
            as_dict["run"] = self.run
            as_dict["id"] = self.id

        return as_dict

    @classmethod
    def from_simple(cls, simple: Dict,
                    universe: Optional[DimensionUniverse] = None,
                    registry: Optional[Registry] = None) -> DatasetRef:
        """Construct a new object from the data returned from the `to_simple`
        method.

        Parameters
        ----------
        simple : `dict` of [`str`, `Any`]
            The value returned by `to_simple()`.
        universe : `DimensionUniverse`
            The special graph of all known dimensions.
            Can be `None` if a registry is provided.
        registry : `lsst.daf.butler.Registry`, optional
            Registry to use to convert simple form of a DatasetRef to
            a full `DatasetRef`. Can be `None` if a full description of
            the type is provided along with a universe.

        Returns
        -------
        ref : `DatasetRef`
            Newly-constructed object.
        """

        # Minimalist component will just specify component and id and
        # require registry to reconstruct
        if set(simple).issubset({"id", "component"}):
            if registry is None:
                raise ValueError("Registry is required to construct component DatasetRef from integer id")
            ref = registry.getDataset(simple["id"])
            if ref is None:
                raise RuntimeError(f"No matching dataset found in registry for id {simple['id']}")
            if "component" in simple:
                ref = ref.makeComponentRef(simple["component"])
            return ref

        if universe is None and registry is None:
            raise ValueError("One of universe or registry must be provided.")

        if universe is None and registry is not None:
            universe = registry.dimensions

        if universe is None:
            # this is for mypy
            raise ValueError("Unable to determine a usable universe")

        datasetType = DatasetType.from_simple(simple["datasetType"], universe=universe, registry=registry)
        dataId = DataCoordinate.from_simple(simple["dataId"], universe=universe)
        return cls(datasetType, dataId,
                   id=simple["id"], run=simple["run"])

    to_json = to_json_generic
    from_json = classmethod(from_json_generic)

    @classmethod
    def _unpickle(
        cls,
        datasetType: DatasetType,
        dataId: DataCoordinate,
        id: Optional[int],
        run: Optional[str],
    ) -> DatasetRef:
        """A custom factory method for use by `__reduce__` as a workaround for
        its lack of support for keyword arguments.
        """
        return cls(datasetType, dataId, id=id, run=run)

    def __reduce__(self) -> tuple:
        return (self._unpickle, (self.datasetType, self.dataId, self.id, self.run))

    def __deepcopy__(self, memo: dict) -> DatasetRef:
        # DatasetRef is recursively immutable; see note in @immutable
        # decorator.
        return self

    def resolved(self, id: int, run: str) -> DatasetRef:
        """Return a new `DatasetRef` with the same data ID and dataset type
        and the given ID and run.

        Parameters
        ----------
        id : `int`
            The unique integer identifier assigned when the dataset is created.
        run : `str`
            The run this dataset was associated with when it was created.

        Returns
        -------
        ref : `DatasetRef`
            A new `DatasetRef`.
        """
        return DatasetRef(datasetType=self.datasetType, dataId=self.dataId,
                          id=id, run=run, conform=False)

    def unresolved(self) -> DatasetRef:
        """Return a new `DatasetRef` with the same data ID and dataset type,
        but no ID or run.

        Returns
        -------
        ref : `DatasetRef`
            A new `DatasetRef`.

        Notes
        -----
        This can be used to compare only the data ID and dataset type of a
        pair of `DatasetRef` instances, regardless of whether either is
        resolved::

            if ref1.unresolved() == ref2.unresolved():
                ...
        """
        return DatasetRef(datasetType=self.datasetType, dataId=self.dataId, conform=False)

    def expanded(self, dataId: DataCoordinate) -> DatasetRef:
        """Return a new `DatasetRef` with the given expanded data ID.

        Parameters
        ----------
        dataId : `DataCoordinate`
            Data ID for the new `DatasetRef`.  Must compare equal to the
            original data ID.

        Returns
        -------
        ref : `DatasetRef`
            A new `DatasetRef` with the given data ID.
        """
        assert dataId == self.dataId
        return DatasetRef(datasetType=self.datasetType, dataId=dataId,
                          id=self.id, run=self.run,
                          conform=False)

    def isComponent(self) -> bool:
        """Boolean indicating whether this `DatasetRef` refers to a
        component of a composite.

        Returns
        -------
        isComponent : `bool`
            `True` if this `DatasetRef` is a component, `False` otherwise.
        """
        return self.datasetType.isComponent()

    def isComposite(self) -> bool:
        """Boolean indicating whether this `DatasetRef` is a composite type.

        Returns
        -------
        isComposite : `bool`
            `True` if this `DatasetRef` is a composite type, `False`
            otherwise.
        """
        return self.datasetType.isComposite()

    def _lookupNames(self) -> Tuple[LookupKey, ...]:
        """Name keys to use when looking up this DatasetRef in a configuration.

        The names are returned in order of priority.

        Returns
        -------
        names : `tuple` of `LookupKey`
            Tuple of the `DatasetType` name and the `StorageClass` name.
            If ``instrument`` is defined in the dataId, each of those names
            is added to the start of the tuple with a key derived from the
            value of ``instrument``.
        """
        # Special case the instrument Dimension since we allow configs
        # to include the instrument name in the hierarchy.
        names: Tuple[LookupKey, ...] = self.datasetType._lookupNames()

        # mypy doesn't think this could return True, because even though
        # __contains__ can take an object of any type, it seems hard-coded to
        # assume it will  return False if the type doesn't match the key type
        # of the Mapping.
        if "instrument" in self.dataId:  # type: ignore
            names = tuple(n.clone(dataId={"instrument": self.dataId["instrument"]})
                          for n in names) + names

        return names

    @staticmethod
    def groupByType(refs: Iterable[DatasetRef]) -> NamedKeyDict[DatasetType, List[DatasetRef]]:
        """Group an iterable of `DatasetRef` by `DatasetType`.

        Parameters
        ----------
        refs : `Iterable` [ `DatasetRef` ]
            `DatasetRef` instances to group.

        Returns
        -------
        grouped : `NamedKeyDict` [ `DatasetType`, `list` [ `DatasetRef` ] ]
            Grouped `DatasetRef` instances.
        """
        result: NamedKeyDict[DatasetType, List[DatasetRef]] = NamedKeyDict()
        for ref in refs:
            result.setdefault(ref.datasetType, []).append(ref)
        return result

    def getCheckedId(self) -> int:
        """Return ``self.id``, or raise if it is `None`.

        This trivial method exists to allow operations that would otherwise be
        natural list comprehensions to check that the ID is not `None` as well.

        Returns
        -------
        id : `int`
            ``self.id`` if it is not `None`.

        Raises
        ------
        AmbiguousDatasetError
            Raised if ``ref.id`` is `None`.
        """
        if self.id is None:
            raise AmbiguousDatasetError(f"ID for dataset {self} is `None`; "
                                        f"a resolved reference is required.")
        return self.id

    def makeComponentRef(self, name: str) -> DatasetRef:
        """Create a `DatasetRef` that corresponds to a component of this
        dataset.

        Parameters
        ----------
        name : `str`
            Name of the component.

        Returns
        -------
        ref : `DatasetRef`
            A `DatasetRef` with a dataset type that corresponds to the given
            component, and the same ID and run
            (which may be `None`, if they are `None` in ``self``).
        """
        return DatasetRef(self.datasetType.makeComponentDatasetType(name), self.dataId,
                          id=self.id, run=self.run)

    datasetType: DatasetType
    """The definition of this dataset (`DatasetType`).

    Cannot be changed after a `DatasetRef` is constructed.
    """

    dataId: DataCoordinate
    """A mapping of `Dimension` primary key values that labels the dataset
    within a Collection (`DataCoordinate`).

    Cannot be changed after a `DatasetRef` is constructed.
    """

    run: Optional[str]
    """The name of the run that produced the dataset.

    Cannot be changed after a `DatasetRef` is constructed; use `resolved` or
    `unresolved` to add or remove this information when creating a new
    `DatasetRef`.
    """

    id: Optional[int]
    """Primary key of the dataset (`int` or `None`).

    Cannot be changed after a `DatasetRef` is constructed; use `resolved` or
    `unresolved` to add or remove this information when creating a new
    `DatasetRef`.
    """

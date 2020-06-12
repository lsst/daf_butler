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
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
)

from ..dimensions import DataCoordinate, DimensionGraph, ExpandedDataCoordinate
from ..configSupport import LookupKey
from ..utils import immutable
from ..named import NamedKeyDict
from .type import DatasetType


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
    hasParentId : `bool`, optional
        If `True` this `DatasetRef` is a component that has the ``id``
        of the composite parent.

    Raises
    ------
    ValueError
        Raised if ``run`` is provided but ``id`` is not, or if ``id`` is
        provided but ``run`` is not.
    """

    __slots__ = ("id", "datasetType", "dataId", "run", "hasParentId")

    def __new__(cls, datasetType: DatasetType, dataId: DataCoordinate, *,
                id: Optional[int] = None,
                run: Optional[str] = None,
                hasParentId: bool = False,
                conform: bool = True) -> DatasetRef:
        self = super().__new__(cls)
        assert isinstance(datasetType, DatasetType)
        self.id = id
        self.datasetType = datasetType
        self.hasParentId = hasParentId
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
        return self

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
        s = f"{self.datasetType.name}@{self.dataId!s}"
        if self.id is not None:
            s += f" (id={self.id})"
        return s

    def __getnewargs_ex__(self) -> Tuple[Tuple[Any, ...], Dict[str, Any]]:
        return ((self.datasetType, self.dataId), {"id": self.id, "run": self.run})

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

    def expanded(self, dataId: ExpandedDataCoordinate) -> DatasetRef:
        """Return a new `DatasetRef` with the given expanded data ID.

        Parameters
        ----------
        dataId : `ExpandedDataCoordinate`
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
            component, with ``hasParentId=True``, and the same ID and run
            (which may be `None`, if they are `None` in ``self``).
        """
        return DatasetRef(self.datasetType.makeComponentDatasetType(name), self.dataId,
                          id=self.id, run=self.run, hasParentId=True)

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

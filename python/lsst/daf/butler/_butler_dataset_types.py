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

__all__ = ("ButlerDatasetTypes",)

from abc import ABC, abstractmethod
from collections.abc import Iterable, Sequence

from ._dataset_type import DatasetType
from ._storage_class import StorageClass
from .dimensions import DimensionGroup


class ButlerDatasetTypes(ABC, Sequence):
    """Methods for working with the dataset types known to the Butler."""

    @abstractmethod
    def get(self, name: str) -> DatasetType:
        """Return the dataset type with the given name.

        Parameters
        ----------
        name : `str`
            Name of the dataset type.

        Returns
        -------
        dataset_type : `DatasetType`
            Dataset type object with the given name.

        Raises
        ------
        MissingDatasetTypeError
            Raised if there is no dataset type with the given name.
        """
        raise NotImplementedError()

    @abstractmethod
    def query(
        self,
        name: str | Iterable[str],
        *,
        at_least_dimensions: Iterable[str] | DimensionGroup | None = None,
        exact_dimensions: Iterable[str] | DimensionGroup | None = None,
        storage_class: str | Iterable[str] | StorageClass | Iterable[StorageClass] | None = None,
        is_calibration: bool | None = None,
    ) -> Iterable[DatasetType]:
        """Query for dataset types matching the given criteria.

        Parameters
        ----------
        name : `str` or `~collections.abc.Iterable` [ `str` ]
            Names or name patterns (glob-style) that returned dataset type
            names must match.  If an iterable, items are OR'd together.
        at_least_dimensions : `Iterable` [ `str` ] or `DimensionGroup`,\
                optional
            Dimensions that returned dataset types must have as a subset.
        exact_dimensions : `Iterable` [ `str` ] or `DimensionGroup`,\
                optional
            Dimensions that returned dataset types must have exactly.
        storage_class : `str` or `~collections.abc.Iterable` [ `str` ],\
                or `StorageClass` or \
                `~collections.abc.Iterable` [ `StorageClass` ], optional
            Storage classes or storage class names that returned dataset types
            must have.  If an iterable, items are OR'd together.
        is_calibration : `bool` or `None`, optional
            If `None`, constrain returned dataset types to be or not be
            calibrations.

        Returns
        -------
        dataset_types : `~collections.abc.Iterable` [ `DatasetType` ]
            An iterable of dataset types.  This is guaranteed to be a regular
            Python in-memory container, not a lazy single-pass iterator, but
            the type of container is currently left unspecified in order to
            leave room for future convenience behavior.

        Notes
        -----
        This method queries all registered dataset types in registry.  To query
        for the types of datasets that are in a collection, instead use::

            info = butler.collections.query_info(
                collections,
                include_summaries=True,
            )

        for a simple summary of the dataset types in each collection (see
        `lsst.daf.butler.ButlerCollections.query_info`).  Or, for
        more complex but powerful queries (including constraints on data IDs or
        dataset counts), use::

            with butler.query() as q:
                dataset_types = q.dataset_types(collections)

        See `lsst.daf.butler.queries.Query.dataset_types` for details.
        """
        raise NotImplementedError()

    @abstractmethod
    def query_names(
        self,
        name: str | Iterable[str],
        *,
        at_least_dimensions: Iterable[str] | DimensionGroup | None = None,
        exact_dimensions: Iterable[str] | DimensionGroup | None = None,
        storage_class: str | Iterable[str] | StorageClass | Iterable[StorageClass] | None = None,
        is_calibration: bool | None = None,
    ) -> Iterable[str]:
        """Query for the names of dataset types matching the given criteria.

        Parameters
        ----------
        name : `str` or `~collections.abc.Iterable` [ `str` ]
            Names or name patterns (glob-style) that returned dataset type
            names must match.  If an iterable, items are OR'd together.
        at_least_dimensions : `Iterable` [ `str` ] or `DimensionGroup`,\
                optional
            Dimensions that returned dataset types must have as a subset.
        exact_dimensions : `Iterable` [ `str` ] or `DimensionGroup`,\
                optional
            Dimensions that returned dataset types must have exactly.
        storage_class : `str` or `~collections.abc.Iterable` [ `str` ],\
                or `StorageClass` or \
                `~collections.abc.Iterable` [ `StorageClass` ], optional
            Storage classes or storage class names that returned dataset types
            must have.  If an iterable, items are OR'd together.
        is_calibration : `bool` or `None`, optional
            If `None`, constrain returned dataset types to be or not be
            calibrations.

        Returns
        -------
        names : `~collections.abc.Iterable` [ `str` ]
            An iterable of dataset types.
        """
        raise NotImplementedError()

    @abstractmethod
    def register(
        self,
        name_or_type: str,
        /,
        dimensions: Iterable[str] | DimensionGroup | None = None,
        storage_class: str | StorageClass | None = None,
        is_calibration: bool | None = None,
    ) -> bool:
        """Register a dataset type.

        It is not an error to register the same `DatasetType` twice.

        Parameters
        ----------
        name_or_type : `str` or `DatasetType`
            The name of the dataset type to be added, or a complete
            `DatasetType` type object to add.
        dimensions : `~colletions.abc.Iterable` [ `str` ] or `DimensionGroup`,\
                optional
            Dimensions for the dataset type.  Required if the first argument
            is just a `str`, and overrides the dimensions if the first argument
            is a `DatasetType`.
        storage_class : `str` or `StorageClass`, optional
            Storage class for the dataset type.  Required if the first argument
            is just a `str`, and overrides the storage class if the first
            arguemnt is a `DatasetType`.
        is_calibration : `bool`, optional
            Whether the dataset type is a calibration.  If the first argument
            is a `str`, defaults to `False`.  If the first argument is a
            `DatasetType` and this argument is not `None`, it overrides the
            value on the `DatasetType`.

        Returns
        -------
        inserted : `bool`
            `True` if a new dataset type was inserted, `False` if an identical
            existing dataset type was found.  Note that in either case the
            dataset type is guaranteed to be defined in the repository
            consistently with the given definition.

        Raises
        ------
        ValueError
            Raised if the dimensions or storage class are invalid.
        lsst.daf.butler.registry.ConflictingDefinitionError
            Raised if this dataset type is already registered with a different
            definition.
        """
        raise NotImplementedError()

    @abstractmethod
    def remove(self, name: str) -> None:
        """Remove the dataset type with the given name.

        .. warning::

            Butler implementations can cache the dataset type definitions.
            This means that deleting the dataset type definition may result in
            unexpected behavior from other butler processes that are active
            that have not seen the deletion.

        Parameters
        ----------
        name : `str` or `tuple` [`str`]
            Name of the type to be removed or tuple containing a list of type
            names to be removed. Wildcards are allowed.

        Raises
        ------
        lsst.daf.butler.registry.OrphanedRecordError
            Raised if an attempt is made to remove the dataset type definition
            when there are still datasets associated with it.

        Notes
        -----
        If the dataset type is not registered the method will return without
        action.
        """
        raise NotImplementedError()

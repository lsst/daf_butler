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

__all__ = ("DatasetTypeCache",)

from collections.abc import Iterable, Iterator

from ...._dataset_type import DatasetType
from ....dimensions import DimensionGroup
from .tables import DynamicTables, TableCache


class DatasetTypeCache:
    """Cache for dataset types.

    Notes
    -----
    This cache is a pair of mappings with different kinds of keys:

    - Dataset type name -> (`DatasetType`, database integer primary key)
    - `DimensionGroup` -> database table information

    In some contexts (e.g. ``resolve_wildcard``) a full list of dataset types
    is needed. To signify that cache content can be used in such contexts,
    cache defines a special ``full`` flag that needs to be set by client.  The
    ``dimensions_full`` flag similarly reports whether all per-dimension-group
    state is present in the cache.
    """

    def __init__(self) -> None:
        self.tables = TableCache()
        self._by_name_cache: dict[str, tuple[DatasetType, int]] = {}
        self._by_dimensions_cache: dict[DimensionGroup, DynamicTables] = {}
        self._full = False
        self._dimensions_full = False

    def clone(self) -> DatasetTypeCache:
        """Make a copy of the caches that are safe to use in another thread.

        Notes
        -----
        After cloning, the ``tables`` cache will be shared between the new
        instance and the current instance. It is safe to read and update
        ``tables`` from multiple threads simultaneously -- the cached values
        are immutable table schemas, and they are looked up one at a time by
        name.

        The other caches are copied, because their access patterns are more
        complex.

        ``full`` and ``dimensions_full`` will initially return `False` in the
        new instance.  This preserves the invariant that a Butler is able to
        see any changes to the database made before the Butler is instantiated.
        The downside is that the cloned cache will have to be re-fetched before
        it can be used for glob searches.
        """
        clone = DatasetTypeCache()
        # Share DynamicTablesCache between instances.
        clone.tables = self.tables
        # The inner key/value objects are immutable in both of these caches, so
        # we can shallow-copy the dicts.
        clone._by_name_cache = self._by_name_cache.copy()
        clone._by_dimensions_cache = self._by_dimensions_cache.copy()
        return clone

    @property
    def full(self) -> bool:
        """`True` if cache holds all known dataset types (`bool`)."""
        return self._full

    @property
    def dimensions_full(self) -> bool:
        """`True` if cache holds all known dataset type dimensions (`bool`)."""
        return self._dimensions_full

    def add(self, dataset_type: DatasetType, id: int) -> None:
        """Add one record to the cache.

        Parameters
        ----------
        dataset_type : `DatasetType`
            Dataset type, replaces any existing dataset type with the same
            name.
        id : `int`
            The dataset type primary key.
            Additional opaque object stored with this dataset type.
        """
        self._by_name_cache[dataset_type.name] = (dataset_type, id)

    def set(
        self,
        data: Iterable[tuple[DatasetType, int]],
        *,
        full: bool = False,
        dimensions_data: Iterable[tuple[DimensionGroup, DynamicTables]] | None = None,
        dimensions_full: bool = False,
    ) -> None:
        """Replace cache contents with the new set of dataset types.

        Parameters
        ----------
        data : `~collections.abc.Iterable`
            Sequence of tuples of `DatasetType` and an extra opaque object.
        full : `bool`, optional
            If `True` then ``data`` contains all known dataset types.
        dimensions_data : `~collections.abc.Iterable`, optional
            Sequence of tuples of `DimensionGroup` and an extra opaque object.
        dimensions_full : `bool`, optional
            If `True` then ``data`` contains all known dataset type dimensions.
        """
        self.clear()
        for item in data:
            self._by_name_cache[item[0].name] = item
        self._full = full
        if dimensions_data is not None:
            self._by_dimensions_cache.update(dimensions_data)
            self._dimensions_full = dimensions_full

    def clear(self) -> None:
        """Remove everything from the cache."""
        self._by_name_cache = {}
        self._by_dimensions_cache = {}
        self._full = False
        self._dimensions_full = False

    def discard(self, name: str) -> None:
        """Remove named dataset type from the cache.

        Parameters
        ----------
        name : `str`
            Name of the dataset type to remove.
        """
        self._by_name_cache.pop(name, None)

    def get(self, name: str) -> tuple[DatasetType | None, int | None]:
        """Return cached info given dataset type name.

        Parameters
        ----------
        name : `str`
            Dataset type name.

        Returns
        -------
        dataset_type : `DatasetType` or `None`
            Cached dataset type, `None` is returned if the name is not in the
            cache.
        extra : `Any` or `None`
            Cached opaque data, `None` is returned if the name is not in the
            cache.
        """
        item = self._by_name_cache.get(name)
        if item is None:
            return (None, None)
        return item

    def get_dataset_type(self, name: str) -> DatasetType | None:
        """Return dataset type given its name.

        Parameters
        ----------
        name : `str`
            Dataset type name.

        Returns
        -------
        dataset_type : `DatasetType` or `None`
            Cached dataset type, `None` is returned if the name is not in the
            cache.
        """
        item = self._by_name_cache.get(name)
        if item is None:
            return None
        return item[0]

    def items(self) -> Iterator[tuple[DatasetType, int]]:
        """Return iterator for the set of items in the cache, can only be
        used if `full` is true.

        Returns
        -------
        iter : `~collections.abc.Iterator`
            Iterator over tuples of `DatasetType` and opaque data.

        Raises
        ------
        RuntimeError
            Raised if ``self.full`` is `False`.
        """
        if not self._full:
            raise RuntimeError("cannot call items() if cache is not full")
        return iter(self._by_name_cache.values())

    def add_by_dimensions(self, dimensions: DimensionGroup, tables: DynamicTables) -> None:
        """Add information about a set of dataset type dimensions to the cache.

        Parameters
        ----------
        dimensions : `DimensionGroup`
            Dimensions of one or more dataset types.
        tables : `DynamicTables`
            Additional opaque object stored with these dimensions.
        """
        self._by_dimensions_cache[dimensions] = tables

    def get_by_dimensions(self, dimensions: DimensionGroup) -> DynamicTables | None:
        """Get information about a set of dataset type dimensions.

        Parameters
        ----------
        dimensions : `DimensionGroup`
            Dimensions of one or more dataset types.

        Returns
        -------
        tables : `DynamicTables` or `None`
            Additional opaque object stored with these dimensions, or `None` if
            these dimensions are not present in the cache.
        """
        return self._by_dimensions_cache.get(dimensions)

    def by_dimensions_items(self) -> Iterator[tuple[DimensionGroup, DynamicTables]]:
        """Return iterator for all dimensions-keyed data in the cache.

        This can only be called if `dimensions_full` is `True`.

        Returns
        -------
        iter : `~collections.abc.Iterator`
            Iterator over tuples of `DimensionGroup` and opaque data.

        Raises
        ------
        RuntimeError
            Raised if ``self.dimensions_full`` is `False`.
        """
        if not self._dimensions_full:
            raise RuntimeError("cannot call by_dimensions_items() if cache does not have full dimensions.")
        return iter(self._by_dimensions_cache.items())

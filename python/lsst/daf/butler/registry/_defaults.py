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

__all__ = ("RegistryDefaults",)

import contextlib
from collections.abc import Sequence, Set
from types import EllipsisType
from typing import TYPE_CHECKING, Any

from lsst.utils.classes import immutable

from .._butler_instance_options import ButlerInstanceOptions
from .._exceptions import MissingCollectionError
from ..dimensions import DataCoordinate
from ._collection_summary import CollectionSummary
from .wildcards import CollectionWildcard

if TYPE_CHECKING:
    from ..registry import CollectionArgType, Registry
    from .sql_registry import SqlRegistry


@immutable
class RegistryDefaults:
    """A struct used to provide the default collections searched or written to
    by a `Registry` or `Butler` instance.

    Parameters
    ----------
    collections : `str` or `~collections.abc.Iterable` [ `str` ], optional
        An expression specifying the collections to be searched (in order) when
        reading datasets.  If a default value for a governor dimension is not
        given via ``**kwargs``, and exactly one value for that dimension
        appears in the datasets in ``collections``, that value is also used as
        the default for that dimension.
        This may be a `str` collection name or an iterable thereof.
        See :ref:`daf_butler_collection_expressions` for more information.
        These collections are not registered automatically and must be
        manually registered before they are used by any `Registry` or `Butler`
        method, but they may be manually registered after a `Registry` or
        `Butler` is initialized with this struct.
    run : `str`, optional
        Name of the `~CollectionType.RUN` collection new datasets should be
        inserted into.  If ``collections`` is `None` and ``run`` is not `None`,
        ``collections`` will be set to ``[run]``.  If not `None`, this
        collection will automatically be registered when the default struct is
        attached to a `Registry` instance.
    infer : `bool`, optional
        If `True` (default) infer default data ID values from the values
        present in the datasets in ``collections``: if all collections have the
        same value (or no value) for a governor dimension, that value will be
        the default for that dimension.  Nonexistent collections are ignored.
        If a default value is provided explicitly for a governor dimension via
        ``**kwargs``, no default will be inferred for that dimension.
    **kwargs : `str`
        Default data ID key-value pairs.  These may only identify "governor"
        dimensions like ``instrument`` and ``skymap``, though this is only
        checked when the defaults struct is actually attached to a `Registry`.
    """

    def __init__(self, collections: Any = None, run: str | None = None, infer: bool = True, **kwargs: str):
        self._original_collection_was_none = collections is None
        self._original_kwargs = dict(kwargs)
        if collections is None:
            if run is not None:
                collections = (run,)
            else:
                collections = ()
        self.collections = CollectionWildcard.from_expression(collections).require_ordered()
        self.run = run
        self._infer = infer
        self._kwargs = kwargs

    @staticmethod
    def from_data_id(data_id: DataCoordinate) -> RegistryDefaults:
        """Create a RegistryDefaults object with a specified ``dataId`` value
        and no default collections.

        Parameters
        ----------
        data_id : `DataCoordinate`
            The default data ID value.
        """
        defaults = RegistryDefaults(None, None, False)
        defaults.dataId = data_id
        defaults._finished = True
        return defaults

    @staticmethod
    def from_butler_instance_options(options: ButlerInstanceOptions) -> RegistryDefaults:
        """Create a `RegistryDefaults` object from the values specified by a
        `ButlerInstanceOptions` object.

        Parameters
        ----------
        options : `ButlerInstanceOptions`
            Butler options object.
        """
        return RegistryDefaults(
            collections=options.collections, run=options.run, infer=options.inferDefaults, **options.kwargs
        )

    def clone(
        self,
        collections: CollectionArgType | None | EllipsisType = ...,
        run: str | None | EllipsisType = ...,
        inferDefaults: bool | EllipsisType = ...,
        dataId: dict[str, str] | EllipsisType = ...,
    ) -> RegistryDefaults:
        """Make a copy of this RegistryDefaults object, optionally modifying
        values.

        Parameters
        ----------
        collections : `~lsst.daf.butler.registry.CollectionArgType` or `None`,\
            optional
            Same as constructor.  If omitted, uses value from original object.
        run : `str` or `None`, optional
            Same as constructor.  If `None`, no default run is used.  If
            omitted, copies value from original object.
        inferDefaults : `bool`, optional
            Same as constructor.  If omitted, copies value from original
            object.
        dataId : `dict` [ `str` , `str` ]
            Same as ``kwargs`` arguments to constructor.  If omitted, copies
            values from original object.

        Returns
        -------
        defaults : `RegistryDefaults`
            New instance if any changes were made, otherwise the original
            instance.

        Notes
        -----
        ``finish()`` must be called on the returned object to complete
        initialization.
        """
        if collections is ... and run is ... and inferDefaults is ... and dataId is ...:
            # Unmodified copy -- this object is immutable so we can just return
            # it and avoid the need for database queries in finish().
            return self

        if collections is ...:
            if self._original_collection_was_none:
                # Ensure that defaulting collections to the run collection
                # works the same as the constructor.
                collections = None
            else:
                collections = self.collections
        if run is ...:
            run = self.run
        if inferDefaults is ...:
            inferDefaults = self._infer
        if dataId is ...:
            dataId = self._original_kwargs

        return RegistryDefaults(collections, run, inferDefaults, **dataId)

    def __repr__(self) -> str:
        collections = f"collections={self.collections!r}" if self.collections else ""
        run = f"run={self.run!r}" if self.run else ""
        if self._kwargs:
            kwargs = ", ".join([f"{k}={v!r}" for k, v in self._kwargs.items()])
        else:
            kwargs = ""
        args = ", ".join([arg for arg in (collections, run, kwargs) if arg])
        return f"{type(self).__name__}({args})"

    def finish(self, registry: Registry | SqlRegistry) -> None:
        """Validate the defaults struct and standardize its data ID.

        This should be called only by a `Registry` instance when the defaults
        struct is first associated with it.

        Parameters
        ----------
        registry : `Registry`
            Registry instance these defaults are being attached to.

        Raises
        ------
        TypeError
            Raised if a non-governor dimension was included in ``**kwargs``
            at construction.
        """
        # Skip re-initialization if it's already been completed.
        # Can't just say 'self._finished' because this class is immutable.
        if hasattr(self, "_finished"):
            return

        allGovernorDimensions = registry.dimensions.governor_dimensions
        if not self._kwargs.keys() <= allGovernorDimensions.names:
            raise TypeError(
                "Only governor dimensions may be identified by a default data "
                f"ID, not {self._kwargs.keys() - allGovernorDimensions.names}.  "
                "(These may just be unrecognized keyword arguments passed at "
                "Butler construction.)"
            )
        if self._infer and self._kwargs.keys() != allGovernorDimensions.names:
            summaries = []
            for collection in self.collections:
                with contextlib.suppress(MissingCollectionError):
                    summaries.append(registry.getCollectionSummary(collection))

            if summaries:
                summary = CollectionSummary.union(*summaries)
                for dimensionName in allGovernorDimensions.names - self._kwargs.keys():
                    values: Set[str] = summary.governors.get(dimensionName, frozenset())
                    if len(values) == 1:
                        (value,) = values
                        self._kwargs[dimensionName] = value
        self.dataId = registry.expandDataId(self._kwargs, withDefaults=False)

        self._finished = True

    collections: Sequence[str]
    """The collections to search by default, in order
    (`~collections.abc.Sequence` [ `str` ]).
    """

    run: str | None
    """Name of the run this butler writes outputs to by default (`str` or
    `None`).
    """

    dataId: DataCoordinate
    """The default data ID (`DataCoordinate`).

    Dimensions without defaults are simply not included.  Only governor
    dimensions are ever included in defaults.

    This attribute may not be accessed before the defaults struct is
    attached to a `Registry` instance.  It always satisfies ``hasFull``.
    """

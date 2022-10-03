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

__all__ = ("RegistryDefaults",)

from collections.abc import Sequence
from typing import TYPE_CHECKING, AbstractSet, Any, Optional

from lsst.utils.classes import immutable

from ..core import DataCoordinate
from ._collection_summary import CollectionSummary
from ._exceptions import MissingCollectionError
from .wildcards import CollectionWildcard

if TYPE_CHECKING:
    from ._registry import Registry


@immutable
class RegistryDefaults:
    """A struct used to provide the default collections searched or written to
    by a `Registry` or `Butler` instance.

    Parameters
    ----------
    collections : `str` or `Iterable` [ `str` ], optional
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

    def __init__(self, collections: Any = None, run: Optional[str] = None, infer: bool = True, **kwargs: str):
        if collections is None:
            if run is not None:
                collections = (run,)
            else:
                collections = ()
        self.collections = CollectionWildcard.from_expression(collections).require_ordered()
        self.run = run
        self._infer = infer
        self._kwargs = kwargs

    def finish(self, registry: Registry) -> None:
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
        allGovernorDimensions = registry.dimensions.getGovernorDimensions()
        if not self._kwargs.keys() <= allGovernorDimensions.names:
            raise TypeError(
                "Only governor dimensions may be identified by a default data "
                f"ID, not {self._kwargs.keys() - allGovernorDimensions.names}.  "
                "(These may just be unrecognized keyword arguments passed at "
                "Butler construction.)"
            )
        if self._infer and not self._kwargs.keys() == allGovernorDimensions.names:
            summaries = []
            for collection in self.collections:
                try:
                    summaries.append(registry.getCollectionSummary(collection))
                except MissingCollectionError:
                    pass
            if summaries:
                summary = CollectionSummary.union(*summaries)
                for dimensionName in allGovernorDimensions.names - self._kwargs.keys():
                    values: AbstractSet[str] = summary.governors.get(dimensionName, frozenset())
                    if len(values) == 1:
                        (value,) = values
                        self._kwargs[dimensionName] = value
        self.dataId = registry.expandDataId(self._kwargs, withDefaults=False)

    collections: Sequence[str]
    """The collections to search by default, in order (`Sequence` [ `str` ]).
    """

    run: Optional[str]
    """Name of the run this butler writes outputs to by default (`str` or
    `None`).
    """

    dataId: DataCoordinate
    """The default data ID (`DataCoordinate`).

    Dimensions without defaults are simply not included.  Only governor
    dimensions are ever included in defaults.

    This attribute may not be accessed before the defaults struct is
    attached to a `Registry` instance.  It always satisfies both ``hasFull``
    and ``hasRecords``.
    """

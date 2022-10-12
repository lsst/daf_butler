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

__all__ = ("QueryBackend",)

from abc import ABC, abstractmethod
from collections.abc import Set
from typing import TYPE_CHECKING, Any

from .._collectionType import CollectionType
from .._exceptions import DatasetTypeError, MissingDatasetTypeError

if TYPE_CHECKING:
    from ...core import DatasetType, DimensionUniverse
    from ..interfaces import CollectionRecord
    from ..managers import RegistryManagerInstances


class QueryBackend(ABC):
    """An interface for constructing and evaluating the
    `~lsst.daf.relation.Relation` objects that comprise registry queries.

    This ABC is expected to have a concrete subclass for each concrete registry
    type.
    """

    @property
    @abstractmethod
    def managers(self) -> RegistryManagerInstances:
        """A struct containing the manager instances that back a SQL registry.

        Notes
        -----
        This property is a temporary interface that will be removed in favor of
        new methods once the manager and storage classes have been integrated
        with `~lsst.daf.relation.Relation`.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def universe(self) -> DimensionUniverse:
        """Definition of all dimensions and dimension elements for this
        registry.
        """
        raise NotImplementedError()

    @abstractmethod
    def resolve_collection_wildcard(
        self,
        expression: Any,
        *,
        collection_types: Set[CollectionType] = CollectionType.all(),
        done: set[str] | None = None,
        flatten_chains: bool = True,
        include_chains: bool | None = None,
    ) -> list[CollectionRecord]:
        """Return the collection records that match a wildcard expression.

        Parameters
        ----------
        expression
            Names and/or patterns for collections; will be passed to
            `CollectionWildcard.from_expression`.
        collection_types : `collections.abc.Set` [ `CollectionType` ], optional
            If provided, only yield collections of these types.
        done : `set` [ `str` ], optional
            A set of collection names that should be skipped, updated to
            include all processed collection names on return.
        flatten_chains : `bool`, optional
            If `True` (default) recursively yield the child collections of
            `~CollectionType.CHAINED` collections.
        include_chains : `bool`, optional
            If `False`, return records for `~CollectionType.CHAINED`
            collections themselves.  The default is the opposite of
            ``flattenChains``: either return records for CHAINED collections or
            their children, but not both.

        Returns
        -------
        records : `list` [ `CollectionRecord` ]
            Matching collection records.
        """
        raise NotImplementedError()

    @abstractmethod
    def resolve_dataset_type_wildcard(
        self,
        expression: Any,
        components: bool | None = None,
        missing: list[str] | None = None,
        explicit_only: bool = False,
    ) -> dict[DatasetType, list[str | None]]:
        """Return the dataset types that match a wildcard expression.

        Parameters
        ----------
        expression
            Names and/or patterns for dataset types; will be passed to
            `DatasetTypeWildcard.from_expression`.
        components : `bool`, optional
            If `True`, apply all expression patterns to component dataset type
            names as well.  If `False`, never apply patterns to components.  If
            `None` (default), apply patterns to components only if their parent
            datasets were not matched by the expression.  Fully-specified
            component datasets (`str` or `DatasetType` instances) are always
            included.
        missing : `list` of `str`, optional
            String dataset type names that were explicitly given (i.e. not
            regular expression patterns) but not found will be appended to this
            list, if it is provided.
        explicit_only : `bool`, optional
            If `True`, require explicit `DatasetType` instances or `str` names,
            with `re.Pattern` instances deprecated and ``...`` prohibited.

        Returns
        -------
        dataset_types : `dict` [ `DatasetType`, `list` [ `None`, `str` ] ]
            A mapping with resolved dataset types as keys and lists of
            matched component names as values, where `None` indicates the
            parent composite dataset type was matched.
        """
        raise NotImplementedError()

    def resolve_single_dataset_type_wildcard(
        self,
        expression: Any,
        components: bool | None = None,
        explicit_only: bool = False,
    ) -> tuple[DatasetType, list[str | None]]:
        """Return a single dataset type that matches a wildcard expression.

        Parameters
        ----------
        expression
            Names and/or patterns for the dataset type; will be passed to
            `DatasetTypeWildcard.from_expression`.
        components : `bool`, optional
            If `True`, apply all expression patterns to component dataset type
            names as well.  If `False`, never apply patterns to components.  If
            `None` (default), apply patterns to components only if their parent
            datasets were not matched by the expression.  Fully-specified
            component datasets (`str` or `DatasetType` instances) are always
            included.

            Values other than `False` are deprecated, and only `False` will be
            supported after v26.  After v27 this argument will be removed
            entirely.
        explicit_only : `bool`, optional
            If `True`, require explicit `DatasetType` instances or `str` names,
            with `re.Pattern` instances deprecated and ``...`` prohibited.

        Returns
        -------
        single_parent : `DatasetType`
            The matched parent dataset type.
        single_components : `list` [ `str` | `None` ]
            The matched components that correspond to this parent, or `None` if
            the parent dataset type itself was matched.

        Notes
        -----
        This method really finds a single parent dataset type and any number of
        components, because it's only the parent dataset type that's known to
        registry at all; many callers are expected to discard the
        ``single_components`` return value.
        """
        missing: list[str] = []
        matching = self.resolve_dataset_type_wildcard(
            expression, components=components, missing=missing, explicit_only=explicit_only
        )
        if not matching:
            if missing:
                raise MissingDatasetTypeError(
                    "\n".join(
                        f"Dataset type {t!r} is not registered, so no instances of it can exist."
                        for t in missing
                    )
                )
            else:
                raise MissingDatasetTypeError(
                    f"No registered dataset types matched expression {expression!r}, "
                    "so no datasets will be found."
                )
        if len(matching) > 1:
            raise DatasetTypeError(
                f"Expression {expression!r} matched multiple parent dataset types: "
                f"{[t.name for t in matching]}, but only one is allowed."
            )
        ((single_parent, single_components),) = matching.items()
        if missing:
            raise DatasetTypeError(
                f"Expression {expression!r} appears to involve multiple dataset types, even though only "
                f"one ({single_parent.name}) is registered, and only one is allowed here."
            )
        return single_parent, single_components

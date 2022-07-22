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
from collections.abc import Iterable, Mapping, Sequence, Set
from typing import TYPE_CHECKING

from lsst.daf.relation import Relation, sql
from lsst.utils.sets.unboundable import UnboundableSet

from ...core import ColumnTag, DataIdValue, DimensionGraph, DatasetType, DimensionUniverse, LogicalColumn
from .._collectionType import CollectionType

if TYPE_CHECKING:
    from ...core.named import NamedValueAbstractSet
    from ..interfaces import CollectionRecord
    from ..managers import RegistryManagerInstances
    from ..wildcards import CollectionSearch, CollectionWildcard
    from ._query_context import QueryContext


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

    def to_sql_select_parts(self, relation: Relation[ColumnTag]) -> sql.SelectParts[ColumnTag, LogicalColumn]:
        """Convert a relation into a decomposed SQLAlchemy SELECT query.

        Parameters
        ----------
        relation : `lsst.daf.relation.Relation`
            Relation to convert.

        Returns
        -------
        select_parts : `lsst.daf.relation.sql.SelectParts`
            Struct representing a simple SELECT query.

        Notes
        -----
        This property is a temporary interface that will be removed in favor of
        new methods once the manager and storage classes have been integrated
        with `~lsst.daf.relation.Relation`.
        """
        raise NotImplementedError()

    @abstractmethod
    def context(self) -> QueryContext:
        """Return a context manager that can be used to execute queries with
        this backend.

        Returns
        -------
        context : `QueryContext`
            Context manager that manages state and connections needed to
            execute queries.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def universe(self) -> DimensionUniverse:
        """Definition of all dimensions and dimension elements for this
        registry.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def parent_dataset_types(self) -> NamedValueAbstractSet[DatasetType]:
        """A set-like object (which may be lazily evaluated) containing all
        parent dataset types known to the registry.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def collection_records(self) -> NamedValueAbstractSet[CollectionRecord]:
        """A set-like object (which may be lazily evaluated) containing all
        collections known to the registry.
        """
        raise NotImplementedError()

    @abstractmethod
    def make_identity_relation(self) -> Relation[ColumnTag]:
        raise NotImplementedError()

    @abstractmethod
    def make_zero_relation(self, columns: Set[ColumnTag], doomed_by: Iterable[str]) -> Relation[ColumnTag]:
        raise NotImplementedError()

    @abstractmethod
    def resolve_dataset_collections(
        self,
        dataset_type: DatasetType,
        collections: CollectionSearch | CollectionWildcard,
        *,
        governors: Mapping[str, UnboundableSet[DataIdValue]] | None = None,
        rejections: list[str] | None = None,
        collection_types: Set[CollectionType] = CollectionType.all(),
        allow_calibration_collections: bool = False,
    ) -> list[CollectionRecord]:
        """Resolve the sequence of collections to query for a dataset type.

        Parameters
        ----------
        dataset_type : `DatasetType`
            Dataset type to be queried in the returned collections.
        collections : `CollectionSearch` or `CollectionWildcard`
            Expression for the collections to be queried.
        governors : `Mapping` [ `str`, `UnboundableSet` ], optional
            Constraints imposed by other aspects of the query on governor
            dimensions; collections inconsistent with these constraints will be
            skipped.
        rejections : `list` [ `str` ], optional
            If not `None`, a `list` that diagnostic messages will be appended
            to, for any collection that matches ``collections`` that is not
            returned.  At least one message is guaranteed whenever the result
            is empty.
        collection_types : `~collections.abc.Set` [ `CollectionType` ], \
                optional
            Collection types to consider when resolving the collection
            expression.
        allow_calibration_collections : `bool`, optional
            If `False`, skip (with a ``rejections`` message) any calibration
            collections that match ``collections`` are not given explicitly by
            name, and raise `NotImplementedError` for any calibration
            collection that is given explicitly.  This is a temporary option
            that will be removed when the query system can handle temporal
            joins involving calibration collections.

        Returns
        -------
        records : `list` [ `CollectionRecord` ]
            A new list of `CollectionRecord` instances, for collections that
            both match ``collections`` and may have datasets of the given type.
        """
        raise NotImplementedError()

    @abstractmethod
    def make_dataset_query_relation(
        self,
        dataset_type: DatasetType,
        collections: Sequence[CollectionRecord],
        columns: Set[str],
        *,
        governors: Mapping[str, UnboundableSet[DataIdValue]] | None = None,
    ) -> Relation[ColumnTag]:
        """Construct a relation that represents an unordered query for datasets
        that returns matching results from all given collections.

        Parameters
        ----------
        dataset_type : `DatasetType`
            Type for the datasets being queried.
        collections : `Sequence` [ `CollectionRecord` ]
            Records for collections to query.  Should generally be the result
            of a call to `resolve_dataset_collections`, and must not be empty.x
        columns : `~collections.abc.Set` [ `str` ]
            Columns to include in the `relation.  Valid columns are:

            - ``dataset_id``: the unique identifier of the dataset.  The type
              is implementation-dependent.  Never nullable.

            - ``ingest_date``: the date and time the dataset was added to the
              data repository.

            - ``run``: the foreign key column to the `~CollectionType.RUN`
              collection holding the dataset (not necessarily the collection
              name).  The type is dependent on the collection manager
              implementation.

            - ``timespan``: the validity range for datasets found in a
              `~CollectionType.CALIBRATION` collection, or ``NULL`` for other
              collection types.

        governors : `Mapping` [ `str`, `UnboundableSet` ], optional
            Constraints imposed by other aspects of the query on governor
            dimensions; collections inconsistent with these constraints will be
            skipped.

        Results
        -------
        relation : `sql.Relation`
            Relation representing a dataset query.
        """
        raise NotImplementedError()

    @abstractmethod
    def make_dataset_search_relation(
        self,
        dataset_type: DatasetType,
        collections: Sequence[CollectionRecord],
        columns: Set[str],
        *,
        join_to: Relation[ColumnTag] | None = None,
        governors: Mapping[str, UnboundableSet[DataIdValue]] | None = None,
    ) -> Relation[ColumnTag]:
        """Construct a relation that represents an order query for datasets
        that returns results from the first matching collection for each
        data ID.

        Parameters
        ----------
        dataset_type : `DatasetType`
            Type for the datasets being search.
        collections : `Sequence` [ `CollectionRecord` ]
            Records for collections to search.  Should generally be the result
            of a call to `resolve_dataset_collections`, and must not be empty.
        columns : `~collections.abc.Set` [ `str` ]
            Columns to include in the `relation.  See
            `make_dataset_query_relation` for options.
        join_to : `Relation`, optional
            Another relation to join with the query for datasets in all
            collections before filtering out out shadowed datasets.
        governors : `Mapping` [ `str`, `UnboundableSet` ], optional
            Constraints imposed by other aspects of the query on governor
            dimensions; collections inconsistent with these constraints will be
            skipped.

        Results
        -------
        relation : `sql.Relation`
            Relation representing a find-first dataset search.
        """
        raise NotImplementedError()

    @abstractmethod
    def make_doomed_dataset_relation(
        self,
        dataset_type: DatasetType,
        columns: Set[str],
        messages: Iterable[str],
    ) -> Relation[ColumnTag]:
        """Construct a relation that represents a doomed query for datasets.

        Parameters
        ----------
        dataset_type : `DatasetType`
            Dataset type being queried.
        columns : `AbstractSet` [ `str` ]
            Dataset columns to include (dimension key columns are always
            included).  See `make_dataset_query_relation` for allowed values.
        messages : `Iterable` [ `str` ]
            Diagnostic messages that explain why the query is doomed to yield
            no rows.

        Results
        -------
        relation : `sql.Relation`
            Relation with the requested columns and no rows.
        """
        raise NotImplementedError()

    @abstractmethod
    def make_dimension_relation(
        self,
        dimensions: DimensionGraph,
        columns: Mapping[str, Set[str]],
        *,
        initial_relation: Relation[ColumnTag] | None = None,
        initial_key_relationships: Set[frozenset[str]] = frozenset(),
        spatial_joins: Iterable[tuple[str, str]] = (),
        governors: Mapping[str, UnboundableSet[DataIdValue]] | None = None,
    ) -> Relation[ColumnTag]:
        raise NotImplementedError()

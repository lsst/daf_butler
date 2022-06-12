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

from abc import abstractmethod
from typing import TYPE_CHECKING, AbstractSet, Generic, Iterable, Mapping, Sequence, TypeVar

from ..core import DatasetType, DimensionGraph, DimensionUniverse, sql
from ._collectionType import CollectionType

if TYPE_CHECKING:
    from ..core.named import NamedValueAbstractSet
    from .interfaces import CollectionRecord
    from .managers import RegistryManagerInstances
    from .wildcards import CollectionSearch, CollectionWildcard


_C = TypeVar("_C", bound=sql.QueryContext, covariant=True)


class QueryBackend(Generic[_C]):
    """An interface for `sql.Relation` and `sql.QueryContext` creation to be
    specialized for different `Registry` implementations.
    """

    @property
    @abstractmethod
    def managers(self) -> RegistryManagerInstances:
        """A struct containing the manager instances that back a SQL registry.

        Notes
        -----
        This property is a temporary interface that will be removed in favor of
        new methods once the manager and storage classes have been integrated
        with `sql.Relation`.
        """
        raise NotImplementedError()

    @abstractmethod
    def make_context(self) -> _C:
        """Return a context manager for relation query execution and other
        operations that involve an active database connection.

        Return
        ------
        context : `QueryContext`
            Context manager.

        Notes
        -----
        A ``with`` block must still be used to actually begin the context and
        clean up after it, so the usual pattern is::

            with backend.make_context() as context:
                # use context

        For backwards compatibility with older query interfaces that did not
        use a context manager, an un-entered `QueryContext` may still sometimes
        be passed to relation-creation methods, but this will result in
        exceptions for relations that require temporary tables.
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

    @property
    @abstractmethod
    def unit_relation(self) -> sql.Relation:
        """A special `Relation` that has no columns but one (conceptual)
        row.

        Joining this relation to any other relation returns the other relation.
        """
        raise NotImplementedError()

    @abstractmethod
    def make_doomed_relation(self, *messages: str, columns: AbstractSet[sql.ColumnTag]) -> sql.Relation:
        """Return a `Relation` object that has no rows, with diagnostic
        messages explaining why.

        Parameters
        ----------
        *messages : `str`
            Diagnostics messages explaining the absence of rows.
        columns : `AbstractSet` [ `ColumnTag` ]
            Columns the relation has.  In some contexts this may not matter,
            since doomed queries should never be executed, but it is necessary
            to allow this special relation to behave like any other.
        """
        raise NotImplementedError()

    @abstractmethod
    def resolve_dataset_collections(
        self,
        dataset_type: DatasetType,
        collections: CollectionSearch | CollectionWildcard,
        *,
        constraints: sql.LocalConstraints | None = None,
        rejections: list[str] | None = None,
        collection_types: AbstractSet[CollectionType] = CollectionType.all(),
        allow_calibration_collections: bool = False,
    ) -> list[CollectionRecord]:
        """Resolve the sequence of collections to query for a dataset type.

        Parameters
        ----------
        dataset_type : `DatasetType`
            Dataset type to be queried in the returned collections.
        collections : `CollectionSearch` or `CollectionWildcard`
            Expression for the collections to be queried.
        constraints : `sql.LocalConstraints`, optional
            Constraints imposed by other aspects of the query (e.g. a WHERE
            clause); collections inconsistent with these constraints will be
            skipped.
        rejections : `list` [ `str` ], optional
            If not `None`, a `list` that diagnostic messages will be appended
            to, for any collection that matches ``collections`` that is not
            returned.  At least one message is guaranteed whenever the result
            is empty.
        collection_types : `AbstractSet` [ `CollectionType` ], optional
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
        columns: AbstractSet[str],
        *,
        join_relation: sql.Relation | None = None,
        constraints: sql.LocalConstraints | None = None,
    ) -> sql.Relation:
        """Construct a relation that represents an unordered query for datasets
        that returns matching results from all given collections.

        Parameters
        ----------
        dataset_type : `DatasetType`
            Type for the datasets being queried.
        collections : `Sequence` [ `CollectionRecord` ]
            Records for collections to query.  Should generally be the result
            of a call to `resolve_dataset_collections`, and must not be empty.x
        columns : `AbstractSet` [ `str` ]
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

        join_relation : `sql.Relation`, optional
            Another relation to join into the result.
        constraints : `sql.LocalConstraints`, optional
            Constraints imposed by other aspects of the query (e.g. a WHERE
            clause); collections inconsistent with these constraints will be
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
        columns: AbstractSet[str],
        *,
        join_relation: sql.Relation | None = None,
        constraints: sql.LocalConstraints | None = None,
    ) -> sql.Relation:
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
        columns : `AbstractSet` [ `str` ]
            Columns to include in the `relation.  See
            `make_dataset_query_relation` for options.
        join_relation : `sql.Relation`, optional
            Another relation to join into the result.
        constraints : `sql.LocalConstraints`, optional
            Constraints imposed by other aspects of the query (e.g. a WHERE
            clause); collections inconsistent with these constraints will be
            skipped.

        Results
        -------
        relation : `sql.Relation`
            Relation representing a find-first dataset search.
        """
        raise NotImplementedError()

    def make_doomed_dataset_relation(
        self,
        dataset_type: DatasetType,
        columns: AbstractSet[str],
        messages: Iterable[str],
        *,
        join_relation: sql.Relation | None = None,
    ) -> sql.Relation:
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
        join_relation : `sql.Relation`, optional
            Another relation to join into the result.

        Results
        -------
        relation : `sql.Relation`
            Relation with the requested columns and no rows.
        """
        column_tags: set[sql.ColumnTag] = {
            sql.DimensionKeyColumnTag(dimension_name)
            for dimension_name in dataset_type.dimensions.required.names
        }
        column_tags.update(sql.DatasetColumnTag.generate(dataset_type.name, columns))
        dataset_relation = self.make_doomed_relation(*messages, columns=column_tags)
        if join_relation is None:
            return dataset_relation
        else:
            return join_relation.join(dataset_relation)

    @abstractmethod
    def make_dimension_relation(
        self,
        dimensions: DimensionGraph,
        sql_columns: Mapping[str, AbstractSet[str]],
        result_columns: Mapping[str, AbstractSet[str]],
        result_records: AbstractSet[str] = frozenset(),
        *,
        spatial_joins: Iterable[tuple[str, str]] = (),
        join_relation: sql.Relation | None = None,
        constraints: sql.LocalConstraints | None = None,
    ) -> sql.Relation:
        raise NotImplementedError()

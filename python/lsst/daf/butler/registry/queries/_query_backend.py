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
from collections.abc import Iterable, Mapping, Sequence, Set
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from lsst.daf.relation import ColumnTag, LeafRelation, Relation

from ...core import DatasetColumnTag, DatasetType, DimensionKeyColumnTag, DimensionUniverse
from .._collectionType import CollectionType
from .._exceptions import DatasetTypeError, MissingDatasetTypeError
from ..wildcards import CollectionWildcard
from ._query_context import QueryContext
from .find_first_dataset import FindFirstDataset

if TYPE_CHECKING:
    from ..interfaces import CollectionRecord
    from ..managers import RegistryManagerInstances


_C = TypeVar("_C", bound=QueryContext)


class QueryBackend(Generic[_C]):
    """An interface for constructing and evaluating the
    `~lsst.daf.relation.Relation` objects that comprise registry queries.

    This ABC is expected to have a concrete subclass for each concrete registry
    type, and most subclasses will be paired with a `QueryContext` subclass.
    See `QueryContext` for the division of responsibilities between these two
    interfaces.
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
        registry (`DimensionUniverse`).
        """
        raise NotImplementedError()

    @abstractmethod
    def context(self) -> _C:
        """Return a context manager that can be used to execute queries with
        this backend.

        Returns
        -------
        context : `QueryContext`
            Context manager that manages state and connections needed to
            execute queries.
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
        components_deprecated: bool = True,
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
        components_deprecated : `bool`, optional
            If `True`, this is a context in which component dataset support is
            deprecated.  This will result in a deprecation warning when
            ``components=True`` or ``components=None`` and a component dataset
            is matched.  In the future this will become an error.

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
        components_deprecated: bool = True,
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
        explicit_only : `bool`, optional
            If `True`, require explicit `DatasetType` instances or `str` names,
            with `re.Pattern` instances deprecated and ``...`` prohibited.
        components_deprecated : `bool`, optional
            If `True`, this is a context in which component dataset support is
            deprecated.  This will result in a deprecation warning when
            ``components=True`` or ``components=None`` and a component dataset
            is matched.  In the future this will become an error.

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
            expression,
            components=components,
            missing=missing,
            explicit_only=explicit_only,
            components_deprecated=components_deprecated,
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

    @abstractmethod
    def filter_dataset_collections(
        self,
        dataset_types: Iterable[DatasetType],
        collections: Sequence[CollectionRecord],
        *,
        governor_constraints: Mapping[str, Set[str]],
        rejections: list[str] | None = None,
    ) -> dict[DatasetType, list[CollectionRecord]]:
        """Filter a sequence of collections to those for which a dataset query
        might succeed.

        Parameters
        ----------
        dataset_types : `Iterable` [ `DatasetType` ]
            Dataset types that are being queried.  Must include only parent
            or standalone dataset types, not components.
        collections : `Sequence` [ `CollectionRecord` ]
            Sequence of collections that will be searched.
        governor_constraints : `Mapping` [ `str`, `~collections.abc.Set` ], \
                optional
            Constraints imposed by other aspects of the query on governor
            dimensions; collections inconsistent with these constraints will be
            skipped.
        rejections : `list` [ `str` ], optional
            If not `None`, a `list` that diagnostic messages will be appended
            to, for any collection that matches ``collections`` that is not
            returned.  At least one message is guaranteed whenever the result
            is empty.

        Returns
        -------
        dataset_collections : `dict` [ `DatasetType`, \
                `list` [ `CollectionRecord` ] ]
            The collections to search for each dataset.  The dictionary's keys
            are always exactly ``dataset_types`` (in the same order), and each
            nested `list` of collections is ordered consistently with the
            given ``collections``.

        Notes
        -----
        This method accepts multiple dataset types and multiple collections at
        once to enable implementations to batch up the fetching of summary
        information needed to relate them.
        """
        raise NotImplementedError()

    def resolve_dataset_collections(
        self,
        dataset_type: DatasetType,
        collections: CollectionWildcard,
        *,
        governor_constraints: Mapping[str, Set[str]],
        rejections: list[str] | None = None,
        collection_types: Set[CollectionType] = CollectionType.all(),
        allow_calibration_collections: bool = False,
    ) -> list[CollectionRecord]:
        """Resolve the sequence of collections to query for a dataset type.

        Parameters
        ----------
        dataset_type : `DatasetType`
            Dataset type to be queried in the returned collections.
        collections : `CollectionWildcard`
            Expression for the collections to be queried.
        governor_constraints : `Mapping` [ `str`, `~collections.abc.Set` ], \
                optional
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

        Notes
        -----
        This is a higher-level driver for `resolve_collection_wildcard` and
        `filter_dataset_collections` that is mostly concerned with handling
        queries against `~Collection.Type.CALIBRATION` collections that aren't
        fully supported yet.  Once that support improves, this method may be
        removed.
        """
        if collections == CollectionWildcard() and collection_types == CollectionType.all():
            collection_types = {CollectionType.RUN}
        explicit_collections = frozenset(collections.strings)
        matching_collection_records = self.resolve_collection_wildcard(
            collections, collection_types=collection_types
        )
        ((_, filtered_collection_records),) = self.filter_dataset_collections(
            [dataset_type],
            matching_collection_records,
            governor_constraints=governor_constraints,
            rejections=rejections,
        ).items()
        if not allow_calibration_collections:
            supported_collection_records: list[CollectionRecord] = []
            for record in filtered_collection_records:
                if record.type is CollectionType.CALIBRATION:
                    # If collection name was provided explicitly then raise,
                    # since this is a kind of query we don't support yet;
                    # otherwise collection is a part of a chained one or regex
                    # match, and we skip it to not break queries of other
                    # included collections.
                    if record.name in explicit_collections:
                        raise NotImplementedError(
                            f"Query for dataset type {dataset_type.name!r} in CALIBRATION-type "
                            f"collection {record.name!r} is not yet supported."
                        )
                    else:
                        if rejections is not None:
                            rejections.append(
                                f"Not searching for dataset {dataset_type.name!r} in CALIBRATION "
                                f"collection {record.name!r} because calibration queries aren't fully "
                                "implemented; this is not an error only because the query structure "
                                "implies that searching this collection may be incidental."
                            )
                            supported_collection_records.append(record)
                else:
                    supported_collection_records.append(record)
        else:
            supported_collection_records = filtered_collection_records
        if not supported_collection_records and rejections is not None and not rejections:
            rejections.append(f"No collections to search matching expression {collections!r}.")
        return supported_collection_records

    @abstractmethod
    def make_dataset_query_relation(
        self,
        dataset_type: DatasetType,
        collections: Sequence[CollectionRecord],
        columns: Set[str],
        context: _C,
    ) -> Relation:
        """Construct a relation that represents an unordered query for datasets
        that returns matching results from all given collections.

        Parameters
        ----------
        dataset_type : `DatasetType`
            Type for the datasets being queried.
        collections : `Sequence` [ `CollectionRecord` ]
            Records for collections to query.  Should generally be the result
            of a call to `resolve_dataset_collections`, and must not be empty.
        context : `QueryContext`
            Context that manages per-query state.
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

        Results
        -------
        relation : `lsst.daf.relation.Relation`
            Relation representing a dataset query.
        """
        raise NotImplementedError()

    def make_dataset_search_relation(
        self,
        dataset_type: DatasetType,
        collections: Sequence[CollectionRecord],
        columns: Set[str],
        context: _C,
        *,
        join_to: Relation | None = None,
    ) -> Relation:
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
        context : `QueryContext`
            Context that manages per-query state.
        join_to : `Relation`, optional
            Another relation to join with the query for datasets in all
            collections before filtering out out shadowed datasets.

        Results
        -------
        relation : `lsst.daf.relation.Relation`
            Relation representing a find-first dataset search.
        """
        base = self.make_dataset_query_relation(
            dataset_type,
            collections,
            columns | {"rank"},
            context=context,
        )
        if join_to is not None:
            base = join_to.join(base)
        # Query-simplification shortcut: if there is only one collection, a
        # find-first search is just a regular result subquery.  Same if there
        # are no collections.
        if len(collections) <= 1:
            return base
        # We filter the dimension keys in the given relation through
        # DimensionGraph.required.names to minimize the set we partition on
        # and order it in a more index-friendly way.  More precisely, any
        # index we define on dimensions will be consistent with this order, but
        # any particular index may not have the same dimension columns.
        dimensions = self.universe.extract(
            [tag.dimension for tag in DimensionKeyColumnTag.filter_from(base.columns)]
        )
        find_first = FindFirstDataset(
            dimensions=DimensionKeyColumnTag.generate(dimensions.required.names),
            rank=DatasetColumnTag(dataset_type.name, "rank"),
        )
        return find_first.apply(base).with_only_columns(base.columns - {find_first.rank})

    def make_doomed_dataset_relation(
        self,
        dataset_type: DatasetType,
        columns: Set[str],
        messages: Iterable[str],
        context: _C,
    ) -> Relation:
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
        context : `QueryContext`, optional
            Context that manages per-query state.

        Results
        -------
        relation : `lsst.daf.relation.Relation`
            Relation with the requested columns and no rows.
        """
        column_tags: set[ColumnTag] = set(
            DimensionKeyColumnTag.generate(dataset_type.dimensions.required.names)
        )
        column_tags.update(DatasetColumnTag.generate(dataset_type.name, columns))
        return LeafRelation.make_doomed(
            context.preferred_engine, columns=column_tags, messages=list(messages)
        )

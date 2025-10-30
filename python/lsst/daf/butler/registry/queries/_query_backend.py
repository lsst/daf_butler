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

__all__ = ("QueryBackend",)

from abc import abstractmethod
from collections.abc import Iterable, Mapping, Sequence, Set
from contextlib import AbstractContextManager
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from lsst.daf.relation import (
    BinaryOperationRelation,
    ColumnTag,
    LeafRelation,
    MarkerRelation,
    Relation,
    UnaryOperationRelation,
)

from ..._collection_type import CollectionType
from ..._column_tags import DatasetColumnTag, DimensionKeyColumnTag
from ..._dataset_type import DatasetType
from ..._exceptions import MissingDatasetTypeError
from ..._exceptions_legacy import DatasetTypeError
from ...dimensions import DimensionGroup, DimensionRecordSet, DimensionUniverse
from ..wildcards import CollectionWildcard
from ._query_context import QueryContext

if TYPE_CHECKING:
    from ..interfaces import CollectionRecord


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
    def universe(self) -> DimensionUniverse:
        """Definition of all dimensions and dimension elements for this
        registry (`DimensionUniverse`).
        """
        raise NotImplementedError()

    @abstractmethod
    def caching_context(self) -> AbstractContextManager[None]:
        """Enable caching of collection records and summaries for the duration
        of the returned context manager.
        """
        raise NotImplementedError()

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
    def get_collection_name(self, key: Any) -> str:
        """Return the collection name associated with a collection primary key
        value.

        Parameters
        ----------
        key : `~typing.Any`
            Collection primary key value.

        Returns
        -------
        name : `str`
            Collection name.
        """
        raise NotImplementedError()

    @abstractmethod
    def resolve_collection_wildcard(
        self,
        expression: Any,
        *,
        collection_types: Set[CollectionType] = CollectionType.all(),
        flatten_chains: bool = True,
        include_chains: bool | None = None,
    ) -> list[CollectionRecord]:
        """Return the collection records that match a wildcard expression.

        Parameters
        ----------
        expression : `~typing.Any`
            Names and/or patterns for collections; will be passed to
            `CollectionWildcard.from_expression`.
        collection_types : `collections.abc.Set` [ `CollectionType` ], optional
            If provided, only yield collections of these types.
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
        missing: list[str] | None = None,
        explicit_only: bool = False,
    ) -> list[DatasetType]:
        """Return the dataset types that match a wildcard expression.

        Parameters
        ----------
        expression : `~typing.Any`
            Names and/or patterns for dataset types; will be passed to
            `DatasetTypeWildcard.from_expression`.
        missing : `list` of `str`, optional
            String dataset type names that were explicitly given (i.e. not
            regular expression patterns) but not found will be appended to this
            list, if it is provided.
        explicit_only : `bool`, optional
            If `True`, require explicit `DatasetType` instances or `str` names,
            with `re.Pattern` instances deprecated and ``...`` prohibited.

        Returns
        -------
        dataset_types : `list` [ `DatasetType` ]
            A list of resolved dataset types.
        """
        raise NotImplementedError()

    def resolve_single_dataset_type_wildcard(
        self,
        expression: Any,
        explicit_only: bool = False,
    ) -> DatasetType:
        """Return a single dataset type that matches a wildcard expression.

        Parameters
        ----------
        expression : `~typing.Any`
            Names and/or patterns for the dataset type; will be passed to
            `DatasetTypeWildcard.from_expression`.
        explicit_only : `bool`, optional
            If `True`, require explicit `DatasetType` instances or `str` names,
            with `re.Pattern` instances deprecated and ``...`` prohibited.

        Returns
        -------
        single : `DatasetType`
            The matched dataset type.
        """
        missing: list[str] = []
        matching = self.resolve_dataset_type_wildcard(
            expression, missing=missing, explicit_only=explicit_only
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
        (single_parent,) = matching
        if missing:
            raise DatasetTypeError(
                f"Expression {expression!r} appears to involve multiple dataset types, even though only "
                f"one ({single_parent.name}) is registered, and only one is allowed here."
            )
        return single_parent

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
        dataset_types : `~collections.abc.Iterable` [ `DatasetType` ]
            Dataset types that are being queried.  Must include only parent
            or standalone dataset types, not components.
        collections : `~collections.abc.Sequence` [ `CollectionRecord` ]
            Sequence of collections that will be searched.
        governor_constraints : `~collections.abc.Mapping` [ `str`, \
                `~collections.abc.Set` [ `str` ] ], optional
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
        governor_constraints : `~collections.abc.Mapping` [ `str`, \
                `~collections.abc.Set` ], optional
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
        columns : `~collections.abc.Set` [ `str` ]
            Dataset columns to include (dimension key columns are always
            included).  See `make_dataset_query_relation` for allowed values.
        messages : `~collections.abc.Iterable` [ `str` ]
            Diagnostic messages that explain why the query is doomed to yield
            no rows.
        context : `QueryContext`
            Context that manages per-query state.

        Returns
        -------
        relation : `lsst.daf.relation.Relation`
            Relation with the requested columns and no rows.
        """
        column_tags: set[ColumnTag] = set(DimensionKeyColumnTag.generate(dataset_type.dimensions.required))
        column_tags.update(DatasetColumnTag.generate(dataset_type.name, columns))
        return context.preferred_engine.make_doomed_relation(columns=column_tags, messages=list(messages))

    @abstractmethod
    def resolve_governor_constraints(
        self, dimensions: DimensionGroup, constraints: Mapping[str, Set[str]]
    ) -> Mapping[str, Set[str]]:
        """Resolve governor dimension constraints provided by user input to
        a query against the content in the `Registry`.

        Parameters
        ----------
        dimensions : `DimensionGroup`
            Dimensions that bound the governor dimensions to consider (via
            ``dimensions.governors``, more specifically).
        constraints : `~collections.abc.Mapping` [ `str`, \
                `~collections.abc.Set` [ `str` ] ]
            Constraints from user input to the query (e.g. from data IDs and
            string expression predicates).

        Returns
        -------
        resolved : `~collections.abc.Mapping` [ `str`, \
                `~collections.abc.Set` [ `str` ] ]
            A shallow copy of ``constraints`` with keys equal to
            ``dimensions.governors.names`` and value sets constrained by the
            Registry content if they were not already in ``constraints``.

        Raises
        ------
        DataIdValueError
            Raised if ``constraints`` includes governor dimension values that
            are not present in the `Registry`.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_dimension_record_cache(self, element_name: str) -> DimensionRecordSet | None:
        """Return a local cache of all `DimensionRecord` objects for a
        dimension element, fetching it if necessary.

        Parameters
        ----------
        element_name : `str`
            Name of the dimension element.

        Returns
        -------
        cache : `~collections.abc.Mapping` [ `DataCoordinate`, \
                `DimensionRecord` ] or `None`
            Mapping from data ID to dimension record, or `None` if this
            element's records are never cached.
        """
        raise NotImplementedError()

    def extract_dimension_relationships(self, relation: Relation) -> set[frozenset[str]]:
        """Extract the dimension key relationships encoded in a relation tree.

        Parameters
        ----------
        relation : `Relation`
            Relation tree to process.

        Returns
        -------
        relationships : `set` [ `frozenset` [ `str` ] ]
            Set of sets of dimension names, where each inner set represents a
            relationship between dimensions.

        Notes
        -----
        Dimension relationships include both many-to-one implied dependencies
        and many-to-many joins backed by "always-join" dimension elements, and
        it's important to join in the dimension table that defines a
        relationship in any query involving dimensions that are a superset of
        that relationship.  For example, let's consider a relation tree that
        joins dataset existence-check relations for two dataset types, with
        dimensions ``{instrument, exposure, detector}`` and ``{instrument,
        physical_filter}``.  The joined relation appears to have all dimension
        keys in its expanded graph present except ``band``, and the system
        could easily correct this by joining that dimension in directly.  But
        it's also missing the ``{instrument, exposure, physical_filter}``
        relationship we'd get from the ``exposure`` dimension's own relation
        (``exposure`` implies ``physical_filter``) and the similar
        ``{instrument, physical_filter, band}`` relationship from the
        ``physical_filter`` dimension relation; we need the relationship logic
        to recognize that those dimensions need to be joined in as well in
        order for the full relation to have rows that represent valid data IDs.

        The implementation of this method relies on the assumption that
        `LeafRelation` objects always have rows that are consistent with all
        defined relationships (i.e. are valid data IDs).  This is true for not
        just dimension relations themselves, but anything created from queries
        based on them, including datasets and query results.  It is possible to
        construct `LeafRelation` objects that don't satisfy this criteria (e.g.
        when accepting in user-provided data IDs), and in this case
        higher-level guards or warnings must be provided.``
        """
        return {
            frozenset(
                tag.dimension
                for tag in DimensionKeyColumnTag.filter_from(leaf_relation.columns & relation.columns)
            )
            for leaf_relation in self._extract_leaf_relations(relation).values()
        }

    def _extract_leaf_relations(self, relation: Relation) -> dict[str, LeafRelation]:
        """Recursively extract leaf relations from a relation tree.

        Parameters
        ----------
        relation : `Relation`
            Tree to process.

        Returns
        -------
        leaves : `dict` [ `str`, `LeafRelation` ]
            Leaf relations, keyed and deduplicated by name.
        """
        match relation:
            case LeafRelation() as leaf:
                return {leaf.name: leaf}
            case UnaryOperationRelation(target=target):
                return self._extract_leaf_relations(target)
            case BinaryOperationRelation(lhs=lhs, rhs=rhs):
                return self._extract_leaf_relations(lhs) | self._extract_leaf_relations(rhs)
            case MarkerRelation(target=target):
                return self._extract_leaf_relations(target)
        raise AssertionError("Match should be exhaustive and all branches should return.")

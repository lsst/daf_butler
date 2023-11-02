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

__all__ = ()

import itertools
from collections.abc import Iterable, Iterator, Mapping, Sequence, Set
from contextlib import contextmanager
from typing import Any, cast, final

from lsst.daf.relation import ColumnError, ColumnTag, Diagnostics, Relation, Sort, SortTerm

from ..._column_tags import DatasetColumnTag, DimensionKeyColumnTag, DimensionRecordColumnTag
from ..._dataset_ref import DatasetRef
from ..._dataset_type import DatasetType
from ...dimensions import DataCoordinate, DimensionElement, DimensionGroup, DimensionRecord
from .._collection_type import CollectionType
from ..wildcards import CollectionWildcard
from ._query_backend import QueryBackend
from ._query_context import QueryContext
from ._readers import DataCoordinateReader, DatasetRefReader, DimensionRecordReader


@final
class Query:
    """A general-purpose representation of a registry query.

    Parameters
    ----------
    dimensions : `DimensionGroup`
        The dimensions that span the query and are used to join its relations
        together.
    backend : `QueryBackend`
        Backend object used to create the query and new ones derived from it.
    context : `QueryContext`
        Context manager that holds relation engines and database connections
        for the query.
    relation : `Relation`
        The relation tree representation of the query as a series of operations
        on tables.
    governor_constraints : `~collections.abc.Mapping` [ `str`, \
            `~collections.abc.Set` [ `str` ] ]
        Constraints on governor dimensions encoded in this query's relation.
        This is a mapping from governor dimension name to sets of values that
        dimension may take.
    is_deferred : `bool`
        If `True`, modifier methods that return a related `Query` object should
        not immediately execute the new query.
    has_record_columns : `bool` or `DimensionElement`
        Whether this query's relation already includes columns for all or some
        dimension element records: `True` means all elements in ``dimensions``
        either have records present in ``record_caches`` or all columns present
        in ``relation``, while a specific `DimensionElement` means that element
        does.
    record_caches : `~collections.abc.Mapping` [ `DimensionElement`, \
            `~collections.abc.Mapping`
            [ `DataCoordinate`, `DimensionRecord` ] ], optional
        Cached dimension record values, organized first by dimension element
        and then by data ID.

    Notes
    -----
    Iterating over a `Query` yields mappings from `ColumnTag` to the associated
    value for each row.  The `iter_data_ids`, `iter_dataset_refs`, and
    `iter_dimension_records` methods can be used to instead iterate over
    various butler primitives derived from these rows.

    Iterating over a `Query` may or may not execute database queries again each
    time, depending on the state of its relation tree - see `Query.run` for
    details.

    Query is immutable; all methods that might appear to modify it in place
    actually return a new object (though many attributes will be shared).

    Query is currently (still) an internal-to-Registry object, with only the
    "QueryResults" classes that are backed by it directly exposed to users.  It
    has been designed with the intent that it will eventually play a larger
    role, either as the main query result object in a redesigned query
    interface, or a "power user" result option that accompanies simpler
    replacements for the current "QueryResults" objects.
    """

    def __init__(
        self,
        dimensions: DimensionGroup,
        backend: QueryBackend[QueryContext],
        context: QueryContext,
        relation: Relation,
        governor_constraints: Mapping[str, Set[str]],
        is_deferred: bool,
        has_record_columns: bool | DimensionElement,
        record_caches: Mapping[DimensionElement, Mapping[DataCoordinate, DimensionRecord]] | None = None,
    ):
        self._dimensions = dimensions
        self._backend = backend
        self._context = context
        self._relation = relation
        self._governor_constraints = governor_constraints
        self._is_deferred = is_deferred
        self._has_record_columns = has_record_columns
        self._record_caches = record_caches if record_caches is not None else {}

    @property
    def dimensions(self) -> DimensionGroup:
        """The dimensions that span the query and are used to join its
        relations together (`DimensionGroup`).
        """
        return self._dimensions

    @property
    def relation(self) -> Relation:
        """The relation tree representation of the query as a series of
        operations on tables (`Relation`).
        """
        return self._relation

    @property
    def has_record_columns(self) -> bool | DimensionElement:
        """Whether this query's relation already includes columns for all or
        some dimension element records (`bool` or `DimensionElement`).
        """
        return self._has_record_columns

    @property
    def backend(self) -> QueryBackend[QueryContext]:
        """Backend object used to create the query and new ones derived from it
        (`QueryBackend`).
        """
        return self._backend

    @contextmanager
    def open_context(self) -> Iterator[None]:
        """Return a context manager that ensures a database connection is
        established and temporary tables and cursors have a defined lifetime.

        Returns
        -------
        context : `contextlib.AbstractContextManager`
            Context manager with no return value.
        """
        if self._context.is_open:
            yield
        else:
            with self._context:
                yield

    def __str__(self) -> str:
        return str(self._relation)

    def __iter__(self) -> Iterator[Mapping[ColumnTag, Any]]:
        return iter(self._context.fetch_iterable(self._relation))

    def iter_data_ids(self, dimensions: DimensionGroup | None = None) -> Iterator[DataCoordinate]:
        """Return an iterator that converts result rows to data IDs.

        Parameters
        ----------
        dimensions : `DimensionGroup`, optional
            Dimensions of the data IDs to return.  If not provided,
            ``self.dimensions`` is used.

        Returns
        -------
        data_ids : `~collections.abc.Iterator` [ `DataCoordinate` ]
            Iterator that yields data IDs.
        """
        if dimensions is None:
            dimensions = self._dimensions
        reader = DataCoordinateReader.make(
            dimensions, records=self._has_record_columns is True, record_caches=self._record_caches
        )
        if not (reader.columns_required <= self.relation.columns):
            raise ColumnError(
                f"Missing column(s) {set(reader.columns_required - self.relation.columns)} "
                f"for data IDs with dimensions {dimensions}."
            )
        return (reader.read(row) for row in self)

    def iter_dataset_refs(
        self, dataset_type: DatasetType, components: Sequence[None | str] = (None,)
    ) -> Iterator[DatasetRef]:
        """Return an iterator that converts result rows to dataset references.

        Parameters
        ----------
        dataset_type : `DatasetType`
            The parent dataset type to yield references for.
        components : `~collections.abc.Sequence` [ `None` or `str` ]
            Which component dataset types to construct refs for from each row
            representing a parent; `None` for the parent itself.

        Returns
        -------
        refs : `~collections.abc.Iterator` [ `DatasetRef` ]
            Iterator that yields (resolved) dataset references.
        """
        reader = DatasetRefReader(
            dataset_type,
            translate_collection=self._backend.get_collection_name,
            records=self._has_record_columns is True,
            record_caches=self._record_caches,
        )
        if not (reader.columns_required <= self.relation.columns):
            raise ColumnError(
                f"Missing column(s) {set(reader.columns_required - self.relation.columns)} "
                f"for datasets with type {dataset_type.name} and dimensions {dataset_type.dimensions}."
            )
        for row in self:
            parent_ref = reader.read(row)
            for component in components:
                if component is None:
                    yield parent_ref
                else:
                    yield parent_ref.makeComponentRef(component)

    def iter_data_ids_and_dataset_refs(
        self, dataset_type: DatasetType, dimensions: DimensionGroup | None = None
    ) -> Iterator[tuple[DataCoordinate, DatasetRef]]:
        """Iterate over pairs of data IDs and dataset refs.

        This permits the data ID dimensions to differ from the dataset
        dimensions.

        Parameters
        ----------
        dataset_type : `DatasetType`
            The parent dataset type to yield references for.
        dimensions : `DimensionGroup`, optional
            Dimensions of the data IDs to return.  If not provided,
            ``self.dimensions`` is used.

        Returns
        -------
        pairs : `~collections.abc.Iterable` [ `tuple` [ `DataCoordinate`,
                `DatasetRef` ] ]
            An iterator over (data ID, dataset reference) pairs.
        """
        if dimensions is None:
            dimensions = self._dimensions
        data_id_reader = DataCoordinateReader.make(
            dimensions, records=self._has_record_columns is True, record_caches=self._record_caches
        )
        dataset_reader = DatasetRefReader(
            dataset_type,
            translate_collection=self._backend.get_collection_name,
            records=self._has_record_columns is True,
            record_caches=self._record_caches,
        )
        if not (data_id_reader.columns_required <= self.relation.columns):
            raise ColumnError(
                f"Missing column(s) {set(data_id_reader.columns_required - self.relation.columns)} "
                f"for data IDs with dimensions {dimensions}."
            )
        if not (dataset_reader.columns_required <= self.relation.columns):
            raise ColumnError(
                f"Missing column(s) {set(dataset_reader.columns_required - self.relation.columns)} "
                f"for datasets with type {dataset_type.name} and dimensions {dataset_type.dimensions}."
            )
        for row in self:
            yield (data_id_reader.read(row), dataset_reader.read(row))

    def iter_dimension_records(self, element: DimensionElement | None = None) -> Iterator[DimensionRecord]:
        """Return an iterator that converts result rows to dimension records.

        Parameters
        ----------
        element : `DimensionElement`, optional
            Dimension element whose records will be returned.  If not provided,
            `has_record_columns` must be a `DimensionElement` instance.

        Returns
        -------
        records : `~collections.abc.Iterator` [ `DimensionRecord` ]
            Iterator that yields dimension records.
        """
        if element is None:
            match self._has_record_columns:
                case True | False:
                    raise ValueError("No default dimension element in query; 'element' must be given.")
                case only_element_with_records:
                    element = only_element_with_records
        if (cache := self._record_caches.get(element)) is not None:
            return (cache[data_id] for data_id in self.iter_data_ids(element.minimal_group))
        else:
            reader = DimensionRecordReader(element)
            if not (reader.columns_required <= self.relation.columns):
                raise ColumnError(
                    f"Missing column(s) {set(reader.columns_required - self.relation.columns)} "
                    f"for records of element {element.name}."
                )
            return (reader.read(row) for row in self)

    def run(self) -> Query:
        """Execute the query and hold its results in memory.

        Returns
        -------
        executed : `Query`
            New query that holds the query results.

        Notes
        -----
        Iterating over the results of a query that has been `run` will always
        iterate over an existing container, while iterating over a query that
        has not been run will result in executing at least some of the query
        each time.

        Running a query also sets its `is_deferred` flag to `False`, which will
        cause new queries constructed by its methods to be run immediately,
        unless ``defer=True`` is passed to the factory method. After a query
        has been run, factory methods will also tend to prefer to apply new
        operations (e.g. `with_only_column`, `sliced`, `sorted`) via Python
        code acting on the existing container rather than going back to SQL,
        which can be less efficient overall that applying operations to a
        deferred query and executing them all only at the end.

        Running a query is represented in terms of relations by adding a
        `~lsst.daf.relation.Materialization` marker relation in the iteration
        engine and then processing the relation tree; this attaches the
        container of rows to that new relation to short-circuit any future
        processing of the tree and lock changes to the tree upstream of it.
        This is very different from the SQL-engine
        `~lsst.daf.relation.Materialization` added to the tree by the
        `materialize` method from a user perspective, though it has a similar
        representation in the relation tree.
        """
        relation = (
            # Make a new relation that definitely ends in the iteration engine
            # (this does nothing if it already does).
            self.relation.transferred_to(self._context.iteration_engine)
            # Make the new relation save its rows to an in-memory Python
            # collection in relation.payload when processed.
            .materialized(name_prefix="run")
        )
        # Actually process the relation, simplifying out trivial relations,
        # executing any SQL queries, and saving results to relation.payload.
        # We discard the simplified relation that's returned, because we want
        # the new query to have any extra diagnostic information contained in
        # the original.
        self._context.process(relation)
        return self._copy(relation, False)

    def materialized(self, defer_postprocessing: bool = True) -> Query:
        """Materialize the results of this query in its context's preferred
        engine.

        Usually this means inserting the results into a temporary table in a
        database.

        Parameters
        ----------
        defer_postprocessing : `bool`, optional
            If `True`, do not execute operations that occur in the context's
            `QueryContext.iteration_engine` up front; instead insert and
            execute a materialization upstream of them (e.g. via a a SQL
            ``INSERT INTO ... SELECT`` statement, with no fetching to the
            client) and execute the postprocessing operations when iterating
            over the query results.  If `False`, and iteration-engine
            postprocessing operations exist, run the full query, execute them
            now, and upload the results.
            If the relation is already in the preferred engine, this option
            is ignored and the materialization will not involve fetching rows
            to the iteration engine at all.  If the relation has already been
            materialized in the iteration engine (i.e. via `run`), then this
            option is again ignored and an upload of the existing rows will
            be performed.

        Returns
        -------
        materialized : `Query`
            Modified query with the same row-and-column content with a
            materialization in ``self.context.preferred_engine``.
        """
        if defer_postprocessing or self.relation.engine == self._context.preferred_engine:
            relation, stripped = self._context.strip_postprocessing(self._relation)
            if relation.engine == self._context.preferred_engine:
                # We got all the way to the engine we want to materialize in.
                # Apply that operation to the tree, process it (which actually
                # creates a temporary table and populates it), and then reapply
                # the stripped operations.
                relation = relation.materialized()
                self._context.process(relation)
                for operation in stripped:
                    relation = operation.apply(
                        relation, transfer=True, preferred_engine=self._context.iteration_engine
                    )
                return self._copy(relation, True)
        # Either defer_postprocessing=False, or attempting to strip off unary
        # operations until we got to the preferred engine didn't work, because
        # this tree doesn't actually involve the preferred engine.  So we just
        # transfer to the preferred engine first, and then materialize,
        # process, and return.
        relation = self._relation.transferred_to(self._context.preferred_engine).materialized()
        self._context.process(relation)
        return self._copy(relation, True)

    def projected(
        self,
        dimensions: DimensionGroup | Iterable[str] | None = None,
        unique: bool = True,
        columns: Iterable[ColumnTag] | None = None,
        defer: bool | None = None,
        drop_postprocessing: bool = False,
        keep_record_columns: bool = True,
    ) -> Query:
        """Return a modified `Query` with a subset of this one's columns.

        Parameters
        ----------
        dimensions : `~collections.abc.Iterable` [ `str` ],
                optional
            Dimensions to include in the new query.  Will be expanded to
            include all required and implied dependencies.  Must be a subset of
            ``self.dimensions``.  If not provided, ``self.dimensions`` is used.
        unique : `bool`, optional
            If `True` (default) deduplicate rows after dropping columns.
        columns : `~collections.abc.Iterable` [ `ColumnTag` ], optional
            Additional dataset or dimension record columns to include in the
            query.  Dimension key columns added here are ignored unless they
            extend beyond the key columns implied by the ``dimensions``
            argument (which is an error).
        defer : `bool`, optional
            If `False`, run the new query immediately.  If `True`, do not.  If
            `None` (default), the ``defer`` option passed when making ``self``
            is used (this option is "sticky").
        drop_postprocessing : `bool`, optional
            Drop any iteration-engine operations that depend on columns that
            are being removed (e.g. region-overlap tests when region columns
            are being dropped), making it more likely that projection and
            deduplication could be performed in the preferred engine, where
            they may be more efficient.
        keep_record_columns : `bool`, optional
            If `True` (default) and this query `has_record_columns`, implicitly
            add any of those to ``columns`` whose dimension element is in the
            given ``dimensions``.

        Returns
        -------
        query : `Query`
            New query with the requested columns only, optionally deduplicated.

        Notes
        -----
        Dataset columns are dropped from the new query unless passed via the
        ``columns`` argument.  All other columns are by default preserved.

        Raises
        ------
        lsst.daf.relation.ColumnError
            Raised if the columns to include in the new query are not all
            present in the current query.
        """
        match dimensions:
            case None:
                dimensions = set(self._dimensions.names)
            case DimensionGroup():
                dimensions = set(dimensions.names)
            case iterable:
                dimensions = set(iterable)
        if columns is not None:
            dimensions.update(tag.dimension for tag in DimensionKeyColumnTag.filter_from(columns))
        dimensions = self._dimensions.universe.conform(dimensions)
        if columns is None:
            columns = set()
        else:
            columns = set(columns)
        columns.update(DimensionKeyColumnTag.generate(dimensions.names))
        if keep_record_columns:
            if self._has_record_columns is True:
                for element_name in dimensions.elements:
                    if element_name not in self._record_caches:
                        columns.update(self.dimensions.universe[element_name].RecordClass.fields.columns)
            elif self._has_record_columns in dimensions.elements:
                element = cast(DimensionElement, self._has_record_columns)
                columns.update(element.RecordClass.fields.columns)
        if drop_postprocessing:
            relation = self._context.drop_invalidated_postprocessing(self._relation, columns)
            # Dropping postprocessing Calculations could cause other columns
            # we had otherwise intended to keep to be dropped as well.
            columns &= relation.columns
        else:
            relation = self._relation
        relation = relation.with_only_columns(columns, preferred_engine=self._context.preferred_engine)
        if unique:
            relation = relation.without_duplicates(preferred_engine=self._context.preferred_engine)
        return self._chain(relation, defer, dimensions=dimensions)

    def with_record_columns(self, dimension_element: str | None = None, defer: bool | None = None) -> Query:
        """Return a modified `Query` with additional dimension record columns
        and/or caches.

        Parameters
        ----------
        dimension_element : `str`, optional
            Name of a single dimension element to add record columns for, or
            `None` default to add them for all elements in `dimensions`.
        defer : `bool`, optional
            If `False`, run the new query immediately.  If `True`, do not.  If
            `None` (default), the ``defer`` option passed when making ``self``
            is used (this option is "sticky").

        Returns
        -------
        query : `Query`
            New query with the requested record columns either in the relation
            or (when possible) available via record caching.

        Notes
        -----
        Adding dimension record columns is fundamentally different from adding
        new dimension key columns or dataset columns, because it is purely an
        addition of columns, not rows - we can always join in a dimension
        element table (if it has not already been included) on keys already
        present in the current relation, confident that there is exactly one
        row in the dimension element table for each row in the current
        relation.
        """
        if self._has_record_columns is True or self._has_record_columns == dimension_element:
            return self
        record_caches = dict(self._record_caches)
        columns_required: set[ColumnTag] = set()
        for element_name in self.dimensions.elements if dimension_element is None else [dimension_element]:
            element = self.dimensions.universe[element_name]
            if element_name in record_caches:
                continue
            if (cache := self._backend.get_dimension_record_cache(element_name, self._context)) is not None:
                record_caches[element] = cache
            else:
                columns_required.update(element.RecordClass.fields.columns.keys())
        # Modify the relation we have to remove any projections that dropped
        # columns we now want, as long the relation's behavior is otherwise
        # unchanged.
        columns_required -= self._relation.columns
        relation, columns_found = self._context.restore_columns(self._relation, columns_required)
        columns_required.difference_update(columns_found)
        if columns_required:
            relation = self._backend.make_dimension_relation(
                self._dimensions,
                columns_required,
                self._context,
                initial_relation=relation,
                # Don't permit joins to use any columns beyond those in the
                # original relation, as that would change what this operation
                # does.
                initial_join_max_columns=frozenset(self._relation.columns),
                governor_constraints=self._governor_constraints,
            )
        return self._chain(
            relation,
            defer=defer,
            has_record_columns=(
                True if dimension_element is None else self.dimensions.universe[dimension_element]
            ),
            record_caches=record_caches,
        )

    def find_datasets(
        self,
        dataset_type: DatasetType,
        collections: Any,
        *,
        find_first: bool = True,
        columns: Set[str] = frozenset(("dataset_id", "run")),
        defer: bool | None = None,
    ) -> Query:
        """Return a modified `Query` that includes a search for datasets of the
        given type.

        Parameters
        ----------
        dataset_type : `DatasetType`
            Dataset type to search for.  May not be a component.
        collections
            Collection search path or pattern.  Must be a single collection
            name or ordered sequence if ``find_first=True``.  See
            :ref:`daf_butler_collection_expressions` for more information.
        find_first : `bool`, optional
            If `True` (default) search collections in order until the first
            match for each data ID is found.  If `False`, return all matches in
            all collections.
        columns : `~collections.abc.Set` [ `str` ]
            Dataset columns to include in the new query.  Options include

            - ``dataset_id``: the unique identifier of the dataset.  The type
              is implementation-dependent.  Never nullable.  Included by
              default.

            - ``ingest_date``: the date and time the dataset was added to the
              data repository.

            - ``run``: the foreign key column to the `~CollectionType.RUN`
              collection holding the dataset (not necessarily the collection
              name).  The type is dependent on the collection manager
              implementation.  Included by default.

            - ``collection``: the foreign key column to the collection type in
              which the dataset was actually in this search.  The type is
              dependent on the collection manager implementation.  This may
              differ from ``run`` if the dataset is present in a matching
              `~CollectionType.TAGGED` or `~CollectionType.CALIBRATION`
              collection, which means the same dataset may also appear multiple
              times in the query results.

            - ``timespan``: the validity range for datasets found in a
              `~CollectionType.CALIBRATION` collection, or ``NULL`` for other
              collection types.

            The default columns (``dataset_id`` and ``run``) are sufficient to
            enable `iter_dataset_refs`, which also takes care of translating
            the internal ``RUN`` collection key into its public name.

            Setting this to an empty set while passing ``find_first=False``
            will return a query that is constrained by dataset existence in
            some matching collection that does not actually return which
            datasets existed.
        defer : `bool`, optional
            If `False`, run the new query immediately.  If `True`, do not.  If
            `None` (default), the ``defer`` option passed when making ``self``
            is used (this option is "sticky").

        Returns
        -------
        query : `Query`
            New query with the requested dataset columns, constrained by the
            existence of datasets of this type in the given collection.

        Raises
        ------
        lsst.daf.relation.ColumnError
            Raised if a dataset search is already present in this query and
            this is a find-first search.
        """
        if find_first and DatasetColumnTag.filter_from(self._relation.columns):
            raise ColumnError(
                "Cannot search for datasets with find_first=True "
                "on a query that already includes dataset columns."
            )
        #
        # TODO: it'd be nice to do a QueryContext.restore_columns call here or
        # similar, to look for dataset-constraint joins already present in the
        # relation and expand them to include dataset-result columns as well,
        # instead of doing a possibly-redundant join here.  But that would
        # require pushing relation usage down further into
        # DatasetStorageManager.make_relation, so that it doesn't need to be
        # given the columns, and then giving the relation system the ability to
        # simplify-away redundant joins when they only provide columns that
        # aren't ultimately used.  The right time to look into that is probably
        # when investigating whether the base QueryBackend should be
        # responsible for producing an "abstract" relation tree of some sort,
        # with the subclasses only responsible for filling it in with payloads
        # (and possibly replacing some leaves with new sub-trees) during when
        # "processed" (or in some other "prepare" step).
        #
        # This is a low priority for three reasons:
        # - there's some chance the database's query optimizer will simplify
        #   away these redundant joins;
        # - at present, the main use of this code path is in QG generation,
        #   where we materialize the initial data ID query into a temp table
        #   and hence can't go back and "recover" those dataset columns anyway;
        #
        collections = CollectionWildcard.from_expression(collections)
        if find_first:
            collections.require_ordered()
        rejections: list[str] = []
        collection_records = self._backend.resolve_dataset_collections(
            dataset_type,
            collections,
            governor_constraints=self._governor_constraints,
            allow_calibration_collections=True,
            rejections=rejections,
        )
        # If the dataset type has dimensions not in the current query, or we
        # need a temporal join for a calibration collection, either restore
        # those columns or join them in.
        full_dimensions = dataset_type.dimensions.as_group().union(self._dimensions)
        relation = self._relation
        record_caches = self._record_caches
        base_columns_required: set[ColumnTag] = {
            DimensionKeyColumnTag(name) for name in full_dimensions.names
        }
        spatial_joins: list[tuple[str, str]] = []
        if not (dataset_type.dimensions <= self._dimensions):
            if self._has_record_columns is True:
                # This query is for expanded data IDs, so if we add new
                # dimensions to the query we need to be able to get records for
                # the new dimensions.
                record_caches = dict(self._record_caches)
                for element_name in full_dimensions.elements:
                    element = full_dimensions.universe[element_name]
                    if element in record_caches:
                        continue
                    if (
                        cache := self._backend.get_dimension_record_cache(element_name, self._context)
                    ) is not None:
                        record_caches[element] = cache
                    else:
                        base_columns_required.update(element.RecordClass.fields.columns.keys())
            # See if we need spatial joins between the current query and the
            # dataset type's dimensions.  The logic here is for multiple
            # spatial joins in general, but in practice it'll be exceedingly
            # rare for there to be more than one.  We start by figuring out
            # which spatial "families" (observations vs. skymaps, skypix
            # systems) are present on only one side and not the other.
            lhs_spatial_families = self._dimensions.spatial - dataset_type.dimensions.spatial
            rhs_spatial_families = dataset_type.dimensions.spatial - self._dimensions.spatial
            # Now we iterate over the Cartesian product of those, so e.g.
            # if the query has {tract, patch, visit} and the dataset type
            # has {htm7} dimensions, the iterations of this loop
            # correspond to: (skymap, htm), (observations, htm).
            for lhs_spatial_family, rhs_spatial_family in itertools.product(
                lhs_spatial_families, rhs_spatial_families
            ):
                # For each pair we add a join between the most-precise element
                # present in each family (e.g. patch beats tract).
                spatial_joins.append(
                    (
                        lhs_spatial_family.choose(
                            full_dimensions.elements.names, self.dimensions.universe
                        ).name,
                        rhs_spatial_family.choose(
                            full_dimensions.elements.names, self.dimensions.universe
                        ).name,
                    )
                )
        # Set up any temporal join between the query dimensions and CALIBRATION
        # collection's validity ranges.
        temporal_join_on: set[ColumnTag] = set()
        if any(r.type is CollectionType.CALIBRATION for r in collection_records):
            for family in self._dimensions.temporal:
                endpoint = family.choose(self._dimensions.elements.names, self.dimensions.universe)
                temporal_join_on.add(DimensionRecordColumnTag(endpoint.name, "timespan"))
            base_columns_required.update(temporal_join_on)
        # Note which of the many kinds of potentially-missing columns we have
        # and add the rest.
        base_columns_required.difference_update(relation.columns)
        if base_columns_required:
            relation = self._backend.make_dimension_relation(
                full_dimensions,
                base_columns_required,
                self._context,
                initial_relation=relation,
                # Don't permit joins to use any columns beyond those in the
                # original relation, as that would change what this
                # operation does.
                initial_join_max_columns=frozenset(self._relation.columns),
                governor_constraints=self._governor_constraints,
                spatial_joins=spatial_joins,
            )
        # Finally we can join in the search for the dataset query.
        columns = set(columns)
        columns.add("dataset_id")
        if not collection_records:
            relation = relation.join(
                self._backend.make_doomed_dataset_relation(dataset_type, columns, rejections, self._context)
            )
        elif find_first:
            relation = self._backend.make_dataset_search_relation(
                dataset_type,
                collection_records,
                columns,
                self._context,
                join_to=relation,
                temporal_join_on=temporal_join_on,
            )
        else:
            relation = self._backend.make_dataset_query_relation(
                dataset_type,
                collection_records,
                columns,
                self._context,
                join_to=relation,
                temporal_join_on=temporal_join_on,
            )
        return self._chain(relation, dimensions=full_dimensions, record_caches=record_caches, defer=defer)

    def sliced(
        self,
        start: int = 0,
        stop: int | None = None,
        defer: bool | None = None,
    ) -> Query:
        """Return a modified `Query` with that takes a slice of this one's
        rows.

        Parameters
        ----------
        start : `int`, optional
            First index to include, inclusive.
        stop : `int` or `None`, optional
            One past the last index to include (i.e. exclusive).
        defer : `bool`, optional
            If `False`, run the new query immediately.  If `True`, do not.  If
            `None` (default), the ``defer`` option passed when making ``self``
            is used (this option is "sticky").

        Returns
        -------
        query : `Query`
            New query with the requested slice.

        Notes
        -----
        This operation must be implemented in the iteration engine if there are
        postprocessing operations, which may be much less efficient than
        performing it in the preferred engine (e.g. via ``LIMIT .. OFFSET ..``
        in SQL).

        Since query row order is usually arbitrary, it usually makes sense to
        call `sorted` before calling `sliced` to make the results
        deterministic.  This is not checked because there are some contexts
        where getting an arbitrary subset of the results of a given size
        still makes sense.
        """
        return self._chain(self._relation[start:stop], defer)

    def sorted(
        self,
        order_by: Iterable[SortTerm],
        defer: bool | None = None,
    ) -> Query:
        """Return a modified `Query` that sorts this one's rows.

        Parameters
        ----------
        order_by : `~collections.abc.Iterable` [ `SortTerm` ]
            Expressions to sort by.
        defer : `bool`, optional
            If `False`, run the new query immediately.  If `True`, do not.  If
            `None` (default), the ``defer`` option passed when making ``self``
            is used (this option is "sticky").

        Returns
        -------
        query : `Query`
            New query with the requested sorting.

        Notes
        -----
        The ``order_by`` expression can include references to dimension record
        columns that were not present in the original relation; this is
        similar to calling `with_record_columns` for those columns first (but
        in this case column requests cannot be satisfied by record caches).
        All other columns referenced must be present in the query already.
        """
        op = Sort(tuple(order_by))
        columns_required = set(op.columns_required)
        columns_required.difference_update(self._relation.columns)
        if columns_required:
            relation, columns_found = self._context.restore_columns(self._relation, columns_required)
            columns_required.difference_update(columns_found)
            if columns_required:
                try:
                    relation = self._backend.make_dimension_relation(
                        self._dimensions,
                        columns_required,
                        self._context,
                        initial_relation=relation,
                        # Don't permit joins to use any columns beyond those in
                        # the original relation, as that would change what this
                        # operation does.
                        initial_join_max_columns=frozenset(self._relation.columns),
                        governor_constraints=self._governor_constraints,
                    )
                except ColumnError as err:
                    raise ColumnError(
                        "Cannot sort by columns that were not included in the original query or "
                        "fully resolved by its dimensions."
                    ) from err
        else:
            relation = self._relation
        relation = op.apply(relation, preferred_engine=self._context.preferred_engine)
        return self._chain(relation, defer)

    def count(self, *, exact: bool = True, discard: bool = False) -> int:
        """Count the number of rows in this query.

        Parameters
        ----------
        exact : `bool`, optional
            If `True` (default), return the exact number of rows.  If `False`,
            returning an upper bound is permitted if it can be done much more
            efficiently, e.g. by running a SQL ``SELECT COUNT(*)`` query but
            ignoring client-side filtering that would otherwise take place.
        discard : `bool`, optional
            If `True`, compute the exact count even if it would require running
            the full query and then throwing away the result rows after
            counting them.  If `False`, this is an error, as the user would
            usually be better off executing the query first to fetch its rows
            into a new query (or passing ``exact=False``).  Ignored if
            ``exact=False``.

        Returns
        -------
        n_rows : `int`
            Number of rows in the query, or an upper bound.  This includes
            duplicates, if there are any.

        Raises
        ------
        RuntimeError
            Raised if an exact count was requested and could not be obtained
            without fetching and discarding rows.
        """
        if self._relation.min_rows == self._relation.max_rows:
            return self._relation.max_rows
        return self._context.count(self._relation, exact=exact, discard=discard)

    def any(self, *, execute: bool = True, exact: bool = True) -> bool:
        """Check whether this query has any result rows at all.

        Parameters
        ----------
        execute : `bool`, optional
            If `True`, execute at least a ``LIMIT 1`` query if it cannot be
            determined prior to execution that the query would return no rows.
        exact : `bool`, optional
            If `True`, run the full query and perform post-query filtering if
            needed, until at least one result row is found.  If `False`, the
            returned result does not account for post-query filtering, and
            hence may be `True` even when all result rows would be filtered
            out.

        Returns
        -------
        any_rows : `bool`
            Whether the query has any rows, or if it may have any rows if
            ``exact=False``.

        Raises
        ------
        RuntimeError
            Raised if an exact check was requested and could not be obtained
            without executing the query.
        """
        if self._relation.min_rows > 0:
            return True
        if self._relation.max_rows == 0:
            return False
        if execute:
            return self._context.any(self._relation, execute=execute, exact=exact)
        elif not exact:
            return True
        raise TypeError("Cannot obtain exact results without executing the query.")

    def explain_no_results(self, execute: bool = True) -> list[str]:
        """Return human-readable messages that may help explain why the query
        yields no results.

        Parameters
        ----------
        execute : `bool`, optional
            If `True` (default) execute simplified versions (e.g. ``LIMIT 1``)
            of aspects of the query to more precisely determine where rows were
            filtered out.

        Returns
        -------
        messages : `~collections.abc.Iterable` [ `str` ]
            String messages that describe reasons the query might not yield any
            results.
        """
        # First try without actually executing any queries.
        diagnostics = Diagnostics.run(self._relation)
        if diagnostics.is_doomed:
            return diagnostics.messages
        if execute:
            # Try again, running LIMIT 1 queries as we walk back down the tree
            # to look for relations with no rows:
            diagnostics = Diagnostics.run(self._relation, executor=self._context.any)
            if diagnostics.is_doomed:
                return diagnostics.messages
        return []

    def _copy(
        self,
        relation: Relation,
        is_deferred: bool,
        dimensions: DimensionGroup | None = None,
        governor_constraints: Mapping[str, Set[str]] | None = None,
        has_record_columns: bool | DimensionElement | None = None,
        record_caches: Mapping[DimensionElement, Mapping[DataCoordinate, DimensionRecord]] | None = None,
    ) -> Query:
        """Return a modified copy of this query with some attributes replaced.

        See class docs for parameter documentation; the only difference here
        is that the defaults are the values ``self`` was constructed with.
        """
        return Query(
            dimensions=self._dimensions if dimensions is None else dimensions,
            backend=self._backend,
            context=self._context,
            relation=relation,
            governor_constraints=(
                governor_constraints if governor_constraints is not None else self._governor_constraints
            ),
            is_deferred=is_deferred,
            has_record_columns=self._has_record_columns if has_record_columns is None else has_record_columns,
            record_caches=self._record_caches if record_caches is None else record_caches,
        )

    def _chain(
        self,
        relation: Relation,
        defer: bool | None,
        dimensions: DimensionGroup | None = None,
        governor_constraints: Mapping[str, Set[str]] | None = None,
        has_record_columns: bool | DimensionElement | None = None,
        record_caches: Mapping[DimensionElement, Mapping[DataCoordinate, DimensionRecord]] | None = None,
    ) -> Query:
        """Return a modified query with a new relation while handling the
        ubiquitous ``defer`` parameter's logic.

        Parameters
        ----------
        relation : `Relation`
            Relation for the new query.
        defer : `bool`
            If `False`, run the new query immediately.  If `True`, do not.  If
            `None` , the ``defer`` option passed when making ``self`` is used
            (this option is "sticky").
        dimensions : `DimensionGroup`, optional
            See class docs.
        governor_constraints : `~collections.abc.Mapping` [ `str`, \
                `~collections.abc.Set` [ `str` ] ], optional
            See class docs.
        has_record_columns : `bool` or `DimensionElement`, optional
            See class docs.
        record_caches : `~collections.abc.Mapping` [ `DimensionElement`, \
                `~collections.abc.Mapping` \
                [ `DataCoordinate`, `DimensionRecord` ] ], optional
            See class docs.

        Returns
        -------
        chained : `Query`
            Modified query, or ``self`` if no modifications were actually
            requested.
        """
        if defer is None:
            defer = self._is_deferred
        if (
            relation is self._relation
            and dimensions is None
            and defer == self._is_deferred
            and record_caches is None
            and has_record_columns is None
            and governor_constraints is None
        ):
            return self
        result = self._copy(
            relation,
            is_deferred=True,
            governor_constraints=governor_constraints,
            dimensions=dimensions,
            has_record_columns=has_record_columns,
            record_caches=record_caches,
        )
        if not defer:
            result = result.run()
        return result

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

__all__ = ("MultipleDatasetQueryBuilder", "MultipleDatasetQueryRow", "DatasetNecessityEnum")

import itertools
import logging
from enum import Enum, auto
from collections import namedtuple
from sqlalchemy.sql import and_

from ..core import DimensionSet, DatasetRef
from .queryBuilder import QueryBuilder
from .singleDatasetQueryBuilder import SingleDatasetQueryBuilder

_LOG = logging.getLogger(__name__)


class MultipleDatasetQueryRow:
    r"""Simple data class holding the `DatasetRef`s and `DataId`s produced
    by a `MultipleDatasetQueryBuilder`.

    Parameters
    ----------
    dataId : `DataId`
        Link values for all common (non-per-`DatasetType`) dimensions.
    datasetRefs : `dict`
        Dictionary mapping `DatasetType` to `DatasetRef`.

    Notes
    -----
    Logically, an instance of this class represents a single "path" connecting
    a set of `DatasetRef`s which exist or may exist for a given set of
    `DatasetType`s based on the `Dimension` relational algebra.

    A multiple-dataset query returns a sequence of `MultipleDatasetQueryRow`
    instances; each instance will have a unique `DataId`, but the
    `DatasetRef`s in ``datasetRefs`` are not necessarily unique.  For example,
    when the `PipelineTask` pre-flight solver generates data for a `Quantum`
    that has two `DatasetRef`s on input and one on output it will create two
    `MultipleDatasetQueryRow` instances with the same `DatasetRef` for the
    output dataset type. It is caller's the responsibility to combine multiple
    `MultipleDatasetQueryRow`s into a suitable structure (e.g.,
    `lsst.pipe.base.QuantumGraph`).

    """
    __slots__ = ("_dataId", "_datasetRefs")

    def __init__(self, dataId, datasetRefs):
        self._dataId = dataId
        self._datasetRefs = datasetRefs

    @property
    def dataId(self):
        """Link values for all common (non-per-`DatasetType`) dimensions
        (`DataId`).
        """
        return self._dataId

    @property
    def datasetRefs(self):
        """Dictionary mapping `DatasetType` to `DatasetRef` (`dict`).
        """
        return self._datasetRefs

    def __str__(self):
        return "(dataId={}, datasetRefs=[{}])".format(
            self.dataId, ', '.join(str(ref) for ref in self.datasetRefs.values()))


class DatasetNecessityEnum(Enum):
    """Enum for describing different kinds of dataset subqueries in a
    `MultipleDatasetQueryBuilder.`

    See `MultipleDatasetQueryBuilder.fromDatasetTypes` for more information.
    """
    REQUIRED = auto()
    OPTIONAL = auto()
    PREREQUISITE = auto()


_SubqueryData = namedtuple("_SubqueryData", ("subquery", "necessity", "links"))

_DeferralData = namedtuple("_DeferralData", ("builder", "necessity", "joinLinks", "perDatasetTypeLinks"))


class MultipleDatasetQueryBuilder(QueryBuilder):
    r"""Specialization of `QueryBuilder` that relates multiple `DatasetType`s
    via their `Dimensions`.

    Most users should call `fromDatasetTypes` to construct an instance of this
    class, rather than invoking the constructor and calling
    `~QueryBuilder.joinDimensionElement` or `joinDataset` directly.

    Parameters
    ----------
    registry : `SqlRegistry`
        Registry instance the query is being run against.
    fromClause : `sqlalchemy.sql.expression.FromClause`, optional
        Initial FROM clause for the query.
    whereClause : SQLAlchemy boolean expression, optional
        Expression to use as the initial WHERE clause.
    """

    def __init__(self, registry, *, fromClause=None, whereClause=None):
        super().__init__(registry, fromClause=fromClause, whereClause=whereClause)
        self._subqueries = {}
        self._deferrals = {}

    @classmethod
    def fromDatasetTypes(cls, registry, originInfo, required=(), optional=(), prerequisite=(),
                         perDatasetTypeDimensions=(), defer=False, addResultColumns=True):
        r"""Build a query that relates multiple `DatasetType`s via their
        dimensions.

        This method ensures that all `Dimension` and `DimensionJoin` tables
        necessary to relate the given datasets are also included.

        Parameters
        ----------
        registry : `SqlRegistry`
            Registry instance the query is being run against.
        originInfo : `DatasetOriginInfo`
            Information about which collections to search for different
            `DatasetType`s.
        required : iterable of `DatasetType`
            DatasetType`s whose presence or absence constrains the query
            results; these are added to the query with an INNER JOIN.
        optional : iterable of `DatasetType`
            `DatasetType`s whose presence or absence does not constrain the
            query results; these are added to the query with a LEFT OUTER
            JOIN. Note that this does nothing unless the ID for this dataset
            is actually requested in the results, via either
            ``addResultColumns`` here or `selectDatasetId`.
        prerequisite : iterable of `DatasetType`
            `DatasetType`s that should not constrain the query results, but
            must be present for all result rows.  These are included with
            a LEFT OUTER JOIN, but the results are checked for NULL.  Unlike
            regular inputs, prerequisite inputs lookups may be deferred
            (see the documentaiton ``defer`` argument).
            Any `DatasetType`'s that are present in both ``required`` and
            ``prerequisite`` are considered ``prerequisite``.
        perDatasetTypeDimensions : iterable of `Dimension` or `str`, optional
            Dimensions (or `str` names thereof) for which different dataset
            types do not need to have the same values in each result row.
        defer : `bool`
            If `True`, defer queries for optional and prerequisite dataset IDs
            until row-by-row processing of the main query's results. Queries
            for required dataset IDs are never deferred.
        addResultColumns : `bool`
            If `True` (default), add result columns to the SELECT clause for
            all dataset IDs and dimension links.
        """
        required = set(required)
        optional = set(optional)
        prerequisite = set(prerequisite)
        required -= prerequisite
        assert required.isdisjoint(optional)
        assert prerequisite.isdisjoint(optional)

        perDatasetTypeDimensions = DimensionSet(registry.dimensions, perDatasetTypeDimensions)
        resultDimensions = registry.dimensions.extract(
            itertools.chain(
                itertools.chain.from_iterable(dsType.dimensions.names for dsType in required),
                itertools.chain.from_iterable(dsType.dimensions.names for dsType in optional),
                itertools.chain.from_iterable(dsType.dimensions.names for dsType in prerequisite),
            )
        )
        _LOG.debug("Original dimensions (needed by DatasetTypes): %s", resultDimensions)
        allDimensions = resultDimensions.union(resultDimensions.implied())
        _LOG.debug("All dimensions (expanded to include implied): %s", allDimensions)
        commonDimensions = registry.dimensions.extract(
            allDimensions.toSet().difference(perDatasetTypeDimensions),
            implied=True
        )
        _LOG.debug("Per-DatasetType dimensions: %s", perDatasetTypeDimensions)
        _LOG.debug("Common dimensions (per-DatasetType dimensions removed): %s", commonDimensions)
        if not commonDimensions.isdisjoint(perDatasetTypeDimensions):
            raise ValueError("Some per-DatasetType dimensions are dependencies of common dimensions")

        self = cls.fromDimensions(registry, dimensions=commonDimensions, addResultColumns=addResultColumns)

        for datasetType in required:
            self.joinDataset(datasetType, originInfo.getInputCollections(datasetType.name),
                             commonDimensions=commonDimensions,
                             addResultColumns=addResultColumns)
        for datasetType in optional:
            self.joinDataset(datasetType, [originInfo.getOutputCollection(datasetType.name)],
                             necessity=DatasetNecessityEnum.OPTIONAL,
                             defer=defer, commonDimensions=commonDimensions,
                             addResultColumns=addResultColumns)
        for datasetType in prerequisite:
            self.joinDataset(datasetType, originInfo.getInputCollections(datasetType.name),
                             necessity=DatasetNecessityEnum.PREREQUISITE,
                             defer=defer, commonDimensions=commonDimensions,
                             addResultColumns=addResultColumns)
        return self

    @property
    def datasetTypes(self):
        """The dataset types this query searches for (`~collections.abc.Set` of
        `DatasetType`).
        """
        return self._subqueries.keys()

    def joinDataset(self, datasetType, collections, necessity=DatasetNecessityEnum.REQUIRED,
                    defer=False, commonDimensions=None, addResultColumns=True):
        """Join an aliased subquery of the dataset table for a particular
        `DatasetType` into the query.

        This method attempts to join the dataset subquery on the dimension
        link columns that identify that `DatasetType`, which in general means
        at least one `Dimension` table for all of those types should be present
        in the query first.  This can be guaranteed by calling
        `fromDatasetTypes` to construct the `QueryBuilder` instead of calling
        this method directly.

        Parameters
        ----------
        datasetType : `DatasetType`
            Object representing the type of dataset to query for.
        collections : `list` of `str`
            String names of the collections in which to search for the dataset,
            ordered from the first to be searched to the last to be searched.
        necessity : `DatasetNecessityEnum`
            Enum value indicating whether and how the existence of this dataset
            should constrain the query results.
        defer : `bool`
            If `True`, defer querying for the IDs for this dataset until
            processing the main query results.  Must be `False` if
            ``necessity`` is `DatasetNecessityEnum.REQUIRED`.  Note that this
            does nothing unless the ID for this dataset is actually requested
            in the results, via either ``addResultColumns`` here or
            `selectDatasetId`.
        commonDimensions : `DimensionGraph`, optional
            Dimensions already present in the query that the dimensions of
            the `DatasetType` should be related to in the query (see
            `SingleDatasetQueryBuilder.relateDimensions`).
        addResultColumns : `bool`
            If `True` (default), add the ``dataset_id`` for this `DatasetType`
            to the result columns in the SELECT clause of the query.
        """
        if datasetType in self._subqueries:
            raise ValueError(f"DatasetType {datasetType.name} already included in query.")
        builder = SingleDatasetQueryBuilder.fromCollections(self.registry, datasetType, collections)
        if commonDimensions is not None:
            perDatasetTypeLinks = datasetType.dimensions.links() - commonDimensions.links()
        else:
            perDatasetTypeLinks = frozenset()
        if perDatasetTypeLinks:
            newLinks = builder.relateDimensions(commonDimensions)
            joinLinks = (commonDimensions.links() & datasetType.dimensions.links()) | newLinks
        else:
            newLinks = frozenset()
            joinLinks = datasetType.dimensions.links()
        if defer:
            if necessity is DatasetNecessityEnum.REQUIRED:
                raise ValueError(f"Cannot defer search for required DatasetType {datasetType.name}.")
            if necessity is DatasetNecessityEnum.OPTIONAL and perDatasetTypeLinks:
                raise ValueError(f"Cannot defer search for optional DatasetType {datasetType.name} "
                                 f"with per-DatasetType links {perDatasetTypeLinks}.")
            self._deferrals[datasetType] = _DeferralData(builder=builder, necessity=necessity,
                                                         joinLinks=joinLinks,
                                                         perDatasetTypeLinks=perDatasetTypeLinks)
        else:
            subquery = builder.build().alias(datasetType.name)
            if commonDimensions is not None and addResultColumns:
                for link in perDatasetTypeLinks:
                    self.resultColumns.addDimensionLink(subquery, link, datasetType=datasetType)
            self.join(subquery, joinLinks, isOuter=(necessity is not DatasetNecessityEnum.REQUIRED))
            self._subqueries[datasetType] = _SubqueryData(subquery=subquery, necessity=necessity,
                                                          links=joinLinks)
            if addResultColumns:
                self.resultColumns.addDatasetId(subquery, datasetType)

    def selectDatasetId(self, datasetType):
        """Add the ``dataset_id`` for the given `DatasetType` to the result
        columns in the SELECT clause of the query.

        Parameters
        ----------
        datasetType : `DatasetType`
            Dataset type for which output IDs should be returned by the query.
            A subquery for this `DatasetType` must have already been added to
            the query via `fromDatasetTypes` or `joinDatasetType`.
        """
        self.resultColumns.addDatasetId(self._subqueries[datasetType].subquery, datasetType)

    def findSelectableForLink(self, link):
        # Docstring inherited from QueryBuilder.findSelectableForLink
        result = super().findSelectableForLink(link)
        if result is None:
            for datasetType, data in self._subqueries.items():
                if data.necessity is DatasetNecessityEnum.REQUIRED and link in data.links:
                    result = data.subquery
                    break
        return result

    def findSelectableByName(self, name):
        # Docstring inherited from QueryBuilder.findSelectableByName
        result = super().findSelectableByName(name)
        if result is None:
            for datasetType, data in self._subqueries.items():
                if name == datasetType.name:
                    result = data.subquery
                    break
        return result

    def convertResultRow(self, managed, *, expandDataIds=True):
        r"""Convert a result row for this query to a `MultipleDatasetQueryRow`.

        Parameters
        ----------
        managed : `ResultColumnsManager.ManagedRow`
            Intermediate result row object to convert.
        expandDataIds : `True`
            If `True` (default), query the registry again to fully populate
            all `DataId`s associated with `DatasetRef`s.  The full-row data
            ID is never expanded.

        Returns
        -------
        row : `MultipleDatasetQueryRow`
            Object containing the `DataId`s and `DatasetRefs` produced by the
            query.
        """
        dataId = managed.makeDataId()
        datasetRefs = {}
        for datasetType, data in self._subqueries.items():
            ref = managed.makeDatasetRef(datasetType, expandDataId=expandDataIds)
            if ref.id is None and data.necessity is DatasetNecessityEnum.PREREQUISITE:
                raise LookupError(f"Search failed for prerequisite dataset "
                                  f"{datasetType.name} associated with data ID {dataId}.")
            datasetRefs[datasetType] = ref
        for datasetType, data in self._deferrals.items():
            # Do the deferred dataset query, by running essentially the same
            # SQL we would have used as a subquery before, but with the
            # JOIN..ON expression now used as a WHERE expression with explicit
            # values from the row's data ID.
            expr = []
            for link in data.joinLinks:
                selectable = data.builder.findSelectableForLink(link)
                column = selectable.columns[link]
                expr.append(column == dataId[link])
            ref = data.builder.executeOne(whereSql=and_(*expr))
            if ref is None:
                if data.necessity is DatasetNecessityEnum.PREREQUISITE:
                    raise LookupError(f"Deferred search failed for prerequisite dataset "
                                      f"{datasetType.name} using data ID {dataId}.")
                assert data.necessity is DatasetNecessityEnum.OPTIONAL, "REQUIRED queries can't be deferred."
                ref = DatasetRef(datasetType,
                                 dataId=managed.makeDataId(datasetType=datasetType,
                                                           expandDataId=expandDataIds))
            datasetRefs[datasetType] = ref
        return MultipleDatasetQueryRow(dataId, datasetRefs)

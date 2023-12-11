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

__all__ = ["ButlerQueryTests"]

import itertools
import os
import re
from abc import ABC, abstractmethod
from collections import defaultdict, namedtuple
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, cast

import astropy.time
import lsst.sphgeom
from lsst.daf.relation import RelationalAlgebraError

from .._dataset_type import DatasetType
from .._exceptions import EmptyQueryResultError
from ..dimensions import DataCoordinate, DataCoordinateSet, SkyPixDimension
from ..registry._collection_type import CollectionType
from ..registry._exceptions import (
    DataIdValueError,
    DatasetTypeError,
    DatasetTypeExpressionError,
    MissingCollectionError,
    MissingDatasetTypeError,
)
from ..transfers import YamlRepoImportBackend
from .utils import TestCaseMixin

if TYPE_CHECKING:
    from .._butler import Butler
    from .._dataset_ref import DatasetRef
    from .._query_results import DataCoordinateQueryResults, DatasetQueryResults, DimensionRecordQueryResults
    from ..dimensions import DimensionGroup, DimensionRecord
    from ..registry.sql_registry import SqlRegistry


class ButlerQueryTests(ABC, TestCaseMixin):
    """Base class for unit tests that test `lsst.daf.butler.Butler.query`
    implementations.
    """

    data_dir: str
    """Root directory containing test data YAML files."""

    @abstractmethod
    def make_butler(self, *args: str) -> Butler:
        """Make Butler instance populated with data used in the tests below.

        Parameters
        ----------
        *args : str
            Names of the files to pass to `load_data`.
        """
        raise NotImplementedError()

    def load_data(self, registry: SqlRegistry, filename: str) -> None:
        """Load registry test data from ``data_dir/<filename>``,
        which should be a YAML import/export file.

        This method should be called from implementations of `make_butler`
        where the Registry should exist.

        Parameters
        ----------
        registry : `SqlRegistry`
            The registry to use.
        filename : `str`
            Location of test data.
        """
        with open(os.path.join(self.data_dir, filename)) as stream:
            backend = YamlRepoImportBackend(stream, registry)
        backend.register()
        backend.load(datastore=None)

    def make_bias_collection(self, registry: SqlRegistry) -> None:
        """Make "biases" collection containing only bias datasets.

        Parameters
        ----------
        registry : `SqlRegistry`
            The registry to use.

        Notes
        -----
        Default test dataset has two collections, each with both flats and
        biases. This adds a new collection for biases, only if "imported_g"
        collection exists (usually loaded from datasets.yaml).

        This method should be called from implementations of `make_butler`
        where the Registry should exist.
        """
        try:
            registry.getCollectionType("imported_g")
        except MissingCollectionError:
            return
        registry.registerCollection("biases", CollectionType.TAGGED)
        registry.associate("biases", registry.queryDatasets("bias", collections=["imported_g"]))

    def test_query_data_ids_convenience(self) -> None:
        """Basic test for `Butler.query_data_ids` method."""
        butler = self.make_butler("base.yaml", "datasets.yaml", "spatial.yaml")

        def _do_query(dimensions: list[str] | str, **kwargs: Any) -> list[DataCoordinate]:
            """Call query_data_ids with some default arguments."""
            return butler._query_data_ids(dimensions, instrument="Cam1", skymap="SkyMap1", **kwargs)

        result = _do_query("visit")
        self.assertEqual(len(result), 2)
        self.assertCountEqual(
            [data_id.mapping for data_id in result],
            [
                {"instrument": "Cam1", "visit": 1, "band": "g", "physical_filter": "Cam1-G"},
                {"instrument": "Cam1", "visit": 2, "band": "r", "physical_filter": "Cam1-R1"},
            ],
        )

        self.assertTrue(all(data_id.hasFull() for data_id in result))
        self.assertFalse(any(data_id.hasRecords() for data_id in result))

        # Test user expression.
        where = "physical_filter = filter_name"
        bind = {"filter_name": "Cam1-G"}
        result = _do_query("visit", where=where, bind=bind)
        self.assertEqual(
            [data_id.mapping for data_id in result],
            [{"instrument": "Cam1", "visit": 1, "band": "g", "physical_filter": "Cam1-G"}],
        )

        # Test chained methods, some modify original result in place, so build
        # new result for each one.
        result = _do_query("visit", order_by="-band")
        self.assertEqual([data_id["visit"] for data_id in result], [2, 1])

        result = _do_query("visit", order_by=("-band",), limit=1)
        self.assertEqual([data_id["visit"] for data_id in result], [2])

        result = _do_query("visit", order_by=("-band",), limit=1, offset=1)
        self.assertEqual([data_id["visit"] for data_id in result], [1])

        with self.assertRaisesRegex(TypeError, "offset is specified without limit"):
            result = _do_query("visit", order_by="-band", offset=1000)

        # Empty result but suppress exception.
        result = _do_query("visit", order_by="-band", limit=1, offset=1000, explain=False)
        self.assertFalse(result)

        # Empty result, will raise an exception.
        with self.assertRaises(EmptyQueryResultError) as exc_cm:
            _do_query("visit", order_by="-band", limit=1, offset=1000)
        self.assertTrue(exc_cm.exception.reasons)

    def test_query_data_ids(self) -> None:
        """Basic test for `Butler.query().data_ids()` method."""
        butler = self.make_butler("base.yaml", "datasets.yaml", "spatial.yaml")

        def _do_query(dimensions: list[str] | str, **kwargs: Any) -> DataCoordinateQueryResults:
            """Call query.data_ids with some default arguments."""
            with butler._query() as query:
                return query.data_ids(dimensions, instrument="Cam1", skymap="SkyMap1", **kwargs)

        result = _do_query("visit")
        self.assertEqual(result.count(), 2)
        self.assertTrue(result.any())
        self.assertCountEqual(
            [data_id.mapping for data_id in result],
            [
                {"instrument": "Cam1", "visit": 1, "band": "g", "physical_filter": "Cam1-G"},
                {"instrument": "Cam1", "visit": 2, "band": "r", "physical_filter": "Cam1-R1"},
            ],
        )

        self.assertTrue(result.has_full())
        self.assertFalse(result.has_records())

        with result.materialize() as materialized:
            result = materialized.expanded()
            self.assertEqual(result.count(), 2)
            self.assertTrue(result.has_records())

        # Test user expression.
        where = "physical_filter = filter_name"
        bind = {"filter_name": "Cam1-G"}
        result = _do_query("visit", where=where, bind=bind)
        self.assertEqual(
            [data_id.mapping for data_id in result],
            [{"instrument": "Cam1", "visit": 1, "band": "g", "physical_filter": "Cam1-G"}],
        )

        # Test chained methods, some modify original result in place, so build
        # new result for each one.
        result = _do_query("visit")
        result = result.order_by("-band")
        self.assertEqual([data_id["visit"] for data_id in result], [2, 1])

        result = _do_query("visit")
        result = result.order_by("-band").limit(1)
        self.assertEqual([data_id["visit"] for data_id in result], [2])

        result = _do_query("visit")
        result = result.order_by("-band").limit(1, 1)
        self.assertEqual([data_id["visit"] for data_id in result], [1])

        result = _do_query("visit")
        result = result.order_by("-band").limit(1, 1000)
        self.assertFalse(result.any())
        self.assertGreater(len(list(result.explain_no_results())), 0)

    def test_query_dimension_records_convenience(self) -> None:
        """Basic test for `Butler.query_dimension_records` method."""
        butler = self.make_butler("base.yaml", "datasets.yaml", "spatial.yaml")

        def _do_query(element: str, **kwargs: Any) -> list[DimensionRecord]:
            """Call query_dimension_records with some default arguments."""
            return butler._query_dimension_records(element, instrument="Cam1", skymap="SkyMap1", **kwargs)

        result = _do_query("visit")
        self.assertEqual(len(result), 2)
        self.assertEqual(
            set((record.id, record.name, record.physical_filter, record.day_obs) for record in result),
            {(1, "1", "Cam1-G", 20210909), (2, "2", "Cam1-R1", 20210909)},
        )

        # Test user expression.
        where = "physical_filter = filter_name"
        bind = {"filter_name": "Cam1-G"}
        result = _do_query("visit", where=where, bind=bind)
        self.assertEqual(len(result), 1)
        self.assertEqual([record.id for record in result], [1])

        result = _do_query("visit", order_by="-visit")
        self.assertEqual([record.id for record in result], [2, 1])

        result = _do_query("visit", order_by=("-visit",), limit=1)
        self.assertEqual([record.id for record in result], [2])

        result = _do_query("visit", order_by=("-visit",), limit=1, offset=1)
        self.assertEqual([record.id for record in result], [1])

        with self.assertRaisesRegex(TypeError, "offset is specified without limit"):
            result = _do_query("visit", order_by="-visit", offset=1000)

        result = _do_query("visit", order_by="-visit", limit=1, offset=1000, explain=False)
        self.assertFalse(result)

        with self.assertRaises(EmptyQueryResultError) as exc_cm:
            _do_query("visit", order_by="-visit", limit=1, offset=1000)
        self.assertTrue(exc_cm.exception.reasons)

    def test_query_dimension_records(self) -> None:
        """Basic test for `_query_dimension_records` method."""
        butler = self.make_butler("base.yaml", "datasets.yaml", "spatial.yaml")

        def _do_query(element: str, **kwargs: Any) -> DimensionRecordQueryResults:
            """Call query.dimension_records with some default arguments."""
            with butler._query() as query:
                return query.dimension_records(element, instrument="Cam1", skymap="SkyMap1", **kwargs)

        result = _do_query("visit")
        self.assertEqual(result.count(), 2)
        self.assertTrue(result.any())
        self.assertEqual(
            set((record.id, record.name, record.physical_filter, record.day_obs) for record in result),
            {(1, "1", "Cam1-G", 20210909), (2, "2", "Cam1-R1", 20210909)},
        )

        # Test user expression.
        where = "physical_filter = filter_name"
        bind = {"filter_name": "Cam1-G"}
        result = _do_query("visit", where=where, bind=bind)
        self.assertEqual(result.count(), 1)
        self.assertEqual([record.id for record in result], [1])

        result = _do_query("visit")
        result = result.order_by("-visit")
        self.assertEqual([record.id for record in result], [2, 1])

        result = _do_query("visit")
        result = result.order_by("-visit").limit(1)
        self.assertEqual([record.id for record in result], [2])

        result = _do_query("visit")
        result = result.order_by("-visit").limit(1, 1)
        self.assertEqual([record.id for record in result], [1])

        result = _do_query("visit")
        result = result.order_by("-visit").limit(1, 1000)
        self.assertFalse(result.any())
        self.assertGreater(len(list(result.explain_no_results())), 0)

    def test_query_datasets_convenience(self) -> None:
        """Basic test for `Butler.query_datasets` method."""
        butler = self.make_butler("base.yaml", "datasets.yaml", "spatial.yaml")

        def _do_query(dataset: Any, **kwargs: Any) -> list[DatasetRef]:
            return butler._query_datasets(dataset, **kwargs)

        result = _do_query(..., collections=["imported_g"])
        self.assertEqual(len(result), 6)
        self.assertCountEqual([ref.dataId["detector"] for ref in result], [1, 2, 3, 2, 3, 4])

        # Test user expression.
        where = "detector IN (detectors) and instrument = instr"
        bind = {"detectors": (2, 3), "instr": "Cam1"}
        result = _do_query(..., collections=..., find_first=False, where=where, bind=bind)
        self.assertEqual(len(result), 8)
        self.assertEqual(set(ref.dataId["detector"] for ref in result), {2, 3})

        where = "detector = 1000000 and instrument = 'Cam1'"
        result = _do_query(..., collections=..., find_first=False, where=where, explain=False)
        self.assertFalse(result)

        with self.assertRaises(EmptyQueryResultError) as exc_cm:
            _do_query(..., collections=..., find_first=False, where=where)
        self.assertTrue(exc_cm.exception.reasons)

    def test_query_datasets(self) -> None:
        """Basic test for `_query_datasets` method."""
        butler = self.make_butler("base.yaml", "datasets.yaml", "spatial.yaml")

        def _do_query(dataset: Any, **kwargs: Any) -> DatasetQueryResults:
            with butler._query() as query:
                return query.datasets(dataset, **kwargs)

        result = _do_query(..., collections=["imported_g"])
        self.assertEqual(result.count(), 6)
        self.assertTrue(result.any())
        self.assertCountEqual([ref.dataId["detector"] for ref in result], [1, 2, 3, 2, 3, 4])

        by_type = list(result.by_dataset_type())
        self.assertEqual(len(by_type), 2)
        self.assertEqual(set(item.dataset_type.name for item in by_type), {"bias", "flat"})

        with result.materialize() as materialized:
            result = materialized.expanded()
            self.assertEqual(result.count(), 6)
            for ref in result:
                self.assertTrue(ref.dataId.hasRecords())

        # Test user expression.
        where = "detector IN (detectors) and instrument = instr"
        bind = {"detectors": (2, 3), "instr": "Cam1"}
        result = _do_query(..., collections=..., find_first=False, where=where, bind=bind)
        self.assertEqual(result.count(), 8)
        self.assertEqual(set(ref.dataId["detector"] for ref in result), {2, 3})

        where = "detector = 1000000 and instrument = 'Cam1'"
        result = _do_query(..., collections=..., find_first=False, where=where, bind=bind)
        self.assertFalse(result.any())
        self.assertGreater(len(list(result.explain_no_results())), 0)

    def test_query_result_summaries(self) -> None:
        """Test summary methods like `count`, `any`, and `explain_no_results`
        on `DataCoordinateQueryResults` and `DatasetQueryResults`.
        """
        # This method was copied almost verbatim from Registry test class,
        # replacing Registry methods with new Butler methods.
        butler = self.make_butler("base.yaml", "datasets.yaml", "spatial.yaml")

        def _query_data_ids(dimensions: list[str] | str, **kwargs: Any) -> DataCoordinateQueryResults:
            with butler._query() as query:
                return query.data_ids(dimensions, **kwargs)

        def _query_datasets(dataset: Any, **kwargs: Any) -> DatasetQueryResults:
            with butler._query() as query:
                return query.datasets(dataset, **kwargs)

        def _query_dimension_records(element: str, **kwargs: Any) -> DimensionRecordQueryResults:
            with butler._query() as query:
                return query.dimension_records(element, **kwargs)

        # First query yields two results, and involves no postprocessing.
        query1 = _query_data_ids(["physical_filter"], band="r")
        self.assertTrue(query1.any(execute=False, exact=False))
        self.assertTrue(query1.any(execute=True, exact=False))
        self.assertTrue(query1.any(execute=True, exact=True))
        self.assertEqual(query1.count(exact=False), 2)
        self.assertEqual(query1.count(exact=True), 2)
        self.assertFalse(list(query1.explain_no_results()))
        # Second query should yield no results, which we should see when
        # we attempt to expand the data ID.
        query2 = _query_data_ids(["physical_filter"], band="h")
        # There's no execute=False, exact=Fals test here because the behavior
        # not something we want to guarantee in this case (and exact=False
        # says either answer is legal).
        self.assertFalse(query2.any(execute=True, exact=False))
        self.assertFalse(query2.any(execute=True, exact=True))
        self.assertEqual(query2.count(exact=False), 0)
        self.assertEqual(query2.count(exact=True), 0)
        self.assertTrue(list(query2.explain_no_results()))
        # These queries yield no results due to various problems that can be
        # spotted prior to execution, yielding helpful diagnostics.
        base_query = _query_data_ids(["detector", "physical_filter"])
        queries_and_snippets: list[Any] = [
            (
                # Dataset type name doesn't match any existing dataset types.
                _query_datasets("nonexistent", collections=..., find_first=False),
                ["nonexistent"],
            ),
            (
                # Dataset type object isn't registered.
                _query_datasets(
                    DatasetType(
                        "nonexistent",
                        dimensions=["instrument"],
                        universe=butler.dimensions,
                        storageClass="Image",
                    ),
                    collections=...,
                    find_first=False,
                ),
                ["nonexistent"],
            ),
            (
                # No datasets of this type in this collection.
                _query_datasets("flat", collections=["biases"]),
                ["flat", "biases"],
            ),
            (
                # No datasets of this type in this collection.
                base_query.find_datasets("flat", collections=["biases"]),
                ["flat", "biases"],
            ),
            (
                # No collections matching at all.
                _query_datasets("flat", collections=re.compile("potato.+"), find_first=False),
                ["potato"],
            ),
        ]

        with self.assertRaises(MissingDatasetTypeError):
            queries_and_snippets.append(
                (
                    # Dataset type name doesn't match any existing dataset
                    # types.
                    _query_data_ids(["detector"], datasets=["nonexistent"], collections=...),
                    ["nonexistent"],
                )
            )
        with self.assertRaises(MissingDatasetTypeError):
            queries_and_snippets.append(
                (
                    # Dataset type name doesn't match any existing dataset
                    # types.
                    _query_dimension_records("detector", datasets=["nonexistent"], collections=...),
                    ["nonexistent"],
                )
            )
        for query, snippets in queries_and_snippets:
            self.assertFalse(query.any(execute=False, exact=False))
            self.assertFalse(query.any(execute=True, exact=False))
            self.assertFalse(query.any(execute=True, exact=True))
            self.assertEqual(query.count(exact=False), 0)
            self.assertEqual(query.count(exact=True), 0)
            messages = list(query.explain_no_results())
            self.assertTrue(messages)
            # Want all expected snippets to appear in at least one message.
            self.assertTrue(
                any(
                    all(snippet in message for snippet in snippets) for message in query.explain_no_results()
                ),
                messages,
            )

        # This query does yield results, but should also emit a warning because
        # dataset type patterns to queryDataIds is deprecated; just look for
        # the warning.
        with self.assertRaises(DatasetTypeExpressionError):
            _query_data_ids(["detector"], datasets=re.compile("^nonexistent$"), collections=...)

        # These queries yield no results due to problems that can be identified
        # by cheap follow-up queries, yielding helpful diagnostics.
        for query, snippets in [
            (
                # No records for one of the involved dimensions.
                _query_data_ids(["subfilter"]),
                ["no rows", "subfilter"],
            ),
            (
                # No records for one of the involved dimensions.
                _query_dimension_records("subfilter"),
                ["no rows", "subfilter"],
            ),
        ]:
            self.assertFalse(query.any(execute=True, exact=False))
            self.assertFalse(query.any(execute=True, exact=True))
            self.assertEqual(query.count(exact=True), 0)
            messages = list(query.explain_no_results())
            self.assertTrue(messages)
            # Want all expected snippets to appear in at least one message.
            self.assertTrue(
                any(
                    all(snippet in message for snippet in snippets) for message in query.explain_no_results()
                ),
                messages,
            )

        # This query yields four overlaps in the database, but one is filtered
        # out in postprocessing.  The count queries aren't accurate because
        # they don't account for duplication that happens due to an internal
        # join against commonSkyPix.
        query3 = _query_data_ids(["visit", "tract"], instrument="Cam1", skymap="SkyMap1")
        self.assertEqual(
            {
                DataCoordinate.standardize(
                    instrument="Cam1",
                    skymap="SkyMap1",
                    visit=v,
                    tract=t,
                    universe=butler.dimensions,
                )
                for v, t in [(1, 0), (2, 0), (2, 1)]
            },
            set(query3),
        )
        self.assertTrue(query3.any(execute=False, exact=False))
        self.assertTrue(query3.any(execute=True, exact=False))
        self.assertTrue(query3.any(execute=True, exact=True))
        self.assertGreaterEqual(query3.count(exact=False), 4)
        self.assertGreaterEqual(query3.count(exact=True, discard=True), 3)
        self.assertFalse(list(query3.explain_no_results()))
        # This query yields overlaps in the database, but all are filtered
        # out in postprocessing.  The count queries again aren't very useful.
        # We have to use `where=` here to avoid an optimization that
        # (currently) skips the spatial postprocess-filtering because it
        # recognizes that no spatial join is necessary.  That's not ideal, but
        # fixing it is out of scope for this ticket.
        query4 = _query_data_ids(
            ["visit", "tract"],
            instrument="Cam1",
            skymap="SkyMap1",
            where="visit=1 AND detector=1 AND tract=0 AND patch=4",
        )
        self.assertFalse(set(query4))
        self.assertTrue(query4.any(execute=False, exact=False))
        self.assertTrue(query4.any(execute=True, exact=False))
        self.assertFalse(query4.any(execute=True, exact=True))
        self.assertGreaterEqual(query4.count(exact=False), 1)
        self.assertEqual(query4.count(exact=True, discard=True), 0)
        messages = list(query4.explain_no_results())
        self.assertTrue(messages)
        self.assertTrue(any("overlap" in message for message in messages))
        # This query should yield results from one dataset type but not the
        # other, which is not registered.
        query5 = _query_datasets(["bias", "nonexistent"], collections=["biases"])
        self.assertTrue(set(query5))
        self.assertTrue(query5.any(execute=False, exact=False))
        self.assertTrue(query5.any(execute=True, exact=False))
        self.assertTrue(query5.any(execute=True, exact=True))
        self.assertGreaterEqual(query5.count(exact=False), 1)
        self.assertGreaterEqual(query5.count(exact=True), 1)
        self.assertFalse(list(query5.explain_no_results()))
        # This query applies a selection that yields no results, fully in the
        # database.  Explaining why it fails involves traversing the relation
        # tree and running a LIMIT 1 query at each level that has the potential
        # to remove rows.
        query6 = _query_dimension_records(
            "detector", where="detector.purpose = 'no-purpose'", instrument="Cam1"
        )
        self.assertEqual(query6.count(exact=True), 0)
        messages = list(query6.explain_no_results())
        self.assertTrue(messages)
        self.assertTrue(any("no-purpose" in message for message in messages))

    def test_query_results(self) -> None:
        """Test querying for data IDs and then manipulating the QueryResults
        object returned to perform other queries.
        """
        # This method was copied almost verbatim from Registry test class,
        # replacing Registry methods with new Butler methods.
        butler = self.make_butler("base.yaml", "datasets.yaml")

        def _query_data_ids(dimensions: list[str] | str, **kwargs: Any) -> DataCoordinateQueryResults:
            with butler._query() as query:
                return query.data_ids(dimensions, **kwargs)

        bias = butler.get_dataset_type("bias")
        flat = butler.get_dataset_type("flat")
        # Obtain expected results from methods other than those we're testing
        # here.  That includes:
        # - the dimensions of the data IDs we want to query:
        expected_dimensions = butler.dimensions.conform(["detector", "physical_filter"])
        # - the dimensions of some other data IDs we'll extract from that:
        expected_subset_dimensions = butler.dimensions.conform(["detector"])
        # - the data IDs we expect to obtain from the first queries:
        expectedDataIds = DataCoordinateSet(
            {
                DataCoordinate.standardize(
                    instrument="Cam1", detector=d, physical_filter=p, universe=butler.dimensions
                )
                for d, p in itertools.product({1, 2, 3}, {"Cam1-G", "Cam1-R1", "Cam1-R2"})
            },
            dimensions=expected_dimensions,
            hasFull=False,
            hasRecords=False,
        )
        # - the flat datasets we expect to find from those data IDs, in just
        #   one collection (so deduplication is irrelevant):
        expectedFlats = [
            butler.find_dataset(
                flat, instrument="Cam1", detector=1, physical_filter="Cam1-R1", collections="imported_r"
            ),
            butler.find_dataset(
                flat, instrument="Cam1", detector=2, physical_filter="Cam1-R1", collections="imported_r"
            ),
            butler.find_dataset(
                flat, instrument="Cam1", detector=3, physical_filter="Cam1-R2", collections="imported_r"
            ),
        ]
        # - the data IDs we expect to extract from that:
        expectedSubsetDataIds = expectedDataIds.subset(expected_subset_dimensions)
        # - the bias datasets we expect to find from those data IDs, after we
        #   subset-out the physical_filter dimension, both with duplicates:
        expectedAllBiases = [
            ref
            for ref in [
                butler.find_dataset(bias, instrument="Cam1", detector=1, collections="imported_g"),
                butler.find_dataset(bias, instrument="Cam1", detector=2, collections="imported_g"),
                butler.find_dataset(bias, instrument="Cam1", detector=3, collections="imported_g"),
                butler.find_dataset(bias, instrument="Cam1", detector=2, collections="imported_r"),
                butler.find_dataset(bias, instrument="Cam1", detector=3, collections="imported_r"),
            ]
            if ref is not None
        ]
        # - ...and without duplicates:
        expectedDeduplicatedBiases = [
            butler.find_dataset(bias, instrument="Cam1", detector=1, collections="imported_g"),
            butler.find_dataset(bias, instrument="Cam1", detector=2, collections="imported_r"),
            butler.find_dataset(bias, instrument="Cam1", detector=3, collections="imported_r"),
        ]
        # Test against those expected results, using a "lazy" query for the
        # data IDs (which re-executes that query each time we use it to do
        # something new).
        dataIds = _query_data_ids(
            ["detector", "physical_filter"],
            where="detector.purpose = 'SCIENCE'",  # this rejects detector=4
            instrument="Cam1",
        )
        self.assertEqual(dataIds.dimensions, expected_dimensions)
        self.assertEqual(set(dataIds), set(expectedDataIds))
        self.assertCountEqual(
            list(
                dataIds.find_datasets(
                    flat,
                    collections=["imported_r"],
                )
            ),
            expectedFlats,
        )
        subsetDataIds = dataIds.subset(expected_subset_dimensions, unique=True)
        self.assertEqual(subsetDataIds.dimensions, expected_subset_dimensions)
        self.assertEqual(set(subsetDataIds), set(expectedSubsetDataIds))
        self.assertCountEqual(
            list(
                subsetDataIds.find_datasets(bias, collections=["imported_r", "imported_g"], find_first=False)
            ),
            expectedAllBiases,
        )
        self.assertCountEqual(
            list(
                subsetDataIds.find_datasets(bias, collections=["imported_r", "imported_g"], find_first=True)
            ),
            expectedDeduplicatedBiases,
        )

        # Searching for a dataset with dimensions we had projected away
        # restores those dimensions.
        self.assertCountEqual(
            list(subsetDataIds.find_datasets("flat", collections=["imported_r"], find_first=True)),
            expectedFlats,
        )

        # Use a component dataset type.
        self.assertCountEqual(
            [
                ref.makeComponentRef("image")
                for ref in subsetDataIds.find_datasets(
                    bias,
                    collections=["imported_r", "imported_g"],
                    find_first=False,
                )
            ],
            [ref.makeComponentRef("image") for ref in expectedAllBiases],
        )

        # Use a named dataset type that does not exist and a dataset type
        # object that does not exist.
        unknown_type = DatasetType("not_known", dimensions=bias.dimensions, storageClass="Exposure")

        # Test both string name and dataset type object.
        tests: tuple[tuple[DatasetType | str, str], ...] = (
            (unknown_type, unknown_type.name),
            (unknown_type.name, unknown_type.name),
        )
        for test_type, test_type_name in tests:
            with self.assertRaisesRegex(DatasetTypeError, expected_regex=test_type_name):
                list(
                    subsetDataIds.find_datasets(
                        test_type, collections=["imported_r", "imported_g"], find_first=True
                    )
                )

        # Materialize the bias dataset queries (only) by putting the results
        # into temporary tables, then repeat those tests.
        with subsetDataIds.find_datasets(
            bias, collections=["imported_r", "imported_g"], find_first=False
        ).materialize() as biases:
            self.assertCountEqual(list(biases), expectedAllBiases)
        with subsetDataIds.find_datasets(
            bias, collections=["imported_r", "imported_g"], find_first=True
        ).materialize() as biases:
            self.assertCountEqual(list(biases), expectedDeduplicatedBiases)
        # Materialize the data ID subset query, but not the dataset queries.
        with subsetDataIds.materialize() as subsetDataIds:
            self.assertEqual(subsetDataIds.dimensions, expected_subset_dimensions)
            self.assertEqual(set(subsetDataIds), set(expectedSubsetDataIds))
            self.assertCountEqual(
                list(
                    subsetDataIds.find_datasets(
                        bias, collections=["imported_r", "imported_g"], find_first=False
                    )
                ),
                expectedAllBiases,
            )
            self.assertCountEqual(
                list(
                    subsetDataIds.find_datasets(
                        bias, collections=["imported_r", "imported_g"], find_first=True
                    )
                ),
                expectedDeduplicatedBiases,
            )
            # Materialize the dataset queries, too.
            with subsetDataIds.find_datasets(
                bias, collections=["imported_r", "imported_g"], find_first=False
            ).materialize() as biases:
                self.assertCountEqual(list(biases), expectedAllBiases)
            with subsetDataIds.find_datasets(
                bias, collections=["imported_r", "imported_g"], find_first=True
            ).materialize() as biases:
                self.assertCountEqual(list(biases), expectedDeduplicatedBiases)
        # Materialize the original query, but none of the follow-up queries.
        with dataIds.materialize() as dataIds:
            self.assertEqual(dataIds.dimensions, expected_dimensions)
            self.assertEqual(set(dataIds), set(expectedDataIds))
            self.assertCountEqual(
                list(
                    dataIds.find_datasets(
                        flat,
                        collections=["imported_r"],
                    )
                ),
                expectedFlats,
            )
            subsetDataIds = dataIds.subset(expected_subset_dimensions, unique=True)
            self.assertEqual(subsetDataIds.dimensions, expected_subset_dimensions)
            self.assertEqual(set(subsetDataIds), set(expectedSubsetDataIds))
            self.assertCountEqual(
                list(
                    subsetDataIds.find_datasets(
                        bias, collections=["imported_r", "imported_g"], find_first=False
                    )
                ),
                expectedAllBiases,
            )
            self.assertCountEqual(
                list(
                    subsetDataIds.find_datasets(
                        bias, collections=["imported_r", "imported_g"], find_first=True
                    )
                ),
                expectedDeduplicatedBiases,
            )
            # Materialize just the bias dataset queries.
            with subsetDataIds.find_datasets(
                bias, collections=["imported_r", "imported_g"], find_first=False
            ).materialize() as biases:
                self.assertCountEqual(list(biases), expectedAllBiases)
            with subsetDataIds.find_datasets(
                bias, collections=["imported_r", "imported_g"], find_first=True
            ).materialize() as biases:
                self.assertCountEqual(list(biases), expectedDeduplicatedBiases)
            # Materialize the subset data ID query, but not the dataset
            # queries.
            with subsetDataIds.materialize() as subsetDataIds:
                self.assertEqual(subsetDataIds.dimensions, expected_subset_dimensions)
                self.assertEqual(set(subsetDataIds), set(expectedSubsetDataIds))
                self.assertCountEqual(
                    list(
                        subsetDataIds.find_datasets(
                            bias, collections=["imported_r", "imported_g"], find_first=False
                        )
                    ),
                    expectedAllBiases,
                )
                self.assertCountEqual(
                    list(
                        subsetDataIds.find_datasets(
                            bias, collections=["imported_r", "imported_g"], find_first=True
                        )
                    ),
                    expectedDeduplicatedBiases,
                )
                # Materialize the bias dataset queries, too, so now we're
                # materializing every single step.
                with subsetDataIds.find_datasets(
                    bias, collections=["imported_r", "imported_g"], find_first=False
                ).materialize() as biases:
                    self.assertCountEqual(list(biases), expectedAllBiases)
                with subsetDataIds.find_datasets(
                    bias, collections=["imported_r", "imported_g"], find_first=True
                ).materialize() as biases:
                    self.assertCountEqual(list(biases), expectedDeduplicatedBiases)

    def test_query_datasets_deduplication(self) -> None:
        """Test that the findFirst option to query.datasets selects datasets
        from collections in the order given".
        """
        # This method was copied almost verbatim from Registry test class,
        # replacing Registry methods with new Butler methods.
        butler = self.make_butler("base.yaml", "datasets.yaml")

        def _query_datasets(dataset: Any, **kwargs: Any) -> DatasetQueryResults:
            with butler._query() as query:
                return query.datasets(dataset, **kwargs)

        self.assertCountEqual(
            list(_query_datasets("bias", collections=["imported_g", "imported_r"], find_first=False)),
            [
                butler.find_dataset("bias", instrument="Cam1", detector=1, collections="imported_g"),
                butler.find_dataset("bias", instrument="Cam1", detector=2, collections="imported_g"),
                butler.find_dataset("bias", instrument="Cam1", detector=3, collections="imported_g"),
                butler.find_dataset("bias", instrument="Cam1", detector=2, collections="imported_r"),
                butler.find_dataset("bias", instrument="Cam1", detector=3, collections="imported_r"),
                butler.find_dataset("bias", instrument="Cam1", detector=4, collections="imported_r"),
            ],
        )
        self.assertCountEqual(
            list(_query_datasets("bias", collections=["imported_g", "imported_r"], find_first=True)),
            [
                butler.find_dataset("bias", instrument="Cam1", detector=1, collections="imported_g"),
                butler.find_dataset("bias", instrument="Cam1", detector=2, collections="imported_g"),
                butler.find_dataset("bias", instrument="Cam1", detector=3, collections="imported_g"),
                butler.find_dataset("bias", instrument="Cam1", detector=4, collections="imported_r"),
            ],
        )
        self.assertCountEqual(
            list(_query_datasets("bias", collections=["imported_r", "imported_g"], find_first=True)),
            [
                butler.find_dataset("bias", instrument="Cam1", detector=1, collections="imported_g"),
                butler.find_dataset("bias", instrument="Cam1", detector=2, collections="imported_r"),
                butler.find_dataset("bias", instrument="Cam1", detector=3, collections="imported_r"),
                butler.find_dataset("bias", instrument="Cam1", detector=4, collections="imported_r"),
            ],
        )

    def test_query_data_ids_order_by(self) -> None:
        """Test order_by and limit on result returned by query.data_ids()."""
        # This method was copied almost verbatim from Registry test class,
        # replacing Registry methods with new Butler methods.
        butler = self.make_butler("base.yaml", "datasets.yaml", "spatial.yaml")

        def do_query(
            dimensions: Iterable[str] = ("visit", "tract"), datasets: Any = None, collections: Any = None
        ) -> DataCoordinateQueryResults:
            with butler._query() as query:
                return query.data_ids(
                    dimensions,
                    datasets=datasets,
                    collections=collections,
                    instrument="Cam1",
                    skymap="SkyMap1",
                )

        Test = namedtuple(
            "Test",
            ("order_by", "keys", "result", "limit", "datasets", "collections"),
            defaults=(None, None, None),
        )

        test_data = (
            Test("tract,visit", "tract,visit", ((0, 1), (0, 1), (0, 2), (0, 2), (1, 2), (1, 2))),
            Test("-tract,visit", "tract,visit", ((1, 2), (1, 2), (0, 1), (0, 1), (0, 2), (0, 2))),
            Test("tract,-visit", "tract,visit", ((0, 2), (0, 2), (0, 1), (0, 1), (1, 2), (1, 2))),
            Test("-tract,-visit", "tract,visit", ((1, 2), (1, 2), (0, 2), (0, 2), (0, 1), (0, 1))),
            Test(
                "tract.id,visit.id",
                "tract,visit",
                ((0, 1), (0, 1), (0, 2)),
                limit=(3,),
            ),
            Test("-tract,-visit", "tract,visit", ((1, 2), (1, 2), (0, 2)), limit=(3,)),
            Test("tract,visit", "tract,visit", ((0, 2), (1, 2), (1, 2)), limit=(3, 3)),
            Test("-tract,-visit", "tract,visit", ((0, 1),), limit=(3, 5)),
            Test(
                "tract,visit.exposure_time", "tract,visit", ((0, 2), (0, 2), (0, 1), (0, 1), (1, 2), (1, 2))
            ),
            Test(
                "-tract,-visit.exposure_time", "tract,visit", ((1, 2), (1, 2), (0, 1), (0, 1), (0, 2), (0, 2))
            ),
            Test("tract,-exposure_time", "tract,visit", ((0, 1), (0, 1), (0, 2), (0, 2), (1, 2), (1, 2))),
            Test("tract,visit.name", "tract,visit", ((0, 1), (0, 1), (0, 2), (0, 2), (1, 2), (1, 2))),
            Test(
                "tract,-timespan.begin,timespan.end",
                "tract,visit",
                ((0, 2), (0, 2), (0, 1), (0, 1), (1, 2), (1, 2)),
            ),
            Test("visit.day_obs,exposure.day_obs", "visit,exposure", ()),
            Test("visit.timespan.begin,-exposure.timespan.begin", "visit,exposure", ()),
            Test(
                "tract,detector",
                "tract,detector",
                ((0, 1), (0, 2), (0, 3), (0, 4), (1, 1), (1, 2), (1, 3), (1, 4)),
                datasets="flat",
                collections="imported_r",
            ),
            Test(
                "tract,detector.full_name",
                "tract,detector",
                ((0, 1), (0, 2), (0, 3), (0, 4), (1, 1), (1, 2), (1, 3), (1, 4)),
                datasets="flat",
                collections="imported_r",
            ),
            Test(
                "tract,detector.raft,detector.name_in_raft",
                "tract,detector",
                ((0, 1), (0, 2), (0, 3), (0, 4), (1, 1), (1, 2), (1, 3), (1, 4)),
                datasets="flat",
                collections="imported_r",
            ),
        )

        for test in test_data:
            order_by = test.order_by.split(",")
            keys = test.keys.split(",")
            query = do_query(keys, test.datasets, test.collections).order_by(*order_by)
            if test.limit is not None:
                query = query.limit(*test.limit)
            dataIds = tuple(tuple(dataId[k] for k in keys) for dataId in query)
            self.assertEqual(dataIds, test.result)

            # and materialize
            query = do_query(keys).order_by(*order_by)
            if test.limit is not None:
                query = query.limit(*test.limit)
            with self.assertRaises(RelationalAlgebraError):
                with query.materialize():
                    pass  # pragma: no cover

        # errors in a name
        for order_by in ("", "-"):
            with self.assertRaisesRegex(ValueError, "Empty dimension name in ORDER BY"):
                list(do_query().order_by(order_by))

        for order_by in ("undimension.name", "-undimension.name"):
            with self.assertRaisesRegex(ValueError, "Unknown dimension element 'undimension'"):
                list(do_query().order_by(order_by))

        for order_by in ("attract", "-attract"):
            with self.assertRaisesRegex(ValueError, "Metadata 'attract' cannot be found in any dimension"):
                list(do_query().order_by(order_by))

        with self.assertRaisesRegex(ValueError, "Metadata 'exposure_time' exists in more than one dimension"):
            list(do_query(("exposure", "visit")).order_by("exposure_time"))

        with self.assertRaisesRegex(
            ValueError,
            r"Timespan exists in more than one dimension element \(exposure, visit\); "
            r"qualify timespan with specific dimension name\.",
        ):
            list(do_query(("exposure", "visit")).order_by("timespan.begin"))

        with self.assertRaisesRegex(
            ValueError, "Cannot find any temporal dimension element for 'timespan.begin'"
        ):
            list(do_query("tract").order_by("timespan.begin"))

        with self.assertRaisesRegex(ValueError, "Cannot use 'timespan.begin' with non-temporal element"):
            list(do_query("tract").order_by("tract.timespan.begin"))

        with self.assertRaisesRegex(ValueError, "Field 'name' does not exist in 'tract'."):
            list(do_query("tract").order_by("tract.name"))

        with self.assertRaisesRegex(
            ValueError, r"Unknown dimension element 'timestamp'; perhaps you meant 'timespan.begin'\?"
        ):
            list(do_query("visit").order_by("timestamp.begin"))

    def test_query_int_range_expressions(self) -> None:
        """Test integer range expressions in ``where`` arguments.

        Note that our expressions use inclusive stop values, unlike Python's.
        """
        butler = self.make_butler("base.yaml")

        def _query_data_ids(dimensions: list[str] | str, **kwargs: Any) -> DataCoordinateQueryResults:
            with butler._query() as query:
                return query.data_ids(dimensions, **kwargs)

        self.assertEqual(
            set(_query_data_ids(["detector"], instrument="Cam1", where="detector IN (1..2)")),
            {butler.registry.expandDataId(instrument="Cam1", detector=n) for n in [1, 2]},
        )
        self.assertEqual(
            set(_query_data_ids(["detector"], instrument="Cam1", where="detector IN (1..4:2)")),
            {butler.registry.expandDataId(instrument="Cam1", detector=n) for n in [1, 3]},
        )
        self.assertEqual(
            set(_query_data_ids(["detector"], instrument="Cam1", where="detector IN (2..4:2)")),
            {butler.registry.expandDataId(instrument="Cam1", detector=n) for n in [2, 4]},
        )

    def test_query_data_ids_expression_error(self) -> None:
        """Test error checking of 'where' expressions in query.data_ids."""
        butler = self.make_butler("base.yaml")

        def _query_data_ids(dimensions: list[str] | str, **kwargs: Any) -> DataCoordinateQueryResults:
            with butler._query() as query:
                return query.data_ids(dimensions, **kwargs)

        bind = {"time": astropy.time.Time("2020-01-01T01:00:00", format="isot", scale="tai")}
        with self.assertRaisesRegex(LookupError, r"No dimension element with name 'foo' in 'foo\.bar'\."):
            _query_data_ids(["detector"], where="foo.bar = 12")
        with self.assertRaisesRegex(
            LookupError, "Dimension element name cannot be inferred in this context."
        ):
            _query_data_ids(["detector"], where="timespan.end < time", bind=bind)

    def test_query_data_ids_governor_exceptions(self) -> None:
        """Test exceptions raised by query.data_ids for incorrect governors."""
        butler = self.make_butler("base.yaml", "datasets.yaml", "spatial.yaml")

        def _query_data_ids(dimensions: list[str] | str, **kwargs: Any) -> DataCoordinateQueryResults:
            with butler._query() as query:
                return query.data_ids(dimensions, **kwargs)

        Test = namedtuple(
            "Test",
            ("dimensions", "dataId", "where", "bind", "kwargs", "exception", "count"),
            defaults=(None, None, None, {}, None, 0),
        )

        test_data = (
            Test("tract,visit", count=6),
            Test("tract,visit", kwargs={"instrument": "Cam1", "skymap": "SkyMap1"}, count=6),
            Test(
                "tract,visit", kwargs={"instrument": "Cam2", "skymap": "SkyMap1"}, exception=DataIdValueError
            ),
            Test("tract,visit", dataId={"instrument": "Cam1", "skymap": "SkyMap1"}, count=6),
            Test(
                "tract,visit", dataId={"instrument": "Cam1", "skymap": "SkyMap2"}, exception=DataIdValueError
            ),
            Test("tract,visit", where="instrument='Cam1' AND skymap='SkyMap1'", count=6),
            Test("tract,visit", where="instrument='Cam1' AND skymap='SkyMap5'", exception=DataIdValueError),
            Test(
                "tract,visit",
                where="instrument=cam AND skymap=map",
                bind={"cam": "Cam1", "map": "SkyMap1"},
                count=6,
            ),
            Test(
                "tract,visit",
                where="instrument=cam AND skymap=map",
                bind={"cam": "Cam", "map": "SkyMap"},
                exception=DataIdValueError,
            ),
        )

        for test in test_data:
            dimensions = test.dimensions.split(",")
            if test.exception:
                with self.assertRaises(test.exception):
                    _query_data_ids(
                        dimensions, data_id=test.dataId, where=test.where, bind=test.bind, **test.kwargs
                    ).count()
            else:
                query = _query_data_ids(
                    dimensions, data_id=test.dataId, where=test.where, bind=test.bind, **test.kwargs
                )
                self.assertEqual(query.count(discard=True), test.count)

            # and materialize
            if test.exception:
                with self.assertRaises(test.exception):
                    query = _query_data_ids(
                        dimensions, data_id=test.dataId, where=test.where, bind=test.bind, **test.kwargs
                    )
            else:
                query = _query_data_ids(
                    dimensions, data_id=test.dataId, where=test.where, bind=test.bind, **test.kwargs
                )
                with query.materialize() as materialized:
                    self.assertEqual(materialized.count(discard=True), test.count)

    def test_query_dimension_records_exceptions(self) -> None:
        """Test exceptions raised by query.dimension_records()."""
        butler = self.make_butler("base.yaml", "datasets.yaml", "spatial.yaml")

        def _query_dimension_records(element: str, **kwargs: Any) -> DimensionRecordQueryResults:
            with butler._query() as query:
                return query.dimension_records(element, **kwargs)

        result = _query_dimension_records("detector")
        self.assertEqual(result.count(), 4)
        result = _query_dimension_records("detector", instrument="Cam1")
        self.assertEqual(result.count(), 4)
        result = _query_dimension_records("detector", data_id={"instrument": "Cam1"})
        self.assertEqual(result.count(), 4)
        result = _query_dimension_records("detector", where="instrument='Cam1'")
        self.assertEqual(result.count(), 4)
        result = _query_dimension_records("detector", where="instrument=instr", bind={"instr": "Cam1"})
        self.assertEqual(result.count(), 4)

        with self.assertRaisesRegex(DataIdValueError, "dimension instrument"):
            result = _query_dimension_records("detector", instrument="NotCam1")

        with self.assertRaisesRegex(DataIdValueError, "dimension instrument"):
            result = _query_dimension_records("detector", data_id={"instrument": "NotCam1"})

        with self.assertRaisesRegex(DataIdValueError, "Unknown values specified for governor dimension"):
            result = _query_dimension_records("detector", where="instrument='NotCam1'")

        with self.assertRaisesRegex(DataIdValueError, "Unknown values specified for governor dimension"):
            result = _query_dimension_records("detector", where="instrument=instr", bind={"instr": "NotCam1"})

    def test_query_dimension_records_order_by(self) -> None:
        """Test order_by and limit on result returned by
        query.dimension_records().
        """
        butler = self.make_butler("base.yaml", "datasets.yaml", "spatial.yaml")

        def _query_dimension_records(element: str, **kwargs: Any) -> DimensionRecordQueryResults:
            with butler._query() as query:
                return query.dimension_records(element, **kwargs)

        def do_query(
            element: str, datasets: Any = None, collections: Any = None
        ) -> DimensionRecordQueryResults:
            return _query_dimension_records(
                element, instrument="Cam1", datasets=datasets, collections=collections
            )

        query = do_query("detector")
        self.assertEqual(len(list(query)), 4)

        Test = namedtuple(
            "Test",
            ("element", "order_by", "result", "limit", "datasets", "collections"),
            defaults=(None, None, None),
        )

        test_data = (
            Test("detector", "detector", (1, 2, 3, 4)),
            Test("detector", "-detector", (4, 3, 2, 1)),
            Test("detector", "raft,-name_in_raft", (2, 1, 4, 3)),
            Test("detector", "-detector.purpose", (4,), limit=(1,)),
            Test("detector", "-purpose,detector.raft,name_in_raft", (2, 3), limit=(2, 2)),
            Test("visit", "visit", (1, 2)),
            Test("visit", "-visit.id", (2, 1)),
            Test("visit", "zenith_angle", (1, 2)),
            Test("visit", "-visit.name", (2, 1)),
            Test("visit", "day_obs,-timespan.begin", (2, 1)),
        )

        for test in test_data:
            order_by = test.order_by.split(",")
            query = do_query(test.element).order_by(*order_by)
            if test.limit is not None:
                query = query.limit(*test.limit)
            dataIds = tuple(rec.id for rec in query)
            self.assertEqual(dataIds, test.result)

        # errors in a name
        for order_by in ("", "-"):
            with self.assertRaisesRegex(ValueError, "Empty dimension name in ORDER BY"):
                list(do_query("detector").order_by(order_by))

        for order_by in ("undimension.name", "-undimension.name"):
            with self.assertRaisesRegex(ValueError, "Element name mismatch: 'undimension'"):
                list(do_query("detector").order_by(order_by))

        for order_by in ("attract", "-attract"):
            with self.assertRaisesRegex(ValueError, "Field 'attract' does not exist in 'detector'."):
                list(do_query("detector").order_by(order_by))

        for order_by in ("timestamp.begin", "-timestamp.begin"):
            with self.assertRaisesRegex(
                ValueError,
                r"Element name mismatch: 'timestamp' instead of 'visit'; "
                r"perhaps you meant 'timespan.begin'\?",
            ):
                list(do_query("visit").order_by(order_by))

    def test_skypix_constraint_queries(self) -> None:
        """Test queries spatially constrained by a skypix data ID."""
        butler = self.make_butler("hsc-rc2-subset.yaml")

        def _query_data_ids(dimensions: list[str] | str, **kwargs: Any) -> DataCoordinateQueryResults:
            with butler._query() as query:
                return query.data_ids(dimensions, **kwargs)

        patch_regions = {
            (data_id["tract"], data_id["patch"]): data_id.region
            for data_id in _query_data_ids(["patch"]).expanded()
            if data_id.region is not None
        }
        skypix_dimension = cast(SkyPixDimension, butler.dimensions["htm11"])
        # This check ensures the test doesn't become trivial due to a config
        # change; if it does, just pick a different HTML level.
        self.assertNotEqual(skypix_dimension, butler.dimensions.commonSkyPix)
        # Gather all skypix IDs that definitely overlap at least one of these
        # patches.
        relevant_skypix_ids = lsst.sphgeom.RangeSet()
        for patch_region in patch_regions.values():
            relevant_skypix_ids |= skypix_dimension.pixelization.interior(patch_region)
        # Look for a "nontrivial" skypix_id that overlaps at least one patch
        # and does not overlap at least one other patch.
        for skypix_id in itertools.chain.from_iterable(
            range(begin, end) for begin, end in relevant_skypix_ids
        ):
            skypix_region = skypix_dimension.pixelization.pixel(skypix_id)
            overlapping_patches = {
                patch_key
                for patch_key, patch_region in patch_regions.items()
                if not patch_region.isDisjointFrom(skypix_region)
            }
            if overlapping_patches and overlapping_patches != patch_regions.keys():
                break
        else:
            raise RuntimeError("Could not find usable skypix ID for this dimension configuration.")
        self.assertEqual(
            {
                (data_id["tract"], data_id["patch"])
                for data_id in _query_data_ids(
                    ["patch"],
                    data_id={skypix_dimension.name: skypix_id},
                )
            },
            overlapping_patches,
        )
        # Test that a three-way join that includes the common skypix system in
        # the dimensions doesn't generate redundant join terms in the query.
        full_data_ids = set(
            _query_data_ids(["tract", "visit", "htm7"], skymap="hsc_rings_v1", instrument="HSC").expanded()
        )
        self.assertGreater(len(full_data_ids), 0)
        for data_id in full_data_ids:
            tract = data_id.records["tract"]
            visit = data_id.records["visit"]
            htm7 = data_id.records["htm7"]
            assert tract is not None and visit is not None and htm7 is not None
            self.assertFalse(tract.region.isDisjointFrom(htm7.region))
            self.assertFalse(visit.region.isDisjointFrom(htm7.region))

    def test_bind_in_query_datasets(self) -> None:
        """Test that the bind parameter is correctly forwarded in
        query.datasets recursion.
        """
        butler = self.make_butler("base.yaml", "datasets.yaml")

        def _query_datasets(dataset: Any, **kwargs: Any) -> DatasetQueryResults:
            with butler._query() as query:
                return query.datasets(dataset, **kwargs)

        # Importing datasets from yaml should go through the code path where
        # we update collection summaries as we insert datasets.
        self.assertEqual(
            set(_query_datasets("flat", band="r", collections=..., find_first=False)),
            set(
                _query_datasets(
                    "flat", where="band=my_band", bind={"my_band": "r"}, collections=..., find_first=False
                )
            ),
        )

    def test_dataset_constrained_dimension_record_queries(self) -> None:
        """Test that query.dimension_records works even when given a dataset
        constraint whose dimensions extend beyond the requested dimension
        element's.
        """
        butler = self.make_butler("base.yaml", "datasets.yaml")

        def _query_dimension_records(element: str, **kwargs: Any) -> DimensionRecordQueryResults:
            with butler._query() as query:
                return query.dimension_records(element, **kwargs)

        # Query for physical_filter dimension records, using a dataset that
        # has both physical_filter and dataset dimensions.
        records = _query_dimension_records(
            "physical_filter",
            datasets=["flat"],
            collections="imported_r",
        )
        self.assertEqual({record.name for record in records}, {"Cam1-R1", "Cam1-R2"})
        # Trying to constrain by all dataset types is an error.
        with self.assertRaises(TypeError):
            list(_query_dimension_records("physical_filter", datasets=..., collections="imported_r"))

    def test_exposure_queries(self) -> None:
        """Test query methods using arguments sourced from the exposure log
        service.

        The most complete test dataset currently available to daf_butler tests
        is hsc-rc2-subset.yaml export (which is unfortunately distinct from the
        the lsst/rc2_subset GitHub repo), but that does not have 'exposure'
        dimension records as it was focused on providing nontrivial spatial
        overlaps between visit+detector and tract+patch.  So in this test we
        need to translate queries that originally used the exposure dimension
        to use the (very similar) visit dimension instead.
        """
        butler = self.make_butler("hsc-rc2-subset.yaml")

        def _query_data_ids(dimensions: list[str] | str, **kwargs: Any) -> DataCoordinateQueryResults:
            with butler._query() as query:
                return query.data_ids(dimensions, **kwargs)

        def _query_dimension_records(element: str, **kwargs: Any) -> DimensionRecordQueryResults:
            with butler._query() as query:
                return query.dimension_records(element, **kwargs)

        self.assertEqual(
            [
                record.id
                for record in _query_dimension_records("visit", instrument="HSC").order_by("id").limit(5)
            ],
            [318, 322, 326, 330, 332],
        )
        self.assertEqual(
            [
                data_id["visit"]
                for data_id in _query_data_ids(["visit"], instrument="HSC").order_by("id").limit(5)
            ],
            [318, 322, 326, 330, 332],
        )
        self.assertEqual(
            [
                record.id
                for record in _query_dimension_records("detector", instrument="HSC")
                .order_by("full_name")
                .limit(5)
            ],
            [73, 72, 71, 70, 65],
        )
        self.assertEqual(
            [
                data_id["detector"]
                for data_id in _query_data_ids(["detector"], instrument="HSC").order_by("full_name").limit(5)
            ],
            [73, 72, 71, 70, 65],
        )

    def test_spatial_join(self) -> None:
        """Test queries that involve spatial overlap joins."""
        butler = self.make_butler("hsc-rc2-subset.yaml")

        def _query_data_ids(
            dimensions: DimensionGroup | list[str] | str, **kwargs: Any
        ) -> DataCoordinateQueryResults:
            with butler._query() as query:
                return query.data_ids(dimensions, **kwargs)

        def _query_dimension_records(element: str, **kwargs: Any) -> DimensionRecordQueryResults:
            with butler._query() as query:
                return query.dimension_records(element, **kwargs)

        # Dictionary of spatial DatabaseDimensionElements, keyed by the name of
        # the TopologicalFamily they belong to.  We'll relate all elements in
        # each family to all of the elements in each other family.
        families = defaultdict(set)
        # Dictionary of {element.name: {dataId: region}}.
        regions = {}
        for element in butler.dimensions.database_elements:
            if element.spatial is not None:
                families[element.spatial.name].add(element)
                regions[element.name] = {
                    record.dataId: record.region for record in _query_dimension_records(element.name)
                }

        # If this check fails, it's not necessarily a problem - it may just be
        # a reasonable change to the default dimension definitions - but the
        # test below depends on there being more than one family to do anything
        # useful.
        self.assertEqual(len(families), 2)

        # Overlap DatabaseDimensionElements with each other.
        for family1, family2 in itertools.combinations(families, 2):
            for element1, element2 in itertools.product(families[family1], families[family2]):
                dimensions = element1.minimal_group | element2.minimal_group
                # Construct expected set of overlapping data IDs via a
                # brute-force comparison of the regions we've already fetched.
                expected = {
                    DataCoordinate.standardize(
                        {**dataId1.required, **dataId2.required}, dimensions=dimensions
                    )
                    for (dataId1, region1), (dataId2, region2) in itertools.product(
                        regions[element1.name].items(), regions[element2.name].items()
                    )
                    if not region1.isDisjointFrom(region2)
                }
                self.assertGreater(len(expected), 2, msg="Test that we aren't just comparing empty sets.")
                queried = set(_query_data_ids(dimensions))
                self.assertEqual(expected, queried)

        # Overlap each DatabaseDimensionElement with the commonSkyPix system.
        commonSkyPix = butler.dimensions.commonSkyPix
        for elementName, these_regions in regions.items():
            dimensions = butler.dimensions[elementName].minimal_group | commonSkyPix.minimal_group
            expected = set()
            for dataId, region in these_regions.items():
                for begin, end in commonSkyPix.pixelization.envelope(region):
                    expected.update(
                        DataCoordinate.standardize(
                            {commonSkyPix.name: index, **dataId.required}, dimensions=dimensions
                        )
                        for index in range(begin, end)
                    )
            self.assertGreater(len(expected), 2, msg="Test that we aren't just comparing empty sets.")
            queried = set(_query_data_ids(dimensions))
            self.assertEqual(expected, queried)

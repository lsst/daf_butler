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

import os
import unittest
from abc import ABC, abstractmethod
from collections.abc import Iterable, Sequence
from operator import attrgetter
from typing import ClassVar
from uuid import UUID

import astropy.coordinates
import astropy.time
from numpy import int64

from lsst.sphgeom import LonLat, Region

from .._butler import Butler
from .._collection_type import CollectionType
from .._dataset_ref import DatasetRef
from .._dataset_type import DatasetType
from .._exceptions import (
    EmptyQueryResultError,
    InvalidQueryError,
    MissingCollectionError,
    MissingDatasetTypeError,
)
from .._timespan import Timespan
from ..dimensions import DataCoordinate, DimensionRecord
from ..direct_query_driver import DirectQueryDriver
from ..queries import DimensionRecordQueryResults, Query
from ..queries.tree import Predicate
from ..registry import NoDefaultCollectionError, RegistryDefaults
from .utils import TestCaseMixin

# Simplified tuples of the detector records we'll frequently be querying for.
DETECTOR_TUPLES = {
    1: ("Cam1", 1, "Aa", "SCIENCE"),
    2: ("Cam1", 2, "Ab", "SCIENCE"),
    3: ("Cam1", 3, "Ba", "SCIENCE"),
    4: ("Cam1", 4, "Bb", "WAVEFRONT"),
}


def make_detector_tuples(records: Iterable[DimensionRecord]) -> dict[int, tuple[str, int, str, str]]:
    """Make tuples with the same entries as DETECTOR_TUPLES from an iterable of
    detector dimension records.

    Parameters
    ----------
    records : `~collections.abc.Iterable` [ `.dimensions.DimensionRecord` ]
        Detector dimension records.

    Returns
    -------
    tuples : `dict` [ `int`, `tuple` ]
        Dictionary mapping detector ID to tuples with the same fields as the
        ``DETECTOR_TUPLES`` constant in this file.
    """
    return {record.id: (record.instrument, record.id, record.full_name, record.purpose) for record in records}


class ButlerQueryTests(ABC, TestCaseMixin):
    """Base class for unit tests that test `lsst.daf.butler.Butler.query`
    implementations.
    """

    data_dir: ClassVar[str]
    """Root directory containing test data YAML files."""

    @abstractmethod
    def make_butler(self, *args: str) -> Butler:
        """Make Butler instance populated with data used in the tests below.

        Parameters
        ----------
        *args : str
            Names of the files to pass to `load_data`.

        Returns
        -------
        butler : `Butler`
            Butler to use for tests.
        """
        raise NotImplementedError()

    def load_data(self, butler: Butler, filename: str) -> None:
        """Load registry test data from ``data_dir/<filename>``,
        which should be a YAML import/export file.

        This method should be called from implementations of `make_butler`
        where the Registry should exist.

        Parameters
        ----------
        butler : `~lsst.daf.butler.Butler`
            The butler to use.
        filename : `str`
            Location of test data.
        """
        butler.import_(
            filename=os.path.join(self.data_dir, filename),
            without_datastore=True,
        )

    def check_detector_records(
        self,
        results: DimensionRecordQueryResults,
        ids: Sequence[int] = (1, 2, 3, 4),
        ordered: bool = False,
        messages: Iterable[str] = (),
        doomed: bool = False,
        has_postprocessing: bool = False,
    ) -> None:
        self.assertEqual(results.element.name, "detector")
        self.assertEqual(results.dimensions, results.dimensions.universe["detector"].minimal_group)
        if has_postprocessing and not doomed:
            self.assertEqual(results.count(discard=True), len(ids))
            self.assertGreaterEqual(results.count(discard=False, exact=False), len(ids))
            with self.assertRaisesRegex(InvalidQueryError, "^Cannot count query rows"):
                results.count()
        else:
            self.assertEqual(results.count(discard=True), len(ids))
            self.assertEqual(results.count(discard=False), len(ids))
            self.assertEqual(results.count(discard=True, exact=False), len(ids))
            self.assertEqual(results.count(discard=False, exact=False), len(ids))
        self.assertEqual(results.any(), bool(ids))
        if not doomed:
            self.assertTrue(results.any(exact=False, execute=False))
            with self.assertRaisesRegex(InvalidQueryError, "^Cannot obtain exact"):
                results.any(exact=True, execute=False)
        else:
            self.assertFalse(results.any(exact=False, execute=False))
            self.assertFalse(results.any(exact=True, execute=False))
        self.assertCountEqual(results.explain_no_results(), list(messages))
        self.check_detector_records_returned(list(results), ids=ids, ordered=ordered)

    def check_detector_records_returned(
        self,
        results: list[DimensionRecord],
        ids: Sequence[int] = (1, 2, 3, 4),
        ordered: bool = False,
    ) -> None:
        expected = [DETECTOR_TUPLES[i] for i in ids]
        queried = list(make_detector_tuples(results).values())
        if ordered:
            self.assertEqual(queried, expected)
        else:
            self.assertCountEqual(queried, expected)

    def test_simple_record_query(self) -> None:
        """Test query-system basics with simple queries for dimension
        records.

        This includes tests for order_by, limit, and where expressions, but
        only for cases where there are no datasets, dimension projections,
        or spatial/temporal overlaps.
        """
        butler = self.make_butler("base.yaml")
        with butler.query() as query:
            _x = query.expression_factory
            results = query.dimension_records("detector")
            self.check_detector_records(results)
            self.check_detector_records_returned(butler.query_dimension_records("detector"))
            self.assertEqual(len(butler.query_dimension_records("detector", limit=0)), 0)
            self.check_detector_records(results.order_by("detector"), ordered=True)
            self.check_detector_records_returned(
                butler.query_dimension_records("detector", order_by="detector"), ordered=True
            )
            self.check_detector_records(
                results.order_by(_x.detector.full_name.desc), [4, 3, 2, 1], ordered=True
            )
            self.check_detector_records_returned(
                butler.query_dimension_records("detector", order_by="-full_name"),
                ids=[4, 3, 2, 1],
                ordered=True,
            )
            self.check_detector_records(results.order_by("detector").limit(2), [1, 2], ordered=True)
            self.check_detector_records_returned(
                butler.query_dimension_records("detector", limit=2, order_by="detector"),
                ids=[1, 2],
                ordered=True,
            )
            with self.assertLogs("lsst.daf.butler", level="WARNING") as wcm:
                self.check_detector_records_returned(
                    butler.query_dimension_records("detector", limit=-2, order_by="-detector"),
                    ids=[4, 3],
                    ordered=True,
                )
            self.assertIn("More dimension records are available", wcm.output[0])
            self.check_detector_records(results.where(_x.detector.raft == "B", instrument="Cam1"), [3, 4])
            self.check_detector_records_returned(
                butler.query_dimension_records(
                    "detector", where="detector.raft = R", bind={"R": "B"}, instrument="Cam1"
                ),
                ids=[3, 4],
            )

    def test_simple_data_coordinate_query(self) -> None:
        butler = self.make_butler("base.yaml")

        expected_detectors = [1, 2, 3, 4]
        universe = butler.dimensions
        expected_coordinates = [
            DataCoordinate.standardize({"instrument": "Cam1", "detector": x}, universe=universe)
            for x in expected_detectors
        ]

        with butler.query() as query:
            # Test empty query
            empty = DataCoordinate.make_empty(butler.dimensions)
            self.assertCountEqual(list(query.data_ids([])), [empty])
            self.assertCountEqual(butler.query_data_ids([]), [empty])

            # Test query for a single dimension
            results = query.data_ids(["detector"])
            self.assertCountEqual(list(results), expected_coordinates)

            # Limit.
            results = query.data_ids(["detector"]).order_by("-detector").limit(2)
            self.assertCountEqual(list(results), expected_coordinates[2:])

        data_ids = butler.query_data_ids("detector")
        self.assertCountEqual(data_ids, expected_coordinates)

        data_ids = butler.query_data_ids("detector", order_by="-detector", limit=2)
        self.assertCountEqual(data_ids, expected_coordinates[2:])

        with self.assertLogs("lsst.daf.butler", level="WARNING") as wcm:
            data_ids = butler.query_data_ids("detector", order_by="-detector", limit=-2)
        self.assertCountEqual(data_ids, expected_coordinates[2:])
        self.assertIn("More data IDs are available", wcm.output[0])

        data_ids = butler.query_data_ids("detector", limit=0)
        self.assertEqual(len(data_ids), 0)

    def test_simple_dataset_query(self) -> None:
        butler = self.make_butler("base.yaml", "datasets.yaml")
        with butler.query() as query:
            refs_q = list(query.datasets("bias", "imported_g").order_by("detector"))
        refs_simple = butler.query_datasets("bias", "imported_g", order_by="detector")
        self.assertCountEqual(refs_q, refs_simple)

        for refs in (refs_q, refs_simple):
            self.assertEqual(len(refs), 3)
            self.assertEqual(refs[0].id, UUID("e15ab039-bc8b-4135-87c5-90902a7c0b22"))
            self.assertEqual(refs[1].id, UUID("51352db4-a47a-447c-b12d-a50b206b17cd"))
            for detector, ref in enumerate(refs, 1):
                self.assertEqual(ref.datasetType.name, "bias")
                self.assertEqual(ref.dataId["instrument"], "Cam1")
                self.assertEqual(ref.dataId["detector"], detector)
                self.assertEqual(ref.run, "imported_g")

        # Try again with limit.
        with butler.query() as query:
            refs_q = list(query.datasets("bias", "imported_g").order_by("detector").limit(2))
        refs_simple = butler.query_datasets("bias", "imported_g", order_by="detector", limit=2)
        self.assertCountEqual(refs_q, refs_simple)
        self.assertEqual(len(refs_q), 2)
        self.assertEqual(refs_q[0].id, UUID("e15ab039-bc8b-4135-87c5-90902a7c0b22"))
        self.assertEqual(refs_q[1].id, UUID("51352db4-a47a-447c-b12d-a50b206b17cd"))

        # limit=0 means test the query but don't return anything and
        # don't complain.
        refs_simple = butler.query_datasets("bias", "imported_g", limit=0, explain=True)
        self.assertEqual(len(refs_simple), 0)

        # Explicitly run with no restrictions.
        refs_simple = butler.query_datasets("bias", collections="*", find_first=False, limit=None)
        self.assertEqual(len(refs_simple), 6)

        # Now limit the number of results and look for a warning.
        with self.assertLogs("lsst.daf.butler", level="WARNING") as lcm:
            refs_simple = butler.query_datasets("bias", collections="*", find_first=False, limit=-4)
        self.assertEqual(len(refs_simple), 4)
        self.assertIn("More datasets are available", lcm.output[0])

        with self.assertRaises(InvalidQueryError) as cm:
            butler.query_datasets("bias", "*", detector=100, instrument="Unknown", find_first=True)
        self.assertIn("Can not use wildcards", str(cm.exception))
        with self.assertRaises(EmptyQueryResultError) as cm2:
            butler.query_datasets("bias", "*", detector=100, instrument="Unknown", find_first=False)
        self.assertIn("doomed", str(cm2.exception))

        # Test for a regression of an issue where "band" was not being included
        # in the data ID, despite being one of the dimensions in the "flat"
        # dataset type.
        #
        # "band" is implied by "physical_filter", so it's technically not a
        # 'required' dimension.  However, the contract of query_datasets is
        # that hasFull() should be true, so implied dimensions must be
        # included.
        refs = butler.query_datasets("flat", "imported_r", where="detector = 2", instrument="Cam1")
        self.assertEqual(len(refs), 1)
        flat = refs[0]
        self.assertTrue(flat.dataId.hasFull())
        self.assertEqual(flat.datasetType.name, "flat")
        self.assertEqual(flat.dataId["instrument"], "Cam1")
        self.assertEqual(flat.dataId["detector"], 2)
        self.assertEqual(flat.dataId["physical_filter"], "Cam1-R1")
        self.assertEqual(flat.dataId["band"], "r")

    def test_general_query(self) -> None:
        """Test Query.general and its result."""
        butler = self.make_butler("base.yaml", "datasets.yaml")
        dimensions = butler.dimensions["detector"].minimal_group

        # Do simple dimension queries.
        with butler.query() as query:
            query = query.join_dimensions(dimensions)
            rows = list(query.general(dimensions).order_by("detector"))
            self.assertEqual(
                rows,
                [
                    {"instrument": "Cam1", "detector": 1},
                    {"instrument": "Cam1", "detector": 2},
                    {"instrument": "Cam1", "detector": 3},
                    {"instrument": "Cam1", "detector": 4},
                ],
            )
            rows = list(
                query.general(dimensions, "detector.full_name", "purpose").order_by(
                    "-detector.purpose", "full_name"
                )
            )
            self.assertEqual(
                rows,
                [
                    {
                        "instrument": "Cam1",
                        "detector": 4,
                        "detector.full_name": "Bb",
                        "detector.purpose": "WAVEFRONT",
                    },
                    {
                        "instrument": "Cam1",
                        "detector": 1,
                        "detector.full_name": "Aa",
                        "detector.purpose": "SCIENCE",
                    },
                    {
                        "instrument": "Cam1",
                        "detector": 2,
                        "detector.full_name": "Ab",
                        "detector.purpose": "SCIENCE",
                    },
                    {
                        "instrument": "Cam1",
                        "detector": 3,
                        "detector.full_name": "Ba",
                        "detector.purpose": "SCIENCE",
                    },
                ],
            )
            rows = list(
                query.general(dimensions, "detector.full_name", "purpose").where(
                    "instrument = 'Cam1' AND purpose = 'WAVEFRONT'"
                )
            )
            self.assertEqual(
                rows,
                [
                    {
                        "instrument": "Cam1",
                        "detector": 4,
                        "detector.full_name": "Bb",
                        "detector.purpose": "WAVEFRONT",
                    },
                ],
            )
            result = query.general(dimensions, dimension_fields={"detector": {"full_name"}})
            self.assertEqual(set(row["detector.full_name"] for row in result), {"Aa", "Ab", "Ba", "Bb"})

        # Use "flat" whose dimension group includes implied dimension.
        flat = butler.get_dataset_type("flat")
        dimensions = butler.dimensions.conform(["detector", "physical_filter"])

        # Do simple dataset queries in RUN collection.
        with butler.query() as query:
            query = query.join_dataset_search("flat", "imported_g")
            # This just returns data IDs.
            rows = list(query.general(dimensions).order_by("detector"))
            self.assertEqual(
                rows,
                [
                    {"instrument": "Cam1", "detector": 2, "physical_filter": "Cam1-G", "band": "g"},
                    {"instrument": "Cam1", "detector": 3, "physical_filter": "Cam1-G", "band": "g"},
                    {"instrument": "Cam1", "detector": 4, "physical_filter": "Cam1-G", "band": "g"},
                ],
            )

            result = query.general(dimensions, dataset_fields={"flat": ...}, find_first=True).order_by(
                "detector"
            )
            ids = {row["flat.dataset_id"] for row in result}
            self.assertEqual(
                ids,
                {
                    UUID("60c8a65c-7290-4c38-b1de-e3b1cdcf872d"),
                    UUID("84239e7f-c41f-46d5-97b9-a27976b98ceb"),
                    UUID("fd51bce1-2848-49d6-a378-f8a122f5139a"),
                },
            )

            # Check what iter_tuples() returns
            row_tuples = list(result.iter_tuples(flat))
            self.assertEqual(len(row_tuples), 3)
            for row_tuple in row_tuples:
                self.assertEqual(len(row_tuple.refs), 1)
                self.assertEqual(row_tuple.refs[0].datasetType, flat)
                self.assertTrue(row_tuple.refs[0].dataId.hasFull())
                self.assertFalse(row_tuple.refs[0].dataId.hasRecords())
                self.assertTrue(row_tuple.data_id.hasFull())
                self.assertFalse(row_tuple.data_id.hasRecords())
                self.assertEqual(row_tuple.data_id.dimensions, dimensions)
                self.assertEqual(row_tuple.raw_row["flat.run"], "imported_g")

            flat1, flat2, flat3 = (row_tuple.refs[0] for row_tuple in row_tuples)

        # Query datasets CALIBRATION/TAGGED collections.
        butler.registry.registerCollection("tagged", CollectionType.TAGGED)
        butler.registry.registerCollection("calib", CollectionType.CALIBRATION)

        # Add two refs to tagged collection.
        butler.registry.associate("tagged", [flat1, flat2])

        # Certify some calibs.
        t1 = astropy.time.Time("2020-01-01T01:00:00", format="isot", scale="tai")
        t2 = astropy.time.Time("2020-01-01T02:00:00", format="isot", scale="tai")
        t3 = astropy.time.Time("2020-01-01T03:00:00", format="isot", scale="tai")
        butler.registry.certify("calib", [flat1], Timespan(t1, t2))
        butler.registry.certify("calib", [flat3], Timespan(t2, t3))
        butler.registry.certify("calib", [flat1], Timespan(t3, None))
        butler.registry.certify("calib", [flat2], Timespan.makeEmpty())

        # Query tagged collection.
        with butler.query() as query:
            query = query.join_dataset_search("flat", ["tagged"])

            result = query.general(
                dimensions, "flat.dataset_id", "flat.run", "flat.collection", find_first=False
            )
            row_tuples = list(result.iter_tuples(flat))
            self.assertEqual(len(row_tuples), 2)
            self.assertEqual({row_tuple.refs[0] for row_tuple in row_tuples}, {flat1, flat2})
            self.assertEqual({row_tuple.raw_row["flat.collection"] for row_tuple in row_tuples}, {"tagged"})

        # Query calib collection.
        with butler.query() as query:
            query = query.join_dataset_search("flat", ["calib"])
            result = query.general(
                dimensions,
                "flat.dataset_id",
                "flat.run",
                "flat.collection",
                "flat.timespan",
                find_first=False,
            )
            row_tuples = list(result.iter_tuples(flat))
            self.assertEqual(len(row_tuples), 4)
            self.assertEqual({row_tuple.refs[0] for row_tuple in row_tuples}, {flat1, flat2, flat3})
            self.assertEqual({row_tuple.raw_row["flat.collection"] for row_tuple in row_tuples}, {"calib"})
            self.assertEqual(
                {row_tuple.raw_row["flat.timespan"] for row_tuple in row_tuples},
                {Timespan(t1, t2), Timespan(t2, t3), Timespan(t3, None), Timespan.makeEmpty()},
            )

        # Query both tagged and calib collection.
        with butler.query() as query:
            query = query.join_dataset_search("flat", ["tagged", "calib"])
            result = query.general(
                dimensions,
                "flat.dataset_id",
                "flat.run",
                "flat.collection",
                "flat.timespan",
                find_first=False,
            )
            row_tuples = list(result.iter_tuples(flat))
            self.assertEqual(len(row_tuples), 6)
            self.assertEqual(
                {row_tuple.raw_row["flat.collection"] for row_tuple in row_tuples}, {"calib", "tagged"}
            )
            self.assertEqual(
                {row_tuple.raw_row["flat.timespan"] for row_tuple in row_tuples},
                {Timespan(t1, t2), Timespan(t2, t3), Timespan(t3, None), Timespan.makeEmpty(), None},
            )

        dimensions = butler.dimensions["detector"].minimal_group

        # Include dimension records into query.
        with butler.query() as query:
            query = query.join_dimensions(dimensions)
            result = query.general(dimensions).order_by("detector")
            rows = list(result.with_dimension_records())
            self.assertEqual(
                rows[0],
                {
                    "instrument": "Cam1",
                    "detector": 1,
                    "instrument.visit_max": 1024,
                    "instrument.visit_system": 1,
                    "instrument.exposure_max": 512,
                    "instrument.detector_max": 4,
                    "instrument.class_name": "lsst.pipe.base.Instrument",
                    "detector.full_name": "Aa",
                    "detector.name_in_raft": "a",
                    "detector.raft": "A",
                    "detector.purpose": "SCIENCE",
                },
            )

        dimensions = butler.dimensions.conform(["detector", "physical_filter"])

        # DataIds should come with records.
        with butler.query() as query:
            query = query.join_dataset_search("flat", "imported_g")
            result = query.general(dimensions, dataset_fields={"flat": ...}, find_first=True).order_by(
                "detector"
            )
            result = result.with_dimension_records()
            row_tuples = list(result.iter_tuples(flat))
            self.assertEqual(len(row_tuples), 3)
            for row_tuple in row_tuples:
                self.assertTrue(row_tuple.data_id.hasRecords())
                self.assertEqual(len(row_tuple.refs), 1)
                self.assertTrue(row_tuple.refs[0].dataId.hasRecords())

    def test_query_ingest_date(self) -> None:
        """Test general query returning ingest_date field."""
        before_ingest = astropy.time.Time.now()
        butler = self.make_butler("base.yaml", "datasets.yaml")
        dimensions = butler.dimensions.conform(["detector", "physical_filter"])

        # Check that returned type of ingest_date is astropy Time, must work
        # for schema versions 1 and 2 of datasets manager.
        with butler.query() as query:
            query = query.join_dataset_search("flat", "imported_g")
            rows = list(query.general(dimensions, dataset_fields={"flat": {"ingest_date"}}, find_first=False))
            self.assertEqual(len(rows), 3)
            for row in rows:
                self.assertIsInstance(row["flat.ingest_date"], astropy.time.Time)

        # Check that WHERE accepts astropy time
        with butler.query() as query:
            query = query.join_dataset_search("flat", "imported_g")
            query1 = query.where("flat.ingest_date < before_ingest", bind={"before_ingest": before_ingest})
            rows = list(query1.general(dimensions))
            self.assertEqual(len(rows), 0)
            query1 = query.where("flat.ingest_date >= before_ingest", bind={"before_ingest": before_ingest})
            rows = list(query1.general(dimensions))
            self.assertEqual(len(rows), 3)
            # Same with a time in string literal.
            query1 = query.where(f"flat.ingest_date < T'mjd/{before_ingest.tai.mjd}'")
            rows = list(query1.general(dimensions))
            self.assertEqual(len(rows), 0)

    def test_implied_union_record_query(self) -> None:
        """Test queries for a dimension ('band') that uses "implied union"
        storage, in which its values are the union of the values for it in a
        another dimension (physical_filter) that implies it.
        """
        butler = self.make_butler("base.yaml")
        band = butler.dimensions["band"]
        self.assertEqual(band.implied_union_target, butler.dimensions["physical_filter"])
        with butler.query() as query:
            self.assertCountEqual(
                list(query.dimension_records("band")),
                [band.RecordClass(name="g"), band.RecordClass(name="r")],
            )
            self.assertCountEqual(
                list(query.where(physical_filter="Cam1-R1", instrument="Cam1").dimension_records("band")),
                [band.RecordClass(name="r")],
            )
        self.assertCountEqual(
            butler.query_dimension_records("band"),
            [band.RecordClass(name="g"), band.RecordClass(name="r")],
        )
        self.assertCountEqual(
            butler.query_dimension_records("band", physical_filter="Cam1-R1", instrument="Cam1"),
            [band.RecordClass(name="r")],
        )

    def test_dataset_constrained_record_query(self) -> None:
        """Test a query for dimension records constrained by the existence of
        datasets of a particular type.
        """
        butler = self.make_butler("base.yaml", "datasets.yaml")
        butler.registry.insertDimensionData("instrument", {"name": "Cam2"})
        butler.collections.register("empty", CollectionType.RUN)
        butler.collections.register("chain", CollectionType.CHAINED)
        butler.collections.redefine_chain("chain", ["imported_g", "empty", "imported_r"])
        with butler.query() as query:
            # No collections here or in defaults is an error.
            with self.assertRaises(NoDefaultCollectionError):
                query.join_dataset_search("bias").dimension_records("detector").any()
        butler.registry.defaults = RegistryDefaults(collections=["chain"])
        with butler.query() as query:
            _x = query.expression_factory
            # Simplest case: this collection only has the first 3 detectors.
            self.check_detector_records(
                query.join_dataset_search("bias", collections=["imported_g"]).dimension_records("detector"),
                [1, 2, 3],
            )
            # Together these collections have two biases for two of the
            # detectors, but this shouldn't cause duplicate results.
            self.check_detector_records(
                query.join_dataset_search("bias", collections=["imported_g", "imported_r"]).dimension_records(
                    "detector"
                ),
            )
            # Again we've got the potential for duplicates due to multiple
            # datasets with the same data ID, and this time we force the
            # deduplication to happen outside the dataset-search subquery by
            # adding a WHERE filter on a dataset column.  We also use the
            # defaulted collection ('chain') to supply the collection.
            self.check_detector_records(
                query.join_dataset_search("bias")
                .where(
                    _x.any(
                        _x.all(_x["bias"].collection == "imported_g", _x.detector.raft == "B"),
                        _x.all(_x["bias"].collection == "imported_r", _x.detector.raft == "A"),
                    ),
                    instrument="Cam1",
                )
                .dimension_records("detector"),
                [2, 3],
            )
            # Flats have dimensions (physical_filter and band) we would
            # normally include in query for detector records.  This also should
            # not cause duplicates.
            self.check_detector_records(
                query.join_dataset_search("flat", collections=["imported_g"]).dimension_records("detector"),
                [2, 3, 4],
            )
            # No results, but for reasons we can't determine before we run the
            # query.
            self.check_detector_records(
                query.join_dataset_search("flat", collections=["imported_g"])
                .where(_x.band == "r")
                .dimension_records("detector"),
                [],
            )
            # No results, and we can diagnose why before we run the query.
            self.check_detector_records(
                query.join_dataset_search("bias", collections=["empty"]).dimension_records("detector"),
                [],
                messages=[
                    "Search for dataset type 'bias' in ['empty'] is doomed to fail.",
                    "No datasets of type 'bias' in collection 'empty'.",
                ],
                doomed=True,
            )
            self.check_detector_records(
                query.join_dataset_search("bias", collections=["imported_g"])
                .where(instrument="Cam2")
                .dimension_records("detector"),
                [],
                messages=[
                    "Search for dataset type 'bias' in ['imported_g'] is doomed to fail.",
                    "No datasets with instrument='Cam2' in collection 'imported_g'.",
                ],
                doomed=True,
            )

    def test_duplicate_overlaps(self) -> None:
        """Test for query option that enables duplicate rows in queries that
        use skypix overalps.
        """
        butler = self.make_butler("base.yaml", "spatial.yaml")
        butler.registry.defaults = RegistryDefaults(instrument="Cam1", skymap="SkyMap1")
        with butler.query() as query:
            data_ids = list(query.data_ids(["visit", "detector", "patch"]).where(visit=1, detector=1))
            self.assertCountEqual(
                [(data_id["tract"], data_id["patch"]) for data_id in data_ids], [(0, 0), (0, 2)]
            )

            query._allow_duplicate_overlaps = True
            data_ids = list(query.data_ids(["visit", "detector", "patch"]).where(visit=1, detector=1))
            self.assertCountEqual(
                [(data_id["tract"], data_id["patch"]) for data_id in data_ids],
                [(0, 0), (0, 0), (0, 2), (0, 2)],
            )

    def test_spatial_overlaps(self) -> None:
        """Test queries for dimension records with spatial overlaps.

        Run tests/data/registry/spatial.py to plot the various regions used in
        this test.
        """
        butler = self.make_butler("base.yaml", "spatial.yaml")
        # Set default governor data ID values both to test that code path and
        # to keep us from having to repeat them in every 'where' call below.
        butler.registry.defaults = RegistryDefaults(instrument="Cam1", skymap="SkyMap1")
        htm7 = butler.dimensions.skypix_dimensions["htm7"]
        with butler.query() as query:
            _x = query.expression_factory
            # Query for detectors from a particular visit that overlap an
            # explicit region.
            self.check_detector_records(
                query.where(
                    _x.visit_detector_region.region.overlaps(htm7.pixelization.pixel(253954)),
                    visit=1,
                ).dimension_records("detector"),
                [1, 3, 4],
                has_postprocessing=True,
            )
            self.check_detector_records_returned(
                butler.query_dimension_records(
                    "detector",
                    where="visit_detector_region.region OVERLAPS region",
                    bind={"region": htm7.pixelization.pixel(253954)},
                    visit=1,
                ),
                ids=[1, 3, 4],
            )
            # Query for detectors from a particular visit that overlap an htm7
            # ID.  This is basically the same query as the last one, but
            # expressed as a spatial join, and we can recognize that
            # postprocessing is not needed (while in the last case it did
            # nothing, but we couldn't tell that in advance because the query
            # didn't know the region came from htm7).
            self.check_detector_records(
                query.where(
                    _x.visit_detector_region.region.overlaps(_x.htm7.region),
                    visit=1,
                    htm7=253954,
                ).dimension_records("detector"),
                [1, 3, 4],
                has_postprocessing=False,
            )
            # Repeat the last query but with the spatial join implicit rather
            # than explicit.
            self.check_detector_records(
                query.where(
                    visit=1,
                    htm7=253954,
                ).dimension_records("detector"),
                [1, 3, 4],
                has_postprocessing=False,
            )
            self.check_detector_records_returned(
                butler.query_dimension_records(
                    "detector",
                    visit=1,
                    htm7=253954,
                ),
                ids=[1, 3, 4],
            )
            # Query for the detectors from any visit that overlap a region:
            # this gets contributions from multiple visits, and would have
            # duplicates if we didn't get rid of them via GROUP BY.
            self.check_detector_records(
                query.where(
                    _x.visit_detector_region.region.overlaps(htm7.pixelization.pixel(253954)),
                ).dimension_records("detector"),
                [1, 2, 3, 4],
                has_postprocessing=True,
            )
            self.check_detector_records_returned(
                butler.query_dimension_records(
                    "detector",
                    where="visit_detector_region.region OVERLAPS region",
                    bind={"region": htm7.pixelization.pixel(253954)},
                ),
                ids=[1, 2, 3, 4],
            )
            # Once again we rewrite the region-constraint query as a spatial
            # join, which drops the postprocessing.  This join has to be
            # explicit because `visit` no longer gets into the query dimensions
            # some other way, and without it `detector` is not spatial.
            self.check_detector_records(
                query.where(
                    _x.visit_detector_region.region.overlaps(_x.htm7.region),
                    htm7=253954,
                ).dimension_records("detector"),
                [1, 2, 3, 4],
                has_postprocessing=False,
            )
            # Query for detectors from any visit that overlap a patch. This
            # requires joining visit_detector_region to htm7 and htm7 to patch,
            # and then some postprocessing.  We want to make sure there are no
            # duplicates from a detector and patch both overlapping multiple
            # htm7 tiles (which affects detectors 1 and 2) and that
            # postprocessing filters out detector 4, which has one htm7 tile in
            # common with the patch but does not actually overlap it.
            self.check_detector_records(
                query.where(
                    _x.visit_detector_region.region.overlaps(_x.patch.region),
                    tract=0,
                    patch=4,
                ).dimension_records("detector"),
                [1, 2, 3],
                has_postprocessing=True,
            )
            # Same as above, but with a materialization.
            self.check_detector_records(
                query.where(
                    _x.visit_detector_region.region.overlaps(_x.patch.region),
                    tract=0,
                    patch=4,
                )
                .materialize(dimensions=["detector"])
                .dimension_records("detector"),
                [1, 2, 3],
                has_postprocessing=True,
            )
            # Query for that patch's region and express the previous query as
            # a region-constraint instead of a spatial join.
            (patch_record,) = query.where(tract=0, patch=4).dimension_records("patch")
            self.check_detector_records(
                query.where(
                    _x.visit_detector_region.region.overlaps(patch_record.region),
                ).dimension_records("detector"),
                [1, 2, 3],
                has_postprocessing=True,
            )
            self.check_detector_records_returned(
                butler.query_dimension_records(
                    "detector",
                    where="visit_detector_region.region OVERLAPS region",
                    bind={"region": patch_record.region},
                ),
                ids=[1, 2, 3],
            )
            # Query for detectors where a patch/visit+detector overlap is
            # satisfied, in the case where there are no rows with an overlap,
            # but the union of the patch regions overlaps the union of the
            # visit+detector regions.
            self.check_detector_records(
                query.where(
                    _x.visit_detector_region.region.overlaps(_x.patch.region),
                    _x.any(
                        _x.all(_x.tract == 1, _x.visit == 1),
                        _x.all(_x.tract == 0, _x.patch == 0, _x.visit == 2),
                    ),
                ).dimension_records("detector"),
                [],
                has_postprocessing=True,
            )
            # Combine postprocessing with order_by and limit.
            self.check_detector_records(
                query.where(
                    _x.visit_detector_region.region.overlaps(patch_record.region),
                )
                .dimension_records("detector")
                .order_by(_x.detector.desc)
                .limit(2),
                [3, 2],
                has_postprocessing=True,
            )
            self.check_detector_records_returned(
                butler.query_dimension_records(
                    "detector",
                    where="visit_detector_region.region OVERLAPS region",
                    bind={"region": patch_record.region},
                    order_by="-detector",
                    limit=2,
                ),
                ids=[3, 2],
            )
            # Try a case where there are some records before postprocessing but
            # none afterwards.
            self.check_detector_records(
                query.where(
                    _x.visit_detector_region.region.overlaps(patch_record.region),
                    detector=4,
                ).dimension_records("detector"),
                [],
                has_postprocessing=True,
            )
            self.check_detector_records_returned(
                butler.query_dimension_records(
                    "detector",
                    where="visit_detector_region.region OVERLAPS region",
                    bind={"region": patch_record.region},
                    detector=4,
                    explain=False,
                ),
                ids=[],
            )
            # Check spatial queries using points instead of regions.
            # This (ra, dec) is a point in the center of the region for visit
            # 1, detector 3.
            ra = 0.25209391431545386  # degrees
            dec = 0.9269112711026793  # degrees

            def _check_visit_id(query: Query) -> None:
                result = list(query.data_ids(["visit", "detector"]))
                self.assertEqual(len(result), 1)
                id = result[0]
                self.assertEqual(id["visit"], 1)
                self.assertEqual(id["detector"], 3)

            # Basic POINT() syntax.
            _check_visit_id(query.where(f"visit_detector_region.region OVERLAPS POINT({ra}, {dec})"))
            _check_visit_id(query.where(f"POINT({ra}, {dec}) OVERLAPS visit_detector_region.region"))

            # dec of 1 is close enough to still be in the region, and tests
            # conversion of integer to float.
            _check_visit_id(query.where(f"visit_detector_region.region OVERLAPS POINT({ra}, 1)"))

            # Negative values are allowed for dec, since it's defined as -90 to
            # 90.  Tract 1, patch 4 slightly overlaps some negative dec values.
            result = list(query.where("patch.region OVERLAPS POINT(0.335, -0.000000001)").data_ids(["patch"]))
            self.assertEqual(len(result), 1)
            id = result[0]
            self.assertEqual(id["patch"], 4)
            self.assertEqual(id["tract"], 1)
            # Out of bounds dec values are not allowed.
            with self.assertRaisesRegex(ValueError, "invalid latitude angle"):
                list(query.where("patch.region OVERLAPS POINT(0.335, -91)").data_ids(["patch"]))

            # Negative ra values are allowed.
            _check_visit_id(query.where(f"POINT({ra - 360}, {dec}) OVERLAPS visit_detector_region.region"))

            # Substitute ra and dec values via bind instead of literals in the
            # string.
            _check_visit_id(
                query.where(
                    "visit_detector_region.region OVERLAPS POINT(ra, dec)", bind={"ra": ra, "dec": dec}
                )
            )

            # Bind in a point object instead of specifying ra/dec separately.
            _check_visit_id(
                query.where(
                    "visit_detector_region.region OVERLAPS my_point",
                    bind={"my_point": LonLat.fromDegrees(ra, dec)},
                )
            )
            _check_visit_id(
                query.where(
                    "visit_detector_region.region OVERLAPS my_point",
                    bind={"my_point": astropy.coordinates.SkyCoord(ra, dec, frame="icrs", unit="deg")},
                )
            )
            # Make sure alternative coordinate frames in astropy SkyCoord are
            # handled.
            _check_visit_id(
                query.where(
                    "visit_detector_region.region OVERLAPS my_point",
                    bind={
                        "my_point": astropy.coordinates.SkyCoord(
                            ra, dec, frame="icrs", unit="deg"
                        ).transform_to("galactic")
                    },
                )
            )

            # Compare against literal values using ExpressionFactory.
            _check_visit_id(
                query.where(_x.visit_detector_region.region.overlaps(LonLat.fromDegrees(ra, dec)))
            )
            _check_visit_id(
                query.where(
                    _x.visit_detector_region.region.overlaps(
                        astropy.coordinates.SkyCoord(ra, dec, frame="icrs", unit="deg")
                    )
                )
            )

            # Check errors for invalid syntax.
            with self.assertRaisesRegex(
                InvalidQueryError, r"Expression 'visit.id' in POINT\(\) is not a literal number."
            ):
                query.where(f"visit_detector_region.region OVERLAPS POINT(visit.id, {dec})")
            with self.assertRaisesRegex(
                InvalidQueryError, r"Expression ''not-a-number'' in POINT\(\) is not a literal number."
            ):
                query.where(f"visit_detector_region.region OVERLAPS POINT({ra}, 'not-a-number')")

            # astropy's SkyCoord can be array-valued, but we expect only a
            # single point.
            array_point = astropy.coordinates.SkyCoord(
                ra=[10, 11, 12, 13], dec=[41, -5, 42, 0], unit="deg", frame="icrs"
            )
            with self.assertRaisesRegex(ValueError, "Astropy SkyCoord contained an array of points"):
                query.where(
                    "visit_detector_region.region OVERLAPS my_point",
                    bind={"my_point": array_point},
                )

    def test_auto_spatial_joins(self) -> None:
        """Test the addition of automatic spatial joins in the presence and
        absence of datasets with dimensions that cross spatial families.
        """
        butler = self.make_butler("base.yaml", "spatial.yaml")
        # Set default governor data ID values both to test that code path and
        # to keep us from having to repeat them in every 'where' call below.
        butler.registry.defaults = RegistryDefaults(instrument="Cam1", skymap="SkyMap1")
        # Add some datasets with {tract, visit, detector} dimensions.
        # These will cover all {visit, detector}s that overlap tract=0.
        cat = DatasetType(
            "cat",
            dimensions=butler.dimensions.conform(["visit", "detector", "tract"]),
            storageClass="ArrowTable",
        )
        butler.registry.registerDatasetType(cat)
        butler.collections.register("run1")
        butler.registry.insertDatasets(
            cat,
            [
                {"instrument": "Cam1", "skymap": "SkyMap1", "visit": 1, "detector": 1, "tract": 0},
                {"instrument": "Cam1", "skymap": "SkyMap1", "visit": 1, "detector": 2, "tract": 0},
                {"instrument": "Cam1", "skymap": "SkyMap1", "visit": 1, "detector": 3, "tract": 0},
                {"instrument": "Cam1", "skymap": "SkyMap1", "visit": 1, "detector": 4, "tract": 0},
                {"instrument": "Cam1", "skymap": "SkyMap1", "visit": 2, "detector": 1, "tract": 0},
                {"instrument": "Cam1", "skymap": "SkyMap1", "visit": 2, "detector": 2, "tract": 0},
            ],
            run="run1",
        )
        with butler.query() as query:
            # When we query for just these dimensions with that dataset type
            # included, we shouldn't need a spatial join, because we assume
            # it's embedded in the rows of that dataset search.
            q1 = query.join_dataset_search(cat, "run1").data_ids(["visit", "detector", "tract"])
            # If there's no explicit spatial join, there's no postprocessing,
            # and hence we can do an exact count with discard=False.
            self.assertEqual(q1.count(exact=True, discard=False), 6)
            self.assertCountEqual(
                [(row["visit"], row["detector"], row["tract"]) for row in q1],
                [
                    (1, 1, 0),
                    (1, 2, 0),
                    (1, 3, 0),
                    (1, 4, 0),
                    (2, 1, 0),
                    (2, 2, 0),
                ],
            )
            # When we query for these dimensions and another one that wants
            # a more precise spatial join, we should get that more precise
            # spatial join.
            q1 = query.join_dataset_search(cat, "run1").data_ids(["visit", "detector", "tract", "patch"])
            self.assertCountEqual(
                [(row["visit"], row["detector"], row["tract"], row["patch"]) for row in q1],
                [
                    (1, 1, 0, 0),
                    (1, 1, 0, 2),
                    (1, 2, 0, 0),
                    (1, 2, 0, 1),
                    (1, 2, 0, 2),
                    (1, 2, 0, 3),
                    (1, 3, 0, 2),
                    (1, 3, 0, 4),
                    (1, 4, 0, 2),
                    (1, 4, 0, 3),
                    (2, 1, 0, 4),
                    (2, 2, 0, 4),
                    (2, 2, 0, 5),
                ],
            )

    def test_common_skypix_overlaps(self) -> None:
        """Test spatial overlap queries that return htm7 records."""
        butler = self.make_butler("base.yaml", "spatial.yaml")
        # Insert some datasets that use a skypix dimension, since some queries
        # are only possible if a superset of the skypix IDs are in the query
        # already.
        cat1 = DatasetType("cat1", dimensions=butler.dimensions.conform(["htm7"]), storageClass="ArrowTable")
        butler.registry.registerDatasetType(cat1)
        butler.registry.registerCollection("refcats", CollectionType.RUN)
        butler.registry.insertDatasets(cat1, [{"htm7": i} for i in range(253952, 253968)], run="refcats")
        with butler.query() as query:
            _x = query.expression_factory
            # Explicit join to patch.
            self.assertCountEqual(
                [
                    record.id
                    for record in query.where(
                        _x.htm7.region.overlaps(_x.patch.region), skymap="SkyMap1", tract=0, patch=4
                    ).dimension_records("htm7")
                ],
                [253954, 253955],
            )
            # Implicit join to patch.
            self.assertCountEqual(
                [
                    record.id
                    for record in query.where(skymap="SkyMap1", tract=0, patch=4).dimension_records("htm7")
                ],
                [253954, 253955],
            )
            self.assertCountEqual(
                [
                    record.id
                    for record in butler.query_dimension_records("htm7", skymap="SkyMap1", tract=0, patch=4)
                ],
                [253954, 253955],
            )
            # Constraint on the patch region (with the query not knowing it
            # corresponds to that patch).
            (patch,) = query.where(skymap="SkyMap1", tract=0, patch=4).dimension_records("patch")
            self.assertCountEqual(
                [
                    record.id
                    for record in query.join_dataset_search("cat1", collections=["refcats"])
                    .where(_x.htm7.region.overlaps(patch.region))
                    .dimension_records("htm7")
                ],
                [253954, 253955],
            )

    def test_spatial_constraint_queries(self) -> None:
        """Test queries in which one spatial dimension in the constraint (data
        ID or ``where`` string) constrains a different spatial dimension in the
        query result columns.
        """
        butler = self.make_butler("hsc-rc2-subset.yaml")
        with butler.query() as query:
            # This tests the case where the 'patch' region is needed for
            # postprocessing, to compare against the visit region, but is not
            # needed in the resulting data ID.
            self.assertEqual(
                [(9813, 72)],
                [
                    (data_id["tract"], data_id["patch"])
                    for data_id in query.data_ids(["patch"]).where({"instrument": "HSC", "visit": 318})
                ],
            )
            self.assertEqual(
                [(9813, 72)],
                [
                    (data_id["tract"], data_id["patch"])
                    for data_id in butler.query_data_ids(["patch"], instrument="HSC", visit=318)
                ],
            )

            # This tests the case where the 'patch' region is needed in
            # postprocessing AND is also returned in the result rows.
            region_hex = (
                "70cc2b4a68b7ecebbf32d931ecb816df3fffe573df5ab9a93f6d2ac3c7faf9ebbf39dad585e2e6de3fa"
                "88934c311b9a93f55833497bef8ebbf15b3fe207ce5de3fae43c0300f6eab3f3e8709597bebebbf77d66"
                "efa5115df3f05874a255d6eab3f"
            )
            self.assertEqual(
                [(9813, 72, region_hex)],
                [
                    (record.tract, record.id, record.region.encode().hex())
                    for record in query.dimension_records("patch").where({"instrument": "HSC", "visit": 318})
                ],
            )
            self.assertEqual(
                [(9813, 72, region_hex)],
                [
                    (record.tract, record.id, record.region.encode().hex())
                    for record in butler.query_dimension_records("patch", instrument="HSC", visit=318)
                ],
            )

    def test_data_coordinate_upload(self) -> None:
        """Test queries for dimension records with a data coordinate upload."""
        butler = self.make_butler("base.yaml", "spatial.yaml")
        with butler.query() as query:
            # Query with a data ID upload that has an irrelevant row (there's
            # no data with "Cam2").
            self.check_detector_records(
                query.join_data_coordinates(
                    [
                        DataCoordinate.standardize(instrument="Cam1", detector=1, universe=butler.dimensions),
                        DataCoordinate.standardize(instrument="Cam1", detector=3, universe=butler.dimensions),
                        DataCoordinate.standardize(instrument="Cam2", detector=4, universe=butler.dimensions),
                    ]
                ).dimension_records("detector"),
                [1, 3],
            )
            # Query with a data ID upload that directly contains duplicates,
            # which should not appear in the results.
            self.check_detector_records(
                query.join_data_coordinates(
                    [
                        DataCoordinate.standardize(instrument="Cam1", detector=1, universe=butler.dimensions),
                        DataCoordinate.standardize(instrument="Cam1", detector=3, universe=butler.dimensions),
                        DataCoordinate.standardize(instrument="Cam1", detector=3, universe=butler.dimensions),
                    ]
                ).dimension_records("detector"),
                [1, 3],
            )
            # Query with a data ID upload that has extra dimensions that could
            # also introduce duplicates if we're not careful.
            self.check_detector_records(
                query.join_data_coordinates(
                    [
                        DataCoordinate.standardize(
                            instrument="Cam1", visit=1, detector=1, universe=butler.dimensions
                        ),
                        DataCoordinate.standardize(
                            instrument="Cam1", visit=2, detector=3, universe=butler.dimensions
                        ),
                        DataCoordinate.standardize(
                            instrument="Cam1", visit=1, detector=3, universe=butler.dimensions
                        ),
                    ]
                ).dimension_records("detector"),
                [1, 3],
            )
            # Query with a data ID upload that has extra dimensions that are
            # used in a constraint.
            self.check_detector_records(
                query.join_data_coordinates(
                    [
                        DataCoordinate.standardize(
                            instrument="Cam1", visit=1, detector=1, universe=butler.dimensions
                        ),
                        DataCoordinate.standardize(
                            instrument="Cam1", visit=2, detector=3, universe=butler.dimensions
                        ),
                        DataCoordinate.standardize(
                            instrument="Cam1", visit=1, detector=3, universe=butler.dimensions
                        ),
                    ]
                )
                .where(instrument="Cam1", visit=2)
                .dimension_records("detector"),
                [3],
            )
            # Query with a data ID upload that must be spatially joined to
            # the other dimensions.  This join is added automatically.
            self.check_detector_records(
                query.join_data_coordinates(
                    [
                        DataCoordinate.standardize(
                            skymap="SkyMap1", tract=1, patch=1, universe=butler.dimensions
                        ),
                        DataCoordinate.standardize(
                            skymap="SkyMap1", tract=1, patch=2, universe=butler.dimensions
                        ),
                        DataCoordinate.standardize(
                            skymap="SkyMap1", tract=1, patch=3, universe=butler.dimensions
                        ),
                    ]
                )
                .where(instrument="Cam1", visit=2)
                .dimension_records("detector"),
                [2, 3, 4],
                has_postprocessing=True,
            )
            # Query with a data ID upload that embeds a spatial relationship.
            # This prevents automatic creation of a spatial join.  To make the
            # test more interesting, the spatial relationship embedded in these
            # data IDs is nonsense: it includes combinations that do not
            # overlap, while leaving out combinations that do overlap.
            self.check_detector_records(
                query.join_data_coordinates(
                    [
                        DataCoordinate.standardize(
                            skymap="SkyMap1",
                            tract=1,
                            patch=1,
                            instrument="Cam1",
                            visit=1,
                            detector=1,
                            universe=butler.dimensions,
                        ),
                        DataCoordinate.standardize(
                            skymap="SkyMap1",
                            tract=1,
                            patch=1,
                            instrument="Cam1",
                            visit=1,
                            detector=2,
                            universe=butler.dimensions,
                        ),
                        DataCoordinate.standardize(
                            skymap="SkyMap1",
                            tract=1,
                            patch=3,
                            instrument="Cam1",
                            visit=1,
                            detector=3,
                            universe=butler.dimensions,
                        ),
                    ]
                )
                .where(skymap="SkyMap1", tract=1, patch=1)
                .dimension_records("detector"),
                [1, 2],
            )
            # Query with an empty data ID upload (not a useful thing to do,
            # but a way to probe edge-case behavior).
            self.check_detector_records(
                query.join_data_coordinates(
                    [
                        DataCoordinate.make_empty(universe=butler.dimensions),
                    ]
                ).dimension_records("detector"),
                [1, 2, 3, 4],
            )

    def test_data_coordinate_upload_force_temp_table(self) -> None:
        """Test queries for dimension records with a data coordinate upload
        that is so big it has to go into a temporary table rather than be
        included directly into the query via bind params (by making the
        threshold for making a a temporary table tiny).

        This test assumes a DirectQueryDriver and is automatically skipped when
        some other driver is found.
        """
        butler = self.make_butler("base.yaml", "spatial.yaml")
        with butler.query() as query:
            if not isinstance(query._driver, DirectQueryDriver):
                raise unittest.SkipTest("Test requires meddling with DirectQueryDriver internals.")
            query._driver._constant_rows_limit = 2
            self.check_detector_records(
                query.join_data_coordinates(
                    [
                        DataCoordinate.standardize(instrument="Cam1", detector=1, universe=butler.dimensions),
                        DataCoordinate.standardize(instrument="Cam1", detector=3, universe=butler.dimensions),
                        DataCoordinate.standardize(instrument="Cam1", detector=4, universe=butler.dimensions),
                    ]
                ).dimension_records("detector"),
                [1, 3, 4],
            )

    def test_materialization(self) -> None:
        """Test querying for dimension records against a materialized previous
        query.
        """
        butler = self.make_butler("base.yaml", "datasets.yaml", "spatial.yaml")
        with butler.query() as query:
            _x = query.expression_factory
            # Simple case where the materialization has just the dimensions
            # we need for the rest of the query.
            self.check_detector_records(
                query.where(_x.detector.raft == "A", instrument="Cam1")
                .materialize()
                .dimension_records("detector"),
                [1, 2],
            )
            # This materialization has extra dimensions that could cause
            # duplicates if we don't SELECT DISTINCT them away.
            self.check_detector_records(
                query.join_dimensions(["visit", "detector"])
                .where(_x.detector.raft == "A", instrument="Cam1")
                .materialize()
                .dimension_records("detector"),
                [1, 2],
            )
            # Materialize a spatial-join, which should prevent the creation
            # of a spatial join in the downstream query.
            self.check_detector_records(
                query.join_dimensions(["visit", "detector", "tract"])
                .materialize()
                .where(skymap="SkyMap1", tract=0, instrument="Cam1", visit=2)
                .dimension_records("detector"),
                [1, 2],
                has_postprocessing=True,
            )
            # Materialize with a dataset join.
            self.check_detector_records(
                query.join_dataset_search("bias", collections=["imported_g"])
                .materialize(datasets=["bias"])
                .dimension_records("detector"),
                [1, 2, 3],
            )

    def test_materialization_find_first(self) -> None:
        """Test querying for datasets with find_first against a materialized
        query.
        """
        butler = self.make_butler("ci_hsc-subset.yaml", "ci_hsc-subset-skymap.yaml")

        run = "HSC/runs/ci_hsc/20240806T180642Z"
        extra_run = "HSC/runs/ci_hsc/20240806T180642Z-extra"

        # Find few datasets to duplicate.
        refs = butler.query_datasets("calexp", run, limit=3)
        data_ids = [ref.dataId for ref in refs]

        butler.collections.register(extra_run)
        butler.registry.insertDatasets("calexp", data_ids, extra_run)

        collections = [run, extra_run, "skymaps"]
        with butler.query() as query:
            query = query.join_dimensions(
                [
                    "instrument",
                    "physical_filter",
                    "band",
                    "visit",
                    "detector",
                    "day_obs",
                    "skymap",
                    "tract",
                ]
            )
            query = query.join_dataset_search("skyMap", collections)
            query = query.join_dataset_search("calexp", collections)
            query = query.where({}, "instrument='HSC' AND skymap='discrete/ci_hsc'", bind=None)
            m_query = query.materialize()
            _ = list(m_query.datasets("skyMap", collections))
            _ = list(m_query.datasets("calexp", collections))

    def test_materialization_no_results(self) -> None:
        """Test querying for datasets when materialized table is empty."""
        butler = self.make_butler("ci_hsc-subset.yaml", "ci_hsc-subset-skymap.yaml")

        run = "HSC/runs/ci_hsc/20240806T180642Z"

        # Register a dataset type but do not add any datasets.
        butler.registry.registerDatasetType(
            DatasetType("nothing", ["visit", "detector"], "int", universe=butler.dimensions)
        )

        collections = [run]
        with butler.query() as query:
            query = query.join_dimensions(
                [
                    "instrument",
                    "physical_filter",
                    "band",
                    "visit",
                    "detector",
                    "day_obs",
                    "skymap",
                    "tract",
                ]
            )
            query = query.join_dataset_search("calexp", collections)
            query = query.join_dataset_search("nothing", collections)
            query = query.where({}, "instrument='HSC' AND skymap='discrete/ci_hsc'", bind=None)
            no_results = "\n".join(query.explain_no_results())
            self.assertIn("No datasets of type 'nothing'", no_results)

            m_query = query.materialize()
            result = m_query.datasets("nothing")
            self.assertFalse(result.any())
            no_results = "\n".join(result.explain_no_results())
            self.assertIn("No datasets of type 'nothing'", no_results)

    def test_timespan_results(self) -> None:
        """Test returning dimension records that include timespans."""
        butler = self.make_butler("base.yaml", "spatial.yaml")
        with butler.query() as query:
            query_results = list(query.dimension_records("visit"))
        simple_results = butler.query_dimension_records("visit")
        for results in (query_results, simple_results):
            self.assertCountEqual(
                [(record.id, record.timespan.begin, record.timespan.end) for record in results],
                [
                    (
                        1,
                        astropy.time.Time("2021-09-09T03:00:00", format="isot", scale="tai"),
                        astropy.time.Time("2021-09-09T03:01:00", format="isot", scale="tai"),
                    ),
                    (
                        2,
                        astropy.time.Time("2021-09-09T03:02:00", format="isot", scale="tai"),
                        astropy.time.Time("2021-09-09T03:03:00", format="isot", scale="tai"),
                    ),
                ],
            )

    def test_direct_driver_paging(self) -> None:
        """Test queries for dimension records that require multiple pages (by
        making the page size tiny for DirectQueryDriver).

        For RemoteQueryDriver, we can't manipulate the page size so this just
        checks that the driver context manager logic is executing.
        """
        butler = self.make_butler("base.yaml")
        # Basic test where pages should be transparent.
        with butler.query() as query:
            if isinstance(query._driver, DirectQueryDriver):
                query._driver._raw_page_size = 2
            self.check_detector_records(
                query.dimension_records("detector"),
                [1, 2, 3, 4],
            )
        # Test that it's an error to continue query iteration after closing the
        # context manager.
        with butler.query() as query:
            if isinstance(query._driver, DirectQueryDriver):
                query._driver._raw_page_size = 2
            iterator = iter(query.dimension_records("detector"))
            next(iterator)
        with self.assertRaisesRegex(RuntimeError, "Cannot continue query result iteration"):
            list(iterator)

    def test_column_expressions(self) -> None:
        """Test queries with a wide variant of column expressions."""
        butler = self.make_butler("base.yaml", "spatial.yaml")
        butler.registry.defaults = RegistryDefaults(instrument="Cam1")
        with butler.query() as query:
            _x = query.expression_factory
            self.check_detector_records(
                query.where(_x.not_(_x.detector != 2)).dimension_records("detector"),
                [2],
            )
            self.check_detector_records_returned(
                butler.query_dimension_records("detector", where="NOT (detector != 2)"),
                [2],
            )
            self.check_detector_records(
                # Empty string expression should evaluate to True.
                query.where(_x.detector == 2, "").dimension_records("detector"),
                [2],
            )
            self.check_detector_records(
                query.where(_x.literal(2) == _x.detector).dimension_records("detector"),
                [2],
            )
            self.check_detector_records(
                query.where(_x.literal(2) == _x.detector + 1).dimension_records("detector"),
                [1],
            )
            self.check_detector_records(
                query.where(-_x.detector == -3).dimension_records("detector"),
                [3],
            )
            self.check_detector_records(
                query.where(_x.detector == 1, _x.detector == 2).dimension_records("detector"),
                [],
                messages=["'where' expression requires both detector=2 and detector=1."],
            )
            self.assertCountEqual(
                [
                    record.id
                    for record in query.where(
                        # Datetime equal to the "begin" of the timespan.
                        _x.visit.timespan.overlaps(
                            astropy.time.Time("2021-09-09T03:00:00", format="isot", scale="tai")
                        )
                    ).dimension_records("visit")
                ],
                # Timespan begin bound is inclusive, so the record should
                # match.
                [1],
            )
            self.assertCountEqual(
                [
                    record.id
                    for record in query.where(
                        # Datetime equal to the "end" of the timespan.
                        _x.visit.timespan.overlaps(
                            astropy.time.Time("2021-09-09T03:01:00", format="isot", scale="tai")
                        )
                    ).dimension_records("visit")
                ],
                # Timespan end bound is exclusive, so we should get no records.
                [],
            )
            self.assertCountEqual(
                [
                    record.id
                    for record in query.where(
                        # In the middle of the timespan.
                        _x.visit.timespan.overlaps(
                            astropy.time.Time("2021-09-09T03:02:30", format="isot", scale="tai")
                        )
                    ).dimension_records("visit")
                ],
                [2],
            )
            self.assertCountEqual(
                [
                    record.id
                    for record in butler.query_dimension_records(
                        # In the middle of the timespan.
                        "visit",
                        where="visit.timespan OVERLAPS(ts)",
                        bind={"ts": astropy.time.Time("2021-09-09T03:02:30", format="isot", scale="tai")},
                    )
                ],
                [2],
            )
            self.assertCountEqual(
                [
                    record.id
                    for record in query.where(
                        _x.visit.timespan.overlaps(
                            Timespan(
                                begin=astropy.time.Time("2021-09-09T03:02:30", format="isot", scale="tai"),
                                end=None,
                            )
                        )
                    ).dimension_records("visit")
                ],
                [2],
            )
            self.assertCountEqual(
                [
                    record.id
                    for record in query.where(
                        _x.not_(
                            _x.visit.timespan.end
                            < astropy.time.Time("2021-09-09T03:02:30", format="isot", scale="tai"),
                        )
                    ).dimension_records("visit")
                ],
                [2],
            )
            self.assertCountEqual(
                [
                    record.id
                    for record in query.where(
                        _x.visit.timespan.begin
                        > astropy.time.Time("2021-09-09T03:01:30", format="isot", scale="tai")
                    ).dimension_records("visit")
                ],
                [2],
            )
            self.assertCountEqual(
                [
                    record.id
                    for record in query.where(
                        (_x.visit.exposure_time + -(5.0 * _x.visit.zenith_angle)) > 0.0
                    ).dimension_records("visit")
                ],
                [1],
            )
            self.assertCountEqual(
                [
                    record.id
                    for record in query.where(_x.visit.exposure_time - 5.0 >= 50.0).dimension_records("visit")
                ],
                [1],
            )
            self.assertCountEqual(
                [record.id for record in query.where(_x.visit.id % 2 != 0).dimension_records("visit")],
                [1],
            )
            self.assertCountEqual(
                [
                    record.id
                    for record in query.where(_x.visit.zenith_angle / 5.0 <= 1.0).dimension_records("visit")
                ],
                [1],
            )
            self.assertCountEqual(
                [record.id for record in query.where(_x.visit.timespan.is_null).dimension_records("visit")],
                [],
            )
            self.assertCountEqual(
                [
                    record.id
                    for record in query.where(_x.visit.exposure_time.is_null).dimension_records("visit")
                ],
                [],
            )
            self.check_detector_records(
                query.where(_x.detector.in_iterable([1, 3, 4])).dimension_records("detector"),
                [1, 3, 4],
            )
            self.check_detector_records_returned(
                butler.query_dimension_records(
                    "detector", where="detector IN (det)", bind={"det": [1, 3, 4]}
                ),
                [1, 3, 4],
            )
            self.check_detector_records(
                query.where(_x.detector.in_range(start=2, stop=None)).dimension_records("detector"),
                [2, 3, 4],
            )
            self.check_detector_records(
                query.where(_x.detector.in_range(start=1, stop=3)).dimension_records("detector"),
                [1, 2],
            )
            self.check_detector_records(
                query.where(_x.detector.in_range(start=1, stop=None, step=2)).dimension_records("detector"),
                [1, 3],
            )
            self.check_detector_records(
                query.where(_x.detector.in_range(start=1, stop=2)).dimension_records("detector"),
                [1],
            )
            # This is a complex way to write a much simpler query ("where
            # detector.raft == 'A'"), but it tests code paths that would
            # otherwise require a lot more test setup.
            self.check_detector_records(
                query.where(
                    _x.detector.in_query(_x.detector, query.where(_x.detector.raft == "A"))
                ).dimension_records("detector"),
                [1, 2],
            )
            # Error to reference tract without skymap in a WHERE clause.
            with self.assertRaises(InvalidQueryError):
                list(query.where(_x.tract == 4).dimension_records("patch"))

    def test_boolean_columns(self) -> None:
        """Test that boolean columns work as expected when specifying
        expressions.
        """
        # Exposure is the only dimension that has boolean columns, and this set
        # of data has all the pre-requisites for exposure set up.
        butler = self.make_butler("hsc-rc2-subset.yaml")

        base_data = {"instrument": "HSC", "physical_filter": "HSC-R", "group": "903342", "day_obs": 20130617}

        TRUE_ID = 1000
        FALSE_ID_1 = 2001
        FALSE_ID_2 = 2002
        NULL_ID_1 = 3000
        NULL_ID_2 = 903342  # already exists in the YAML file
        records = [
            {"id": TRUE_ID, "obs_id": "true-1", "can_see_sky": True},
            {"id": FALSE_ID_1, "obs_id": "false-1", "can_see_sky": False, "observation_type": "science"},
            {"id": FALSE_ID_2, "obs_id": "false-2", "can_see_sky": False, "observation_type": None},
            {"id": NULL_ID_1, "obs_id": "null-1", "can_see_sky": None},
        ]
        for record in records:
            butler.registry.insertDimensionData("exposure", base_data | record)

        # Go through the registry interface to cover the old query system, too.
        # This can be removed once the old query system is removed.
        def _run_registry_query(where: str) -> list[int]:
            return _get_exposure_ids_from_dimension_records(
                butler.registry.queryDimensionRecords("exposure", where=where, instrument="HSC")
            )

        def _run_simple_query(where: str) -> list[int]:
            return _get_exposure_ids_from_dimension_records(
                butler.query_dimension_records("exposure", where=where, instrument="HSC")
            )

        def _run_query(where: str) -> list[int]:
            with butler.query() as query:
                return _get_exposure_ids_from_dimension_records(
                    query.dimension_records("exposure").where(where, instrument="HSC")
                )

        # Test boolean columns in the `where` string syntax.
        for test, query_func in [
            ("registry", _run_registry_query),
            ("new-query", _run_query),
            ("simple", _run_simple_query),
        ]:
            with self.subTest(test):
                # Boolean columns should be usable standalone as an expression.
                self.assertCountEqual(query_func("exposure.can_see_sky"), [TRUE_ID])

                # You can find false values in the column with NOT.  The NOT of
                # NULL is NULL, consistent with SQL semantics -- so records
                # with NULL can_see_sky are not included here.
                self.assertCountEqual(query_func("NOT exposure.can_see_sky"), [FALSE_ID_1, FALSE_ID_2])

                # Make sure the bare column composes with other expressions
                # correctly.
                self.assertCountEqual(
                    query_func("exposure.can_see_sky OR exposure = 2001"), [TRUE_ID, FALSE_ID_1]
                )

        # Find nulls and non-nulls.
        #
        # This is run only against the new query system.  It appears that the
        # `= NULL` syntax never had test coverage in the old query system and
        # doesn't work for any column types.  Not worth fixing since we are
        # dropping that code soon.
        nulls = [NULL_ID_1, NULL_ID_2]
        non_nulls = [TRUE_ID, FALSE_ID_1, FALSE_ID_2]
        self.assertCountEqual(_run_query("exposure.can_see_sky = NULL"), nulls)
        self.assertCountEqual(_run_query("exposure.can_see_sky != NULL"), non_nulls)
        self.assertCountEqual(_run_query("NULL = exposure.can_see_sky"), nulls)
        self.assertCountEqual(_run_query("NULL != exposure.can_see_sky"), non_nulls)

        # You can't do a NULL check on an arbitrary boolean predicate.
        with self.assertRaises(InvalidQueryError):
            _run_query("NULL = (exposure.can_see_sky AND exposure = 2001)")

        # Check null finding for non-boolean columns, too.
        self.assertEqual(
            _run_query("exposure.observation_type = NULL AND NOT exposure.can_see_sky"), [FALSE_ID_2]
        )
        self.assertEqual(
            _run_query("exposure.observation_type != NULL AND NOT exposure.can_see_sky"), [FALSE_ID_1]
        )
        self.assertEqual(
            _run_query("NULL = exposure.observation_type AND NOT exposure.can_see_sky"), [FALSE_ID_2]
        )
        self.assertEqual(
            _run_query("NULL != exposure.observation_type AND NOT exposure.can_see_sky"), [FALSE_ID_1]
        )

        # Test boolean columns in ExpressionFactory.
        with butler.query() as query:
            x = query.expression_factory

            def do_query(constraint: Predicate) -> list[int]:
                return _get_exposure_ids_from_dimension_records(
                    query.dimension_records("exposure").where(constraint, instrument="HSC")
                )

            # Boolean columns should be usable standalone as a Predicate.
            self.assertCountEqual(do_query(x.exposure.can_see_sky.as_boolean()), [TRUE_ID])

            # You can find false values in the column with NOT.  The NOT of
            # NULL is NULL, consistent with SQL semantics -- so records
            # with NULL can_see_sky are not included here.
            self.assertCountEqual(
                do_query(x.exposure.can_see_sky.as_boolean().logical_not()), [FALSE_ID_1, FALSE_ID_2]
            )

            # Searching for nulls works.
            self.assertCountEqual(do_query(x.exposure.can_see_sky.is_null), [NULL_ID_1, NULL_ID_2])

            # Attempting to use operators that only apply to non-boolean types
            # is an error.
            with self.assertRaisesRegex(
                InvalidQueryError,
                r"Boolean expression 'exposure.can_see_sky' can't be used directly in other expressions."
                r" Call the 'as_boolean\(\)' method to convert it to a Predicate instead.",
            ):
                x.exposure.can_see_sky == 1

            # Non-boolean types can't be converted directly to Predicate.
            with self.assertRaisesRegex(
                InvalidQueryError,
                r"Expression 'exposure.observation_type' with type 'string' can't be used directly"
                r" as a boolean value.",
            ):
                x.exposure.observation_type.as_boolean()

    def test_dataset_region_queries(self) -> None:
        """Test region queries for datasets."""
        # Import data to play with.
        butler = self.make_butler("base.yaml", "ci_hsc-subset.yaml")

        run = "HSC/runs/ci_hsc/20240806T180642Z"
        with butler.query() as query:
            # Return everything.
            results = query.datasets("calexp", collections=run)
            # Sort by data coordinate.
            refs = sorted(results.with_dimension_records(), key=attrgetter("dataId"))
            self.assertEqual(len(refs), 33)

        # Use a region from the first visit.
        first_visit_region = refs[0].dataId.visit.region  # type: ignore

        # Get a visit detector region from the first ref.
        with butler.query() as query:
            data_id = refs[0].dataId.mapping
            records = list(query.dimension_records("visit_detector_region").where(**data_id))  # type: ignore
            self.assertEqual(len(records), 1)

        for pos, count in (
            ("CIRCLE 320. -0.25 10.", 33),  # Match everything.
            ("CIRCLE 321.0 -0.4 0.01", 1),  # Should be small region on 1 detector.
            ("CIRCLE 321.1 -0.35 0.02", 2),
            ("CIRCLE 321.1 -0.48 0.05", 1),  # Center off the region.
            ("CIRCLE 321.0 -0.5 0.01", 0),  # No overlap.
            (first_visit_region.to_ivoa_pos(), 33),  # Visit region overlaps everything.
            (records[0].region.to_ivoa_pos(), 17),  # Some overlap.
        ):
            with butler.query() as query:
                results = query.datasets("calexp", collections=run)
                results = results.where(
                    "instrument = 'HSC' AND visit_detector_region.region OVERLAPS(POS)",
                    bind={"POS": Region.from_ivoa_pos(pos)},
                )
                refs = list(results)
                self.assertEqual(len(refs), count, f"POS={pos} REFS={refs}")

                simple_refs = butler.query_datasets(
                    "calexp",
                    collections=run,
                    instrument="HSC",
                    where="visit_detector_region.region OVERLAPS(POS)",
                    bind={"POS": Region.from_ivoa_pos(pos)},
                    explain=False,
                )
                self.assertCountEqual(refs, simple_refs)

    def test_dataset_time_queries(self) -> None:
        """Test temporal queries for datasets."""
        # Import data to play with.
        butler = self.make_butler("base.yaml", "ci_hsc-subset.yaml")

        # Some times from the test data.
        v_903334_pre = astropy.time.Time("2013-01-01T12:00:00", scale="tai", format="isot")
        v_903334_mid = astropy.time.Time("2013-06-17T13:29:20", scale="tai", format="isot")
        v_904014_pre = astropy.time.Time("2013-11-01T12:00:00", scale="tai", format="isot")
        v_904014_post = astropy.time.Time("2013-12-21T12:00:00", scale="tai", format="isot")

        with butler.query() as query:
            run = "HSC/runs/ci_hsc/20240806T180642Z"
            results = query.datasets("calexp", collections=run)

            # Use a time during the middle of a visit.
            v_903334 = results.where(
                "instrument = 'HSC' and visit.timespan OVERLAPS(ts)", bind={"ts": v_903334_mid}
            )
            self.assertEqual(len(list(v_903334)), 4)

            # Timespan covering first half of the data.
            first_half = results.where(
                "instrument = 'HSC' and visit.timespan OVERLAPS(t1, t2)",
                bind={"t1": v_903334_pre, "t2": v_904014_pre},
            )
            self.assertEqual(len(list(first_half)), 17)

            # Query using a timespan object.
            with_ts = results.where(
                "instrument = 'HSC' and visit.timespan OVERLAPS(ts)",
                bind={"ts": Timespan(v_904014_pre, v_904014_post)},
            )
            self.assertEqual(len(list(with_ts)), 16)

    def test_calibration_join_queries(self) -> None:
        """Test using the 'general' query result type to join observations to
        calibration datasets temporally.

        We have to use general results because we want calibration DatasetRefs
        and data IDs that include the observation identifiers (which are not
        part of the calibration dataset dimensions).
        """
        butler = self.make_butler("base.yaml", "datasets.yaml")
        # Set up some timestamps.
        t1 = astropy.time.Time("2020-01-01T01:00:00", format="isot", scale="tai")
        t2 = astropy.time.Time("2020-01-01T02:00:00", format="isot", scale="tai")
        t3 = astropy.time.Time("2020-01-01T03:00:00", format="isot", scale="tai")
        t4 = astropy.time.Time("2020-01-01T04:00:00", format="isot", scale="tai")
        t5 = astropy.time.Time("2020-01-01T05:00:00", format="isot", scale="tai")
        # Insert some exposure records with timespans between each sequential
        # pair of those.
        butler.registry.insertDimensionData(
            "day_obs", {"instrument": "Cam1", "id": 20200101, "timespan": Timespan(t1, t5)}
        )
        butler.registry.insertDimensionData(
            "group",
            {"instrument": "Cam1", "name": "group0"},
            {"instrument": "Cam1", "name": "group1"},
            {"instrument": "Cam1", "name": "group2"},
            {"instrument": "Cam1", "name": "group3"},
        )
        butler.registry.insertDimensionData(
            "exposure",
            {
                "instrument": "Cam1",
                "id": 0,
                "group": "group0",
                "obs_id": "zero",
                "physical_filter": "Cam1-G",
                "day_obs": 20200101,
                "timespan": Timespan(t1, t2),
            },
            {
                "instrument": "Cam1",
                "id": 1,
                "group": "group1",
                "obs_id": "one",
                "physical_filter": "Cam1-G",
                "day_obs": 20200101,
                "timespan": Timespan(t2, t3),
            },
            {
                "instrument": "Cam1",
                "id": 2,
                "group": "group2",
                "obs_id": "two",
                "physical_filter": "Cam1-G",
                "day_obs": 20200101,
                "timespan": Timespan(t3, t4),
            },
            {
                "instrument": "Cam1",
                "id": 3,
                "group": "group3",
                "obs_id": "three",
                "physical_filter": "Cam1-G",
                "day_obs": 20200101,
                "timespan": Timespan(t4, t5),
            },
        )
        # Get references to the datasets we imported.
        bias = butler.get_dataset_type("bias")
        bias2a = butler.find_dataset("bias", instrument="Cam1", detector=2, collections="imported_g")
        assert bias2a is not None
        bias3a = butler.find_dataset("bias", instrument="Cam1", detector=3, collections="imported_g")
        assert bias3a is not None
        bias2b = butler.find_dataset("bias", instrument="Cam1", detector=2, collections="imported_r")
        assert bias2b is not None
        bias3b = butler.find_dataset("bias", instrument="Cam1", detector=3, collections="imported_r")
        assert bias3b is not None
        # Register the main calibration collection we'll be working with.
        collection = "Cam1/calibs"
        butler.collections.register(collection, type=CollectionType.CALIBRATION)
        # Certify 2a dataset with [t2, t4) validity.
        butler.registry.certify(collection, [bias2a], Timespan(begin=t2, end=t4))
        # Certify 3a over [t1, t3).
        butler.registry.certify(collection, [bias3a], Timespan(begin=t1, end=t3))
        # Certify 2b and 3b together over [t4, ∞).
        butler.registry.certify(collection, [bias2b, bias3b], Timespan(begin=t4, end=None))
        # Query for (bias, exposure, detector) combinations.
        base_data_id = DataCoordinate.standardize(instrument="Cam1", universe=butler.dimensions)
        with butler.query() as q:
            x = q.expression_factory
            q = q.join_dimensions(["exposure"])
            q = q.join_dataset_search("bias", [collection])
            # Query for all calibs with an explicit temporal join.
            self.assertCountEqual(
                [
                    (data_id, refs[0])
                    for data_id, refs, _ in q.where(
                        x["bias"].timespan.overlaps(x.exposure.timespan), base_data_id
                    )
                    .general(
                        butler.dimensions.conform(["exposure", "detector"]),
                        dataset_fields={"bias": ...},
                        find_first=True,
                    )
                    .iter_tuples(bias)
                ],
                [
                    (DataCoordinate.standardize(base_data_id, detector=2, exposure=1), bias2a),
                    (DataCoordinate.standardize(base_data_id, detector=2, exposure=2), bias2a),
                    (DataCoordinate.standardize(base_data_id, detector=3, exposure=0), bias3a),
                    (DataCoordinate.standardize(base_data_id, detector=3, exposure=1), bias3a),
                    (DataCoordinate.standardize(base_data_id, detector=2, exposure=3), bias2b),
                    (DataCoordinate.standardize(base_data_id, detector=3, exposure=3), bias3b),
                ],
            )
            # Query for all calibs with the temporal join implicit and the
            # dimensions given as an incomplete list (detector is added by
            # the dataset results).
            self.assertCountEqual(
                [
                    (data_id, refs[0])
                    for data_id, refs, _ in q.where(base_data_id)
                    .general(["exposure"], dataset_fields={"bias": ...}, find_first=True)
                    .iter_tuples(bias)
                ],
                [
                    (DataCoordinate.standardize(base_data_id, detector=2, exposure=1), bias2a),
                    (DataCoordinate.standardize(base_data_id, detector=2, exposure=2), bias2a),
                    (DataCoordinate.standardize(base_data_id, detector=3, exposure=0), bias3a),
                    (DataCoordinate.standardize(base_data_id, detector=3, exposure=1), bias3a),
                    (DataCoordinate.standardize(base_data_id, detector=2, exposure=3), bias2b),
                    (DataCoordinate.standardize(base_data_id, detector=3, exposure=3), bias3b),
                ],
            )

    def test_collection_query_info(self) -> None:
        butler = self.make_butler("base.yaml", "datasets.yaml")

        info = butler.collections.query_info("imported_g", include_summary=True)
        self.assertEqual(len(info), 1)
        dataset_types = info[0].dataset_types
        assert dataset_types is not None
        self.assertCountEqual(dataset_types, ["flat", "bias"])

        info = butler.collections.query_info("imported_g", include_summary=True, summary_datasets=["flat"])
        self.assertEqual(len(info), 1)
        dataset_types = info[0].dataset_types
        assert dataset_types is not None
        self.assertCountEqual(dataset_types, ["flat"])

    def test_dataset_queries(self) -> None:
        butler = self.make_butler("base.yaml", "spatial.yaml")

        # Need a dataset with some spatial information to trigger aggregate
        # value logic in queries.
        butler.registry.registerDatasetType(
            DatasetType("dt", ["visit", "detector"], "int", universe=butler.dimensions)
        )
        butler.collections.register("run1")
        butler.registry.insertDatasets("dt", [{"instrument": "Cam1", "visit": 1, "detector": 1}], "run1")

        # Tests for a regression of DM-46340, where invalid SQL would be
        # generated when the list of collections is a single run collection and
        # there is region-postprocessing logic involved.  This was due to
        # missing type information associated with the "run" dataset field.
        result = butler.query_datasets(
            "dt",
            "run1",
            where="instrument='Cam1' and skymap='SkyMap1' and visit=1 and tract=0",
            with_dimension_records=True,
        )
        self.assertEqual(result[0].dataId, {"instrument": "Cam1", "visit": 1, "detector": 1})
        self.assertEqual(result[0].run, "run1")

        # A similar issue to the "run" issue above was occuring with the
        # 'collection' dataset field.
        with butler.query() as query:
            rows = list(
                query.join_dataset_search("dt", "run1")
                .where("instrument='Cam1' and skymap='SkyMap1' and visit=1 and tract=0")
                .general(
                    dimensions=["visit", "detector"],
                    dataset_fields={"dt": set(["collection"])},
                    find_first=True,
                )
            )
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["visit"], 1)
            self.assertEqual(rows[0]["dt.collection"], "run1")

        # Test that dataset fields like ingest_date can be used in the 'where'
        # clause.
        result = butler.query_datasets("dt", "run1", where="ingest_date > T'2000-01-01'")
        self.assertEqual(len(result), 1)
        result = butler.query_datasets("dt", "run1", where="ingest_date < T'2000-01-01'", explain=False)
        self.assertEqual(len(result), 0)

    def test_multiple_instrument_queries(self) -> None:
        """Test that multiple-instrument queries are not rejected as having
        governor dimension ambiguities.
        """
        butler = self.make_butler("base.yaml")
        butler.registry.insertDimensionData("instrument", {"name": "Cam2"})
        self.assertCountEqual(
            butler.query_data_ids(["detector"], where="instrument='Cam1' OR instrument='Cam2'"),
            [
                DataCoordinate.standardize(instrument="Cam1", detector=n, universe=butler.dimensions)
                for n in range(1, 5)
            ],
        )
        self.assertCountEqual(
            butler.query_data_ids(
                ["detector"],
                where="(instrument='Cam1' OR instrument='Cam2') AND visit.region OVERLAPS region",
                bind={"region": Region.from_ivoa_pos("CIRCLE 320. -0.25 10.")},
                explain=False,
            ),
            # No visits in this test dataset means no result, but the point of
            # the test is just that the query can be constructed at all.
            [],
        )
        self.assertCountEqual(
            butler.query_data_ids(
                ["instrument"],
                where="(instrument='Cam1' AND detector=2) OR (instrument='Cam2' AND detector=500)",
                explain=False,
            ),
            [DataCoordinate.standardize(instrument="Cam1", universe=butler.dimensions)],
        )

    def test_default_data_id(self) -> None:
        butler = self.make_butler("base.yaml")
        butler.registry.insertDimensionData("instrument", {"name": "Cam2"})
        butler.registry.insertDimensionData(
            "physical_filter", {"instrument": "Cam2", "name": "Cam2-G", "band": "g"}
        )

        # With no default data ID, queries should return results for all
        # instruments.
        result = butler.query_dimension_records("physical_filter")
        names = [x.name for x in result]
        self.assertCountEqual(names, ["Cam1-G", "Cam1-R1", "Cam1-R2", "Cam2-G"])

        result = butler.query_dimension_records("physical_filter", where="band='g'")
        names = [x.name for x in result]
        self.assertCountEqual(names, ["Cam1-G", "Cam2-G"])

        # When there is no default data ID and a where clause references
        # something depending on instrument, it throws an error as a
        # sanity check.
        # In this case, 'instrument' is not part of the dimensions returned by
        # the query, so there is extra logic needed to detect the need for the
        # default data ID.
        with self.assertRaisesRegex(
            InvalidQueryError,
            "Query 'where' expression references a dimension dependent on instrument"
            " without constraining it directly.",
        ):
            butler.query_data_ids(["band"], where="physical_filter='Cam1-G'")

        # Override the default data ID to specify a default instrument for
        # subsequent tests.
        butler.registry.defaults = RegistryDefaults(instrument="Cam1")

        # When a where clause references something depending on instrument, use
        # the default data ID to constrain the instrument.
        # In this case, 'instrument' is not part of the dimensions returned by
        # the query, so there is extra logic needed to detect the need for the
        # default data ID.
        data_ids = butler.query_data_ids(["band"], where="physical_filter='Cam1-G'")
        self.assertEqual([x["band"] for x in data_ids], ["g"])
        # Default data ID instrument=Cam1 does not match Cam2, so there are no
        # results.
        data_ids = butler.query_data_ids(["band"], where="physical_filter='Cam2-G'", explain=False)
        self.assertEqual(data_ids, [])
        # Overriding the default lets us get the results.
        data_ids = butler.query_data_ids(["band"], where="instrument='Cam2' and physical_filter='Cam2-G'")
        self.assertEqual([x["band"] for x in data_ids], ["g"])

        # Query for a dimension that depends on instrument should pull in the
        # default data ID instrument="Cam1" to constrain results.
        result = butler.query_dimension_records("physical_filter")
        names = [x.name for x in result]
        self.assertCountEqual(names, ["Cam1-G", "Cam1-R1", "Cam1-R2"])

        # Query for a dimension that depends on instrument should pull in the
        # default data ID instrument="Cam1" to constrain results, if the where
        # clause does not explicitly specify instrument.
        result = butler.query_dimension_records("physical_filter", where="band='g'")
        names = [x.name for x in result]
        self.assertEqual(names, ["Cam1-G"])

        # Queries that specify instrument explicitly in the where clause
        # should ignore the default data ID.
        result = butler.query_dimension_records("physical_filter", where="instrument='Cam2'")
        names = [x.name for x in result]
        self.assertCountEqual(names, ["Cam2-G"])

        result = butler.query_dimension_records("physical_filter", where="instrument IN ('Cam2')")
        names = [x.name for x in result]
        self.assertCountEqual(names, ["Cam2-G"])

    def test_unusual_column_literals(self) -> None:
        butler = self.make_butler("base.yaml")

        # Users frequently use numpy integer types as literals in queries.
        result = butler.query_dimension_records(
            "detector", data_id={"instrument": "Cam1", "detector": int64(1)}
        )
        names = [x.full_name for x in result]
        self.assertEqual(names, ["Aa"])

        result = butler.query_dimension_records(
            "detector", where="instrument='Cam1' and detector=an_integer", bind={"an_integer": int64(2)}
        )
        names = [x.full_name for x in result]
        self.assertEqual(names, ["Ab"])

        with butler.query() as query:
            x = query.expression_factory
            result = list(
                query.dimension_records("detector").where(x.instrument == "Cam1", x.detector == int64(3))
            )
            names = [x.full_name for x in result]
            self.assertEqual(names, ["Ba"])

    def test_query_all_datasets(self) -> None:
        butler = self.make_butler("base.yaml", "datasets.yaml")

        # Make sure that refs are coming out well-formed.
        datasets = butler._query_all_datasets("imported_r", where="detector = 2", instrument="Cam1")
        datasets.sort(key=lambda ref: ref.datasetType.name)
        self.assertEqual(len(datasets), 2)
        bias = datasets[0]
        self.assertEqual(bias.datasetType.name, "bias")
        self.assertEqual(bias.dataId["instrument"], "Cam1")
        self.assertEqual(bias.dataId["detector"], 2)
        self.assertEqual(bias.run, "imported_r")
        self.assertEqual(bias.id, UUID("87f3e68d-258d-41b7-8ea5-edf3557ccb30"))
        flat = datasets[1]
        self.assertEqual(flat.datasetType.name, "flat")
        self.assertEqual(flat.dataId["instrument"], "Cam1")
        self.assertEqual(flat.dataId["detector"], 2)
        self.assertEqual(flat.dataId["physical_filter"], "Cam1-R1")
        self.assertEqual(flat.dataId["band"], "r")
        self.assertEqual(flat.run, "imported_r")
        self.assertEqual(flat.id, UUID("c1296796-56c5-4acf-9b49-40d920c6f840"))

        # Querying for everything finds everything.
        results = butler._query_all_datasets("*", find_first=False)
        self.assertEqual(len(results), 13)

        # constraining by data ID works
        detector_1_ids = ("d0bb04cd-d697-4a83-ba53-cdfcd58e3a0c", "e15ab039-bc8b-4135-87c5-90902a7c0b22")
        results = butler._query_all_datasets(
            "*", data_id={"detector": 1, "instrument": "Cam1"}, find_first=False
        )
        self.assertCountEqual(detector_1_ids, _ref_uuids(results))

        # bind values work.
        results = butler._query_all_datasets(
            "*", where="detector=my_bind and instrument='Cam1'", bind={"my_bind": 1}, find_first=False
        )
        self.assertCountEqual(detector_1_ids, _ref_uuids(results))

        # find_first requires ordered collections.
        with self.assertRaisesRegex(InvalidQueryError, "Can not use wildcards"):
            results = butler._query_all_datasets("*")

        butler.collections.register("chain", CollectionType.CHAINED)
        butler.collections.redefine_chain("chain", ["imported_g", "imported_r"])
        results = butler._query_all_datasets(
            "chain", where="detector=2 and instrument = 'Cam1'", find_first=True
        )
        # find_first searches the collection chain in order.
        self.assertCountEqual(
            _ref_uuids(results),
            [
                "51352db4-a47a-447c-b12d-a50b206b17cd",  # imported_g bias
                "60c8a65c-7290-4c38-b1de-e3b1cdcf872d",  # imported_g flat
                "c1296796-56c5-4acf-9b49-40d920c6f840",  # imported_r flat
                # There is also a bias dataset with detector=2 in imported_r,
                # but it is masked by the presence of the same data ID in
                # imported_g.
            ],
        )

        # collection searches work.
        results = butler._query_all_datasets(
            "*g", where="detector=1 and instrument = 'Cam1'", find_first=False
        )
        self.assertEqual(_ref_uuids(results), ["e15ab039-bc8b-4135-87c5-90902a7c0b22"])

        # we raise for missing collections with explicit names.
        with self.assertRaises(MissingCollectionError):
            results = butler._query_all_datasets("nonexistent")
        # we don't raise for collection wildcard searches that find nothing.
        results = butler._query_all_datasets("nonexistent*", find_first=False)
        self.assertEqual(results, [])

        # dataset type searches work.
        results = butler._query_all_datasets(
            "*", name="b*", where="detector=1 and instrument = 'Cam1'", find_first=False
        )
        self.assertEqual(_ref_uuids(results), ["e15ab039-bc8b-4135-87c5-90902a7c0b22"])

        # Missing dataset types raise.
        with self.assertRaises(MissingDatasetTypeError):
            results = butler._query_all_datasets("chain", name=["notfound", "flat"])
        with self.assertRaises(MissingDatasetTypeError):
            results = butler._query_all_datasets("chain", name="notfound*")

        # Limit of 3 lands at the boundary of a dataset type.
        # Limit of 4 is in the middle of a dataset type.
        for limit in [3, 4]:
            with self.subTest(limit=limit):
                results = butler._query_all_datasets("imported_g", limit=limit)
                self.assertEqual(len(results), limit)
                with self.assertLogs(level="WARNING") as log:
                    results = butler._query_all_datasets("imported_g", limit=-limit)
                    self.assertEqual(len(results), limit)
                self.assertIn("requested limit", log.output[0])

        results = butler._query_all_datasets("imported_g", limit=0)
        self.assertEqual(len(results), 0)

        # 'where' constraints that don't apply to all dataset types follow the
        # same rules as query_datasets.
        results = butler._query_all_datasets(
            "*", where="detector = 2 and band = 'g' and instrument = 'Cam1'", find_first=False
        )
        self.assertCountEqual(
            _ref_uuids(results),
            [
                # bias does not have 'band'
                "51352db4-a47a-447c-b12d-a50b206b17cd",
                "87f3e68d-258d-41b7-8ea5-edf3557ccb30",
                # flat does have 'band', and we filter based on it
                "60c8a65c-7290-4c38-b1de-e3b1cdcf872d",
            ],
        )

        # Default collections and data ID apply.
        butler.registry.defaults = RegistryDefaults(collections="imported_g")
        results = butler._query_all_datasets(where="detector = 2")
        self.assertCountEqual(
            _ref_uuids(results),
            ["51352db4-a47a-447c-b12d-a50b206b17cd", "60c8a65c-7290-4c38-b1de-e3b1cdcf872d"],
        )

    def test_irrelevant_governor_constraints(self) -> None:
        """Test that constraining an irrelevant governor dimension doesn't
        break dataset queries.
        """
        butler = self.make_butler("base.yaml", "spatial.yaml")
        butler.registry.insertDimensionData("instrument", {"name": "Cam2"})
        a = DatasetType("a", {"detector"}, "StructuredDataDict", universe=butler.dimensions)
        b = DatasetType("b", {"tract"}, "StructuredDataDict", universe=butler.dimensions)
        butler.registry.registerDatasetType(a)
        butler.registry.registerDatasetType(b)
        collection = "run1"
        butler.collections.register(collection)
        (ref_a,) = butler.registry.insertDatasets(a, [{"instrument": "Cam1", "detector": 2}], run=collection)
        (ref_b,) = butler.registry.insertDatasets(b, [{"skymap": "SkyMap1", "tract": 1}], run=collection)
        # First, some sanity-check query that's mostly to check the test setup.
        self.assertEqual(butler.query_datasets("a", collections=collection), [ref_a])
        self.assertEqual(butler.query_datasets("b", collections=collection), [ref_b])
        # Now check that we can get both with an irrelevant constraint.
        # In the first case, there is a dataset of a different type that is
        # consistent with the constraint in the collection:
        self.assertEqual(butler.query_datasets("a", collections=collection, skymap="SkyMap1"), [ref_a])
        # In the second case there is a dataset of a different type that is
        # inconsistent with the constraint in the collection:
        self.assertEqual(butler.query_datasets("b", collections=collection, instrument="Cam2"), [ref_b])


def _get_exposure_ids_from_dimension_records(dimension_records: Iterable[DimensionRecord]) -> list[int]:
    output = []
    for rec in dimension_records:
        id = rec.dataId["exposure"]
        assert isinstance(id, int)
        output.append(id)

    return output


def _ref_uuids(refs: list[DatasetRef]) -> list[str]:
    return [str(ref.id) for ref in refs]

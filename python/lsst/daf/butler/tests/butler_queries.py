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
from typing import ClassVar

import astropy.time

from .._butler import Butler
from .._dataset_type import DatasetType
from .._exceptions import InvalidQueryError
from .._timespan import Timespan
from ..dimensions import DataCoordinate, DimensionRecord
from ..direct_query_driver import DirectQueryDriver
from ..queries import DimensionRecordQueryResults
from ..registry import CollectionType, NoDefaultCollectionError, RegistryDefaults
from ..registry.sql_registry import SqlRegistry
from ..transfers import YamlRepoImportBackend
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
        with butler._query() as query:
            _x = query.expression_factory
            results = query.dimension_records("detector")
            self.check_detector_records(results)
            self.check_detector_records(results.order_by("detector"), ordered=True)
            self.check_detector_records(
                results.order_by(_x.detector.full_name.desc), [4, 3, 2, 1], ordered=True
            )
            self.check_detector_records(results.order_by("detector").limit(2), [1, 2], ordered=True)
            self.check_detector_records(results.where(_x.detector.raft == "B", instrument="Cam1"), [3, 4])

    def test_implied_union_record_query(self) -> None:
        """Test queries for a dimension ('band') that uses "implied union"
        storage, in which its values are the union of the values for it in a
        another dimension (physical_filter) that implies it.
        """
        butler = self.make_butler("base.yaml")
        band = butler.dimensions["band"]
        self.assertEqual(band.implied_union_target, butler.dimensions["physical_filter"])
        with butler._query() as query:
            self.assertCountEqual(
                list(query.dimension_records("band")),
                [band.RecordClass(name="g"), band.RecordClass(name="r")],
            )
            self.assertCountEqual(
                list(query.where(physical_filter="Cam1-R1", instrument="Cam1").dimension_records("band")),
                [band.RecordClass(name="r")],
            )

    def test_dataset_constrained_record_query(self) -> None:
        """Test a query for dimension records constrained by the existence of
        datasets of a particular type.
        """
        butler = self.make_butler("base.yaml", "datasets.yaml")
        butler.registry.insertDimensionData("instrument", {"name": "Cam2"})
        butler.registry.registerCollection("empty", CollectionType.RUN)
        butler.registry.registerCollection("chain", CollectionType.CHAINED)
        butler.registry.setCollectionChain("chain", ["imported_g", "empty", "imported_r"])
        with butler._query() as query:
            # No collections here or in defaults is an error.
            with self.assertRaises(NoDefaultCollectionError):
                query.join_dataset_search("bias").dimension_records("detector").any()
        butler.registry.defaults = RegistryDefaults(collections=["chain"])
        with butler._query() as query:
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
                    "Search for dataset type 'bias' is doomed to fail.",
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
                    "Search for dataset type 'bias' is doomed to fail.",
                    "No datasets with instrument='Cam2' in collection 'imported_g'.",
                ],
                doomed=True,
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
        with butler._query() as query:
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
        with butler._query() as query:
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

    def test_data_coordinate_upload(self) -> None:
        """Test queries for dimension records with a data coordinate upload."""
        butler = self.make_butler("base.yaml", "spatial.yaml")
        with butler._query() as query:
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
        with butler._query() as query:
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
        with butler._query() as query:
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
                query.join_dimensions(["visit", "detector", "tract"]).materialize()
                # The patch constraint here should do nothing, because only the
                # spatial join from the materialization should exist.  The
                # behavior is surprising no matter what here, and the
                # recommendation to users is to add an explicit overlap
                # expression any time it's not obvious what the default is.
                .where(skymap="SkyMap1", tract=0, instrument="Cam1", visit=2, patch=5).dimension_records(
                    "detector"
                ),
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

    def test_timespan_results(self) -> None:
        """Test returning dimension records that include timespans."""
        butler = self.make_butler("base.yaml", "spatial.yaml")
        with butler._query() as query:
            self.assertCountEqual(
                [
                    (record.id, record.timespan.begin, record.timespan.end)
                    for record in query.dimension_records("visit")
                ],
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
        making the threshold for making a a temporary table tiny).

        This test assumes a DirectQueryDriver and is automatically skipped when
        some other driver is found.
        """
        butler = self.make_butler("base.yaml")
        # Basic test where pages should be transparent.
        with butler._query() as query:
            if not isinstance(query._driver, DirectQueryDriver):
                raise unittest.SkipTest("Test requires meddling with DirectQueryDriver internals.")
            query._driver._raw_page_size = 2
            self.check_detector_records(
                query.dimension_records("detector"),
                [1, 2, 3, 4],
            )
        # Test that it's an error to continue query iteration after closing the
        # context manager.
        with butler._query() as query:
            assert isinstance(query._driver, DirectQueryDriver)
            query._driver._raw_page_size = 2
            iterator = iter(query.dimension_records("detector"))
            next(iterator)
        with self.assertRaisesRegex(RuntimeError, "Cannot continue query result iteration"):
            list(iterator)

    def test_column_expressions(self) -> None:
        """Test queries with a wide variant of column expressions."""
        butler = self.make_butler("base.yaml", "spatial.yaml")
        butler.registry.defaults = RegistryDefaults(instrument="Cam1")
        with butler._query() as query:
            _x = query.expression_factory
            self.check_detector_records(
                query.where(_x.not_(_x.detector != 2)).dimension_records("detector"),
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

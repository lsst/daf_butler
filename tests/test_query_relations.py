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

import difflib
import os
import os.path
import re
import unittest

from lsst.daf.butler.registry import MissingSpatialOverlapError, Registry, RegistryConfig, queries
from lsst.daf.butler.transfers import YamlRepoImportBackend

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class TestQueryRelationsTests(unittest.TestCase):
    """Tests for registry queries that check that the generated relation tree
    matches expectations.

    These tests are somewhat fragile - there are multiple valid relation trees
    for most registry queries, just as there are multiple valid SQL queries,
    and since we check the relation tree via string comparisons we are
    also sensitive to irrelevant things like column ordering.  But these
    differences are deterministic, and checking the relation trees instead of
    the query results puts a much smaller burden on test-data creation and
    inspection (as well as making tests go faster), making it much easier to
    test many combinations of arguments.

    Note that daf_relation provides good test coverage of the process of going
    from relation trees to SQL.
    """

    @classmethod
    def setUpClass(cls) -> None:
        config = RegistryConfig()
        config["db"] = "sqlite://"
        cls.registry = Registry.createFromConfig(config)
        # We need just enough test data to have valid dimension records for
        # all of the dimensions we're concerned with, and we want to pick
        # values for each dimension that correspond to a spatiotemporal
        # overlap.  Without that, we'd be fighting optimizations built into the
        # query system that simplify things as soon as it can spot that there
        # will be no overall results.
        data_file = os.path.normpath(os.path.join(TESTDIR, "data", "registry", "hsc-rc2-subset.yaml"))
        with open(data_file, "r") as stream:
            backend = YamlRepoImportBackend(stream, cls.registry)
        backend.register()
        backend.load(datastore=None)
        assert (
            cls.registry.dimensions.commonSkyPix.name == "htm7"
        ), "If this changes, update the skypix levels below to have one below and one above."
        cls.htm7 = 222340
        cls.htm11 = 56919188
        cls.instrument = "HSC"
        cls.skymap = "hsc_rings_v1"
        cls.visit = 404
        cls.tract = 9615
        cls.detector = 0
        cls.patch = 14
        cls.data_id = cls.registry.expandDataId(
            htm7=cls.htm7,
            htm11=cls.htm11,
            instrument=cls.instrument,
            skymap=cls.skymap,
            visit=cls.visit,
            tract=cls.tract,
            detector=cls.detector,
            patch=cls.patch,
        )
        cls.band = cls.data_id["band"]
        cls.physical_filter = cls.data_id["physical_filter"]

    def assert_relation_str(
        self,
        expected: str,
        *results: queries.DataCoordinateQueryResults
        | queries.DimensionRecordQueryResults
        | queries.ParentDatasetQueryResults,
    ) -> None:
        """A specialized test assert that checks that one or more registry
        queries have relation trees that match the given string.

        Parameters
        ----------
        expected : `str`
            Expected relation tree, corresponding to
            ``lsst.daf.relation.Relation.__str__`` (which is much more concise
            and readable than the `repr` version, once you get used to it).
            Any newlines and indentation will be stripped.
        *results
            Query result objects returned by queryDataIds,
            queryDimensionRecords, or queryDatasets.
        """
        # Drop newlines and leading/trailing space.
        expected = expected.replace("\n", " ").strip()
        # Drop duplicate spaces (i.e. indentation).
        expected = re.sub(r" \s+", " ", expected)
        # Drop spaces next to parentheses and square brackets.
        expected = re.sub(r"\s*(\[|\(|\)|\])\s*", r"\1", expected)
        differ = difflib.Differ()
        for n, result in enumerate(results):
            result_str = str(result._query.relation)
            if expected != result_str:
                message_lines = [f"Unexpected relation string for query {n}:"]
                message_lines.extend(
                    differ.compare(
                        [expected],
                        [result_str],
                    )
                )
                raise AssertionError("\n".join(message_lines))

    def test_spatial_constraints(self) -> None:
        """Test query constraints from data IDs and WHERE clauses that imply a
        spatial region.
        """
        # Constrain one set of regular spatial dimensions from another.
        # This needs post-query filtering in the iteration engine.
        self.assert_relation_str(
            f"""
            Π[band, patch, skymap, tract](
                σ[regions_overlap(patch.region, visit_detector_region.region)](
                    →[iteration](
                        select(
                            Π[band, patch, patch.region, skymap, tract, visit_detector_region.region](
                                σ[
                                    band={self.band!r}
                                    and instrument={self.instrument!r}
                                    and detector={self.detector!r}
                                    and physical_filter={self.physical_filter!r}
                                    and visit={self.visit!r}
                                ](
                                    patch_htm7_overlap
                                    ⋈ visit_detector_region_htm7_overlap
                                    ⋈ physical_filter
                                    ⋈ patch
                                    ⋈ visit
                                    ⋈ visit_detector_region
                                )
                            )
                        )
                    )
                )
            )
            """,
            self.registry.queryDataIds(
                ["patch", "band"], instrument=self.instrument, visit=self.visit, detector=self.detector
            ),
            self.registry.queryDataIds(
                ["patch", "band"],
                where=(
                    f"band={self.band!r} "
                    f"and instrument={self.instrument!r} "
                    f"and detector={self.detector!r} "
                    f"and physical_filter={self.physical_filter!r} "
                    f"and visit={self.visit!r}"
                ),
            ),
        )
        # Constrain the special common skypix dimension from a regular
        # dimension.  This does not need any post-query filtering.
        self.assert_relation_str(
            # It would be better if this query didn't join in visit and
            # physical_filter - it does that to ensure all implied dimension
            # relationships are satisfied in the results, but the dimensions
            # implied by visit are not present in the results and play no role
            # in the constraints.  But it'd be hard to fix that and any fix
            # would be very rarely exercised.
            f"""
            select(
                Π[htm7](
                    σ[
                        band={self.band!r}
                        and instrument={self.instrument!r}
                        and detector={self.detector!r}
                        and physical_filter={self.physical_filter!r}
                        and visit={self.visit!r}
                    ](
                        visit_detector_region_htm7_overlap
                        ⋈ physical_filter
                        ⋈ visit
                    )
                )
            )
            """,
            self.registry.queryDataIds(
                ["htm7"], instrument=self.instrument, visit=self.visit, detector=self.detector
            ),
            # For regular dimension constraints we can also support having the
            # data ID expressed as a 'where' expression.  The query would also
            # have the same behavior with only visit and detector specified
            # in the 'where' string, but it'd change the expected string.
            self.registry.queryDataIds(
                ["htm7"],
                where=(
                    f"band={self.band!r} "
                    f"and instrument={self.instrument!r} "
                    f"and detector={self.detector!r} "
                    f"and physical_filter={self.physical_filter!r} "
                    f"and visit={self.visit!r}"
                ),
            ),
        )
        # We can't constrain any other skypix system spatially, because we
        # don't have overlap rows for those in the database.  But in the future
        # we might be able to fake it with an iteration-engine spatial join, or
        # utilize explicitly-materialized overlaps.
        with self.assertRaises(MissingSpatialOverlapError):
            self.registry.queryDataIds(
                ["htm11"],
                instrument=self.instrument,
                visit=self.visit,
                detector=self.detector,
            )
        # Constrain a regular spatial dimension (patch) from a non-common
        # skypix dimension common.  In general this requires post-query
        # filtering to get only the patches that overlap the skypix pixel.  We
        # could special-case skypix dimensions that are coarser than the common
        # dimension and part of the same system to simplify both the SQL query
        # and avoid post-query filtering, but we don't at present.
        self.assert_relation_str(
            f"""
            Π[patch, skymap, tract](
                σ[
                    regions_overlap(
                        patch.region,
                        {self.registry.dimensions["htm11"].pixelization.pixel(self.htm11)}
                    )
                ](
                    →[iteration](
                        select(
                            Π[patch, patch.region, skymap, tract](
                                σ[htm7={self.htm7!r}](
                                    patch_htm7_overlap ⋈ patch
                                )
                            )
                        )
                    )
                )
            )
            """,
            self.registry.queryDataIds(["patch"], htm11=self.htm11),
        )
        # Constrain a regular spatial dimension (patch) from the common
        # skypix dimension.  This does not require post-query filtering.
        self.assert_relation_str(
            f"""
            select(
                Π[patch, skymap, tract](
                    σ[htm7={self.htm7!r}](
                        patch_htm7_overlap
                    )
                )
            )
            """,
            self.registry.queryDataIds(["patch"], htm7=self.htm7),
        )
        # Constrain a regular dimension (detector) via a different dimension
        # (visit) that combine together to define a more fine-grained region,
        # and also constrain via a skypix dimension other than the common one.
        # Once again we could special-case this for skypix dimensions that are
        # coarser than the common dimension in the same syste, but we don't.
        self.assert_relation_str(
            # This query also doesn't need visit or physical_filter joined in,
            # but we can live with that.
            f"""
            Π[detector, instrument](
                σ[
                    regions_overlap(
                        visit_detector_region.region,
                        {self.registry.dimensions["htm11"].pixelization.pixel(self.htm11)}
                    )
                ](
                    →[iteration](
                        select(
                            Π[detector, instrument, visit_detector_region.region](
                                σ[
                                    band={self.band!r}
                                    and instrument={self.instrument!r}
                                    and physical_filter={self.physical_filter!r}
                                    and visit={self.visit!r}
                                    and htm7={self.htm7!r}
                                ](
                                    visit_detector_region_htm7_overlap
                                    ⋈ physical_filter
                                    ⋈ visit
                                    ⋈ visit_detector_region
                                )
                            )
                        )
                    )
                )
            )
            """,
            self.registry.queryDataIds(
                ["detector"], visit=self.visit, instrument=self.instrument, htm11=self.htm11
            ),
        )
        # Constrain a regular dimension (detector) via a different dimension
        # (visit) that combine together to define a more fine-grained region,
        # and also constrain via the common-skypix system.
        self.assert_relation_str(
            # This query also doesn't need visit or physical_filter joined in,
            # but we can live with that.
            f"""
            select(
                Π[detector, instrument](
                    σ[
                        band={self.band!r}
                        and htm7={self.htm7!r}
                        and instrument={self.instrument!r}
                        and physical_filter={self.physical_filter!r}
                        and visit={self.visit!r}
                    ](
                        visit_detector_region_htm7_overlap
                        ⋈ physical_filter
                        ⋈ visit
                    )
                )
            )
            """,
            self.registry.queryDataIds(
                ["detector"], visit=self.visit, instrument=self.instrument, htm7=self.htm7
            ),
        )


if __name__ == "__main__":
    unittest.main()

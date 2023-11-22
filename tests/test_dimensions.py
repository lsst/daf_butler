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

import copy
import itertools
import math
import os
import pickle
import unittest
from collections.abc import Iterator
from dataclasses import dataclass
from random import Random

import lsst.sphgeom
from lsst.daf.butler import (
    Config,
    DataCoordinate,
    DataCoordinateSequence,
    DataCoordinateSet,
    Dimension,
    DimensionConfig,
    DimensionElement,
    DimensionGroup,
    DimensionPacker,
    DimensionUniverse,
    NamedKeyDict,
    NamedValueSet,
    TimespanDatabaseRepresentation,
    YamlRepoImportBackend,
    ddl,
)
from lsst.daf.butler.registry import RegistryConfig, _RegistryFactory

DIMENSION_DATA_FILE = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "data", "registry", "hsc-rc2-subset.yaml")
)


def loadDimensionData() -> DataCoordinateSequence:
    """Load dimension data from an export file included in the code repository.

    Returns
    -------
    dataIds : `DataCoordinateSet`
        A set containing all data IDs in the export file.
    """
    # Create an in-memory SQLite database and Registry just to import the YAML
    # data and retreive it as a set of DataCoordinate objects.
    config = RegistryConfig()
    config["db"] = "sqlite://"
    registry = _RegistryFactory(config).create_from_config()
    with open(DIMENSION_DATA_FILE) as stream:
        backend = YamlRepoImportBackend(stream, registry)
    backend.register()
    backend.load(datastore=None)
    dimensions = registry.dimensions.conform(["visit", "detector", "tract", "patch"])
    return registry.queryDataIds(dimensions).expanded().toSequence()


class ConcreteTestDimensionPacker(DimensionPacker):
    """A concrete `DimensionPacker` for testing its base class implementations.

    This class just returns the detector ID as-is.
    """

    def __init__(self, fixed: DataCoordinate, dimensions: DimensionGroup):
        super().__init__(fixed, dimensions)
        self._n_detectors = fixed.records["instrument"].detector_max
        self._max_bits = (self._n_detectors - 1).bit_length()

    @property
    def maxBits(self) -> int:
        # Docstring inherited from DimensionPacker.maxBits
        return self._max_bits

    def _pack(self, dataId: DataCoordinate) -> int:
        # Docstring inherited from DimensionPacker._pack
        return dataId["detector"]

    def unpack(self, packedId: int) -> DataCoordinate:
        # Docstring inherited from DimensionPacker.unpack
        return DataCoordinate.standardize(
            {
                "instrument": self.fixed["instrument"],
                "detector": packedId,
            },
            dimensions=self._dimensions,
        )


class DimensionTestCase(unittest.TestCase):
    """Tests for dimensions.

    All tests here rely on the content of ``config/dimensions.yaml``, either
    to test that the definitions there are read in properly or just as generic
    data for testing various operations.
    """

    def setUp(self):
        self.universe = DimensionUniverse()

    def checkGroupInvariants(self, group: DimensionGroup):
        elements = list(group.elements)
        for n, element_name in enumerate(elements):
            element = self.universe[element_name]
            # Ordered comparisons on graphs behave like sets.
            self.assertLessEqual(element.minimal_group, group)
            # Ordered comparisons on elements correspond to the ordering within
            # a DimensionUniverse (topological, with deterministic
            # tiebreakers).
            for other_name in elements[:n]:
                other = self.universe[other_name]
                self.assertLess(other, element)
                self.assertLessEqual(other, element)
            for other_name in elements[n + 1 :]:
                other = self.universe[other_name]
                self.assertGreater(other, element)
                self.assertGreaterEqual(other, element)
            if isinstance(element, Dimension):
                self.assertEqual(element.minimal_group.required, element.required)
        self.assertEqual(self.universe.conform(group.required), group)
        self.assertCountEqual(
            group.required,
            [
                dimension_name
                for dimension_name in group.names
                if not any(
                    dimension_name in self.universe[other_name].minimal_group.implied
                    for other_name in group.elements
                )
            ],
        )
        self.assertCountEqual(group.implied, group.names - group.required)
        self.assertCountEqual(group.names, itertools.chain(group.required, group.implied))
        # Check primary key traversal order: each element should follow any it
        # requires, and element that is implied by any other in the graph
        # follow at least one of those.
        seen: set[str] = set()
        for element_name in group.lookup_order:
            element = self.universe[element_name]
            with self.subTest(required=group.required, implied=group.implied, element=element):
                seen.add(element_name)
                self.assertLessEqual(element.minimal_group.required, seen)
                if element_name in group.implied:
                    self.assertTrue(any(element_name in self.universe[s].implied for s in seen))
        self.assertCountEqual(seen, group.elements)

    def testConfigPresent(self):
        config = self.universe.dimensionConfig
        self.assertIsInstance(config, DimensionConfig)

    def testCompatibility(self):
        # Simple check that should always be true.
        self.assertTrue(self.universe.isCompatibleWith(self.universe))

        # Create a universe like the default universe but with a different
        # version number.
        clone = self.universe.dimensionConfig.copy()
        clone["version"] = clone["version"] + 1_000_000  # High version number
        universe_clone = DimensionUniverse(config=clone)
        with self.assertLogs("lsst.daf.butler.dimensions", "INFO") as cm:
            self.assertTrue(self.universe.isCompatibleWith(universe_clone))
        self.assertIn("differing versions", "\n".join(cm.output))

        # Create completely incompatible universe.
        config = Config(
            {
                "version": 1,
                "namespace": "compat_test",
                "skypix": {
                    "common": "htm7",
                    "htm": {
                        "class": "lsst.sphgeom.HtmPixelization",
                        "max_level": 24,
                    },
                },
                "elements": {
                    "A": {
                        "keys": [
                            {
                                "name": "id",
                                "type": "int",
                            }
                        ],
                        "storage": {
                            "cls": "lsst.daf.butler.registry.dimensions.table.TableDimensionRecordStorage",
                        },
                    },
                    "B": {
                        "keys": [
                            {
                                "name": "id",
                                "type": "int",
                            }
                        ],
                        "storage": {
                            "cls": "lsst.daf.butler.registry.dimensions.table.TableDimensionRecordStorage",
                        },
                    },
                },
                "packers": {},
            }
        )
        universe2 = DimensionUniverse(config=config)
        self.assertFalse(universe2.isCompatibleWith(self.universe))

    def testVersion(self):
        self.assertEqual(self.universe.namespace, "daf_butler")
        # Test was added starting at version 2.
        self.assertGreaterEqual(self.universe.version, 2)

    def testConfigRead(self):
        self.assertEqual(
            set(self.universe.getStaticDimensions().names),
            {
                "instrument",
                "visit",
                "visit_system",
                "exposure",
                "detector",
                "physical_filter",
                "band",
                "subfilter",
                "skymap",
                "tract",
                "patch",
            }
            | {f"htm{level}" for level in range(25)}
            | {f"healpix{level}" for level in range(18)},
        )

    def testGraphs(self):
        self.checkGroupInvariants(self.universe.empty.as_group())
        for element in self.universe.getStaticElements():
            self.checkGroupInvariants(element.minimal_group)

    def testInstrumentDimensions(self):
        group = self.universe.conform(["exposure", "detector", "visit"])
        self.assertCountEqual(
            group.names,
            ("instrument", "exposure", "detector", "visit", "physical_filter", "band"),
        )
        self.assertCountEqual(group.required, ("instrument", "exposure", "detector", "visit"))
        self.assertCountEqual(group.implied, ("physical_filter", "band"))
        self.assertCountEqual(group.elements - group.names, ("visit_detector_region", "visit_definition"))
        self.assertCountEqual(group.governors, {"instrument"})

    def testCalibrationDimensions(self):
        group = self.universe.conform(["physical_filter", "detector"])
        self.assertCountEqual(group.names, ("instrument", "detector", "physical_filter", "band"))
        self.assertCountEqual(group.required, ("instrument", "detector", "physical_filter"))
        self.assertCountEqual(group.implied, ("band",))
        self.assertCountEqual(group.elements, group.names)
        self.assertCountEqual(group.governors, {"instrument"})

    def testObservationDimensions(self):
        group = self.universe.conform(["exposure", "detector", "visit"])
        self.assertCountEqual(
            group.names,
            ("instrument", "detector", "visit", "exposure", "physical_filter", "band"),
        )
        self.assertCountEqual(group.required, ("instrument", "detector", "exposure", "visit"))
        self.assertCountEqual(group.implied, ("physical_filter", "band"))
        self.assertCountEqual(group.elements - group.names, ("visit_detector_region", "visit_definition"))
        self.assertCountEqual(group.spatial.names, ("observation_regions",))
        self.assertCountEqual(group.temporal.names, ("observation_timespans",))
        self.assertCountEqual(group.governors, {"instrument"})
        self.assertEqual(group.spatial.names, {"observation_regions"})
        self.assertEqual(group.temporal.names, {"observation_timespans"})
        self.assertEqual(next(iter(group.spatial)).governor, self.universe["instrument"])
        self.assertEqual(next(iter(group.temporal)).governor, self.universe["instrument"])
        self.assertEqual(self.universe["visit_definition"].populated_by, self.universe["visit"])
        self.assertEqual(self.universe["visit_system_membership"].populated_by, self.universe["visit"])
        self.assertEqual(self.universe["visit_detector_region"].populated_by, self.universe["visit"])
        self.assertEqual(
            self.universe.get_elements_populated_by(self.universe["visit"]),
            NamedValueSet(
                {
                    self.universe["visit"],
                    self.universe["visit_definition"],
                    self.universe["visit_system_membership"],
                    self.universe["visit_detector_region"],
                }
            ),
        )

    def testSkyMapDimensions(self):
        group = self.universe.conform(["patch"])
        self.assertEqual(group.names, {"skymap", "tract", "patch"})
        self.assertEqual(group.required, {"skymap", "tract", "patch"})
        self.assertEqual(group.implied, set())
        self.assertEqual(group.elements, group.names)
        self.assertEqual(group.governors, {"skymap"})
        self.assertEqual(group.spatial.names, {"skymap_regions"})
        self.assertEqual(next(iter(group.spatial)).governor, self.universe["skymap"])

    def testSubsetCalculation(self):
        """Test that independent spatial and temporal options are computed
        correctly.
        """
        group = self.universe.conform(["visit", "detector", "tract", "patch", "htm7", "exposure"])
        self.assertCountEqual(group.spatial.names, ("observation_regions", "skymap_regions", "htm"))
        self.assertCountEqual(group.temporal.names, ("observation_timespans",))

    def testSchemaGeneration(self):
        tableSpecs: NamedKeyDict[DimensionElement, ddl.TableSpec] = NamedKeyDict({})
        for element in self.universe.getStaticElements():
            if element.hasTable and element.viewOf is None:
                tableSpecs[element] = element.RecordClass.fields.makeTableSpec(
                    TimespanReprClass=TimespanDatabaseRepresentation.Compound,
                )
        for element, tableSpec in tableSpecs.items():
            for dep in element.required:
                with self.subTest(element=element.name, dep=dep.name):
                    if dep != element:
                        self.assertIn(dep.name, tableSpec.fields)
                        self.assertEqual(tableSpec.fields[dep.name].dtype, dep.primaryKey.dtype)
                        self.assertEqual(tableSpec.fields[dep.name].length, dep.primaryKey.length)
                        self.assertEqual(tableSpec.fields[dep.name].nbytes, dep.primaryKey.nbytes)
                        self.assertFalse(tableSpec.fields[dep.name].nullable)
                        self.assertTrue(tableSpec.fields[dep.name].primaryKey)
                    else:
                        self.assertIn(element.primaryKey.name, tableSpec.fields)
                        self.assertEqual(
                            tableSpec.fields[element.primaryKey.name].dtype, dep.primaryKey.dtype
                        )
                        self.assertEqual(
                            tableSpec.fields[element.primaryKey.name].length, dep.primaryKey.length
                        )
                        self.assertEqual(
                            tableSpec.fields[element.primaryKey.name].nbytes, dep.primaryKey.nbytes
                        )
                        self.assertFalse(tableSpec.fields[element.primaryKey.name].nullable)
                        self.assertTrue(tableSpec.fields[element.primaryKey.name].primaryKey)
            for dep in element.implied:
                with self.subTest(element=element.name, dep=dep.name):
                    self.assertIn(dep.name, tableSpec.fields)
                    self.assertEqual(tableSpec.fields[dep.name].dtype, dep.primaryKey.dtype)
                    self.assertFalse(tableSpec.fields[dep.name].primaryKey)
            for foreignKey in tableSpec.foreignKeys:
                self.assertIn(foreignKey.table, tableSpecs)
                self.assertIn(foreignKey.table, element.dimensions)
                self.assertEqual(len(foreignKey.source), len(foreignKey.target))
                for source, target in zip(foreignKey.source, foreignKey.target, strict=True):
                    self.assertIn(source, tableSpec.fields.names)
                    self.assertIn(target, tableSpecs[foreignKey.table].fields.names)
                    self.assertEqual(
                        tableSpec.fields[source].dtype, tableSpecs[foreignKey.table].fields[target].dtype
                    )
                    self.assertEqual(
                        tableSpec.fields[source].length, tableSpecs[foreignKey.table].fields[target].length
                    )
                    self.assertEqual(
                        tableSpec.fields[source].nbytes, tableSpecs[foreignKey.table].fields[target].nbytes
                    )

    def testPickling(self):
        # Pickling and copying should always yield the exact same object within
        # a single process (cross-process is impossible to test here).
        universe1 = DimensionUniverse()
        universe2 = pickle.loads(pickle.dumps(universe1))
        universe3 = copy.copy(universe1)
        universe4 = copy.deepcopy(universe1)
        self.assertIs(universe1, universe2)
        self.assertIs(universe1, universe3)
        self.assertIs(universe1, universe4)
        for element1 in universe1.getStaticElements():
            element2 = pickle.loads(pickle.dumps(element1))
            self.assertIs(element1, element2)
            group1 = element1.minimal_group
            group2 = pickle.loads(pickle.dumps(group1))
            self.assertIs(group1, group2)


@dataclass
class SplitByStateFlags:
    """A struct that separates data IDs with different states but the same
    values.
    """

    minimal: DataCoordinateSequence | None = None
    """Data IDs that only contain values for required dimensions.

    `DataCoordinateSequence.hasFull()` will return `True` for this if and only
    if ``minimal.dimensions.implied`` has no elements.
    `DataCoordinate.hasRecords()` will always return `False`.
    """

    complete: DataCoordinateSequence | None = None
    """Data IDs that contain values for all dimensions.

    `DataCoordinateSequence.hasFull()` will always `True` and
    `DataCoordinate.hasRecords()` will always return `True` for this attribute.
    """

    expanded: DataCoordinateSequence | None = None
    """Data IDs that contain values for all dimensions as well as records.

    `DataCoordinateSequence.hasFull()` and `DataCoordinate.hasRecords()` will
    always return `True` for this attribute.
    """

    def chain(self, n: int | None = None) -> Iterator[DataCoordinate]:
        """Iterate over the data IDs of different types.

        Parameters
        ----------
        n : `int`, optional
            If provided (`None` is default), iterate over only the ``nth``
            data ID in each attribute.

        Yields
        ------
        dataId : `DataCoordinate`
            A data ID from one of the attributes in this struct.
        """
        if n is None:
            s = slice(None, None)
        else:
            s = slice(n, n + 1)
        if self.minimal is not None:
            yield from self.minimal[s]
        if self.complete is not None:
            yield from self.complete[s]
        if self.expanded is not None:
            yield from self.expanded[s]


class DataCoordinateTestCase(unittest.TestCase):
    """Test `DataCoordinate`."""

    RANDOM_SEED = 10

    @classmethod
    def setUpClass(cls):
        cls.allDataIds = loadDimensionData()

    def setUp(self):
        self.rng = Random(self.RANDOM_SEED)

    def randomDataIds(self, n: int, dataIds: DataCoordinateSequence | None = None):
        """Select random data IDs from those loaded from test data.

        Parameters
        ----------
        n : `int`
             Number of data IDs to select.
        dataIds : `DataCoordinateSequence`, optional
            Data IDs to select from.  Defaults to ``self.allDataIds``.

        Returns
        -------
        selected : `DataCoordinateSequence`
            ``n`` Data IDs randomly selected from ``dataIds`` with replacement.
        """
        if dataIds is None:
            dataIds = self.allDataIds
        return DataCoordinateSequence(
            self.rng.sample(dataIds, n),
            dimensions=dataIds.dimensions,
            hasFull=dataIds.hasFull(),
            hasRecords=dataIds.hasRecords(),
            check=False,
        )

    def randomDimensionSubset(self, n: int = 3, group: DimensionGroup | None = None) -> DimensionGroup:
        """Generate a random `DimensionGroup` that has a subset of the
        dimensions in a given one.

        Parameters
        ----------
        n : `int`
             Number of dimensions to select, before automatic expansion by
             `DimensionGroup`.
        group : `DimensionGroup`, optional
            Dimensions to select from.  Defaults to
            ``self.allDataIds.dimensions``.

        Returns
        -------
        selected : `DimensionGroup`
            ``n`` or more dimensions randomly selected from ``group`` with
            replacement.
        """
        if group is None:
            group = self.allDataIds.dimensions
        return group.universe.conform(self.rng.sample(list(group.names), max(n, len(group))))

    def splitByStateFlags(
        self,
        dataIds: DataCoordinateSequence | None = None,
        *,
        expanded: bool = True,
        complete: bool = True,
        minimal: bool = True,
    ) -> SplitByStateFlags:
        """Given a sequence of data IDs, generate new equivalent sequences
        containing less information.

        Parameters
        ----------
        dataIds : `DataCoordinateSequence`, optional.
            Data IDs to start from.  Defaults to ``self.allDataIds``.
            ``dataIds.hasRecords()`` and ``dataIds.hasFull()`` must both return
            `True`.
        expanded : `bool`, optional
            If `True` (default) include the original data IDs that contain all
            information in the result.
        complete : `bool`, optional
            If `True` (default) include data IDs for which ``hasFull()``
            returns `True` but ``hasRecords()`` does not.
        minimal : `bool`, optional
            If `True` (default) include data IDS that only contain values for
            required dimensions, for which ``hasFull()`` may not return `True`.

        Returns
        -------
        split : `SplitByStateFlags`
            A dataclass holding the indicated data IDs in attributes that
            correspond to the boolean keyword arguments.
        """
        if dataIds is None:
            dataIds = self.allDataIds
        assert dataIds.hasFull() and dataIds.hasRecords()
        result = SplitByStateFlags(expanded=dataIds)
        if complete:
            result.complete = DataCoordinateSequence(
                [
                    DataCoordinate.standardize(e.mapping, dimensions=dataIds.dimensions)
                    for e in result.expanded
                ],
                dimensions=dataIds.dimensions,
            )
            self.assertTrue(result.complete.hasFull())
            self.assertFalse(result.complete.hasRecords())
        if minimal:
            result.minimal = DataCoordinateSequence(
                [
                    DataCoordinate.standardize(e.required, dimensions=dataIds.dimensions)
                    for e in result.expanded
                ],
                dimensions=dataIds.dimensions,
            )
            self.assertEqual(result.minimal.hasFull(), not dataIds.dimensions.implied)
            self.assertFalse(result.minimal.hasRecords())
        if not expanded:
            result.expanded = None
        return result

    def testMappingViews(self):
        """Test that the ``mapping`` and ``required`` attributes in
        `DataCoordinate` are self-consistent and consistent with the
        ``dimensions`` property.
        """
        for _ in range(5):
            dimensions = self.randomDimensionSubset()
            dataIds = self.randomDataIds(n=1).subset(dimensions)
            split = self.splitByStateFlags(dataIds)
            for dataId in split.chain():
                with self.subTest(dataId=dataId):
                    self.assertEqual(dataId.required.keys(), dataId.dimensions.required)
                    self.assertEqual(
                        list(dataId.required.values()), [dataId[d] for d in dataId.dimensions.required]
                    )
                    self.assertEqual(
                        list(dataId.required_values), [dataId[d] for d in dataId.dimensions.required]
                    )
                    self.assertEqual(dataId.required.keys(), dataId.dimensions.required)
            for dataId in itertools.chain(split.complete, split.expanded):
                with self.subTest(dataId=dataId):
                    self.assertTrue(dataId.hasFull())
                    self.assertEqual(dataId.dimensions.names, dataId.mapping.keys())
                    self.assertEqual(
                        list(dataId.mapping.values()), [dataId[k] for k in dataId.mapping.keys()]
                    )

    def test_pickle(self):
        for _ in range(5):
            dimensions = self.randomDimensionSubset()
            dataIds = self.randomDataIds(n=1).subset(dimensions)
            split = self.splitByStateFlags(dataIds)
            for data_id in split.chain():
                s = pickle.dumps(data_id)
                read_data_id: DataCoordinate = pickle.loads(s)
                self.assertEqual(data_id, read_data_id)
                self.assertEqual(data_id.hasFull(), read_data_id.hasFull())
                self.assertEqual(data_id.hasRecords(), read_data_id.hasRecords())
                if data_id.hasFull():
                    self.assertEqual(data_id.mapping, read_data_id.mapping)
                    if data_id.hasRecords():
                        for element_name in data_id.dimensions.elements:
                            self.assertEqual(
                                data_id.records[element_name], read_data_id.records[element_name]
                            )

    def test_record_attributes(self):
        """Test that dimension records are available as attributes on expanded
        data coordinates.
        """
        for _ in range(5):
            dimensions = self.randomDimensionSubset()
            dataIds = self.randomDataIds(n=1).subset(dimensions)
            split = self.splitByStateFlags(dataIds)
            for data_id in split.expanded:
                for element_name in data_id.dimensions.elements:
                    self.assertIs(getattr(data_id, element_name), data_id.records[element_name])
                    self.assertIn(element_name, dir(data_id))
                with self.assertRaisesRegex(AttributeError, "^not_a_dimension_name$"):
                    data_id.not_a_dimension_name
            for data_id in itertools.chain(split.minimal, split.complete):
                for element_name in data_id.dimensions.elements:
                    with self.assertRaisesRegex(AttributeError, "only available on expanded DataCoordinates"):
                        getattr(data_id, element_name)
                with self.assertRaisesRegex(AttributeError, "^not_a_dimension_name$"):
                    data_id.not_a_dimension_name

    def testEquality(self):
        """Test that different `DataCoordinate` instances with different state
        flags can be compared with each other and other mappings.
        """
        dataIds = self.randomDataIds(n=2)
        split = self.splitByStateFlags(dataIds)
        # Iterate over all combinations of different states of DataCoordinate,
        # with the same underlying data ID values.
        for a0, b0 in itertools.combinations(split.chain(0), 2):
            self.assertEqual(a0, b0)
        # Same thing, for a different data ID value.
        for a1, b1 in itertools.combinations(split.chain(1), 2):
            self.assertEqual(a1, b1)
        # Iterate over all combinations of different states of DataCoordinate,
        # with different underlying data ID values.
        for a0, b1 in itertools.product(split.chain(0), split.chain(1)):
            self.assertNotEqual(a0, b1)
            self.assertNotEqual(a1, b0)

    def testStandardize(self):
        """Test constructing a DataCoordinate from many different kinds of
        input via `DataCoordinate.standardize` and `DataCoordinate.subset`.
        """
        for _ in range(5):
            dimensions = self.randomDimensionSubset()
            dataIds = self.randomDataIds(n=1).subset(dimensions)
            split = self.splitByStateFlags(dataIds)
            for dataId in split.chain():
                # Passing in any kind of DataCoordinate alone just returns
                # that object.
                self.assertIs(dataId, DataCoordinate.standardize(dataId))
                # Same if we also explicitly pass the dimensions we want.
                self.assertIs(dataId, DataCoordinate.standardize(dataId, dimensions=dataId.dimensions))
                # Same if we pass the dimensions and some irrelevant
                # kwargs.
                self.assertIs(
                    dataId, DataCoordinate.standardize(dataId, dimensions=dataId.dimensions, htm7=12)
                )
                # Test constructing a new data ID from this one with a
                # subset of the dimensions.
                # This is not possible for some combinations of
                # dimensions if hasFull is False (see
                # `DataCoordinate.subset` docs).
                newDimensions = self.randomDimensionSubset(n=1, group=dataId.dimensions)
                if dataId.hasFull() or dataId.dimensions.required >= newDimensions.required:
                    newDataIds = [
                        dataId.subset(newDimensions),
                        DataCoordinate.standardize(dataId, dimensions=newDimensions),
                        DataCoordinate.standardize(dataId, dimensions=newDimensions, htm7=12),
                    ]
                    for newDataId in newDataIds:
                        with self.subTest(newDataId=newDataId, type=type(dataId)):
                            commonKeys = dataId.dimensions.required & newDataId.dimensions.required
                            self.assertTrue(commonKeys)
                            self.assertEqual(
                                [newDataId[k] for k in commonKeys],
                                [dataId[k] for k in commonKeys],
                            )
                            # This should never "downgrade" from
                            # Complete to Minimal or Expanded to Complete.
                            if dataId.hasRecords():
                                self.assertTrue(newDataId.hasRecords())
                            if dataId.hasFull():
                                self.assertTrue(newDataId.hasFull())
            # Start from a complete data ID, and pass its values in via several
            # different ways that should be equivalent.
            for dataId in split.complete:
                # Split the keys (dimension names) into two random subsets, so
                # we can pass some as kwargs below.
                keys1 = set(self.rng.sample(list(dataId.dimensions.names), len(dataId.dimensions) // 2))
                keys2 = dataId.dimensions.names - keys1
                newCompleteDataIds = [
                    DataCoordinate.standardize(dataId.mapping, universe=dataId.universe),
                    DataCoordinate.standardize(dataId.mapping, dimensions=dataId.dimensions),
                    DataCoordinate.standardize(
                        DataCoordinate.make_empty(dataId.dimensions.universe), **dataId.mapping
                    ),
                    DataCoordinate.standardize(
                        DataCoordinate.make_empty(dataId.dimensions.universe),
                        dimensions=dataId.dimensions,
                        **dataId.mapping,
                    ),
                    DataCoordinate.standardize(**dataId.mapping, universe=dataId.universe),
                    DataCoordinate.standardize(dimensions=dataId.dimensions, **dataId.mapping),
                    DataCoordinate.standardize(
                        {k: dataId[k] for k in keys1},
                        universe=dataId.universe,
                        **{k: dataId[k] for k in keys2},
                    ),
                    DataCoordinate.standardize(
                        {k: dataId[k] for k in keys1},
                        dimensions=dataId.dimensions,
                        **{k: dataId[k] for k in keys2},
                    ),
                ]
                for newDataId in newCompleteDataIds:
                    with self.subTest(dataId=dataId, newDataId=newDataId, type=type(dataId)):
                        self.assertEqual(dataId, newDataId)
                        self.assertTrue(newDataId.hasFull())

    def testUnion(self):
        """Test `DataCoordinate.union`."""
        # Make test groups to combine; mostly random, but with a few explicit
        # cases to make sure certain edge cases are covered.
        groups = [self.randomDimensionSubset(n=2) for i in range(2)]
        groups.append(self.allDataIds.universe["visit"].minimal_group)
        groups.append(self.allDataIds.universe["detector"].minimal_group)
        groups.append(self.allDataIds.universe["physical_filter"].minimal_group)
        groups.append(self.allDataIds.universe["band"].minimal_group)
        # Iterate over all combinations, including the same graph with itself.
        for group1, group2 in itertools.product(groups, repeat=2):
            parentDataIds = self.randomDataIds(n=1)
            split1 = self.splitByStateFlags(parentDataIds.subset(group1))
            split2 = self.splitByStateFlags(parentDataIds.subset(group2))
            (parentDataId,) = parentDataIds
            for lhs, rhs in itertools.product(split1.chain(), split2.chain()):
                unioned = lhs.union(rhs)
                with self.subTest(lhs=lhs, rhs=rhs, unioned=unioned):
                    self.assertEqual(unioned.dimensions, group1.union(group2))
                    self.assertEqual(unioned, parentDataId.subset(unioned.dimensions))
                    if unioned.hasFull():
                        self.assertEqual(unioned.subset(lhs.dimensions), lhs)
                        self.assertEqual(unioned.subset(rhs.dimensions), rhs)
                    if lhs.hasFull() and rhs.hasFull():
                        self.assertTrue(unioned.hasFull())
                    if lhs.dimensions >= unioned.dimensions and lhs.hasFull():
                        self.assertTrue(unioned.hasFull())
                        if lhs.hasRecords():
                            self.assertTrue(unioned.hasRecords())
                    if rhs.dimensions >= unioned.dimensions and rhs.hasFull():
                        self.assertTrue(unioned.hasFull())
                        if rhs.hasRecords():
                            self.assertTrue(unioned.hasRecords())
                    if lhs.dimensions.required | rhs.dimensions.required >= unioned.dimensions.names:
                        self.assertTrue(unioned.hasFull())
                    if (
                        lhs.hasRecords()
                        and rhs.hasRecords()
                        and lhs.dimensions.elements | rhs.dimensions.elements >= unioned.dimensions.elements
                    ):
                        self.assertTrue(unioned.hasRecords())

    def testRegions(self):
        """Test that data IDs for a few known dimensions have the expected
        regions.
        """
        for dataId in self.randomDataIds(n=4).subset(self.allDataIds.universe.conform(["visit"])):
            self.assertIsNotNone(dataId.region)
            self.assertEqual(dataId.dimensions.spatial.names, {"observation_regions"})
            self.assertEqual(dataId.region, dataId.records["visit"].region)
        for dataId in self.randomDataIds(n=4).subset(self.allDataIds.universe.conform(["visit", "detector"])):
            self.assertIsNotNone(dataId.region)
            self.assertEqual(dataId.dimensions.spatial.names, {"observation_regions"})
            self.assertEqual(dataId.region, dataId.records["visit_detector_region"].region)
        for dataId in self.randomDataIds(n=4).subset(self.allDataIds.universe.conform(["tract"])):
            self.assertIsNotNone(dataId.region)
            self.assertEqual(dataId.dimensions.spatial.names, {"skymap_regions"})
            self.assertEqual(dataId.region, dataId.records["tract"].region)
        for dataId in self.randomDataIds(n=4).subset(self.allDataIds.universe.conform(["patch"])):
            self.assertIsNotNone(dataId.region)
            self.assertEqual(dataId.dimensions.spatial.names, {"skymap_regions"})
            self.assertEqual(dataId.region, dataId.records["patch"].region)
        for data_id in self.randomDataIds(n=1).subset(self.allDataIds.universe.conform(["visit", "tract"])):
            self.assertEqual(data_id.region.relate(data_id.records["visit"].region), lsst.sphgeom.WITHIN)
            self.assertEqual(data_id.region.relate(data_id.records["tract"].region), lsst.sphgeom.WITHIN)

    def testTimespans(self):
        """Test that data IDs for a few known dimensions have the expected
        timespans.
        """
        for dataId in self.randomDataIds(n=4).subset(self.allDataIds.universe.conform(["visit"])):
            self.assertIsNotNone(dataId.timespan)
            self.assertEqual(dataId.dimensions.temporal.names, {"observation_timespans"})
            self.assertEqual(dataId.timespan, dataId.records["visit"].timespan)
            self.assertEqual(dataId.timespan, dataId.visit.timespan)
        # Also test the case for non-temporal DataIds.
        for dataId in self.randomDataIds(n=4).subset(self.allDataIds.universe.conform(["patch"])):
            self.assertIsNone(dataId.timespan)

    def testIterableStatusFlags(self):
        """Test that DataCoordinateSet and DataCoordinateSequence compute
        their hasFull and hasRecords flags correctly from their elements.
        """
        dataIds = self.randomDataIds(n=10)
        split = self.splitByStateFlags(dataIds)
        for cls in (DataCoordinateSet, DataCoordinateSequence):
            self.assertTrue(cls(split.expanded, dimensions=dataIds.dimensions, check=True).hasFull())
            self.assertTrue(cls(split.expanded, dimensions=dataIds.dimensions, check=False).hasFull())
            self.assertTrue(cls(split.expanded, dimensions=dataIds.dimensions, check=True).hasRecords())
            self.assertTrue(cls(split.expanded, dimensions=dataIds.dimensions, check=False).hasRecords())
            self.assertTrue(cls(split.complete, dimensions=dataIds.dimensions, check=True).hasFull())
            self.assertTrue(cls(split.complete, dimensions=dataIds.dimensions, check=False).hasFull())
            self.assertFalse(cls(split.complete, dimensions=dataIds.dimensions, check=True).hasRecords())
            self.assertFalse(cls(split.complete, dimensions=dataIds.dimensions, check=False).hasRecords())
            with self.assertRaises(ValueError):
                cls(split.complete, dimensions=dataIds.dimensions, hasRecords=True, check=True)
            self.assertEqual(
                cls(split.minimal, dimensions=dataIds.dimensions, check=True).hasFull(),
                not dataIds.dimensions.implied,
            )
            self.assertEqual(
                cls(split.minimal, dimensions=dataIds.dimensions, check=False).hasFull(),
                not dataIds.dimensions.implied,
            )
            self.assertFalse(cls(split.minimal, dimensions=dataIds.dimensions, check=True).hasRecords())
            self.assertFalse(cls(split.minimal, dimensions=dataIds.dimensions, check=False).hasRecords())
            with self.assertRaises(ValueError):
                cls(split.minimal, dimensions=dataIds.dimensions, hasRecords=True, check=True)
            if dataIds.dimensions.implied:
                with self.assertRaises(ValueError):
                    cls(split.minimal, dimensions=dataIds.dimensions, hasFull=True, check=True)

    def testSetOperations(self):
        """Test for self-consistency across DataCoordinateSet's operations."""
        c = self.randomDataIds(n=10).toSet()
        a = self.randomDataIds(n=20).toSet() | c
        b = self.randomDataIds(n=20).toSet() | c
        # Make sure we don't have a particularly unlucky random seed, since
        # that would make a lot of this test uninteresting.
        self.assertNotEqual(a, b)
        self.assertGreater(len(a), 0)
        self.assertGreater(len(b), 0)
        # The rest of the tests should not depend on the random seed.
        self.assertEqual(a, a)
        self.assertNotEqual(a, a.toSequence())
        self.assertEqual(a, a.toSequence().toSet())
        self.assertEqual(a, a.toSequence().toSet())
        self.assertEqual(b, b)
        self.assertNotEqual(b, b.toSequence())
        self.assertEqual(b, b.toSequence().toSet())
        self.assertEqual(a & b, a.intersection(b))
        self.assertLessEqual(a & b, a)
        self.assertLessEqual(a & b, b)
        self.assertEqual(a | b, a.union(b))
        self.assertGreaterEqual(a | b, a)
        self.assertGreaterEqual(a | b, b)
        self.assertEqual(a - b, a.difference(b))
        self.assertLessEqual(a - b, a)
        self.assertLessEqual(b - a, b)
        self.assertEqual(a ^ b, a.symmetric_difference(b))
        self.assertGreaterEqual(a ^ b, (a | b) - (a & b))

    def testPackers(self):
        (instrument_data_id,) = self.allDataIds.subset(
            self.allDataIds.universe.conform(["instrument"])
        ).toSet()
        (detector_data_id,) = self.randomDataIds(n=1).subset(self.allDataIds.universe.conform(["detector"]))
        packer = ConcreteTestDimensionPacker(instrument_data_id, detector_data_id.dimensions)
        packed_id, max_bits = packer.pack(detector_data_id, returnMaxBits=True)
        self.assertEqual(packed_id, detector_data_id["detector"])
        self.assertEqual(max_bits, packer.maxBits)
        self.assertEqual(
            max_bits, math.ceil(math.log2(instrument_data_id.records["instrument"].detector_max))
        )
        self.assertEqual(packer.pack(detector_data_id), packed_id)
        self.assertEqual(packer.pack(detector=detector_data_id["detector"]), detector_data_id["detector"])
        self.assertEqual(packer.unpack(packed_id), detector_data_id)


if __name__ == "__main__":
    unittest.main()

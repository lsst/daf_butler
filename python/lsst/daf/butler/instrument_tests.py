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

"""Helpers for writing tests against subclassses of Instrument.

These are not tests themselves, but can be subclassed (plus unittest.TestCase)
to get a functional test of an Instrument.
"""

import abc
import dataclasses
import os

from lsst.daf.butler import Registry
from lsst.daf.butler import ButlerConfig
from lsst.utils import getPackageDir


@dataclasses.dataclass
class InstrumentTestData:
    """Values to test against in sublcasses of `InstrumentTests`.
    """

    name: str
    """The name of the Camera this instrument describes."""

    nDetectors: int
    """The number of detectors in the Camera."""

    firstDetectorName: str
    """The name of the first detector in the Camera."""

    physical_filters: {str}
    """A subset of the physical filters should be registered."""


class InstrumentTests(metaclass=abc.ABCMeta):
    """Tests of sublcasses of Instrument.

    TestCase subclasses must derive from this, then `TestCase`, and override
    ``data`` and ``instrument``.
    """

    data = None
    """`InstrumentTestData` containing the values to test against."""

    instrument = None
    """The `lsst.daf.butler.Instrument` to be tested."""

    def test_name(self):
        self.assertEqual(self.instrument.getName(), self.data.name)

    def test_getCamera(self):
        """Test that getCamera() returns a reasonable Camera definition.
        """
        camera = self.instrument.getCamera()
        self.assertEqual(camera.getName(), self.instrument.getName())
        self.assertEqual(len(camera), self.data.nDetectors)
        self.assertEqual(next(iter(camera)).getName(), self.data.firstDetectorName)

    def test_register(self):
        """Test that register() sets appropriate Dimensions.
        """
        registryConfigPath = os.path.join(getPackageDir("daf_butler"), "tests/config/basic/butler.yaml")
        registry = Registry.fromConfig(ButlerConfig(registryConfigPath))
        # check that the registry starts out empty
        self.assertEqual(list(registry.queryDimensions(["instrument"])), [])
        self.assertEqual(list(registry.queryDimensions(["detector"])), [])
        self.assertEqual(list(registry.queryDimensions(["physical_filter"])), [])

        # register the instrument and check that certain dimensions appear
        self.instrument.register(registry)
        instrumentDataIds = list(registry.queryDimensions(["instrument"]))
        self.assertEqual(len(instrumentDataIds), 1)
        instrumentNames = {dataId["instrument"] for dataId in instrumentDataIds}
        self.assertEqual(instrumentNames, {self.data.name})
        detectorDataIds = list(registry.queryDimensions(["detector"]))
        self.assertEqual(len(detectorDataIds), self.data.nDetectors)
        detectorNames = {dataId.records["detector"].full_name for dataId in detectorDataIds}
        self.assertIn(self.data.firstDetectorName, detectorNames)
        physicalFilterDataIds = list(registry.queryDimensions(["physical_filter"]))
        filterNames = {dataId['physical_filter'] for dataId in physicalFilterDataIds}
        self.assertGreaterEqual(filterNames, self.data.physical_filters)

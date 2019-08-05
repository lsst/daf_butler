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

import os
import unittest

from lsst.daf.butler.core import Registry
from lsst.daf.butler.core.butlerConfig import ButlerConfig

from lsst.daf.butler.instrument import Instrument

"""Tests for Instrument.
"""


class DummyCam(Instrument):

    class FilterDefinitions:
        """Stopgap until Instrument is moved into obs_base and we can use the
        real FilterDefinitions there.
        """
        def defineFilters(self):
            pass
    filterDefinitions = FilterDefinitions()

    @classmethod
    def getName(cls):
        return "DummyCam"

    def getCamera(self):
        return None

    def register(self, registry):
        """Insert Instrument, physical_filter, and detector entries into a
        `Registry`.
        """
        dataId = {"instrument": self.getName()}
        registry.insertDimensionData("instrument", dataId)
        for f in ("dummy_g", "dummy_u"):
            registry.insertDimensionData("physical_filter",
                                         dict(dataId, physical_filter=f, abstract_filter=f[-1]))
        for d in (1, 2):
            registry.insertDimensionData("detector",
                                         dict(dataId, id=d, full_name=str(d)))

    def getRawFormatter(self, dataId):
        # Docstring inherited fromt Instrument.getRawFormatter.
        return None

    def writeCuratedCalibrations(self, butler):
        pass

    def applyConfigOverrides(self, name, config):
        pass


class InstrumentTestCase(unittest.TestCase):
    """Test for Instrument.
    """

    def setUp(self):
        self.testDir = os.path.dirname(__file__)
        self.configFile = os.path.join(self.testDir, "config/basic/butler.yaml")

    def testRegister(self):
        registry = Registry.fromConfig(ButlerConfig(self.configFile))
        dummyCam = DummyCam()
        dummyCam.register(registry)


if __name__ == "__main__":
    unittest.main()

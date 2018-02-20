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

"""A simple Camera DataUnit for the ci_hsc test data."""
import os

import numpy as np

from lsst.daf.butler.core.units import Camera, PhysicalSensor, PhysicalFilter, AbstractFilter


class HyperSuprimeCam(Camera):
    """A simple Camera for the ci_hsc test data.

    May or may not be superseded by the full definition in obs_subaru once that
    is available; it may always be useful to have a simpler copy here for test
    purposes.
    """

    DATA_ROOT = os.path.join(
        os.path.split(__file__)[0],
        "../../../../tests/data/ci_hsc"
    )

    def makePhysicalSensors(self):
        """Return the list of PhysicalSensors associated with this Camera."""
        table = np.load(os.path.join(self.DATA_ROOT, "PhysicalSensor.npy"))
        return [
            PhysicalSensor(
                camera=self,
                number=int(record["physical_sensor_number"]),
                name=str(record["name"]),
                group=str(record["group"]),
                purpose=str(record["purpose"]),
            )
            for record in table
        ]

    def makePhysicalFilters(self):
        """Return the list of PhysicalFilters associated with this Camera."""
        table = np.load(os.path.join(self.DATA_ROOT, "AbstractFilter.npy"))
        abstract = {
            str(record["abstract_filter_name"]):
                AbstractFilter(name=str(record["abstract_filter_name"]))
            for record in table
        }
        table = np.load(os.path.join(self.DATA_ROOT, "PhysicalFilter.npy"))
        return [
            PhysicalFilter(
                camera=self,
                name=str(record["physical_filter_name"]),
                abstract=abstract.get(str(record["abstract_filter_name"]), None)
            )
            for record in table
        ]


Camera.instances["HSC"] = HyperSuprimeCam(name="HSC")

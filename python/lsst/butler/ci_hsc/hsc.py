"""A simple Camera DataUnit for the ci_hsc test data."""
import os

import numpy as np

from ..units import Camera, PhysicalSensor, PhysicalFilter, AbstractFilter


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

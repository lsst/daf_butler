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


__all__ = ("Instrument", )


class Instrument:
    """A template method class that can register itself with a
    `Registry.

    This class should be subclassed by various implementations.

    Attributes
    ----------
    camera : `str`
        Name of the camera.
    physicalFilters : `list`
        List of PhysicalFilter entries (each entry being a dict).
    sensors : `list`
        List of Sensor entries (each entry being a dict).
    """
    @property
    def camera(self):
        raise NotImplementedError('Must be specified by derived class')
    
    physicalFilters = []
    sensors = []
    
    def register(self, registry):
        """Register an instance of this `Instrument` with a `Registry`.

        Creates all relevant `DataUnit` entries.
        """
        self._addCamera(registry)
        self._addPhysicalFilters(registry)
        self._addSensors(registry)

    def _addCamera(self, registry):
        registry.addDataUnitEntry('Camera', {'camera': self.camera})

    def _addPhysicalFilters(self, registry):
        for entry in self.physicalFilters:
            if 'camera' not in entry:
                entry['camera'] = self.camera
            registry.addDataUnitEntry('PhysicalFilter', entry)

    def _addSensors(self, registry):
        for entry in self.sensors:
            if 'camera' not in entry:
                entry['camera'] = self.camera
            registry.addDataUnitEntry('Sensor', entry)

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

__all__ = ("Instrument", "makeExposureEntryFromObsInfo", "makeVisitEntryFromObsInfo")


# TODO: all code in this module probably needs to be moved to a higher-level
# package (perhaps obs_base), but it is needed by the gen2convert subpackage.
# We should probably move that as well.


class Instrument:
    """A template method class that can register itself with a
    `Registry.

    This class should be subclassed by various implementations.
    Subclasses should provide all relevant attributes, as documented
    below.

    Attributes
    ----------
    camera : `str`
        Name of the camera.  Must be provided by subclass.
    physicalFilters : `list`
        List of PhysicalFilter entries (each entry being a dict).
    sensors : `list`
        List of Sensor entries (each entry being a dict).
    """
    camera = None
    physicalFilters = []
    sensors = []

    def register(self, registry):
        """Register an instance of this `Instrument` with a `Registry`.

        Creates all relevant `DataUnit` entries.
        """
        assert self.camera is not None
        self._addCamera(registry)
        self._addPhysicalFilters(registry)
        self._addSensors(registry)

    def _addCamera(self, registry):
        registry.addDataUnitEntry("Camera", {"camera": self.camera})

    def _addPhysicalFilters(self, registry):
        for entry in self.physicalFilters:
            if "camera" not in entry:
                entry["camera"] = self.camera
            registry.addDataUnitEntry("PhysicalFilter", entry)

    def _addSensors(self, registry):
        for entry in self.sensors:
            if 'camera' not in entry:
                entry['camera'] = self.camera
            registry.addDataUnitEntry('Sensor', entry)


def makeExposureEntryFromObsInfo(dataId, obsInfo):
    """Construct an Exposure DataUnit entry from
    `astro_metadata_translator.ObservationInfo`.

    Parameters
    ----------
    dataId : `dict`
        Dictionary of DataUnit primary/foreign key values for Exposure
        ("camera", "exposure", optionally "visit" and "physical_filter").
    obsInfo : `astro_metadata_translator.ObservationInfo`
        A `~astro_metadata_translator.ObservationInfo` object corresponding to the
        Exposure.

    Returns
    -------
    entry : `dict`
        A dictionary containing all fields in the Exposure table.
    """
    result = {
        "datetime_begin": obsInfo.datetime_begin.to_datetime(),
        "datetime_end": obsInfo.datetime_end.to_datetime(),
        "exposure_time": obsInfo.exposure_time,
        "dark_time": obsInfo.dark_time
    }
    result.update(dataId)
    return result


def makeVisitEntryFromObsInfo(dataId, obsInfo):
    """Construct a Visit DataUnit entry from
    `astro_metadata_translator.ObservationInfo`.

    Parameters
    ----------
    dataId : `dict`
        Dictionary of DataUnit primary/foreign key values for Visit ("camera",
        "visit", optionally "physical_filter").
    obsInfo : `astro_metadata_translator.ObservationInfo`
        A `~astro_metadata_translator.ObservationInfo` object corresponding to the
        Visit.

    Returns
    -------
    entry : `dict`
        A dictionary containing all fields in the Visit table aside from
        "region".
    """
    result = {
        "datetime_begin": obsInfo.datetime_begin.to_datetime(),
        "datetime_end": obsInfo.datetime_end.to_datetime(),
        "exposure_time": obsInfo.exposure_time,
    }
    result.update(dataId)
    return result

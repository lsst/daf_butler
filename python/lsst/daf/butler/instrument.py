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

__all__ = ("Instrument", "updateExposureEntryFromObsInfo", "updateVisitEntryFromObsInfo")

from inspect import isabstract
from abc import ABCMeta, abstractmethod
from lsst.daf.butler import DataId


# TODO: all code in this module probably needs to be moved to a higher-level
# package (perhaps obs_base), but it is needed by the gen2convert subpackage.
# We should probably move that as well.


class Instrument(metaclass=ABCMeta):
    """Base class for Instrument-specific logic for the Gen3 Butler.

    Concrete instrument subclasses should either be directly constructable
    with no arguments or provide a 'factory' `staticmethod`, `classmethod`, or
    other callable class attribute that takes no arguments and returns a new
    `Instrument` instance.
    """

    factories = {}
    """Global dictionary that maps instrument name used in the registry to
    a no-argument callable that can be used to construct a Python instance.
    """

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if not isabstract(cls):
            factory = getattr(cls, "factory", cls)
            Instrument.factories[cls.getName()] = factory

    @classmethod
    @abstractmethod
    def getName(cls):
        raise NotImplementedError()

    @abstractmethod
    def register(self, registry):
        """Insert Instrument, PhysicalFilter, and Detector entries into a
        `Registry`.
        """
        raise NotImplementedError()

    @abstractmethod
    def getRawFormatter(self, dataId):
        """Return the Formatter class that should be used to read a particular
        raw file.

        Parameters
        ----------
        dataId : `DataId`
            Dimension-link identifier for the raw file or files being ingested.

        Returns
        -------
        formatter : `Formatter`
            Object that reads the file into an `lsst.afw.image.Exposure`
            instance.
        """
        raise NotImplementedError()


def updateExposureEntryFromObsInfo(dataId, obsInfo):
    """Construct an Exposure Dimension entry from
    `astro_metadata_translator.ObservationInfo`.

    Parameters
    ----------
    dataId : `dict` or `DataId`
        Dictionary of Dimension link fields for (at least) Exposure. If a true
        `DataId`, this object will be modified and returned.
    obsInfo : `astro_metadata_translator.ObservationInfo`
        A `~astro_metadata_translator.ObservationInfo` object corresponding to
        the Exposure.

    Returns
    -------
    dataId : `DataId`
        A data ID with the entry for the Exposure dimension updated.
    """
    dataId = DataId(dataId)
    dataId.entries[dataId.dimensions()["Exposure"]].update(
        datetime_begin=obsInfo.datetime_begin.to_datetime(),
        datetime_end=obsInfo.datetime_end.to_datetime(),
        exposure_time=obsInfo.exposure_time.to_value("s"),
        dark_time=obsInfo.dark_time.to_value("s")
    )
    return dataId


def updateVisitEntryFromObsInfo(dataId, obsInfo):
    """Construct a Visit Dimension entry from
    `astro_metadata_translator.ObservationInfo`.

    Parameters
    ----------
    dataId : `dict` or `DataId`
        Dictionary of Dimension link fields for (at least) Visit. If a true
        `DataId`, this object will be modified and returned.
    obsInfo : `astro_metadata_translator.ObservationInfo`
        A `~astro_metadata_translator.ObservationInfo` object corresponding to
        the Exposure.

    Returns
    -------
    dataId : `DataId`
        A data ID with the entry for the Visit dimension updated.
    """
    dataId = DataId(dataId)
    dataId.entries[dataId.dimensions()["Visit"]].update(
        datetime_begin=obsInfo.datetime_begin.to_datetime(),
        datetime_end=obsInfo.datetime_end.to_datetime(),
        exposure_time=obsInfo.exposure_time.to_value("s"),
    )
    return dataId

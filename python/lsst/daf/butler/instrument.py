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

__all__ = ["ObservationDimensionPacker"]

from lsst.daf.butler import DataCoordinate, DimensionGraph, DimensionPacker


class ObservationDimensionPacker(DimensionPacker):
    """A `DimensionPacker` for visit+detector or exposure+detector, given an
    instrument.
    """

    def __init__(self, fixed: DataCoordinate, dimensions: DimensionGraph):
        super().__init__(fixed, dimensions)
        self._instrumentName = fixed["instrument"]
        record = fixed.records["instrument"]
        assert record is not None
        if self.dimensions.required.names == set(["instrument", "visit", "detector"]):
            self._observationName = "visit"
            obsMax = record.visit_max
        elif dimensions.required.names == set(["instrument", "exposure", "detector"]):
            self._observationName = "exposure"
            obsMax = record.exposure_max
        else:
            raise ValueError(f"Invalid dimensions for ObservationDimensionPacker: {dimensions.required}")
        self._detectorMax = record.detector_max
        self._maxBits = (obsMax * self._detectorMax).bit_length()

    @property
    def maxBits(self) -> int:
        # Docstring inherited from DimensionPacker.maxBits
        return self._maxBits

    def _pack(self, dataId: DataCoordinate) -> int:
        # Docstring inherited from DimensionPacker._pack
        return dataId["detector"] + self._detectorMax * dataId[self._observationName]

    def unpack(self, packedId: int) -> DataCoordinate:
        # Docstring inherited from DimensionPacker.unpack
        observation, detector = divmod(packedId, self._detectorMax)
        return DataCoordinate.standardize(
            {
                "instrument": self._instrumentName,
                "detector": detector,
                self._observationName: observation,
            },
            graph=self.dimensions,
        )

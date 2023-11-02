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

__all__ = ["ObservationDimensionPacker"]

# TODO: Remove this entire module on DM-38687.

from deprecated.sphinx import deprecated
from lsst.daf.butler import DataCoordinate, DimensionGraph, DimensionGroup, DimensionPacker


# TODO: remove on DM-38687.
@deprecated(
    "Deprecated in favor of configurable dimension packers.  Will be removed after v26.",
    version="v26",
    category=FutureWarning,
)
class ObservationDimensionPacker(DimensionPacker):
    """A `DimensionPacker` for visit+detector or exposure+detector, given an
    instrument.
    """

    def __init__(self, fixed: DataCoordinate, dimensions: DimensionGraph | DimensionGroup):
        super().__init__(fixed, dimensions)
        self._instrumentName = fixed["instrument"]
        record = fixed.records["instrument"]
        assert record is not None
        if self._dimensions.required.names == {"instrument", "visit", "detector"}:
            self._observationName = "visit"
            obsMax = record.visit_max
        elif dimensions.required.names == {"instrument", "exposure", "detector"}:
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
            dimensions=self._dimensions,
        )

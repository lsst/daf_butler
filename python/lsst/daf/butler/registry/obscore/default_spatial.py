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

from __future__ import annotations

__all__ = ["DefaultSpatialObsCorePlugin"]

import warnings
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Optional

import sqlalchemy
from lsst.daf.butler import DatasetId
from lsst.sphgeom import ConvexPolygon, LonLat, Region

from ...core import ddl
from ._spatial import RegionTypeWarning, SpatialObsCorePlugin

if TYPE_CHECKING:
    from ..interfaces import Database
    from ._records import Record

# Columns added/filled by this plugin
_COLUMNS = (
    ddl.FieldSpec(name="s_ra", dtype=sqlalchemy.Float, doc="Central right ascension, ICRS (deg)"),
    ddl.FieldSpec(name="s_dec", dtype=sqlalchemy.Float, doc="Central declination, ICRS (deg)"),
    ddl.FieldSpec(name="s_fov", dtype=sqlalchemy.Float, doc="Diameter (bounds) of the covered region (deg)"),
    ddl.FieldSpec(
        name="s_region",
        dtype=sqlalchemy.String,
        length=65535,
        doc="Sky region covered by the data product (expressed in ICRS frame)",
    ),
)


class DefaultSpatialObsCorePlugin(SpatialObsCorePlugin):
    """Class for a spatial ObsCore plugin which creates standard spatial
    obscore columns.
    """

    def __init__(self, *, name: str, config: Mapping[str, Any]):
        self._name = name

    @classmethod
    def initialize(
        cls, *, name: str, config: Mapping[str, Any], db: Optional[Database]
    ) -> SpatialObsCorePlugin:
        # docstring inherited.
        return cls(name=name, config=config)

    def extend_table_spec(self, table_spec: ddl.TableSpec) -> None:
        # docstring inherited.
        table_spec.fields.update(_COLUMNS)

    def make_records(self, dataset_id: DatasetId, region: Optional[Region]) -> Optional[Record]:
        # docstring inherited.

        if region is None:
            return None

        record: Record = {}

        # Get spatial parameters from the bounding circle.
        circle = region.getBoundingCircle()
        center = LonLat(circle.getCenter())
        record["s_ra"] = center.getLon().asDegrees()
        record["s_dec"] = center.getLat().asDegrees()
        record["s_fov"] = circle.getOpeningAngle().asDegrees() * 2

        if isinstance(region, ConvexPolygon):
            poly = ["POLYGON ICRS"]
            for vertex in region.getVertices():
                lon_lat = LonLat(vertex)
                poly += [
                    f"{lon_lat.getLon().asDegrees():.6f}",
                    f"{lon_lat.getLat().asDegrees():.6f}",
                ]
            record["s_region"] = " ".join(poly)
        else:
            warnings.warn(
                f"Unexpected region type for obscore dataset {dataset_id}: {type(region)}",
                category=RegionTypeWarning,
            )

        return record

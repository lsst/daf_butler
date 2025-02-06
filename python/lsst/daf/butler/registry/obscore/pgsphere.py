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

from __future__ import annotations

__all__ = ["PgSphereObsCorePlugin"]

from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any

import sqlalchemy
from sqlalchemy.dialects.postgresql.base import ischema_names
from sqlalchemy.types import UserDefinedType

from lsst.sphgeom import ConvexPolygon, LonLat, Region

from ... import ddl
from ._spatial import MissingDatabaseError, RegionTypeError, SpatialObsCorePlugin

if TYPE_CHECKING:
    from ..interfaces import Database
    from ._records import Record


class PgSpherePoint(UserDefinedType):
    """SQLAlchemy type representing pgSphere point (spoint) type.

    On Python side this type corresponds to `lsst.sphgeom.LonLat`.
    Only a limited set of methods is implemented, sufficient to store the
    data in the database.
    """

    cache_ok = True

    def get_col_spec(self, **kw: Any) -> str:
        """Return name of the column type.

        Parameters
        ----------
        **kw
            Keyword Parameters.

        Returns
        -------
        spec : `str`
            Always returns "SPOINT".
        """
        return "SPOINT"

    def bind_processor(self, dialect: sqlalchemy.engine.Dialect) -> Callable:
        """Return processor method for bind values.

        Parameters
        ----------
        dialect : `sqlalchemy.engine.Dialect`
            The relevant dialect.

        Returns
        -------
        processor : `~collections.abc.Callable`
            The processor method.
        """

        def _process(value: LonLat | None) -> str | None:
            if value is None:
                return None
            lon = value.getLon().asRadians()
            lat = value.getLat().asRadians()
            return f"({lon},{lat})"

        return _process


class PgSpherePolygon(UserDefinedType):
    """SQLAlchemy type representing pgSphere polygon (spoly) type.

    On Python side it corresponds to a sequence of `lsst.sphgeom.LonLat`
    instances (sphgeom polygons are convex, while pgSphere polygons do not
    have to be). Only a limited set of methods is implemented, sufficient to
    store the data in the database.
    """

    cache_ok = True

    def get_col_spec(self, **kw: Any) -> str:
        """Return name of the column type.

        Parameters
        ----------
        **kw
            Keyword Parameters.

        Returns
        -------
        spec : `str`
            Always returns "SPOLY".
        """
        return "SPOLY"

    def bind_processor(self, dialect: sqlalchemy.engine.Dialect) -> Callable:
        """Return processor method for bind values.

        Parameters
        ----------
        dialect : `sqlalchemy.engine.Dialect`
            The relevant dialect.

        Returns
        -------
        processor : `~collections.abc.Callable`
            The processor method.
        """

        def _process(value: Sequence[LonLat] | None) -> str | None:
            if value is None:
                return None
            points = []
            for lonlat in value:
                lon = lonlat.getLon().asRadians()
                lat = lonlat.getLat().asRadians()
                points.append(f"({lon},{lat})")
            return "{" + ",".join(points) + "}"

        return _process


# To suppress SAWarning about unknown types we need to make them known, this
# is not explicitly documented but it is what other people do.
ischema_names["spoint"] = PgSpherePoint
ischema_names["spoly"] = PgSpherePolygon


class PgSphereObsCorePlugin(SpatialObsCorePlugin):
    """Spatial ObsCore plugin which creates pg_sphere geometries.

    Parameters
    ----------
    name : `str`
        The name.
    config : `~collections.abc.Mapping` [`str`, `~typing.Any`]
        The configuration.

    Notes
    -----
    This plugin adds and fills two columns to obscore table - one for the
    region (polygon), another for the position of the center of bounding
    circle. Both columns are indexed. Column names can be changed via plugin
    configuration.
    """

    def __init__(self, *, name: str, config: Mapping[str, Any]):
        self._name = name
        self._region_column_name = config.get("region_column", "pgsphere_region")
        self._position_column_name = config.get("position_column", "pgsphere_position")

    @classmethod
    def initialize(cls, *, name: str, config: Mapping[str, Any], db: Database | None) -> SpatialObsCorePlugin:
        # docstring inherited.

        if db is None:
            raise MissingDatabaseError("Database access is required for pgSphere plugin")

        # Check that engine is Postgres and pgSphere extension is enabled.
        if db.dialect.name != "postgresql":
            raise RuntimeError("PgSphere spatial plugin for obscore requires PostgreSQL database.")
        query = "SELECT COUNT(*) FROM pg_extension WHERE extname='pg_sphere'"
        with db.query(sqlalchemy.sql.text(query)) as result:
            if result.scalar() == 0:
                raise RuntimeError(
                    "PgSphere spatial plugin for obscore requires the pgSphere extension. "
                    "Please run `CREATE EXTENSION pg_sphere;` on a database containing obscore table "
                    "from a PostgreSQL superuser account."
                )

        return cls(name=name, config=config)

    def extend_table_spec(self, table_spec: ddl.TableSpec) -> None:
        # docstring inherited.
        table_spec.fields.update(
            (
                ddl.FieldSpec(
                    name=self._region_column_name,
                    dtype=PgSpherePolygon,
                    doc="pgSphere polygon for this record region.",
                ),
                ddl.FieldSpec(
                    name=self._position_column_name,
                    dtype=PgSpherePoint,
                    doc="pgSphere position for this record, center of bounding circle.",
                ),
            )
        )
        # Spatial columns need GIST index type
        table_spec.indexes.add(ddl.IndexSpec(self._region_column_name, postgresql_using="gist"))
        table_spec.indexes.add(ddl.IndexSpec(self._position_column_name, postgresql_using="gist"))

    def make_records(self, region: Region | None) -> Record | None:
        # docstring inherited.

        if region is None:
            return None

        record: Record = {}
        circle = region.getBoundingCircle()
        record[self._position_column_name] = LonLat(circle.getCenter())

        # Presently we can only handle polygons
        if isinstance(region, ConvexPolygon):
            poly_points = [LonLat(vertex) for vertex in region.getVertices()]
            record[self._region_column_name] = poly_points
        else:
            raise RegionTypeError(f"Unexpected region type: {type(region)}")

        return record

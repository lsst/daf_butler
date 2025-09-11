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

"""A script that creates and displays test data for topological spatial
operations.

The test data created by this script is intended to cover the complete set of
interesting topological relationships between visit, visit_detector_region,
tract, patch, and commonSkyPix with a relatively small number of actual
dimension records.  We use a different sky pixelization for visit- and
skymap-based regions as an easy way to get different kinds of regions with
interesting relationships.

The data created by this script must be imported after that in base.yaml
(which defines the instrument and detectors it assumes).  It defines visits
that don't actually correspond to any exposures; those could be added in
another export file (along with the visit_definition records that relate them)
in the future.
"""

from __future__ import annotations

import argparse
import os.path
from collections.abc import Callable, Iterable, Iterator
from typing import Any

import numpy as np
import yaml
from astropy.time import Time
from astropy.wcs import WCS
from matplotlib import pyplot

import lsst.daf.butler  # register Time/YAML conversions. # noqa: F401
from lsst.sphgeom import (
    ConvexPolygon,
    HealpixPixelization,
    HtmPixelization,
    LonLat,
    Mq3cPixelization,
    Pixelization,
    Q3cPixelization,
    RangeSet,
    UnitVector3d,
    Vector3d,
)

# Pixelization for which one pixel defines the overall area of interest.
PARENT_PIX = Mq3cPixelization(6)

# Pixelization used as the commonSkyPix in butler (needs to be kept consistent
# with the dimensions.yaml used in tests).
COMMON_PIX = HtmPixelization(7)

# Pixelization used to define visit and visit-detector regions.
# Doesn't matter what this is, as long as its different from COMMON_PIX and
# PATCH_GRID_PIX so we get interesting overlaps.
DETECTOR_GRID_PIX = Mq3cPixelization(10)

# Pixelization used to define tract and patch regions.
# Doesn't matter what this is, as long as its different from COMMON_PIX and
# DETECTOR_GRID_PIX so we get interesting overlaps.
PATCH_GRID_PIX = Q3cPixelization(9)

# Name of the instrument; this matches the dimension record in base.yaml,
# which must be imported before the YAML file created by this script.
INSTRUMENT_NAME = "Cam1"

# Name of the skymap.
SKYMAP_NAME = "SkyMap1"

# Data used to define the visits and visit-detector regions.
#
# The write_yaml function adds fields common to all records and transforms
# skypix indices into actual regions.
#
# Edit this data structure and examine the resulting plots to add more test
# dimension records; running with ``--show-detector-grid`` may be helpful.
VISIT_DATA: dict[int, dict[str, Any]] = {
    1: {
        "physical_filter": "Cam1-G",
        "day_obs": 20210909,
        "exposure_time": 60.0,
        "target_name": "test_target",
        "observation_reason": "science",
        "science_program": "test_survey",
        "zenith_angle": 5.0,
        "datetime_begin": Time("2021-09-09T03:00:00", format="isot", scale="tai"),
        "datetime_end": Time("2021-09-09T03:01:00", format="isot", scale="tai"),
        "detector_regions": {
            1: [12058870, 12058871, 12058872, 12058873],
            2: [12058823, 12058824, 12058818, 12058829],
            3: [12058848, 12058849, 12058850, 12058851],
            4: [12058846],
        },
    },
    2: {
        "physical_filter": "Cam1-R1",
        "day_obs": 20210909,
        "exposure_time": 45.0,
        "target_name": "test_target",
        "observation_reason": "science",
        "science_program": "test_survey",
        "zenith_angle": 10.0,
        "datetime_begin": Time("2021-09-09T03:02:00", format="isot", scale="tai"),
        "datetime_end": Time("2021-09-09T03:03:00", format="isot", scale="tai"),
        "detector_regions": {
            1: [12058857, 12058854, 12058646, 12058649],
            2: [12058842, 12058841, 12058661, 12058662],
            3: [12058642, 12058653, 12058641, 12058654],
            4: [12058659],
        },
    },
}

# Data used to define the tract and patch regions.
#
# The write_yaml function adds fields common to all records and transforms
# skypix indices into actual regions.
#
# Edit this data structure and examine the resulting plots to add more test
# dimension records; running with ``--show-patch-grid`` may be helpful.
TRACT_DATA: dict[int, dict[int, dict[str, Any]]] = {
    0: {
        0: {"cell_x": 0, "cell_y": 0, "region": 458787},
        1: {"cell_x": 1, "cell_y": 0, "region": 458790},
        2: {"cell_x": 0, "cell_y": 1, "region": 458785},
        3: {"cell_x": 1, "cell_y": 1, "region": 458788},
        4: {"cell_x": 0, "cell_y": 2, "region": 458763},
        5: {"cell_x": 1, "cell_y": 2, "region": 458766},
    },
    1: {
        0: {"cell_x": 0, "cell_y": 0, "region": 458761},
        1: {"cell_x": 1, "cell_y": 0, "region": 458764},
        2: {"cell_x": 0, "cell_y": 1, "region": 458755},
        3: {"cell_x": 1, "cell_y": 1, "region": 458758},
        4: {"cell_x": 0, "cell_y": 2, "region": 458753},
        5: {"cell_x": 1, "cell_y": 2, "region": 458756},
    },
}


def main() -> None:
    """Run script."""
    parser = argparse.ArgumentParser(description="Create and examine spatial-topology registry test data.")
    default_filename = os.path.join(os.path.dirname(__file__), "spatial.yaml")
    parser.add_argument(
        "--filename", type=str, default=default_filename, help="Filename for YAML export file."
    )
    parser.add_argument(
        "--show-detector-grid",
        action="store_true",
        default=False,
        help="Show the skypix grid used to define visit/detector regions.",
    )
    parser.add_argument(
        "--show-patch-grid",
        action="store_true",
        default=False,
        help="Show the skypix grid used to define patch regions.",
    )
    parser.add_argument(
        "--no-common-skypix-grid",
        dest="common_skypix_grid",
        action="store_false",
        default=True,
        help="Do not show the common skypix grid.",
    )
    parser.add_argument(
        "--show-healpix-grid",
        type=int,
        default=[],
        help="Show a HEALPIX grid of this level.",
        action="append",
    )
    parser.add_argument(
        "--no-plot", action="store_false", dest="make_plots", default=True, help="Do not plot the regions."
    )
    parser.add_argument(
        "--no-write",
        action="store_false",
        dest="write_yaml",
        default=True,
        help="Do not write the YAML export file.",
    )
    namespace = parser.parse_args()
    if namespace.make_plots:
        make_plots(
            detector_grid=namespace.show_detector_grid,
            patch_grid=namespace.show_patch_grid,
            healpix_grids=namespace.show_healpix_grid,
            common_skypix_grid=namespace.common_skypix_grid,
        )
    if namespace.write_yaml:
        write_yaml(namespace.filename)


def make_plots(
    detector_grid: bool, patch_grid: bool, common_skypix_grid: bool = True, healpix_grids: Iterable[int] = ()
) -> None:
    """Plot the regions of the dimension records defined by this script.

    Parameters
    ----------
    detector_grid : `bool`
        If `True`, show the skypix grid used to define visit and visit-detector
        regions.
    patch_grid : `bool`
        If `True`, show the skypix grid used to define tract and patch regions.
    common_skypix_grid : `bool`, optional
        If `True`, show the common skypix grid.
    healpix_grids : `~collections.abc.Iterable` [`int`], optional
        Levels of healpix grids to display.
    """
    parent_index = PARENT_PIX.index(UnitVector3d(1, 0, 0))
    parent_pixel = PARENT_PIX.pixel(parent_index)
    common_ranges = COMMON_PIX.envelope(parent_pixel)
    detector_grid_ranges = DETECTOR_GRID_PIX.interior(parent_pixel)
    patch_grid_ranges = PATCH_GRID_PIX.envelope(parent_pixel)
    wcs = make_tangent_wcs(parent_pixel.getCentroid())
    labels_used = set()
    pyplot.figure(figsize=(16, 16))
    pyplot.axis("off")
    if common_skypix_grid:
        plot_pixels(
            COMMON_PIX,
            wcs,
            flatten_ranges(common_ranges),
            polygons(facecolor="none", edgecolor="black", label="htm7"),
            index_labels(color="black", alpha=0.5),
        )
    if detector_grid:
        plot_pixels(
            DETECTOR_GRID_PIX,
            wcs,
            flatten_ranges(detector_grid_ranges),
            polygons(
                edgecolor="black",
                linewidth=1,
                alpha=0.5,
                linestyle=":",
                facecolor="none",
            ),
            index_labels(color="black", alpha=0.5),
        )
    if patch_grid:
        plot_pixels(
            PATCH_GRID_PIX,
            wcs,
            flatten_ranges(patch_grid_ranges),
            polygons(
                edgecolor="black",
                linewidth=1,
                alpha=0.5,
                linestyle=":",
                facecolor="none",
            ),
            index_labels(color="black", alpha=0.5),
        )
    for healpix_level in healpix_grids:
        pixelization = HealpixPixelization(healpix_level)
        healpix_ranges = pixelization.envelope(parent_pixel)
        plot_pixels(
            pixelization,
            wcs,
            flatten_ranges(healpix_ranges),
            polygons(
                edgecolor="magenta",
                linewidth=1,
                alpha=0.5,
                linestyle=":",
                facecolor="none",
                label=f"healpix{healpix_level}",
            ),
            index_labels(color="magenta", alpha=0.5),
        )
    colors = iter(["red", "blue", "cyan", "green"])
    for (visit_id, visit_data), color in zip(VISIT_DATA.items(), colors, strict=False):
        for detector_id, pixel_indices in visit_data["detector_regions"].items():
            label: str | None = f"visit={visit_id}"
            if label in labels_used:
                label = None
            else:
                labels_used.add(label)
            plot_hull(
                DETECTOR_GRID_PIX,
                wcs,
                pixel_indices,
                polygons(
                    edgecolor="none",
                    facecolor=color,
                    alpha=0.25,
                    label=label,
                ),
                labels(
                    text=str(detector_id),
                    color=color,
                ),
            )
    for (tract_id, tract_data), color in zip(TRACT_DATA.items(), colors, strict=True):
        for patch_id, patch_data in tract_data.items():
            label = f"tract={tract_id}"
            if label in labels_used:
                label = None
            else:
                labels_used.add(label)
            plot_pixels(
                PATCH_GRID_PIX,
                wcs,
                [patch_data["region"]],
                polygons(
                    edgecolor=color,
                    facecolor=color,
                    alpha=0.25,
                    label=label,
                ),
                labels(
                    text=str(patch_id),
                    color=color,
                ),
            )
    parent_vertices = wcs.wcs_world2pix(np.array([lonlat_tuple(v) for v in parent_pixel.getVertices()]), 0)
    pyplot.xlim(parent_vertices[:, 0].min(), parent_vertices[:, 0].max())
    pyplot.ylim(parent_vertices[:, 1].min(), parent_vertices[:, 1].max())
    pyplot.legend()
    pyplot.show()


def write_yaml(filename: str) -> None:
    """Write the YAML export script with dimension record definitions.

    Parameters
    ----------
    filename : `str`
        Name of the file to write.

    Notes
    -----
    This creates a YAML export file that defines records for the following
    dimensions:

    - visit_system
    - visit
    - visit_detector_region
    - skymap
    - tract
    - patch

    """
    day_obs_records = [{"instrument": INSTRUMENT_NAME, "id": 20210909}]
    visit_records = []
    visit_detector_records = []
    for visit_id, visit_data in VISIT_DATA.items():
        visit_vertices = []
        for detector_id, pixel_indices in visit_data["detector_regions"].items():
            detector_vertices = []
            for index in pixel_indices:
                polygon = DETECTOR_GRID_PIX.pixel(index)
                detector_vertices.extend(polygon.getVertices())
            visit_vertices.extend(detector_vertices)
            visit_detector_records.append(
                {
                    "instrument": INSTRUMENT_NAME,
                    "visit": visit_id,
                    "detector": detector_id,
                    "region": ConvexPolygon(detector_vertices),
                }
            )
        visit_record = visit_data.copy()
        del visit_record["detector_regions"]
        visit_record["instrument"] = INSTRUMENT_NAME
        visit_record["id"] = visit_id
        visit_record["name"] = str(visit_id)
        visit_record["region"] = ConvexPolygon(visit_vertices)
        visit_records.append(visit_record)
    skymap_records = [
        {
            "name": SKYMAP_NAME,
            "hash": b"notreallyahashofanything!",
            "tract_max": 50,
            "patch_nx_max": 2,
            "patch_ny_max": 3,
        },
    ]
    tract_records = []
    patch_records = []
    for tract_id, tract_data in TRACT_DATA.items():
        tract_vertices = []
        for patch_id, patch_data in tract_data.items():
            patch_polygon = PATCH_GRID_PIX.pixel(patch_data["region"])
            tract_vertices.extend(patch_polygon.getVertices())
            patch_record = patch_data.copy()
            patch_record["region"] = patch_polygon
            patch_record["id"] = patch_id
            patch_record["tract"] = tract_id
            patch_record["skymap"] = SKYMAP_NAME
            patch_records.append(patch_record)
        tract_record: dict[str, Any] = {}
        tract_record["id"] = tract_id
        tract_record["skymap"] = SKYMAP_NAME
        tract_record["region"] = ConvexPolygon(tract_vertices)
        tract_records.append(tract_record)
    document = {
        "description": "Butler Data Repository Export",
        "version": "1.0.2",
        "universe_version": 7,
        "universe_namespace": "daf_butler",
        "data": [
            {
                "type": "dimension",
                "element": "day_obs",
                "records": day_obs_records,
            },
            {
                "type": "dimension",
                "element": "visit",
                "records": visit_records,
            },
            {
                "type": "dimension",
                "element": "visit_detector_region",
                "records": visit_detector_records,
            },
            {
                "type": "dimension",
                "element": "skymap",
                "records": skymap_records,
            },
            {
                "type": "dimension",
                "element": "tract",
                "records": tract_records,
            },
            {
                "type": "dimension",
                "element": "patch",
                "records": patch_records,
            },
        ],
    }
    with open(filename, mode="w") as file:
        file.write("# Spatial test data; see spatial.py for more information.\n")
        yaml.dump(document, file, sort_keys=False)


def lonlat_tuple(position: LonLat | Vector3d) -> tuple[float, float]:
    """Transform a `lsst.sphgeom.LonLat` or `lsst.sphgeom.Vector3d` to a
    2-tuple of `float` degrees.
    """
    lonlat = LonLat(position)
    return (lonlat.getLon().asDegrees(), lonlat.getLat().asDegrees())


def make_tangent_wcs(position: LonLat | Vector3d) -> WCS:
    """Create an `astropy.WCS` that maps the sky to a tangent plane with
    degree-unit pixels at the given point.

    Notes
    -----
    This uses astropy instead of afw just to avoid a new dependency.  I suspect
    that with a tiny bit of math I could just convert directly from
    `sphgeom.UnitVector3d` to a point in a Euclidean 2-d plane, which is all I
    want, but this seems fine as-is.
    """
    result = WCS(naxis=2)
    result.wcs.crpix = [0.0, 0.0]
    result.wcs.crval = lonlat_tuple(position)
    result.wcs.ctype = ["RA---TAN", "DEC--TAN"]
    result.wcs.cd = [[1.0, 0.0], [0.0, 1.0]]
    return result


def project_polygon_center(wcs: WCS, polygon: ConvexPolygon) -> np.ndarray:
    """Return the WCS-projected center of the given polygon as a 2-element
    `float` array.
    """
    return wcs.wcs_world2pix(np.array(lonlat_tuple(polygon.getCentroid()))[np.newaxis, :], 0)[0]


def project_polygon_vertices(wcs: WCS, polygon: ConvexPolygon) -> np.ndarray:
    """Return the WCS-projected vertices of the given polygon as a `float`
    array with shape ``(n, 2)``.
    """
    vertices_sky = []
    for vertex in polygon.getVertices():
        vertices_sky.append(lonlat_tuple(vertex))
    return wcs.wcs_world2pix(np.array(vertices_sky), 0)


def plot_pixels(
    pixelization: Pixelization,
    wcs: WCS,
    indices: Iterable[int],
    *callbacks: Callable[[int, np.ndarray, np.ndarray], None],
) -> None:
    """Perform plotting actions defined by callbacks on each of a series of
    skypix pixels.

    Parameters
    ----------
    pixelization : `lsst.sphgeom.Pixelization`
        Pixelization that interprets ``indices``.
    wcs : `WCS`
        Tangent plane to project spherical polygons onto.
    indices : `Iterable` [ `int` ]
        Pixel indices to plot.
    *callbacks
        Callbacks to call for each pixel, passing the pixel index, the
        projected center, and the projected vertices.
    """
    for index in indices:
        polygon = pixelization.pixel(index)
        center = project_polygon_center(wcs, polygon)
        vertices = project_polygon_vertices(wcs, polygon)
        for callback in callbacks:
            callback(index, center, vertices)


def plot_hull(
    pixelization: Pixelization,
    wcs: WCS,
    indices: Iterable[int],
    *callbacks: Callable[[list[int], np.ndarray, np.ndarray], None],
) -> None:
    """Perform plotting actions defined by callbacks on the convex hull of
    a series of skypix pixels.

    Parameters
    ----------
    pixelization : `lsst.sphgeom.Pixelization`
        Pixelization that interprets ``indices``.
    wcs : `WCS`
        Tangent plane to project spherical polygons onto.
    indices : `Iterable` [ `int` ]
        Pixel indices to plot.
    *callbacks
        Callbacks to call passing the list of pixel indices, the
        projected center of the convex hull, and the projected vertices of the
        convex hull.
    """
    vertices = []
    indices = list(indices)
    for index in indices:
        polygon = pixelization.pixel(index)
        vertices.extend(polygon.getVertices())
    polygon = ConvexPolygon(vertices)
    projected_center = project_polygon_center(wcs, polygon)
    projected_vertices = project_polygon_vertices(wcs, polygon)
    for callback in callbacks:
        callback(indices, projected_center, projected_vertices)


def polygons(label: str | None = None, **kwargs: Any) -> Callable[[Any, np.ndarray, np.ndarray], None]:
    """Return a callback for use with `plot_pixels` and `plot_hull` that plots
    polygon vertices.

    Parameters
    ----------
    label : `str`, optional
        Legend label for all polygons.  Automatically deduplicated so the
        legend will only contain one entry for each call.
    **kwargs
        Forwarded to `matplotlib.pyplot.fill`.

    Returns
    -------
    func : `~collections.abc.Callable`
        Callable for use with `plot_hull` or `plot_pixels`.
    """
    labels_used = set()

    def func(index: Any, center: np.ndarray, vertices: np.ndarray) -> None:
        if label is not None and label in labels_used:
            label_to_use = None
        else:
            label_to_use = label
            labels_used.add(label)
        pyplot.fill(
            vertices[:, 0],
            vertices[:, 1],
            label=label_to_use,
            **kwargs,
        )

    return func


def index_labels(**kwargs: Any) -> Callable[[int, np.ndarray, np.ndarray], None]:
    """Return a callback for use with `plot_pixels` and `plot_hull` that adds
    text annotations with pixel indices at pixel centers.

    Parameters
    ----------
    **kwargs
        Forwarded to `matplotlib.pyplot.text`.
    """

    def func(index: int, center: np.ndarray, vertices: np.ndarray) -> None:
        pyplot.text(
            center[0],
            center[1],
            str(index),
            ha="center",
            va="center",
            **kwargs,
        )

    return func


def labels(text: str, **kwargs: Any) -> Callable[[Any, np.ndarray, np.ndarray], None]:
    """Return a callback for use with `plot_pixels` and `plot_hull` that adds
    text annotations with the given text.

    Parameters
    ----------
    text : `str`
        Label text.
    **kwargs
        Forwarded to `matplotlib.pyplot.text`.
    """

    def func(index: Any, center: np.ndarray, vertices: np.ndarray) -> None:
        pyplot.text(center[0], center[1], text, ha="center", va="center", **kwargs)

    return func


def flatten_ranges(ranges: RangeSet) -> Iterator[int]:
    """Flatten an `lsst.sphgeom.RangeSet` into an iterator over pixel
    indices.
    """
    for begin, end in ranges:
        yield from range(begin, end)


if __name__ == "__main__":
    main()

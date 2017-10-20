import lsst.sphgeom
import lsst.afw.geom

def makeBoxWcsRegion(box, wcs, margin):
    """Construct a spherical ConvexPolygon from a WCS and a bounding box.

    Parameters:
    -----------
    box : afw.geom.Box2I or afw.geom.Box2D
        A box in the pixel coordinate system defined by the WCS.
    wcs : afw.image.Wcs
        A mapping from a pixel coordinate system to the sky.
    margin : float
        A buffer in pixels to grow the box by (in all directions) before
        transforming it to sky coordinates.

    Returns a sphgeom.ConvexPolygon.
    """
    box = lsst.afw.geom.Box2D(box)
    box.grow(margin)
    vertices = []
    for point in box.getCorners():
        coord = wcs.pixelToSky(point)
        lonlat = lsst.sphgeom.LonLat.fromRadians(coord.getRa().asRadians(),
                                                 coord.getDec().asRadians())
        vertices.append(lsst.sphgeom.UnitVector3d(lonlat))
    return lsst.sphgeom.ConvexPolygon(vertices)

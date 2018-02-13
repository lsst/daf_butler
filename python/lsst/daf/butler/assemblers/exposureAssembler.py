#
# LSST Data Management System
#
# Copyright 2018  AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
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
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <https://www.lsstcorp.org/LegalNotices/>.

"""Support for assembling and disassembling afw Exposures."""

# Need to enable PSFs to be instantiated
import lsst.afw.detection  # noqa F401

from lsst.daf.butler.core.composites import genericDisassembler, genericGetter
from lsst.daf.butler.core.composites import validComponents

EXPOSURE_COMPONENTS = set(("image", "variance", "mask", "wcs", "psf"))
EXPOSURE_INFO_COMPONENTS = set(("apCorrMap", "coaddInputs", "calib", "metadata",
                                "filter", "transmissionCurve", "visitInfo"))


def _groupRequestedComponents(storageClass):
    """Group requested components into top level and ExposureInfo.

    Parameters
    ----------
    storageClass : `StorageClass`
        Place to retrieve list of requested components.

    Returns
    -------
    expComps : `dict`
        Components associated with the top level Exposure.
    expInfoComps : `dict`
        Components associated with the ExposureInfo
    """
    requested = set(storageClass.components.keys())
    expItems = requested & EXPOSURE_COMPONENTS
    expInfoItems = requested & EXPOSURE_INFO_COMPONENTS
    return expItems, expInfoItems


def getComponentFromExposure(composite, componentName):
    """Get a component from an Exposure

    Parameters
    ----------
    composite : `Exposure`
        `Exposure` to access component.
    componentName : `str`
        Name of component to retrieve.

    Returns
    -------
    component : `object`
        The component. Can be None.

    Raises
    ------
    AttributeError
        The component can not be found.
    """
    if componentName in EXPOSURE_COMPONENTS:
        return genericGetter(composite, componentName)
    elif componentName in EXPOSURE_INFO_COMPONENTS:
        return genericGetter(composite.getInfo(), componentName)
    else:
        raise AttributeError("Do not know how to retrieve component {} from {}".format(componentName,
                                                                                       type(composite)))


def validExposureComponents(composite, storageClass):
    """Extract requested components from a composite.

    Parameters
    ----------
    composite : `object`
        Composite from which to extract components.
    storageClass : `StorageClass`
        `StorageClass` associated with this composite object.
        If None, it is assumed this is not to be treated as a composite.

    Returns
    -------
    comps : `dict`
        Non-None components extracted from the composite, indexed by the component
        name as derived from the `StorageClass`.
    """
    # For Exposure we call the generic version twice: once for top level
    # components, and again for ExposureInfo.
    expItems, expInfoItems = _groupRequestedComponents(storageClass)

    components = validComponents(composite, storageClass)
    infoComps = validComponents(composite.getInfo(), storageClass)
    components.update(infoComps)
    return components


def exposureDisassembler(composite, storageClass):
    """Implementation of a disassembler for afw Exposure.

    This implementation attempts to extract components from the parent
    by looking for attributes of the same name or getter methods derived
    from the component name.

    Parameters
    ----------
    composite : `object`
        Parent composite object consisting of components to be extracted.
    storageClass : `StorageClass`
        `StorageClass` associated with the parent, with defined components.

    Returns
    -------
    components : `dict`
        `dict` with keys matching the components defined in `storageClass`
        and values being `DatasetComponent` instances describing the component.

    Raises
    ------
    ValueError
        A requested component can not be found in the parent using generic
        lookups.
    TypeError
        The parent object does not match the supplied `StorageClass`.
    """
    if not storageClass.validateInstance(composite):
        raise TypeError("Unexpected type mismatch between parent and StorageClass"
                        " ({} != {})".format(type(composite), storageClass.pytype))

    # Only look for components that are defined by the StorageClass
    components = {}
    expItems, expInfoItems = _groupRequestedComponents(storageClass)

    fromExposure = genericDisassembler(composite, storageClass, subset=expItems)
    components.update(fromExposure)

    fromExposureInfo = genericDisassembler(composite, storageClass,
                                           subset=expInfoItems, override=composite.getInfo())
    components.update(fromExposureInfo)

    return components

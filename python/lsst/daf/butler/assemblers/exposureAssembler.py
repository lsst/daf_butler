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

"""Support for assembling and disassembling afw Exposures."""

# Need to enable PSFs to be instantiated
import lsst.afw.detection  # noqa F401
from lsst.afw.image import makeExposure, makeMaskedImage

from lsst.daf.butler import CompositeAssembler


class ExposureAssembler(CompositeAssembler):

    EXPOSURE_COMPONENTS = set(("image", "variance", "mask", "wcs", "psf"))
    EXPOSURE_INFO_COMPONENTS = set(("apCorrMap", "coaddInputs", "calib", "metadata",
                                    "filter", "transmissionCurve", "visitInfo"))

    def _groupRequestedComponents(self):
        """Group requested components into top level and ExposureInfo.

        Returns
        -------
        expComps : `dict`
            Components associated with the top level Exposure.
        expInfoComps : `dict`
            Components associated with the ExposureInfo

        Raises
        ------
        ValueError
            There are components defined in the storage class that are not
            expected by this assembler.
        """
        requested = set(self.storageClass.components.keys())

        # Check that we are requesting something that we support
        unknown = requested - (self.EXPOSURE_COMPONENTS | self.EXPOSURE_INFO_COMPONENTS)
        if unknown:
            raise ValueError("Asking for unrecognized component: {}".format(unknown))

        expItems = requested & self.EXPOSURE_COMPONENTS
        expInfoItems = requested & self.EXPOSURE_INFO_COMPONENTS
        return expItems, expInfoItems

    def getComponent(self, composite, componentName):
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
        if componentName in self.EXPOSURE_COMPONENTS:
            return super().getComponent(composite, componentName)
        elif componentName in self.EXPOSURE_INFO_COMPONENTS:
            if hasattr(composite, "getInfo"):
                # it is possible for this method to be called with
                # an ExposureInfo composite so trap for that and only get
                # the ExposureInfo if the method is supported
                composite = composite.getInfo()
            return super().getComponent(composite, componentName)
        else:
            raise AttributeError("Do not know how to retrieve component {} from {}".format(componentName,
                                                                                           type(composite)))

    def getValidComponents(self, composite):
        """Extract all non-None components from a composite.

        Parameters
        ----------
        composite : `object`
            Composite from which to extract components.

        Returns
        -------
        comps : `dict`
            Non-None components extracted from the composite, indexed by the
            component name as derived from the `self.storageClass`.
        """
        # For Exposure we call the generic version twice: once for top level
        # components, and again for ExposureInfo.
        expItems, expInfoItems = self._groupRequestedComponents()

        components = super().getValidComponents(composite)
        infoComps = super().getValidComponents(composite.getInfo())
        components.update(infoComps)
        return components

    def disassemble(self, composite):
        """Disassemble an afw Exposure.

        This implementation attempts to extract components from the parent
        by looking for attributes of the same name or getter methods derived
        from the component name.

        Parameters
        ----------
        composite : `lsst.afw.Exposure`
            `Exposure` composite object consisting of components to be
            extracted.

        Returns
        -------
        components : `dict`
            `dict` with keys matching the components defined in
            `self.storageClass` and values being `DatasetComponent` instances
            describing the component.

        Raises
        ------
        ValueError
            A requested component can not be found in the parent using generic
            lookups.
        TypeError
            The parent object does not match the supplied `self.storageClass`.
        """
        if not self.storageClass.validateInstance(composite):
            raise TypeError("Unexpected type mismatch between parent and StorageClass"
                            " ({} != {})".format(type(composite), self.storageClass.pytype))

        # Only look for components that are defined by the StorageClass
        components = {}
        expItems, expInfoItems = self._groupRequestedComponents()

        fromExposure = super().disassemble(composite, subset=expItems)
        components.update(fromExposure)

        fromExposureInfo = super().disassemble(composite,
                                               subset=expInfoItems, override=composite.getInfo())
        components.update(fromExposureInfo)

        return components

    def assemble(self, components):
        """Construct an Exposure from components.

        Parameters
        ----------
        components : `dict`
            All the components from which to construct the Exposure.
            Some can be missing.

        Returns
        -------
        exposure : `~lsst.afw.image.Exposure`
            Assembled exposure.

        Raises
        ------
        ValueError
            Some supplied components are not recognized.
        """
        components = components.copy()
        maskedImageComponents = {}
        hasMaskedImage = False
        for component in ("image", "variance", "mask"):
            value = None
            if component in components:
                hasMaskedImage = True
                value = components.pop(component)
            maskedImageComponents[component] = value

        wcs = None
        if "wcs" in components:
            wcs = components.pop("wcs")

        pytype = self.storageClass.pytype
        if hasMaskedImage:
            maskedImage = makeMaskedImage(**maskedImageComponents)
            exposure = makeExposure(maskedImage, wcs=wcs)

            if not isinstance(exposure, pytype):
                raise RuntimeError("Unexpected type created in assembly;"
                                   " was {} expected {}".format(type(exposure), pytype))

        else:
            exposure = pytype()
            if wcs is not None:
                exposure.setWcs(wcs)

        # Set other components
        exposure.setPsf(components.pop("psf", None))
        exposure.setCalib(components.pop("calib", None))

        info = exposure.getInfo()
        if "visitInfo" in components:
            info.setVisitInfo(components.pop("visitInfo"))
        info.setApCorrMap(components.pop("apCorrMap", None))
        info.setCoaddInputs(components.pop("coaddInputs", None))

        # If we have some components left over that is a problem
        if components:
            raise ValueError("The following components were not understood:"
                             " {}".format(list(components.keys())))

        return exposure

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

from abc import ABCMeta, abstractmethod

from .fitsExposureFormatter import FitsExposureFormatter


__all__ = ("FitsRawFormatterBase",)


class FitsRawFormatterBase(FitsExposureFormatter, metaclass=ABCMeta):
    """Abstract base class for reading and writing raw data to and from
    FITS files.

    Subclasses must provide implementations of `readImage` and
    `makeRawVisitInfo`.  Other methods may also be overridden to provide
    additional components (most default to `None`).
    """

    @abstractmethod
    def readImage(self, fileDescriptor):
        """Read just the image component of the Exposure.

        Parameters
        ----------
        fileDescriptor : `FileDescriptor`
            Identifies the file to read and parameters to be used for reading.

        Returns
        -------
        image : `~lsst.afw.image.Image`
            In-memory image component.
        """
        raise NotImplementedError("Must be implemented by subclasses.")

    def readMask(self, fileDescriptor):
        """Read just the mask component of the Exposure.

        May return None (as the default implementation does) to indicate that
        there is no mask information to be extracted (at least not trivially)
        from the raw data.  This will prohibit direct reading of just the mask,
        and set the mask of the full Exposure to zeros.

        Parameters
        ----------
        fileDescriptor : `FileDescriptor`
            Identifies the file to read and parameters to be used for reading.

        Returns
        -------
        mask : `~lsst.afw.image.Mask`
            In-memory mask component.
        """
        return None

    def readVariance(self, fileDescriptor):
        """Read just the variance component of the Exposure.

        May return None (as the default implementation does) to indicate that
        there is no variance information to be extracted (at least not
        trivially) from the raw data.  This will prohibit direct reading of
        just the variance, and set the variance of the full Exposure to zeros.

        Parameters
        ----------
        fileDescriptor : `FileDescriptor`
            Identifies the file to read and parameters to be used for reading.

        Returns
        -------
        image : `~lsst.afw.image.Image`
            In-memory variance component.
        """
        return None

    def stripMetadata(self, metadata):
        """Remove metadata entries that are parsed into components.

        Parameters
        ----------
        metadata : `~lsst.daf.base.PropertyList`
            Header metadata, to be modified in-place.
        """
        self.makeVisitInfo(metadata)
        self.makeWcs(metadata)

    @abstractmethod
    def makeVisitInfo(self, metadata):
        """Construct a VisitInfo from metadata.

        Parameters
        ----------
        metadata : `~lsst.daf.base.PropertyList`
            Header metadata.  May be modified in-place.

        Returns
        -------
        visitInfo : `~lsst.afw.image.VisitInfo`
            Structured metadata about the observation.
        """
        raise NotImplementedError("Must be implemented by subclasses.")

    def makeWcs(self, metadata):
        """Construct a SkyWcs from metadata.

        Parameters
        ----------
        metadata : `~lsst.daf.base.PropertyList`
            Header metadata.  May be modified in-place.

        Returns
        -------
        wcs : `~lsst.afw.geom.SkyWcs`
            Reversible mapping from pixel coordinates to sky coordinates.
        """
        from lsst.afw.geom import makeSkyWcs
        return makeSkyWcs(metadata, strip=True)

    def readImageComponent(self, fileDescriptor, component):
        """Read the image, mask, or variance component of an Exposure.

        Parameters
        ----------
        fileDescriptor : `FileDescriptor`
            Identifies the file to read and parameters to be used for reading.
        component : `str`, optional
            Component to read from the file.  Always one of "image",
            "variance", or "mask".

        Returns
        -------
        image : `~lsst.afw.image.Image` or `~lsst.afw.image.Mask`
            In-memory image, variance, or mask component.
        """
        if component == "image":
            return self.readImage(fileDescriptor)
        elif component == "mask":
            return self.readMask(fileDescriptor)
        elif component == "variance":
            return self.readVariance(fileDescriptor)

    def readInfoComponent(self, fileDescriptor, component):
        """Read a component held by ExposureInfo.

        The implementation provided by FitsRawFormatter provides only "wcs"
        and "visitInfo".  When adding support for other components, subclasses
        should delegate to `super()` for those and update `readFull` with
        similar logic.

        Parameters
        ----------
        fileDescriptor : `FileDescriptor`
            Identifies the file to read and parameters to be used for reading.
        component : `str`, optional
            Component to read from the file.

        Returns
        -------
        obj : component-dependent
            In-memory component object.
        """
        if component == "visitInfo":
            return self.makeVisitInfo(self.readMetadata(fileDescriptor))
        elif component == "wcs":
            return self.makeWcs(self.readMetadata(fileDescriptor))
        return None

    def readFull(self, fileDescriptor, parameters=None):
        """Read the full Exposure object.

        Parameters
        ----------
        fileDescriptor : `FileDescriptor`
            Identifies the file to read and parameters to be used for reading.
        parameters : `dict`, optional
            If specified a dictionary of slicing parameters that overrides
            those in ``fileDescriptor`.

        Returns
        -------
        exposure : `~lsst.afw.image.Exposure`
            Complete in-memory exposure.
        """
        from lsst.afw.image import makeExposure, makeMaskedImage
        full = makeExposure(makeMaskedImage(self.readImage(fileDescriptor)))
        mask = self.readMask(fileDescriptor)
        if mask is not None:
            full.setMask(mask)
        variance = self.readVariance(fileDescriptor)
        if variance is not None:
            full.setVariance(variance)
        metadata = self.readMetadata(fileDescriptor)
        info = full.getInfo()
        info.setVisitInfo(self.makeVisitInfo(metadata))
        info.setWcs(self.makeWcs(metadata))
        # We shouldn't have to call stripMetadata() here because that should
        # have been done by makeVisitInfo and makeWcs (or by subclasses that
        # strip metadata for other components when constructing them).
        full.setMetadata(metadata)
        return full

    def write(self, inMemoryDataset, fileDescriptor):
        """Write a Python object to a file.

        Parameters
        ----------
        inMemoryDataset : `object`
            The Python object to store.
        fileDescriptor : `FileDescriptor`
            Identifies the file to read, type to read it into and parameters
            to be used for reading.

        Returns
        -------
        path : `str`
            The `URI` where the primary file is stored.
        """
        raise NotImplementedError("Raw data cannot be `put`.")

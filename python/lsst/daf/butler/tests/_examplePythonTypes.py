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

"""
Python classes that can be used to test datastores without requiring
large external dependencies on python classes such as afw or serialization
formats such as FITS or HDF5.
"""

__all__ = ("ListAssembler", "MetricsAssembler", "MetricsExample")


import copy
from lsst.daf.butler import CompositeAssembler


class MetricsExample:
    """Smorgasboard of information that might be the result of some
    processing.

    Parameters
    ----------
    summary : `dict`
        Simple dictionary mapping key performance metrics to a scalar
        result.
    output : `dict`
        Structured nested data.
    data : `list`, optional
        Arbitrary array data.
    """

    def __init__(self, summary=None, output=None, data=None):
        self.summary = summary
        self.output = output
        self.data = data

    def __eq__(self, other):
        return self.summary == other.summary and self.output == other.output and self.data == other.data

    def exportAsDict(self):
        """Convert object contents to a single python dict."""
        exportDict = {"summary": self.summary,
                      "output": self.output}
        if self.data is not None:
            exportDict["data"] = list(self.data)
        return exportDict

    def _asdict(self):
        """Convert object contents to a single Python dict.

        This interface is used for JSON serialization.

        Returns
        -------
        exportDict : `dict`
            Object contents in the form of a dict with keys corresponding
            to object attributes.
        """
        return self.exportAsDict()

    @classmethod
    def makeFromDict(cls, exportDict):
        """Create a new object from a dict that is compatible with that
        created by `exportAsDict`.

        Parameters
        ----------
        exportDict : `dict`
            `dict` with keys "summary", "output", and (optionally) "data".

        Returns
        -------
        newobject : `MetricsExample`
            New `MetricsExample` object.
        """
        data = None
        if "data" in exportDict:
            data = exportDict["data"]
        return cls(exportDict["summary"], exportDict["output"], data)


class ListAssembler(CompositeAssembler):
    """Parameter handler for list parameters"""

    def handleParameters(self, inMemoryDataset, parameters=None):
        """Modify the in-memory dataset using the supplied parameters,
        returning a possibly new object.

        Parameters
        ----------
        inMemoryDataset : `object`
            Object to modify based on the parameters.
        parameters : `dict`
            Parameters to apply. Values are specific to the parameter.
            Supported parameters are defined in the associated
            `StorageClass`.  If no relevant parameters are specified the
            inMemoryDataset will be return unchanged.

        Returns
        -------
        inMemoryDataset : `object`
            Updated form of supplied in-memory dataset, after parameters
            have been used.
        """
        inMemoryDataset = copy.deepcopy(inMemoryDataset)
        use = self.storageClass.filterParameters(parameters, subset={"slice"})
        if use:
            inMemoryDataset = inMemoryDataset[use["slice"]]
        return inMemoryDataset


class MetricsAssembler(CompositeAssembler):
    """Parameter handler for parameters using Metrics"""

    def handleParameters(self, inMemoryDataset, parameters=None):
        """Modify the in-memory dataset using the supplied parameters,
        returning a possibly new object.

        Parameters
        ----------
        inMemoryDataset : `object`
            Object to modify based on the parameters.
        parameters : `dict`
            Parameters to apply. Values are specific to the parameter.
            Supported parameters are defined in the associated
            `StorageClass`.  If no relevant parameters are specified the
            inMemoryDataset will be return unchanged.

        Returns
        -------
        inMemoryDataset : `object`
            Updated form of supplied in-memory dataset, after parameters
            have been used.
        """
        inMemoryDataset = copy.deepcopy(inMemoryDataset)
        use = self.storageClass.filterParameters(parameters, subset={"slice"})
        if use:
            inMemoryDataset.data = inMemoryDataset.data[use["slice"]]
        return inMemoryDataset

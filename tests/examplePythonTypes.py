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

"""
Python classes that can be used to test datastores without requiring
large external dependencies on python classes such as afw or serialization
formats such as FITS or HDF5.
"""


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

    def __init__(self, summary, output, data=None):
        self.summary = summary
        self.output = output
        self.data = data

    def exportAsDict(self):
        """Convert object contents to a single python dict."""
        exportDict = {"summary": self.summary,
                      "output": self.output}
        if self.data is not None:
            exportDict["data"] = list(self.data)
        return exportDict

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


class MetricsExampleC(MetricsExample):
    """Version of `MetricsExample` that we treat as a composite.
    """

    @staticmethod
    def assembleMetrics(exportDict):
        """Construct an object from a dict of components.

        Parameters
        ----------
        exportDict : `dict`
            `dict` with keys "summary", "output", and (optionally) "data".

        Returns
        -------
        metrics : `MetricsExample`
            Newly constructed instance.
        """
        return MetricsExample.makeFromDict(exportDict)

    def disassembleMetrics(self, storageClass):
        """Disassemble a metrics object into components.

        Parameters
        ----------
        storageClass : `StorageClass`
            The storage class to be associated with the disassembly.

        Returns
        -------
        components : `dict`
            Individual components with keys matching the keys of the components
            defined in `storageClass` and values being a tuple with the first
            element the component and the second element the `StorageClass` of
            the component.

        Raises
        ------
        e : ValueError
            A requested component can not be found.
        """
        requested = set(storageClass.components.keys())
        components = {}
        for c in list(requested):
            # Remove from list
            requested.remove(c)
            sc = storageClass.components[c]
            if hasattr(self, c):
                components[c] = (getattr(self, c), sc)

        if requested:
            raise ValueError("There are unhandled components ({})".format(requested))

        return components

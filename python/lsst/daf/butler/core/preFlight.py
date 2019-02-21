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

__all__ = ("DatasetOriginInfo", "DatasetOriginInfoDef",)


from abc import ABCMeta, abstractmethod


class DatasetOriginInfo(metaclass=ABCMeta):
    """Interface for classes providing collection information for pre-flight.

    Pre-flight supports per-DatasetType collections, multiple collections
    can be searched for input Datasets and there is one output collection
    per Dataset. Typically one collection will be used for all inputs and
    possibly separate single collection will be used for all outputs. Some
    configurations may need more complex setup, and this interface
    encapsulates all possible behaviors.
    """

    @abstractmethod
    def getInputCollections(self, datasetTypeName):
        """Return ordered list of input collections for given dataset type.

        Returns
        -------
        collections : `list` of `str`
            Names of input collections.
        """
        pass

    @abstractmethod
    def getOutputCollection(self, datasetTypeName):
        """Return output collection name for given dataset type.

        Returns
        -------
        collection : `str`
            Name of output collection.
        """
        pass


class DatasetOriginInfoDef(DatasetOriginInfo):
    """Default implementation of the DatasetOriginInfo.

    Parameters
    ----------
    defaultInputs : `list` of `str`
        Default list of input collections, used for dataset types for which
        there are no overrides.
    defaultOutput : `str`
        Default output collection, used for dataset types for which there is
        no override.
    inputOverrides : `dict` {`str`: `list` of `str`}, optional
        Per-DatasetType overrides for input collections. The key is the name
        of the DatasetType, the value is a list of input collection names.
    outputOverrides : `dict` {`str`: `str`}, optional
        Per-DatasetType overrides for output collections. The key is the name
        of the DatasetType, the value is output collection name.
    """
    def __init__(self, defaultInputs, defaultOutput, inputOverrides=None,
                 outputOverrides=None):
        self.defaultInputs = defaultInputs
        self.defaultOutput = defaultOutput
        self.inputOverrides = inputOverrides or {}
        self.outputOverrides = outputOverrides or {}

    def getInputCollections(self, datasetTypeName):
        """Return ordered list of input collections for given dataset type.

        Returns
        -------
        collections : `list` of `str`
            Names of input collections.
        """
        return self.inputOverrides.get(datasetTypeName, self.defaultInputs)

    def getOutputCollection(self, datasetTypeName):
        """Return output collection name for given dataset type.

        Returns
        -------
        collection : `str`
            Name of output collection.
        """
        return self.outputOverrides.get(datasetTypeName, self.defaultOutput)

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


__all__ = ["makeTestButler", "addDatasetType"]


from lsst.daf.butler import Butler, DatasetType


def makeTestButler(root, dataIds):
    """Create an empty repository with default configuration.

    Parameters
    ----------
    root : `str`
        The location of the root directory for the repository.
    dataIds : `~collections.abc.Mapping` [`str`, `iterable`]
        A mapping keyed by the dimensions used in the test. Each value
        is a mapping of fields and values for that dimension. See
        :file:`daf/butler/config/dimensions.yaml` for required fields,
        listed as "keys" and "requires" under each dimension's entry.

    Returns
    -------
    butler : `lsst.daf.butler.Butler`
        A Butler referring to the new repository.
    """
    # TODO: takes 5 seconds to run; split up into class-level Butler
    #     with test-level runs after DM-21246
    Butler.makeRepo(root)
    butler = Butler(root, run="test")
    for dimension, values in dataIds.items():
        butler.registry.insertDimensionData(dimension, *values)
    return butler


def addDatasetType(butler, name, dimensions, storageClass):
    """Add a new dataset type to a repository.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The repository to update.
    name : `str`
        The name of the dataset type.
    dimensions : `set` [`str`]
        The dimensions of the new dataset type.
    storageClass : `str`
        The storage class the dataset will use.

    Returns
    -------
    datasetType : `lsst.daf.butler.DatasetType`
        The new type.

    Raises
    ------
    ValueError
        Raised if the dimensions or storage class is invalid.
    """
    try:
        datasetType = DatasetType(name, dimensions, storageClass,
                                  universe=butler.registry.dimensions)
        butler.registry.registerDatasetType(datasetType)
        return datasetType
    except KeyError as e:
        raise ValueError from e

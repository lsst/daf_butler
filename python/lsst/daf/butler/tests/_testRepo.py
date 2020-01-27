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
        is an iterable of names for that dimension (e.g., detector IDs for
        `"detector"`). Related dimensions (e.g., instruments and detectors)
        are linked arbitrarily.

    Returns
    -------
    butler : `lsst.daf.butler.Butler`
        A Butler referring to the new repository.

    Notes
    -----
    This function provides a "quick and dirty" repository for simple unit
    tests that don't depend on complex data relationships. Because it assigns
    dimension relationships and other metadata abitrarily, it is ill-suited
    for tests where the structure of the data matters. If you need such a
    dataset, create it directly or use a saved test dataset.
    """
    # TODO: takes 5 seconds to run; split up into class-level Butler
    #     with test-level runs after DM-21246
    Butler.makeRepo(root)
    butler = Butler(root, run="test")
    dimensionRecords = _makeRecords(dataIds, butler.registry.dimensions)
    for dimension, records in dimensionRecords.items():
        butler.registry.insertDimensionData(dimension, *records)
    return butler


def _makeRecords(dataIds, universe):
    """Create cross-linked dimension records from a collection of
    data ID values.

    Parameters
    ----------
    dataIds : `~collections.abc.Mapping` [`str`, `iterable`]
        A mapping keyed by the dimensions of interest. Each value is an
        iterable of names for that dimension (e.g., detector IDs for
        `"detector"`).
    universe : lsst.daf.butler.DimensionUniverse
        Set of all known dimensions and their relationships.

    Returns
    -------
    dataIds : `~collections.abc.Mapping` [`str`, `iterable`]
        A mapping keyed by the dimensions of interest, giving one
        `~lsst.daf.butler.DimensionRecord` for each input name. Related
        dimensions (e.g., instruments and detectors) are linked arbitrarily.
    """
    expandedIds = {}
    # Provide alternate keys like detector names
    for name, values in dataIds.items():
        expandedIds[name] = []
        dimension = universe[name]
        for value in values:
            expandedValue = {}
            for key in dimension.uniqueKeys:
                if key.nbytes:
                    castType = bytes
                else:
                    castType = key.dtype().python_type
                try:
                    castValue = castType(value)
                except TypeError:
                    castValue = castType()
                expandedValue[key.name] = castValue
            for key in dimension.metadata:
                if not key.nullable:
                    expandedValue[key.name] = key.dtype().python_type(value)
            expandedIds[name].append(expandedValue)

    # Pick cross-relationships arbitrarily
    for name, values in expandedIds.items():
        dimension = universe[name]
        for value in values:
            for other in dimension.graph.required:
                if other != dimension:
                    relation = expandedIds[other.name][0]
                    value[other.name] = relation[other.primaryKey.name]
            # Do not recurse, to keep the user from having to provide
            # irrelevant dimensions
            for other in dimension.implied:
                if other != dimension and other.name in expandedIds and other.viewOf is None:
                    relation = expandedIds[other.name][0]
                    value[other.name] = relation[other.primaryKey.name]

    return {dimension: [universe[dimension].RecordClass.fromDict(value) for value in values]
            for dimension, values in expandedIds.items()}


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

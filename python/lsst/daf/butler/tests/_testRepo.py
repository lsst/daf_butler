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


__all__ = ["makeTestRepo", "makeTestCollection", "addDatasetType", "expandUniqueId"]

import random
from lsst.daf.butler import Butler, Config, DatasetType


def makeTestRepo(root, dataIds, *, config=None, **kwargs):
    """Create an empty repository with dummy data IDs.

    Parameters
    ----------
    root : `str`
        The location of the root directory for the repository.
    dataIds : `~collections.abc.Mapping` [`str`, `iterable`]
        A mapping keyed by the dimensions used in the test. Each value
        is an iterable of names for that dimension (e.g., detector IDs for
        `"detector"`). Related dimensions (e.g., instruments and detectors)
        are linked arbitrarily.
    config : `lsst.daf.butler.Config`, optional
        A configuration for the repository (for details, see
        `lsst.daf.butler.Butler.makeRepo`). If omitted, creates a repository
        with default dataset and storage types, but optimized for speed.
        The defaults set ``.datastore.cls``, ``.datastore.checksum`` and
        ``.registry.db``.  If a supplied config does not specify these values
        the internal defaults will be used to ensure that we have a usable
        configuration.
    **kwargs
        Extra arguments to `lsst.daf.butler.Butler.makeRepo`.

    Returns
    -------
    butler : `lsst.daf.butler.Butler`
        A Butler referring to the new repository. This Butler is provided only
        for additional setup; to keep test cases isolated, it is highly
        recommended that each test create its own Butler with a
        unique run/collection. See `makeTestCollection`.

    Notes
    -----
    This function provides a "quick and dirty" repository for simple unit
    tests that don't depend on complex data relationships. Because it assigns
    dimension relationships and other metadata abitrarily, it is ill-suited
    for tests where the structure of the data matters. If you need such a
    dataset, create it directly or use a saved test dataset.

    Since the values in ``dataIds`` uniquely determine the repository's
    data IDs, the fully linked IDs can be recovered by calling
    `expandUniqueId`, so long as no other code has inserted dimensions into
    the repository registry.
    """
    defaults = Config()
    defaults["datastore", "cls"] = "lsst.daf.butler.datastores.inMemoryDatastore.InMemoryDatastore"
    defaults["datastore", "checksum"] = False  # In case of future changes
    defaults["registry", "db"] = "sqlite:///:memory:"

    if config:
        defaults.update(config)

    # newConfig guards against location-related keywords like outfile
    newConfig = Butler.makeRepo(root, config=defaults, **kwargs)
    butler = Butler(newConfig, writeable=True)
    dimensionRecords = _makeRecords(dataIds, butler.registry.dimensions)
    for dimension, records in dimensionRecords.items():
        butler.registry.insertDimensionData(dimension, *records)
    return butler


def makeTestCollection(repo):
    """Create a read/write Butler to a fresh collection.

    Parameters
    ----------
    repo : `lsst.daf.butler.Butler`
        A previously existing Butler to a repository, such as that returned by
        `~lsst.daf.butler.Butler.makeRepo` or `makeTestRepo`.

    Returns
    -------
    butler : `lsst.daf.butler.Butler`
        A Butler referring to a new collection in the repository at ``root``.
        The collection is (almost) guaranteed to be new.
    """
    # Create a "random" collection name
    # Speed matters more than cryptographic guarantees
    collection = "test" + str(random.randrange(1_000_000_000))
    return Butler(butler=repo, run=collection)


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
            for other in dimension.required:
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


def expandUniqueId(butler, partialId):
    """Return a complete data ID matching some criterion.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The repository to query.
    partialId : `~collections.abc.Mapping` [`str`, any]
        A mapping of known dimensions and values.

    Returns
    -------
    dataId : `lsst.daf.butler.DataCoordinate`
        The unique data ID that matches ``partialId``.

    Raises
    ------
    ValueError
        Raised if ``partialId`` does not uniquely identify a data ID.

    Notes
    -----
    This method will only work correctly if all dimensions attached to the
    target dimension (eg., "physical_filter" for "visit") are known to the
    repository, even if they're not needed to identify a dataset.

    Examples
    --------
    .. code-block:: py

       >>> butler = makeTestRepo(
               "testdir", {"instrument": ["notACam"], "detector": [1]})
       >>> expandUniqueId(butler, {"detector": 1})
       DataCoordinate({instrument, detector}, ('notACam', 1))
    """
    # The example is *not* a doctest because it requires dangerous I/O
    registry = butler.registry
    dimensions = registry.dimensions.extract(partialId.keys()).required

    query = " AND ".join(f"{dimension} = {value!r}" for dimension, value in partialId.items())

    dataId = [id for id in registry.queryDimensions(dimensions, where=query, expand=False)]
    if len(dataId) == 1:
        return dataId[0]
    else:
        raise ValueError(f"Found {len(dataId)} matches for {partialId}, expected 1.")


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

    Notes
    -----
    Dataset types are shared across all collections in a repository, so this
    function does not need to be run for each collection.
    """
    try:
        datasetType = DatasetType(name, dimensions, storageClass,
                                  universe=butler.registry.dimensions)
        butler.registry.registerDatasetType(datasetType)
        return datasetType
    except KeyError as e:
        raise ValueError from e

# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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

from __future__ import annotations

__all__ = [
    "makeTestRepo",
    "makeTestCollection",
    "addDatasetType",
    "expandUniqueId",
    "DatastoreMock",
    "addDataIdValue",
]

import random
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import sqlalchemy
from lsst.daf.butler import (
    Butler,
    Config,
    DataCoordinate,
    DatasetRef,
    DatasetType,
    Dimension,
    DimensionUniverse,
    FileDataset,
    StorageClass,
)

if TYPE_CHECKING:
    from lsst.daf.butler import DatasetId


def makeTestRepo(
    root: str, dataIds: Mapping[str, Iterable] | None = None, *, config: Config | None = None, **kwargs: Any
) -> Butler:
    """Create an empty test repository.

    Parameters
    ----------
    root : `str`
        The location of the root directory for the repository.
    dataIds : `~collections.abc.Mapping` [`str`, `iterable`], optional
        A mapping keyed by the dimensions used in the test. Each value is an
        iterable of names for that dimension (e.g., detector IDs for
        `"detector"`). Related dimensions (e.g., instruments and detectors) are
        linked arbitrarily, with values created for implied dimensions only
        when needed. This parameter is provided for compatibility with old
        code; newer code should make the repository, then call
        `~lsst.daf.butler.tests.addDataIdValue`.
    config : `lsst.daf.butler.Config`, optional
        A configuration for the repository (for details, see
        `lsst.daf.butler.Butler.makeRepo`). If omitted, creates a repository
        with default dataset and storage types, but optimized for speed.  The
        defaults set ``.datastore.cls``, ``.datastore.checksum`` and
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
        recommended that each test create its own Butler with a unique
        run/collection. See `makeTestCollection`.

    Notes
    -----
    This function provides a "quick and dirty" repository for simple unit tests
    that don't depend on complex data relationships. It is ill-suited for tests
    where the structure of the data matters. If you need such a dataset, create
    it directly or use a saved test dataset.
    """
    defaults = Config()
    defaults["datastore", "cls"] = "lsst.daf.butler.datastores.inMemoryDatastore.InMemoryDatastore"
    defaults["datastore", "checksum"] = False  # In case of future changes
    defaults["registry", "db"] = "sqlite:///<butlerRoot>/gen3.sqlite3"

    if config:
        defaults.update(config)

    if not dataIds:
        dataIds = {}

    # Disable config root by default so that our registry override will
    # not be ignored.
    # newConfig guards against location-related keywords like outfile
    newConfig = Butler.makeRepo(root, config=defaults, forceConfigRoot=False, **kwargs)
    butler = Butler.from_config(newConfig, writeable=True)
    dimensionRecords = _makeRecords(dataIds, butler.dimensions)
    for dimension, records in dimensionRecords.items():
        if butler.dimensions[dimension].viewOf is None:
            butler.registry.insertDimensionData(dimension, *records)
    return butler


def makeTestCollection(repo: Butler, uniqueId: str | None = None) -> Butler:
    """Create a read/write Butler to a fresh collection.

    Parameters
    ----------
    repo : `lsst.daf.butler.Butler`
        A previously existing Butler to a repository, such as that returned by
        `~lsst.daf.butler.Butler.makeRepo` or `makeTestRepo`.
    uniqueId : `str`, optional
        A collection ID guaranteed by external code to be unique across all
        calls to ``makeTestCollection`` for the same repository.

    Returns
    -------
    butler : `lsst.daf.butler.Butler`
        A Butler referring to a new collection in the repository at ``root``.
        The collection is (almost) guaranteed to be new.

    Notes
    -----
    This function creates a single run collection that does not necessarily
    conform to any repository conventions. It is only suitable for creating an
    isolated test area, and not for repositories intended for real data
    processing or analysis.
    """
    if not uniqueId:
        # Create a "random" collection name
        # Speed matters more than cryptographic guarantees
        uniqueId = str(random.randrange(1_000_000_000))
    collection = "test_" + uniqueId
    return Butler.from_config(butler=repo, run=collection)


def _makeRecords(dataIds: Mapping[str, Iterable], universe: DimensionUniverse) -> Mapping[str, Iterable]:
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
    # Create values for all dimensions that are (recursive) required or implied
    # dependencies of the given ones.
    complete_data_id_values = {}
    for dimension_name in universe.conform(dataIds.keys()).names:
        if dimension_name in dataIds:
            complete_data_id_values[dimension_name] = list(dataIds[dimension_name])
        if dimension_name not in complete_data_id_values:
            complete_data_id_values[dimension_name] = [
                _makeRandomDataIdValue(universe.dimensions[dimension_name])
            ]

    # Start populating dicts that will become DimensionRecords by providing
    # alternate keys like detector names
    record_dicts_by_dimension_name: dict[str, list[dict[str, str | int | bytes]]] = {}
    for name, values in complete_data_id_values.items():
        record_dicts_by_dimension_name[name] = []
        dimension_el = universe[name]
        for value in values:
            # _fillAllKeys wants Dimension and not DimensionElement.
            # universe.__getitem__ says it returns DimensionElement but this
            # really does also seem to be a Dimension here.
            record_dicts_by_dimension_name[name].append(
                _fillAllKeys(dimension_el, value)  # type: ignore[arg-type]
            )

    # Pick cross-relationships arbitrarily
    for name, record_dicts in record_dicts_by_dimension_name.items():
        dimension_el = universe[name]
        for record_dict in record_dicts:
            for other in dimension_el.dimensions:
                if other != dimension_el:
                    relation = record_dicts_by_dimension_name[other.name][0]
                    record_dict[other.name] = relation[other.primaryKey.name]

    return {
        dimension: [universe[dimension].RecordClass(**record_dict) for record_dict in record_dicts]
        for dimension, record_dicts in record_dicts_by_dimension_name.items()
    }


def _fillAllKeys(dimension: Dimension, value: str | int) -> dict[str, str | int | bytes]:
    """Create an arbitrary mapping of all required keys for a given dimension
    that do not refer to other dimensions.

    Parameters
    ----------
    dimension : `lsst.daf.butler.Dimension`
        The dimension for which to generate a set of keys (e.g., detector).
    value
        The value assigned to ``dimension`` (e.g., detector ID).

    Returns
    -------
    expandedValue : `dict` [`str`]
        A mapping of dimension keys to values. ``dimension's`` primary key
        maps to ``value``, but all other mappings (e.g., detector name)
        are arbitrary.
    """
    expandedValue: dict[str, str | int | bytes] = {}
    for key in dimension.uniqueKeys:
        if key.nbytes:
            # For `bytes` fields, we want something that casts at least `str`
            # and `int` values to bytes and yields b'' when called with no
            # arguments (as in the except block below).  Unfortunately, the
            # `bytes` type itself fails for both `str` and `int`, but this
            # lambda does what we need.  This particularly important for the
            # skymap dimensions' bytes 'hash' field, which has a unique
            # constraint; without this, all skymaps would get a hash of b''
            # and end up conflicting.
            castType = lambda *args: str(*args).encode()  # noqa: E731
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
    return expandedValue


def _makeRandomDataIdValue(dimension: Dimension) -> int | str:
    """Generate a random value of the appropriate type for a data ID key.

    Parameters
    ----------
    dimension : `Dimension`
        Dimension the value corresponds to.

    Returns
    -------
    value : `int` or `str`
        Random value.
    """
    if dimension.primaryKey.getPythonType() is str:
        return str(random.randrange(1000))
    else:
        return random.randrange(1000)


def expandUniqueId(butler: Butler, partialId: Mapping[str, Any]) -> DataCoordinate:
    """Return a complete data ID matching some criterion.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The repository to query.
    partialId : `~collections.abc.Mapping` [`str`]
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
    repository, even if they're not needed to identify a dataset. This function
    is only suitable for certain kinds of test repositories, and not for
    repositories intended for real data processing or analysis.

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
    dimensions = registry.dimensions.conform(partialId.keys()).required

    query = " AND ".join(f"{dimension} = {value!r}" for dimension, value in partialId.items())

    # Much of the purpose of this function is to do something we explicitly
    # reject most of the time: query for a governor dimension (e.g. instrument)
    # given something that depends on it (e.g. visit), hence check=False.
    dataId = list(registry.queryDataIds(dimensions, where=query, check=False))
    if len(dataId) == 1:
        return dataId[0]
    else:
        raise ValueError(f"Found {len(dataId)} matches for {partialId}, expected 1.")


def _findOrInventDataIdValue(
    butler: Butler, data_id: dict[str, str | int], dimension: Dimension
) -> tuple[str | int, bool]:
    """Look up an arbitrary value for a dimension that is consistent with a
    partial data ID that does not specify that dimension, or invent one if no
    such value exists.

    Parameters
    ----------
    butler : `Butler`
        Butler to use to look up data ID values.
    data_id : `dict` [ `str`, `str` or `int` ]
        Dictionary of possibly-related data ID values.
    dimension : `Dimension`
        Dimension to obtain a value for.

    Returns
    -------
    value : `int` or `str`
        Value for this dimension.
    invented : `bool`
        `True` if the value had to be invented, `False` if a compatible value
        already existed.
    """
    # No values given by caller for this dimension.  See if any exist
    # in the registry that are consistent with the values of dimensions
    # we do have:
    match_data_id = {key: data_id[key] for key in data_id.keys() & dimension.dimensions.names}
    matches = list(butler.registry.queryDimensionRecords(dimension, dataId=match_data_id).limit(1))
    if not matches:
        # Nothing in the registry matches: invent a data ID value
        # with the right type (actual value does not matter).
        # We may or may not actually make a record with this; that's
        # easier to check later.
        dimension_value = _makeRandomDataIdValue(dimension)
        return dimension_value, True
    else:
        # A record does exist in the registry.  Use its data ID value.
        dim_value = matches[0].dataId[dimension.name]
        assert dim_value is not None
        return dim_value, False


def _makeDimensionRecordDict(data_id: dict[str, str | int], dimension: Dimension) -> dict[str, Any]:
    """Create a dictionary that can be used to build a `DimensionRecord` that
    is consistent with the given data ID.

    Parameters
    ----------
    data_id : `dict` [ `str`, `str` or `int` ]
        Dictionary that contains values for at least all of
        ``dimension.dimensions.names`` (the main dimension, its recursive
        required dependencies, and its non-recursive implied dependencies).
    dimension : `Dimension`
        Dimension to build a record dictionary for.

    Returns
    -------
    record_dict : `dict` [ `str`, `object` ]
        Dictionary that can be passed as ``**kwargs`` to this dimensions
        record class constructor.
    """
    # Add the primary key field for this dimension.
    record_dict: dict[str, Any] = {dimension.primaryKey.name: data_id[dimension.name]}
    # Define secondary keys (e.g., detector name given detector id)
    record_dict.update(_fillAllKeys(dimension, data_id[dimension.name]))
    # Set the foreign key values for any related dimensions that should
    # appear in the record.
    for related_dimension in dimension.dimensions:
        if related_dimension.name != dimension.name:
            record_dict[related_dimension.name] = data_id[related_dimension.name]
    return record_dict


def addDataIdValue(butler: Butler, dimension: str, value: str | int, **related: str | int) -> None:
    """Add the records that back a new data ID to a repository.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The repository to update.
    dimension : `str`
        The name of the dimension to gain a new value.
    value
        The value to register for the dimension.
    **related
        Any existing dimensions to be linked to ``value``.

    Notes
    -----
    Related dimensions (e.g., the instrument associated with a detector) may be
    specified using ``related``, which requires a value for those dimensions to
    have been added to the repository already (generally with a previous call
    to `addDataIdValue`.  Any dependencies of the given dimension that are not
    included in ``related`` will be linked to existing values arbitrarily, and
    (for implied dependencies only) created and also inserted into the registry
    if they do not exist.  Values for required dimensions and those given in
    ``related`` are never created.

    Because this function creates filler data, it is only suitable for test
    repositories. It should not be used for repositories intended for real data
    processing or analysis, which have known dimension values.

    Examples
    --------
    See the guide on :ref:`using-butler-in-tests-make-repo` for usage examples.
    """
    # Example is not doctest, because it's probably unsafe to create even an
    # in-memory butler in that environment.
    try:
        full_dimension = butler.dimensions[dimension]
    except KeyError as e:
        raise ValueError from e
    # Bad keys ignored by registry code
    extra_keys = related.keys() - full_dimension.minimal_group.names
    if extra_keys:
        raise ValueError(
            f"Unexpected keywords {extra_keys} not found in {full_dimension.minimal_group.names}"
        )

    # Assemble a dictionary data ID holding the given primary dimension value
    # and all of the related ones.
    data_id: dict[str, int | str] = {dimension: value}
    data_id.update(related)

    # Compute the set of all dimensions that these recursively depend on.
    all_dimensions = butler.dimensions.conform(data_id.keys())

    # Create dicts that will become DimensionRecords for all of these data IDs.
    # This iteration is guaranteed to be in topological order, so we can count
    # on new data ID values being invented before they are needed.
    record_dicts_by_dimension: dict[Dimension, dict[str, Any]] = {}
    for dimension_name in all_dimensions.names:
        dimension_obj = butler.dimensions.dimensions[dimension_name]
        dimension_value = data_id.get(dimension_name)
        if dimension_value is None:
            data_id[dimension_name], invented = _findOrInventDataIdValue(butler, data_id, dimension_obj)
            if not invented:
                # No need to make a new record; one already exists.
                continue
        if dimension_name in related:
            # Caller passed in a value of this dimension explicitly, but it
            # isn't the primary dimension they asked to have a record created
            # for.  That means they expect this record to already exist.
            continue
        if dimension_name != dimension and dimension_name in all_dimensions.required:
            # We also don't want to automatically create new dimension records
            # for required dimensions (except for the main dimension the caller
            # asked for); those are also asserted by the caller to already
            # exist.
            continue
        if dimension_obj.viewOf is not None:
            # Don't need to bother generating full records for dimensions whose
            # records are just a view into some other's records anyway.
            continue
        record_dicts_by_dimension[dimension_obj] = _makeDimensionRecordDict(data_id, dimension_obj)

    # Sync those dimension record dictionaries with the database.
    for dimension_obj, record_dict in record_dicts_by_dimension.items():
        record = dimension_obj.RecordClass(**record_dict)
        try:
            butler.registry.syncDimensionData(dimension_obj, record)
        except sqlalchemy.exc.IntegrityError as e:
            raise RuntimeError(
                "Could not create data ID value. Automatic relationship generation "
                "may have failed; try adding keywords to assign a specific instrument, "
                "physical_filter, etc. based on the nested exception message."
            ) from e


def addDatasetType(butler: Butler, name: str, dimensions: set[str], storageClass: str) -> DatasetType:
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
        datasetType = DatasetType(name, dimensions, storageClass, universe=butler.dimensions)
        butler.registry.registerDatasetType(datasetType)
        return datasetType
    except KeyError as e:
        raise ValueError from e


class DatastoreMock:
    """Mocks a butler datastore.

    Has functions that mock the datastore in a butler. Provides an `apply`
    function to replace the relevent butler datastore functions with the mock
    functions.
    """

    @staticmethod
    def apply(butler: Butler) -> None:
        """Apply datastore mocks to a butler."""
        butler._datastore.export = DatastoreMock._mock_export  # type: ignore
        butler._datastore.get = DatastoreMock._mock_get  # type: ignore
        butler._datastore.ingest = MagicMock()  # type: ignore

    @staticmethod
    def _mock_export(
        refs: Iterable[DatasetRef], *, directory: str | None = None, transfer: str | None = None
    ) -> Iterable[FileDataset]:
        """Mock of `Datastore.export` that satisfies the requirement that
        the refs passed in are included in the `FileDataset` objects
        returned.

        This can be used to construct a `Datastore` mock that can be used
        in repository export via::

            datastore = unittest.mock.Mock(spec=Datastore)
            datastore.export = DatastoreMock._mock_export

        """
        for ref in refs:
            yield FileDataset(
                refs=[ref], path="mock/path", formatter="lsst.daf.butler.formatters.json.JsonFormatter"
            )

    @staticmethod
    def _mock_get(
        ref: DatasetRef,
        parameters: Mapping[str, Any] | None = None,
        storageClass: StorageClass | str | None = None,
    ) -> tuple[DatasetId, Mapping[str, Any] | None]:
        """Mock of `Datastore.get` that just returns the integer dataset ID
        value and parameters it was given.
        """
        return (ref.id, parameters)

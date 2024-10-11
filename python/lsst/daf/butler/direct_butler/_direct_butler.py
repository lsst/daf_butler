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

"""Butler top level classes."""

from __future__ import annotations

__all__ = (
    "DirectButler",
    "ButlerValidationError",
)

import collections.abc
import contextlib
import io
import itertools
import logging
import numbers
import os
import warnings
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator, MutableMapping, Sequence
from types import EllipsisType
from typing import TYPE_CHECKING, Any, ClassVar, TextIO, cast

from deprecated.sphinx import deprecated
from lsst.resources import ResourcePath, ResourcePathExpression
from lsst.utils.introspection import get_class_of
from lsst.utils.logging import VERBOSE, getLogger
from sqlalchemy.exc import IntegrityError

from .._butler import Butler
from .._butler_config import ButlerConfig
from .._butler_instance_options import ButlerInstanceOptions
from .._collection_type import CollectionType
from .._dataset_existence import DatasetExistence
from .._dataset_ref import DatasetRef
from .._dataset_type import DatasetType
from .._deferredDatasetHandle import DeferredDatasetHandle
from .._exceptions import DatasetNotFoundError, DimensionValueError, EmptyQueryResultError, ValidationError
from .._limited_butler import LimitedButler
from .._registry_shim import RegistryShim
from .._storage_class import StorageClass, StorageClassFactory
from .._timespan import Timespan
from ..datastore import Datastore, NullDatastore
from ..dimensions import DataCoordinate, Dimension
from ..direct_query_driver import DirectQueryDriver
from ..progress import Progress
from ..queries import Query
from ..registry import (
    ConflictingDefinitionError,
    DataIdError,
    MissingDatasetTypeError,
    RegistryDefaults,
    _RegistryFactory,
)
from ..registry.sql_registry import SqlRegistry
from ..transfers import RepoExportContext
from ..utils import transactional
from ._direct_butler_collections import DirectButlerCollections

if TYPE_CHECKING:
    from lsst.resources import ResourceHandleProtocol

    from .._dataset_ref import DatasetId
    from .._file_dataset import FileDataset
    from ..datastore import DatasetRefURIs
    from ..dimensions import DataId, DataIdValue, DimensionElement, DimensionRecord, DimensionUniverse
    from ..registry import CollectionArgType, Registry
    from ..transfers import RepoImportBackend

_LOG = getLogger(__name__)


class ButlerValidationError(ValidationError):
    """There is a problem with the Butler configuration."""

    pass


class DirectButler(Butler):  # numpydoc ignore=PR02
    """Main entry point for the data access system.

    Parameters
    ----------
    config : `ButlerConfig`
        The configuration for this Butler instance.
    registry : `SqlRegistry`
        The object that manages dataset metadata and relationships.
    datastore : Datastore
        The object that manages actual dataset storage.
    storageClasses : StorageClassFactory
        An object that maps known storage class names to objects that fully
        describe them.

    Notes
    -----
    Most users should call the top-level `Butler`.``from_config`` instead of
    using this constructor directly.
    """

    # This is __new__ instead of __init__ because we have to support
    # instantiation via the legacy constructor Butler.__new__(), which
    # reads the configuration and selects which subclass to instantiate.  The
    # interaction between __new__ and __init__ is kind of wacky in Python.  If
    # we were using __init__ here, __init__ would be called twice (once when
    # the DirectButler instance is constructed inside Butler.from_config(), and
    # a second time with the original arguments to Butler() when the instance
    # is returned from Butler.__new__()
    def __new__(
        cls,
        *,
        config: ButlerConfig,
        registry: SqlRegistry,
        datastore: Datastore,
        storageClasses: StorageClassFactory,
    ) -> DirectButler:
        self = cast(DirectButler, super().__new__(cls))
        self._config = config
        self._registry = registry
        self._datastore = datastore
        self.storageClasses = storageClasses

        # For execution butler the datastore needs a special
        # dependency-inversion trick. This is not used by regular butler,
        # but we do not have a way to distinguish regular butler from execution
        # butler.
        self._datastore.set_retrieve_dataset_type_method(self._retrieve_dataset_type)

        self._registry_shim = RegistryShim(self)

        return self

    @classmethod
    def create_from_config(
        cls,
        config: ButlerConfig,
        *,
        options: ButlerInstanceOptions,
        without_datastore: bool = False,
    ) -> DirectButler:
        """Construct a Butler instance from a configuration file.

        Parameters
        ----------
        config : `ButlerConfig`
            The configuration for this Butler instance.
        options : `ButlerInstanceOptions`
            Default values and other settings for the Butler instance.
        without_datastore : `bool`, optional
            If `True` do not attach a datastore to this butler. Any attempts
            to use a datastore will fail.

        Notes
        -----
        Most users should call the top-level `Butler`.``from_config``
        instead of using this function directly.
        """
        if "run" in config or "collection" in config:
            raise ValueError("Passing a run or collection via configuration is no longer supported.")

        defaults = RegistryDefaults.from_butler_instance_options(options)
        try:
            butlerRoot = config.get("root", config.configDir)
            writeable = options.writeable
            if writeable is None:
                writeable = options.run is not None
            registry = _RegistryFactory(config).from_config(
                butlerRoot=butlerRoot, writeable=writeable, defaults=defaults
            )
            if without_datastore:
                datastore: Datastore = NullDatastore(None, None)
            else:
                datastore = Datastore.fromConfig(
                    config, registry.getDatastoreBridgeManager(), butlerRoot=butlerRoot
                )
            # TODO: Once datastore drops dependency on registry we can
            # construct datastore first and pass opaque tables to registry
            # constructor.
            registry.make_datastore_tables(datastore.get_opaque_table_definitions())
            storageClasses = StorageClassFactory()
            storageClasses.addFromConfig(config)

            return DirectButler(
                config=config, registry=registry, datastore=datastore, storageClasses=storageClasses
            )
        except Exception:
            # Failures here usually mean that configuration is incomplete,
            # just issue an error message which includes config file URI.
            _LOG.error(f"Failed to instantiate Butler from config {config.configFile}.")
            raise

    def clone(
        self,
        *,
        collections: CollectionArgType | None | EllipsisType = ...,
        run: str | None | EllipsisType = ...,
        inferDefaults: bool | EllipsisType = ...,
        dataId: dict[str, str] | EllipsisType = ...,
    ) -> DirectButler:
        # Docstring inherited
        defaults = self._registry.defaults.clone(collections, run, inferDefaults, dataId)
        registry = self._registry.copy(defaults)

        return DirectButler(
            registry=registry,
            config=self._config,
            datastore=self._datastore.clone(registry.getDatastoreBridgeManager()),
            storageClasses=self.storageClasses,
        )

    GENERATION: ClassVar[int] = 3
    """This is a Generation 3 Butler.

    This attribute may be removed in the future, once the Generation 2 Butler
    interface has been fully retired; it should only be used in transitional
    code.
    """

    def _retrieve_dataset_type(self, name: str) -> DatasetType | None:
        """Return DatasetType defined in registry given dataset type name."""
        try:
            return self.get_dataset_type(name)
        except MissingDatasetTypeError:
            return None

    @classmethod
    def _unpickle(
        cls,
        config: ButlerConfig,
        collections: tuple[str, ...] | None,
        run: str | None,
        defaultDataId: dict[str, str],
        writeable: bool,
    ) -> DirectButler:
        """Callable used to unpickle a Butler.

        We prefer not to use ``Butler.__init__`` directly so we can force some
        of its many arguments to be keyword-only (note that ``__reduce__``
        can only invoke callables with positional arguments).

        Parameters
        ----------
        config : `ButlerConfig`
            Butler configuration, already coerced into a true `ButlerConfig`
            instance (and hence after any search paths for overrides have been
            utilized).
        collections : `tuple` [ `str` ]
            Names of the default collections to read from.
        run : `str`, optional
            Name of the default `~CollectionType.RUN` collection to write to.
        defaultDataId : `dict` [ `str`, `str` ]
            Default data ID values.
        writeable : `bool`
            Whether the Butler should support write operations.

        Returns
        -------
        butler : `Butler`
            A new `Butler` instance.
        """
        return cls.create_from_config(
            config=config,
            options=ButlerInstanceOptions(
                collections=collections, run=run, writeable=writeable, kwargs=defaultDataId
            ),
        )

    def __reduce__(self) -> tuple:
        """Support pickling."""
        return (
            DirectButler._unpickle,
            (
                self._config,
                self.collections.defaults,
                self.run,
                dict(self._registry.defaults.dataId.required),
                self._registry.isWriteable(),
            ),
        )

    def __str__(self) -> str:
        return (
            f"Butler(collections={self.collections}, run={self.run}, "
            f"datastore='{self._datastore}', registry='{self._registry}')"
        )

    def isWriteable(self) -> bool:
        # Docstring inherited.
        return self._registry.isWriteable()

    def _caching_context(self) -> contextlib.AbstractContextManager[None]:
        """Context manager that enables caching."""
        return self._registry.caching_context()

    @contextlib.contextmanager
    def transaction(self) -> Iterator[None]:
        """Context manager supporting `Butler` transactions.

        Transactions can be nested.
        """
        with self._registry.transaction(), self._datastore.transaction():
            yield

    def _standardizeArgs(
        self,
        datasetRefOrType: DatasetRef | DatasetType | str,
        dataId: DataId | None = None,
        for_put: bool = True,
        **kwargs: Any,
    ) -> tuple[DatasetType, DataId | None]:
        """Standardize the arguments passed to several Butler APIs.

        Parameters
        ----------
        datasetRefOrType : `DatasetRef`, `DatasetType`, or `str`
            When `DatasetRef` the `dataId` should be `None`.
            Otherwise the `DatasetType` or name thereof.
        dataId : `dict` or `DataCoordinate`
            A `dict` of `Dimension` link name, value pairs that label the
            `DatasetRef` within a Collection. When `None`, a `DatasetRef`
            should be provided as the second argument.
        for_put : `bool`, optional
            If `True` this call is invoked as part of a `Butler.put()`.
            Otherwise it is assumed to be part of a `Butler.get()`. This
            parameter is only relevant if there is dataset type
            inconsistency.
        **kwargs
            Additional keyword arguments used to augment or construct a
            `DataCoordinate`.  See `DataCoordinate.standardize`
            parameters.

        Returns
        -------
        datasetType : `DatasetType`
            A `DatasetType` instance extracted from ``datasetRefOrType``.
        dataId : `dict` or `DataId`, optional
            Argument that can be used (along with ``kwargs``) to construct a
            `DataId`.

        Notes
        -----
        Butler APIs that conceptually need a DatasetRef also allow passing a
        `DatasetType` (or the name of one) and a `DataId` (or a dict and
        keyword arguments that can be used to construct one) separately. This
        method accepts those arguments and always returns a true `DatasetType`
        and a `DataId` or `dict`.

        Standardization of `dict` vs `DataId` is best handled by passing the
        returned ``dataId`` (and ``kwargs``) to `Registry` APIs, which are
        generally similarly flexible.
        """
        externalDatasetType: DatasetType | None = None
        internalDatasetType: DatasetType | None = None
        if isinstance(datasetRefOrType, DatasetRef):
            if dataId is not None or kwargs:
                raise ValueError("DatasetRef given, cannot use dataId as well")
            externalDatasetType = datasetRefOrType.datasetType
            dataId = datasetRefOrType.dataId
        else:
            # Don't check whether DataId is provided, because Registry APIs
            # can usually construct a better error message when it wasn't.
            if isinstance(datasetRefOrType, DatasetType):
                externalDatasetType = datasetRefOrType
            else:
                internalDatasetType = self.get_dataset_type(datasetRefOrType)

        # Check that they are self-consistent
        if externalDatasetType is not None:
            internalDatasetType = self.get_dataset_type(externalDatasetType.name)
            if externalDatasetType != internalDatasetType:
                # We can allow differences if they are compatible, depending
                # on whether this is a get or a put. A get requires that
                # the python type associated with the datastore can be
                # converted to the user type. A put requires that the user
                # supplied python type can be converted to the internal
                # type expected by registry.
                relevantDatasetType = internalDatasetType
                if for_put:
                    is_compatible = internalDatasetType.is_compatible_with(externalDatasetType)
                else:
                    is_compatible = externalDatasetType.is_compatible_with(internalDatasetType)
                    relevantDatasetType = externalDatasetType
                if not is_compatible:
                    raise ValueError(
                        f"Supplied dataset type ({externalDatasetType}) inconsistent with "
                        f"registry definition ({internalDatasetType})"
                    )
                # Override the internal definition.
                internalDatasetType = relevantDatasetType

        assert internalDatasetType is not None
        return internalDatasetType, dataId

    def _rewrite_data_id(
        self, dataId: DataId | None, datasetType: DatasetType, **kwargs: Any
    ) -> tuple[DataId | None, dict[str, Any]]:
        """Rewrite a data ID taking into account dimension records.

        Take a Data ID and keyword args and rewrite it if necessary to
        allow the user to specify dimension records rather than dimension
        primary values.

        This allows a user to include a dataId dict with keys of
        ``exposure.day_obs`` and ``exposure.seq_num`` instead of giving
        the integer exposure ID.  It also allows a string to be given
        for a dimension value rather than the integer ID if that is more
        convenient. For example, rather than having to specifying the
        detector with ``detector.full_name``, a string given for ``detector``
        will be interpreted as the full name and converted to the integer
        value.

        Keyword arguments can also use strings for dimensions like detector
        and exposure but python does not allow them to include ``.`` and
        so the ``exposure.day_obs`` syntax can not be used in a keyword
        argument.

        Parameters
        ----------
        dataId : `dict` or `DataCoordinate`
            A `dict` of `Dimension` link name, value pairs that will label the
            `DatasetRef` within a Collection.
        datasetType : `DatasetType`
            The dataset type associated with this dataId. Required to
            determine the relevant dimensions.
        **kwargs
            Additional keyword arguments used to augment or construct a
            `DataId`.  See `DataId` parameters.

        Returns
        -------
        dataId : `dict` or `DataCoordinate`
            The, possibly rewritten, dataId. If given a `DataCoordinate` and
            no keyword arguments, the original dataId will be returned
            unchanged.
        **kwargs : `dict`
            Any unused keyword arguments (would normally be empty dict).
        """
        # Process dimension records that are using record information
        # rather than ids
        newDataId: dict[str, DataIdValue] = {}
        byRecord: dict[str, dict[str, Any]] = defaultdict(dict)

        if isinstance(dataId, DataCoordinate):
            # Do nothing if we have a DataCoordinate and no kwargs.
            if not kwargs:
                return dataId, kwargs
            # If we have a DataCoordinate with kwargs, we know the
            # DataCoordinate only has values for real dimensions.
            newDataId.update(dataId.mapping)
        elif dataId:
            # The data is mapping, which means it might have keys like
            # "exposure.obs_id" (unlike kwargs, because a "." is not allowed in
            # a keyword parameter).
            for k, v in dataId.items():
                if isinstance(k, str) and "." in k:
                    # Someone is using a more human-readable dataId
                    dimensionName, record = k.split(".", 1)
                    byRecord[dimensionName][record] = v
                else:
                    newDataId[k] = v

        # Go through the updated dataId and check the type in case someone is
        # using an alternate key.  We have already filtered out the compound
        # keys dimensions.record format.
        not_dimensions = {}

        # Will need to look in the dataId and the keyword arguments
        # and will remove them if they need to be fixed or are unrecognized.
        for dataIdDict in (newDataId, kwargs):
            # Use a list so we can adjust the dict safely in the loop
            for dimensionName in list(dataIdDict):
                value = dataIdDict[dimensionName]
                try:
                    dimension = self.dimensions.dimensions[dimensionName]
                except KeyError:
                    # This is not a real dimension
                    not_dimensions[dimensionName] = value
                    del dataIdDict[dimensionName]
                    continue

                # Convert an integral type to an explicit int to simplify
                # comparisons here
                if isinstance(value, numbers.Integral):
                    value = int(value)

                if not isinstance(value, dimension.primaryKey.getPythonType()):
                    for alternate in dimension.alternateKeys:
                        if isinstance(value, alternate.getPythonType()):
                            byRecord[dimensionName][alternate.name] = value
                            del dataIdDict[dimensionName]
                            _LOG.debug(
                                "Converting dimension %s to %s.%s=%s",
                                dimensionName,
                                dimensionName,
                                alternate.name,
                                value,
                            )
                            break
                    else:
                        _LOG.warning(
                            "Type mismatch found for value '%r' provided for dimension %s. "
                            "Could not find matching alternative (primary key has type %s) "
                            "so attempting to use as-is.",
                            value,
                            dimensionName,
                            dimension.primaryKey.getPythonType(),
                        )

        # By this point kwargs and newDataId should only include valid
        # dimensions. Merge kwargs in to the new dataId and log if there
        # are dimensions in both (rather than calling update).
        for k, v in kwargs.items():
            if k in newDataId and newDataId[k] != v:
                _LOG.debug(
                    "Keyword arg %s overriding explicit value in dataId of %s with %s", k, newDataId[k], v
                )
            newDataId[k] = v
        # No need to retain any values in kwargs now.
        kwargs = {}

        # If we have some unrecognized dimensions we have to try to connect
        # them to records in other dimensions.  This is made more complicated
        # by some dimensions having records with clashing names.  A mitigation
        # is that we can tell by this point which dimensions are missing
        # for the DatasetType but this does not work for calibrations
        # where additional dimensions can be used to constrain the temporal
        # axis.
        if not_dimensions:
            # Search for all dimensions even if we have been given a value
            # explicitly. In some cases records are given as well as the
            # actually dimension and this should not be an error if they
            # match.
            mandatoryDimensions = datasetType.dimensions.names  # - provided

            candidateDimensions: set[str] = set()
            candidateDimensions.update(mandatoryDimensions)

            # For calibrations we may well be needing temporal dimensions
            # so rather than always including all dimensions in the scan
            # restrict things a little. It is still possible for there
            # to be confusion over day_obs in visit vs exposure for example.
            # If we are not searching calibration collections things may
            # fail but they are going to fail anyway because of the
            # ambiguousness of the dataId...
            if datasetType.isCalibration():
                for dim in self.dimensions.dimensions:
                    if dim.temporal:
                        candidateDimensions.add(str(dim))

            # Look up table for the first association with a dimension
            guessedAssociation: dict[str, dict[str, Any]] = defaultdict(dict)

            # Keep track of whether an item is associated with multiple
            # dimensions.
            counter: Counter[str] = Counter()
            assigned: dict[str, set[str]] = defaultdict(set)

            # Go through the missing dimensions and associate the
            # given names with records within those dimensions
            matched_dims = set()
            for dimensionName in candidateDimensions:
                dimension = self.dimensions.dimensions[dimensionName]
                fields = dimension.metadata.names | dimension.uniqueKeys.names
                for field in not_dimensions:
                    if field in fields:
                        guessedAssociation[dimensionName][field] = not_dimensions[field]
                        counter[dimensionName] += 1
                        assigned[field].add(dimensionName)
                        matched_dims.add(field)

            # Calculate the fields that matched nothing.
            never_found = set(not_dimensions) - matched_dims

            if never_found:
                raise DimensionValueError(f"Unrecognized keyword args given: {never_found}")

            # There is a chance we have allocated a single dataId item
            # to multiple dimensions. Need to decide which should be retained.
            # For now assume that the most popular alternative wins.
            # This means that day_obs with seq_num will result in
            # exposure.day_obs and not visit.day_obs
            # Also prefer an explicitly missing dimension over an inferred
            # temporal dimension.
            for fieldName, assignedDimensions in assigned.items():
                if len(assignedDimensions) > 1:
                    # Pick the most popular (preferring mandatory dimensions)
                    requiredButMissing = assignedDimensions.intersection(mandatoryDimensions)
                    if requiredButMissing:
                        candidateDimensions = requiredButMissing
                    else:
                        candidateDimensions = assignedDimensions

                        # If this is a choice between visit and exposure and
                        # neither was a required part of the dataset type,
                        # (hence in this branch) always prefer exposure over
                        # visit since exposures are always defined and visits
                        # are defined from exposures.
                        if candidateDimensions == {"exposure", "visit"}:
                            candidateDimensions = {"exposure"}

                    # Select the relevant items and get a new restricted
                    # counter.
                    theseCounts = {k: v for k, v in counter.items() if k in candidateDimensions}
                    duplicatesCounter: Counter[str] = Counter()
                    duplicatesCounter.update(theseCounts)

                    # Choose the most common. If they are equally common
                    # we will pick the one that was found first.
                    # Returns a list of tuples
                    selected = duplicatesCounter.most_common(1)[0][0]

                    _LOG.debug(
                        "Ambiguous dataId entry '%s' associated with multiple dimensions: %s."
                        " Removed ambiguity by choosing dimension %s.",
                        fieldName,
                        ", ".join(assignedDimensions),
                        selected,
                    )

                    for candidateDimension in assignedDimensions:
                        if candidateDimension != selected:
                            del guessedAssociation[candidateDimension][fieldName]

            # Update the record look up dict with the new associations
            for dimensionName, values in guessedAssociation.items():
                if values:  # A dict might now be empty
                    _LOG.debug(
                        "Assigned non-dimension dataId keys to dimension %s: %s", dimensionName, values
                    )
                    byRecord[dimensionName].update(values)

        if byRecord:
            # Some record specifiers were found so we need to convert
            # them to the Id form
            for dimensionName, values in byRecord.items():
                if dimensionName in newDataId:
                    _LOG.debug(
                        "DataId specified explicit %s dimension value of %s in addition to"
                        " general record specifiers for it of %s. Checking for self-consistency.",
                        dimensionName,
                        newDataId[dimensionName],
                        str(values),
                    )
                    # Get the actual record and compare with these values.
                    # Only query with relevant data ID values.
                    filtered_data_id = {
                        k: v for k, v in newDataId.items() if k in self.dimensions[dimensionName].required
                    }
                    try:
                        recs = self.query_dimension_records(
                            dimensionName,
                            data_id=filtered_data_id,
                        )
                    except (DataIdError, EmptyQueryResultError):
                        raise DimensionValueError(
                            f"Could not find dimension '{dimensionName}'"
                            f" with dataId {filtered_data_id} as part of comparing with"
                            f" record values {byRecord[dimensionName]}"
                        ) from None
                    if len(recs) == 1:
                        errmsg: list[str] = []
                        for k, v in values.items():
                            if (recval := getattr(recs[0], k)) != v:
                                errmsg.append(f"{k} ({recval} != {v})")
                        if errmsg:
                            raise DimensionValueError(
                                f"Dimension {dimensionName} in dataId has explicit value"
                                f" {newDataId[dimensionName]} inconsistent with"
                                f" {dimensionName} dimension record: " + ", ".join(errmsg)
                            )
                    else:
                        # Multiple matches for an explicit dimension
                        # should never happen but let downstream complain.
                        pass
                    continue

                # Do not use data ID keys in query that aren't relevant.
                # Otherwise we can have detector queries being constrained
                # by an exposure ID that doesn't exist and return no matches
                # for a detector even though it's a good detector name.
                filtered_data_id = {
                    k: v
                    for k, v in newDataId.items()
                    if k in self.dimensions[dimensionName].minimal_group.names
                }

                def _get_attr(obj: Any, attr: str) -> Any:
                    # Used to implement x.exposure.seq_num when given
                    # x and "exposure.seq_num".
                    for component in attr.split("."):
                        obj = getattr(obj, component)
                    return obj

                with self.query() as q:
                    x = q.expression_factory
                    # Build up a WHERE expression.
                    predicates = tuple(_get_attr(x, f"{dimensionName}.{k}") == v for k, v in values.items())
                    extra_args: dict[str, Any] = {}  # For mypy.
                    extra_args.update(filtered_data_id)
                    extra_args.update(kwargs)
                    q = q.where(x.all(*predicates), **extra_args)
                    records = set(q.dimension_records(dimensionName))

                if len(records) != 1:
                    if len(records) > 1:
                        # visit can have an ambiguous answer without involving
                        # visit_system. The default visit_system is defined
                        # by the instrument.
                        if (
                            dimensionName == "visit"
                            and "visit_system_membership" in self.dimensions
                            and "visit_system" in self.dimensions["instrument"].metadata
                        ):
                            instrument_records = self.query_dimension_records(
                                "instrument",
                                data_id=newDataId,
                                explain=False,
                                **kwargs,
                            )
                            if len(instrument_records) == 1:
                                visit_system = instrument_records[0].visit_system
                                if visit_system is None:
                                    # Set to a value that will never match.
                                    visit_system = -1

                                # Look up each visit in the
                                # visit_system_membership records.
                                for rec in records:
                                    membership = self.query_dimension_records(
                                        # Use bind to allow zero results.
                                        # This is a fully-specified query.
                                        "visit_system_membership",
                                        instrument=instrument_records[0].name,
                                        visit_system=visit_system,
                                        visit=rec.id,
                                        explain=False,
                                    )
                                    if membership:
                                        # This record is the right answer.
                                        records = {rec}
                                        break

                        # The ambiguity may have been resolved so check again.
                        if len(records) > 1:
                            _LOG.debug(
                                "Received %d records from constraints of %s", len(records), str(values)
                            )
                            for r in records:
                                _LOG.debug("- %s", str(r))
                            raise DimensionValueError(
                                f"DataId specification for dimension {dimensionName} is not"
                                f" uniquely constrained to a single dataset by {values}."
                                f" Got {len(records)} results."
                            )
                    else:
                        raise DimensionValueError(
                            f"DataId specification for dimension {dimensionName} matched no"
                            f" records when constrained by {values}"
                        )

                # Get the primary key from the real dimension object
                dimension = self.dimensions.dimensions[dimensionName]
                if not isinstance(dimension, Dimension):
                    raise RuntimeError(
                        f"{dimension.name} is not a true dimension, and cannot be used in data IDs."
                    )
                newDataId[dimensionName] = getattr(records.pop(), dimension.primaryKey.name)

        return newDataId, kwargs

    def _findDatasetRef(
        self,
        datasetRefOrType: DatasetRef | DatasetType | str,
        dataId: DataId | None = None,
        *,
        collections: Any = None,
        predict: bool = False,
        run: str | None = None,
        datastore_records: bool = False,
        timespan: Timespan | None = None,
        **kwargs: Any,
    ) -> DatasetRef:
        """Shared logic for methods that start with a search for a dataset in
        the registry.

        Parameters
        ----------
        datasetRefOrType : `DatasetRef`, `DatasetType`, or `str`
            When `DatasetRef` the `dataId` should be `None`.
            Otherwise the `DatasetType` or name thereof.
        dataId : `dict` or `DataCoordinate`, optional
            A `dict` of `Dimension` link name, value pairs that label the
            `DatasetRef` within a Collection. When `None`, a `DatasetRef`
            should be provided as the first argument.
        collections : Any, optional
            Collections to be searched, overriding ``self.collections``.
            Can be any of the types supported by the ``collections`` argument
            to butler construction.
        predict : `bool`, optional
            If `True`, return a newly created `DatasetRef` with a unique
            dataset ID if finding a reference in the `Registry` fails.
            Defaults to `False`.
        run : `str`, optional
            Run collection name to use for creating `DatasetRef` for predicted
            datasets. Only used if ``predict`` is `True`.
        datastore_records : `bool`, optional
            If `True` add datastore records to returned `DatasetRef`.
        timespan : `Timespan` or `None`, optional
            A timespan that the validity range of the dataset must overlap.
            If not provided and this is a calibration dataset type, an attempt
            will be made to find the timespan from any temporal coordinate
            in the data ID.
        **kwargs
            Additional keyword arguments used to augment or construct a
            `DataId`.  See `DataId` parameters.

        Returns
        -------
        ref : `DatasetRef`
            A reference to the dataset identified by the given arguments.
            This can be the same dataset reference as given if it was
            resolved.

        Raises
        ------
        LookupError
            Raised if no matching dataset exists in the `Registry` (and
            ``predict`` is `False`).
        ValueError
            Raised if a resolved `DatasetRef` was passed as an input, but it
            differs from the one found in the registry.
        TypeError
            Raised if no collections were provided.
        """
        datasetType, dataId = self._standardizeArgs(datasetRefOrType, dataId, for_put=False, **kwargs)
        if isinstance(datasetRefOrType, DatasetRef):
            if collections is not None:
                warnings.warn("Collections should not be specified with DatasetRef", stacklevel=3)
            # May need to retrieve datastore records if requested.
            if datastore_records and datasetRefOrType._datastore_records is None:
                datasetRefOrType = self._registry.get_datastore_records(datasetRefOrType)
            return datasetRefOrType

        dataId, kwargs = self._rewrite_data_id(dataId, datasetType, **kwargs)

        if datasetType.isCalibration():
            # Because this is a calibration dataset, first try to make a
            # standardize the data ID without restricting the dimensions to
            # those of the dataset type requested, because there may be extra
            # dimensions that provide temporal information for a validity-range
            # lookup.
            dataId = DataCoordinate.standardize(
                dataId, universe=self.dimensions, defaults=self._registry.defaults.dataId, **kwargs
            )
            if timespan is None:
                if dataId.dimensions.temporal:
                    dataId = self._registry.expandDataId(dataId)
                    # Use the timespan from the data ID to constrain the
                    # calibration lookup, but only if the caller has not
                    # specified an explicit timespan.
                    timespan = dataId.timespan
                else:
                    # Try an arbitrary timespan. Downstream will fail if this
                    # results in more than one matching dataset.
                    timespan = Timespan(None, None)
        else:
            # Standardize the data ID to just the dimensions of the dataset
            # type instead of letting registry.findDataset do it, so we get the
            # result even if no dataset is found.
            dataId = DataCoordinate.standardize(
                dataId,
                dimensions=datasetType.dimensions,
                defaults=self._registry.defaults.dataId,
                **kwargs,
            )
        # Always lookup the DatasetRef, even if one is given, to ensure it is
        # present in the current collection.
        ref = self.find_dataset(
            datasetType,
            dataId,
            collections=collections,
            timespan=timespan,
            datastore_records=datastore_records,
        )
        if ref is None:
            if predict:
                if run is None:
                    run = self.run
                    if run is None:
                        raise TypeError("Cannot predict dataset ID/location with run=None.")
                return DatasetRef(datasetType, dataId, run=run)
            else:
                if collections is None:
                    collections = self._registry.defaults.collections
                raise DatasetNotFoundError(
                    f"Dataset {datasetType.name} with data ID {dataId} "
                    f"could not be found in collections {collections}."
                )
        if datasetType != ref.datasetType:
            # If they differ it is because the user explicitly specified
            # a compatible dataset type to this call rather than using the
            # registry definition. The DatasetRef must therefore be recreated
            # using the user definition such that the expected type is
            # returned.
            ref = DatasetRef(
                datasetType, ref.dataId, run=ref.run, id=ref.id, datastore_records=ref._datastore_records
            )

        return ref

    @transactional
    def put(
        self,
        obj: Any,
        datasetRefOrType: DatasetRef | DatasetType | str,
        /,
        dataId: DataId | None = None,
        *,
        run: str | None = None,
        **kwargs: Any,
    ) -> DatasetRef:
        """Store and register a dataset.

        Parameters
        ----------
        obj : `object`
            The dataset.
        datasetRefOrType : `DatasetRef`, `DatasetType`, or `str`
            When `DatasetRef` is provided, ``dataId`` should be `None`.
            Otherwise the `DatasetType` or name thereof. If a fully resolved
            `DatasetRef` is given the run and ID are used directly.
        dataId : `dict` or `DataCoordinate`
            A `dict` of `Dimension` link name, value pairs that label the
            `DatasetRef` within a Collection. When `None`, a `DatasetRef`
            should be provided as the second argument.
        run : `str`, optional
            The name of the run the dataset should be added to, overriding
            ``self.run``. Not used if a resolved `DatasetRef` is provided.
        **kwargs
            Additional keyword arguments used to augment or construct a
            `DataCoordinate`.  See `DataCoordinate.standardize`
            parameters. Not used if a resolve `DatasetRef` is provided.

        Returns
        -------
        ref : `DatasetRef`
            A reference to the stored dataset, updated with the correct id if
            given.

        Raises
        ------
        TypeError
            Raised if the butler is read-only or if no run has been provided.
        """
        if isinstance(datasetRefOrType, DatasetRef):
            # This is a direct put of predefined DatasetRef.
            _LOG.debug("Butler put direct: %s", datasetRefOrType)
            if run is not None:
                warnings.warn("Run collection is not used for DatasetRef", stacklevel=3)
            # If registry already has a dataset with the same dataset ID,
            # dataset type and DataId, then _importDatasets will do nothing and
            # just return an original ref. We have to raise in this case, there
            # is a datastore check below for that.
            self._registry._importDatasets([datasetRefOrType], expand=True)
            # Before trying to write to the datastore check that it does not
            # know this dataset. This is prone to races, of course.
            if self._datastore.knows(datasetRefOrType):
                raise ConflictingDefinitionError(f"Datastore already contains dataset: {datasetRefOrType}")
            # Try to write dataset to the datastore, if it fails due to a race
            # with another write, the content of stored data may be
            # unpredictable.
            try:
                self._datastore.put(obj, datasetRefOrType)
            except IntegrityError as e:
                raise ConflictingDefinitionError(f"Datastore already contains dataset: {e}") from e
            return datasetRefOrType

        _LOG.debug("Butler put: %s, dataId=%s, run=%s", datasetRefOrType, dataId, run)
        if not self.isWriteable():
            raise TypeError("Butler is read-only.")
        datasetType, dataId = self._standardizeArgs(datasetRefOrType, dataId, **kwargs)

        # Handle dimension records in dataId
        dataId, kwargs = self._rewrite_data_id(dataId, datasetType, **kwargs)

        # Add Registry Dataset entry.
        dataId = self._registry.expandDataId(dataId, dimensions=datasetType.dimensions, **kwargs)
        (ref,) = self._registry.insertDatasets(datasetType, run=run, dataIds=[dataId])
        self._datastore.put(obj, ref)

        return ref

    def getDeferred(
        self,
        datasetRefOrType: DatasetRef | DatasetType | str,
        /,
        dataId: DataId | None = None,
        *,
        parameters: dict | None = None,
        collections: Any = None,
        storageClass: str | StorageClass | None = None,
        timespan: Timespan | None = None,
        **kwargs: Any,
    ) -> DeferredDatasetHandle:
        """Create a `DeferredDatasetHandle` which can later retrieve a dataset,
        after an immediate registry lookup.

        Parameters
        ----------
        datasetRefOrType : `DatasetRef`, `DatasetType`, or `str`
            When `DatasetRef` the `dataId` should be `None`.
            Otherwise the `DatasetType` or name thereof.
        dataId : `dict` or `DataCoordinate`, optional
            A `dict` of `Dimension` link name, value pairs that label the
            `DatasetRef` within a Collection. When `None`, a `DatasetRef`
            should be provided as the first argument.
        parameters : `dict`
            Additional StorageClass-defined options to control reading,
            typically used to efficiently read only a subset of the dataset.
        collections : Any, optional
            Collections to be searched, overriding ``self.collections``.
            Can be any of the types supported by the ``collections`` argument
            to butler construction.
        storageClass : `StorageClass` or `str`, optional
            The storage class to be used to override the Python type
            returned by this method. By default the returned type matches
            the dataset type definition for this dataset. Specifying a
            read `StorageClass` can force a different type to be returned.
            This type must be compatible with the original type.
        timespan : `Timespan` or `None`, optional
            A timespan that the validity range of the dataset must overlap.
            If not provided and this is a calibration dataset type, an attempt
            will be made to find the timespan from any temporal coordinate
            in the data ID.
        **kwargs
            Additional keyword arguments used to augment or construct a
            `DataId`.  See `DataId` parameters.

        Returns
        -------
        obj : `DeferredDatasetHandle`
            A handle which can be used to retrieve a dataset at a later time.

        Raises
        ------
        LookupError
            Raised if no matching dataset exists in the `Registry` or
            datastore.
        ValueError
            Raised if a resolved `DatasetRef` was passed as an input, but it
            differs from the one found in the registry.
        TypeError
            Raised if no collections were provided.
        """
        if isinstance(datasetRefOrType, DatasetRef):
            # Do the quick check first and if that fails, check for artifact
            # existence. This is necessary for datastores that are configured
            # in trust mode where there won't be a record but there will be
            # a file.
            if self._datastore.knows(datasetRefOrType) or self._datastore.exists(datasetRefOrType):
                ref = datasetRefOrType
            else:
                raise LookupError(f"Dataset reference {datasetRefOrType} does not exist.")
        else:
            ref = self._findDatasetRef(
                datasetRefOrType, dataId, collections=collections, timespan=timespan, **kwargs
            )
        return DeferredDatasetHandle(butler=self, ref=ref, parameters=parameters, storageClass=storageClass)

    def get(
        self,
        datasetRefOrType: DatasetRef | DatasetType | str,
        /,
        dataId: DataId | None = None,
        *,
        parameters: dict[str, Any] | None = None,
        collections: Any = None,
        storageClass: StorageClass | str | None = None,
        timespan: Timespan | None = None,
        **kwargs: Any,
    ) -> Any:
        """Retrieve a stored dataset.

        Parameters
        ----------
        datasetRefOrType : `DatasetRef`, `DatasetType`, or `str`
            When `DatasetRef` the `dataId` should be `None`.
            Otherwise the `DatasetType` or name thereof.
            If a resolved `DatasetRef`, the associated dataset
            is returned directly without additional querying.
        dataId : `dict` or `DataCoordinate`
            A `dict` of `Dimension` link name, value pairs that label the
            `DatasetRef` within a Collection. When `None`, a `DatasetRef`
            should be provided as the first argument.
        parameters : `dict`
            Additional StorageClass-defined options to control reading,
            typically used to efficiently read only a subset of the dataset.
        collections : Any, optional
            Collections to be searched, overriding ``self.collections``.
            Can be any of the types supported by the ``collections`` argument
            to butler construction.
        storageClass : `StorageClass` or `str`, optional
            The storage class to be used to override the Python type
            returned by this method. By default the returned type matches
            the dataset type definition for this dataset. Specifying a
            read `StorageClass` can force a different type to be returned.
            This type must be compatible with the original type.
        timespan : `Timespan` or `None`, optional
            A timespan that the validity range of the dataset must overlap.
            If not provided and this is a calibration dataset type, an attempt
            will be made to find the timespan from any temporal coordinate
            in the data ID.
        **kwargs
            Additional keyword arguments used to augment or construct a
            `DataCoordinate`.  See `DataCoordinate.standardize`
            parameters.

        Returns
        -------
        obj : `object`
            The dataset.

        Raises
        ------
        LookupError
            Raised if no matching dataset exists in the `Registry`.
        TypeError
            Raised if no collections were provided.

        Notes
        -----
        When looking up datasets in a `~CollectionType.CALIBRATION` collection,
        this method requires that the given data ID include temporal dimensions
        beyond the dimensions of the dataset type itself, in order to find the
        dataset with the appropriate validity range.  For example, a "bias"
        dataset with native dimensions ``{instrument, detector}`` could be
        fetched with a ``{instrument, detector, exposure}`` data ID, because
        ``exposure`` is a temporal dimension.
        """
        _LOG.debug("Butler get: %s, dataId=%s, parameters=%s", datasetRefOrType, dataId, parameters)
        ref = self._findDatasetRef(
            datasetRefOrType,
            dataId,
            collections=collections,
            datastore_records=True,
            timespan=timespan,
            **kwargs,
        )
        return self._datastore.get(ref, parameters=parameters, storageClass=storageClass)

    def getURIs(
        self,
        datasetRefOrType: DatasetRef | DatasetType | str,
        /,
        dataId: DataId | None = None,
        *,
        predict: bool = False,
        collections: Any = None,
        run: str | None = None,
        **kwargs: Any,
    ) -> DatasetRefURIs:
        """Return the URIs associated with the dataset.

        Parameters
        ----------
        datasetRefOrType : `DatasetRef`, `DatasetType`, or `str`
            When `DatasetRef` the `dataId` should be `None`.
            Otherwise the `DatasetType` or name thereof.
        dataId : `dict` or `DataCoordinate`
            A `dict` of `Dimension` link name, value pairs that label the
            `DatasetRef` within a Collection. When `None`, a `DatasetRef`
            should be provided as the first argument.
        predict : `bool`
            If `True`, allow URIs to be returned of datasets that have not
            been written.
        collections : Any, optional
            Collections to be searched, overriding ``self.collections``.
            Can be any of the types supported by the ``collections`` argument
            to butler construction.
        run : `str`, optional
            Run to use for predictions, overriding ``self.run``.
        **kwargs
            Additional keyword arguments used to augment or construct a
            `DataCoordinate`.  See `DataCoordinate.standardize`
            parameters.

        Returns
        -------
        uris : `DatasetRefURIs`
            The URI to the primary artifact associated with this dataset (if
            the dataset was disassembled within the datastore this may be
            `None`), and the URIs to any components associated with the dataset
            artifact. (can be empty if there are no components).
        """
        ref = self._findDatasetRef(
            datasetRefOrType, dataId, predict=predict, run=run, collections=collections, **kwargs
        )
        return self._datastore.getURIs(ref, predict)

    def get_dataset_type(self, name: str) -> DatasetType:
        return self._registry.getDatasetType(name)

    def get_dataset(
        self,
        id: DatasetId,
        *,
        storage_class: str | StorageClass | None = None,
        dimension_records: bool = False,
        datastore_records: bool = False,
    ) -> DatasetRef | None:
        ref = self._registry.getDataset(id)
        if ref is not None:
            if dimension_records:
                ref = ref.expanded(
                    self._registry.expandDataId(ref.dataId, dimensions=ref.datasetType.dimensions)
                )
            if storage_class:
                ref = ref.overrideStorageClass(storage_class)
            if datastore_records:
                ref = self._registry.get_datastore_records(ref)
        return ref

    def find_dataset(
        self,
        dataset_type: DatasetType | str,
        data_id: DataId | None = None,
        *,
        collections: str | Sequence[str] | None = None,
        timespan: Timespan | None = None,
        storage_class: str | StorageClass | None = None,
        dimension_records: bool = False,
        datastore_records: bool = False,
        **kwargs: Any,
    ) -> DatasetRef | None:
        # Handle any parts of the dataID that are not using primary dimension
        # keys.
        if isinstance(dataset_type, str):
            actual_type = self.get_dataset_type(dataset_type)
        else:
            actual_type = dataset_type

        # Store the component for later.
        component_name = actual_type.component()
        if actual_type.isComponent():
            parent_type = actual_type.makeCompositeDatasetType()
        else:
            parent_type = actual_type

        data_id, kwargs = self._rewrite_data_id(data_id, parent_type, **kwargs)

        ref = self._registry.findDataset(
            parent_type,
            data_id,
            collections=collections,
            timespan=timespan,
            datastore_records=datastore_records,
            **kwargs,
        )
        if ref is not None and dimension_records:
            ref = ref.expanded(self._registry.expandDataId(ref.dataId, dimensions=ref.datasetType.dimensions))
        if ref is not None and component_name:
            ref = ref.makeComponentRef(component_name)
        if ref is not None and storage_class is not None:
            ref = ref.overrideStorageClass(storage_class)

        return ref

    def retrieveArtifacts(
        self,
        refs: Iterable[DatasetRef],
        destination: ResourcePathExpression,
        transfer: str = "auto",
        preserve_path: bool = True,
        overwrite: bool = False,
    ) -> list[ResourcePath]:
        # Docstring inherited.
        return self._datastore.retrieveArtifacts(
            refs,
            ResourcePath(destination),
            transfer=transfer,
            preserve_path=preserve_path,
            overwrite=overwrite,
        )

    def exists(
        self,
        dataset_ref_or_type: DatasetRef | DatasetType | str,
        /,
        data_id: DataId | None = None,
        *,
        full_check: bool = True,
        collections: Any = None,
        **kwargs: Any,
    ) -> DatasetExistence:
        # Docstring inherited.
        existence = DatasetExistence.UNRECOGNIZED

        if isinstance(dataset_ref_or_type, DatasetRef):
            if collections is not None:
                warnings.warn("Collections should not be specified with DatasetRef", stacklevel=2)
            if data_id is not None:
                warnings.warn("A DataID should not be specified with DatasetRef", stacklevel=2)
            ref = dataset_ref_or_type
            registry_ref = self._registry.getDataset(dataset_ref_or_type.id)
            if registry_ref is not None:
                existence |= DatasetExistence.RECORDED

                if dataset_ref_or_type != registry_ref:
                    # This could mean that storage classes differ, so we should
                    # check for that but use the registry ref for the rest of
                    # the method.
                    if registry_ref.is_compatible_with(dataset_ref_or_type):
                        # Use the registry version from now on.
                        ref = registry_ref
                    else:
                        raise ValueError(
                            f"The ref given to exists() ({ref}) has the same dataset ID as one "
                            f"in registry but has different incompatible values ({registry_ref})."
                        )
        else:
            try:
                ref = self._findDatasetRef(dataset_ref_or_type, data_id, collections=collections, **kwargs)
            except (LookupError, TypeError):
                return existence
            existence |= DatasetExistence.RECORDED

        if self._datastore.knows(ref):
            existence |= DatasetExistence.DATASTORE

        if full_check:
            if self._datastore.exists(ref):
                existence |= DatasetExistence._ARTIFACT
        elif existence.value != DatasetExistence.UNRECOGNIZED.value:
            # Do not add this flag if we have no other idea about a dataset.
            existence |= DatasetExistence(DatasetExistence._ASSUMED)

        return existence

    def _exists_many(
        self,
        refs: Iterable[DatasetRef],
        /,
        *,
        full_check: bool = True,
    ) -> dict[DatasetRef, DatasetExistence]:
        # Docstring inherited.
        existence = {ref: DatasetExistence.UNRECOGNIZED for ref in refs}

        # Registry does not have a bulk API to check for a ref.
        for ref in refs:
            registry_ref = self._registry.getDataset(ref.id)
            if registry_ref is not None:
                # It is possible, albeit unlikely, that the given ref does
                # not match the one in registry even though the UUID matches.
                # When checking a single ref we raise, but it's impolite to
                # do that when potentially hundreds of refs are being checked.
                # We could change the API to only accept UUIDs and that would
                # remove the ability to even check and remove the worry
                # about differing storage classes. Given the ongoing discussion
                # on refs vs UUIDs and whether to raise or have a new
                # private flag, treat this as a private API for now.
                existence[ref] |= DatasetExistence.RECORDED

        # Ask datastore if it knows about these refs.
        knows = self._datastore.knows_these(refs)
        for ref, known in knows.items():
            if known:
                existence[ref] |= DatasetExistence.DATASTORE

        if full_check:
            mexists = self._datastore.mexists(refs)
            for ref, exists in mexists.items():
                if exists:
                    existence[ref] |= DatasetExistence._ARTIFACT
        else:
            # Do not set this flag if nothing is known about the dataset.
            for ref in existence:
                if existence[ref] != DatasetExistence.UNRECOGNIZED:
                    existence[ref] |= DatasetExistence._ASSUMED

        return existence

    def removeRuns(self, names: Iterable[str], unstore: bool = True) -> None:
        # Docstring inherited.
        if not self.isWriteable():
            raise TypeError("Butler is read-only.")
        names = list(names)
        refs: list[DatasetRef] = []
        all_dataset_types = [dt.name for dt in self._registry.queryDatasetTypes(...)]
        for name in names:
            collectionType = self._registry.getCollectionType(name)
            if collectionType is not CollectionType.RUN:
                raise TypeError(f"The collection type of '{name}' is {collectionType.name}, not RUN.")
            with self.query() as query:
                # Work out the dataset types that are relevant.
                collections_info = self.collections.query_info(name, include_summary=True)
                filtered_dataset_types = self.collections._filter_dataset_types(
                    all_dataset_types, collections_info
                )
                for dt in filtered_dataset_types:
                    refs.extend(query.datasets(dt, collections=name))
        with self._datastore.transaction(), self._registry.transaction():
            if unstore:
                self._datastore.trash(refs)
            else:
                self._datastore.forget(refs)
            for name in names:
                self._registry.removeCollection(name)
        if unstore:
            # Point of no return for removing artifacts
            self._datastore.emptyTrash()

    def pruneDatasets(
        self,
        refs: Iterable[DatasetRef],
        *,
        disassociate: bool = True,
        unstore: bool = False,
        tags: Iterable[str] = (),
        purge: bool = False,
    ) -> None:
        # docstring inherited from LimitedButler

        if not self.isWriteable():
            raise TypeError("Butler is read-only.")
        if purge:
            if not disassociate:
                raise TypeError("Cannot pass purge=True without disassociate=True.")
            if not unstore:
                raise TypeError("Cannot pass purge=True without unstore=True.")
        elif disassociate:
            tags = tuple(tags)
            if not tags:
                raise TypeError("No tags provided but disassociate=True.")
            for tag in tags:
                collectionType = self._registry.getCollectionType(tag)
                if collectionType is not CollectionType.TAGGED:
                    raise TypeError(
                        f"Cannot disassociate from collection '{tag}' "
                        f"of non-TAGGED type {collectionType.name}."
                    )
        # Transform possibly-single-pass iterable into something we can iterate
        # over multiple times.
        refs = list(refs)
        # Pruning a component of a DatasetRef makes no sense since registry
        # doesn't know about components and datastore might not store
        # components in a separate file
        for ref in refs:
            if ref.datasetType.component():
                raise ValueError(f"Can not prune a component of a dataset (ref={ref})")
        # We don't need an unreliable Datastore transaction for this, because
        # we've been extra careful to ensure that Datastore.trash only involves
        # mutating the Registry (it can _look_ at Datastore-specific things,
        # but shouldn't change them), and hence all operations here are
        # Registry operations.
        with self._datastore.transaction(), self._registry.transaction():
            if unstore:
                self._datastore.trash(refs)
            if purge:
                self._registry.removeDatasets(refs)
            elif disassociate:
                assert tags, "Guaranteed by earlier logic in this function."
                for tag in tags:
                    self._registry.disassociate(tag, refs)
        # We've exited the Registry transaction, and apparently committed.
        # (if there was an exception, everything rolled back, and it's as if
        # nothing happened - and we never get here).
        # Datastore artifacts are not yet gone, but they're clearly marked
        # as trash, so if we fail to delete now because of (e.g.) filesystem
        # problems we can try again later, and if manual administrative
        # intervention is required, it's pretty clear what that should entail:
        # deleting everything on disk and in private Datastore tables that is
        # in the dataset_location_trash table.
        if unstore:
            # Point of no return for removing artifacts
            self._datastore.emptyTrash()

    @transactional
    def ingest(
        self,
        *datasets: FileDataset,
        transfer: str | None = "auto",
        record_validation_info: bool = True,
    ) -> None:
        # Docstring inherited.
        if not self.isWriteable():
            raise TypeError("Butler is read-only.")

        _LOG.verbose("Ingesting %d file dataset%s.", len(datasets), "" if len(datasets) == 1 else "s")
        if not datasets:
            return

        progress = Progress("lsst.daf.butler.Butler.ingest", level=logging.DEBUG)

        # We need to reorganize all the inputs so that they are grouped
        # by dataset type and run. Multiple refs in a single FileDataset
        # are required to share the run and dataset type.
        groupedData: MutableMapping[tuple[DatasetType, str], list[FileDataset]] = defaultdict(list)

        # Track DataIDs that are being ingested so we can spot issues early
        # with duplication. Retain previous FileDataset so we can report it.
        groupedDataIds: MutableMapping[tuple[DatasetType, str], dict[DataCoordinate, FileDataset]] = (
            defaultdict(dict)
        )

        # And the nested loop that populates it:
        for dataset in progress.wrap(datasets, desc="Grouping by dataset type"):
            # Somewhere to store pre-existing refs if we have an
            # execution butler.
            existingRefs: list[DatasetRef] = []

            for ref in dataset.refs:
                group_key = (ref.datasetType, ref.run)

                if ref.dataId in groupedDataIds[group_key]:
                    raise ConflictingDefinitionError(
                        f"Ingest conflict. Dataset {dataset.path} has same"
                        " DataId as other ingest dataset"
                        f" {groupedDataIds[group_key][ref.dataId].path} "
                        f" ({ref.dataId})"
                    )

                groupedDataIds[group_key][ref.dataId] = dataset

            if existingRefs:
                if len(dataset.refs) != len(existingRefs):
                    # Keeping track of partially pre-existing datasets is hard
                    # and should generally never happen. For now don't allow
                    # it.
                    raise ConflictingDefinitionError(
                        f"For dataset {dataset.path} some dataIds already exist"
                        " in registry but others do not. This is not supported."
                    )

                # Store expanded form in the original FileDataset.
                dataset.refs = existingRefs
            else:
                groupedData[group_key].append(dataset)

        # Now we can bulk-insert into Registry for each DatasetType.
        for (datasetType, this_run), grouped_datasets in progress.iter_item_chunks(
            groupedData.items(), desc="Bulk-inserting datasets by type"
        ):
            refs_to_import = []
            for dataset in grouped_datasets:
                refs_to_import.extend(dataset.refs)

            n_refs = len(refs_to_import)
            _LOG.verbose(
                "Importing %d ref%s of dataset type %r into run %r",
                n_refs,
                "" if n_refs == 1 else "s",
                datasetType.name,
                this_run,
            )

            # Import the refs and expand the DataCoordinates since we can't
            # guarantee that they are expanded and Datastore will need
            # the records.
            imported_refs = self._registry._importDatasets(refs_to_import, expand=True)
            assert set(imported_refs) == set(refs_to_import)

            # Replace all the refs in the FileDataset with expanded versions.
            # Pull them off in the order we put them on the list.
            for dataset in grouped_datasets:
                n_dataset_refs = len(dataset.refs)
                dataset.refs = imported_refs[:n_dataset_refs]
                del imported_refs[:n_dataset_refs]

        # Bulk-insert everything into Datastore.
        # We do not know if any of the registry entries already existed
        # (_importDatasets only complains if they exist but differ) so
        # we have to catch IntegrityError explicitly.
        try:
            self._datastore.ingest(
                *datasets, transfer=transfer, record_validation_info=record_validation_info
            )
        except IntegrityError as e:
            raise ConflictingDefinitionError(f"Datastore already contains one or more datasets: {e}") from e

    @contextlib.contextmanager
    def export(
        self,
        *,
        directory: str | None = None,
        filename: str | None = None,
        format: str | None = None,
        transfer: str | None = None,
    ) -> Iterator[RepoExportContext]:
        # Docstring inherited.
        if directory is None and transfer is not None:
            raise TypeError("Cannot transfer without providing a directory.")
        if transfer == "move":
            raise TypeError("Transfer may not be 'move': export is read-only")
        if format is None:
            if filename is None:
                raise TypeError("At least one of 'filename' or 'format' must be provided.")
            else:
                _, format = os.path.splitext(filename)
                if not format:
                    raise ValueError("Please specify a file extension to determine export format.")
                format = format[1:]  # Strip leading ".""
        elif filename is None:
            filename = f"export.{format}"
        if directory is not None:
            filename = os.path.join(directory, filename)
        formats = self._config["repo_transfer_formats"]
        if format not in formats:
            raise ValueError(f"Unknown export format {format!r}, allowed: {','.join(formats.keys())}")
        BackendClass = get_class_of(formats[format, "export"])
        with open(filename, "w") as stream:
            backend = BackendClass(stream, universe=self.dimensions)
            try:
                helper = RepoExportContext(self, backend=backend, directory=directory, transfer=transfer)
                with self._caching_context():
                    yield helper
            except BaseException:
                raise
            else:
                helper._finish()

    def import_(
        self,
        *,
        directory: ResourcePathExpression | None = None,
        filename: ResourcePathExpression | TextIO | None = None,
        format: str | None = None,
        transfer: str | None = None,
        skip_dimensions: set | None = None,
        record_validation_info: bool = True,
        without_datastore: bool = False,
    ) -> None:
        # Docstring inherited.
        if not self.isWriteable():
            raise TypeError("Butler is read-only.")
        if format is None:
            if filename is None:
                raise TypeError("At least one of 'filename' or 'format' must be provided.")
            else:
                _, format = os.path.splitext(filename)  # type: ignore
        elif filename is None:
            filename = ResourcePath(f"export.{format}", forceAbsolute=False)
        if directory is not None:
            directory = ResourcePath(directory, forceDirectory=True)
        # mypy doesn't think this will work but it does in python >= 3.10.
        if isinstance(filename, ResourcePathExpression):  # type: ignore
            filename = ResourcePath(filename, forceAbsolute=False)  # type: ignore
            if not filename.isabs() and directory is not None:
                potential = directory.join(filename)
                exists_in_cwd = filename.exists()
                exists_in_dir = potential.exists()
                if exists_in_cwd and exists_in_dir:
                    _LOG.warning(
                        "A relative path for filename was specified (%s) which exists relative to cwd. "
                        "Additionally, the file exists relative to the given search directory (%s). "
                        "Using the export file in the given directory.",
                        filename,
                        potential,
                    )
                    # Given they specified an explicit directory and that
                    # directory has the export file in it, assume that that
                    # is what was meant despite the file in cwd.
                    filename = potential
                elif exists_in_dir:
                    filename = potential
                elif not exists_in_cwd and not exists_in_dir:
                    # Raise early.
                    raise FileNotFoundError(
                        f"Export file could not be found in {filename.abspath()} or {potential.abspath()}."
                    )
        BackendClass: type[RepoImportBackend] = get_class_of(
            self._config["repo_transfer_formats"][format]["import"]
        )

        def doImport(importStream: TextIO | ResourceHandleProtocol) -> None:
            with self._caching_context():
                backend = BackendClass(importStream, self)  # type: ignore[call-arg]
                backend.register()
                with self.transaction():
                    backend.load(
                        datastore=self._datastore if not without_datastore else None,
                        directory=directory,
                        transfer=transfer,
                        skip_dimensions=skip_dimensions,
                        record_validation_info=record_validation_info,
                    )

        if isinstance(filename, ResourcePath):
            # We can not use open() here at the moment because of
            # DM-38589 since yaml does stream.read(8192) in a loop.
            stream = io.StringIO(filename.read().decode())
            doImport(stream)
        else:
            doImport(filename)  # type: ignore

    def transfer_dimension_records_from(
        self, source_butler: LimitedButler | Butler, source_refs: Iterable[DatasetRef]
    ) -> None:
        # Allowed dimensions in the target butler.
        elements = frozenset(element for element in self.dimensions.elements if element.has_own_table)

        data_ids = {ref.dataId for ref in source_refs}

        dimension_records = self._extract_all_dimension_records_from_data_ids(
            source_butler, data_ids, elements
        )

        # Insert order is important.
        for element in self.dimensions.sorted(dimension_records.keys()):
            records = [r for r in dimension_records[element].values()]
            # Assume that if the record is already present that we can
            # use it without having to check that the record metadata
            # is consistent.
            self._registry.insertDimensionData(element, *records, skip_existing=True)
            _LOG.debug("Dimension '%s' -- number of records transferred: %d", element.name, len(records))

    def _extract_all_dimension_records_from_data_ids(
        self,
        source_butler: LimitedButler | Butler,
        data_ids: set[DataCoordinate],
        allowed_elements: frozenset[DimensionElement],
    ) -> dict[DimensionElement, dict[DataCoordinate, DimensionRecord]]:
        primary_records = self._extract_dimension_records_from_data_ids(
            source_butler, data_ids, allowed_elements
        )

        can_query = True if isinstance(source_butler, Butler) else False

        additional_records: dict[DimensionElement, dict[DataCoordinate, DimensionRecord]] = defaultdict(dict)
        for original_element, record_mapping in primary_records.items():
            # Get dimensions that depend on this dimension.
            populated_by = self.dimensions.get_elements_populated_by(
                self.dimensions[original_element.name]  # type: ignore
            )

            for data_id in record_mapping.keys():
                for element in populated_by:
                    if element not in allowed_elements:
                        continue
                    if element.name == original_element.name:
                        continue

                    if element.name in primary_records:
                        # If this element has already been stored avoid
                        # re-finding records since that may lead to additional
                        # spurious records. e.g. visit is populated_by
                        # visit_detector_region but querying
                        # visit_detector_region by visit will return all the
                        # detectors for this visit -- the visit dataId does not
                        # constrain this.
                        # To constrain the query the original dataIds would
                        # have to be scanned.
                        continue

                    if not can_query:
                        raise RuntimeError(
                            f"Transferring populated_by records like {element.name} requires a full Butler."
                        )

                    records = source_butler.query_dimension_records(  # type: ignore
                        element.name,
                        explain=False,
                        **data_id.mapping,  # type: ignore
                    )
                    for record in records:
                        additional_records[record.definition].setdefault(record.dataId, record)

        # The next step is to walk back through the additional records to
        # pick up any missing content (such as visit_definition needing to
        # know the exposure). Want to ensure we do not request records we
        # already have.
        missing_data_ids = set()
        for name, record_mapping in additional_records.items():
            for data_id in record_mapping.keys():
                if data_id not in primary_records[name]:
                    missing_data_ids.add(data_id)

        # Fill out the new records. Assume that these new records do not
        # also need to carry over additional populated_by records.
        secondary_records = self._extract_dimension_records_from_data_ids(
            source_butler, missing_data_ids, allowed_elements
        )

        # Merge the extra sets of records in with the original.
        for name, record_mapping in itertools.chain(additional_records.items(), secondary_records.items()):
            primary_records[name].update(record_mapping)

        return primary_records

    def _extract_dimension_records_from_data_ids(
        self,
        source_butler: LimitedButler | Butler,
        data_ids: set[DataCoordinate],
        allowed_elements: frozenset[DimensionElement],
    ) -> dict[DimensionElement, dict[DataCoordinate, DimensionRecord]]:
        dimension_records: dict[DimensionElement, dict[DataCoordinate, DimensionRecord]] = defaultdict(dict)

        for data_id in data_ids:
            # Need an expanded record, if not expanded that we need a full
            # butler with registry (allow mocks with registry too).
            if not data_id.hasRecords():
                if registry := getattr(source_butler, "registry", None):
                    data_id = registry.expandDataId(data_id)
                else:
                    raise TypeError("Input butler needs to be a full butler to expand DataId.")
            # If this butler doesn't know about a dimension in the source
            # butler things will break later.
            for element_name in data_id.dimensions.elements:
                record = data_id.records[element_name]
                if record is not None and record.definition in allowed_elements:
                    dimension_records[record.definition].setdefault(record.dataId, record)

        return dimension_records

    def transfer_from(
        self,
        source_butler: LimitedButler,
        source_refs: Iterable[DatasetRef],
        transfer: str = "auto",
        skip_missing: bool = True,
        register_dataset_types: bool = False,
        transfer_dimensions: bool = False,
        dry_run: bool = False,
    ) -> collections.abc.Collection[DatasetRef]:
        # Docstring inherited.
        if not self.isWriteable():
            raise TypeError("Butler is read-only.")
        progress = Progress("lsst.daf.butler.Butler.transfer_from", level=VERBOSE)

        # Will iterate through the refs multiple times so need to convert
        # to a list if this isn't a collection.
        if not isinstance(source_refs, collections.abc.Collection):
            source_refs = list(source_refs)

        original_count = len(source_refs)
        _LOG.info("Transferring %d datasets into %s", original_count, str(self))

        # In some situations the datastore artifact may be missing
        # and we do not want that registry entry to be imported.
        # Asking datastore is not sufficient, the records may have been
        # purged, we have to ask for the (predicted) URI and check
        # existence explicitly. Execution butler is set up exactly like
        # this with no datastore records.
        artifact_existence: dict[ResourcePath, bool] = {}
        if skip_missing:
            dataset_existence = source_butler._datastore.mexists(
                source_refs, artifact_existence=artifact_existence
            )
            source_refs = [ref for ref, exists in dataset_existence.items() if exists]
            filtered_count = len(source_refs)
            n_missing = original_count - filtered_count
            _LOG.verbose(
                "%d dataset%s removed because the artifact does not exist. Now have %d.",
                n_missing,
                "" if n_missing == 1 else "s",
                filtered_count,
            )

        # Importing requires that we group the refs by dataset type and run
        # before doing the import.
        source_dataset_types = set()
        grouped_refs = defaultdict(list)
        for ref in source_refs:
            grouped_refs[ref.datasetType, ref.run].append(ref)
            source_dataset_types.add(ref.datasetType)

        # Check to see if the dataset type in the source butler has
        # the same definition in the target butler and register missing
        # ones if requested. Registration must happen outside a transaction.
        newly_registered_dataset_types = set()
        for datasetType in source_dataset_types:
            if register_dataset_types:
                # Let this raise immediately if inconsistent. Continuing
                # on to find additional inconsistent dataset types
                # might result in additional unwanted dataset types being
                # registered.
                try:
                    if self._registry.registerDatasetType(datasetType):
                        newly_registered_dataset_types.add(datasetType)
                except ConflictingDefinitionError as e:
                    # Be safe and require that conversions be bidirectional
                    # when there are storage class mismatches. This is because
                    # get() will have to support conversion from source to
                    # target python type (the source formatter will be
                    # returning source python type) but there also is an
                    # expectation that people will want to be able to get() in
                    # the target using the source python type, which will not
                    # require conversion for transferred datasets but might
                    # for target-native types. Additionally, butler.get does
                    # not know that the formatter will return the wrong
                    # python type and so will always check that the conversion
                    # works even though it won't need it.
                    target_dataset_type = self.get_dataset_type(datasetType.name)
                    target_compatible_with_source = target_dataset_type.is_compatible_with(datasetType)
                    source_compatible_with_target = datasetType.is_compatible_with(target_dataset_type)
                    if not (target_compatible_with_source and source_compatible_with_target):
                        if target_compatible_with_source:
                            e.add_note(
                                "Target dataset type storage class is compatible with source "
                                "but the reverse is not true."
                            )
                        elif source_compatible_with_target:
                            e.add_note(
                                "Source dataset type storage class is compatible with target "
                                "but the reverse is not true."
                            )
                        else:
                            e.add_note("If storage classes differ, please register converters.")
                        raise
            else:
                # If the dataset type is missing, let it fail immediately.
                target_dataset_type = self.get_dataset_type(datasetType.name)
                if target_dataset_type != datasetType:
                    target_compatible_with_source = target_dataset_type.is_compatible_with(datasetType)
                    source_compatible_with_target = datasetType.is_compatible_with(target_dataset_type)
                    # Both conversion directions are currently required.
                    if not (target_compatible_with_source and source_compatible_with_target):
                        msg = ""
                        if target_compatible_with_source:
                            msg = (
                                "Target storage class is compatible with the source storage class "
                                "but the reverse is not true."
                            )
                        elif source_compatible_with_target:
                            msg = (
                                "Source storage class is compatible with the target storage class"
                                " but the reverse is not true."
                            )
                        else:
                            msg = "If storage classes differ register converters."
                        if msg:
                            msg = f"({msg})"
                        raise ConflictingDefinitionError(
                            "Source butler dataset type differs from definition"
                            f" in target butler: {datasetType} !="
                            f" {target_dataset_type} {msg}"
                        )
        if newly_registered_dataset_types:
            # We may have registered some even if there were inconsistencies
            # but should let people know (or else remove them again).
            _LOG.verbose(
                "Registered the following dataset types in the target Butler: %s",
                ", ".join(d.name for d in newly_registered_dataset_types),
            )
        else:
            _LOG.verbose("All required dataset types are known to the target Butler")

        dimension_records: dict[DimensionElement, dict[DataCoordinate, DimensionRecord]] = defaultdict(dict)
        if transfer_dimensions:
            # Collect all the dimension records for these refs.
            # All dimensions are to be copied but the list of valid dimensions
            # come from this butler's universe.
            elements = frozenset(element for element in self.dimensions.elements if element.has_own_table)
            dataIds = {ref.dataId for ref in source_refs}
            dimension_records = self._extract_all_dimension_records_from_data_ids(
                source_butler, dataIds, elements
            )

        handled_collections: set[str] = set()

        # Do all the importing in a single transaction.
        with self.transaction():
            if dimension_records and not dry_run:
                _LOG.verbose("Ensuring that dimension records exist for transferred datasets.")
                # Order matters.
                for element in self.dimensions.sorted(dimension_records.keys()):
                    records = [r for r in dimension_records[element].values()]
                    # Assume that if the record is already present that we can
                    # use it without having to check that the record metadata
                    # is consistent.
                    self._registry.insertDimensionData(element, *records, skip_existing=True)

            n_imported = 0
            for (datasetType, run), refs_to_import in progress.iter_item_chunks(
                grouped_refs.items(), desc="Importing to registry by run and dataset type"
            ):
                if run not in handled_collections:
                    # May need to create output collection. If source butler
                    # has a registry, ask for documentation string.
                    run_doc = None
                    if registry := getattr(source_butler, "registry", None):
                        run_doc = registry.getCollectionDocumentation(run)
                    if not dry_run:
                        registered = self.collections.register(run, doc=run_doc)
                    else:
                        registered = True
                    handled_collections.add(run)
                    if registered:
                        _LOG.verbose("Creating output run %s", run)

                n_refs = len(refs_to_import)
                _LOG.verbose(
                    "Importing %d ref%s of dataset type %s into run %s",
                    n_refs,
                    "" if n_refs == 1 else "s",
                    datasetType.name,
                    run,
                )

                # Assume we are using UUIDs and the source refs will match
                # those imported.
                if not dry_run:
                    imported_refs = self._registry._importDatasets(refs_to_import)
                else:
                    imported_refs = refs_to_import

                # Check all the UUIDs were imported, ignoring dataset type
                # storage class differences.
                imported_uuids = set(ref.id for ref in imported_refs)
                uuids_to_import = set(ref.id for ref in refs_to_import)
                assert set(imported_uuids) == set(uuids_to_import)
                n_imported += len(imported_refs)

            assert len(source_refs) == n_imported
            _LOG.verbose("Imported %d datasets into destination butler", n_imported)

            # Ask the datastore to transfer. The datastore has to check that
            # the source datastore is compatible with the target datastore.
            accepted, rejected = self._datastore.transfer_from(
                source_butler._datastore,
                source_refs,
                transfer=transfer,
                artifact_existence=artifact_existence,
                dry_run=dry_run,
            )
            if rejected:
                # For now, accept the registry entries but not the files.
                _LOG.warning(
                    "%d datasets were rejected and %d accepted for dataset type %s in run %r.",
                    len(rejected),
                    len(accepted),
                    datasetType,
                    run,
                )

        return source_refs

    def validateConfiguration(
        self,
        logFailures: bool = False,
        datasetTypeNames: Iterable[str] | None = None,
        ignore: Iterable[str] | None = None,
    ) -> None:
        # Docstring inherited.
        if datasetTypeNames:
            datasetTypes = [self.get_dataset_type(name) for name in datasetTypeNames]
        else:
            datasetTypes = list(self._registry.queryDatasetTypes())

        # filter out anything from the ignore list
        if ignore:
            ignore = set(ignore)
            datasetTypes = [
                e for e in datasetTypes if e.name not in ignore and e.nameAndComponent()[0] not in ignore
            ]
        else:
            ignore = set()

        # For each datasetType that has an instrument dimension, create
        # a DatasetRef for each defined instrument
        datasetRefs = []

        # Find all the registered instruments (if "instrument" is in the
        # universe).
        if "instrument" in self.dimensions:
            instruments = {rec.name for rec in self.query_dimension_records("instrument", explain=False)}

            for datasetType in datasetTypes:
                if "instrument" in datasetType.dimensions:
                    # In order to create a conforming dataset ref, create
                    # fake DataCoordinate values for the non-instrument
                    # dimensions. The type of the value does not matter here.
                    dataId = {dim: 1 for dim in datasetType.dimensions.names if dim != "instrument"}

                    for instrument in instruments:
                        datasetRef = DatasetRef(
                            datasetType,
                            DataCoordinate.standardize(
                                dataId, instrument=instrument, dimensions=datasetType.dimensions
                            ),
                            run="validate",
                        )
                        datasetRefs.append(datasetRef)

        entities: list[DatasetType | DatasetRef] = []
        entities.extend(datasetTypes)
        entities.extend(datasetRefs)

        datastoreErrorStr = None
        try:
            self._datastore.validateConfiguration(entities, logFailures=logFailures)
        except ValidationError as e:
            datastoreErrorStr = str(e)

        # Also check that the LookupKeys used by the datastores match
        # registry and storage class definitions
        keys = self._datastore.getLookupKeys()

        failedNames = set()
        failedDataId = set()
        for key in keys:
            if key.name is not None:
                if key.name in ignore:
                    continue

                # skip if specific datasetType names were requested and this
                # name does not match
                if datasetTypeNames and key.name not in datasetTypeNames:
                    continue

                # See if it is a StorageClass or a DatasetType
                if key.name in self.storageClasses:
                    pass
                else:
                    try:
                        self.get_dataset_type(key.name)
                    except KeyError:
                        if logFailures:
                            _LOG.critical(
                                "Key '%s' does not correspond to a DatasetType or StorageClass", key
                            )
                        failedNames.add(key)
            else:
                # Dimensions are checked for consistency when the Butler
                # is created and rendezvoused with a universe.
                pass

            # Check that the instrument is a valid instrument
            # Currently only support instrument so check for that
            if key.dataId:
                dataIdKeys = set(key.dataId)
                if {"instrument"} != dataIdKeys:
                    if logFailures:
                        _LOG.critical("Key '%s' has unsupported DataId override", key)
                    failedDataId.add(key)
                elif key.dataId["instrument"] not in instruments:
                    if logFailures:
                        _LOG.critical("Key '%s' has unknown instrument", key)
                    failedDataId.add(key)

        messages = []

        if datastoreErrorStr:
            messages.append(datastoreErrorStr)

        for failed, msg in (
            (failedNames, "Keys without corresponding DatasetType or StorageClass entry: "),
            (failedDataId, "Keys with bad DataId entries: "),
        ):
            if failed:
                msg += ", ".join(str(k) for k in failed)
                messages.append(msg)

        if messages:
            raise ValidationError(";\n".join(messages))

    @property
    @deprecated(
        "Please use 'collections' instead. collection_chains will be removed after v28.",
        version="v28",
        category=FutureWarning,
    )
    def collection_chains(self) -> DirectButlerCollections:
        """Object with methods for modifying collection chains."""
        return DirectButlerCollections(self._registry)

    @property
    def collections(self) -> DirectButlerCollections:
        """Object with methods for modifying and inspecting collections."""
        return DirectButlerCollections(self._registry)

    @property
    def run(self) -> str | None:
        """Name of the run this butler writes outputs to by default (`str` or
        `None`).

        This is an alias for ``self.registry.defaults.run``.  It cannot be set
        directly in isolation, but all defaults may be changed together by
        assigning a new `RegistryDefaults` instance to
        ``self.registry.defaults``.
        """
        return self._registry.defaults.run

    @property
    def registry(self) -> Registry:
        """The object that manages dataset metadata and relationships
        (`Registry`).

        Many operations that don't involve reading or writing butler datasets
        are accessible only via `Registry` methods. Eventually these methods
        will be replaced by equivalent `Butler` methods.
        """
        return self._registry_shim

    @property
    def dimensions(self) -> DimensionUniverse:
        # Docstring inherited.
        return self._registry.dimensions

    def query(self) -> contextlib.AbstractContextManager[Query]:
        # Docstring inherited.
        return self._registry._query()

    def _query_driver(
        self,
        default_collections: Iterable[str],
        default_data_id: DataCoordinate,
    ) -> contextlib.AbstractContextManager[DirectQueryDriver]:
        """Set up a QueryDriver instance for use with this Butler.  Although
        this is marked as a private method, it is also used by Butler server.
        """
        return self._registry._query_driver(default_collections, default_data_id)

    def _preload_cache(self) -> None:
        """Immediately load caches that are used for common operations."""
        self._registry.preload_cache()

    _config: ButlerConfig
    """Configuration for this Butler instance."""

    _registry: SqlRegistry
    """The object that manages dataset metadata and relationships
    (`SqlRegistry`).

    Most operations that don't involve reading or writing butler datasets are
    accessible only via `SqlRegistry` methods.
    """

    datastore: Datastore
    """The object that manages actual dataset storage (`Datastore`).

    Direct user access to the datastore should rarely be necessary; the primary
    exception is the case where a `Datastore` implementation provides extra
    functionality beyond what the base class defines.
    """

    storageClasses: StorageClassFactory
    """An object that maps known storage class names to objects that fully
    describe them (`StorageClassFactory`).
    """

    _registry_shim: RegistryShim
    """Shim object to provide a legacy public interface for querying via the
    the ``registry`` property.
    """

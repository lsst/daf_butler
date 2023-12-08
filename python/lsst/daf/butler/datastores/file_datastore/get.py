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

__all__ = (
    "DatastoreFileGetInformation",
    "DatasetLocationInformation",
    "generate_datastore_get_information",
    "get_dataset_as_python_object_from_get_info",
)

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, TypeAlias

from lsst.daf.butler import DatasetRef, FileDescriptor, Formatter, Location, StorageClass
from lsst.daf.butler.datastore.cache_manager import AbstractDatastoreCacheManager
from lsst.daf.butler.datastore.generic_base import post_process_get
from lsst.daf.butler.datastore.stored_file_info import StoredFileInfo
from lsst.utils.introspection import get_instance_of
from lsst.utils.logging import getLogger
from lsst.utils.timer import time_this

log = getLogger(__name__)

DatasetLocationInformation: TypeAlias = tuple[Location, StoredFileInfo]


@dataclass(frozen=True)
class DatastoreFileGetInformation:
    """Collection of useful parameters needed to retrieve a file from
    a Datastore.
    """

    location: Location
    """The location from which to read the dataset."""

    formatter: Formatter
    """The `Formatter` to use to deserialize the dataset."""

    info: StoredFileInfo
    """Stored information about this file and its formatter."""

    assemblerParams: Mapping[str, Any]
    """Parameters to use for post-processing the retrieved dataset."""

    formatterParams: Mapping[str, Any]
    """Parameters that were understood by the associated formatter."""

    component: str | None
    """The component to be retrieved (can be `None`)."""

    readStorageClass: StorageClass
    """The `StorageClass` of the dataset being read."""


def generate_datastore_get_information(
    fileLocations: list[DatasetLocationInformation],
    *,
    ref: DatasetRef,
    parameters: Mapping[str, Any] | None,
    readStorageClass: StorageClass | None = None,
) -> list[DatastoreFileGetInformation]:
    """Process parameters and instantiate formatters for in preparation for
    retrieving an artifact and converting it to a Python object.

    Parameters
    ----------
    fileLocations : `list`[`DatasetLocationInformation`]
        List of file locations for this artifact and their associated datastore
        records.
    ref : `DatasetRef`
        The registry information associated with this artifact.
    parameters : `Mapping`[`str`, `Any`]
        `StorageClass` and `Formatter` parameters.
    readStorageClass : `StorageClass` | `None`, optional
        The StorageClass to use when ultimately returning the resulting object
        from the get.  Defaults to the `StorageClass` specified by ``ref``.

    Returns
    -------
    getInfo : `list` [`DatastoreFileGetInformation`]
        The parameters needed to retrieve each file.
    """
    if readStorageClass is None:
        readStorageClass = ref.datasetType.storageClass

    # Is this a component request?
    refComponent = ref.datasetType.component()

    disassembled = len(fileLocations) > 1
    fileGetInfo = []
    for location, storedFileInfo in fileLocations:
        # The storage class used to write the file
        writeStorageClass = storedFileInfo.storageClass

        # If this has been disassembled we need read to match the write
        if disassembled:
            readStorageClass = writeStorageClass

        formatter = get_instance_of(
            storedFileInfo.formatter,
            FileDescriptor(
                location,
                readStorageClass=readStorageClass,
                storageClass=writeStorageClass,
                parameters=parameters,
            ),
            ref.dataId,
        )

        formatterParams, notFormatterParams = formatter.segregateParameters()

        # Of the remaining parameters, extract the ones supported by
        # this StorageClass (for components not all will be handled)
        assemblerParams = readStorageClass.filterParameters(notFormatterParams)

        # The ref itself could be a component if the dataset was
        # disassembled by butler, or we disassembled in datastore and
        # components came from the datastore records
        component = storedFileInfo.component if storedFileInfo.component else refComponent

        fileGetInfo.append(
            DatastoreFileGetInformation(
                location,
                formatter,
                storedFileInfo,
                assemblerParams,
                formatterParams,
                component,
                readStorageClass,
            )
        )

    return fileGetInfo


def _read_artifact_into_memory(
    getInfo: DatastoreFileGetInformation,
    ref: DatasetRef,
    cache_manager: AbstractDatastoreCacheManager,
    isComponent: bool = False,
    cache_ref: DatasetRef | None = None,
) -> Any:
    """Read the artifact from datastore into in memory object.

    Parameters
    ----------
    getInfo : `DatastoreFileGetInformation`
        Information about the artifact within the datastore.
    ref : `DatasetRef`
        The registry information associated with this artifact.
    isComponent : `bool`
        Flag to indicate if a component is being read from this artifact.
    cache_manager : `AbstractDatastoreCacheManager`
        The cache manager to use for caching retrieved files
    cache_ref : `DatasetRef`, optional
        The DatasetRef to use when looking up the file in the cache.
        This ref must have the same ID as the supplied ref but can
        be a parent ref or component ref to indicate to the cache whether
        a composite file is being requested from the cache or a component
        file. Without this the cache will default to the supplied ref but
        it can get confused with read-only derived components for
        disassembled composites.

    Returns
    -------
    inMemoryDataset : `object`
        The artifact as a python object.
    """
    location = getInfo.location
    uri = location.uri
    log.debug("Accessing data from %s", uri)

    if cache_ref is None:
        cache_ref = ref
    if cache_ref.id != ref.id:
        raise ValueError(
            "The supplied cache dataset ref refers to a different dataset than expected:"
            f" {ref.id} != {cache_ref.id}"
        )

    # Cannot recalculate checksum but can compare size as a quick check
    # Do not do this if the size is negative since that indicates
    # we do not know.
    recorded_size = getInfo.info.file_size

    def check_resource_size(resource_size: int) -> None:
        if recorded_size >= 0 and resource_size != recorded_size:
            raise RuntimeError(
                "Integrity failure in Datastore. "
                f"Size of file {uri} ({resource_size}) "
                f"does not match size recorded in registry of {recorded_size}"
            )

    # For the general case we have choices for how to proceed.
    # 1. Always use a local file (downloading the remote resource to a
    #    temporary file if needed).
    # 2. Use a threshold size and read into memory and use bytes.
    # Use both for now with an arbitrary hand off size.
    # This allows small datasets to be downloaded from remote object
    # stores without requiring a temporary file.

    formatter = getInfo.formatter
    nbytes_max = 10_000_000  # Arbitrary number that we can tune
    if recorded_size >= 0 and recorded_size <= nbytes_max and formatter.can_read_bytes():
        with cache_manager.find_in_cache(cache_ref, uri.getExtension()) as cached_file:
            if cached_file is not None:
                desired_uri = cached_file
                msg = f" (cached version of {uri})"
            else:
                desired_uri = uri
                msg = ""
            with time_this(log, msg="Reading bytes from %s%s", args=(desired_uri, msg)):
                serializedDataset = desired_uri.read()
                check_resource_size(len(serializedDataset))
        log.debug(
            "Deserializing %s from %d bytes from location %s with formatter %s",
            f"component {getInfo.component}" if isComponent else "",
            len(serializedDataset),
            uri,
            formatter.name(),
        )
        try:
            result = formatter.fromBytes(
                serializedDataset, component=getInfo.component if isComponent else None
            )
        except Exception as e:
            raise ValueError(
                f"Failure from formatter '{formatter.name()}' for dataset {ref.id}"
                f" ({ref.datasetType.name} from {uri}): {e}"
            ) from e
    else:
        # Read from file.

        # Have to update the Location associated with the formatter
        # because formatter.read does not allow an override.
        # This could be improved.
        location_updated = False
        msg = ""

        # First check in cache for local version.
        # The cache will only be relevant for remote resources but
        # no harm in always asking. Context manager ensures that cache
        # file is not deleted during cache expiration.
        with cache_manager.find_in_cache(cache_ref, uri.getExtension()) as cached_file:
            if cached_file is not None:
                msg = f"(via cache read of remote file {uri})"
                uri = cached_file
                location_updated = True

            with uri.as_local() as local_uri:
                check_resource_size(local_uri.size())
                can_be_cached = False
                if uri != local_uri:
                    # URI was remote and file was downloaded
                    cache_msg = ""
                    location_updated = True

                    if cache_manager.should_be_cached(cache_ref):
                        # In this scenario we want to ask if the downloaded
                        # file should be cached but we should not cache
                        # it until after we've used it (to ensure it can't
                        # be expired whilst we are using it).
                        can_be_cached = True

                        # Say that it is "likely" to be cached because
                        # if the formatter read fails we will not be
                        # caching this file.
                        cache_msg = " and likely cached"

                    msg = f"(via download to local file{cache_msg})"

                # Calculate the (possibly) new location for the formatter
                # to use.
                newLocation = Location(*local_uri.split()) if location_updated else None

                log.debug(
                    "Reading%s from location %s %s with formatter %s",
                    f" component {getInfo.component}" if isComponent else "",
                    uri,
                    msg,
                    formatter.name(),
                )
                try:
                    with (
                        formatter._updateLocation(newLocation),
                        time_this(
                            log,
                            msg="Reading%s from location %s %s with formatter %s",
                            args=(
                                f" component {getInfo.component}" if isComponent else "",
                                uri,
                                msg,
                                formatter.name(),
                            ),
                        ),
                    ):
                        result = formatter.read(component=getInfo.component if isComponent else None)
                except Exception as e:
                    raise ValueError(
                        f"Failure from formatter '{formatter.name()}' for dataset {ref.id}"
                        f" ({ref.datasetType.name} from {uri}): {e}"
                    ) from e

                # File was read successfully so can move to cache
                if can_be_cached:
                    cache_manager.move_to_cache(local_uri, cache_ref)

    return post_process_get(
        result, ref.datasetType.storageClass, getInfo.assemblerParams, isComponent=isComponent
    )


def get_dataset_as_python_object_from_get_info(
    allGetInfo: list[DatastoreFileGetInformation],
    *,
    ref: DatasetRef,
    parameters: Mapping[str, Any] | None,
    cache_manager: AbstractDatastoreCacheManager,
) -> Any:
    """Retrieve an artifact from storage and return it as a Python object.

    Parameters
    ----------
    allGetInfo : `list`[`DatastoreFileGetInformation`]
        Pre-processed information about each file associated with this
        artifact.
    ref : `DatasetRef`
        The registry information associated with this artifact.
    parameters : `Mapping`[`str`, `Any`]
        `StorageClass` and `Formatter` parameters.
    cache_manager : `AbstractDatastoreCacheManager`
        The cache manager to use for caching retrieved files.

    Returns
    -------
    python_object : `typing.Any`
        The retrieved artifact, converted to a Python object according to the
        `StorageClass` specified in ``ref``.
    """
    refStorageClass = ref.datasetType.storageClass
    refComponent = ref.datasetType.component()
    # Create mapping from component name to related info
    allComponents = {i.component: i for i in allGetInfo}

    # By definition the dataset is disassembled if we have more
    # than one record for it.
    isDisassembled = len(allGetInfo) > 1

    # Look for the special case where we are disassembled but the
    # component is a derived component that was not written during
    # disassembly. For this scenario we need to check that the
    # component requested is listed as a derived component for the
    # composite storage class
    isDisassembledReadOnlyComponent = False
    if isDisassembled and refComponent:
        # The composite storage class should be accessible through
        # the component dataset type
        compositeStorageClass = ref.datasetType.parentStorageClass

        # In the unlikely scenario where the composite storage
        # class is not known, we can only assume that this is a
        # normal component. If that assumption is wrong then the
        # branch below that reads a persisted component will fail
        # so there is no need to complain here.
        if compositeStorageClass is not None:
            isDisassembledReadOnlyComponent = refComponent in compositeStorageClass.derivedComponents

    if isDisassembled and not refComponent:
        # This was a disassembled dataset spread over multiple files
        # and we need to put them all back together again.
        # Read into memory and then assemble

        # Check that the supplied parameters are suitable for the type read
        refStorageClass.validateParameters(parameters)

        # We want to keep track of all the parameters that were not used
        # by formatters.  We assume that if any of the component formatters
        # use a parameter that we do not need to apply it again in the
        # assembler.
        usedParams = set()

        components: dict[str, Any] = {}
        for getInfo in allGetInfo:
            # assemblerParams are parameters not understood by the
            # associated formatter.
            usedParams.update(set(getInfo.formatterParams))

            component = getInfo.component

            if component is None:
                raise RuntimeError(f"Internal error in datastore assembly of {ref}")

            # We do not want the formatter to think it's reading
            # a component though because it is really reading a
            # standalone dataset -- always tell reader it is not a
            # component.
            components[component] = _read_artifact_into_memory(
                getInfo, ref.makeComponentRef(component), cache_manager, isComponent=False
            )

        inMemoryDataset = ref.datasetType.storageClass.delegate().assemble(components)

        # Any unused parameters will have to be passed to the assembler
        if parameters:
            unusedParams = {k: v for k, v in parameters.items() if k not in usedParams}
        else:
            unusedParams = {}

        # Process parameters
        return ref.datasetType.storageClass.delegate().handleParameters(
            inMemoryDataset, parameters=unusedParams
        )

    elif isDisassembledReadOnlyComponent:
        compositeStorageClass = ref.datasetType.parentStorageClass
        if compositeStorageClass is None:
            raise RuntimeError(
                f"Unable to retrieve derived component '{refComponent}' since"
                "no composite storage class is available."
            )

        if refComponent is None:
            # Mainly for mypy
            raise RuntimeError("Internal error in datastore: component can not be None here")

        # Assume that every derived component can be calculated by
        # forwarding the request to a single read/write component.
        # Rather than guessing which rw component is the right one by
        # scanning each for a derived component of the same name,
        # we ask the storage class delegate directly which one is best to
        # use.
        compositeDelegate = compositeStorageClass.delegate()
        forwardedComponent = compositeDelegate.selectResponsibleComponent(refComponent, set(allComponents))

        # Select the relevant component
        rwInfo = allComponents[forwardedComponent]

        # For now assume that read parameters are validated against
        # the real component and not the requested component
        forwardedStorageClass = rwInfo.formatter.fileDescriptor.readStorageClass
        forwardedStorageClass.validateParameters(parameters)

        # The reference to use for the caching must refer to the forwarded
        # component and not the derived component.
        cache_ref = ref.makeCompositeRef().makeComponentRef(forwardedComponent)

        # Unfortunately the FileDescriptor inside the formatter will have
        # the wrong write storage class so we need to create a new one
        # given the immutability constraint.
        writeStorageClass = rwInfo.info.storageClass

        # We may need to put some thought into parameters for read
        # components but for now forward them on as is
        readFormatter = type(rwInfo.formatter)(
            FileDescriptor(
                rwInfo.location,
                readStorageClass=refStorageClass,
                storageClass=writeStorageClass,
                parameters=parameters,
            ),
            ref.dataId,
        )

        # The assembler can not receive any parameter requests for a
        # derived component at this time since the assembler will
        # see the storage class of the derived component and those
        # parameters will have to be handled by the formatter on the
        # forwarded storage class.
        assemblerParams: dict[str, Any] = {}

        # Need to created a new info that specifies the derived
        # component and associated storage class
        readInfo = DatastoreFileGetInformation(
            rwInfo.location,
            readFormatter,
            rwInfo.info,
            assemblerParams,
            {},
            refComponent,
            refStorageClass,
        )

        return _read_artifact_into_memory(readInfo, ref, cache_manager, isComponent=True, cache_ref=cache_ref)

    else:
        # Single file request or component from that composite file
        for lookup in (refComponent, None):
            if lookup in allComponents:
                getInfo = allComponents[lookup]
                break
        else:
            raise FileNotFoundError(f"Component {refComponent} not found for ref {ref} in datastore")

        # Do not need the component itself if already disassembled
        if isDisassembled:
            isComponent = False
        else:
            isComponent = getInfo.component is not None

        # For a component read of a composite we want the cache to
        # be looking at the composite ref itself.
        cache_ref = ref.makeCompositeRef() if isComponent else ref

        # For a disassembled component we can validate parameters against
        # the component storage class directly
        if isDisassembled:
            refStorageClass.validateParameters(parameters)
        else:
            # For an assembled composite this could be a derived
            # component derived from a real component. The validity
            # of the parameters is not clear. For now validate against
            # the composite storage class
            getInfo.formatter.fileDescriptor.storageClass.validateParameters(parameters)

        return _read_artifact_into_memory(
            getInfo, ref, cache_manager, isComponent=isComponent, cache_ref=cache_ref
        )

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
    "DatasetLocationInformation",
    "DatastoreFileGetInformation",
    "generate_datastore_get_information",
    "get_dataset_as_python_object_from_get_info",
)

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, TypeAlias

from lsst.daf.butler import (
    DatasetRef,
    FileDescriptor,
    FileIntegrityError,
    Formatter,
    FormatterV1inV2,
    FormatterV2,
    Location,
    StorageClass,
)
from lsst.daf.butler.datastore.cache_manager import AbstractDatastoreCacheManager
from lsst.daf.butler.datastore.generic_base import post_process_get
from lsst.daf.butler.datastore.stored_file_info import StoredFileInfo
from lsst.utils.introspection import get_instance_of
from lsst.utils.logging import getLogger

log = getLogger(__name__)

DatasetLocationInformation: TypeAlias = tuple[Location, StoredFileInfo]


@dataclass(frozen=True)
class DatastoreFileGetInformation:
    """Collection of useful parameters needed to retrieve a file from
    a Datastore.
    """

    location: Location
    """The location from which to read the dataset."""

    formatter: Formatter | FormatterV2
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
    """The `StorageClass` that the `Formatter` will return."""

    componentStorageClass: StorageClass | None = None
    """The `StorageClass` of the component to extract from the object returned
    by the `Formatter`, or `None` if the `Formatter` returns the requested
    object directly.

    This is only set when the requested component is defined by the read
    `StorageClass` but not by the `StorageClass` the file was written with.
    The `Formatter` then knows nothing of the component, so the composite is
    read and converted first (to ``readStorageClass``) and the component is
    extracted from the converted form.
    """


def _describe_components(storageClass: StorageClass) -> str:
    """Return a description of the components a `StorageClass` recognizes,
    suitable for inclusion in a log message.

    Parameters
    ----------
    storageClass : `StorageClass`
        Storage class to describe.

    Returns
    -------
    description : `str`
        Comma-separated list of component names, or ``"none"``.
    """
    return ", ".join(sorted(storageClass.allComponents())) or "none"


def _warn_about_converted_component_read(
    registry_ref: DatasetRef,
    component: str,
    writeStorageClass: StorageClass,
    parentReadStorageClass: StorageClass,
) -> None:
    """Warn that a component request needs the whole dataset to be read.

    Parameters
    ----------
    registry_ref : `DatasetRef`
        The dataset as defined in the repository, used to identify the dataset
        in the message.
    component : `str`
        Name of the component that was requested.
    writeStorageClass : `StorageClass`
        The `StorageClass` the dataset was written with, which does not define
        ``component``.
    parentReadStorageClass : `StorageClass`
        The `StorageClass` the composite has to be converted to in order to
        obtain ``component``.
    """
    message = (
        "Component %r was requested from dataset %s but storage class %s, which the dataset was "
        "written with, does not define it (components it does define: %s). The entire dataset must "
        "therefore be retrieved and converted to storage class %s before %r can be extracted from "
        "the result. This is slower than reading the component on its own: the storage class "
        "override has made this a less efficient request."
    )
    args: list[Any] = [
        component,
        registry_ref,
        writeStorageClass.name,
        _describe_components(writeStorageClass),
        parentReadStorageClass.name,
        component,
    ]
    registryStorageClass = registry_ref.datasetType.storageClass
    if registryStorageClass != writeStorageClass:
        # The dataset type definition has changed since the dataset was
        # written, so the caller may be surprised that the component they were
        # offered is not available from the file itself.
        message += (
            " The dataset type is now defined with storage class %s (components: %s), which is why "
            "the component appeared to be available."
        )
        args += [registryStorageClass.name, _describe_components(registryStorageClass)]
    log.warning(message, *args)


def generate_datastore_get_information(
    fileLocations: list[DatasetLocationInformation],
    *,
    registry_ref: DatasetRef,
    read_ref: DatasetRef,
    parameters: Mapping[str, Any] | None,
) -> list[DatastoreFileGetInformation]:
    """Process parameters and instantiate formatters for in preparation for
    retrieving an artifact and converting it to a Python object.

    Parameters
    ----------
    fileLocations : `list` [`DatasetLocationInformation`]
        List of file locations for this artifact and their associated datastore
        records.
    registry_ref : `DatasetRef`
        The registry information associated with this artifact, using the
        dataset type definition from the repository and never naming a
        component.  This is the ref given to the `Formatter`.
    read_ref : `DatasetRef`
        The dataset the caller asked for.  Its `StorageClass` is the one to
        return and its component (if any) is the component to extract.  This
        differs from ``registry_ref`` when a read-time `StorageClass` override
        has been requested or a component has been requested.
    parameters : `~collections.abc.Mapping` [`str`, `typing.Any`]
        `StorageClass` and `Formatter` parameters.

    Returns
    -------
    getInfo : `list` [`DatastoreFileGetInformation`]
        The parameters needed to retrieve each file.

    Notes
    -----
    The `StorageClass` that each file was written with is taken from the
    datastore records in ``fileLocations`` and can differ from the one in
    ``registry_ref`` if the dataset type definition has been changed since the
    dataset was written.
    """
    readStorageClass = read_ref.datasetType.storageClass

    # Is this a component request?
    refComponent = read_ref.datasetType.component()

    # The storage class of the composite that the caller wants to read. This
    # differs from the write storage class when there is an override.
    parentReadStorageClass = read_ref.datasetType.parentStorageClass

    disassembled = len(fileLocations) > 1
    fileGetInfo = []
    for location, storedFileInfo in fileLocations:
        # The storage class used to write the file
        writeStorageClass = storedFileInfo.storageClass
        thisReadStorageClass = readStorageClass
        componentStorageClass = None

        # If this has been disassembled we need read to match the write
        # except for if a component has specified an override.
        if disassembled and storedFileInfo.component != refComponent:
            thisReadStorageClass = writeStorageClass
        elif (
            refComponent is not None
            and storedFileInfo.component is None
            and parentReadStorageClass is not None
            and refComponent not in writeStorageClass.allComponents()
        ):
            # The component only exists in the storage class the caller is
            # converting to, so the formatter can not extract it. Read the
            # whole composite as the converted type instead and extract the
            # component from that.
            componentStorageClass = readStorageClass
            thisReadStorageClass = parentReadStorageClass
            _warn_about_converted_component_read(
                registry_ref, refComponent, writeStorageClass, parentReadStorageClass
            )

        formatter = get_instance_of(
            storedFileInfo.formatter,
            FileDescriptor(
                location,
                readStorageClass=thisReadStorageClass,
                storageClass=writeStorageClass,
                parameters=parameters,
                component=storedFileInfo.component,
            ),
            dataId=registry_ref.dataId,
            ref=registry_ref,
        )

        formatterParams, notFormatterParams = formatter.segregate_parameters()

        # Of the remaining parameters, extract the ones supported by
        # this StorageClass (for components not all will be handled)
        assemblerParams = thisReadStorageClass.filterParameters(notFormatterParams)

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
                thisReadStorageClass,
                componentStorageClass,
            )
        )

    return fileGetInfo


def _read_artifact_into_memory(
    getInfo: DatastoreFileGetInformation,
    ref: DatasetRef,
    cache_manager: AbstractDatastoreCacheManager,
    isComponent: bool = False,
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

    Returns
    -------
    inMemoryDataset : `object`
        The artifact as a python object.
    """
    location = getInfo.location
    uri = location.uri
    log.debug("Accessing data from %s", uri)

    # Cannot recalculate checksum but can compare size as a quick check
    # Do not do this if the size is negative since that indicates
    # we do not know.
    recorded_size = getInfo.info.file_size

    formatter = getInfo.formatter

    if isinstance(formatter, Formatter):
        formatter = FormatterV1inV2(
            formatter.file_descriptor,
            ref=ref,
            formatter=formatter,
            write_parameters=formatter.write_parameters,
            write_recipes=formatter.write_recipes,
        )

    assert isinstance(formatter, FormatterV2)

    try:
        result = formatter.read(
            component=getInfo.component if isComponent else None,
            expected_size=recorded_size,
            cache_manager=cache_manager,
        )
    except (FileNotFoundError, FileIntegrityError):
        # This is expected for the case where the resource is missing
        # or the information we passed to the formatter about the file size
        # is incorrect.
        # Allow them to propagate up.
        raise
    except Exception as e:
        # For clarity, include any notes that may have been added by the
        # formatter to this new exception.
        notes = "\n".join(getattr(e, "__notes__", []))
        if notes:
            notes = "\n" + notes
        raise ValueError(
            f"Failure from formatter '{formatter.name()}' for dataset {ref.id}"
            f" ({ref.datasetType.name} from {uri}): {e}{notes}"
        ) from e

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
    allGetInfo : `list` [`DatastoreFileGetInformation`]
        Pre-processed information about each file associated with this
        artifact.
    ref : `DatasetRef`
        The registry information associated with this artifact.
    parameters : `~collections.abc.Mapping` [`str`, `typing.Any`]
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
        forwardedStorageClass = rwInfo.formatter.file_descriptor.readStorageClass
        forwardedStorageClass.validateParameters(parameters)

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
                component=forwardedComponent,
            ),
            dataId=ref.dataId,
            ref=ref,
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

        return _read_artifact_into_memory(readInfo, ref, cache_manager, isComponent=True)

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

        # For a disassembled component we can validate parameters against
        # the component storage class directly
        if isDisassembled:
            refStorageClass.validateParameters(parameters)
        else:
            # For an assembled composite this could be a derived
            # component derived from a real component. The validity
            # of the parameters is not clear. For now validate against
            # the composite storage class
            getInfo.formatter.file_descriptor.storageClass.validateParameters(parameters)

        if getInfo.componentStorageClass is not None:
            # The formatter knows nothing of this component because it is
            # defined by the read storage class and not by the storage class
            # the file was written with. Read the whole composite, letting the
            # formatter convert it to the requested composite type, and then
            # extract the component from the result.
            if refComponent is None:
                raise RuntimeError("Internal error in datastore: component can not be None here")
            compositeRef = ref.makeCompositeRef()
            composite = _read_artifact_into_memory(getInfo, compositeRef, cache_manager, isComponent=False)
            delegate = compositeRef.datasetType.storageClass.delegate()
            return getInfo.componentStorageClass.coerce_type(delegate.getComponent(composite, refComponent))

        return _read_artifact_into_memory(getInfo, ref, cache_manager, isComponent=isComponent)

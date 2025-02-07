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

__all__ = (
    "FileIntegrityError",
    "Formatter",
    "FormatterFactory",
    "FormatterNotImplementedError",
    "FormatterParameter",
    "FormatterV1inV2",
    "FormatterV2",
)

import contextlib
import copy
import logging
import os
import zipfile
from abc import ABCMeta, abstractmethod
from collections.abc import Callable, Iterator, Mapping, Set
from typing import TYPE_CHECKING, Any, BinaryIO, ClassVar, TypeAlias, final

from lsst.resources import ResourceHandleProtocol, ResourcePath
from lsst.utils.introspection import get_full_type_name
from lsst.utils.timer import time_this

from ._config import Config
from ._config_support import LookupKey, processLookupConfigs
from ._file_descriptor import FileDescriptor
from ._location import Location
from .dimensions import DataCoordinate, DimensionUniverse
from .mapping_factory import MappingFactory

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from ._dataset_provenance import DatasetProvenance
    from ._dataset_ref import DatasetRef
    from ._dataset_type import DatasetType
    from ._storage_class import StorageClass
    from .datastore.cache_manager import AbstractDatastoreCacheManager

    # Define a new special type for functions that take "entity"
    Entity: TypeAlias = DatasetType | DatasetRef | StorageClass | str


class FileIntegrityError(RuntimeError):
    """The file metadata is inconsistent with the metadata supplied by
    the datastore.
    """


class FormatterNotImplementedError(NotImplementedError):
    """Formatter does not implement the specific read or write method
    that is being requested.
    """


class FormatterV2:
    """Interface for reading and writing datasets using URIs.

    The formatters are associated with a particular `StorageClass`.

    Parameters
    ----------
    file_descriptor : `FileDescriptor`, optional
        Identifies the file to read or write, and the associated storage
        classes and parameter information.
    ref : `DatasetRef`
        The dataset associated with this formatter. Should not be a component
        dataset ref.
    write_parameters : `dict`, optional
         Parameters to control how the dataset is serialized.
    write_recipes : `dict`, optional
        Detailed write recipes indexed by recipe name.

    **kwargs
        Additional arguments that will be ignored but allow for
        `Formatter` V1 parameters to be given.

    Notes
    -----
    A `FormatterV2` author should not override the default `read` or `write`
    method. Instead for read the formatter author should implement one or all
    of `read_from_stream`, `read_from_uri`, or `read_from_local_file`. The
    method `read_from_uri` will always be attempted first and could be more
    efficient (since it allows the possibility for a subset of the data file to
    be accessed remotely when parameters or components are specified) but it
    will not update the local cache. If the entire contents of the remote file
    are being accessed (no component or parameters defined) and the dataset
    would be cached, `read_from_uri` will be called with a local file. If the
    file is remote and the parameters that have been included are known to be
    more efficiently handled with a local file, the `read_from_uri` method can
    return `NotImplemented` to indicate that a local file should be given
    instead.

    Similarly for writes, the `write` method can not be subclassed. Instead
    the formatter author should implement `to_bytes` or `write_local_file`.
    For local URIs the system will always call `write_local_file` first (which
    by default will call `to_bytes`) to ensure atomic writes are implemented.
    For remote URIs with local caching disabled, `to_bytes` will be called
    first and the remote updated directly. If the dataset should be cached
    it will always be written locally first.
    """

    unsupported_parameters: ClassVar[Set[str] | None] = frozenset()
    """Set of read parameters not understood by this `Formatter`. An empty set
    means all parameters are supported.  `None` indicates that no parameters
    are supported. These parameters should match those defined in the storage
    class definition. (`frozenset`).
    """

    supported_write_parameters: ClassVar[Set[str] | None] = None
    """Parameters understood by this formatter that can be used to control
    how a dataset is serialized. `None` indicates that no parameters are
    supported."""

    default_extension: ClassVar[str | None] = None
    """Default extension to use when writing a file.

    Can be `None` if the extension is determined dynamically. Use the
    `get_write_extension` method to get the actual extension to use.
    """

    supported_extensions: ClassVar[Set[str]] = frozenset()
    """Set of all extensions supported by this formatter.

    Any extension assigned to the ``default_extension`` property will be
    automatically included in the list of supported extensions.
    """

    can_read_from_uri: ClassVar[bool] = False
    """Declare whether `read_from_uri` is available to this formatter."""

    can_read_from_stream: ClassVar[bool] = False
    """Declare whether `read_from_stream` is available to this formatter."""

    can_read_from_local_file: ClassVar[bool] = False
    """Declare whether `read_from_file` is available to this formatter."""

    def __init__(
        self,
        file_descriptor: FileDescriptor,
        *,
        ref: DatasetRef,
        write_parameters: Mapping[str, Any] | None = None,
        write_recipes: Mapping[str, Any] | None = None,
        # Compatibility parameters. Unused in v2.
        **kwargs: Any,
    ):
        if not isinstance(file_descriptor, FileDescriptor):
            raise TypeError("File descriptor must be a FileDescriptor")

        self._file_descriptor = file_descriptor

        if ref.isComponent():
            # It is a component ref for disassembled composites.
            ref = ref.makeCompositeRef()
        self._dataset_ref = ref

        # Check that the write parameters are allowed
        if write_parameters:
            if self.supported_write_parameters is None:
                raise ValueError(
                    f"This formatter does not accept any write parameters. Got: {', '.join(write_parameters)}"
                )
            else:
                given = set(write_parameters)
                unknown = given - self.supported_write_parameters
                if unknown:
                    s = "s" if len(unknown) != 1 else ""
                    unknownStr = ", ".join(f"'{u}'" for u in unknown)
                    raise ValueError(f"This formatter does not accept parameter{s} {unknownStr}")

        self._write_parameters = write_parameters
        self._write_recipes = self.validate_write_recipes(write_recipes)

    def __str__(self) -> str:
        return f"{self.name()}@{self.file_descriptor.location.uri}"

    def __repr__(self) -> str:
        return f"{self.name()}({self.file_descriptor!r})"

    @property
    def file_descriptor(self) -> FileDescriptor:
        """File descriptor associated with this formatter
        (`FileDescriptor`).
        """
        return self._file_descriptor

    @property
    def data_id(self) -> DataCoordinate:
        """Return Data ID associated with this formatter (`DataCoordinate`)."""
        return self._dataset_ref.dataId

    @property
    def dataset_ref(self) -> DatasetRef:
        """Return Dataset Ref associated with this formatter (`DatasetRef`)."""
        return self._dataset_ref

    @property
    def write_parameters(self) -> Mapping[str, Any]:
        """Parameters to use when writing out datasets."""
        if self._write_parameters is not None:
            return self._write_parameters
        return {}

    @property
    def write_recipes(self) -> Mapping[str, Any]:
        """Detailed write Recipes indexed by recipe name."""
        if self._write_recipes is not None:
            return self._write_recipes
        return {}

    def get_write_extension(self) -> str:
        """Extension to use when writing a file."""
        default_extension = self.default_extension
        extension = default_extension if default_extension is not None else ""
        return extension

    def can_accept(self, in_memory_dataset: Any) -> bool:
        """Indicate whether this formatter can accept the specified
        storage class directly.

        Parameters
        ----------
        in_memory_dataset : `object`
            The dataset that is to be written.

        Returns
        -------
        accepts : `bool`
            If `True` the formatter can write data of this type without
            requiring datastore to convert it. If `False` the datastore
            will attempt to convert before writing.

        Notes
        -----
        The base class always returns `False` even if the given type is an
        instance of the storage class type. This will result in a storage
        class conversion no-op but also allows mocks with mocked storage
        classes to work properly.
        """
        return False

    @classmethod
    def validate_write_recipes(cls, recipes: Mapping[str, Any] | None) -> Mapping[str, Any] | None:
        """Validate supplied recipes for this formatter.

        The recipes are supplemented with default values where appropriate.

        Parameters
        ----------
        recipes : `dict`
            Recipes to validate.

        Returns
        -------
        validated : `dict`
            Validated recipes.

        Raises
        ------
        RuntimeError
            Raised if validation fails. The default implementation raises
            if any recipes are given.
        """
        if recipes:
            raise RuntimeError(f"This formatter does not understand these write recipes: {recipes}")
        return recipes

    @classmethod
    def name(cls) -> str:
        """Return the fully qualified name of the formatter.

        Returns
        -------
        name : `str`
            Fully-qualified name of formatter class.
        """
        return get_full_type_name(cls)

    def _is_disassembled(self) -> bool:
        """Return `True` if this formatter is looking at a disassembled
        component.
        """
        return self.file_descriptor.component is not None

    def _check_resource_size(self, uri: ResourcePath, recorded_size: int, resource_size: int) -> None:
        """Compare the recorded size with the resource size.

        The given URI will not be accessed.
        """
        if recorded_size >= 0 and resource_size != recorded_size:
            raise FileIntegrityError(
                "Integrity failure in Datastore. "
                f"Size of file {uri} ({resource_size}) "
                f"does not match size recorded in registry of {recorded_size}"
            )

    def _get_cache_ref(self) -> DatasetRef:
        """Get the `DatasetRef` to use for cache look ups.

        Returns
        -------
        ref : `lsst.daf.butler.DatasetRef`
            The dataset ref to use when looking in the cache.
            For single-file dataset this will be the dataset ref directly.
            If this is disassembled we need the component and the component
            will be in the `FileDescriptor`.
        """
        if self.file_descriptor.component is None:
            cache_ref = self.dataset_ref
        else:
            cache_ref = self.dataset_ref.makeComponentRef(self.file_descriptor.component)
        return cache_ref

    def _ensure_cache(
        self, cache_manager: AbstractDatastoreCacheManager | None = None
    ) -> AbstractDatastoreCacheManager:
        """Return the cache if given else return a null cache."""
        if cache_manager is None:
            # Circular import avoidance.
            from .datastore.cache_manager import DatastoreDisabledCacheManager

            cache_manager = DatastoreDisabledCacheManager(None, None)
        return cache_manager

    def read(
        self,
        component: str | None = None,
        expected_size: int = -1,
        cache_manager: AbstractDatastoreCacheManager | None = None,
    ) -> Any:
        """Read a Dataset.

        Parameters
        ----------
        component : `str`, optional
            Component to read from the file. Only used if the `StorageClass`
            for reading differed from the `StorageClass` used to write the
            file.
        expected_size : `int`, optional
            If known, the expected size of the resource to read. This can be
            used for verification or to decide whether to do a direct read or a
            file download. ``-1`` indicates the file size is not known.
        cache_manager : `AbstractDatastoreCacheManager`
            A cache manager to use to allow a formatter to cache a remote file
            locally or read a cached file that is already local.

        Returns
        -------
        in_memory_dataset : `object`
            The requested Dataset.

        Notes
        -----
        This method should not be subclassed. Instead formatter subclasses
        should re-implement the specific `read_from_*` methods as appropriate.
        Each of these methods has a corresponding class property that must
        be `True` for the method to be called.

        The priority for reading is:

        * `read_from_uri`
        * `read_from_stream`
        * `read_from_local_file`
        * `read_from_uri` (but with a local file)

        Any of these methods can return `NotImplemented` if there is a desire
        to skip to the next one in the list. If a dataset is being requested
        with no component, no parameters, and it should also be added to the
        local cache, the first two calls will be skipped (unless
        `read_from_stream` is the only implemented read method) such that a
        local file will be used.

        A Formatter can also read a file from within a Zip file if the
        URI associated with the `FileDescriptor` corresponds to a file with
        a `.zip` extension and a URI fragment of the form
        ``zip-path={path_in_zip}``. When reading a file from within a Zip
        file the priority for reading is:

        * `read_from_stream`
        * `read_from_local_file`
        * `read_from_uri`

        There are multiple cases that must be handled for reading:

        For a single file:

        * No component requested, read the whole file.
        * Component requested, optionally read the component efficiently,
          else read the whole file and extract the component.
        * Derived component requested, read whole file or read relevant
          component and derive.

        Disassembled Composite:

        * The file to read here is the component itself. Formatter only knows
          about this one component file. Should be no component specified
          in the ``read`` call but the `FileDescriptor` will know which
          component this is.
        * A derived component. The file to read is a component but not the
          specified component. The caching needs the component from which
          it's derived.

        Raises
        ------
        FormatterNotImplementedError
            Raised if no implementations were found that could read this
            resource.
        """
        # If the file to read is a ZIP file with a fragment requesting
        # a file within the ZIP file, it is no longer possible to use the
        # direct read from URI option and the contents of the Zip file must
        # be extracted.
        uri = self.file_descriptor.location.uri
        if uri.fragment and uri.unquoted_fragment.startswith("zip-path="):
            _, _, path_in_zip = uri.unquoted_fragment.partition("=")

            # Open the Zip file using ResourcePath.
            with uri.open("rb") as fd:
                with zipfile.ZipFile(fd) as zf:  # type: ignore
                    if self.can_read_from_stream:
                        with contextlib.closing(zf.open(path_in_zip)) as zip_fd:
                            result = self.read_from_stream(zip_fd, component, expected_size=expected_size)

                        if result is not NotImplemented:
                            return result

                    # For now for both URI and local file options we retrieve
                    # the bytes to a temporary local and use that.
                    _, suffix = os.path.splitext(path_in_zip)
                    with ResourcePath.temporary_uri(suffix=suffix) as tmp_uri:
                        tmp_uri.write(zf.read(path_in_zip))

                        if self.can_read_from_local_file:
                            result = self.read_from_local_file(
                                tmp_uri.ospath, component, expected_size=expected_size
                            )
                            if result is not NotImplemented:
                                return result
                        if self.can_read_from_uri:
                            result = self.read_from_uri(tmp_uri, component, expected_size=expected_size)
                            if result is not NotImplemented:
                                return result

            raise FormatterNotImplementedError(
                f"Formatter {self.name()} could not read the file at {uri} using any method."
            )

        # If the there are no parameters, no component request, and
        # the file should be cached, it is better to defer the read_from_uri
        # to use the local file and populate the cache.
        prefer_local = False
        if (
            component is None
            and not self.file_descriptor.parameters
            and self._ensure_cache(cache_manager).should_be_cached(self._get_cache_ref())
        ):
            prefer_local = True

        # First see if the formatter can support direct remote read from
        # a URI. This can be called later from the local path. If it returns
        # NotImplemented the formatter decided on its own that local
        # reads are preferred.
        if not prefer_local and self.can_read_from_uri:
            result = self.read_directly_from_possibly_cached_uri(
                component, expected_size, cache_manager=cache_manager
            )
            if result is not NotImplemented:
                return result

        # Some formatters might want to be able to read directly from
        # an open file stream. This is preferred over forcing a download
        # to local file system unless a file read option is available and we
        # want to store it in the cache because the whole file is being read.
        if self.can_read_from_stream and not (
            prefer_local and (self.can_read_from_uri or self.can_read_from_local_file)
        ):
            result = self.read_from_possibly_cached_stream(
                component, expected_size, cache_manager=cache_manager
            )
            if result is not NotImplemented:
                return result

        # Finally, try to read the local file.
        if self.can_read_from_local_file or self.can_read_from_uri:
            result = self.read_from_possibly_cached_local_file(
                component, expected_size, cache_manager=cache_manager
            )
            if result is not NotImplemented:
                return result

        raise FormatterNotImplementedError(
            f"Formatter {self.name()} could not read the file at {uri} using any method."
        )

    def _read_from_possibly_cached_location_no_cache_write(
        self,
        callback: Callable[[ResourcePath, str | None, int], Any],
        component: str | None = None,
        expected_size: int = -1,
        *,
        cache_manager: AbstractDatastoreCacheManager | None = None,
    ) -> Any:
        """Read from the cache and call payload without writing to cache."""
        cache_manager = self._ensure_cache(cache_manager)

        uri = self.file_descriptor.location.uri
        cache_ref = self._get_cache_ref()

        # The component for log messages is either the component requested
        # explicitly or the component from the file descriptor.
        log_component = component if component is not None else self.file_descriptor.component

        with cache_manager.find_in_cache(cache_ref, uri.getExtension()) as cached_file:
            if cached_file is not None:
                desired_uri = cached_file
                msg = f" (cached version of {uri})"
            else:
                desired_uri = uri
                msg = ""

            if desired_uri.isLocal:
                # Do not spend the time doing a slow size() call to a remote
                # resource.
                self._check_resource_size(desired_uri, expected_size, desired_uri.size())

            with time_this(
                log,
                msg="Reading%s from file handle %s%s with formatter %s",
                args=(
                    f" component {log_component}" if log_component else "",
                    desired_uri,
                    msg,
                    self.name(),
                ),
            ):
                return callback(desired_uri, component, expected_size)

    def read_from_possibly_cached_stream(
        self,
        component: str | None = None,
        expected_size: int = -1,
        *,
        cache_manager: AbstractDatastoreCacheManager | None = None,
    ) -> Any:
        """Read from a stream, checking for possible presence in local cache.

        Parameters
        ----------
        component : `str`, optional
            Component to read from the file. Only used if the `StorageClass`
            for reading differed from the `StorageClass` used to write the
            file.
        expected_size : `int`, optional
            If known, the expected size of the resource to read. This can be
            used for verification or to decide whether to do a direct read or a
            file download. ``-1`` indicates the file size is not known.
        cache_manager : `AbstractDatastoreCacheManager`
            A cache manager to use to allow a formatter to check if there is
            a copy of the file in the local cache.

        Returns
        -------
        in_memory_dataset : `object` or `NotImplemented`
            The requested Dataset or an indication that the read mode was
            not implemented.

        Notes
        -----
        Calls `read_from_stream` but will first check the datastore cache
        in case the file is present locally. This method will not download
        a file to the local cache.
        """

        def _open_stream(uri: ResourcePath, comp: str | None, size: int = -1) -> Any:
            with uri.open("rb") as fd:
                return self.read_from_stream(fd, comp, expected_size=size)

        return self._read_from_possibly_cached_location_no_cache_write(
            _open_stream, component, expected_size=expected_size, cache_manager=cache_manager
        )

    def read_directly_from_possibly_cached_uri(
        self,
        component: str | None = None,
        expected_size: int = -1,
        *,
        cache_manager: AbstractDatastoreCacheManager | None = None,
    ) -> Any:
        """Read from arbitrary URI, checking for possible presence in local
         cache.

        Parameters
        ----------
        component : `str`, optional
            Component to read from the file. Only used if the `StorageClass`
            for reading differed from the `StorageClass` used to write the
            file.
        expected_size : `int`, optional
            If known, the expected size of the resource to read. This can be
            ``-1`` indicates the file size is not known.
        cache_manager : `AbstractDatastoreCacheManager`
            A cache manager to use to allow a formatter to check if there is
            a copy of the file in the local cache.

        Returns
        -------
        in_memory_dataset : `object` or `NotImplemented`
            The requested Dataset or an indication that the read mode was
            not implemented.

        Notes
        -----
        This method will first check the datastore cache
        in case the file is present locally. This method will not cache a
        remote dataset and will only do a size check for local files to avoid
        unnecessary round trips to a remote server.

        The URI will be read by calling `read_from_uri`.
        """

        def _open_uri(uri: ResourcePath, comp: str | None, size: int = -1) -> Any:
            return self.read_from_uri(uri, comp, expected_size=size)

        return self._read_from_possibly_cached_location_no_cache_write(
            _open_uri, component, expected_size=expected_size, cache_manager=cache_manager
        )

    def read_from_possibly_cached_local_file(
        self,
        component: str | None = None,
        expected_size: int = -1,
        *,
        cache_manager: AbstractDatastoreCacheManager | None = None,
    ) -> Any:
        """Read a dataset ensuring that a local file is used, checking the
        cache for it.

        Parameters
        ----------
        component : `str`, optional
            Component to read from the file. Only used if the `StorageClass`
            for reading differed from the `StorageClass` used to write the
            file.
        expected_size : `int`, optional
            If known, the expected size of the resource to read. This can be
            used for verification or to decide whether to do a direct read or a
            file download. ``-1`` indicates the file size is not known.
        cache_manager : `AbstractDatastoreCacheManager`
            A cache manager to use to allow a formatter to cache a remote file
            locally or read a cached file that is already local.

        Returns
        -------
        in_memory_dataset : `object` or `NotImplemented`
            The requested Dataset or an indication that the read mode was
            not implemented.

        Notes
        -----
        The file will be downloaded and cached if it is a remote resource.
        The file contents will be read using `read_from_local_file` or
        `read_from_uri`, with preference given to the former.
        """
        cache_manager = self._ensure_cache(cache_manager)
        uri = self.file_descriptor.location.uri

        # Need to have something we can look up in the cache.
        cache_ref = self._get_cache_ref()

        # The component for log messages is either the component requested
        # explicitly or the component from the file descriptor.
        log_component = component if component is not None else self.file_descriptor.component

        result = NotImplemented

        # Ensure we have a local file.
        with cache_manager.find_in_cache(cache_ref, uri.getExtension()) as cached_file:
            if cached_file is not None:
                msg = f"(via cache read of remote file {uri})"
                uri = cached_file
            else:
                msg = ""

            with uri.as_local() as local_uri:
                self._check_resource_size(self.file_descriptor.location.uri, expected_size, local_uri.size())
                can_be_cached = False
                if uri != local_uri:
                    # URI was remote and file was downloaded
                    cache_msg = ""

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

                with time_this(
                    log,
                    msg="Reading%s from location %s %s with formatter %s",
                    args=(
                        f" component {log_component}" if log_component else "",
                        uri,
                        msg,
                        self.name(),
                    ),
                ):
                    if self.can_read_from_local_file:
                        result = self.read_from_local_file(
                            local_uri.ospath, component=component, expected_size=expected_size
                        )
                    if result is NotImplemented and self.can_read_from_uri:
                        # If the direct URI reader was skipped earlier and
                        # there is no explicit local file implementation, pass
                        # in the guaranteed local URI to the generic reader.
                        result = self.read_from_uri(
                            local_uri, component=component, expected_size=expected_size
                        )

                # File was read successfully so can move to cache.
                # Also move to cache even if NotImplemented was returned.
                if can_be_cached:
                    cache_manager.move_to_cache(local_uri, cache_ref)

        return result

    def read_from_uri(self, uri: ResourcePath, component: str | None = None, expected_size: int = -1) -> Any:
        """Read a dataset from a URI that can be local or remote.

        Parameters
        ----------
        uri : `lsst.resources.ResourcePath`
            URI to use to read the dataset. This URI can be local or remote
            and can refer to the actual resource or to a locally cached file.
        component : `str` or `None`, optional
            The component to be read from the dataset.
        expected_size : `int`, optional
            If known, the expected size of the resource to read. This can be
            ``-1`` indicates the file size is not known.

        Returns
        -------
        in_memory_dataset : `object` or `NotImplemented`
            The Python object read from the resource or `NotImplemented`.

        Raises
        ------
        FormatterNotImplementedError
            Raised if there is no support for direct reads from a, possibly,
            remote URI.

        Notes
        -----
        This method is only called if the class property
        ``can_read_from_uri`` is set to `True`.

        It is possible that a cached local file will be given to this method
        even if it was originally a remote URI. This can happen if the original
        write resulted in the file being added to the local cache.

        If the full file is being read this file will not be added to the
        local cache. Consider returning `NotImplemented` in
        this situation, for example if there are no parameters or component
        specified, and allowing the system to fall back to calling
        `read_from_local_file` (which will populate the cache if configured
        to do so).
        """
        return NotImplemented

    def read_from_stream(
        self, stream: BinaryIO | ResourceHandleProtocol, component: str | None = None, expected_size: int = -1
    ) -> Any:
        """Read from an open file descriptor.

        Parameters
        ----------
        stream : `lsst.resources.ResourceHandleProtocol` or \
                `typing.BinaryIO`
            File stream to use to read the dataset.
        component : `str` or `None`, optional
            The component to be read from the dataset.
        expected_size : `int`, optional
            If known, the expected size of the resource to read. This can be
            ``-1`` indicates the file size is not known.

        Returns
        -------
        in_memory_dataset : `object` or `NotImplemented`
            The Python object read from the stream or `NotImplemented`.

        Notes
        -----
        Only called if the class property ``can_read_from_stream`` is `True`.
        """
        return NotImplemented

    def read_from_local_file(self, path: str, component: str | None = None, expected_size: int = -1) -> Any:
        """Read a dataset from a URI guaranteed to refer to the local file
        system.

        Parameters
        ----------
        path : `str`
            Path to a local file that should be read.
        component : `str` or `None`, optional
            The component to be read from the dataset.
        expected_size : `int`, optional
            If known, the expected size of the resource to read. This can be
            ``-1`` indicates the file size is not known.

        Returns
        -------
        in_memory_dataset : `object` or `NotImplemented`
            The Python object read from the resource or `NotImplemented`.

        Raises
        ------
        FormatterNotImplementedError
            Raised if there is no implementation written to read data
            from a local file.

        Notes
        -----
        This method will only be called if the class property
        ``can_read_from_local_file`` is `True` and other options were not
        used.
        """
        return NotImplemented

    def add_provenance(
        self, in_memory_dataset: Any, /, *, provenance: DatasetProvenance | None = None
    ) -> Any:
        """Add provenance to the dataset.

        Parameters
        ----------
        in_memory_dataset : `object`
            The dataset to serialize.
        provenance : `DatasetProvenance` or `None`, optional
            Provenance to attach to dataset.

        Returns
        -------
        dataset_to_write : `object`
            The dataset to use for serialization. Can be the same object as
            given.

        Notes
        -----
        The base class implementation returns the given object unchanged.
        """
        return in_memory_dataset

    @final
    def write(
        self,
        in_memory_dataset: Any,
        /,
        *,
        cache_manager: AbstractDatastoreCacheManager | None = None,
        provenance: DatasetProvenance | None = None,
    ) -> None:
        """Write a Dataset.

        Parameters
        ----------
        in_memory_dataset : `object`
            The Dataset to serialize.
        cache_manager : `AbstractDatastoreCacheManager`
            A cache manager to use to allow a formatter to cache the written
            file.
        provenance : `DatasetProvenance` | `None`, optional
            Provenance to attach to the file being written.

        Raises
        ------
        FormatterNotImplementedError
            Raised if the formatter subclass has not implemented
            `write_local_file` and `to_bytes` was not called.
        Exception
            Raised if there is an error serializing the dataset to disk.

        Notes
        -----
        The intent is for subclasses to implement either `to_bytes` or
        `write_local_file` or both and not to subclass this method.
        """
        # Ensure we are using the correct file extension.
        uri = self.file_descriptor.location.uri.updatedExtension(self.get_write_extension())

        # Attach any provenance to the dataset. This could involve returning
        # a different object.
        in_memory_dataset = self.add_provenance(in_memory_dataset, provenance=provenance)

        written = self.write_direct(in_memory_dataset, uri, cache_manager)
        if not written:
            self.write_locally_then_move(in_memory_dataset, uri, cache_manager)

    def write_direct(
        self,
        in_memory_dataset: Any,
        uri: ResourcePath,
        cache_manager: AbstractDatastoreCacheManager | None = None,
    ) -> bool:
        """Serialize and write directly to final location.

        Parameters
        ----------
        in_memory_dataset : `object`
            The Dataset to serialize.
        uri : `lsst.resources.ResourcePath`
            URI to use when writing the serialized dataset.
        cache_manager : `AbstractDatastoreCacheManager`
            A cache manager to use to allow a formatter to cache the written
            file.

        Returns
        -------
        written : `bool`
            Flag to indicate whether the direct write did happen.

        Raises
        ------
        Exception
            Raised if there was a failure from serializing to bytes that
            was not `FormatterNotImplementedError`.

        Notes
        -----
        This method will call `to_bytes` to serialize the in-memory dataset
        and then will call the `~lsst.resources.ResourcePath.write` method
        directly.

        If the dataset should be cached or is local the file will not be
        written and the method will return `False`. This is because local URIs
        should be written to a temporary file name and then renamed to allow
        atomic writes. That path is handled by `write_locally_then_move`
        through `write_local_file`) and is preferred over this method being
        subclassed and the atomic write re-implemented.
        """
        cache_manager = self._ensure_cache(cache_manager)

        # For remote URIs some datasets can be serialized directly
        # to bytes and sent to the remote datastore without writing a
        # file. If the dataset is intended to be saved to the cache
        # a file is always written and direct write to the remote
        # datastore is bypassed.
        data_written = False
        if not uri.isLocal and not cache_manager.should_be_cached(self._get_cache_ref()):
            # Remote URI that is not cached so can write directly.
            try:
                serialized_dataset = self.to_bytes(in_memory_dataset)
            except FormatterNotImplementedError:
                # Fallback to the file writing option.
                pass
            except Exception as e:
                e.add_note(
                    f"Failed to serialize dataset {self.dataset_ref} of "
                    f"type {get_full_type_name(in_memory_dataset)} to bytes."
                )
                raise
            else:
                log.debug("Writing bytes directly to %s", uri)
                uri.write(serialized_dataset, overwrite=True)
                log.debug("Successfully wrote bytes directly to %s", uri)
                data_written = True
        return data_written

    def write_locally_then_move(
        self,
        in_memory_dataset: Any,
        uri: ResourcePath,
        cache_manager: AbstractDatastoreCacheManager | None = None,
    ) -> None:
        """Write file to file system and then move to final location.

        Parameters
        ----------
        in_memory_dataset : `object`
            The Dataset to serialize.
        uri : `lsst.resources.ResourcePath`
            URI to use when writing the serialized dataset.
        cache_manager : `AbstractDatastoreCacheManager`
            A cache manager to use to allow a formatter to cache the written
            file.

        Raises
        ------
        FormatterNotImplementedError
            Raised if the formatter subclass has not implemented
            `write_local_file`.
        Exception
            Raised if there is an error serializing the dataset to disk.
        """
        cache_manager = self._ensure_cache(cache_manager)

        # Always write to a temporary even if
        # using a local file system -- that gives us atomic writes.
        # If a process is killed as the file is being written we do not
        # want it to remain in the correct place but in corrupt state.
        # For local files write to the output directory not temporary dir.
        prefix = uri.dirname() if uri.isLocal else None
        with ResourcePath.temporary_uri(suffix=uri.getExtension(), prefix=prefix) as temporary_uri:
            # Need to configure the formatter to write to a different
            # location and that needs us to overwrite internals
            log.debug("Writing dataset to temporary location at %s", temporary_uri)

            # Assumes that if write_local_file is not subclassed that
            # to_bytes will be called using the base class definition.
            try:
                self.write_local_file(in_memory_dataset, temporary_uri)
            except Exception as e:
                e.add_note(
                    f"Failed to serialize dataset {self.dataset_ref} of type"
                    f" {get_full_type_name(in_memory_dataset)} to "
                    f"temporary location {temporary_uri}."
                )
                raise

            # Use move for a local file since that becomes an efficient
            # os.rename. For remote resources we use copy to allow the
            # file to be cached afterwards.
            transfer = "move" if uri.isLocal else "copy"

            uri.transfer_from(temporary_uri, transfer=transfer, overwrite=True)

            if transfer == "copy":
                # Cache if required
                cache_manager.move_to_cache(temporary_uri, self._get_cache_ref())

        log.debug("Successfully wrote dataset to %s via a temporary file.", uri)

    def write_local_file(self, in_memory_dataset: Any, uri: ResourcePath) -> None:
        """Serialize the in-memory dataset to a local file.

        Parameters
        ----------
        in_memory_dataset : `object`
            The Python object to serialize.
        uri : `ResourcePath`
            The URI to use when writing the file.

        Notes
        -----
        By default this method will attempt to call `to_bytes` and then
        write these bytes to the file.

        Raises
        ------
        FormatterNotImplementedError
            Raised if the formatter subclass has not implemented this method
            or has failed to implement the `to_bytes` method.
        """
        log.debug("Writing bytes directly to %s", uri)
        uri.write(self.to_bytes(in_memory_dataset))
        log.debug("Successfully wrote bytes directly to %s", uri)

    def to_bytes(self, in_memory_dataset: Any) -> bytes:
        """Serialize the in-memory dataset to bytes.

        Parameters
        ----------
        in_memory_dataset : `object`
            The Python object to serialize.

        Returns
        -------
        serialized_dataset : `bytes`
            Bytes representing the serialized dataset.

        Raises
        ------
        FormatterNotImplementedError
            Raised if the formatter has not implemented the method. This will
            not cause a problem if `write_local_file` has been implemented.
        """
        raise FormatterNotImplementedError(
            f"This formatter can not convert {get_full_type_name(in_memory_dataset)} directly to bytes."
        )

    def make_updated_location(self, location: Location) -> Location:
        """Return a new `Location` updated with this formatter's extension.

        Parameters
        ----------
        location : `Location`
            The location to update.

        Returns
        -------
        updated : `Location`
            A new `Location` with a new file extension applied.
        """
        location = location.clone()
        # If the extension is "" the extension will be removed.
        location.updateExtension(self.get_write_extension())
        return location

    @classmethod
    def validate_extension(cls, location: Location) -> None:
        """Check the extension of the provided location for compatibility.

        Parameters
        ----------
        location : `Location`
            Location from which to extract a file extension.

        Raises
        ------
        ValueError
            Raised if the formatter does not understand this extension.
        """
        supported = set(cls.supported_extensions)
        default = cls.default_extension  # type: ignore

        # If extension is implemented as an instance property it won't return
        # a string when called as a class property. Assume that
        # the supported extensions class property is complete.
        if default is not None and isinstance(default, str):
            supported.add(default)

        # Get the file name from the uri
        file = location.uri.basename()

        # Check that this file name ends with one of the supported extensions.
        # This is less prone to confusion than asking the location for
        # its extension and then doing a set comparison
        for ext in supported:
            if file.endswith(ext):
                return

        raise ValueError(
            f"Extension '{location.getExtension()}' on '{location}' "
            f"is not supported by Formatter '{cls.__name__}' (supports: {supported})"
        )

    def predict_path(self) -> str:
        """Return the path that would be returned by write.

        Does not write any data file.

        Uses the `FileDescriptor` associated with the instance.

        Returns
        -------
        path : `str`
            Path within datastore that would be associated with the location
            stored in this `Formatter`.
        """
        updated = self.make_updated_location(self.file_descriptor.location)
        return updated.pathInStore.path

    def segregate_parameters(self, parameters: dict[str, Any] | None = None) -> tuple[dict, dict]:
        """Segregate the supplied parameters.

        This splits the parameters into those understood by the
        formatter and those not understood by the formatter.

        Any unsupported parameters are assumed to be usable by associated
        assemblers.

        Parameters
        ----------
        parameters : `dict`, optional
            Parameters with values that have been supplied by the caller
            and which might be relevant for the formatter.  If `None`
            parameters will be read from the registered `FileDescriptor`.

        Returns
        -------
        supported : `dict`
            Those parameters supported by this formatter.
        unsupported : `dict`
            Those parameters not supported by this formatter.
        """
        if parameters is None:
            parameters = self.file_descriptor.parameters

        if parameters is None:
            return {}, {}

        if self.unsupported_parameters is None:
            # Support none of the parameters
            return {}, parameters.copy()

        # Start by assuming all are supported
        supported = parameters.copy()
        unsupported = {}

        # And remove any we know are not supported
        for p in set(supported):
            if p in self.unsupported_parameters:
                unsupported[p] = supported.pop(p)

        return supported, unsupported


class Formatter(metaclass=ABCMeta):
    """Interface for reading and writing Datasets.

    The formatters are associated with a particular `StorageClass`.

    Parameters
    ----------
    fileDescriptor : `FileDescriptor`, optional
        Identifies the file to read or write, and the associated storage
        classes and parameter information.
    dataId : `DataCoordinate`
        Data ID associated with this formatter.
    writeParameters : `dict`, optional
        Parameters to control how the dataset is serialized.
    writeRecipes : `dict`, optional
        Detailed write recipes indexed by recipe name.
    **kwargs
        Additional parameters that can allow parameters
        from `FormatterV2` to be provided.

    Notes
    -----
    All Formatter subclasses should share the base class's constructor
    signature.
    """

    # Now assuming that Formatter v1 can only refer to files so can add
    # this property for compatibility with v2.
    extension: str | None = None
    """Default file extension to use for writing files. None means that no
    modifications will be made to the supplied file extension. (`str`)"""

    unsupportedParameters: ClassVar[Set[str] | None] = frozenset()
    """Set of read parameters not understood by this `Formatter`. An empty set
    means all parameters are supported.  `None` indicates that no parameters
    are supported. These parameters should match those defined in the storage
    class definition. (`frozenset`).
    """

    supportedWriteParameters: ClassVar[Set[str] | None] = None
    """Parameters understood by this formatter that can be used to control
    how a dataset is serialized. `None` indicates that no parameters are
    supported."""

    supportedExtensions: ClassVar[Set[str]] = frozenset()
    """Set of all extensions supported by this formatter.

    Only expected to be populated by Formatters that write files. Any extension
    assigned to the ``extension`` property will be automatically included in
    the list of supported extensions."""

    def __init__(
        self,
        fileDescriptor: FileDescriptor,
        *,
        dataId: DataCoordinate | None = None,
        writeParameters: Mapping[str, Any] | None = None,
        writeRecipes: Mapping[str, Any] | None = None,
        # Allow FormatterV2 parameters to be dropped.
        **kwargs: Any,
    ):
        if not isinstance(fileDescriptor, FileDescriptor):
            raise TypeError("File descriptor must be a FileDescriptor")
        self._fileDescriptor = fileDescriptor

        if dataId is None:
            raise RuntimeError("dataId is now required for formatter initialization")
        if not isinstance(dataId, DataCoordinate):
            raise TypeError(f"DataId is required to be a DataCoordinate but got {type(dataId)}.")
        self._dataId = dataId

        # V2 compatibility.
        if writeParameters is None:
            writeParameters = kwargs.get("write_parameters")
        if writeRecipes is None:
            writeRecipes = kwargs.get("write_recipes")

        # Check that the write parameters are allowed
        if writeParameters:
            if self.supportedWriteParameters is None:
                raise ValueError(
                    f"This formatter does not accept any write parameters. Got: {', '.join(writeParameters)}"
                )
            else:
                given = set(writeParameters)
                unknown = given - self.supportedWriteParameters
                if unknown:
                    s = "s" if len(unknown) != 1 else ""
                    unknownStr = ", ".join(f"'{u}'" for u in unknown)
                    raise ValueError(f"This formatter does not accept parameter{s} {unknownStr}")

        self._writeParameters = writeParameters
        self._writeRecipes = self.validate_write_recipes(writeRecipes)

    def __str__(self) -> str:
        return f"{self.name()}@{self.fileDescriptor.location.path}"

    def __repr__(self) -> str:
        return f"{self.name()}({self.fileDescriptor!r})"

    @property
    def fileDescriptor(self) -> FileDescriptor:
        """File descriptor associated with this formatter
        (`FileDescriptor`).
        """
        return self._fileDescriptor

    @property
    def dataId(self) -> DataCoordinate:
        """Return Data ID associated with this formatter (`DataCoordinate`)."""
        return self._dataId

    @property
    def writeParameters(self) -> Mapping[str, Any]:
        """Parameters to use when writing out datasets."""
        if self._writeParameters is not None:
            return self._writeParameters
        return {}

    @property
    def writeRecipes(self) -> Mapping[str, Any]:
        """Detailed write Recipes indexed by recipe name."""
        if self._writeRecipes is not None:
            return self._writeRecipes
        return {}

    def can_accept(self, in_memory_dataset: Any) -> bool:
        """Indicate whether this formatter can accept the specified
        storage class directly.

        Parameters
        ----------
        in_memory_dataset : `object`
            The dataset that is to be written.

        Returns
        -------
        accepts : `bool`
            If `True` the formatter can write data of this type without
            requiring datastore to convert it. If `False` the datastore
            will attempt to convert before writing.

        Notes
        -----
        The base class checks that the given python type matches
        the python type specified for this formatter when
        constructed.
        """
        return isinstance(in_memory_dataset, self.file_descriptor.storageClass.pytype)

    @classmethod
    def validateWriteRecipes(cls, recipes: Mapping[str, Any] | None) -> Mapping[str, Any] | None:
        """Validate supplied recipes for this formatter.

        The recipes are supplemented with default values where appropriate.

        Parameters
        ----------
        recipes : `dict`
            Recipes to validate.

        Returns
        -------
        validated : `dict`
            Validated recipes.

        Raises
        ------
        RuntimeError
            Raised if validation fails.  The default implementation raises
            if any recipes are given.
        """
        if recipes:
            raise RuntimeError(f"This formatter does not understand these writeRecipes: {recipes}")
        return recipes

    @classmethod
    def validate_write_recipes(cls, recipes: Mapping[str, Any] | None) -> Mapping[str, Any] | None:
        return cls.validateWriteRecipes(recipes)

    @classmethod
    def name(cls) -> str:
        """Return the fully qualified name of the formatter.

        Returns
        -------
        name : `str`
            Fully-qualified name of formatter class.
        """
        return get_full_type_name(cls)

    @abstractmethod
    def read(self, component: str | None = None) -> Any:
        """Read a Dataset.

        Parameters
        ----------
        component : `str`, optional
            Component to read from the file. Only used if the `StorageClass`
            for reading differed from the `StorageClass` used to write the
            file.

        Returns
        -------
        inMemoryDataset : `object`
            The requested Dataset.
        """
        raise FormatterNotImplementedError("Type does not support reading")

    @abstractmethod
    def write(self, inMemoryDataset: Any) -> None:
        """Write a Dataset.

        Parameters
        ----------
        inMemoryDataset : `object`
            The Dataset to store.
        """
        raise FormatterNotImplementedError("Type does not support writing")

    @classmethod
    def can_read_bytes(cls) -> bool:
        """Indicate if this formatter can format from bytes.

        Returns
        -------
        can : `bool`
            `True` if the `fromBytes` method is implemented.
        """
        # We have no property to read so instead try to format from a byte
        # and see what happens
        try:
            # We know the arguments are incompatible
            cls.fromBytes(cls, b"")  # type: ignore
        except FormatterNotImplementedError:
            return False
        except Exception:
            # There will be problems with the bytes we are supplying so ignore
            pass
        return True

    def fromBytes(self, serializedDataset: bytes, component: str | None = None) -> object:
        """Read serialized data into a Dataset or its component.

        Parameters
        ----------
        serializedDataset : `bytes`
            Bytes object to unserialize.
        component : `str`, optional
            Component to read from the Dataset. Only used if the `StorageClass`
            for reading differed from the `StorageClass` used to write the
            file.

        Returns
        -------
        inMemoryDataset : `object`
            The requested data as a Python object. The type of object
            is controlled by the specific formatter.
        """
        raise FormatterNotImplementedError("Type does not support reading from bytes.")

    def toBytes(self, inMemoryDataset: Any) -> bytes:
        """Serialize the Dataset to bytes based on formatter.

        Parameters
        ----------
        inMemoryDataset : `object`
            The Python object to serialize.

        Returns
        -------
        serializedDataset : `bytes`
            Bytes representing the serialized dataset.
        """
        raise FormatterNotImplementedError("Type does not support writing to bytes.")

    @contextlib.contextmanager
    def _updateLocation(self, location: Location | None) -> Iterator[Location]:
        """Temporarily replace the location associated with this formatter.

        Parameters
        ----------
        location : `Location`
            New location to use for this formatter. If `None` the
            formatter will not change but it will still return
            the old location. This allows it to be used in a code
            path where the location may not need to be updated
            but the with block is still convenient.

        Yields
        ------
        old : `Location`
            The old location that will be restored.

        Notes
        -----
        This is an internal method that should be used with care.
        It may change in the future. Should be used as a context
        manager to restore the location when the temporary is no
        longer required.
        """
        old = self._fileDescriptor.location
        try:
            if location is not None:
                self._fileDescriptor.location = location
            yield old
        finally:
            if location is not None:
                self._fileDescriptor.location = old

    def make_updated_location(self, location: Location) -> Location:
        """Return a new `Location` updated with this formatter's extension.

        Parameters
        ----------
        location : `Location`
            The location to update.

        Returns
        -------
        updated : `Location`
            A new `Location` with a new file extension applied.

        Raises
        ------
        NotImplementedError
            Raised if there is no ``extension`` attribute associated with
            this formatter.

        Notes
        -----
        This method is available to all Formatters but might not be
        implemented by all formatters. It requires that a formatter set
        an ``extension`` attribute containing the file extension used when
        writing files.  If ``extension`` is `None` the supplied file will
        not be updated. Not all formatters write files so this is not
        defined in the base class.
        """
        location = location.clone()
        try:
            # We are deliberately allowing extension to be undefined by
            # default in the base class and mypy complains.
            location.updateExtension(self.extension)  # type:ignore
        except AttributeError:
            raise NotImplementedError("No file extension registered with this formatter") from None
        return location

    @classmethod
    def validate_extension(cls, location: Location) -> None:
        """Check the extension of the provided location for compatibility.

        Parameters
        ----------
        location : `Location`
            Location from which to extract a file extension.

        Raises
        ------
        NotImplementedError
            Raised if file extensions are a concept not understood by this
            formatter.
        ValueError
            Raised if the formatter does not understand this extension.

        Notes
        -----
        This method is available to all Formatters but might not be
        implemented by all formatters. It requires that a formatter set
        an ``extension`` attribute containing the file extension used when
        writing files.  If ``extension`` is `None` only the set of supported
        extensions will be examined.
        """
        supported = set(cls.supportedExtensions)

        try:
            # We are deliberately allowing extension to be undefined by
            # default in the base class and mypy complains.
            default = cls.extension  # type: ignore
        except AttributeError:
            raise NotImplementedError("No file extension registered with this formatter") from None

        # If extension is implemented as an instance property it won't return
        # a string when called as a class property. Assume that
        # the supported extensions class property is complete.
        if default is not None and isinstance(default, str):
            supported.add(default)

        # Get the file name from the uri
        file = location.uri.basename()

        # Check that this file name ends with one of the supported extensions.
        # This is less prone to confusion than asking the location for
        # its extension and then doing a set comparison
        for ext in supported:
            if file.endswith(ext):
                return

        raise ValueError(
            f"Extension '{location.getExtension()}' on '{location}' "
            f"is not supported by Formatter '{cls.__name__}' (supports: {supported})"
        )

    def predict_path(self) -> str:
        """Return the path that would be returned by write.

        Does not write any data file.

        Uses the `FileDescriptor` associated with the instance.

        Returns
        -------
        path : `str`
            Path within datastore that would be associated with the location
            stored in this `Formatter`.
        """
        updated = self.make_updated_location(self.fileDescriptor.location)
        return updated.pathInStore.path

    def segregate_parameters(self, parameters: dict[str, Any] | None = None) -> tuple[dict, dict]:
        """Segregate the supplied parameters.

        This splits the parameters into those understood by the
        formatter and those not understood by the formatter.

        Any unsupported parameters are assumed to be usable by associated
        assemblers.

        Parameters
        ----------
        parameters : `dict`, optional
            Parameters with values that have been supplied by the caller
            and which might be relevant for the formatter.  If `None`
            parameters will be read from the registered `FileDescriptor`.

        Returns
        -------
        supported : `dict`
            Those parameters supported by this formatter.
        unsupported : `dict`
            Those parameters not supported by this formatter.
        """
        if parameters is None:
            parameters = self.fileDescriptor.parameters

        if parameters is None:
            return {}, {}

        if self.unsupportedParameters is None:
            # Support none of the parameters
            return {}, parameters.copy()

        # Start by assuming all are supported
        supported = parameters.copy()
        unsupported = {}

        # And remove any we know are not supported
        for p in set(supported):
            if p in self.unsupportedParameters:
                unsupported[p] = supported.pop(p)

        return supported, unsupported

    # Support classic V1 interface.
    makeUpdatedLocation = make_updated_location
    validateExtension = validate_extension
    segregateParameters = segregate_parameters
    predictPath = predict_path

    # Compatibility with V2 properties.
    @property
    def write_parameters(self) -> Mapping[str, Any]:
        return self.writeParameters

    @property
    def write_recipes(self) -> Mapping[str, Any]:
        return self.writeRecipes

    @property
    def file_descriptor(self) -> FileDescriptor:
        return self.fileDescriptor

    @property
    def data_id(self) -> DataCoordinate:
        return self.dataId


class FormatterFactory:
    """Factory for `Formatter` instances."""

    defaultKey = LookupKey("default")
    """Configuration key associated with default write parameter settings."""

    writeRecipesKey = LookupKey("write_recipes")
    """Configuration key associated with write recipes."""

    def __init__(self) -> None:
        self._mappingFactory = MappingFactory(Formatter)

    def __contains__(self, key: LookupKey | str) -> bool:
        """Indicate whether the supplied key is present in the factory.

        Parameters
        ----------
        key : `LookupKey`, `str` or objects with ``name`` attribute
            Key to use to lookup in the factory whether a corresponding
            formatter is present.

        Returns
        -------
        in : `bool`
            `True` if the supplied key is present in the factory.
        """
        return key in self._mappingFactory

    def registerFormatters(self, config: Config, *, universe: DimensionUniverse) -> None:
        """Bulk register formatters from a config.

        Parameters
        ----------
        config : `Config`
            ``formatters`` section of a configuration.
        universe : `DimensionUniverse`, optional
            Set of all known dimensions, used to expand and validate any used
            in lookup keys.

        Notes
        -----
        The configuration can include one level of hierarchy where an
        instrument-specific section can be defined to override more general
        template specifications.  This is represented in YAML using a
        key of form ``instrument<name>`` which can then define templates
        that will be returned if a `DatasetRef` contains a matching instrument
        name in the data ID.

        The config is parsed using the function
        `~lsst.daf.butler.configSubset.processLookupConfigs`.

        The values for formatter entries can be either a simple string
        referring to a python type or a dict representing the formatter and
        parameters to be hard-coded into the formatter constructor. For
        the dict case the following keys are supported:

        - formatter: The python type to be used as the formatter class.
        - parameters: A further dict to be passed directly to the
            ``write_parameters`` Formatter constructor to seed it.
            These parameters are validated at instance creation and not at
            configuration.

        Additionally, a special ``default`` section can be defined that
        uses the formatter type (class) name as the keys and specifies
        default write parameters that should be used whenever an instance
        of that class is constructed.

        .. code-block:: yaml

           formatters:
             default:
               lsst.daf.butler.formatters.example.ExampleFormatter:
                 max: 10
                 min: 2
                 comment: Default comment
             calexp: lsst.daf.butler.formatters.example.ExampleFormatter
             coadd:
               formatter: lsst.daf.butler.formatters.example.ExampleFormatter
               parameters:
                 max: 5

        Any time an ``ExampleFormatter`` is constructed it will use those
        parameters. If an explicit entry later in the configuration specifies
        a different set of parameters, the two will be merged with the later
        entry taking priority.  In the example above ``calexp`` will use
        the default parameters but ``coadd`` will override the value for
        ``max``.

        Formatter configuration can also include a special section describing
        collections of write parameters that can be accessed through a
        simple label.  This allows common collections of options to be
        specified in one place in the configuration and reused later.
        The ``write_recipes`` section is indexed by Formatter class name
        and each key is the label to associate with the parameters.

        .. code-block:: yaml

           formatters:
             write_recipes:
               lsst.obs.base.formatters.fitsExposure.FixExposureFormatter:
                 lossless:
                   ...
                 noCompression:
                   ...

        By convention a formatter that uses write recipes will support a
        ``recipe`` write parameter that will refer to a recipe name in
        the ``write_recipes`` component.  The `Formatter` will be constructed
        in the `FormatterFactory` with all the relevant recipes and
        will not attempt to filter by looking at ``write_parameters`` in
        advance.  See the specific formatter documentation for details on
        acceptable recipe options.
        """
        allowed_keys = {"formatter", "parameters"}

        contents = processLookupConfigs(config, allow_hierarchy=True, universe=universe)

        # Extract any default parameter settings
        defaultParameters = contents.get(self.defaultKey, {})
        if not isinstance(defaultParameters, Mapping):
            raise RuntimeError(
                "Default formatter parameters in config can not be a single string"
                f" (got: {type(defaultParameters)})"
            )

        # Extract any global write recipes -- these are indexed by
        # Formatter class name.
        writeRecipes = contents.get(self.writeRecipesKey, {})
        if isinstance(writeRecipes, str):
            raise RuntimeError(
                f"The formatters.{self.writeRecipesKey} section must refer to a dict not '{writeRecipes}'"
            )

        for key, f in contents.items():
            # default is handled in a special way
            if key == self.defaultKey:
                continue
            if key == self.writeRecipesKey:
                continue

            # Can be a str or a dict.
            specificWriteParameters = {}
            if isinstance(f, str):
                formatter = f
            elif isinstance(f, Mapping):
                all_keys = set(f)
                unexpected_keys = all_keys - allowed_keys
                if unexpected_keys:
                    raise ValueError(f"Formatter {key} uses unexpected keys {unexpected_keys} in config")
                if "formatter" not in f:
                    raise ValueError(f"Mandatory 'formatter' key missing for formatter key {key}")
                formatter = f["formatter"]
                if "parameters" in f:
                    specificWriteParameters = f["parameters"]
            else:
                raise ValueError(f"Formatter for key {key} has unexpected value: '{f}'")

            # Apply any default parameters for this formatter
            writeParameters = copy.deepcopy(defaultParameters.get(formatter, {}))
            writeParameters.update(specificWriteParameters)

            kwargs: dict[str, Any] = {}
            if writeParameters:
                kwargs["write_parameters"] = writeParameters

            if formatter in writeRecipes:
                kwargs["write_recipes"] = writeRecipes[formatter]

            self.registerFormatter(key, formatter, **kwargs)

    def getLookupKeys(self) -> set[LookupKey]:
        """Retrieve the look up keys for all the registry entries.

        Returns
        -------
        keys : `set` of `LookupKey`
            The keys available for matching in the registry.
        """
        return self._mappingFactory.getLookupKeys()

    def getFormatterClassWithMatch(
        self, entity: Entity
    ) -> tuple[LookupKey, type[Formatter | FormatterV2], dict[str, Any]]:
        """Get the matching formatter class along with the registry key.

        Parameters
        ----------
        entity : `DatasetRef`, `DatasetType`, `StorageClass`, or `str`
            Entity to use to determine the formatter to return.
            `StorageClass` will be used as a last resort if `DatasetRef`
            or `DatasetType` instance is provided.  Supports instrument
            override if a `DatasetRef` is provided configured with an
            ``instrument`` value for the data ID.

        Returns
        -------
        matchKey : `LookupKey`
            The key that resulted in the successful match.
        formatter : `type`
            The class of the registered formatter.
        formatter_kwargs : `dict`
            Keyword arguments that are associated with this formatter entry.
        """
        names = (LookupKey(name=entity),) if isinstance(entity, str) else entity._lookupNames()
        matchKey, formatter, formatter_kwargs = self._mappingFactory.getClassFromRegistryWithMatch(names)
        log.debug(
            "Retrieved formatter %s from key '%s' for entity '%s'",
            get_full_type_name(formatter),
            matchKey,
            entity,
        )

        return matchKey, formatter, formatter_kwargs

    def getFormatterClass(self, entity: Entity) -> type:
        """Get the matching formatter class.

        Parameters
        ----------
        entity : `DatasetRef`, `DatasetType`, `StorageClass`, or `str`
            Entity to use to determine the formatter to return.
            `StorageClass` will be used as a last resort if `DatasetRef`
            or `DatasetType` instance is provided.  Supports instrument
            override if a `DatasetRef` is provided configured with an
            ``instrument`` value for the data ID.

        Returns
        -------
        formatter : `type`
            The class of the registered formatter.
        """
        _, formatter, _ = self.getFormatterClassWithMatch(entity)
        return formatter

    def getFormatterWithMatch(
        self, entity: Entity, *args: Any, **kwargs: Any
    ) -> tuple[LookupKey, Formatter | FormatterV2]:
        """Get a new formatter instance along with the matching registry key.

        Parameters
        ----------
        entity : `DatasetRef`, `DatasetType`, `StorageClass`, or `str`
            Entity to use to determine the formatter to return.
            `StorageClass` will be used as a last resort if `DatasetRef`
            or `DatasetType` instance is provided.  Supports instrument
            override if a `DatasetRef` is provided configured with an
            ``instrument`` value for the data ID.
        *args : `tuple`
            Positional arguments to use pass to the object constructor.
        **kwargs
            Keyword arguments to pass to object constructor.

        Returns
        -------
        matchKey : `LookupKey`
            The key that resulted in the successful match.
        formatter : `Formatter`
            An instance of the registered formatter.
        """
        names = (LookupKey(name=entity),) if isinstance(entity, str) else entity._lookupNames()
        matchKey, formatter = self._mappingFactory.getFromRegistryWithMatch(names, *args, **kwargs)
        log.debug(
            "Retrieved formatter %s from key '%s' for entity '%s'",
            get_full_type_name(formatter),
            matchKey,
            entity,
        )

        return matchKey, formatter

    def getFormatter(self, entity: Entity, *args: Any, **kwargs: Any) -> Formatter | FormatterV2:
        """Get a new formatter instance.

        Parameters
        ----------
        entity : `DatasetRef`, `DatasetType`, `StorageClass`, or `str`
            Entity to use to determine the formatter to return.
            `StorageClass` will be used as a last resort if `DatasetRef`
            or `DatasetType` instance is provided.  Supports instrument
            override if a `DatasetRef` is provided configured with an
            ``instrument`` value for the data ID.
        *args : `tuple`
            Positional arguments to use pass to the object constructor.
        **kwargs
            Keyword arguments to pass to object constructor.

        Returns
        -------
        formatter : `Formatter`
            An instance of the registered formatter.
        """
        _, formatter = self.getFormatterWithMatch(entity, *args, **kwargs)
        return formatter

    def registerFormatter(
        self,
        type_: LookupKey | str | StorageClass | DatasetType,
        formatter: str,
        *,
        overwrite: bool = False,
        **kwargs: Any,
    ) -> None:
        """Register a `Formatter`.

        Parameters
        ----------
        type_ : `LookupKey`, `str`, `StorageClass` or `DatasetType`
            Type for which this formatter is to be used.  If a `LookupKey`
            is not provided, one will be constructed from the supplied string
            or by using the ``name`` property of the supplied entity.
        formatter : `str` or class of type `Formatter`
            Identifies a `Formatter` subclass to use for reading and writing
            Datasets of this type.  Can be a `Formatter` class.
        overwrite : `bool`, optional
            If `True` an existing entry will be replaced by the new value.
            Default is `False`.
        **kwargs
            Keyword arguments to always pass to object constructor when
            retrieved.

        Raises
        ------
        ValueError
            Raised if the formatter does not name a valid formatter type and
            ``overwrite`` is `False`.
        """
        self._mappingFactory.placeInRegistry(type_, formatter, overwrite=overwrite, **kwargs)


class FormatterV1inV2(FormatterV2):
    """An implementation of a V2 formatter that provides a compatibility
    interface for V1 formatters.

    Parameters
    ----------
    file_descriptor : `FileDescriptor`, optional
        Identifies the file to read or write, and the associated storage
        classes and parameter information.  Its value can be `None` if the
        caller will never call `Formatter.read` or `Formatter.write`.
    ref : `DatasetRef`
        The dataset associated with this formatter. Should not be a component
        dataset ref.
    formatter : `Formatter`
        A version 1 `Formatter` instance. The V2 formatter layer forwards calls
        to this formatter.
    write_parameters : `dict`, optional
        Any parameters to be hard-coded into this instance to control how
        the dataset is serialized.
    write_recipes : `dict`, optional
        Detailed write Recipes indexed by recipe name.
    **kwargs
        Additional arguments that will be ignored but allow for
        `Formatter` V1 parameters to be given.
    """

    can_read_from_local_file = True
    """This formatter can read from a local file."""

    def __init__(
        self,
        file_descriptor: FileDescriptor,
        *,
        ref: DatasetRef,
        formatter: Formatter,
        write_parameters: Mapping[str, Any] | None = None,
        write_recipes: Mapping[str, Any] | None = None,
        # Compatibility parameters. Unused in v2.
        **kwargs: Any,
    ):
        if not isinstance(formatter, Formatter):
            raise TypeError(f"Formatter parameter was not a V1 formatter (was {type(formatter)})")

        # Replace the class property with instance values from this
        # V1 formatter so that the V2 __init__ will be able to validate.
        self.supported_write_parameters = formatter.supportedWriteParameters  # type: ignore
        self._formatter = formatter

        super().__init__(
            file_descriptor, ref=ref, write_parameters=write_parameters, write_recipes=write_recipes
        )

    def get_write_extension(self) -> str:
        ext = self._formatter.extension
        return ext if ext is not None else ""

    def segregate_parameters(self, parameters: dict[str, Any] | None = None) -> tuple[dict, dict]:
        return self._formatter.segregate_parameters(parameters)

    def validate_write_recipes(  # type: ignore
        self,
        recipes: Mapping[str, Any] | None,
    ) -> Mapping[str, Any] | None:
        # This should be a class method but a class method can not work
        # for a dynamic shim. Luckily the shim is only used as an instance.
        return self._formatter.validate_write_recipes(recipes)

    def read_from_local_file(self, path: str, component: str | None = None, expected_size: int = -1) -> Any:
        # Need to temporarily override the location since the V1 formatter
        # will not know anything about this local file.

        # V2 does not have a fromBytes equivalent.
        if self._formatter.can_read_bytes():
            with open(path, "rb") as fd:
                serialized_dataset = fd.read()
            return self._formatter.fromBytes(serialized_dataset, component=component)

        location = Location(None, path)
        with self._formatter._updateLocation(location):
            try:
                result = self._formatter.read(component=component)
            except NotImplementedError:
                # V1 raises NotImplementedError but V2 is expecting something
                # slightly different.
                return NotImplemented
        return result

    def to_bytes(self, in_memory_dataset: Any) -> bytes:
        try:
            return self._formatter.toBytes(in_memory_dataset)
        except NotImplementedError as e:
            # V1 raises NotImplementedError but V2 is expecting something
            # slightly different.
            raise FormatterNotImplementedError(str(e)) from e

    def write_local_file(self, in_memory_dataset: Any, uri: ResourcePath) -> None:
        with self._formatter._updateLocation(Location(None, uri)):
            try:
                self._formatter.write(in_memory_dataset)
            except NotImplementedError as e:
                # V1 raises NotImplementedError but V2 is expecting something
                # slightly different.
                raise FormatterNotImplementedError(str(e)) from e


# Type to use when allowing a Formatter or its class name
FormatterParameter: TypeAlias = str | type[Formatter] | Formatter | FormatterV2 | type[FormatterV2]

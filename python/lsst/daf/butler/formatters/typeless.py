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

"""Support for reading and writing files to a POSIX file system."""

from __future__ import annotations

__all__ = ["TypelessFormatter"]

import dataclasses
from typing import TYPE_CHECKING, Any

from lsst.daf.butler import FormatterV2

if TYPE_CHECKING:
    from lsst.daf.butler import StorageClass
    from lsst.daf.butler.datastore.cache_manager import AbstractDatastoreCacheManager


class TypelessFormatter(FormatterV2):
    """Formatter V2 base class that attempts to coerce generic objects
    read in subclasses into the correct Python type.

    Notes
    -----
    This class provides a ``read()`` method that will run `FormatterV2.read`
    and coerce the return type using a variety of techniques. Use the
    standard `FormatterV2` methods for reading bytes/files and writing
    bytes/files.
    """

    def read(
        self,
        component: str | None = None,
        expected_size: int = -1,
        cache_manager: AbstractDatastoreCacheManager | None = None,
    ) -> Any:
        # Do the standard read of the base class.
        data = super().read(component, expected_size, cache_manager)

        # Assemble the requested dataset and potentially return only its
        # component coercing it to its appropriate pytype.
        data = self._assemble_dataset(data, component)

        # Special case components by allowing a formatter to return None
        # to indicate that the component was understood but is missing.
        if data is None and component is None:
            raise ValueError(f"Unable to read data with URI {self.file_descriptor.location.uri}")

        return data

    def _assemble_dataset(self, data: Any, component: str | None = None) -> Any:
        """Assembles and coerces the dataset, or one of its components,
        into an appropriate python type and returns it.

        Parameters
        ----------
        data : `dict` or `object`
            Composite or a dict that, or which component, needs to be
            coerced to the python type specified in "fileDescriptor"
        component : `str`, optional
            Component to read from the file. Only used if the `StorageClass`
            for reading differed from the `StorageClass` used to write the
            file.

        Returns
        -------
        inMemoryDataset : `object`
            The requested data as a Python object. The type of object
            is controlled by the specific formatter.
        """
        file_descriptor = self.file_descriptor

        # Get the read and write storage classes.
        read_storage_class = file_descriptor.readStorageClass
        write_storage_class = file_descriptor.storageClass

        if component is not None:
            # Requesting a component implies that we need to first ensure
            # that the composite is the correct python type. Lie to the
            # coercion routine since the read StorageClass is not relevant
            # if we want the original.
            data = self._coerce_type(data, write_storage_class, write_storage_class)

            # Concrete composite written as a single file (we hope)
            # so try to get the component.
            try:
                data = file_descriptor.storageClass.delegate().getComponent(data, component)
            except AttributeError:
                # Defer the complaint
                data = None

            # Update the write storage class to match that of the component.
            # It should be safe to use the component storage class directly
            # since that should match what was returned from getComponent
            # (else we could create a temporary storage class guaranteed to
            # match the python type we have).
            write_storage_class = write_storage_class.allComponents()[component]

        # Coerce to the requested type.
        data = self._coerce_type(data, write_storage_class, read_storage_class)

        return data

    def _coerce_builtin_type(self, in_memory_dataset: Any, write_storage_class: StorageClass) -> Any:
        """Coerce the supplied in-memory dataset to the written python type if
        it is currently a built-in type.

        Parameters
        ----------
        in_memory_dataset : `object`
            Object to coerce to expected type.
        write_storage_class : `StorageClass`
            Storage class used to serialize this data.

        Returns
        -------
        in_memory_dataset : `object`
            Object of expected type ``write_storage_class.pytype``.

        Notes
        -----
        This method only modifies the supplied object if the object is:

        * Not already the required type.
        * Not `None`.
        * Looks like a built-in type.

        It is intended to be used as a helper for file formats that do not
        store the original Python type information in serialized form and
        instead return built-in types such as `dict` and `list` that need
        to be converted to the required form. This happens before
        `StorageClass` converters trigger so that constructors can be
        called that can build the original type first before checking the
        requested Python type. This is important for Pydantic models where
        the internal structure of the model may not match the `dict` form
        in a scenario where the user has requested a `dict`.
        """
        if (
            in_memory_dataset is not None
            and not isinstance(in_memory_dataset, write_storage_class.pytype)
            and type(in_memory_dataset).__module__ == "builtins"
        ):
            # Try different ways of converting to the required type.
            # Pydantic v1 uses parse_obj and some non-pydantic classes
            # use that convention. Pydantic v2 uses model_validate.
            for method_name in ("model_validate", "parse_obj"):
                if method := getattr(write_storage_class.pytype, method_name, None):
                    return method(in_memory_dataset)
            if isinstance(in_memory_dataset, dict):
                if dataclasses.is_dataclass(write_storage_class.pytype):
                    # Dataclasses accept key/value parameters.
                    in_memory_dataset = write_storage_class.pytype(**in_memory_dataset)
                elif write_storage_class.isComposite():
                    # Assume that this type can be constructed
                    # using the registered assembler from a dict.
                    in_memory_dataset = write_storage_class.delegate().assemble(
                        in_memory_dataset, pytype=write_storage_class.pytype
                    )
                else:
                    # Unpack the dict and hope that works.
                    in_memory_dataset = write_storage_class.pytype(**in_memory_dataset)
            else:
                # Hope that we can pass the arguments in directly.
                in_memory_dataset = write_storage_class.pytype(in_memory_dataset)

        return in_memory_dataset

    def _coerce_type(
        self, in_memory_dataset: Any, write_storage_class: StorageClass, read_storage_class: StorageClass
    ) -> Any:
        """Coerce the supplied in-memory dataset to the correct python type.

        Parameters
        ----------
        in_memory_dataset : `object`
            Object to coerce to expected type.
        write_storage_class : `StorageClass`
            Storage class used to serialize this data.
        read_storage_class : `StorageClass`
            Storage class requested as the outcome.

        Returns
        -------
        in_memory_dataset : `object`
            Object of expected type ``readStorageClass.pytype``.
        """
        in_memory_dataset = self._coerce_builtin_type(in_memory_dataset, write_storage_class)
        return read_storage_class.coerce_type(in_memory_dataset)

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
    "DoNothingFormatter",
    "FormatterTest",
    "LenientYamlFormatter",
    "MetricsExampleFormatter",
    "MultipleExtensionsFormatter",
    "SingleExtensionFormatter",
)

import json
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, BinaryIO

import yaml

from lsst.resources import ResourceHandleProtocol

from .._formatter import Formatter, FormatterV2
from ..formatters.json import JsonFormatter
from ..formatters.yaml import YamlFormatter

if TYPE_CHECKING:
    from .._dataset_provenance import DatasetProvenance
    from .._location import Location


class DoNothingFormatter(Formatter):
    """A test formatter that does not need to format anything and has
    parameters.
    """

    def read(self, component: str | None = None) -> Any:
        raise NotImplementedError("Type does not support reading")

    def write(self, inMemoryDataset: Any) -> None:
        raise NotImplementedError("Type does not support writing")


class FormatterTest(Formatter):
    """A test formatter that does not need to format anything."""

    supportedWriteParameters = frozenset({"min", "max", "median", "comment", "extra", "recipe"})

    def read(self, component: str | None = None) -> Any:
        raise NotImplementedError("Type does not support reading")

    def write(self, inMemoryDataset: Any) -> None:
        raise NotImplementedError("Type does not support writing")

    @staticmethod
    def validate_write_recipes(recipes: Mapping[str, Any] | None) -> Mapping[str, Any] | None:
        if not recipes:
            return recipes
        for recipeName in recipes:
            if "mode" not in recipes[recipeName]:
                raise RuntimeError("'mode' is a required write recipe parameter")
        return recipes


class SingleExtensionFormatter(DoNothingFormatter):
    """A do nothing formatter that has a single extension registered."""

    extension = ".fits"


class MultipleExtensionsFormatter(SingleExtensionFormatter):
    """A formatter that has multiple extensions registered."""

    supportedExtensions = frozenset({".fits.gz", ".fits.fz", ".fit"})


class LenientYamlFormatter(YamlFormatter):
    """A test formatter that allows any file extension but always reads and
    writes YAML.
    """

    @classmethod
    def validate_extension(cls, location: Location) -> None:
        return


class MetricsExampleFormatter(FormatterV2):
    """A specialist test formatter for metrics that supports components
    directly without assembler delegate.
    """

    supported_extensions = frozenset({".yaml", ".json"})
    default_extension = ".yaml"
    can_read_from_stream = True

    def read_from_stream(
        self, stream: BinaryIO | ResourceHandleProtocol, component: str | None = None, expected_size: int = -1
    ) -> Any:
        """Read data from a file.

        Parameters
        ----------
        stream : `typing.BinaryIO` or `lsst.resources.ResourceHandleProtocol`
            Open file handle to read from.
        component : `str`, optional
            Component to read from the file. Only used if the `StorageClass`
            for reading differed from the `StorageClass` used to write the
            file.
        expected_size : `int`, optional
            The expected size of the resource.

        Returns
        -------
        inMemoryDataset : `object`
            The requested data as a Python object. The type of object
            is controlled by the specific formatter.

        Raises
        ------
        ValueError
            Component requested but this file does not seem to be a concrete
            composite.
        KeyError
            Raised when parameters passed with fileDescriptor are not
            supported.
        """
        # This formatter can not read a subset from disk because it
        # uses yaml or json.
        if ".yaml" in stream.name:
            data = yaml.load(stream, Loader=yaml.SafeLoader)
        elif ".json" in stream.name:
            data = json.load(stream)
        else:
            raise RuntimeError(f"Unsupported file extension found in path '{stream.name}'")

        # We can slice up front if required
        parameters = self.file_descriptor.parameters
        if "data" in data and parameters and "slice" in parameters:
            data["data"] = data["data"][parameters["slice"]]

        pytype = self.file_descriptor.storageClass.pytype
        in_memory_dataset = pytype(**data)

        if not component:
            return in_memory_dataset

        if component == "summary":
            return in_memory_dataset.summary
        elif component == "output":
            return in_memory_dataset.output
        elif component == "data":
            return in_memory_dataset.data
        elif component == "counter":
            return len(in_memory_dataset.data)
        raise ValueError(f"Unsupported component: {component}")

    def to_bytes(self, in_memory_dataset: Any) -> bytes:
        """Write a Dataset.

        Parameters
        ----------
        in_memory_dataset : `object`
            The Dataset to store.

        Returns
        -------
        serialized_dataset : `bytes`
            The in-memory dataset as bytes.
        """
        serialized = yaml.dump(in_memory_dataset._asdict())
        return serialized.encode()


class MetricsExampleDataFormatter(Formatter):
    """A specialist test formatter for the data component of a MetricsExample.

    This is needed if the MetricsExample is disassembled and we want to
    support the derived component.
    """

    unsupportedParameters = None
    """Let the assembler delegate handle slice"""

    extension = ".yaml"
    """Always write YAML"""

    def read(self, component: str | None = None) -> Any:
        """Read data from a file.

        Parameters
        ----------
        component : `str`, optional
            Component to read from the file. Only used if the `StorageClass`
            for reading differed from the `StorageClass` used to write the
            file.

        Returns
        -------
        inMemoryDataset : `object`
            The requested data as a Python object. The type of object
            is controlled by the specific formatter.

        Raises
        ------
        ValueError
            Component requested but this file does not seem to be a concrete
            composite.
        KeyError
            Raised when parameters passed with fileDescriptor are not
            supported.
        """
        # This formatter can not read a subset from disk because it
        # uses yaml.
        path = self.fileDescriptor.location.path
        with open(path) as fd:
            data = yaml.load(fd, Loader=yaml.SafeLoader)

        # We can slice up front if required
        parameters = self.fileDescriptor.parameters
        if parameters and "slice" in parameters:
            data = data[parameters["slice"]]

        # This should be a native list
        inMemoryDataset = data

        if not component:
            return inMemoryDataset

        if component == "counter":
            return len(inMemoryDataset)
        raise ValueError(f"Unsupported component: {component}")

    def write(self, inMemoryDataset: Any) -> None:
        """Write a Dataset.

        Parameters
        ----------
        inMemoryDataset : `object`
            The Dataset to store.

        Returns
        -------
        path : `str`
            The path to where the Dataset was stored within the datastore.
        """
        fileDescriptor = self.fileDescriptor

        # Update the location with the formatter-preferred file extension
        fileDescriptor.location.updateExtension(self.extension)

        with open(fileDescriptor.location.path, "w") as fd:
            yaml.dump(inMemoryDataset, fd)


class MetricsExampleModelProvenanceFormatter(JsonFormatter):
    """Specialist formatter to test provenance addition."""

    def add_provenance(
        self, in_memory_dataset: Any, /, *, provenance: DatasetProvenance | None = None
    ) -> Any:
        # Copy it to prove that works.
        new = in_memory_dataset.model_copy()
        new.provenance = provenance
        new.dataset_id = self.dataset_ref.id
        return new
